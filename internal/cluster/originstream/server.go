package originstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// FloorsSource returns the primary's current floors, or nil while it has
// none to offer (coverage off).
type FloorsSource func() *Floors

// Server is the primary side: it keeps one session per standby writer, each
// with its own queue, so a slow standby only loses its own batches.
type Server struct {
	queueBytes int64
	floors     FloorsSource
	logger     zerolog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	closed   bool
	stop     chan struct{}
	wg       sync.WaitGroup

	lastDropLog atomic.Int64
}

type session struct {
	node   string
	conn   net.Conn
	key    []byte
	logger zerolog.Logger

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*Entry
	bytes  int64   // queued plus being written: released as each frame is written
	floors *Floors // latest floors not yet sent
	closed bool
	seq    uint64
	// floorsSet counts setFloors calls; floorsWritten is the count whose
	// floors were last written (FlushFloors waits on it).
	floorsSet, floorsWritten uint64
}

const (
	floorsEvery      = time.Second
	floorsHeartbeat  = 5 * time.Second
	frameWriteBudget = 30 * time.Second
	// DefaultQueueBytes bounds what the primary holds for one standby.
	DefaultQueueBytes = 64 << 20
)

// NewServer builds the primary side. Start it with Start.
func NewServer(queueBytes int64, floors FloorsSource, logger zerolog.Logger) *Server {
	if queueBytes <= 0 {
		queueBytes = DefaultQueueBytes
	}
	return &Server{queueBytes: queueBytes, floors: floors, logger: logger, sessions: map[string]*session{}, stop: make(chan struct{})}
}

// Start runs the floors publisher.
func (s *Server) Start() {
	s.wg.Add(1)
	go s.publishFloors()
}

// Hook is the WAL replication hook. It runs under an ingest shard lock, so
// it only appends to in-memory queues and never blocks. Entries without
// coverage are not streamed.
func (s *Server) Hook(re *wal.ReplicationEntry) {
	if re == nil || re.Coverage == nil {
		return
	}
	if len(re.Coverage.Key) > maxKeyLen || len(re.Payload) > maxFrame {
		return // cannot be framed; the WAL caps both far below this
	}
	e := &Entry{
		TimestampUS: re.TimestampUS,
		Instance:    re.Coverage.Instance,
		Key:         re.Coverage.Key,
		CSeq:        re.Coverage.CSeq,
		Chunk:       re.Coverage.Chunk,
		Chunks:      re.Coverage.Chunks,
		Payload:     re.Payload,
	}
	s.mu.Lock()
	for _, ss := range s.sessions {
		if !ss.enqueue(e, s.queueBytes) {
			s.noteDrop(ss.node)
		}
	}
	s.mu.Unlock()
}

func (s *Server) noteDrop(node string) {
	metrics.Get().IncOriginStreamDropped()
	now := time.Now().UnixNano()
	if last := s.lastDropLog.Load(); now-last > int64(10*time.Second) && s.lastDropLog.CompareAndSwap(last, now) {
		s.logger.Warn().Str("standby", node).
			Msg("Standby writer is not keeping up with ingest; batches not sent to it are missing from its copy until registered")
	}
}

func entrySize(e *Entry) int64 { return int64(len(e.Payload) + len(e.Key) + entryFixed) }

func (ss *session) enqueue(e *Entry, budget int64) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.closed {
		return true
	}
	size := entrySize(e)
	// An empty queue admits any one entry, or a batch larger than the
	// budget would never reach any standby.
	if ss.bytes > 0 && ss.bytes+size > budget {
		return false
	}
	ss.queue = append(ss.queue, e)
	ss.bytes += size
	ss.cond.Signal()
	return true
}

// Accept takes over conn for standby node after a successful hello. A
// previous session for the same node is closed.
func (s *Server) Accept(node string, conn net.Conn, sessionKey []byte) {
	ss := &session{node: node, conn: conn, key: sessionKey, logger: s.logger.With().Str("standby", node).Logger()}
	ss.cond = sync.NewCond(&ss.mu)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	old := s.sessions[node]
	if old != nil {
		old.markClosed()
	}
	s.sessions[node] = ss
	metrics.Get().SetOriginStreamSessions(int64(len(s.sessions)))
	s.wg.Add(1) // under s.mu, so it never races Stop's Wait
	s.mu.Unlock()
	if old != nil {
		old.closeConn()
	}
	if f := s.floors(); f != nil {
		ss.setFloors(f)
	}
	ss.logger.Info().Msg("Streaming committed batches to standby writer")
	go func() {
		defer s.wg.Done()
		ss.run()
		s.mu.Lock()
		if s.sessions[node] == ss {
			delete(s.sessions, node)
		}
		metrics.Get().SetOriginStreamSessions(int64(len(s.sessions)))
		s.mu.Unlock()
	}()
}

func (ss *session) setFloors(f *Floors) uint64 {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.floors = f
	ss.floorsSet++
	ss.cond.Signal()
	return ss.floorsSet
}

// flushed reports whether the floors of setFloors call gen (or later) were
// written (done and ok), or the session ended first (done, not ok).
func (ss *session) flushed(gen uint64) (done, ok bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.floorsWritten >= gen {
		return true, true
	}
	return ss.closed, false
}

// FlushFloors sends the current floors to every session and waits until
// each wrote them (or ended), or ctx ends. A writer leaving the cluster
// calls it last, so its standbys know every batch it registered (#1207).
func (s *Server) FlushFloors(ctx context.Context) error {
	f := s.floors()
	if f == nil {
		return nil
	}
	s.mu.Lock()
	type wait struct {
		ss  *session
		gen uint64
	}
	waits := make([]wait, 0, len(s.sessions))
	for _, ss := range s.sessions {
		waits = append(waits, wait{ss, ss.setFloors(f)})
	}
	s.mu.Unlock()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	var lost []string
	for _, w := range waits {
		for {
			done, ok := w.ss.flushed(w.gen)
			if done {
				if !ok {
					lost = append(lost, w.ss.node)
				}
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	if len(lost) > 0 {
		return fmt.Errorf("origin stream sessions ended before the floors were sent: %v", lost)
	}
	return nil
}

// markClosed stops the session under its lock; closeConn closes the
// connection, never under a lock: Server.Hook waits on s.mu with an ingest
// shard lock held, and closing a TLS connection writes close_notify, which
// can block for its write deadline on a peer that stopped reading.
func (ss *session) markClosed() {
	ss.mu.Lock()
	ss.closed = true
	ss.cond.Broadcast()
	ss.mu.Unlock()
}

func (ss *session) closeConn() { _ = ss.conn.Close() }

func (ss *session) close() {
	ss.markClosed()
	ss.closeConn()
}

// run writes queued entries in order, then pending floors. Floors go after
// the entries queued before them, never ahead.
func (ss *session) run() {
	defer ss.close()
	for {
		ss.mu.Lock()
		for !ss.closed && len(ss.queue) == 0 && ss.floors == nil {
			ss.cond.Wait()
		}
		if ss.closed {
			ss.mu.Unlock()
			return
		}
		batch := ss.queue
		ss.queue = nil
		f := ss.floors
		ss.floors = nil
		gen := ss.floorsSet
		ss.mu.Unlock()
		for _, e := range batch {
			if !validEntry(e) {
				ss.mu.Lock()
				ss.bytes -= entrySize(e)
				ss.mu.Unlock()
				continue
			}
			ss.seq++
			err := ss.write(frameEntry, entryHead(ss.seq, e), e.Payload)
			ss.mu.Lock()
			ss.bytes -= entrySize(e)
			ss.mu.Unlock()
			if err != nil {
				ss.logger.Info().Err(err).Msg("Origin stream session ended")
				return
			}
		}
		if f != nil {
			ss.seq++
			head, err := floorsHead(ss.seq, f)
			if err == nil {
				err = ss.write(frameFloors, head, nil)
			}
			if err != nil {
				// A floors frame that cannot be written is not a network
				// blip when it is too large: say so loudly.
				ss.logger.Warn().Err(err).Msg("Origin stream session ended writing registration progress")
				return
			}
			ss.mu.Lock()
			ss.floorsWritten = gen
			ss.mu.Unlock()
		}
	}
}

func (ss *session) write(typ byte, head, payload []byte) error {
	_ = ss.conn.SetWriteDeadline(time.Now().Add(frameWriteBudget))
	return writeFrame(ss.conn, ss.key, typ, head, payload)
}

// publishFloors sends the floors to every session when they change, and at
// least every floorsHeartbeat so a standby can tell a quiet primary from a
// dead connection.
func (s *Server) publishFloors() {
	defer s.wg.Done()
	t := time.NewTicker(floorsEvery)
	defer t.Stop()
	var last []byte
	lastSent := time.Time{}
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		f := s.floors()
		if f == nil {
			continue
		}
		raw, _ := json.Marshal(f)
		if bytes.Equal(raw, last) && time.Since(lastSent) < floorsHeartbeat {
			continue
		}
		last, lastSent = raw, time.Now()
		s.mu.Lock()
		for _, ss := range s.sessions {
			ss.setFloors(f)
		}
		s.mu.Unlock()
	}
}

// CloseSessions ends every session; standbys reconnect to whichever writer
// is then the designated primary. Called when this node stops being it.
func (s *Server) CloseSessions() {
	for _, ss := range s.snapshotAndMark() {
		ss.closeConn()
	}
}

func (s *Server) snapshotAndMark() []*session {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*session, 0, len(s.sessions))
	for _, ss := range s.sessions {
		ss.markClosed()
		out = append(out, ss)
	}
	return out
}

// Sessions reports the standby nodes currently streamed to.
func (s *Server) Sessions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.sessions))
	for n := range s.sessions {
		out = append(out, n)
	}
	return out
}

// Stop ends every session and the publisher.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.stop)
	s.mu.Unlock()
	for _, ss := range s.snapshotAndMark() {
		ss.closeConn()
	}
	s.wg.Wait()
	metrics.Get().SetOriginStreamSessions(0)
}
