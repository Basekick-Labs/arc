package originstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

var testKey = bytes.Repeat([]byte{7}, 32)

func TestEntryFrameRoundTripAndTamper(t *testing.T) {
	e := &Entry{TimestampUS: 11, Instance: 0xabc, Key: "db/cpu", CSeq: 5, Chunk: 1, Chunks: 3, Payload: []byte("payload-bytes")}
	var buf bytes.Buffer
	if err := writeFrame(&buf, testKey, frameEntry, entryHead(9, e), e.Payload); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	typ, body, err := readFrame(bytes.NewReader(raw), testKey)
	if err != nil || typ != frameEntry {
		t.Fatalf("readFrame = %d, %v", typ, err)
	}
	seq, got, err := parseEntry(body)
	if err != nil || seq != 9 || got.Key != "db/cpu" || got.CSeq != 5 || got.Chunk != 1 || got.Chunks != 3 || got.Instance != 0xabc || got.TimestampUS != 11 || string(got.Payload) != "payload-bytes" {
		t.Fatalf("parseEntry = %d %+v %v", seq, got, err)
	}
	// Every byte after the length is authenticated: type, seq, ts, coverage,
	// key, payload and the MAC itself.
	for i := 4; i < len(raw); i++ {
		bad := append([]byte(nil), raw...)
		bad[i] ^= 0x01
		if _, _, err := readFrame(bytes.NewReader(bad), testKey); err == nil {
			t.Fatalf("a flipped byte at %d passed authentication", i)
		}
	}
	if _, _, err := readFrame(bytes.NewReader(raw), bytes.Repeat([]byte{8}, 32)); !errors.Is(err, errMAC) {
		t.Fatalf("another session key: %v", err)
	}
}

func TestFloorsFrameRoundTrip(t *testing.T) {
	f := &Floors{Instance: 3, AppliedIndex: 42, Keys: map[string]KeyFloor{"db/cpu": {Floor: 7, Settled: [][2]uint64{{9, 12}}, Lost: []uint64{8}}}}
	head, err := floorsHead(4, f)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := writeFrame(&buf, testKey, frameFloors, head, nil); err != nil {
		t.Fatal(err)
	}
	typ, body, err := readFrame(&buf, testKey)
	if err != nil || typ != frameFloors {
		t.Fatal(err)
	}
	seq, got, err := parseFloors(body)
	if err != nil || seq != 4 || got.AppliedIndex != 42 || got.Keys["db/cpu"].Floor != 7 || got.Keys["db/cpu"].Settled[0] != [2]uint64{9, 12} {
		t.Fatalf("parseFloors = %d %+v %v", seq, got, err)
	}
}

func openTestStore(t *testing.T, dir string, max int64) *Store {
	t.Helper()
	s, err := OpenStore(dir, max, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func ent(inst uint64, key string, cseq uint64) *Entry {
	return &Entry{Instance: inst, Key: key, CSeq: cseq, Chunks: 1, Payload: []byte(fmt.Sprintf("%s-%d", key, cseq))}
}

func keyGaps(s *Store, origin string, inst uint64, key string) intervalSet {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.instances[instanceID{origin, inst}]
	if d == nil || d.keys[key] == nil {
		return nil
	}
	return append(intervalSet(nil), d.keys[key].gaps...)
}

func TestStoreGapTracking(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []uint64{1, 2, 5} { // 3 and 4 never arrived
		must(s.Append("p", ent(1, "db/a", c)))
	}
	// cseq 6 in three chunks, chunk 1 missing.
	must(s.Append("p", &Entry{Instance: 1, Key: "db/a", CSeq: 6, Chunk: 0, Chunks: 3}))
	must(s.Append("p", &Entry{Instance: 1, Key: "db/a", CSeq: 6, Chunk: 2, Chunks: 3}))
	// cseq 7 starts with chunk 0 of 2, and its chunk 1 never arrives: 8 comes.
	must(s.Append("p", &Entry{Instance: 1, Key: "db/a", CSeq: 7, Chunk: 0, Chunks: 2}))
	must(s.Append("p", ent(1, "db/a", 8)))
	want := intervalSet{{3, 4}, {6, 7}}
	if got := keyGaps(s, "p", 1, "db/a"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("gaps = %v, want %v", got, want)
	}
	if st := s.Stats(); st.IncompleteKeys != 1 {
		t.Fatalf("incomplete keys = %d, want 1", st.IncompleteKeys)
	}
}

func TestStoreRebuildsAndTruncatesADamagedTail(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	for c := uint64(1); c <= 3; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "p", "*", "seg-*.recv"))
	if len(segs) != 1 {
		t.Fatalf("segments = %v", segs)
	}
	f, _ := os.OpenFile(segs[0], os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write([]byte{0, 0, 0, 40, 1, 2}) // a torn record
	_ = f.Close()
	r := openTestStore(t, dir, 0)
	st := r.Stats()
	if st.Segments != 1 {
		t.Fatalf("stats = %+v", st)
	}
	r.mu.Lock()
	k := r.instances[instanceID{"p", 1}].keys["db/a"]
	r.mu.Unlock()
	if k.last != 3 {
		t.Fatalf("rebuilt last = %d, want 3", k.last)
	}
	info, _ := os.Stat(segs[0])
	if info.Size() != st.Bytes {
		t.Fatalf("file %d bytes, store says %d: the torn tail was not truncated", info.Size(), st.Bytes)
	}
}

func TestStoreCapStopsAndResumes(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 200)
	var stored int
	for c := uint64(1); c <= 20; c++ {
		before := s.Stats().Bytes
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
		if s.Stats().Bytes > before {
			stored++
		}
	}
	if st := s.Stats(); !st.Capped || st.Bytes > 200 || stored == 20 {
		t.Fatalf("stats = %+v stored=%d: the cap did not stop storing", st, stored)
	}
	if g := keyGaps(s, "p", 1, "db/a"); len(g) == 0 {
		t.Fatal("batches dropped at the cap are not marked missing")
	}
	// Floors cover everything and the files are here: trim frees the store
	// and storing resumes.
	if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 20}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Trim(context.Background(), allReady); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("p", ent(1, "db/a", 21)); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Capped || st.Bytes == 0 {
		t.Fatalf("stats after trim = %+v: storing did not resume", st)
	}
}

func TestStoreTrimNeedsFloorsAndLocalFiles(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	s.segBytes = 1 // one record per segment
	for c := uint64(1); c <= 6; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}

	// No floors: nothing goes.
	if _, _ = s.Trim(context.Background(), allReady); s.Stats().Segments != 6 {
		t.Fatalf("trimmed without floors: %+v", s.Stats())
	}
	// Floor 2, settled 4..5, lost 3; files not here yet: nothing goes.
	f := &Floors{Instance: 1, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 2, Settled: [][2]uint64{{4, 5}}, Lost: []uint64{3}}}}
	if err := s.AcceptFloors("p", f); err != nil {
		t.Fatal(err)
	}
	if _, _ = s.Trim(context.Background(), noneReady); s.Stats().Segments != 6 {
		t.Fatalf("trimmed without the files: %+v", s.Stats())
	}
	var asked uint64
	if _, _ = s.Trim(context.Background(), func(_ context.Context, keys []TrimKey) map[TrimKey]bool {
		out := map[TrimKey]bool{}
		for _, k := range keys {
			asked = k.AppliedIndex
			out[k] = k.Origin == "p" && k.Key == "db/a"
		}
		return out
	}); s.Stats().Segments != 2 {
		t.Fatalf("segments after trim = %d, want 2 (lost 3 and unsettled 6 stay)", s.Stats().Segments)
	}
	if asked != 9 {
		t.Fatalf("readiness asked for applied index %d, want 9", asked)
	}
	// The floors survive a restart: a reopened store trims by them.
	_ = s.Close()
	r := openTestStore(t, dir, 0)
	if err := r.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 6, Lost: []uint64{3}}}}); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	r2 := openTestStore(t, dir, 0)
	if _, _ = r2.Trim(context.Background(), allReady); r2.Stats().Segments != 0 {
		// The floor passed the lost batch 3: it is registered after all.
		t.Fatalf("segments after a reloaded floor = %d, want 0", r2.Stats().Segments)
	}
}

func TestIntervalSetWithRange(t *testing.T) {
	var v intervalSet
	v = v.withRange(5, 7)
	v = v.withRange(1, 2)
	v = v.withRange(3, 4) // joins both
	v = v.withRange(10, 10)
	if fmt.Sprint(v) != "[[1 7] [10 10]]" {
		t.Fatalf("set = %v", v)
	}
	if fmt.Sprint(v.above(6)) != "[[7 7] [10 10]]" {
		t.Fatalf("above = %v", v.above(6))
	}
}

// --- end to end over loopback ---

type testPrimary struct {
	server   *Server
	ln       net.Listener
	instance uint64
	secret   string
	refuse   atomic.Bool
}

func startTestPrimary(t *testing.T, floors FloorsSource, queueBytes int64) *testPrimary {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &testPrimary{server: NewServer(queueBytes, floors, zerolog.Nop()), ln: ln, instance: 0x77, secret: "s3cret"}
	p.server.Start()
	t.Cleanup(func() { ln.Close(); p.server.Stop() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
			if err != nil {
				conn.Close()
				continue
			}
			h := msg.Payload.(*protocol.OriginStreamHello)
			errText := ""
			if p.refuse.Load() || security.ValidateOriginStreamHelloHMAC(p.secret, h.Version, h.Nonce, h.NodeID, h.ClusterName, h.Timestamp, h.HMAC, time.Minute) != nil {
				errText = "refused"
			}
			ts := time.Now().Unix()
			inst := strconv.FormatUint(p.instance, 16)
			ack := &protocol.OriginStreamHelloAck{OriginNodeID: "primary", Instance: inst, Error: errText, Timestamp: ts,
				HMAC: security.ComputeOriginStreamHelloAckHMAC(p.secret, h.Nonce, "primary", inst, errText, ts)}
			_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgOriginStreamHelloAck, Payload: ack}, 5*time.Second)
			if errText != "" {
				conn.Close()
				continue
			}
			key, _ := security.DeriveOriginStreamSessionKey(p.secret, h.Nonce)
			p.server.Accept(h.NodeID, conn, key)
		}
	}()
	return p
}

func startTestStandby(t *testing.T, p *testPrimary, node string, store *Store) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := NewClient(ClientConfig{
		Store: store, LocalNodeID: node, ClusterName: "c", SharedSecret: p.secret,
		Target: func() (string, string, bool) { return "primary", p.ln.Addr().String(), true },
		Dial: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Ready:  allReady,
		Logger: zerolog.Nop(),
	})
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return cancel
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func hook(p *testPrimary, key string, cseq uint64, payload string) {
	p.server.Hook(&wal.ReplicationEntry{TimestampUS: 1, Payload: []byte(payload),
		Coverage: &wal.Coverage{Instance: p.instance, Key: key, CSeq: cseq, Chunks: 1}})
}

func TestEndToEndEntriesAndFloors(t *testing.T) {
	var floorsVal atomic.Pointer[Floors]
	p := startTestPrimary(t, func() *Floors { return floorsVal.Load() }, 0)
	store := openTestStore(t, t.TempDir(), 0)
	startTestStandby(t, p, "standby-1", store)
	waitFor(t, "session", func() bool { return len(p.server.Sessions()) == 1 })

	p.server.Hook(&wal.ReplicationEntry{Payload: []byte("no coverage")}) // not streamed
	for c := uint64(1); c <= 50; c++ {
		hook(p, "db/a", c, fmt.Sprintf("row-%d", c))
	}
	waitFor(t, "50 entries", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		d := store.instances[instanceID{"primary", p.instance}]
		return d != nil && d.keys["db/a"] != nil && d.keys["db/a"].last == 50
	})
	if g := keyGaps(store, "primary", p.instance, "db/a"); len(g) != 0 {
		t.Fatalf("gaps on a clean stream: %v", g)
	}
	floorsVal.Store(&Floors{Instance: p.instance, AppliedIndex: 5, Keys: map[string]KeyFloor{"db/a": {Floor: 50}}})
	waitFor(t, "floors persisted", func() bool {
		raw, err := os.ReadFile(filepath.Join(store.root, "primary", fmt.Sprintf("%016x", p.instance), floorsFile))
		return err == nil && bytes.Contains(raw, []byte(`"f":50`))
	})
	if _, err := store.Trim(context.Background(), allReady); err != nil {
		t.Fatal(err)
	}
	if st := store.Stats(); st.Bytes != 0 {
		t.Fatalf("store after trim = %+v", st)
	}
}

// One standby that stops reading loses its own batches; the other keeps
// receiving everything.
func TestSlowStandbyOnlyLosesItsOwn(t *testing.T) {
	p := startTestPrimary(t, func() *Floors { return nil }, 8<<20)
	good := openTestStore(t, t.TempDir(), 0)
	startTestStandby(t, p, "good", good)
	// A "standby" that completes the hello and then never reads.
	conn, err := net.Dial("tcp", p.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	nonce, _ := security.GenerateNonce()
	ts := time.Now().Unix()
	_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgOriginStreamHello, Payload: &protocol.OriginStreamHello{
		Version: ProtocolVersion, NodeID: "stuck", ClusterName: "c", Nonce: nonce, Timestamp: ts, HMAC: security.ComputeOriginStreamHelloHMAC(p.secret, ProtocolVersion, nonce, "stuck", "c", ts)}}, time.Second)
	if _, err := protocol.ReceiveMessage(conn, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "two sessions", func() bool { return len(p.server.Sessions()) == 2 })

	dropped := metrics.Get().Snapshot()["origin_stream_dropped"].(int64)
	big := string(bytes.Repeat([]byte("x"), 64<<10))
	const n = 600 // ~38 MiB: past the stuck connection's socket buffers and its 8 MiB queue
	for c := uint64(1); c <= n; c++ {
		hook(p, "db/a", c, big)
		time.Sleep(time.Millisecond) // ~64 MiB/s: the good standby keeps up, under -race too
	}
	waitFor(t, "the good standby to receive everything", func() bool {
		good.mu.Lock()
		defer good.mu.Unlock()
		d := good.instances[instanceID{"primary", p.instance}]
		return d != nil && d.keys["db/a"] != nil && d.keys["db/a"].last == n
	})
	if g := keyGaps(good, "primary", p.instance, "db/a"); len(g) != 0 {
		t.Fatalf("the good standby has gaps %v because another standby is slow", g)
	}
	if after := metrics.Get().Snapshot()["origin_stream_dropped"].(int64); after <= dropped {
		t.Fatal("the stuck standby lost nothing: the test did not exercise a full queue")
	}
}

func TestRefusedHelloBacksOffAndRetries(t *testing.T) {
	p := startTestPrimary(t, func() *Floors { return nil }, 0)
	p.refuse.Store(true)
	store := openTestStore(t, t.TempDir(), 0)
	startTestStandby(t, p, "s", store)
	time.Sleep(300 * time.Millisecond)
	if len(p.server.Sessions()) != 0 {
		t.Fatal("a refused hello opened a session")
	}
	p.refuse.Store(false)
	waitFor(t, "session after the refusal lifts", func() bool { return len(p.server.Sessions()) == 1 })
}

func TestCloseSessionsReconnects(t *testing.T) {
	p := startTestPrimary(t, func() *Floors { return nil }, 0)
	store := openTestStore(t, t.TempDir(), 0)
	startTestStandby(t, p, "s", store)
	waitFor(t, "session", func() bool { return len(p.server.Sessions()) == 1 })
	hook(p, "db/a", 1, "one")
	p.server.CloseSessions()
	waitFor(t, "session closed", func() bool { return len(p.server.Sessions()) == 0 })
	waitFor(t, "reconnected", func() bool { return len(p.server.Sessions()) == 1 })
	hook(p, "db/a", 3, "three") // 2 was produced while disconnected
	waitFor(t, "gap marked", func() bool { return len(keyGaps(store, "primary", p.instance, "db/a")) > 0 })
}

// A replayed frame (valid MAC, old sequence) and an entry for another
// instance both end the session before anything is stored.
func TestClientRejectsReplayedAndForeignFrames(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frames func(key []byte) [][]byte
		want   uint64 // highest cseq the store may hold
	}{
		{"replay", func(key []byte) [][]byte {
			one := rawEntryFrame(key, 1, &Entry{Instance: 0x77, Key: "db/a", CSeq: 1, Chunks: 1, Payload: []byte("a")})
			two := rawEntryFrame(key, 2, &Entry{Instance: 0x77, Key: "db/a", CSeq: 2, Chunks: 1, Payload: []byte("b")})
			return [][]byte{one, one, two}
		}, 1},
		{"foreign instance", func(key []byte) [][]byte {
			return [][]byte{rawEntryFrame(key, 1, &Entry{Instance: 0x99, Key: "db/a", CSeq: 1, Chunks: 1, Payload: []byte("a")})}
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			var accepts atomic.Int64
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					accepts.Add(1)
					msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
					if err != nil {
						conn.Close()
						continue
					}
					h := msg.Payload.(*protocol.OriginStreamHello)
					ts := time.Now().Unix()
					_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgOriginStreamHelloAck, Payload: &protocol.OriginStreamHelloAck{
						OriginNodeID: "primary", Instance: "77", Timestamp: ts,
						HMAC: security.ComputeOriginStreamHelloAckHMAC("s", h.Nonce, "primary", "77", "", ts)}}, 5*time.Second)
					key, _ := security.DeriveOriginStreamSessionKey("s", h.Nonce)
					for _, f := range tc.frames(key) {
						_, _ = conn.Write(f)
					}
					time.Sleep(200 * time.Millisecond)
					conn.Close()
				}
			}()
			store := openTestStore(t, t.TempDir(), 0)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			c := NewClient(ClientConfig{Store: store, LocalNodeID: "s1", ClusterName: "c", SharedSecret: "s",
				Target: func() (string, string, bool) { return "primary", ln.Addr().String(), true },
				Dial: func(ctx context.Context, addr string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "tcp", addr)
				},
				Ready: noneReady, Logger: zerolog.Nop()})
			go func() { c.Run(ctx); close(done) }()
			waitFor(t, "the first session", func() bool { return accepts.Load() >= 1 })
			time.Sleep(400 * time.Millisecond)
			cancel()
			<-done
			store.mu.Lock()
			defer store.mu.Unlock()
			for id, d := range store.instances {
				for key, k := range d.keys {
					if k.last > tc.want || (tc.want == 0 && k.last > 0) {
						t.Fatalf("%v %s holds up to %d, want at most %d", id, key, k.last, tc.want)
					}
				}
			}
		})
	}
}

func rawEntryFrame(key []byte, seq uint64, e *Entry) []byte {
	var buf bytes.Buffer
	_ = writeFrame(&buf, key, frameEntry, entryHead(seq, e), e.Payload)
	return buf.Bytes()
}

func allReady(_ context.Context, keys []TrimKey) map[TrimKey]bool {
	out := map[TrimKey]bool{}
	for _, k := range keys {
		out[k] = true
	}
	return out
}

func noneReady(context.Context, []TrimKey) map[TrimKey]bool { return map[TrimKey]bool{} }

type blockingCloseConn struct {
	net.Conn
	release chan struct{}
}

func (c *blockingCloseConn) Close() error { <-c.release; return c.Conn.Close() }

// Closing a session must not hold the server lock: Hook takes it with an
// ingest shard lock held, and a TLS close can block on a stalled peer.
func TestCloseDoesNotBlockTheHook(t *testing.T) {
	srv := NewServer(0, func() *Floors { return nil }, zerolog.Nop())
	srv.Start()
	a, b := net.Pipe()
	defer b.Close()
	conn := &blockingCloseConn{Conn: a, release: make(chan struct{})}
	srv.Accept("s", conn, testKey)
	go srv.CloseSessions()
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		srv.Hook(&wal.ReplicationEntry{Payload: []byte("x"), Coverage: &wal.Coverage{Instance: 1, Key: "db/a", CSeq: 1, Chunks: 1}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Hook blocked behind a connection close")
	}
	close(conn.release)
	srv.Stop()
}

// A batch larger than the queue budget still goes out when the queue is empty.
func TestOversizedBatchStillStreams(t *testing.T) {
	p := startTestPrimary(t, func() *Floors { return nil }, 1<<10)
	store := openTestStore(t, t.TempDir(), 0)
	startTestStandby(t, p, "s", store)
	waitFor(t, "session", func() bool { return len(p.server.Sessions()) == 1 })
	hook(p, "db/a", 1, string(bytes.Repeat([]byte("y"), 64<<10)))
	waitFor(t, "the oversized batch", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		d := store.instances[instanceID{"primary", p.instance}]
		return d != nil && d.keys["db/a"] != nil && d.keys["db/a"].last == 1
	})
}

// Reopening a trimmed store does not invent gaps over what was trimmed.
func TestReopenAfterTrimInventsNoGaps(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	s.segBytes = 1
	for c := uint64(1); c <= 6; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AcceptFloors("p", &Floors{Instance: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 2, Settled: [][2]uint64{{4, 5}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Trim(context.Background(), allReady); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	gapsBefore := metrics.Get().Snapshot()["standby_stream_gaps"].(int64)
	r := openTestStore(t, dir, 0)
	if g := keyGaps(r, "p", 1, "db/a"); len(g) != 0 {
		t.Fatalf("reopened store reports gaps %v over trimmed batches", g)
	}
	if after := metrics.Get().Snapshot()["standby_stream_gaps"].(int64); after != gapsBefore {
		t.Fatalf("reopen counted %d new gaps", after-gapsBefore)
	}
	if st := r.Stats(); st.Segments != 2 || st.IncompleteKeys != 0 {
		t.Fatalf("stats = %+v, want 2 segments (3 and 6) and no incomplete key", st)
	}
}

// At the cap, a replaced instance of the same origin is evicted before the
// current one stops being stored.
func TestCapEvictsAReplacedInstanceFirst(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 400)
	for c := uint64(1); c <= 8; c++ {
		_ = s.Append("p", ent(1, "db/a", c))
	}
	before := s.Stats().Bytes
	if before == 0 {
		t.Fatal("nothing stored")
	}
	s.SetCurrent("p", 2) // the primary restarted
	for c := uint64(1); c <= 8; c++ {
		if err := s.Append("p", ent(2, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	if g := keyGaps(s, "p", 2, "db/a"); len(g) != 0 {
		t.Fatalf("the current instance lost batches %v while a replaced one held the space", g)
	}
	s.mu.Lock()
	old := len(s.instances[instanceID{"p", 1}].segs)
	s.mu.Unlock()
	if old != 0 {
		t.Fatalf("the replaced instance still holds %d segments", old)
	}
}

// A standby restarts with everything trimmed while its primary keeps
// running: the next batch above the floor is not a gap.
func TestRestartWithEverythingTrimmedContinuesAtTheFloor(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	if err := s.AcceptFloors("p", &Floors{Instance: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 10}}}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	r := openTestStore(t, dir, 0)
	if err := r.Append("p", ent(1, "db/a", 11)); err != nil {
		t.Fatal(err)
	}
	if g := keyGaps(r, "p", 1, "db/a"); len(g) != 0 {
		t.Fatalf("gaps %v after a restart at the floor", g)
	}
}
