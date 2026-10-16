package cluster

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1265: in shared storage a DELETE asks every other node to flush its
// buffered rows of the scope and waits for what they flushed to register.

// fakeScopeFlusher records the scopes it was asked to flush.
type fakeScopeFlusher struct {
	mu      sync.Mutex
	scopes  []string
	failed  bool
	err     error
	onFlush func()
}

func (f *fakeScopeFlusher) FlushScopeAndWait(_ context.Context, database, measurement string) (map[string]uint64, error) {
	f.mu.Lock()
	f.scopes = append(f.scopes, database+"/"+measurement)
	on := f.onFlush
	f.mu.Unlock()
	if on != nil {
		on()
	}
	return nil, f.err
}

func (f *fakeScopeFlusher) HasFlushFailure() bool { return f.failed }

func (f *fakeScopeFlusher) asked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.scopes...)
}

// flushRig is a shared-storage asker that is the Raft leader ("raft-1").
func flushRig(t *testing.T) *Coordinator {
	t.Helper()
	rig := originRig(t, "raft-1", RoleWriter)
	rig.cfg.SharedStorageMode = true
	rig.raftFSM = rig.raftNode.FSM()
	return rig
}

// flushResponder serves node id with the flusher and outbox (either may be
// nil) at an address the rig resolves.
func flushResponder(t *testing.T, rig *Coordinator, id string, f DeleteScopeFlusher, outbox *RegistrationOutbox) *Coordinator {
	t.Helper()
	c := pendingResponder(t, rig, id, RoleWriter, outbox, osSecret)
	if f != nil {
		c.SetDeleteScopeFlusher(f)
	}
	return c
}

const flushHour = "db/cpu/2026/10/11/10/"

func TestFlushScopeRoundTrip(t *testing.T) {
	rig := flushRig(t)
	outbox := newTestOutbox(t, t.TempDir())
	f := &fakeScopeFlusher{}
	// Registered by the flush, as the buffer's flush journals its file
	// before FlushScopeAndWait returns.
	f.onFlush = func() { register(outbox, flushHour+"cpu_a.parquet", 10) }
	outbox.RegisterFile("db", "cpu2", "db/cpu2/2026/10/11/10/cpu2_a.parquet", time.Now(), 10, "abc")
	outbox.RegisterFile("other", "cpu", "other/cpu/2026/10/11/10/cpu_a.parquet", time.Now(), 10, "abc")
	flushResponder(t, rig, "writer-1", f, outbox)

	paths, err := rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(paths, ",") != flushHour+"cpu_a.parquet" {
		t.Fatalf("paths %v, want only the file of db/cpu (not db/cpu2, not another database)", paths)
	}
	if got := f.asked(); len(got) != 1 || got[0] != "db/cpu" {
		t.Fatalf("flushed %v, want db/cpu", got)
	}

	// The whole database.
	paths, err = rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("paths %v, want db/cpu and db/cpu2", paths)
	}
}

func TestFlushScopeStatuses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flusher *fakeScopeFlusher
		outbox  bool
		wantErr string
	}{
		{"flush failure outstanding", &fakeScopeFlusher{failed: true}, true, "flush failed"},
		{"flush error", &fakeScopeFlusher{err: errors.New("disk full")}, true, "flush failed"},
		{"no journal", &fakeScopeFlusher{}, false, ""},
		{"a writer without its buffer", nil, false, "flush failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := flushRig(t)
			var outbox *RegistrationOutbox
			if tc.outbox {
				outbox = newTestOutbox(t, t.TempDir())
			}
			var f DeleteScopeFlusher
			if tc.flusher != nil {
				f = tc.flusher
			}
			flushResponder(t, rig, "writer-1", f, outbox)
			paths, err := rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "cpu")
			if tc.wantErr == "" {
				if err != nil || len(paths) != 0 {
					t.Fatalf("paths %v err %v, want none", paths, err)
				}
				return
			}
			if !errors.Is(err, errNoFlushAnswer) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// askFlush sends a request to responder over a pipe, as node from, and
// returns the reply.
func askFlush(t *testing.T, responder *Coordinator, from, secret, nonce, database, macDatabase string) *protocol.FlushScopeReply {
	t.Helper()
	server, client := net.Pipe()
	defer client.Close()
	ts := time.Now().Unix()
	q := &protocol.FlushScopeRequest{NodeID: from, ClusterName: "test-cluster", Database: database, Measurement: "cpu", Nonce: nonce, Timestamp: ts,
		HMAC: security.ComputeFlushScopeRequestHMAC(secret, nonce, from, "test-cluster", macDatabase, "cpu", ts)}
	go func() {
		responder.handleFlushScopeRequest(server, q)
		server.Close()
	}()
	msg, err := protocol.ReceiveMessage(client, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return msg.Payload.(*protocol.FlushScopeReply)
}

// Only the Raft leader may ask, with the secret, once per nonce, for the
// scope the MAC covers; a refused request flushes nothing.
func TestFlushScopeAuthentication(t *testing.T) {
	rig := flushRig(t)
	f := &fakeScopeFlusher{}
	responder := flushResponder(t, rig, "writer-1", f, newTestOutbox(t, t.TempDir()))

	if r := askFlush(t, responder, "raft-1", osSecret, "n1", "db", "db"); r.Status != protocol.FlushStatusOK {
		t.Fatalf("the leader was refused: %s", r.Status)
	}
	for _, tc := range []struct {
		name, from, secret, nonce, db, macDB string
	}{
		{"not the leader", "writer-2", osSecret, "n2", "db", "db"},
		{"wrong secret", "raft-1", "another-secret", "n3", "db", "db"},
		{"replayed nonce", "raft-1", osSecret, "n1", "db", "db"},
		{"database not covered by the MAC", "raft-1", osSecret, "n4", "db", "other"},
		{"path in the database", "raft-1", osSecret, "n5", "../db", "../db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if r := askFlush(t, responder, tc.from, tc.secret, tc.nonce, tc.db, tc.macDB); r.Status != protocol.FlushStatusRefused {
				t.Fatalf("status %s, want refused", r.Status)
			}
		})
	}
	if got := f.asked(); len(got) != 1 {
		t.Fatalf("flushed %v, want only the leader's request", got)
	}
}

// The reply is bound to the request's scope: one for another database is
// not accepted.
func TestFlushScopeReplyBoundToScope(t *testing.T) {
	rig := flushRig(t)
	serveListener(t, rig, "writer-1", func(conn net.Conn) {
		defer conn.Close()
		msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
		if err != nil {
			return
		}
		q := msg.Payload.(*protocol.FlushScopeRequest)
		ts := time.Now().Unix()
		reply := &protocol.FlushScopeReply{NodeID: "writer-1", Status: protocol.FlushStatusOK, Timestamp: ts,
			HMAC: security.ComputeFlushScopeReplyHMAC(osSecret, q.Nonce, "writer-1", "test-cluster", "otherdb", q.Measurement, protocol.FlushStatusOK, nil, ts)}
		_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgFlushScopeReply, Payload: reply}, 5*time.Second)
	})
	_, err := rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "cpu")
	if !errors.Is(err, errNoFlushAnswer) {
		t.Fatalf("a reply for another scope was trusted: %v", err)
	}
}

// Every other node is asked, readers too; one that does not answer is named,
// and the files the others flushed are waited for until they register.
func TestFlushPeersForDelete(t *testing.T) {
	rig := flushRig(t)
	outbox := newTestOutbox(t, t.TempDir())
	w1 := &fakeScopeFlusher{}
	w1.onFlush = func() { register(outbox, flushHour+"cpu_a.parquet", 10) }
	flushResponder(t, rig, "writer-1", w1, outbox)
	oldNode(t, rig, "writer-2")
	r1 := &fakeScopeFlusher{}
	flushResponder(t, rig, "reader-1", r1, nil)

	go func() {
		// writer-1's registration lands while the DELETE waits.
		time.Sleep(300 * time.Millisecond)
		e := makeFileEntry(flushHour+"cpu_a.parquet", []byte("a"), "writer-1")
		e.CreatedAt = time.Now()
		_ = rig.raftNode.RegisterFile(e, 5*time.Second)
	}()
	start := time.Now()
	unflushed, err := rig.FlushPeersForDelete(context.Background(), "db", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 300*time.Millisecond {
		t.Fatal("returned before writer-1's flushed file registered")
	}
	if len(unflushed) != 1 || !strings.Contains(unflushed[0], "writer-2") || !strings.Contains(unflushed[0], "allow_unreachable") {
		t.Fatalf("unflushed %v, want writer-2 named with the remedy", unflushed)
	}
	if len(w1.asked()) != 1 || len(r1.asked()) != 1 {
		t.Fatalf("writer-1 asked %v, reader-1 asked %v; want both once", w1.asked(), r1.asked())
	}
}

// A flushed file that does not register in time names its node.
func TestFlushPeersForDeleteLateRegistration(t *testing.T) {
	old := peerRegisterWait
	peerRegisterWait = 300 * time.Millisecond
	t.Cleanup(func() { peerRegisterWait = old })
	rig := flushRig(t)
	outbox := newTestOutbox(t, t.TempDir())
	w1 := &fakeScopeFlusher{}
	w1.onFlush = func() { register(outbox, flushHour+"cpu_a.parquet", 10) }
	flushResponder(t, rig, "writer-1", w1, outbox)
	flushResponder(t, rig, "writer-2", &fakeScopeFlusher{}, nil)
	flushResponder(t, rig, "reader-1", &fakeScopeFlusher{}, nil)

	unflushed, err := rig.FlushPeersForDelete(context.Background(), "db", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if len(unflushed) != 1 || !strings.Contains(unflushed[0], "writer-1") || !strings.Contains(unflushed[0], "cpu_a.parquet") || !strings.Contains(unflushed[0], "outbox") {
		t.Fatalf("unflushed %v, want writer-1 and its file", unflushed)
	}
}

// Outside shared storage nothing is asked.
func TestFlushPeersForDeleteOnlyInSharedStorage(t *testing.T) {
	rig := flushRig(t)
	rig.cfg.SharedStorageMode = false
	w1 := &fakeScopeFlusher{}
	flushResponder(t, rig, "writer-1", w1, nil)
	unflushed, err := rig.FlushPeersForDelete(context.Background(), "db", "cpu")
	if err != nil || len(unflushed) != 0 || len(w1.asked()) != 0 {
		t.Fatalf("unflushed %v err %v asked %v; want nothing asked", unflushed, err, w1.asked())
	}
}

func TestValidFlushScope(t *testing.T) {
	for _, tc := range []struct {
		db, m string
		ok    bool
	}{
		{"db", "cpu", true}, {"db", "", true}, {"a..b", "c", true},
		{"", "cpu", false}, {"..", "", false}, {"db", "..", false}, {"a/b", "", false}, {"db", "c/d", false}, {"db\x00", "", false},
	} {
		if got := validFlushScope(tc.db, tc.m); got != tc.ok {
			t.Errorf("validFlushScope(%q, %q) = %v, want %v", tc.db, tc.m, got, tc.ok)
		}
	}
}

// A reply from another node than the one asked is not trusted, even with a
// valid MAC.
func TestFlushScopeReplyFromAnotherNode(t *testing.T) {
	rig := flushRig(t)
	serveListener(t, rig, "writer-1", func(conn net.Conn) {
		defer conn.Close()
		msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
		if err != nil {
			return
		}
		q := msg.Payload.(*protocol.FlushScopeRequest)
		ts := time.Now().Unix()
		reply := &protocol.FlushScopeReply{NodeID: "writer-9", Status: protocol.FlushStatusOK, Timestamp: ts,
			HMAC: security.ComputeFlushScopeReplyHMAC(osSecret, q.Nonce, "writer-9", "test-cluster", q.Database, q.Measurement, protocol.FlushStatusOK, nil, ts)}
		_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgFlushScopeReply, Payload: reply}, 5*time.Second)
	})
	_, err := rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "cpu")
	if !errors.Is(err, errNoFlushAnswer) || !strings.Contains(err.Error(), "writer-9") {
		t.Fatalf("a reply as another node was trusted: %v", err)
	}
}

// A flushed file that is gone from storage (its writer withdrew it, or
// settled it as lost) will never register: not waited for.
func TestFlushPeersForDeleteDoesNotWaitForAGoneFile(t *testing.T) {
	rig := flushRig(t)
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	rig.storage = store
	outbox := newTestOutbox(t, t.TempDir())
	w1 := &fakeScopeFlusher{}
	w1.onFlush = func() { register(outbox, flushHour+"cpu_gone.parquet", 10) }
	flushResponder(t, rig, "writer-1", w1, outbox)
	flushResponder(t, rig, "writer-2", &fakeScopeFlusher{}, nil)
	flushResponder(t, rig, "reader-1", &fakeScopeFlusher{}, nil)
	start := time.Now()
	unflushed, err := rig.FlushPeersForDelete(context.Background(), "db", "cpu")
	if err != nil || len(unflushed) != 0 {
		t.Fatalf("unflushed %v err %v", unflushed, err)
	}
	if time.Since(start) > peerRegisterWait/2 {
		t.Fatalf("waited %s for a file that is gone", time.Since(start))
	}
}

// The node running the DELETE is not asked: it flushed itself already, and
// asking would dial its own coordinator.
func TestFlushPeersForDeleteSkipsItself(t *testing.T) {
	rig := flushRig(t)
	rig.localNode = NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	flushResponder(t, rig, "writer-2", &fakeScopeFlusher{}, nil)
	flushResponder(t, rig, "reader-1", &fakeScopeFlusher{}, nil)
	unflushed, err := rig.FlushPeersForDelete(context.Background(), "db", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range unflushed {
		if strings.Contains(line, "writer-1") {
			t.Fatalf("the node asked itself: %v", unflushed)
		}
	}
}

// A node that cannot ingest and has no buffer has nothing to flush.
func TestFlushScopeNodeWithoutIngest(t *testing.T) {
	rig := flushRig(t)
	pendingResponder(t, rig, "reader-1", RoleReader, nil, osSecret)
	paths, err := rig.requestPeerFlush(context.Background(), "reader-1", rig.leaderCoordinatorAddress("reader-1"), "db", "cpu")
	if err != nil || len(paths) != 0 {
		t.Fatalf("paths %v err %v", paths, err)
	}
}

// The reply budget is for the scope's files: many files pending in another
// database do not make the answer too big, as many of the scope do.
func TestFlushScopeReplyBudgetIsTheScopes(t *testing.T) {
	old := flushReplyBudget
	flushReplyBudget = 500
	t.Cleanup(func() { flushReplyBudget = old })
	rig := flushRig(t)
	outbox := newTestOutbox(t, t.TempDir())
	for i := 0; i < 20; i++ {
		outbox.RegisterFile("other", "cpu", "other/cpu/2026/10/11/10/cpu_"+strings.Repeat("x", i+1)+".parquet", time.Now(), 10, "abc")
	}
	flushResponder(t, rig, "writer-1", &fakeScopeFlusher{}, outbox)
	if _, err := rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "cpu"); err != nil {
		t.Fatalf("other databases' files counted against the scope: %v", err)
	}
	for i := 0; i < 20; i++ {
		register(outbox, flushHour+"cpu_"+strings.Repeat("y", i+1)+".parquet", 10)
	}
	_, err := rig.requestPeerFlush(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"), "db", "cpu")
	if !errors.Is(err, errNoFlushAnswer) || !strings.Contains(err.Error(), "more files") {
		t.Fatalf("err %v, want too many", err)
	}
}

// End to end with a real ingest buffer and registration outbox on the other
// writer: rows it acknowledged and still buffers (a buffer age of ten
// minutes) are in a registered file when the call returns. Without the call
// they are in no file.
func TestFlushPeersForDeleteFlushesARealBuffer(t *testing.T) {
	rig := flushRig(t)
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	rig.storage = backend
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 8}, backend, zerolog.Nop())
	t.Cleanup(func() { _ = buffer.Close() })
	outbox := newTestOutbox(t, t.TempDir())
	buffer.SetFileRegistrar(outbox)
	outbox.HandOff(rigManifest{rig}, backend, "writer-1")
	flushResponder(t, rig, "writer-1", buffer, outbox)
	flushResponder(t, rig, "writer-2", &fakeScopeFlusher{}, nil)
	flushResponder(t, rig, "reader-1", &fakeScopeFlusher{}, nil)

	ts := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC).UnixMicro()
	if err := buffer.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{"time": {ts, ts + 1}, "value": {1.0, 2.0}}); err != nil {
		t.Fatal(err)
	}
	if files, _ := backend.List(context.Background(), "db/cpu/"); len(files) != 0 {
		t.Fatalf("precondition: rows already in a file: %v", files)
	}
	unflushed, err := rig.FlushPeersForDelete(context.Background(), "db", "cpu")
	if err != nil || len(unflushed) != 0 {
		t.Fatalf("unflushed %v err %v", unflushed, err)
	}
	files, err := backend.List(context.Background(), "db/cpu/")
	if err != nil || len(files) != 1 {
		t.Fatalf("files %v err %v, want the flushed one", files, err)
	}
	if _, ok := rig.GetFileEntry(files[0]); !ok {
		t.Fatalf("%s not registered when the call returned", files[0])
	}
}
