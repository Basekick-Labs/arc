package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1214: on a shared object store the orphan-storage sweep runs on the active
// compactor, which has no registration outbox. A writer's flushed file is an
// unregistered object until its registration commits; the sweep learns what
// is pending by asking every node that can ingest, and from the pending lists
// writers publish to the store.

const pendingHour = "testdb/cpu/2026/10/14/03/"

// pendingResponder serves a coordinator for node id, on the rig's Raft, at a
// real TCP address the rig's registry resolves id to. outbox may be nil.
func pendingResponder(t *testing.T, rig *Coordinator, id string, role NodeRole, outbox *RegistrationOutbox, secret string) *Coordinator {
	t.Helper()
	local := NewNode(id, id, role, "test-cluster")
	c := &Coordinator{
		cfg:        &config.ClusterConfig{SharedSecret: secret, ClusterName: "test-cluster", NodeID: id},
		localNode:  local,
		raftNode:   rig.raftNode,
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
		logger:     zerolog.Nop(),
	}
	if outbox != nil {
		c.SetPendingRegistrations(outbox.PendingPaths)
	}
	serveListener(t, rig, id, func(conn net.Conn) { c.handlePeerConnection(conn) })
	return c
}

// serveListener accepts on a fresh TCP port, handing each connection to
// handle, and registers id at that address in the rig's registry.
func serveListener(t *testing.T, rig *Coordinator, id string, handle func(net.Conn)) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(conn)
		}
	}()
	n := NewNode(id, id, RoleWriter, "test-cluster")
	n.SetAddresses(ln.Addr().String(), "")
	if err := rig.registry.Register(n); err != nil {
		t.Fatal(err)
	}
}

// oldNode serves id as a node before #1214 does: it reads the query, cannot
// decode the type, and closes the connection.
func oldNode(t *testing.T, rig *Coordinator, id string) {
	serveListener(t, rig, id, func(conn net.Conn) {
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 5)
		_, _ = io.ReadFull(conn, buf)
	})
}

// age sets the flush time of a pending entry.
func age(r *RegistrationOutbox, path string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byPath[path].rec.FlushedAt = at
}

// Every pending, parked and recently confirmed file, whenever it was flushed:
// the writer's clock is not the store's, and an age filter on its side could
// leave out a sweep candidate.
func TestPendingRegistrationsQueryRoundTrip(t *testing.T) {
	rig := originRig(t, "reader-1", RoleReader)
	outbox := newTestOutbox(t, t.TempDir())
	register(outbox, pendingHour+"a.parquet", 10)
	register(outbox, pendingHour+"b.parquet", 10)
	age(outbox, pendingHour+"a.parquet", time.Now().Add(-2*time.Hour))
	age(outbox, pendingHour+"b.parquet", time.Now().Add(time.Hour)) // a writer clock running ahead
	outbox.mu.Lock()
	outbox.parked[pendingHour+"parked.parquet"] = &parkedEntry{rec: parkedRecord{Rec: journalRecord{Path: pendingHour + "parked.parquet"}}}
	outbox.recent[pendingHour+"recent.parquet"] = recentConfirm{rec: journalRecord{Path: pendingHour + "recent.parquet"}, at: time.Now()}
	outbox.recent[pendingHour+"stale.parquet"] = recentConfirm{rec: journalRecord{Path: pendingHour + "stale.parquet"}, at: time.Now().Add(-time.Hour)}
	outbox.mu.Unlock()
	pendingResponder(t, rig, "writer-1", RoleWriter, outbox, osSecret)

	paths, err := rig.queryPendingRegistrations(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{pendingHour + "a.parquet", pendingHour + "b.parquet", pendingHour + "parked.parquet", pendingHour + "recent.parquet"}
	if strings.Join(paths, ",") != strings.Join(want, ",") {
		t.Fatalf("paths %v, want %v", paths, want)
	}
}

func TestPendingRegistrationsQueryAuthentication(t *testing.T) {
	rig := originRig(t, "reader-1", RoleReader)
	outbox := newTestOutbox(t, t.TempDir())
	register(outbox, pendingHour+"a.parquet", 10)
	pendingResponder(t, rig, "writer-1", RoleWriter, outbox, "another-secret")
	_, err := rig.queryPendingRegistrations(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"))
	if !errors.Is(err, errNoPendingAnswer) {
		t.Fatalf("a responder with another secret was trusted: %v", err)
	}

	// A querier that is not a node of the cluster is refused.
	stranger := originRig(t, "compactor-9", RoleCompactor)
	pendingResponder(t, stranger, "writer-1", RoleWriter, outbox, osSecret)
	_, err = stranger.queryPendingRegistrations(context.Background(), "writer-1", stranger.leaderCoordinatorAddress("writer-1"))
	if err == nil || !strings.Contains(err.Error(), protocol.PendingStatusRefused) {
		t.Fatalf("a querier outside the cluster was answered: %v", err)
	}
}

// The reply is bound to the query's nonce, covers its paths, and a query's
// nonce is accepted once.
func TestPendingRegistrationsReplyBindingAndReplay(t *testing.T) {
	rig := originRig(t, "reader-1", RoleReader)
	outbox := newTestOutbox(t, t.TempDir())
	register(outbox, pendingHour+"a.parquet", 10)
	responder := pendingResponder(t, rig, "writer-1", RoleWriter, outbox, osSecret)

	// A replayed query is refused.
	ask := func(nonce string, secret ...string) *protocol.PendingRegistrationsReply {
		return askAs(t, responder, "test-cluster", nonce, secret...)
	}
	if forged := ask("nonce-0", "another-secret"); forged.Status != protocol.PendingStatusRefused {
		t.Fatalf("a query signed with another secret was answered: %+v", forged)
	}
	first := ask("nonce-1")
	if first.Status != protocol.PendingStatusOK || len(first.Paths) != 1 {
		t.Fatalf("first query: %+v", first)
	}
	if again := ask("nonce-1"); again.Status != protocol.PendingStatusRefused {
		t.Fatalf("a replayed query was answered: %+v", again)
	}

	// A recorded reply is not accepted for another query's nonce, and one with
	// its paths stripped is not accepted at all.
	if err := security.ValidatePendingRegistrationsReplyHMAC(osSecret, "nonce-1", first.NodeID, "test-cluster", first.Status, first.Paths, first.Timestamp, first.HMAC, time.Minute); err != nil {
		t.Fatalf("the genuine reply does not validate: %v", err)
	}
	if security.ValidatePendingRegistrationsReplyHMAC(osSecret, "nonce-2", first.NodeID, "test-cluster", first.Status, first.Paths, first.Timestamp, first.HMAC, time.Minute) == nil {
		t.Fatal("a reply validated against another query's nonce")
	}
	if security.ValidatePendingRegistrationsReplyHMAC(osSecret, "nonce-1", first.NodeID, "test-cluster", first.Status, nil, first.Timestamp, first.HMAC, time.Minute) == nil {
		t.Fatal("a reply with its paths stripped validated")
	}

	// End to end: a man in the middle that replays the recorded reply to a
	// new query, or strips its paths, is no answer.
	for name, forge := range map[string]func(*protocol.PendingRegistrationsReply) *protocol.PendingRegistrationsReply{
		"replayed": func(*protocol.PendingRegistrationsReply) *protocol.PendingRegistrationsReply { return first },
		"stripped": func(r *protocol.PendingRegistrationsReply) *protocol.PendingRegistrationsReply {
			c := *r
			c.Paths = nil
			return &c
		},
	} {
		id := "writer-2"
		serveListener(t, rig, id, func(conn net.Conn) {
			defer conn.Close()
			msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
			if err != nil {
				return
			}
			q := msg.Payload.(*protocol.PendingRegistrationsQuery)
			ts := time.Now().Unix()
			genuine := &protocol.PendingRegistrationsReply{NodeID: id, Status: protocol.PendingStatusOK, Paths: []string{pendingHour + "x.parquet"}, Timestamp: ts,
				HMAC: security.ComputePendingRegistrationsReplyHMAC(osSecret, q.Nonce, id, "test-cluster", protocol.PendingStatusOK, []string{pendingHour + "x.parquet"}, ts)}
			reply := forge(genuine)
			if name == "replayed" {
				reply = &protocol.PendingRegistrationsReply{NodeID: id, Status: first.Status, Paths: first.Paths, Timestamp: first.Timestamp, HMAC: first.HMAC}
			}
			_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgPendingRegistrationsReply, Payload: reply}, 5*time.Second)
		})
		if _, err := rig.queryPendingRegistrations(context.Background(), id, rig.leaderCoordinatorAddress(id)); !errors.Is(err, errNoPendingAnswer) {
			t.Fatalf("%s reply accepted: %v", name, err)
		}
	}
}

// A node before #1214 closes the connection on the unknown message type:
// no answer.
func TestPendingRegistrationsOldNodeIsNoAnswer(t *testing.T) {
	rig := originRig(t, "reader-1", RoleReader)
	oldNode(t, rig, "writer-1")
	_, err := rig.queryPendingRegistrations(context.Background(), "writer-1", rig.leaderCoordinatorAddress("writer-1"))
	if !errors.Is(err, errNoPendingAnswer) {
		t.Fatalf("an old node's closed connection was an answer: %v", err)
	}
}

func TestPendingBeforeTooMany(t *testing.T) {
	outbox := newTestOutbox(t, t.TempDir())
	for _, n := range []string{"a", "b", "c"} {
		register(outbox, pendingHour+n+".parquet", 10)
	}
	if paths, tooMany := outbox.PendingPaths(2 * len(pendingHour+"a.parquet")); !tooMany || paths != nil {
		t.Fatalf("over the budget: %v %v", paths, tooMany)
	}
	if paths, tooMany := outbox.PendingPaths(pendingReplyBudget); tooMany || len(paths) != 3 {
		t.Fatalf("within the budget: %v %v", paths, tooMany)
	}
}

// PendingRegistrationsElsewhere asks every node of the cluster that can
// ingest; one that does not answer, or answers that it cannot tell, holds the
// sweep, named.
func TestPendingRegistrationsElsewhereHoldsOnANodeThatCannotTell(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, rig *Coordinator)
		why   string
	}{
		{"old build", func(t *testing.T, rig *Coordinator) { oldNode(t, rig, "writer-2") }, "writer-2"},
		{"down", func(t *testing.T, rig *Coordinator) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			_ = ln.Close()
			n := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
			n.SetAddresses(addr, "")
			_ = rig.registry.Register(n)
		}, "writer-2"},
		{"no journal", func(t *testing.T, rig *Coordinator) {
			pendingResponder(t, rig, "writer-2", RoleWriter, nil, osSecret)
		}, "without a registration journal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := originRig(t, "reader-1", RoleReader)
			outbox := newTestOutbox(t, t.TempDir())
			register(outbox, pendingHour+"a.parquet", 10)
			pendingResponder(t, rig, "writer-1", RoleWriter, outbox, osSecret)
			tc.setup(t, rig)
			paths, held := rig.PendingRegistrationsElsewhere(context.Background())
			if _, ok := paths[pendingHour+"a.parquet"]; !ok {
				t.Fatalf("writer-1's pending file missing: %v", paths)
			}
			if len(held) != 1 || !strings.Contains(held[0], tc.why) {
				t.Fatalf("held %v, want one naming %s", held, tc.why)
			}
		})
	}
}

// A writer publishes its pending set on Stop and Abandon; the drain publishes
// it while a file waits, rewrites it when the set changes, and deletes it once
// nothing is pending.
func TestOutboxPublishesItsPendingList(t *testing.T) {

	t.Run("stop", func(t *testing.T) {
		store := newPendingStore(t)
		r := newTestOutbox(t, t.TempDir())
		r.SetPendingPublisher(store, "writer-1")
		key := publishedKey(r)
		register(r, pendingHour+"a.parquet", 10)
		r.Stop()
		if got := readPublished(t, store, key); len(got) != 1 || got[0] != pendingHour+"a.parquet" {
			t.Fatalf("published %v", got)
		}
	})

	t.Run("abandon", func(t *testing.T) {
		store := newPendingStore(t)
		r := newTestOutbox(t, t.TempDir())
		r.SetPendingPublisher(store, "writer-1")
		key := publishedKey(r)
		register(r, pendingHour+"a.parquet", 10)
		if !r.Abandon() {
			t.Fatal("not abandoned")
		}
		if got := readPublished(t, store, key); len(got) != 1 {
			t.Fatalf("published %v", got)
		}
	})

	t.Run("drain", func(t *testing.T) {
		saveAge, saveEvery := outboxAlarmAge, outboxPublishEvery
		outboxAlarmAge, outboxPublishEvery = 0, 0
		t.Cleanup(func() { outboxAlarmAge, outboxPublishEvery = saveAge, saveEvery })

		store := newPendingStore(t)
		r := newTestOutbox(t, t.TempDir())
		r.SetPendingPublisher(store, "writer-1")
		key := publishedKey(r)
		m := newFakeOutboxManifest()
		// Registrations fail until released: the leader is cut off.
		m.batchErrs = make([]error, 1<<20)
		for i := range m.batchErrs {
			m.batchErrs[i] = ErrLeaderUnreachable
		}
		sizes := &fakeOutboxStorage{sizes: map[string]int64{pendingHour + "a.parquet": 10, pendingHour + "b.parquet": 10}}
		register(r, pendingHour+"a.parquet", 10)
		r.HandOff(m, sizes, "writer-1")
		waitOutbox(t, "published", func() bool { return len(readPublishedQuiet(store, key)) == 1 })
		register(r, pendingHour+"b.parquet", 10)
		waitOutbox(t, "rewritten", func() bool { return len(readPublishedQuiet(store, key)) == 2 })

		m.mu.Lock()
		m.batchErrs = nil
		m.mu.Unlock()
		waitOutbox(t, "deleted", func() bool {
			ok, _ := store.Exists(context.Background(), key)
			return !ok
		})
		if !m.isRegistered(pendingHour+"a.parquet") || !m.isRegistered(pendingHour+"b.parquet") {
			t.Fatal("not registered")
		}
	})
}

func newPendingStore(t *testing.T) *fakeObjectStore {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &fakeObjectStore{LocalBackend: b, meta: map[string]string{}, failDelete: map[string]error{}}
}

func readPublished(t *testing.T, store storage.Backend, key string) []string {
	t.Helper()
	data, err := store.Read(context.Background(), key)
	if err != nil {
		t.Fatalf("no published list: %v", err)
	}
	var doc PendingRegistrationsDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.NodeID != "writer-1" || doc.ClusterName != "test-cluster" {
		t.Fatalf("doc %+v", doc)
	}
	var out []string
	for _, f := range doc.Files {
		out = append(out, f.Path)
	}
	return out
}

func readPublishedQuiet(store storage.Backend, key string) []string {
	data, err := store.Read(context.Background(), key)
	if err != nil {
		return nil
	}
	var doc PendingRegistrationsDoc
	if json.Unmarshal(data, &doc) != nil {
		return nil
	}
	var out []string
	for _, f := range doc.Files {
		out = append(out, f.Path)
	}
	return out
}

// End to end through the production wiring on the compactor: the reconciler
// on a shared object store keeps a file a writer reports pending and one a
// departed writer published, deletes the real orphan, and deletes nothing
// while a writer cannot be asked.
func TestReconcilerKeepsFilesPendingElsewhere(t *testing.T) {
	for _, tc := range []struct {
		name   string
		writer func(t *testing.T, rig *Coordinator)
		held   bool
	}{
		{"every writer answers", func(t *testing.T, rig *Coordinator) {
			pendingResponder(t, rig, "writer-2", RoleWriter, newTestOutbox(t, t.TempDir()), osSecret)
		}, false},
		{"a writer does not answer", func(t *testing.T, rig *Coordinator) { oldNode(t, rig, "writer-2") }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := originRig(t, "reader-1", RoleReader) // the sweeping node; any non-writer
			store := newPendingStore(t)
			rig.storage = store

			pendingRPC := pendingHour + "cpu_rpc.parquet"
			pendingListed := pendingHour + "cpu_listed.parquet"
			orphan := pendingHour + "cpu_orphan.parquet"
			for _, p := range []string{pendingRPC, pendingListed, orphan} {
				if err := store.Write(context.Background(), p, []byte("x")); err != nil {
					t.Fatal(err)
				}
				old := time.Now().Add(-48 * time.Hour)
				if err := os.Chtimes(filepath.Join(store.GetBasePath(), p), old, old); err != nil {
					t.Fatal(err)
				}
			}

			// writer-1 is cut off from the leader and still holds pendingRPC.
			w1 := newTestOutbox(t, t.TempDir())
			register(w1, pendingRPC, 1)
			age(w1, pendingRPC, time.Now().Add(-48*time.Hour))
			pendingResponder(t, rig, "writer-1", RoleWriter, w1, osSecret)
			tc.writer(t, rig)

			// writer-9 left the cluster with pendingListed unregistered: it
			// published it on Stop and is no longer asked.
			gone := newTestOutbox(t, t.TempDir())
			gone.SetPendingPublisher(store, "writer-9")
			register(gone, pendingListed, 1)
			gone.Stop()
			goneKey := publishedKey(gone)

			rec, err := reconciliation.NewReconciler(reconciliation.Config{
				Enabled: true, BackendKind: reconciliation.BackendShared, DeletePreManifestOrphans: true,
				MaxRootWalkDatabases: -1, // the default root walk: the manifest lists nothing
			}, rig, store, nil, nil, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			rec.SetRemotePendingSource(func(ctx context.Context) reconciliation.RemotePending {
				paths, held := rig.PendingRegistrationsElsewhere(ctx)
				return reconciliation.RemotePending{Paths: paths, Held: held}
			})
			run, err := rec.Reconcile(context.Background(), false)
			if err != nil {
				t.Fatal(err)
			}
			exists := func(p string) bool {
				ok, _ := store.Exists(context.Background(), p)
				return ok
			}
			if !exists(pendingRPC) || !exists(pendingListed) {
				t.Fatalf("a pending file was deleted: rpc %v listed %v (run %+v)", exists(pendingRPC), exists(pendingListed), run)
			}
			if !exists(goneKey) {
				t.Fatal("the published list itself was swept")
			}
			if run.StorageFileCount != 3 {
				t.Fatalf("storage file count %d: the walk saw something besides the three data files", run.StorageFileCount)
			}
			if tc.held {
				if !run.StorageSweepHeld || !strings.Contains(run.StorageSweepHeldReason, "writer-2") || !exists(orphan) || run.Aborted {
					t.Fatalf("not held on the silent writer: %+v", run)
				}
				return
			}
			if run.StorageSweepHeld || exists(orphan) || run.SkippedPendingRemote != 2 {
				t.Fatalf("held %v (%s), orphan kept %v, skipped %d", run.StorageSweepHeld, run.StorageSweepHeldReason, exists(orphan), run.SkippedPendingRemote)
			}
		})
	}
}

func publishedKey(r *RegistrationOutbox) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.publisher.key
}

// askAs runs one query against responder, signed with secret (default
// osSecret), from reader-1 of cluster.
func askAs(t *testing.T, responder *Coordinator, cluster, nonce string, secret ...string) *protocol.PendingRegistrationsReply {
	t.Helper()
	key := osSecret
	if len(secret) > 0 {
		key = secret[0]
	}
	server, client := net.Pipe()
	defer client.Close()
	ts := time.Now().Unix()
	q := &protocol.PendingRegistrationsQuery{NodeID: "reader-1", ClusterName: cluster, Nonce: nonce, Timestamp: ts,
		HMAC: security.ComputePendingRegistrationsQueryHMAC(key, nonce, "reader-1", cluster, ts)}
	go func() {
		responder.handlePendingRegistrationsQuery(server, q)
		server.Close()
	}()
	msg, err := protocol.ReceiveMessage(client, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return msg.Payload.(*protocol.PendingRegistrationsReply)
}
