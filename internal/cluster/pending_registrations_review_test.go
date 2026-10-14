package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// #1214 review: what a writer that leaves, restarts, or publishes nothing
// leaves the orphan sweep, and what the sweep trusts in a reply or a list.

// flakyStore fails its first fails writes, every write when fails < 0, and
// counts reads.
type flakyStore struct {
	*fakeObjectStore
	fails  atomic.Int64
	reads  atomic.Int64
	readOK bool
}

func (f *flakyStore) Write(ctx context.Context, path string, data []byte) error {
	if n := f.fails.Load(); n != 0 {
		if n > 0 {
			f.fails.Add(-1)
		}
		return errors.New("write refused")
	}
	return f.fakeObjectStore.Write(ctx, path, data)
}

func (f *flakyStore) Read(ctx context.Context, path string) ([]byte, error) {
	f.reads.Add(1)
	if !f.readOK {
		return nil, errors.New("access denied")
	}
	return f.fakeObjectStore.Read(ctx, path)
}

func shortPublishRetry(t *testing.T) {
	save := outboxPublishRetry
	outboxPublishRetry = time.Millisecond
	t.Cleanup(func() { outboxPublishRetry = save })
}

// Stop retries a failed publish; a writer whose pending set is published may
// leave.
func TestOutboxStopRetriesThePublish(t *testing.T) {
	shortPublishRetry(t)
	store := &flakyStore{fakeObjectStore: newPendingStore(t), readOK: true}
	store.fails.Store(stopPublishAttempts - 1)
	r := newTestOutbox(t, t.TempDir())
	r.SetPendingPublisher(store, "writer-1")
	register(r, pendingHour+"a.parquet", 10)
	r.Stop()
	if got := readPublishedQuiet(store, publishedKey(r)); len(got) != 1 {
		t.Fatalf("published %v after %d failed writes", got, stopPublishAttempts-1)
	}
	if !r.CoverForLeave() {
		t.Fatal("a published pending set does not cover the leave")
	}
}

// A writer whose pending set could not be published does not leave the
// cluster: a node that left is no longer asked, and nothing else names its
// files. One that published leaves as before.
func TestCoordinatorLeavesOnlyWithItsPendingSetPublished(t *testing.T) {
	shortPublishRetry(t)
	for _, tc := range []struct {
		name    string
		fails   int64
		pending bool
		leaves  bool
	}{
		{"published", 0, true, true},
		{"nothing pending", -1, false, true},
		{"publish failed", -1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &flakyStore{fakeObjectStore: newPendingStore(t), readOK: true}
			store.fails.Store(tc.fails)
			r := newTestOutbox(t, t.TempDir())
			r.SetPendingPublisher(store, "local-1")
			if tc.pending {
				register(r, pendingHour+"a.parquet", 10)
			}
			r.Stop()

			seed, _ := countingSeed(t)
			c := discoveryTestCoordinator(t, seed)
			c.stopCh = make(chan struct{})
			c.running = true
			c.SetLeaveGate(r.CoverForLeave)
			var broadcasts atomic.Int64
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					broadcasts.Add(1)
					_ = conn.Close()
				}
			}()
			peer := NewNode("peer-1", "peer-1", RoleReader, "test-cluster")
			peer.State = StateHealthy
			peer.SetAddresses(ln.Addr().String(), "127.0.0.1:1")
			if err := c.registry.Register(peer); err != nil {
				t.Fatal(err)
			}
			_ = c.Stop()
			// The listener counts on its own goroutine, which may run after
			// Stop returns: wait for a leave that is expected, and give one
			// that is not the same time to show up.
			deadline := time.Now().Add(2 * time.Second)
			for broadcasts.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if got := broadcasts.Load() > 0; got != tc.leaves {
				t.Fatalf("left the cluster: %v, want %v", got, tc.leaves)
			}
		})
	}
}

// Without a publisher ID nothing is published, so a pending set is not
// covered; a publish that failed at Stop is tried again at the leave.
func TestOutboxCoverForLeave(t *testing.T) {
	shortPublishRetry(t)
	t.Run("no publisher id", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, PublisherIDFile), 0o700); err != nil {
			t.Fatal(err)
		}
		r := newTestOutbox(t, dir)
		r.SetPendingPublisher(newPendingStore(t), "writer-1")
		if !r.CoverForLeave() {
			t.Fatal("nothing pending is covered")
		}
		register(r, pendingHour+"a.parquet", 10)
		r.Stop()
		if r.CoverForLeave() {
			t.Fatal("an unpublished pending set covered the leave")
		}
	})
	t.Run("recovers at the leave", func(t *testing.T) {
		store := &flakyStore{fakeObjectStore: newPendingStore(t), readOK: true}
		store.fails.Store(-1)
		r := newTestOutbox(t, t.TempDir())
		r.SetPendingPublisher(store, "writer-1")
		register(r, pendingHour+"a.parquet", 10)
		r.Stop()
		store.fails.Store(0)
		if !r.CoverForLeave() {
			t.Fatal("the leave did not publish once more")
		}
		if got := readPublishedQuiet(store, publishedKey(r)); len(got) != 1 {
			t.Fatalf("published %v", got)
		}
	})
}

// A file flushed after Stop (the final flush racing it) is journaled for the
// next start and published too, before the node leaves.
func TestOutboxPublishesAFileJournaledAfterStop(t *testing.T) {
	store := newPendingStore(t)
	r := newTestOutbox(t, t.TempDir())
	r.SetPendingPublisher(store, "writer-1")
	register(r, pendingHour+"a.parquet", 10)
	r.Stop()
	register(r, pendingHour+"late.parquet", 10)
	if got := readPublishedQuiet(store, publishedKey(r)); len(got) != 2 {
		t.Fatalf("published %v, want the file journaled after Stop too", got)
	}
	if !r.CoverForLeave() {
		t.Fatal("not covered")
	}
}

// The list is keyed by a publisher ID kept next to the journal, not by the
// node ID (hostname-PID on a VM): a restarted writer under a new node ID
// overwrites, and finally deletes, the list its previous start left.
func TestOutboxPublisherIDSurvivesARestart(t *testing.T) {
	store := newPendingStore(t)
	dir := t.TempDir()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	first.SetPendingPublisher(store, "host-111")
	register(first, pendingHour+"a.parquet", 10)
	first.Stop()
	key := publishedKey(first)
	crash(first)

	second := newTestOutbox(t, dir)
	second.SetPendingPublisher(store, "host-222")
	if publishedKey(second) != key {
		t.Fatalf("keys %s and %s: a restart publishes under a new key", key, publishedKey(second))
	}
	data, err := store.Read(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	var doc PendingRegistrationsDoc
	if err := json.Unmarshal(data, &doc); err != nil || doc.NodeID != "host-111" {
		t.Fatalf("doc %+v %v", doc, err)
	}
	m := newFakeOutboxManifest()
	second.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{pendingHour + "a.parquet": 10}}, "host-222")
	waitOutbox(t, "previous list deleted", func() bool {
		ok, _ := store.Exists(context.Background(), key)
		return !ok
	})
	if keys, _ := store.List(context.Background(), PendingRegistrationsRoot+"/"); len(keys) != 0 {
		t.Fatalf("lists left: %v", keys)
	}
}

// A list the sweep cannot read, parse, or trust holds it, named; the files of
// the lists it can read are still returned.
func TestReadPublishedPendingHoldsWhatItCannotTrust(t *testing.T) {
	ctx := context.Background()
	write := func(t *testing.T, store *flakyStore, key string, doc any) {
		t.Helper()
		var data []byte
		if raw, ok := doc.([]byte); ok {
			data = raw
		} else {
			data, _ = json.Marshal(doc)
		}
		if err := store.fakeObjectStore.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	good := PendingRegistrationsDoc{ClusterName: "test-cluster", Files: []PendingRegistrationFile{{Path: pendingHour + "good.parquet"}}}
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, store *flakyStore) string
	}{
		{"does not parse", func(t *testing.T, store *flakyStore) string {
			write(t, store, PendingRegistrationsRoot+"/bad.json", []byte("{not json"))
			return "bad.json"
		}},
		{"unreadable", func(t *testing.T, store *flakyStore) string {
			write(t, store, PendingRegistrationsRoot+"/locked.json", good)
			store.readOK = false
			return "locked.json"
		}},
		{"another cluster", func(t *testing.T, store *flakyStore) string {
			write(t, store, PendingRegistrationsRoot+"/foreign.json",
				PendingRegistrationsDoc{ClusterName: "other", Files: []PendingRegistrationFile{{Path: pendingHour + "foreign.parquet"}}})
			return "foreign.json"
		}},
		{"too large", func(t *testing.T, store *flakyStore) string {
			save := maxPublishedListBytes
			maxPublishedListBytes = 8
			t.Cleanup(func() { maxPublishedListBytes = save })
			write(t, store, PendingRegistrationsRoot+"/huge.json", good)
			return "huge.json"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &flakyStore{fakeObjectStore: newPendingStore(t), readOK: true}
			name := tc.setup(t, store)
			paths, held := ReadPublishedPending(ctx, store, "test-cluster")
			if len(held) != 1 || !strings.Contains(held[0], name) {
				t.Fatalf("held %v, want one naming %s", held, name)
			}
			for _, p := range paths {
				if strings.Contains(p, "foreign") {
					t.Fatal("a list of another cluster was trusted")
				}
			}
			if tc.name == "too large" && store.reads.Load() != 0 {
				t.Fatal("an over-size list was read")
			}
		})
	}

	// A good list beside them is read.
	store := &flakyStore{fakeObjectStore: newPendingStore(t), readOK: true}
	write(t, store, PendingRegistrationsRoot+"/w1.json", good)
	paths, held := ReadPublishedPending(ctx, store, "test-cluster")
	if len(held) != 0 || len(paths) != 1 {
		t.Fatalf("paths %v held %v", paths, held)
	}
}

// A reply must come from the node that was asked: a genuine, signed reply of
// another node is no answer for this one.
func TestPendingRegistrationsReplyFromAnotherNodeIsRejected(t *testing.T) {
	rig := originRig(t, "reader-1", RoleReader)
	serveListener(t, rig, "writer-2", func(conn net.Conn) {
		defer conn.Close()
		msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
		if err != nil {
			return
		}
		q := msg.Payload.(*protocol.PendingRegistrationsQuery)
		ts := time.Now().Unix()
		reply := &protocol.PendingRegistrationsReply{NodeID: "writer-1", Status: protocol.PendingStatusOK, Timestamp: ts,
			HMAC: security.ComputePendingRegistrationsReplyHMAC(osSecret, q.Nonce, "writer-1", "test-cluster", protocol.PendingStatusOK, nil, ts)}
		_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgPendingRegistrationsReply, Payload: reply}, 5*time.Second)
	})
	if _, err := rig.queryPendingRegistrations(context.Background(), "writer-2", rig.leaderCoordinatorAddress("writer-2")); !errors.Is(err, errNoPendingAnswer) {
		t.Fatalf("writer-1's reply accepted for writer-2: %v", err)
	}
}

// A query for another cluster is refused, even signed with this one's secret.
func TestPendingRegistrationsQueryFromAnotherClusterIsRefused(t *testing.T) {
	rig := originRig(t, "reader-1", RoleReader)
	outbox := newTestOutbox(t, t.TempDir())
	responder := pendingResponder(t, rig, "writer-1", RoleWriter, outbox, osSecret)
	if reply := askAs(t, responder, "other-cluster", "n-other"); reply.Status != protocol.PendingStatusRefused {
		t.Fatalf("a query for another cluster was answered: %+v", reply)
	}
}
