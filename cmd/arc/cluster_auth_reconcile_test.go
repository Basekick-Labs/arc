package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// #1336: the boot reconcile runs only once this node has caught up with the
// leader, because until then the FSM holds the last local snapshot — older
// than what SQLite already has.

type fakeReconcileFSM struct {
	reconciler func(clusterraft.AuthSnapshot)
	runs       atomic.Int32
}

func (f *fakeReconcileFSM) SetAuthReconciler(fn func(clusterraft.AuthSnapshot)) { f.reconciler = fn }
func (f *fakeReconcileFSM) ReconcileAuthNow() bool                              { f.runs.Add(1); return true }

type fakeReconcileCluster struct {
	failures atomic.Int32 // SyncManifest fails this many times, then succeeds
	calls    atomic.Int32
}

func (c *fakeReconcileCluster) SyncManifest(context.Context) error {
	c.calls.Add(1)
	if c.failures.Add(-1) >= 0 {
		return errors.New("no leader")
	}
	return nil
}
func (c *fakeReconcileCluster) RaftTerm() uint64 { return 3 }

func newReconcileAuth(t *testing.T) *auth.AuthManager {
	t.Helper()
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	return am
}

func TestBootReconcileWaitsForTheLeaderSync(t *testing.T) {
	am := newReconcileAuth(t)
	fsm := &fakeReconcileFSM{}
	coord := &fakeReconcileCluster{}
	coord.failures.Store(2)

	done := startAuthReconcile(context.Background(), fsm, coord, am.GetDB(), am, nil, time.Millisecond, zerolog.Nop())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("boot reconcile never finished")
	}
	if got := coord.calls.Load(); got != 3 {
		t.Errorf("SyncManifest called %d times, want 3 (two failures, then success)", got)
	}
	if got := fsm.runs.Load(); got != 1 {
		t.Errorf("ReconcileAuthNow ran %d times, want once after the sync", got)
	}
	if fsm.reconciler == nil {
		t.Error("no reconciler installed for snapshot installs")
	}
}

// A node that never catches up never reconciles against its stale FSM.
func TestBootReconcileNeverRunsAgainstAStaleFSM(t *testing.T) {
	am := newReconcileAuth(t)
	fsm := &fakeReconcileFSM{}
	coord := &fakeReconcileCluster{}
	coord.failures.Store(1 << 30)

	ctx, cancel := context.WithCancel(context.Background())
	done := startAuthReconcile(ctx, fsm, coord, am.GetDB(), am, nil, time.Millisecond, zerolog.Nop())
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("boot reconcile did not stop on shutdown")
	}
	if got := fsm.runs.Load(); got != 0 {
		t.Fatalf("reconciled %d times without ever syncing with the leader", got)
	}
}

// The installed reconciler writes the FSM's state into SQLite and the boot
// run carries the Raft term (the rebuilt-history check needs it).
func TestInstalledReconcilerRevokesInSQLite(t *testing.T) {
	am := newReconcileAuth(t)
	tok := clusterraft.TokenEntry{ID: 50, Name: "ci", Permissions: "read", TokenHash: "h", TokenPrefix: "p", Enabled: true, CreatedAtUnixNano: 1}
	if err := am.ApplyCreateToken(auth.ClusterTokenEntry{ID: tok.ID, Name: tok.Name, Permissions: tok.Permissions,
		TokenHash: tok.TokenHash, TokenPrefix: tok.TokenPrefix, CreatedAtUnixNano: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}

	fsm := &fakeReconcileFSM{}
	coord := &fakeReconcileCluster{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // only the installed reconciler matters here
	<-startAuthReconcile(ctx, fsm, coord, am.GetDB(), am, nil, time.Millisecond, zerolog.Nop())

	tok.Enabled = false
	fsm.reconciler(clusterraft.AuthSnapshot{Tokens: []clusterraft.TokenEntry{tok}, AppliedIndex: 100})
	var enabled int
	if err := am.GetDB().QueryRow(`SELECT enabled FROM api_tokens WHERE id = 50`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 0 {
		t.Fatal("a token revoked in the cluster is still enabled in SQLite after the reconcile")
	}
}
