package raft

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1336: a snapshot install hands the auth reconciler the restored auth state,
// and a reconcile can never write an older copy over a newer apply.

func createToken(t *testing.T, f *ClusterFSM, idx uint64, name string) {
	t.Helper()
	if res := applyCmd(t, f, idx, CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry(name)}); res != nil {
		t.Fatalf("create %s: %v", name, res)
	}
}

func restoredFrom(t *testing.T, f *ClusterFSM) io.ReadCloser {
	t.Helper()
	return io.NopCloser(bytes.NewReader(persistFSM(t, f)))
}

func TestRestoreHandsTheAuthReconcilerTheRestoredState(t *testing.T) {
	leader := NewClusterFSM(zerolog.Nop())
	createToken(t, leader, 10, "kept")
	createToken(t, leader, 11, "revoked")
	if res := applyCmd(t, leader, 12, CommandRevokeToken, RevokeTokenPayload{ID: 11}); res != nil {
		t.Fatal(res)
	}

	follower := NewClusterFSM(zerolog.Nop())
	var calls []AuthSnapshot
	follower.SetAuthReconciler(func(s AuthSnapshot) { calls = append(calls, s) })
	if err := follower.Restore(restoredFrom(t, leader)); err != nil {
		t.Fatal(err)
	}

	if len(calls) != 1 {
		t.Fatalf("reconciler called %d times, want 1", len(calls))
	}
	got := map[int64]bool{}
	for _, tok := range calls[0].Tokens {
		got[tok.ID] = tok.Enabled
	}
	if enabled, ok := got[11]; !ok || enabled {
		t.Errorf("restored copy: token 11 present=%v enabled=%v, want present and revoked", ok, enabled)
	}
	if calls[0].AppliedIndex != 12 {
		t.Errorf("applied index %d, want 12", calls[0].AppliedIndex)
	}
}

// A snapshot entry Restore refuses is quarantined, not presented as deleted.
func TestRestoreReportsQuarantinedAuthEntries(t *testing.T) {
	bad := makeTokenEntry("bad")
	bad.ID = 20
	bad.TokenHash = "" // fails validateTokenEntry
	good := makeTokenEntry("good")
	good.ID = 21

	f := NewClusterFSM(zerolog.Nop())
	var got AuthSnapshot
	f.SetAuthReconciler(func(s AuthSnapshot) { got = s })
	if err := f.Restore(restoreJSON(t, FSMSnapshot{Tokens: map[int64]*TokenEntry{20: &bad, 21: &good}, AppliedIndex: 30})); err != nil {
		t.Fatal(err)
	}
	if !got.Quarantined[authTableTokens][20] {
		t.Errorf("quarantined = %v, want token 20", got.Quarantined)
	}
	if got.Quarantined[authTableTokens][21] {
		t.Error("a valid token reported as quarantined")
	}
}

// The boot restore inside NewRaft runs before cmd/arc wires a reconciler; a
// nil reconciler must be harmless.
func TestRestoreWithoutAuthReconciler(t *testing.T) {
	leader := NewClusterFSM(zerolog.Nop())
	createToken(t, leader, 10, "a")
	f := NewClusterFSM(zerolog.Nop())
	if err := f.Restore(restoredFrom(t, leader)); err != nil {
		t.Fatal(err)
	}
	if f.ReconcileAuthNow() {
		t.Error("ReconcileAuthNow reported a run with no reconciler set")
	}
}

// An apply that lands while a reconcile holds its copy must write its change
// AFTER the copy, never before: otherwise the reconcile would put a revoked
// token back. The seam parks the reconcile between taking its copy and
// writing it; the revoke's apply mutates the FSM and its callback must wait.
func TestReconcileNeverWritesAnOlderCopyOverANewerApply(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	createToken(t, f, 10, "tok")

	var mu sync.Mutex
	sqlite := map[int64]bool{} // token id -> enabled, as the "local SQLite" sees it
	f.SetAuthCallbacks(nil, nil, func(id int64) {
		mu.Lock()
		sqlite[id] = false
		mu.Unlock()
	}, nil, nil)
	f.SetAuthReconciler(func(s AuthSnapshot) {
		mu.Lock()
		defer mu.Unlock()
		for _, tok := range s.Tokens {
			sqlite[tok.ID] = tok.Enabled
		}
	})

	applied := make(chan struct{})
	f.authReconcileCopied = func() {
		go func() {
			defer close(applied)
			if res := applyCmd(t, f, 11, CommandRevokeToken, RevokeTokenPayload{ID: 10}); res != nil {
				t.Errorf("revoke: %v", res)
			}
		}()
		// Wait until the revoke is in the FSM: its callback is now due.
		deadline := time.Now().Add(5 * time.Second)
		for {
			if tok := f.GetTokenByID(10); tok != nil && !tok.Enabled {
				break
			}
			if time.Now().After(deadline) {
				t.Error("revoke never reached the FSM")
				return
			}
			time.Sleep(time.Millisecond)
		}
		// Give an unserialised callback every chance to run first.
		time.Sleep(20 * time.Millisecond)
	}
	if !f.ReconcileAuthNow() {
		t.Fatal("no reconciler")
	}
	<-applied

	mu.Lock()
	defer mu.Unlock()
	if sqlite[10] {
		t.Fatal("the reconcile's older copy overwrote the revoke: the token authenticates again")
	}
}
