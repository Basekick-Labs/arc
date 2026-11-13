package edgesync

import (
	"context"
	"errors"
	"testing"
	"time"
)

// deleterFor deletes from the rig's backend the way main.go's deleter does:
// gone is success, a size mismatch keeps the file.
func deleterFor(r *agentRig, fail error) LocalDeleter {
	return func(ctx context.Context, path string, want int64) error {
		if fail != nil {
			return fail
		}
		size, err := r.backend.StatFile(ctx, path)
		if err != nil {
			return err
		}
		if size < 0 {
			return nil
		}
		if size != want {
			return errors.New("size mismatch")
		}
		return r.backend.Delete(ctx, path)
	}
}

func localExists(t *testing.T, r *agentRig, path string) bool {
	t.Helper()
	size, err := r.backend.StatFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return size >= 0
}

func ledgerRow(t *testing.T, r *agentRig, path string) (state string, hubState string, deleted bool) {
	t.Helper()
	var hs *string
	var del *time.Time
	if err := r.ledger.db.QueryRow(`SELECT state, hub_state, deleted_at FROM sync_ledger WHERE path = ?`, path).Scan(&state, &hs, &del); err != nil {
		t.Fatal(err)
	}
	if hs != nil {
		hubState = *hs
	}
	return state, hubState, del != nil
}

// The pass that syncs a file does not delete it; a later pass, once the hub
// verifies it holds the file, does. The hub keeps its copy.
func TestDeleteAfterSync_DeletesOnlyAfterALaterVerification(t *testing.T) {
	ctx := context.Background()
	r := newAgentRig(t)
	r.agent.SetDeleteAfterSync(0, deleterFor(r, nil))
	r.writeFile(t, agentPath, []byte("payload"))

	res, err := r.agent.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 1 || res.LocalDeleted != 0 || !localExists(t, r, agentPath) {
		t.Fatalf("first pass sent=%d deleted=%d: the pass that delivers a file must not delete it", res.Sent, res.LocalDeleted)
	}
	if res, err = r.agent.Run(ctx); err != nil || res.LocalDeleted != 1 {
		t.Fatalf("second pass: deleted=%d err=%v, want 1", res.LocalDeleted, err)
	}
	if localExists(t, r, agentPath) {
		t.Fatal("the local copy survived although the hub verified it holds it")
	}
	if _, held := r.transport.Has(DefaultHubID, agentPath); !held {
		t.Fatal("the hub lost its copy")
	}
	if _, hs, del := ledgerRow(t, r, agentPath); !del || hs != HubStateHeld {
		t.Fatalf("ledger hub_state=%q deleted=%v, want held and deleted", hs, del)
	}
}

// With a delay, a freshly synced file is not verified yet.
func TestDeleteAfterSync_RespectsTheDelay(t *testing.T) {
	ctx := context.Background()
	r := newAgentRig(t)
	r.agent.SetDeleteAfterSync(time.Hour, deleterFor(r, nil))
	r.writeFile(t, agentPath, []byte("payload"))
	for i := 0; i < 2; i++ {
		if _, err := r.agent.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !localExists(t, r, agentPath) {
		t.Fatal("deleted before the delay passed")
	}
}

// A hub that cannot prove it holds a file (compacted or removed receipt)
// keeps the spoke's copy, and the row is not asked again within a day.
func TestDeleteAfterSync_KeepsWhatTheHubCannotProve(t *testing.T) {
	ctx := context.Background()
	r := newAgentRig(t)
	r.writeFile(t, agentPath, []byte("payload"))
	if _, err := r.agent.Run(ctx); err != nil { // sync, feature off
		t.Fatal(err)
	}
	r.transport.MarkUnheld(DefaultHubID, agentPath)
	r.agent.SetDeleteAfterSync(0, deleterFor(r, nil))
	res, err := r.agent.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.VerifyKept != 1 || !localExists(t, r, agentPath) {
		t.Fatalf("kept=%d exists=%v, want kept", res.VerifyKept, localExists(t, r, agentPath))
	}
	if _, hs, _ := ledgerRow(t, r, agentPath); hs != HubStateKept {
		t.Fatalf("hub_state=%q, want kept", hs)
	}
	res, _ = r.agent.Run(ctx)
	if res.VerifyKept != 0 {
		t.Fatal("a kept row was verified again within the recheck interval")
	}
}

// A file the hub forgot entirely is re-queued while it exists locally, and
// re-sent: the re-offer the admin forget route relies on (#611).
func TestDeleteAfterSync_RequeuesWhatTheHubForgot(t *testing.T) {
	ctx := context.Background()
	r := newAgentRig(t)
	r.writeFile(t, agentPath, []byte("payload"))
	if _, err := r.agent.Run(ctx); err != nil {
		t.Fatal(err)
	}
	r.transport.Forget(DefaultHubID, agentPath)
	r.agent.SetDeleteAfterSync(0, deleterFor(r, errors.New("must not be called")))
	res, err := r.agent.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.VerifyRequeued != 1 || !localExists(t, r, agentPath) {
		t.Fatalf("requeued=%d exists=%v", res.VerifyRequeued, localExists(t, r, agentPath))
	}
	if st, _, _ := ledgerRow(t, r, agentPath); st != string(StatePending) {
		t.Fatalf("state %q, want pending", st)
	}
	res, err = r.agent.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, held := r.transport.Has(DefaultHubID, agentPath); !held || res.Sent != 1 {
		t.Fatalf("re-send: sent=%d held=%v", res.Sent, held)
	}
}

// A hub that predates verification gets nothing deleted; a deleter failure
// keeps the file and the row.
func TestDeleteAfterSync_NothingDeletedWithoutProof(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]func(r *agentRig){
		"old hub":        func(r *agentRig) { r.transport.NoVerify = true; r.agent.SetDeleteAfterSync(0, deleterFor(r, nil)) },
		"deleter failed": func(r *agentRig) { r.agent.SetDeleteAfterSync(0, deleterFor(r, errors.New("disk busy"))) },
	} {
		t.Run(name, func(t *testing.T) {
			r := newAgentRig(t)
			r.writeFile(t, agentPath, []byte("payload"))
			if _, err := r.agent.Run(ctx); err != nil {
				t.Fatal(err)
			}
			setup(r)
			if _, err := r.agent.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if !localExists(t, r, agentPath) {
				t.Fatal("deleted without proof")
			}
			if _, _, del := ledgerRow(t, r, agentPath); del {
				t.Fatal("row marked deleted")
			}
		})
	}
}

// A file already gone locally (spoke compaction or retention took it) is
// marked deleted when the hub holds it, and needs no delete.
func TestDeleteAfterSync_AlreadyGoneIsMarked(t *testing.T) {
	ctx := context.Background()
	r := newAgentRig(t)
	r.writeFile(t, agentPath, []byte("payload"))
	if _, err := r.agent.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.backend.Delete(ctx, agentPath); err != nil {
		t.Fatal(err)
	}
	r.agent.SetDeleteAfterSync(0, deleterFor(r, nil))
	if _, err := r.agent.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if _, hs, del := ledgerRow(t, r, agentPath); !del || hs != HubStateHeld {
		t.Fatalf("hub_state=%q deleted=%v, want held and deleted", hs, del)
	}
}
