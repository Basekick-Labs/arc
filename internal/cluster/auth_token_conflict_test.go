package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// #1625: a token create or rename to a taken name answers 409, and a rotate of
// a revoked token is refused. These tests run a real AuthManager against a real
// ClusterFSM, with the FSM's refusal reaching the AuthManager through
// wrapApplyError in the two shapes production produces: typed on the leader,
// and as the leader's text in a ForwardRejectedError on a follower.

// fsmProposer applies every proposal to one real FSM and returns its refusal
// the way CoordinatorAuthProposer would on a leader (forwarded=false) or on a
// follower (forwarded=true).
type fsmProposer struct {
	fsm       *raft.ClusterFSM
	forwarded bool
	logIdx    atomic.Uint64
	proposals atomic.Int64
	// beforeRotate, when set, is a command committed and applied just ahead
	// of the next rotate: a revoke or delete from another node that the
	// rotate's pre-check could not see yet.
	beforeRotate *raft.Command
}

func (p *fsmProposer) apply(cmd raft.Command) interface{} {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return p.fsm.Apply(&hraft.Log{Index: p.logIdx.Add(1), Data: data})
}

func (p *fsmProposer) Propose(_ context.Context, cmdType uint8, payload []byte, _ time.Duration) error {
	p.proposals.Add(1)
	ct := raft.CommandType(cmdType)
	if ct == raft.CommandRotateToken && p.beforeRotate != nil {
		if res, _ := p.apply(*p.beforeRotate).(error); res != nil {
			return res
		}
		p.beforeRotate = nil
	}
	refusal, _ := p.apply(raft.Command{Type: ct, Payload: payload}).(error)
	if refusal == nil {
		return nil
	}
	// Production wraps the leader's refusal in raft's unexported
	// commandRefused, whose Unwrap keeps the chain; the raw error stands in.
	if p.forwarded {
		// handleForwardApply sends err.Error() with apply_failed.
		return wrapApplyError(ct, &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: refusal.Error()})
	}
	return wrapApplyError(ct, refusal)
}

func (p *fsmProposer) IsLeader() bool { return !p.forwarded }

func newFSMAuthRig(t *testing.T, forwarded bool) (*auth.AuthManager, *fsmProposer) {
	t.Helper()
	am, err := auth.NewAuthManager(t.TempDir()+"/auth.db", time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() {
		am.SetRaftProposer(nil)
		_ = am.Close()
	})
	fsm := raft.NewClusterFSM(zerolog.Nop())
	wireAuthCallbacks(t, fsm, am)
	prop := &fsmProposer{fsm: fsm, forwarded: forwarded}
	am.SetRaftProposer(prop)
	return am, prop
}

func clusterTokenID(t *testing.T, am *auth.AuthManager, name string) int64 {
	t.Helper()
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	for _, tk := range tokens {
		if tk.Name == name {
			return tk.ID
		}
	}
	t.Fatalf("no token named %q", name)
	return 0
}

func TestClusterTokenNameConflictThroughWrapApplyError(t *testing.T) {
	for _, forwarded := range []bool{false, true} {
		t.Run(fmt.Sprintf("forwarded=%v", forwarded), func(t *testing.T) {
			am, _ := newFSMAuthRig(t, forwarded)
			ctx := context.Background()
			for _, name := range []string{"a", "b"} {
				if _, err := am.CreateToken(ctx, name, "", "read", nil); err != nil {
					t.Fatalf("CreateToken(%s): %v", name, err)
				}
			}

			const want = "token with name 'a' already exists"
			_, err := am.CreateToken(ctx, "a", "", "read", nil)
			if !errors.Is(err, auth.ErrNameConflict) || err.Error() != want {
				t.Errorf("create of a taken name: %v, want ErrNameConflict %q", err, want)
			}

			taken := "a"
			err = am.UpdateToken(ctx, clusterTokenID(t, am, "b"), &taken, nil, nil, nil)
			if !errors.Is(err, auth.ErrNameConflict) || err.Error() != want {
				t.Errorf("rename to a taken name: %v, want ErrNameConflict %q", err, want)
			}
			clusterTokenID(t, am, "b") // the refused rename left the name alone

			// Any other refusal is not a name conflict.
			perms := "nope"
			err = am.UpdateToken(ctx, clusterTokenID(t, am, "b"), nil, nil, &perms, nil)
			if err == nil || errors.Is(err, auth.ErrNameConflict) {
				t.Errorf("invalid-permission refusal: %v, want an error that is not ErrNameConflict", err)
			}
		})
	}
}

func TestWrapApplyErrorTagsOnlyTokenNameRefusals(t *testing.T) {
	const tokenText = `update token: token name "a" already exists`
	for _, tc := range []struct {
		name string
		cmd  raft.CommandType
		err  error
		want bool
	}{
		{"forwarded token rename", raft.CommandUpdateToken, &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: tokenText}, true},
		{"forwarded token create", raft.CommandCreateToken, &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: `create token: token name "a" already exists`}, true},
		{"same text, another code", raft.CommandUpdateToken, &ForwardRejectedError{Code: protocol.ForwardCodeClaimConflict, Message: tokenText}, false},
		{"token text on another command", raft.CommandCreateOrganization, &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: tokenText}, false},
		{"forwarded organization name", raft.CommandCreateOrganization, &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: `create_organization: organization name "a" already exists`}, false},
		{"forwarded team name", raft.CommandCreateTeam, &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: `create_team: team name "a" already exists in organization 1`}, false},
		{"untyped leader text", raft.CommandUpdateToken, errors.New(tokenText), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := wrapApplyError(tc.cmd, tc.err)
			if errors.Is(got, auth.ErrNameConflict) != tc.want {
				t.Errorf("ErrNameConflict = %v, want %v (%v)", !tc.want, tc.want, got)
			}
			if !errors.Is(got, auth.ErrApplyFailed) {
				t.Errorf("%v no longer matches ErrApplyFailed", got)
			}
			// The message is the one wrapApplyError always produced.
			if want := fmt.Sprintf("%v: %v", auth.ErrApplyFailed, tc.err); got.Error() != want {
				t.Errorf("message %q, want %q", got.Error(), want)
			}
		})
	}
	if got := wrapApplyError(raft.CommandCreateToken, ErrNoLeaderKnown); got != auth.ErrLeaderUnknown {
		t.Errorf("no leader: %v, want ErrLeaderUnknown", got)
	}
}

// TestClusterRotateOfRevokedTokenIsRefused: the revoke goes through the FSM
// and its apply callback, so enabled=0 is really in this node's SQLite; the
// rotate is then refused before anything is proposed.
func TestClusterRotateOfRevokedTokenIsRefused(t *testing.T) {
	am, prop := newFSMAuthRig(t, false)
	ctx := context.Background()
	secret, err := am.CreateToken(ctx, "svc", "", "read", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	id := clusterTokenID(t, am, "svc")
	if err := am.RevokeToken(ctx, id); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	before := prop.proposals.Load()

	value, err := am.RotateToken(ctx, id)
	if !errors.Is(err, auth.ErrTokenRevoked) || value != "" {
		t.Fatalf("rotate of a revoked token: value %q, err %v, want ErrTokenRevoked", value, err)
	}
	if n := prop.proposals.Load() - before; n != 0 {
		t.Errorf("%d proposals for a revoked token, want 0", n)
	}
	if am.VerifyToken(secret) != nil {
		t.Error("the revoked token authenticates")
	}
}

// TestClusterRotateAfterAnUnseenRevokeOrDeleteIsRefused: a revoke or delete
// committed elsewhere just ahead of the rotate passes the pre-check, which
// reads this node's SQLite before it applied it. The FSM then applies the
// rotate to the revoked token, or drops it for the deleted one; either way the
// new value never authenticates. The re-read after the proposal refuses it.
func TestClusterRotateAfterAnUnseenRevokeOrDeleteIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  raft.CommandType
		want error
	}{
		{"revoke", raft.CommandRevokeToken, auth.ErrTokenRevoked},
		{"delete", raft.CommandDeleteToken, auth.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			am, prop := newFSMAuthRig(t, false)
			ctx := context.Background()
			secret, err := am.CreateToken(ctx, "svc", "", "read", nil)
			if err != nil {
				t.Fatalf("CreateToken: %v", err)
			}
			id := clusterTokenID(t, am, "svc")
			payload, err := json.Marshal(struct {
				ID int64 `json:"id"`
			}{id})
			if err != nil {
				t.Fatal(err)
			}
			prop.beforeRotate = &raft.Command{Type: tc.cmd, Payload: payload}

			value, err := am.RotateToken(ctx, id)
			if !errors.Is(err, tc.want) || value != "" {
				t.Fatalf("rotate after an unseen %s: value %q, err %v, want %v", tc.name, value, err, tc.want)
			}
			if prop.beforeRotate != nil {
				t.Fatal("the rotate was not proposed, so the test did not reach the re-read")
			}
			if am.VerifyToken(secret) != nil {
				t.Errorf("the old secret authenticates after the %s", tc.name)
			}
		})
	}
}
