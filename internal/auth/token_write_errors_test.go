package auth

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// fsmLikeTokenProposer answers token commands the way the cluster FSM does
// for the cases these tests need: an update, revoke, rotate or delete of an
// ID it does not hold is dropped as an idempotent no-op (Propose returns nil,
// nothing applied), and a refusal is returned wrapped in ErrApplyFailed, as
// cluster.wrapApplyError does. A name-conflict refusal must be set already
// tagged ErrNameConflict (nameRefusal), the shape wrapApplyError gives it; the
// tagging itself is tested in internal/cluster against a real FSM. It records
// every proposal so a test can require that nothing was proposed at all.
type fsmLikeTokenProposer struct {
	mu       sync.Mutex
	proposed []uint8
	refuse   error // returned for every proposal when set
}

func (p *fsmLikeTokenProposer) Propose(_ context.Context, cmdType uint8, _ []byte, _ time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.proposed = append(p.proposed, cmdType)
	if p.refuse != nil {
		return fmt.Errorf("%w: %w", ErrApplyFailed, p.refuse)
	}
	return nil
}

func (p *fsmLikeTokenProposer) IsLeader() bool { return true }

// nameRefusal is the FSM's refusal of a taken token name as the cluster
// proposer hands it on: the FSM's text, tagged ErrNameConflict.
func nameRefusal(op, name string) error {
	return tagError(ErrNameConflict, fmt.Errorf("%s token: token name %q already exists", op, name))
}

func (p *fsmLikeTokenProposer) proposals() []uint8 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint8(nil), p.proposed...)
}

func newTokenWriteTestManager(t *testing.T) *AuthManager {
	t.Helper()
	am, err := NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() {
		am.SetRaftProposer(nil)
		_ = am.Close()
	})
	return am
}

// tokenWritesOfID runs every token write that names a token by ID and returns
// each one's error, keyed by operation.
func tokenWritesOfID(am *AuthManager, id int64) map[string]error {
	ctx := context.Background()
	desc := "changed"
	value, rotateErr := am.RotateToken(ctx, id)
	if rotateErr == nil && value != "" {
		rotateErr = fmt.Errorf("rotate returned a token value for ID %d", id)
	}
	return map[string]error{
		"update":         am.UpdateToken(ctx, id, nil, &desc, nil, nil),
		"update (no-op)": am.UpdateToken(ctx, id, nil, nil, nil, nil),
		"revoke":         am.RevokeToken(ctx, id),
		"rotate":         rotateErr,
		"delete":         am.DeleteToken(ctx, id),
	}
}

func TestTokenWritesOfUnknownIDAreNotFound(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered=%v", clustered), func(t *testing.T) {
			am := newTokenWriteTestManager(t)
			prop := &fsmLikeTokenProposer{}
			if clustered {
				am.SetRaftProposer(prop)
			}

			for op, err := range tokenWritesOfID(am, 4242) {
				if !errors.Is(err, ErrNotFound) {
					t.Errorf("%s of an unknown ID: err = %v, want ErrNotFound", op, err)
					continue
				}
				if err.Error() != "token not found" {
					t.Errorf("%s of an unknown ID: message = %q, want %q", op, err.Error(), "token not found")
				}
			}
			// The FSM drops these as no-ops, so a proposal could only ever
			// answer success. Nothing may reach it.
			if got := prop.proposals(); len(got) != 0 {
				t.Errorf("proposed %v for an unknown ID, want nothing", got)
			}
		})
	}
}

func TestTokenWritesOfExistingIDStillSucceed(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered=%v", clustered), func(t *testing.T) {
			am := newTokenWriteTestManager(t)
			// Created before the proposer is wired, so the row is in local
			// SQLite as it would be once the FSM applied the create.
			if _, err := am.CreateToken(context.Background(), "svc", "", "read", nil); err != nil {
				t.Fatalf("CreateToken: %v", err)
			}
			id := tokenIDByName(t, am, "svc")
			prop := &fsmLikeTokenProposer{}
			if clustered {
				am.SetRaftProposer(prop)
			}

			ctx := context.Background()
			desc := "changed"
			if err := am.UpdateToken(ctx, id, nil, &desc, nil, nil); err != nil {
				t.Errorf("update: %v", err)
			}
			if err := am.UpdateToken(ctx, id, nil, nil, nil, nil); err != nil {
				t.Errorf("no-op update: %v", err)
			}
			// Rotate before revoke: a revoked token is not rotated (#1625).
			if v, err := am.RotateToken(ctx, id); err != nil || v == "" {
				t.Errorf("rotate: value %q, err %v", v, err)
			}
			if err := am.RevokeToken(ctx, id); err != nil {
				t.Errorf("revoke: %v", err)
			}
			if err := am.DeleteToken(ctx, id); err != nil {
				t.Errorf("delete: %v", err)
			}
			if clustered {
				want := []uint8{ProposalCommandUpdateToken, ProposalCommandRotateToken, ProposalCommandRevokeToken, ProposalCommandDeleteToken}
				if got := prop.proposals(); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("proposed %v, want %v", got, want)
				}
			}
		})
	}
}

func TestTokenNameConflictIsTagged(t *testing.T) {
	ctx := context.Background()

	t.Run("standalone", func(t *testing.T) {
		am := newTokenWriteTestManager(t)
		for _, name := range []string{"a", "b"} {
			if _, err := am.CreateToken(ctx, name, "", "read", nil); err != nil {
				t.Fatalf("CreateToken(%s): %v", name, err)
			}
		}

		_, err := am.CreateToken(ctx, "a", "", "read", nil)
		requireNameConflict(t, "create", err, "a")

		taken := "a"
		err = am.UpdateToken(ctx, tokenIDByName(t, am, "b"), &taken, nil, nil, nil)
		requireNameConflict(t, "rename", err, "a")
		if id := tokenIDByName(t, am, "b"); id == 0 {
			t.Error("the refused rename changed the token's name")
		}

		// Renaming a token to its own name is not a conflict.
		own := "b"
		if err := am.UpdateToken(ctx, tokenIDByName(t, am, "b"), &own, nil, nil, nil); err != nil {
			t.Errorf("rename to its own name: %v", err)
		}
		// A rename of an unknown ID is not found, not a conflict.
		if err := am.UpdateToken(ctx, 4242, &taken, nil, nil, nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("rename of an unknown ID to a taken name: %v, want ErrNotFound", err)
		}
	})

	t.Run("clustered", func(t *testing.T) {
		am := newTokenWriteTestManager(t)
		if _, err := am.CreateToken(ctx, "b", "", "read", nil); err != nil {
			t.Fatalf("CreateToken: %v", err)
		}
		id := tokenIDByName(t, am, "b")
		prop := &fsmLikeTokenProposer{}
		am.SetRaftProposer(prop)

		// The FSM's refusal texts (applyCreateToken, applyUpdateToken).
		prop.refuse = nameRefusal("create", "a")
		_, err := am.CreateToken(ctx, "a", "", "read", nil)
		requireNameConflict(t, "create", err, "a")

		prop.refuse = nameRefusal("update", "a")
		taken := "a"
		err = am.UpdateToken(ctx, id, &taken, nil, nil, nil)
		requireNameConflict(t, "rename", err, "a")

		// The FSM's text alone, untagged, is no longer read as a conflict:
		// the cluster proposer is the one place that classifies it.
		prop.refuse = fmt.Errorf("create token: token name %q already exists", "a")
		if _, err := am.CreateToken(ctx, "a", "", "read", nil); err == nil || errors.Is(err, ErrNameConflict) {
			t.Errorf("an untagged refusal: err = %v, want an untagged error", err)
		}

		// Any other refusal is not a name conflict.
		prop.refuse = errors.New("update token: invalid permission \"x\"")
		perms := "x"
		if err := am.UpdateToken(ctx, id, nil, nil, &perms, nil); err == nil || errors.Is(err, ErrNameConflict) {
			t.Errorf("a permission refusal: err = %v, want an untagged error", err)
		}
	})
}

func requireNameConflict(t *testing.T, op string, err error, name string) {
	t.Helper()
	if !errors.Is(err, ErrNameConflict) {
		t.Fatalf("%s with a taken name: err = %v, want ErrNameConflict", op, err)
	}
	if want := fmt.Sprintf("token with name '%s' already exists", name); err.Error() != want {
		t.Errorf("%s with a taken name: message = %q, want %q", op, err.Error(), want)
	}
}

func tokenIDByName(t *testing.T, am *AuthManager, name string) int64 {
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
	return 0
}
