package raft

import (
	"errors"
	"testing"

	"github.com/hashicorp/raft"
)

// TestTokenNameRefusalIsTypedWithUnchangedText pins the FSM's refusal of a
// taken token name on create and on rename: it matches ErrTokenNameExists, so
// the leader's proposer can tag it without reading text (#1625), and its text
// is byte-identical to the literal the FSM returned before. A follower only
// ever sees that text, and older followers, ensureFirstToken and the recovery
// token match "already exists" in it.
func TestTokenNameRefusalIsTypedWithUnchangedText(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	for i, name := range []string{"a", "b"} {
		cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry(name)})
		if r := fsm.Apply(&raft.Log{Data: cmd, Index: uint64(i + 1)}); r != nil {
			t.Fatalf("create %s: %v", name, r)
		}
	}

	dup := makeTokenEntry("a")
	dup.TokenPrefix = "other-prefix"
	create := fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: dup}), Index: 3})
	rename := fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandUpdateToken, UpdateTokenPayload{
		ID: 2, Name: "a", ChangedFields: []string{"name"},
	}), Index: 4})

	for _, tc := range []struct {
		op   string
		got  interface{}
		want string
	}{
		{"create", create, `create token: token name "a" already exists`},
		{"rename", rename, `update token: token name "a" already exists`},
	} {
		err, ok := tc.got.(error)
		if !ok || err == nil {
			t.Fatalf("%s of a taken name: result %v, want an error", tc.op, tc.got)
		}
		if !errors.Is(err, ErrTokenNameExists) {
			t.Errorf("%s of a taken name: %v does not match ErrTokenNameExists", tc.op, err)
		}
		if err.Error() != tc.want {
			t.Errorf("%s of a taken name: text %q, want %q", tc.op, err.Error(), tc.want)
		}
	}

	// A taken name is a client error, not a malformed command: neither
	// refusal counts as a rejected token.
	if n := fsm.RejectedTokensCount(); n != 0 {
		t.Errorf("rejected-token count after two name refusals = %d, want 0", n)
	}

	// Other token refusals are not name conflicts.
	bad := makeTokenEntry("c")
	bad.Permissions = "nope"
	other, _ := fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: bad}), Index: 5}).(error)
	if other == nil || errors.Is(other, ErrTokenNameExists) {
		t.Errorf("invalid-permission refusal: %v, want an error that is not ErrTokenNameExists", other)
	}
}
