package cluster

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func newForwardAckTestCoordinator(secret string) *Coordinator {
	local := NewNode("follower", "follower", RoleWriter, joinTestCluster)
	return &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: joinTestCluster, SharedSecret: secret},
		localNode: local,
		logger:    zerolog.Nop(),
	}
}

func signedAck(secret, reqNonce, status string, code protocol.ForwardApplyCode, errMsg string) *protocol.ForwardApplyAck {
	ack := &protocol.ForwardApplyAck{Status: status, Code: code, Error: errMsg}
	if secret != "" {
		ack.AuthTimestamp = time.Now().Unix()
		ack.AuthHMAC = security.ComputeForwardAckHMAC(secret, reqNonce, ack.AuthTimestamp,
			security.ForwardAckAuthFields{Status: ack.Status, Code: string(ack.Code), Error: ack.Error})
	}
	return ack
}

// TestCheckForwardAck_AcceptsValid is the control.
func TestCheckForwardAck_AcceptsValid(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)
	const nonce = "req-nonce"
	if err := c.checkForwardAck(signedAck(joinTestSecret, nonce, "ok", "", ""), nonce); err != nil {
		t.Fatalf("valid ack rejected: %v", err)
	}
}

// TestCheckForwardAck_RejectsForgedSuccess is the headline case: a forged "ok"
// makes the follower believe a RegisterFile or DeleteFile was committed when it
// was not, silently dropping the entry from the manifest.
func TestCheckForwardAck_RejectsForgedSuccess(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)

	cases := map[string]*protocol.ForwardApplyAck{
		"unsigned":  {Status: "ok"},
		"bad MAC":   {Status: "ok", AuthTimestamp: time.Now().Unix(), AuthHMAC: strings.Repeat("0", 64)},
		"wrong key": signedAck("not-the-secret", "req-nonce", "ok", "", ""),
	}
	for name, ack := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.checkForwardAck(ack, "req-nonce"); err == nil {
				t.Errorf("forged success ack (%s) was accepted", name)
			}
		})
	}
}

// TestCheckForwardAck_RejectsForgedErrorCodes is the subtler half, and the
// reason Code is bound and the check runs BEFORE the Status branch.
//
// An unsigned {error, not_leader} is mapped by the caller to ErrNoLeaderKnown
// and retried as a transient leadership change; an unsigned {apply_failed,
// "... already exists"} is read by auth bootstrap as "already initialised".
// Both let an on-path attacker steer the caller without ever forging a
// success.
func TestCheckForwardAck_RejectsForgedErrorCodes(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)

	forged := map[string]*protocol.ForwardApplyAck{
		"not_leader":   {Status: "error", Code: protocol.ForwardCodeNotLeader},
		"apply_failed": {Status: "error", Code: protocol.ForwardCodeApplyFailed, Error: "token already exists"},
		"auth":         {Status: "error", Code: protocol.ForwardCodeAuth, Error: "authentication failed"},
	}
	for name, ack := range forged {
		t.Run(name, func(t *testing.T) {
			err := c.checkForwardAck(ack, "req-nonce")
			if err == nil {
				t.Fatalf("forged error ack (%s) was accepted", name)
			}
			// It must not surface as a transient-retry signal, or the caller
			// would spin against an attacker instead of failing.
			if errors.Is(err, ErrNoLeaderKnown) {
				t.Error("a forged ack was mapped to ErrNoLeaderKnown — attacker controls retry behaviour")
			}
		})
	}
}

// TestCheckForwardAck_RejectsMutatedCode: the ack is authentic but its Code
// was flipped in flight.
func TestCheckForwardAck_RejectsMutatedCode(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)
	const nonce = "req-nonce"

	ack := signedAck(joinTestSecret, nonce, "error", protocol.ForwardCodeApplyFailed, "disk full")
	ack.Code = protocol.ForwardCodeNotLeader // now looks transient

	if err := c.checkForwardAck(ack, nonce); err == nil {
		t.Error("an ack with a mutated Code was accepted — Code is outside the MAC")
	}
}

// TestCheckForwardAck_RejectsWrongNonce: an ack captured from an earlier
// round-trip must not satisfy a later request on the same pooled connection.
func TestCheckForwardAck_RejectsWrongNonce(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)
	ack := signedAck(joinTestSecret, "an-earlier-nonce", "ok", "", "")
	if err := c.checkForwardAck(ack, "the-current-nonce"); err == nil {
		t.Error("an ack bound to a different request's nonce was accepted")
	}
}

// TestCheckForwardAck_NoSecretAcceptsUnsigned: without a shared secret there
// is nothing to verify with, and forward-apply refuses to run at all earlier
// in the call path.
func TestCheckForwardAck_NoSecretAcceptsUnsigned(t *testing.T) {
	c := newForwardAckTestCoordinator("")
	if err := c.checkForwardAck(&protocol.ForwardApplyAck{Status: "ok"}, ""); err != nil {
		t.Fatalf("unsigned ack rejected with no secret configured: %v", err)
	}
}

// TestSignForwardAck_RoundTrips proves the leader's signing path and the
// follower's checking path agree — they are written separately, so a field
// ordering mistake in one would otherwise only show up in a live cluster.
func TestSignForwardAck_RoundTrips(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)
	const nonce = "req-nonce"

	for _, ack := range []*protocol.ForwardApplyAck{
		{Status: "ok"},
		{Status: "error", Code: protocol.ForwardCodeNotLeader, Error: "not the current leader"},
		{Status: "error", Code: protocol.ForwardCodeApplyFailed, Error: "token already exists"},
	} {
		c.signForwardAck(ack, nonce)
		if err := c.checkForwardAck(ack, nonce); err != nil {
			t.Errorf("ack %+v failed its own validation: %v", ack, err)
		}
	}
}

// TestHandleForwardApply_FailsClosedWithoutReplayGuard covers the cell opened
// by tightening the forward-apply and replicate-sync replay checks from
// `cache != nil && !Track(...)` to `!Track(...)`.
//
// Those two sites are not reachable with a nil cache in production — Start()
// installs it before the listener accepts — but the previous form would have
// skipped replay protection entirely if that ever stopped holding, with every
// test still green. Now an absent guard rejects, matching the handshake
// validators. This test pins that, and the nil-safety of Track that makes it
// work without a panic.
func TestHandleForwardApply_FailsClosedWithoutReplayGuard(t *testing.T) {
	c := newForwardAckTestCoordinator(joinTestSecret)
	if c.nonceCache != nil {
		t.Fatal("this coordinator is built without a nonce cache by design")
	}

	// The bare call must report "not new" rather than panicking, which is
	// what lets the caller drop its nil check.
	if c.nonceCache.Track("node", "nonce") {
		t.Error("Track on a nil cache reported the nonce as new — the guard would fail open")
	}
}
