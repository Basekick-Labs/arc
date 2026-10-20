package security

import (
	"strings"
	"testing"
	"time"
)

var (
	n1 = strings.Repeat("1", 64)
	n2 = strings.Repeat("2", 64)
	nx = strings.Repeat("a", 64)
)

func fwdIdentity() ForwardIdentity {
	return ForwardIdentity{NodeID: "a", TargetID: "b", Cluster: "c", TokenID: 3, TokenFP: "fp", Method: "POST", RequestURI: "/w?db=x",
		Database: "x", Measurement: "m", ContentType: "t", ContentEncoding: "gzip", Timestamp: time.Now().Unix(), Nonce: "n"}
}

// The MAC covers every field the receiver acts on.
func TestForwardIdentityHMAC_BindsEveryField(t *testing.T) {
	const secret = "s3cret"
	base := fwdIdentity()
	mac := ComputeForwardIdentityHMAC(secret, base)
	if err := ValidateForwardIdentityHMAC(secret, base, mac, ForwardIdentityTolerance); err != nil {
		t.Fatalf("valid: %v", err)
	}
	for name, mut := range map[string]func(*ForwardIdentity){
		"node": func(f *ForwardIdentity) { f.NodeID = "z" }, "fingerprint": func(f *ForwardIdentity) { f.TokenFP = "other" }, "target": func(f *ForwardIdentity) { f.TargetID = "z" },
		"cluster": func(f *ForwardIdentity) { f.Cluster = "z" }, "token": func(f *ForwardIdentity) { f.TokenID = 4 },
		"method": func(f *ForwardIdentity) { f.Method = "PUT" }, "uri": func(f *ForwardIdentity) { f.RequestURI = "/w?db=y" },
		"database": func(f *ForwardIdentity) { f.Database = "y" }, "measurement": func(f *ForwardIdentity) { f.Measurement = "n" },
		"type": func(f *ForwardIdentity) { f.ContentType = "u" }, "encoding": func(f *ForwardIdentity) { f.ContentEncoding = "" },
		"timestamp": func(f *ForwardIdentity) { f.Timestamp++ }, "nonce": func(f *ForwardIdentity) { f.Nonce = "o" },
	} {
		f := base
		mut(&f)
		if ValidateForwardIdentityHMAC(secret, f, mac, ForwardIdentityTolerance) == nil {
			t.Errorf("%s is not bound", name)
		}
	}
	if ValidateForwardIdentityHMAC("", base, ComputeForwardIdentityHMAC("", base), ForwardIdentityTolerance) == nil {
		t.Error("an empty secret must never validate")
	}
	stale := base
	stale.Timestamp -= int64(ForwardIdentityTolerance/time.Second) + 2
	if ValidateForwardIdentityHMAC(secret, stale, ComputeForwardIdentityHMAC(secret, stale), ForwardIdentityTolerance) == nil {
		t.Error("a stale timestamp validated")
	}
	if MsgTypeForwardRequest == MsgTypeForwardAck || MsgTypeForwardRequest == MsgTypeJoin {
		t.Error("label collides with another MAC family")
	}
}

// A nonce is rejected for at least 2×tolerance+1s after it was first seen,
// and the cache forgets it after that.
func TestForwardNonceCache_RemembersForTheWholeWindow(t *testing.T) {
	c := NewForwardNonceCache(ForwardIdentityTolerance)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	for i := range c.shards {
		c.shards[i].rotated = now
	}
	ttl := 2*ForwardIdentityTolerance + time.Second
	if !c.Track("a", n1) || c.Track("a", n1) {
		t.Fatal("first must be new, second a replay")
	}
	if !c.Track("b", n1) {
		t.Fatal("nonces are per node")
	}
	// Seen just before a rotation: still remembered a whole ttl later.
	now = now.Add(ttl - time.Millisecond)
	if !c.Track("a", n2) {
		t.Fatal("fresh nonce")
	}
	for _, step := range []time.Duration{time.Millisecond, ttl / 2, ttl - 2*time.Millisecond} {
		now = time.Unix(1000, 0).Add(ttl - time.Millisecond).Add(step)
		if c.Track("a", n2) {
			t.Fatalf("forgotten %v after it was seen", step)
		}
	}
	now = now.Add(3 * ttl)
	if !c.Track("a", n2) {
		t.Fatal("never forgotten")
	}
	var nilCache *ForwardNonceCache
	if nilCache.Track("a", nx) {
		t.Fatal("a nil cache must fail closed")
	}
}

// An entry added just before its shard rotates is remembered for a whole
// ttl after the rotation, not dropped by the next one.
func TestForwardNonceCache_EntryAddedBeforeRotation(t *testing.T) {
	c := NewForwardNonceCache(ForwardIdentityTolerance)
	t0 := time.Unix(1000, 0)
	now := t0
	c.now = func() time.Time { return now }
	for i := range c.shards {
		c.shards[i].rotated = t0
	}
	ttl := 2*ForwardIdentityTolerance + time.Second
	now = t0.Add(ttl/2 - time.Millisecond)
	if !c.Track("a", nx) {
		t.Fatal("new")
	}
	for _, at := range []time.Duration{ttl/2 + time.Millisecond, ttl + time.Millisecond, ttl + ttl/2 - 2*time.Millisecond} {
		now = t0.Add(at)
		if c.Track("a", nx) {
			t.Fatalf("forgotten at %v, seen at %v", at, ttl/2-time.Millisecond)
		}
	}
}

// A nonce that is not one GenerateNonce makes is refused, not hashed.
func TestForwardNonceCache_RefusesMalformedNonce(t *testing.T) {
	c := NewForwardNonceCache(ForwardIdentityTolerance)
	for _, n := range []string{"", "1", strings.Repeat("z", 64), strings.Repeat("1", 63), strings.Repeat("1", 66)} {
		if c.Track("a", n) {
			t.Errorf("accepted %q", n)
		}
	}
}
