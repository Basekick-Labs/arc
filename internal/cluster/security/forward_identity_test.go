package security

import (
	"testing"
	"time"
)

func fwdIdentity() ForwardIdentity {
	return ForwardIdentity{NodeID: "a", TargetID: "b", Cluster: "c", TokenID: 3, Method: "POST", RequestURI: "/w?db=x",
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
		"node": func(f *ForwardIdentity) { f.NodeID = "z" }, "target": func(f *ForwardIdentity) { f.TargetID = "z" },
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
	if !c.Track("a", "1") || c.Track("a", "1") {
		t.Fatal("first must be new, second a replay")
	}
	if !c.Track("b", "1") {
		t.Fatal("nonces are per node")
	}
	// Seen just before a rotation: still remembered a whole ttl later.
	now = now.Add(ttl - time.Millisecond)
	if !c.Track("a", "2") {
		t.Fatal("fresh nonce")
	}
	for _, step := range []time.Duration{time.Millisecond, ttl / 2, ttl - 2*time.Millisecond} {
		now = time.Unix(1000, 0).Add(ttl - time.Millisecond).Add(step)
		if c.Track("a", "2") {
			t.Fatalf("forgotten %v after it was seen", step)
		}
	}
	now = now.Add(3 * ttl)
	if !c.Track("a", "2") {
		t.Fatal("never forgotten")
	}
	var nilCache *ForwardNonceCache
	if nilCache.Track("a", "x") {
		t.Fatal("a nil cache must fail closed")
	}
}
