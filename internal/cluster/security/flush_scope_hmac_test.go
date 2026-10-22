package security

import (
	"testing"
	"time"
)

// #1265: the flush scope request is bound to its sender, cluster and scope;
// the reply to the request's nonce, the scope, its status and every path.
func TestFlushScopeHMACBindsEveryField(t *testing.T) {
	const secret = "s"
	ts := time.Now().Unix()
	mac := ComputeFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "cpu", ts)
	if err := ValidateFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "cpu", ts, mac, HMACTimestampTolerance); err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]func() error{
		"secret": func() error {
			return ValidateFlushScopeRequestHMAC("x", "n", "leader", "c", "db", "cpu", ts, mac, HMACTimestampTolerance)
		},
		"nonce": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "m", "leader", "c", "db", "cpu", ts, mac, HMACTimestampTolerance)
		},
		"node": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "n", "other", "c", "db", "cpu", ts, mac, HMACTimestampTolerance)
		},
		"cluster": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "n", "leader", "d", "db", "cpu", ts, mac, HMACTimestampTolerance)
		},
		"database": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "n", "leader", "c", "db2", "cpu", ts, mac, HMACTimestampTolerance)
		},
		"measurement": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "mem", ts, mac, HMACTimestampTolerance)
		},
		"whole db": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "", ts, mac, HMACTimestampTolerance)
		},
		"timestamp": func() error {
			return ValidateFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "cpu", ts+1, mac, HMACTimestampTolerance)
		},
		"stale": func() error {
			old := ts - 3600
			return ValidateFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "cpu", old, ComputeFlushScopeRequestHMAC(secret, "n", "leader", "c", "db", "cpu", old), HMACTimestampTolerance)
		},
	} {
		if v() == nil {
			t.Errorf("request: a changed %s was accepted", name)
		}
	}

	paths := []string{"db/cpu/a.parquet", "db/cpu/b.parquet"}
	rmac := ComputeFlushScopeReplyHMAC(secret, "n", "w1", "c", "db", "cpu", "ok", paths, ts)
	if err := ValidateFlushScopeReplyHMAC(secret, "n", "w1", "c", "db", "cpu", "ok", paths, ts, rmac, HMACTimestampTolerance); err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]func() error{
		"nonce": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "m", "w1", "c", "db", "cpu", "ok", paths, ts, rmac, HMACTimestampTolerance)
		},
		"node": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w2", "c", "db", "cpu", "ok", paths, ts, rmac, HMACTimestampTolerance)
		},
		"cluster": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w1", "d", "db", "cpu", "ok", paths, ts, rmac, HMACTimestampTolerance)
		},
		"database": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w1", "c", "db2", "cpu", "ok", paths, ts, rmac, HMACTimestampTolerance)
		},
		"measurement": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w1", "c", "db", "mem", "ok", paths, ts, rmac, HMACTimestampTolerance)
		},
		"status": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w1", "c", "db", "cpu", "untracked", paths, ts, rmac, HMACTimestampTolerance)
		},
		"a stripped path": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w1", "c", "db", "cpu", "ok", paths[:1], ts, rmac, HMACTimestampTolerance)
		},
		"timestamp": func() error {
			return ValidateFlushScopeReplyHMAC(secret, "n", "w1", "c", "db", "cpu", "ok", paths, ts+1, rmac, HMACTimestampTolerance)
		},
	} {
		if v() == nil {
			t.Errorf("reply: a changed %s was accepted", name)
		}
	}
}
