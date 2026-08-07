package security

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

// syncFileArgs is one complete set of file-MAC inputs, so a test can vary
// exactly one field and confirm the MAC changes.
type syncFileArgs struct {
	secret    string
	nonce     string
	spokeID   string
	hubID     string
	path      string
	sha256    string
	timestamp int64
}

func validFileArgs() syncFileArgs {
	return syncFileArgs{
		secret:    "shared-secret-for-rocket-01",
		nonce:     "5f3a9c1e2b7d4816a0c3e5f7d9b1a3c5",
		spokeID:   "rocket-01",
		hubID:     "ground-station",
		path:      "metrics/cpu/2026/08/07/14/cpu_123.parquet",
		sha256:    "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
		timestamp: time.Now().Unix(),
	}
}

func (a syncFileArgs) compute() string {
	mac, err := ComputeSyncFileHMAC(a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp)
	if err != nil {
		panic("compute: " + err.Error())
	}
	return mac
}

func (a syncFileArgs) validate(mac string) error {
	return ValidateSyncFileHMAC(a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256,
		a.timestamp, mac, HMACTimestampTolerance)
}

// mustFileMAC computes a file MAC, failing the test on error.
func mustFileMAC(t *testing.T, secret, nonce, spokeID, hubID, path, digest string, ts int64) string {
	t.Helper()
	mac, err := ComputeSyncFileHMAC(secret, nonce, spokeID, hubID, path, digest, ts)
	if err != nil {
		t.Fatalf("ComputeSyncFileHMAC: %v", err)
	}
	return mac
}

func mustReconcileMAC(t *testing.T, secret, nonce, spokeID, hubID string, body []byte, ts int64) string {
	t.Helper()
	mac, err := ComputeSyncReconcileHMAC(secret, nonce, spokeID, hubID, body, ts)
	if err != nil {
		t.Fatalf("ComputeSyncReconcileHMAC: %v", err)
	}
	return mac
}

// The NoT variants are for table closures that have no *testing.T in scope.
func mustFileMACNoT(secret, nonce, spokeID, hubID, path, digest string, ts int64) string {
	mac, err := ComputeSyncFileHMAC(secret, nonce, spokeID, hubID, path, digest, ts)
	if err != nil {
		panic("ComputeSyncFileHMAC: " + err.Error())
	}
	return mac
}

func mustReconcileMACNoT(secret, nonce, spokeID, hubID string, body []byte, ts int64) string {
	mac, err := ComputeSyncReconcileHMAC(secret, nonce, spokeID, hubID, body, ts)
	if err != nil {
		panic("ComputeSyncReconcileHMAC: " + err.Error())
	}
	return mac
}

func TestSyncFileHMAC_RoundTrip(t *testing.T) {
	a := validFileArgs()
	if err := a.validate(a.compute()); err != nil {
		t.Fatalf("a freshly computed MAC failed to validate: %v", err)
	}
}

func TestSyncFileHMAC_EveryBoundFieldIsLoadBearing(t *testing.T) {
	// Each case tampers with exactly one field between signing and
	// validating. If any of these passes, that field is not actually bound
	// and an attacker can vary it freely under a captured MAC.
	tests := []struct {
		field  string
		tamper func(*syncFileArgs)
		attack string
	}{
		{
			field:  "spokeID",
			tamper: func(a *syncFileArgs) { a.spokeID = "rocket-02" },
			attack: "one spoke writing into another's namespace (§6.3)",
		},
		{
			field:  "hubID",
			tamper: func(a *syncFileArgs) { a.hubID = "other-hub" },
			attack: "replaying a captured request at a different hub sharing the secret",
		},
		{
			field:  "path",
			tamper: func(a *syncFileArgs) { a.path = "metrics/cpu/2026/08/07/14/other.parquet" },
			attack: "reusing a MAC for file A to write file B",
		},
		{
			field:  "sha256",
			tamper: func(a *syncFileArgs) { a.sha256 = strings.Repeat("a", 64) },
			attack: "smuggling different bytes under a MAC minted for the original content",
		},
		{
			field:  "nonce",
			tamper: func(a *syncFileArgs) { a.nonce = "0000000000000000000000000000000f" },
			attack: "detaching the request from its replay-cache slot",
		},
		{
			field:  "secret",
			tamper: func(a *syncFileArgs) { a.secret = "a-different-spokes-secret" },
			attack: "a revoked or foreign spoke authenticating",
		},
		{
			field:  "timestamp",
			tamper: func(a *syncFileArgs) { a.timestamp++ },
			attack: "shifting a request within the freshness window",
		},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			signed := validFileArgs()
			mac := signed.compute()

			tampered := signed
			tt.tamper(&tampered)

			if err := tampered.validate(mac); err == nil {
				t.Fatalf("MAC validated after %s was changed — enables %s", tt.field, tt.attack)
			}
		})
	}
}

func TestSyncFileHMAC_FreshnessWindow(t *testing.T) {
	tests := []struct {
		name    string
		offset  time.Duration
		wantErr bool
	}{
		{"now", 0, false},
		{"just inside the past window", -(HMACTimestampTolerance - time.Minute), false},
		{"just inside the future window", HMACTimestampTolerance - time.Minute, false},
		{"well past the window", -(HMACTimestampTolerance + time.Minute), true},
		// Symmetric on purpose: a badly-skewed edge clock produces
		// future-dated requests, and accepting them without bound would let a
		// captured MAC stay valid indefinitely.
		{"well into the future", HMACTimestampTolerance + time.Minute, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := validFileArgs()
			a.timestamp = time.Now().Add(tt.offset).Unix()
			err := a.validate(a.compute())

			if tt.wantErr {
				if err == nil {
					t.Fatalf("timestamp %v from now validated; want rejection", tt.offset)
				}
				if !errors.Is(err, ErrSyncAuthExpired) {
					t.Errorf("err = %v, want ErrSyncAuthExpired so an operator can tell skew from forgery", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("timestamp %v from now rejected: %v", tt.offset, err)
			}
		})
	}
}

func TestSyncFileHMAC_ErrorsAreDistinguishable(t *testing.T) {
	// Clock skew and a bad MAC need different operator responses — fix NTP
	// versus investigate a forgery — so they must not collapse into one error.
	t.Run("expired", func(t *testing.T) {
		a := validFileArgs()
		a.timestamp = time.Now().Add(-2 * HMACTimestampTolerance).Unix()
		err := a.validate(a.compute())
		if !errors.Is(err, ErrSyncAuthExpired) {
			t.Errorf("err = %v, want ErrSyncAuthExpired", err)
		}
		if errors.Is(err, ErrSyncAuthInvalid) {
			t.Error("an expired-but-authentic request reported as a MAC failure")
		}
	})

	t.Run("bad mac", func(t *testing.T) {
		a := validFileArgs()
		err := a.validate(mustFileMAC(t, "wrong-secret", a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp))
		if !errors.Is(err, ErrSyncAuthInvalid) {
			t.Errorf("err = %v, want ErrSyncAuthInvalid", err)
		}
	})
}

func TestSyncFileHMAC_MalformedMACFailsClosed(t *testing.T) {
	a := validFileArgs()
	// A non-hex or truncated MAC must be rejected outright rather than
	// panicking or, worse, comparing successfully against a decode error.
	for _, mac := range []string{
		"",
		"not-hex-at-all",
		"abc",                          // odd length
		"zz" + strings.Repeat("0", 62), // invalid hex digits
		strings.Repeat("0", 62),        // valid hex, wrong length
		a.compute() + "00",             // valid prefix, extra bytes
	} {
		if err := a.validate(mac); err == nil {
			t.Errorf("malformed MAC %q validated", mac)
		}
	}
}

func TestSyncReconcileHMAC_RoundTripAndBodyBinding(t *testing.T) {
	const (
		secret  = "shared-secret"
		nonce   = "5f3a9c1e2b7d4816a0c3e5f7d9b1a3c5"
		spokeID = "rocket-01"
		hubID   = "ground-station"
	)
	ts := time.Now().Unix()
	body := []byte(`[{"path":"a.parquet","sha256":"aa","size":1}]`)

	mac := mustReconcileMAC(t, secret, nonce, spokeID, hubID, body, ts)
	if err := ValidateSyncReconcileHMAC(secret, nonce, spokeID, hubID, body, ts, mac, HMACTimestampTolerance); err != nil {
		t.Fatalf("round trip failed: %v", err)
	}

	// Reconcile answers "which of these paths do you hold", so an unbound body
	// would let a replayed request probe an arbitrary path list — a
	// data-inventory oracle over the hub.
	probe := []byte(`[{"path":"someone-elses-secret.parquet","sha256":"bb","size":1}]`)
	if err := ValidateSyncReconcileHMAC(secret, nonce, spokeID, hubID, probe, ts, mac, HMACTimestampTolerance); err == nil {
		t.Fatal("a substituted path list validated under the original MAC — the body is not bound")
	}

	// A single flipped byte must invalidate it.
	altered := append([]byte(nil), body...)
	altered[len(altered)-2] ^= 0xFF
	if err := ValidateSyncReconcileHMAC(secret, nonce, spokeID, hubID, altered, ts, mac, HMACTimestampTolerance); err == nil {
		t.Fatal("a one-byte body change validated under the original MAC")
	}
}

func TestSyncReconcileHMAC_EmptyAndNilBodyAgree(t *testing.T) {
	const (
		secret  = "shared-secret"
		nonce   = "abcd"
		spokeID = "rocket-01"
		hubID   = "ground-station"
	)
	ts := time.Now().Unix()

	// A spoke with nothing pending sends an empty list. nil and []byte{} must
	// produce the same MAC, or "nothing to reconcile" fails depending on how
	// the caller happened to represent empty.
	nilMAC := mustReconcileMAC(t, secret, nonce, spokeID, hubID, nil, ts)
	emptyMAC := mustReconcileMAC(t, secret, nonce, spokeID, hubID, []byte{}, ts)
	if nilMAC != emptyMAC {
		t.Errorf("nil body MAC %q != empty body MAC %q", nilMAC, emptyMAC)
	}
	if err := ValidateSyncReconcileHMAC(secret, nonce, spokeID, hubID, nil, ts, emptyMAC, HMACTimestampTolerance); err != nil {
		t.Errorf("empty-body MAC failed to validate against nil body: %v", err)
	}
}

func TestSyncHMAC_DomainSeparation(t *testing.T) {
	const (
		secret  = "shared-secret"
		nonce   = "5f3a9c1e2b7d4816a0c3e5f7d9b1a3c5"
		spokeID = "rocket-01"
		hubID   = "ground-station"
		path    = "metrics/cpu/f.parquet"
		digest  = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	)
	ts := time.Now().Unix()

	t.Run("file MAC is not a reconcile MAC", func(t *testing.T) {
		// Both bind (spokeID, hubID, ..., ts) under the same secret. Without
		// the leading label they would be shape-compatible, and a MAC minted
		// for one operation could be presented as the other.
		fileMAC := mustFileMAC(t, secret, nonce, spokeID, hubID, path, digest, ts)
		body := []byte(path)
		if err := ValidateSyncReconcileHMAC(secret, nonce, spokeID, hubID, body, ts, fileMAC, HMACTimestampTolerance); err == nil {
			t.Error("a file MAC validated as a reconcile MAC")
		}

		reconcileMAC := mustReconcileMAC(t, secret, nonce, spokeID, hubID, body, ts)
		if err := ValidateSyncFileHMAC(secret, nonce, spokeID, hubID, path, digest, ts, reconcileMAC, HMACTimestampTolerance); err == nil {
			t.Error("a reconcile MAC validated as a file MAC")
		}
	})

	t.Run("sync MAC is not a cluster fetch MAC", func(t *testing.T) {
		// ComputeFetchHMAC's input is `nonce \x00 nodeID \x00 clusterName \x00
		// path \x00 timestamp` — five NUL-delimited fields with a path in the
		// fourth slot. A sync file MAC without its label would be the same
		// shape, so a deployment sharing a secret between the cluster and a
		// spoke could cross the two. The label makes them unrelated.
		fetchMAC := ComputeFetchHMAC(secret, nonce, spokeID, hubID, path, ts)
		if err := ValidateSyncFileHMAC(secret, nonce, spokeID, hubID, path, digest, ts, fetchMAC, HMACTimestampTolerance); err == nil {
			t.Error("a cluster fetch MAC validated as a sync file MAC")
		}

		syncMAC := mustFileMAC(t, secret, nonce, spokeID, hubID, path, digest, ts)
		if err := ValidateFetchHMAC(secret, nonce, spokeID, hubID, path, ts, syncMAC, HMACTimestampTolerance); err == nil {
			t.Error("a sync file MAC validated as a cluster fetch MAC")
		}
	})
}

func TestSyncHMAC_FieldSmugglingIsPrevented(t *testing.T) {
	const (
		secret = "shared-secret"
		nonce  = "abcd"
		digest = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	)
	ts := time.Now().Unix()

	// Fields are NUL-delimited so no two DIFFERENT field arrangements can
	// produce the same signed input. The pairs below concatenate identically
	// once the delimiters are removed, and none of them contains a NUL itself
	// — so this fails if and only if the delimiters are doing the work.
	//
	// A test case that injects a literal NUL into a field is USELESS here: the
	// injected NUL survives into the concatenation whether or not the format
	// string supplies delimiters, so the two inputs differ under both the
	// correct and the broken implementation and the assertion holds either
	// way. An earlier version of this test did exactly that, and mutation
	// testing showed a delimiter-free build passing it. The pairs must instead
	// be NUL-free and rely on the boundary alone.
	tests := []struct {
		name string
		a, b func() string
	}{
		{
			name: "spokeID/hubID boundary",
			a:    func() string { return mustFileMACNoT(secret, nonce, "rocket", "01", "p", digest, ts) },
			b:    func() string { return mustFileMACNoT(secret, nonce, "rocket01", "", "p", digest, ts) },
		},
		{
			name: "hubID/path boundary",
			a:    func() string { return mustFileMACNoT(secret, nonce, "s", "hub", "path", digest, ts) },
			b:    func() string { return mustFileMACNoT(secret, nonce, "s", "hubpath", "", digest, ts) },
		},
		{
			name: "nonce/spokeID boundary",
			a:    func() string { return mustFileMACNoT(secret, "ab", "cd", "h", "p", digest, ts) },
			b:    func() string { return mustFileMACNoT(secret, "abcd", "", "h", "p", digest, ts) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.a() == tt.b() {
				t.Error("two different field arrangements produced the same MAC — a field can be smuggled across the boundary")
			}
		})
	}

	ra := mustReconcileMACNoT(secret, nonce, "rocket", "01", nil, ts)
	rb := mustReconcileMACNoT(secret, nonce, "rocket01", "", nil, ts)
	if ra == rb {
		t.Error("reconcile: the spokeID/hubID boundary is not delimited")
	}
}

func TestSyncHMAC_LabelIsBoundAndCorrect(t *testing.T) {
	// Golden-value tests pinning each MAC's exact canonical input.
	//
	// A differential test ("the two MACs differ") cannot detect a wrong label:
	// the file MAC has seven fields and reconcile has six, so they stay
	// distinct even if both used the SAME label or neither used one. The
	// realistic bug is a copy-paste — a new sync MAC added later reusing
	// syncLabelFile — and only pinning the value catches it.
	//
	// If a change here breaks these, the canonical input changed, which is a
	// WIRE-COMPATIBILITY BREAK: every deployed spoke's MACs stop validating.
	// Update the constants only alongside a deliberate protocol version bump.
	const (
		secret  = "test-secret"
		nonce   = "test-nonce"
		spokeID = "test-spoke"
		hubID   = "test-hub"
		path    = "test/path.parquet"
		digest  = "test-sha256"
		ts      = int64(1750000000)
	)

	t.Run("file", func(t *testing.T) {
		// Length-prefixed: len(field) + ":" + field, concatenated.
		want := hex.EncodeToString(computeRawHMAC(secret,
			"9:sync-file"+
				"10:"+nonce+
				"10:"+spokeID+
				"8:"+hubID+
				"17:"+path+
				"11:"+digest+
				"10:1750000000"))
		got := mustFileMAC(t, secret, nonce, spokeID, hubID, path, digest, ts)
		if got != want {
			t.Errorf("canonical input changed.\n got %s\nwant %s\nThe label must be %q and fields length-prefixed in order: label, nonce, spokeID, hubID, path, contentSHA256, timestamp",
				got, want, "sync-file")
		}
	})

	t.Run("reconcile", func(t *testing.T) {
		body := []byte("test-body")
		sum := sha256.Sum256(body)
		want := hex.EncodeToString(computeRawHMAC(secret,
			"14:sync-reconcile"+
				"10:"+nonce+
				"10:"+spokeID+
				"8:"+hubID+
				"64:"+hex.EncodeToString(sum[:])+
				"10:1750000000"))
		got := mustReconcileMAC(t, secret, nonce, spokeID, hubID, body, ts)
		if got != want {
			t.Errorf("canonical input changed.\n got %s\nwant %s\nThe label must be %q and fields length-prefixed in order: label, nonce, spokeID, hubID, bodySHA256, timestamp",
				got, want, "sync-reconcile")
		}
	})

	t.Run("labels are distinct", func(t *testing.T) {
		// Guards the copy-paste where a new MAC reuses an existing label.
		if syncLabelFile == syncLabelReconcile {
			t.Error("the two sync domain labels are identical — cross-operation replay is possible")
		}
	})
}

func TestSyncNonceKey_IsPerSpoke(t *testing.T) {
	// NonceCache keys on (id, nonce). The id must be the spoke, so one
	// spoke's nonces cannot collide with or evict another's.
	a, err := SyncNonceKey("rocket-01")
	if err != nil {
		t.Fatalf("SyncNonceKey: %v", err)
	}
	b, err := SyncNonceKey("rocket-02")
	if err != nil {
		t.Fatalf("SyncNonceKey: %v", err)
	}
	if a == b {
		t.Error("two spokes share a nonce-cache identity")
	}

	// Namespaced so a sync nonce cannot collide with a cluster node ID that
	// happens to match a spoke ID.
	if !strings.HasPrefix(a, "sync:") {
		t.Error("sync nonce keys are not namespaced away from cluster node IDs")
	}
}

func TestSyncHMAC_ReplayNeedsTheNonceCache(t *testing.T) {
	// This documents a real limitation as an executable assertion: an
	// unmodified replay inside the freshness window DOES validate, because
	// every bound field is unchanged. Freshness alone is not replay
	// protection — the nonce cache is, which is why handlers must Track after
	// validating.
	a := validFileArgs()
	mac := a.compute()

	if err := a.validate(mac); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	if err := a.validate(mac); err != nil {
		t.Fatalf("replay unexpectedly rejected by the MAC alone: %v", err)
	}

	// With the cache in front, the second delivery is refused.
	cache := NewNonceCache(HMACTimestampTolerance)
	key, err := SyncNonceKey(a.spokeID)
	if err != nil {
		t.Fatalf("SyncNonceKey: %v", err)
	}
	if !cache.Track(key, a.nonce) {
		t.Fatal("first Track reported a replay")
	}
	if cache.Track(key, a.nonce) {
		t.Error("the nonce cache accepted a replayed nonce")
	}

	// A different spoke reusing the same nonce value must not be blocked by
	// the first spoke's entry.
	other, err := SyncNonceKey("rocket-02")
	if err != nil {
		t.Fatalf("SyncNonceKey: %v", err)
	}
	if !cache.Track(other, a.nonce) {
		t.Error("one spoke's nonce blocked another spoke's identical nonce")
	}
}

func TestSyncHMAC_EmptySecretStillProducesDistinctMACs(t *testing.T) {
	// An empty secret must not be silently equivalent to a populated one.
	// Callers are expected to reject empty secrets before reaching here
	// (spoke registration requires one), but the primitive must not make an
	// unconfigured deployment accidentally interoperable.
	a := validFileArgs()
	empty := a
	empty.secret = ""

	if empty.compute() == a.compute() {
		t.Error("an empty secret produced the same MAC as a real one")
	}
	if err := a.validate(empty.compute()); err == nil {
		t.Error("a MAC computed with an empty secret validated against the real secret")
	}
}

func TestSyncReconcileHMAC_FreshnessWindow(t *testing.T) {
	// The reconcile path needs its own freshness coverage. Every other
	// reconcile test signs with time.Now(), so deleting the freshness check
	// from ValidateSyncReconcileHMAC left the whole suite green — and a
	// captured reconcile request would then replay indefinitely, which is the
	// data-inventory oracle the body binding exists to prevent.
	const (
		secret  = "shared-secret"
		nonce   = "5f3a9c1e2b7d4816a0c3e5f7d9b1a3c5"
		spokeID = "rocket-01"
		hubID   = "ground-station"
	)
	body := []byte(`[{"path":"a.parquet","sha256":"aa","size":1}]`)

	tests := []struct {
		name    string
		offset  time.Duration
		wantErr bool
	}{
		{"now", 0, false},
		{"just inside the past window", -(HMACTimestampTolerance - time.Minute), false},
		{"just inside the future window", HMACTimestampTolerance - time.Minute, false},
		{"well past the window", -(HMACTimestampTolerance + time.Minute), true},
		{"well into the future", HMACTimestampTolerance + time.Minute, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := time.Now().Add(tt.offset).Unix()
			mac := mustReconcileMAC(t, secret, nonce, spokeID, hubID, body, ts)
			err := ValidateSyncReconcileHMAC(secret, nonce, spokeID, hubID, body, ts, mac, HMACTimestampTolerance)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("timestamp %v from now validated; want rejection", tt.offset)
				}
				if !errors.Is(err, ErrSyncAuthExpired) {
					t.Errorf("err = %v, want ErrSyncAuthExpired", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("timestamp %v from now rejected: %v", tt.offset, err)
			}
		})
	}
}

func TestSyncHMAC_NULFieldsAreRejected(t *testing.T) {
	// The delimiter must never reach the canonical input. A NUL inside any
	// field lets an attacker re-partition it — five fields become seven — and
	// a MAC from a DIFFERENT family verifies here. ComputeFetchHMAC is the
	// live example: its canonical input has no domain label, and it travels
	// over the raw TCP coordinator protocol as JSON, which carries NUL fine.
	// Setting the fetch nonce to "sync-file" and embedding NULs in its nodeID
	// and path reproduces a sync file MAC over attacker-chosen spokeID and
	// hubID — a cross-spoke namespace write, the property §6.3 depends on.
	const (
		secret = "shared-cluster-secret"
		digest = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	)
	ts := time.Now().Unix()

	t.Run("compute rejects NUL in any field", func(t *testing.T) {
		fields := []struct {
			name                                     string
			nonce, spokeID, hubID, path, contentHash string
		}{
			{"nonce", "n\x00x", "s", "h", "p", digest},
			{"spokeID", "n", "s\x00x", "h", "p", digest},
			{"hubID", "n", "s", "h\x00x", "p", digest},
			{"path", "n", "s", "h", "p\x00x", digest},
			{"contentSHA256", "n", "s", "h", "p", digest + "\x00x"},
		}
		for _, f := range fields {
			t.Run(f.name, func(t *testing.T) {
				if _, err := ComputeSyncFileHMAC(secret, f.nonce, f.spokeID, f.hubID, f.path, f.contentHash, ts); !errors.Is(err, ErrSyncAuthMalformedField) {
					t.Errorf("err = %v, want ErrSyncAuthMalformedField", err)
				}
			})
		}

		if _, err := ComputeSyncReconcileHMAC(secret, "n", "s\x00x", "h", nil, ts); !errors.Is(err, ErrSyncAuthMalformedField) {
			t.Errorf("reconcile: err = %v, want ErrSyncAuthMalformedField", err)
		}
	})

	t.Run("validate rejects NUL before checking the MAC", func(t *testing.T) {
		// Rejected on the validate side too: an attacker computes the MAC
		// elsewhere, so guarding only the compute path would not help.
		if err := ValidateSyncFileHMAC(secret, "n", "s\x00x", "h", "p", digest, ts, "deadbeef", HMACTimestampTolerance); !errors.Is(err, ErrSyncAuthMalformedField) {
			t.Errorf("err = %v, want ErrSyncAuthMalformedField", err)
		}
		if err := ValidateSyncReconcileHMAC(secret, "n", "s\x00x", "h", nil, ts, "deadbeef", HMACTimestampTolerance); !errors.Is(err, ErrSyncAuthMalformedField) {
			t.Errorf("reconcile: err = %v, want ErrSyncAuthMalformedField", err)
		}
	})

	t.Run("cross-family forgery is closed", func(t *testing.T) {
		// The actual attack, end to end.
		fetchMAC := ComputeFetchHMAC(secret, "sync-file", "nonce\x00spoke", "hub", "path\x00sha", ts)
		if err := ValidateSyncFileHMAC(secret, "nonce", "spoke", "hub", "path", "sha", ts, fetchMAC, HMACTimestampTolerance); err == nil {
			t.Error("a cluster fetch MAC validated as a sync file MAC — cross-spoke namespace write is possible")
		}

		body := []byte("the reconcile body")
		fwdMAC := ComputeForwardHMAC(secret, "sync-reconcile", "nonce\x00spoke", "hub", body, ts)
		if err := ValidateSyncReconcileHMAC(secret, "nonce", "spoke", "hub", body, ts, fwdMAC, HMACTimestampTolerance); err == nil {
			t.Error("a cluster forward MAC validated as a sync reconcile MAC")
		}
	})

	t.Run("body may contain NUL", func(t *testing.T) {
		// The reconcile body is hashed to a fixed-length hex digest before
		// entering the canonical input, so its bytes cannot re-partition it.
		// Rejecting NUL there would break legitimate binary payloads.
		body := []byte("path\x00with\x00nuls")
		mac, err := ComputeSyncReconcileHMAC(secret, "n", "s", "h", body, ts)
		if err != nil {
			t.Fatalf("a NUL in the body was rejected: %v", err)
		}
		if err := ValidateSyncReconcileHMAC(secret, "n", "s", "h", body, ts, mac, HMACTimestampTolerance); err != nil {
			t.Errorf("round trip with a NUL-containing body failed: %v", err)
		}
	})
}

func TestSyncHMAC_WithReplayConsumesTheNonce(t *testing.T) {
	// The wrapper exists because the split version is a footgun: a handler
	// that validates and forgets to Track is fully replayable for the whole
	// freshness window, and no test would notice.
	a := validFileArgs()
	mac := a.compute()
	cache := NewNonceCache(HMACTimestampTolerance)

	if err := ValidateSyncFileHMACWithReplay(cache, a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp, mac, HMACTimestampTolerance); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	err := ValidateSyncFileHMACWithReplay(cache, a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp, mac, HMACTimestampTolerance)
	if !errors.Is(err, ErrSyncAuthReplay) {
		t.Errorf("replay: err = %v, want ErrSyncAuthReplay", err)
	}

	// A replay must be distinguishable from a forgery — the request is
	// authentic, which is what makes it worth alerting on.
	if errors.Is(err, ErrSyncAuthInvalid) {
		t.Error("a replay was reported as a MAC failure")
	}
}

func TestSyncHMAC_WithReplayDoesNotBurnSlotsOnForgery(t *testing.T) {
	// A forged request must not consume the nonce. Otherwise an attacker who
	// can guess or observe a nonce locks out the legitimate request carrying
	// it, turning replay protection into a denial-of-service primitive.
	a := validFileArgs()
	cache := NewNonceCache(HMACTimestampTolerance)

	forged := "0000000000000000000000000000000000000000000000000000000000000000"
	if err := ValidateSyncFileHMACWithReplay(cache, a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp, forged, HMACTimestampTolerance); !errors.Is(err, ErrSyncAuthInvalid) {
		t.Fatalf("forged MAC: err = %v, want ErrSyncAuthInvalid", err)
	}

	// The genuine request carrying that same nonce must still be accepted.
	if err := ValidateSyncFileHMACWithReplay(cache, a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp, a.compute(), HMACTimestampTolerance); err != nil {
		t.Errorf("a forged attempt burned the nonce slot: %v", err)
	}
}

func TestSyncHMAC_WithReplayRequiresAGuard(t *testing.T) {
	// Fail closed: a nil guard must be an error, never a silent skip of
	// replay protection.
	a := validFileArgs()
	if err := ValidateSyncFileHMACWithReplay(nil, a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256, a.timestamp, a.compute(), HMACTimestampTolerance); err == nil {
		t.Error("a nil replay guard was accepted")
	}
	if err := ValidateSyncReconcileHMACWithReplay(nil, a.secret, a.nonce, a.spokeID, a.hubID, nil, a.timestamp, "x", HMACTimestampTolerance); err == nil {
		t.Error("reconcile: a nil replay guard was accepted")
	}
}

func TestSyncHMAC_ReconcileWithReplayConsumesTheNonce(t *testing.T) {
	const (
		secret  = "shared-secret"
		nonce   = "5f3a9c1e2b7d4816a0c3e5f7d9b1a3c5"
		spokeID = "rocket-01"
		hubID   = "ground-station"
	)
	ts := time.Now().Unix()
	body := []byte(`[{"path":"a.parquet","sha256":"aa","size":1}]`)
	mac := mustReconcileMAC(t, secret, nonce, spokeID, hubID, body, ts)
	cache := NewNonceCache(HMACTimestampTolerance)

	if err := ValidateSyncReconcileHMACWithReplay(cache, secret, nonce, spokeID, hubID, body, ts, mac, HMACTimestampTolerance); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := ValidateSyncReconcileHMACWithReplay(cache, secret, nonce, spokeID, hubID, body, ts, mac, HMACTimestampTolerance); !errors.Is(err, ErrSyncAuthReplay) {
		t.Errorf("replay: err = %v, want ErrSyncAuthReplay", err)
	}
}

func TestSyncNonceKey_NULCannotCollide(t *testing.T) {
	// NonceCache.Track builds `id \x00 nonce`, so an unsanitized NUL in
	// spokeID makes two different (spoke, nonce) pairs share a cache key —
	// letting one spoke consume another's replay slot.
	// A NUL-bearing spoke ID is refused outright rather than escaped:
	// percent-encoding is not injective unless the escape character is also
	// escaped, which would trade one collision for another.
	if _, err := SyncNonceKey("spoke\x00extra"); !errors.Is(err, ErrSyncAuthMalformedField) {
		t.Errorf("err = %v, want ErrSyncAuthMalformedField", err)
	}

	key, err := SyncNonceKey("spoke")
	if err != nil {
		t.Fatalf("a well-formed spoke ID was rejected: %v", err)
	}
	cache := NewNonceCache(HMACTimestampTolerance)
	if !cache.Track(key, "nonce") {
		t.Error("first Track reported a replay")
	}
}

func TestSyncHMAC_SubSecondToleranceIsRejected(t *testing.T) {
	// Timestamps are second-granularity, so int64(tolerance.Seconds())
	// truncates anything under a second to zero and rejects every request.
	// That fails closed, but silently — a caller passing 500ms would see all
	// traffic denied with an "expired" error and no clue why.
	a := validFileArgs()
	err := ValidateSyncFileHMAC(a.secret, a.nonce, a.spokeID, a.hubID, a.path, a.sha256,
		a.timestamp, a.compute(), 500*time.Millisecond)
	if err == nil {
		t.Fatal("a sub-second tolerance was accepted")
	}
	if errors.Is(err, ErrSyncAuthExpired) {
		t.Error("a sub-second tolerance reported as an expired timestamp; it is a configuration error")
	}

	// Exactly one second is the smallest meaningful value and must work.
	fresh := validFileArgs()
	if err := ValidateSyncFileHMAC(fresh.secret, fresh.nonce, fresh.spokeID, fresh.hubID, fresh.path, fresh.sha256,
		fresh.timestamp, fresh.compute(), time.Second); err != nil {
		t.Errorf("a one-second tolerance was rejected: %v", err)
	}
}
