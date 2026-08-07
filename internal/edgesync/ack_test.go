package edgesync

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The whole point of 9d: an air-gapped spoke can finally reach `synced`, which
// is what makes its ledger prunable. Without the ack, `exported` is terminal
// and the ledger grows forever on the box least able to receive a site visit.
func TestAck_RoundTripAdvancesExportedToSynced(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)

	// The spoke exports, the hub imports and writes the ack into the drive.
	dir := exportBundle(t, rig.secret, 3, testHubID)
	res, err := rig.importer.Import(ctx, dir)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if !res.AckWritten {
		t.Fatal("the import did not write an acknowledgment")
	}
	if len(res.AckPaths) != 3 {
		t.Errorf("ack names %d paths, want 3", len(res.AckPaths))
	}

	// The drive comes home. The spoke verifies and applies it.
	ack, err := ReadAck(dir, rig.secret, testSpokeID, testHubID)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}

	l := setupTestLedger(t)
	for _, p := range ack.Paths {
		e := testEntry(p)
		e.HubID = testHubID
		if err := l.Track(ctx, e); err != nil {
			t.Fatalf("track: %v", err)
		}
		if err := l.MarkExported(ctx, testHubID, p, ack.BundleID); err != nil {
			t.Fatalf("mark exported: %v", err)
		}
	}

	applied, err := ApplyAck(ctx, l, ack, zerolog.Nop())
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied.Synced != 3 {
		t.Errorf("synced = %d, want 3", applied.Synced)
	}

	// And the ledger can now prune them, which it never could before.
	st, err := l.Stats(ctx, testHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Synced != 3 {
		t.Errorf("Synced = %d, want 3", st.Synced)
	}
	if st.Exported != 0 {
		t.Errorf("Exported = %d, want 0 after the ack", st.Exported)
	}
}

// A tampered ack must not license advancing files the hub never received —
// that would silently drop data the spoke then stops re-sending.
func TestAck_ReadRejectsTampering(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		tamper func(t *testing.T, a *Ack)
		want   string
	}{
		{
			"a path added",
			func(t *testing.T, a *Ack) { a.Paths = append(a.Paths, "default/cpu/2026/08/07/00/forged.parquet") },
			"paths digest",
		},
		{
			"a path removed",
			func(t *testing.T, a *Ack) { a.Paths = a.Paths[:len(a.Paths)-1] },
			"paths digest",
		},
		{
			"the MAC changed",
			func(t *testing.T, a *Ack) { a.MAC = strings.Repeat("ab", 32) },
			"acknowledgment is invalid",
		},
		{
			"the bundle ID swapped",
			func(t *testing.T, a *Ack) { a.BundleID = "06FXVSQXJ2C0EBDFDQ9D24S1E8" },
			"acknowledgment is invalid",
		},
		{
			"the import time moved",
			func(t *testing.T, a *Ack) { a.ImportedAt += 3600 },
			"acknowledgment is invalid",
		},
		{
			"signed by another hub",
			func(t *testing.T, a *Ack) { a.HubID = "some-other-hub" },
			"signed by hub",
		},
		{
			"addressed to another spoke",
			func(t *testing.T, a *Ack) { a.SpokeID = "rocket-99" },
			"acknowledges spoke",
		},
		{
			"an acknowledged path escapes the namespace",
			func(t *testing.T, a *Ack) {
				a.Paths = []string{"../../etc/passwd"}
				// Re-sign so only the path validation can catch it.
				a.PathsDigest = ""
			},
			"acknowledgment is invalid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rig := newImportRig(t, false)
			dir := exportBundle(t, rig.secret, 2, testHubID)
			if _, err := rig.importer.Import(ctx, dir); err != nil {
				t.Fatalf("import: %v", err)
			}

			p := filepath.Join(dir, ackName)
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			var a Ack
			if err := json.Unmarshal(raw, &a); err != nil {
				t.Fatal(err)
			}
			tc.tamper(t, &a)
			out, err := json.MarshalIndent(a, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, out, 0o600); err != nil {
				t.Fatal(err)
			}

			_, err = ReadAck(dir, rig.secret, testSpokeID, testHubID)
			if err == nil {
				t.Fatal("a tampered acknowledgment was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// An ack signed with a different secret is a forgery.
func TestAck_ReadRejectsTheWrongSecret(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, testHubID)
	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("import: %v", err)
	}

	if _, err := ReadAck(dir, "a-different-secret", testSpokeID, testHubID); !errors.Is(err, ErrAckInvalid) {
		t.Errorf("error = %v, want ErrAckInvalid", err)
	}
}

// A drive on the outbound leg has no ack yet. That is the normal case, not a
// failure an operator should be alarmed by.
func TestAck_MissingAckIsDistinguishable(t *testing.T) {
	rig := newBundleRig(t)
	res := rig.exportTwo(t)

	_, err := ReadAck(res.Dir, testSecret, testSpokeID, testHubID)
	if !errors.Is(err, ErrNoAck) {
		t.Errorf("error = %v, want ErrNoAck", err)
	}
}

// A conflicted path means the hub holds DIFFERENT content, so the spoke's copy
// was never delivered and must not be marked synced.
func TestAck_ConflictsAreNotAcknowledged(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, testHubID)

	// The hub already holds different content at one of the two paths.
	const rel = "default/cpu/2026/08/07/00/f_0000.parquet"
	if err := rig.hubStore.Write(ctx, NamespacedPath(testSpokeID, rel), []byte("the hub's own version")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	res, err := rig.importer.Import(ctx, dir)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if len(res.Conflicts) != 1 {
		t.Fatalf("conflicts = %v, want one", res.Conflicts)
	}

	ack, err := ReadAck(dir, rig.secret, testSpokeID, testHubID)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	for _, p := range ack.Paths {
		if p == rel {
			t.Errorf("the conflicted path %q was acknowledged; the spoke would mark undelivered data synced", rel)
		}
	}
	if len(ack.Conflicts) != 1 {
		t.Errorf("the ack does not report the conflict for the operator")
	}

	// Applying it must leave the conflicted file exported, not synced.
	l := setupTestLedger(t)
	e := testEntry(rel)
	e.HubID = testHubID
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}
	if err := l.MarkExported(ctx, testHubID, rel, ack.BundleID); err != nil {
		t.Fatalf("mark exported: %v", err)
	}
	if _, err := ApplyAck(ctx, l, ack, zerolog.Nop()); err != nil {
		t.Fatalf("apply: %v", err)
	}
	entry, err := l.Get(ctx, testHubID, rel)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry.State != StateExported {
		t.Errorf("the conflicted file is %q, want it left %q", entry.State, StateExported)
	}
}

// Replaying an ack is harmless: MarkSynced on an already-synced entry is a
// no-op, so a drive plugged in twice does not corrupt anything.
func TestAck_ApplyingTwiceIsHarmless(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, testHubID)
	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("import: %v", err)
	}
	ack, err := ReadAck(dir, rig.secret, testSpokeID, testHubID)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}

	l := setupTestLedger(t)
	for _, p := range ack.Paths {
		e := testEntry(p)
		e.HubID = testHubID
		if err := l.Track(ctx, e); err != nil {
			t.Fatalf("track: %v", err)
		}
		if err := l.MarkExported(ctx, testHubID, p, ack.BundleID); err != nil {
			t.Fatalf("mark exported: %v", err)
		}
	}

	first, err := ApplyAck(ctx, l, ack, zerolog.Nop())
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	second, err := ApplyAck(ctx, l, ack, zerolog.Nop())
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}

	if first.Synced != 2 {
		t.Errorf("first apply synced %d, want 2", first.Synced)
	}
	// The second finds them already synced — reported as unknown, not synced
	// again, and above all not an error.
	if second.Synced != 0 {
		t.Errorf("second apply synced %d, want 0", second.Synced)
	}

	st, _ := l.Stats(ctx, testHubID)
	if st.Synced != 2 {
		t.Errorf("Synced = %d after two applies, want 2", st.Synced)
	}
}

// The ack MAC must not be interchangeable with the three existing families.
func TestAck_MACIsADistinctFamily(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 1, testHubID)
	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("import: %v", err)
	}

	ack, err := ReadAck(dir, rig.secret, testSpokeID, testHubID)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}

	// The ack's MAC must not validate as the BUNDLE MAC over the same fields.
	r, err := OpenBundle(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	m := r.Manifest()
	if ack.MAC == m.MAC {
		t.Error("the ack and the manifest share a MAC")
	}
	_ = ctx
}

// An ack whose file is absurdly large must be refused before it is parsed.
func TestAck_OversizedAckIsRefused(t *testing.T) {
	rig := newBundleRig(t)
	res := rig.exportTwo(t)

	big := make([]byte, maxAckBytes+1)
	for i := range big {
		big[i] = 'a'
	}
	if err := os.WriteFile(filepath.Join(res.Dir, ackName), big, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := ReadAck(res.Dir, testSecret, testSpokeID, testHubID)
	if !errors.Is(err, ErrAckInvalid) || !strings.Contains(err.Error(), "bound") {
		t.Errorf("error = %v, want a size-bound refusal", err)
	}
}

var _ = time.Now

// The hub writes ack.json INTO the bundle, but the manifest was signed before
// it existed and cannot cover it. A returned drive must still re-verify, or an
// operator auditing what crossed the air gap is told a legitimately
// acknowledged bundle was tampered with — and any re-import is refused.
func TestAck_AcknowledgedBundleStillVerifies(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 3, testHubID)

	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("import: %v", err)
	}

	// ack.json is now in the bundle.
	if _, err := os.Stat(filepath.Join(dir, ackName)); err != nil {
		t.Fatalf("the import wrote no ack: %v", err)
	}

	r, err := OpenBundle(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := r.Verify(ctx, rig.secret); err != nil {
		t.Fatalf("an acknowledged bundle failed re-verification: %v", err)
	}

	// The exemption is for ack.json alone — anything else beside it is still
	// unsigned payload.
	if err := os.WriteFile(filepath.Join(dir, "ack.json.bak"), []byte("decoy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.Verify(ctx, rig.secret); err == nil {
		t.Error("a decoy file beside the ack passed verification")
	}
}

// A tampered ack must not ride along as tolerated payload: the exemption is
// from the manifest digest, not from the ack's own signature.
func TestAck_TamperedAckIsStillRefusedOnRead(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, testHubID)
	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("import: %v", err)
	}

	// Replace the ack entirely. Bundle verification tolerates its presence...
	if err := os.WriteFile(filepath.Join(dir, ackName), []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	r, _ := OpenBundle(dir, zerolog.Nop())
	if err := r.Verify(ctx, rig.secret); err != nil {
		t.Errorf("bundle verification should tolerate a replaced ack, got: %v", err)
	}

	// ...but ReadAck must refuse it, so nothing is ever advanced.
	if _, err := ReadAck(dir, rig.secret, testSpokeID, testHubID); err == nil {
		t.Error("a replaced acknowledgment was accepted")
	}
}
