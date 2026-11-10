package edgesync

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"
)

// #1561: SpokeNamespace is the (database, measurement) a spoke's file is
// served under, which RBAC is checked against. It must name exactly the
// segments the file lands under.
func TestSpokeNamespace(t *testing.T) {
	for _, c := range []struct {
		src     string
		meas    string
		wantErr bool
		shallow bool
	}{
		{"metrics/cpu/2026/08/07/14/a.parquet", "metrics", false, false},
		{"metrics/cpu/2026/08/07/a_daily.parquet", "metrics", false, false},
		{"x.parquet", "", true, true},
		{"../escape/a.parquet", "", true, false},
		{"/metrics/cpu/a.parquet", "", true, false},
		{"metrics//cpu/a.parquet", "", true, false},
		{"", "", true, false},
	} {
		db, meas, err := SpokeNamespace("rocket-01", c.src)
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: want an error, got (%q, %q)", c.src, db, meas)
			}
			if got := errors.Is(err, ErrSpokePathNoMeasurement); got != c.shallow {
				t.Errorf("%q: ErrSpokePathNoMeasurement = %v, want %v (%v)", c.src, got, c.shallow, err)
			}
			continue
		}
		if err != nil || db != "rocket-01" || meas != c.meas {
			t.Errorf("%q: (%q, %q, %v), want (rocket-01, %q)", c.src, db, meas, err, c.meas)
			continue
		}
		// The checked pair is where the file lands.
		segs := strings.Split(NamespacedPath("rocket-01", c.src), "/")
		if segs[0] != db || segs[1] != meas || path.Join(segs...) != NamespacedPath("rocket-01", c.src) {
			t.Errorf("%q: checked %s/%s, file lands at %s", c.src, db, meas, NamespacedPath("rocket-01", c.src))
		}
	}
	if _, _, err := SpokeNamespace("../x", "metrics/cpu/a.parquet"); err == nil {
		t.Error("an invalid spoke ID was accepted")
	}
}

// ReconcileAllowed answers a denied entry as missing without looking it up,
// keeps answering the permitted ones, and validates every entry first.
func TestReconcileAllowed(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)
	granted := ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/a.parquet", SHA256: sha256Hex([]byte("a"))}
	hidden := ReconcileEntry{Path: "logs/app/2026/08/07/14/b.parquet", SHA256: sha256Hex([]byte("b"))}
	seedFile(t, idx, backend, "rocket-01", granted.Path, granted.SHA256)
	seedFile(t, idx, backend, "rocket-01", hidden.Path, hidden.SHA256)
	onlyMetrics := func(p string) (bool, error) { return strings.HasPrefix(p, "metrics/"), nil }

	res, err := rec.ReconcileAllowed(ctx, "rocket-01", []ReconcileEntry{granted, hidden}, onlyMetrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Validate(); err != nil {
		t.Fatalf("invalid result: %v", err)
	}
	if len(res.Present) != 1 || res.Present[0] != granted.Path {
		t.Errorf("present = %v, want [%s]", res.Present, granted.Path)
	}
	if len(res.Missing) != 1 || res.Missing[0] != hidden.Path {
		t.Errorf("missing = %v, want the denied entry [%s]", res.Missing, hidden.Path)
	}

	// Validation still covers a denied entry: a malformed batch is refused,
	// not half-answered.
	bad := ReconcileEntry{Path: "logs/../x.parquet", SHA256: sha256Hex([]byte("x"))}
	if _, err := rec.ReconcileAllowed(ctx, "rocket-01", []ReconcileEntry{granted, bad}, onlyMetrics); err == nil {
		t.Error("a malformed denied entry was accepted")
	}
	dup := []ReconcileEntry{hidden, hidden}
	if _, err := rec.ReconcileAllowed(ctx, "rocket-01", dup, onlyMetrics); err == nil {
		t.Error("a duplicated denied entry was accepted")
	}

	// A denied entry's stale index row (no file behind it) is not forgotten:
	// the stale sweep runs only over permitted entries.
	stale := ReconcileEntry{Path: "logs/app/2026/08/07/14/stale.parquet", SHA256: sha256Hex([]byte("s"))}
	recordFile(t, idx, "rocket-01", stale.Path, stale.SHA256)
	if _, err := rec.ReconcileAllowed(ctx, "rocket-01", []ReconcileEntry{granted, stale}, onlyMetrics); err != nil {
		t.Fatal(err)
	}
	if held, err := idx.Lookup(ctx, "rocket-01", []string{stale.Path}); err != nil || len(held) != 1 {
		t.Errorf("denied stale row: held %v, %v; want it kept", held, err)
	}

	// A decision that cannot be made fails the whole reconcile.
	boom := errors.New("store down")
	if _, err := rec.ReconcileAllowed(ctx, "rocket-01", []ReconcileEntry{granted}, func(string) (bool, error) { return false, boom }); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the allow error", err)
	}
}
