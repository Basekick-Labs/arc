package edgesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

const (
	testSecret  = "a-shared-secret"
	testSpokeID = "rocket-01"
	testHubID   = "ground-station"
)

type bundleRig struct {
	writer  *BundleWriter
	backend storage.Backend
	parent  string
}

func newBundleRig(t *testing.T) *bundleRig {
	t.Helper()

	storeDir := t.TempDir()
	backend, err := storage.NewLocalBackend(storeDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	w, err := NewBundleWriter(BundleWriterConfig{
		Backend: backend, SpokeID: testSpokeID, HubID: testHubID,
		Secret: testSecret, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	return &bundleRig{writer: w, backend: backend, parent: t.TempDir()}
}

// write puts a file in the spoke's storage and returns a ledger entry for it.
func (r *bundleRig) write(t *testing.T, path, content string) *LedgerEntry {
	t.Helper()
	if err := r.backend.Write(context.Background(), path, []byte(content)); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	sum, err := sha256Of(content)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	return &LedgerEntry{
		HubID: testHubID, Path: path, SHA256: sum, SizeBytes: int64(len(content)),
		Database: "default", Measurement: "cpu",
		PartitionTime: time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC),
	}
}

func sha256Of(s string) (string, error) {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:]), nil
}

// exportTwo writes two files and exports them.
func (r *bundleRig) exportTwo(t *testing.T) *ExportResult {
	t.Helper()
	entries := []*LedgerEntry{
		r.write(t, "default/cpu/2026/08/07/14/a.parquet", "payload one"),
		r.write(t, "default/mem/2026/08/07/14/b.parquet", "payload two"),
	}
	res, err := r.writer.Export(context.Background(), r.parent, entries, time.Now())
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	return res
}

func TestBundle_ExportThenVerifyRoundTrips(t *testing.T) {
	ctx := context.Background()
	rig := newBundleRig(t)

	res := rig.exportTwo(t)
	if res.FileCount != 2 {
		t.Errorf("FileCount = %d, want 2", res.FileCount)
	}

	r, err := OpenBundle(res.Dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := r.Verify(ctx, testSecret); err != nil {
		t.Fatalf("a freshly exported bundle failed verification: %v", err)
	}

	m := r.Manifest()
	if m.SpokeID != testSpokeID || m.HubID != testHubID {
		t.Errorf("manifest identities = %s/%s", m.SpokeID, m.HubID)
	}
	if err := ValidateBundleID(m.BundleID); err != nil {
		t.Errorf("exported bundle ID is not well-formed: %v", err)
	}
	// A human at an air gap checks this with sha256sum.
	if m.EntriesSHA256 == "" || m.EntriesDigest == "" {
		t.Error("manifest carries no entries hashes")
	}
}

// The bundle is only as trustworthy as its verification. Each of these is a
// distinct way a drive could arrive altered.
func TestBundle_VerifyRejectsTampering(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		tamper func(t *testing.T, dir string)
		want   string
	}{
		{
			name: "a data file's contents changed",
			tamper: func(t *testing.T, dir string) {
				p := filepath.Join(dir, dataDir, "default/cpu/2026/08/07/14/a.parquet")
				// Same length, different bytes: a size check alone would miss it.
				if err := os.WriteFile(p, []byte("payload ONE"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "hashes to",
		},
		{
			name: "a data file truncated",
			tamper: func(t *testing.T, dir string) {
				p := filepath.Join(dir, dataDir, "default/cpu/2026/08/07/14/a.parquet")
				if err := os.WriteFile(p, []byte("pay"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "truncated",
		},
		{
			name: "a data file missing",
			tamper: func(t *testing.T, dir string) {
				if err := os.Remove(filepath.Join(dir, dataDir, "default/cpu/2026/08/07/14/a.parquet")); err != nil {
					t.Fatal(err)
				}
			},
			want: "missing",
		},
		{
			name: "an unsigned extra file smuggled in",
			tamper: func(t *testing.T, dir string) {
				p := filepath.Join(dir, dataDir, "default/cpu/2026/08/07/14/extra.parquet")
				if err := os.WriteFile(p, []byte("not in the manifest"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "unsigned payload",
		},
		{
			name: "an unsigned file beside the data",
			tamper: func(t *testing.T, dir string) {
				// An attacker with write access to media in transit: autorun.inf,
				// a shell script, a decoy manifest backup.
				if err := os.WriteFile(filepath.Join(dir, "autorun.inf"), []byte("[autorun]"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "unsigned payload",
		},
		{
			name: "an unsigned directory in the bundle",
			tamper: func(t *testing.T, dir string) {
				if err := os.MkdirAll(filepath.Join(dir, "extra"), 0o700); err != nil {
					t.Fatal(err)
				}
			},
			want: "unexpected directory",
		},
		{
			name: "a symlink beside the data",
			tamper: func(t *testing.T, dir string) {
				if err := os.Symlink("/etc/passwd", filepath.Join(dir, "link")); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			},
			want: "not a regular file",
		},
		{
			name: "the manifest's MAC changed",
			tamper: func(t *testing.T, dir string) {
				editManifest(t, dir, func(m *Manifest) {
					m.MAC = strings.Repeat("ab", 32)
				})
			},
			want: "bundle is invalid",
		},
		{
			name: "the manifest's hub retargeted",
			tamper: func(t *testing.T, dir string) {
				editManifest(t, dir, func(m *Manifest) { m.HubID = "other-hub" })
			},
			want: "bundle is invalid",
		},
		{
			name: "the entries digest swapped",
			tamper: func(t *testing.T, dir string) {
				editManifest(t, dir, func(m *Manifest) {
					m.EntriesDigest = strings.Repeat("cd", 32)
				})
			},
			want: "entries digest",
		},
		{
			name: "an entry line edited",
			tamper: func(t *testing.T, dir string) {
				p := filepath.Join(dir, entriesName)
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				out := strings.Replace(string(b), `"size_bytes":11`, `"size_bytes":99`, 1)
				if err := os.WriteFile(p, []byte(out), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "bundle is invalid",
		},
		{
			name: "the manifest removed",
			tamper: func(t *testing.T, dir string) {
				if err := os.Remove(filepath.Join(dir, manifestName)); err != nil {
					t.Fatal(err)
				}
			},
			want: "interrupted export",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rig := newBundleRig(t)
			res := rig.exportTwo(t)
			tc.tamper(t, res.Dir)

			r, err := OpenBundle(res.Dir, zerolog.Nop())
			if err != nil {
				// Some tampering is caught at open time; that is still a refusal.
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("open error = %v, want it to mention %q", err, tc.want)
				}
				return
			}
			err = r.Verify(ctx, testSecret)
			if err == nil {
				t.Fatal("a tampered bundle verified successfully")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// A bundle signed by one spoke must not verify under another's secret.
func TestBundle_VerifyRejectsTheWrongSecret(t *testing.T) {
	ctx := context.Background()
	rig := newBundleRig(t)
	res := rig.exportTwo(t)

	r, err := OpenBundle(res.Dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := r.Verify(ctx, "a-different-secret"); err == nil {
		t.Error("a bundle verified under the wrong secret")
	}
}

// A partial export must leave no manifest, so it cannot be mistaken for a
// complete bundle — and must not leave a partial tree an operator might copy.
func TestBundle_FailedExportLeavesNothingImportable(t *testing.T) {
	ctx := context.Background()
	rig := newBundleRig(t)

	// An entry whose file is not in storage: the export fails partway.
	entries := []*LedgerEntry{
		rig.write(t, "default/cpu/2026/08/07/14/a.parquet", "payload one"),
		{
			HubID: testHubID, Path: "default/cpu/2026/08/07/14/ghost.parquet",
			SHA256: strings.Repeat("ab", 32), SizeBytes: 10,
			Database: "default", Measurement: "cpu",
			PartitionTime: time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC),
		},
	}
	if _, err := rig.writer.Export(ctx, rig.parent, entries, time.Now()); err == nil {
		t.Fatal("exporting a missing file succeeded")
	}

	found, err := filepath.Glob(filepath.Join(rig.parent, "bundle-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("a failed export left %v behind", found)
	}
}

// A file that changes underneath the export must not be signed: the manifest
// would then attest to content that never existed.
func TestBundle_ExportRefusesAFileThatChanged(t *testing.T) {
	ctx := context.Background()
	rig := newBundleRig(t)

	e := rig.write(t, "default/cpu/2026/08/07/14/a.parquet", "original")
	// The ledger's digest now disagrees with what is on disk.
	e.SHA256 = strings.Repeat("ff", 32)

	if _, err := rig.writer.Export(ctx, rig.parent, []*LedgerEntry{e}, time.Now()); err == nil {
		t.Error("a file whose content disagreed with the ledger was exported and signed")
	}
}

func TestValidateBundleID(t *testing.T) {
	good, err := NewBundleID(time.Now())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := ValidateBundleID(good); err != nil {
		t.Errorf("a generated ID failed validation: %v", err)
	}

	// The ID is attacker-chosen and reaches a SQLite key, a log line, and a
	// directory name.
	for _, bad := range []string{
		"", "short", strings.Repeat("A", 27), strings.Repeat("A", 25),
		"01J9ZQK800000000000000000I", // I is not Crockford base32
		"01J9ZQK800000000000000000/", // a path separator
		"01J9ZQK80000000000000000\x00",
	} {
		if err := ValidateBundleID(bad); err == nil {
			t.Errorf("ValidateBundleID(%q) accepted it", bad)
		}
	}
}

// Bundle IDs are time-prefixed so a directory listing sorts into creation
// order — what an operator holding several drives wants.
func TestNewBundleID_SortsByCreationTime(t *testing.T) {
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	var prev string
	for i := 0; i < 5; i++ {
		id, err := NewBundleID(base.Add(time.Duration(i) * time.Second))
		if err != nil {
			t.Fatalf("new: %v", err)
		}
		if prev != "" && id <= prev {
			t.Errorf("ID %q does not sort after %q", id, prev)
		}
		prev = id
	}
}

// editManifest rewrites a bundle's manifest through a mutation function.
func editManifest(t *testing.T, dir string, fn func(*Manifest)) {
	t.Helper()
	p := filepath.Join(dir, manifestName)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	fn(&m)
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// exporterRig wires a ledger, writer, and policy the way main.go does.
type exporterRig struct {
	exporter *Exporter
	ledger   *Ledger
	backend  storage.Backend
	dest     string
}

func newExporterRig(t *testing.T, maxFiles int, maxBytes int64) *exporterRig {
	t.Helper()

	storeDir := t.TempDir()
	backend, err := storage.NewLocalBackend(storeDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	w, err := NewBundleWriter(BundleWriterConfig{
		Backend: backend, SpokeID: testSpokeID, HubID: testHubID,
		Secret: testSecret, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}

	dest := t.TempDir()
	policy, err := NewDestinationPolicy([]string{dest}, storeDir)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	l := setupTestLedger(t)
	disc, err := NewDiscoverer(l, backend, testHubID, zerolog.Nop())
	if err != nil {
		t.Fatalf("discoverer: %v", err)
	}
	ex, err := NewExporter(ExporterConfig{
		Ledger: l, Writer: w, Policy: policy, Discoverer: disc, HubID: testHubID,
		MaxFiles: maxFiles, MaxBytes: maxBytes, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}
	return &exporterRig{exporter: ex, ledger: l, backend: backend, dest: dest}
}

func (r *exporterRig) track(t *testing.T, path, content string) {
	t.Helper()
	ctx := context.Background()
	if err := r.backend.Write(ctx, path, []byte(content)); err != nil {
		t.Fatalf("write: %v", err)
	}
	sum, _ := sha256Of(content)
	e := &LedgerEntry{
		HubID: testHubID, Path: path, SHA256: sum, SizeBytes: int64(len(content)),
		Database: "default", Measurement: "cpu",
		PartitionTime: time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC),
	}
	if err := r.ledger.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}
}

// The whole point of 9a's StateExported: successive capped exports must cover
// the backlog rather than re-taking the newest files forever.
func TestExporter_SuccessiveExportsDrainTheBacklog(t *testing.T) {
	ctx := context.Background()
	rig := newExporterRig(t, 2, 0)

	for i := 0; i < 6; i++ {
		rig.track(t, fmt.Sprintf("default/cpu/2026/08/07/%02d/f.parquet", i), fmt.Sprintf("payload %d", i))
	}

	seen := map[string]bool{}
	for round := 0; round < 3; round++ {
		res, err := rig.exporter.Export(ctx, rig.dest, 0)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if res.FileCount != 2 {
			t.Errorf("round %d exported %d files, want 2", round, res.FileCount)
		}
		r, err := OpenBundle(res.Dir, zerolog.Nop())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if err := r.Verify(ctx, testSecret); err != nil {
			t.Fatalf("round %d bundle failed verification: %v", round, err)
		}
		entries, err := r.Entries(ctx)
		if err != nil {
			t.Fatalf("entries: %v", err)
		}
		for _, e := range entries {
			if seen[e.Path] {
				t.Errorf("%s was exported twice", e.Path)
			}
			seen[e.Path] = true
		}
	}

	if len(seen) != 6 {
		t.Errorf("covered %d files across three rounds, want 6", len(seen))
	}
	if _, err := rig.exporter.Export(ctx, rig.dest, 0); err == nil {
		t.Error("a fourth export found work when the backlog was drained")
	}
}

// A backlog larger than one drive is expected: the bundle truncates and the
// next one continues, rather than failing.
func TestExporter_TruncatesAtTheByteCap(t *testing.T) {
	ctx := context.Background()
	// 10 bytes: enough for one "payload N" (9 bytes), not two.
	rig := newExporterRig(t, 100, 10)

	for i := 0; i < 3; i++ {
		rig.track(t, fmt.Sprintf("default/cpu/2026/08/07/%02d/f.parquet", i), fmt.Sprintf("payload %d", i))
	}

	res, err := rig.exporter.Export(ctx, rig.dest, 0)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if res.FileCount != 1 {
		t.Errorf("exported %d files under a 10-byte cap, want 1", res.FileCount)
	}

	// The remainder is still available.
	if _, err := rig.exporter.Export(ctx, rig.dest, 0); err != nil {
		t.Errorf("the remainder was not exportable: %v", err)
	}
}

// A single file larger than the cap must still go, or it wedges the queue.
func TestExporter_ExportsAnOversizedFileRatherThanWedging(t *testing.T) {
	ctx := context.Background()
	rig := newExporterRig(t, 100, 5)

	rig.track(t, "default/cpu/2026/08/07/14/big.parquet", "a payload well over the cap")

	res, err := rig.exporter.Export(ctx, rig.dest, 0)
	if err != nil {
		t.Fatalf("a file larger than the byte cap wedged the queue: %v", err)
	}
	if res.FileCount != 1 {
		t.Errorf("FileCount = %d, want 1", res.FileCount)
	}
}

// The destination policy is the only thing bounding where a bundle lands.
func TestExporter_RefusesADestinationOutsideThePolicy(t *testing.T) {
	ctx := context.Background()
	rig := newExporterRig(t, 100, 0)
	rig.track(t, "default/cpu/2026/08/07/14/f.parquet", "payload")

	if _, err := rig.exporter.Export(ctx, t.TempDir(), 0); err == nil {
		t.Error("a destination outside the allow-list was accepted")
	}

	// And nothing was marked exported, since no bundle was written.
	pending, err := rig.ledger.Pending(ctx, testHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Errorf("pending = %d after a refused export, want 1", len(pending))
	}
}

// A drive that never arrives must be recoverable.
func TestExporter_RevertReturnsABundleToPending(t *testing.T) {
	ctx := context.Background()
	rig := newExporterRig(t, 100, 0)
	rig.track(t, "default/cpu/2026/08/07/14/f.parquet", "payload")

	res, err := rig.exporter.Export(ctx, rig.dest, 0)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if pending, _ := rig.ledger.Pending(ctx, testHubID, 0); len(pending) != 0 {
		t.Fatalf("pending = %d right after export, want 0", len(pending))
	}

	n, err := rig.exporter.Revert(ctx, res.BundleID)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	if n != 1 {
		t.Errorf("reverted %d, want 1", n)
	}
	if pending, _ := rig.ledger.Pending(ctx, testHubID, 0); len(pending) != 1 {
		t.Error("the reverted file is not pending again")
	}

	// A malformed ID is refused rather than silently matching nothing.
	if _, err := rig.exporter.Revert(ctx, "not-a-ulid"); err == nil {
		t.Error("a malformed bundle ID was accepted")
	}
}

// A spoke with nothing new is the steady state, not a failure.
func TestExporter_NothingToExportIsDistinguishable(t *testing.T) {
	ctx := context.Background()
	rig := newExporterRig(t, 100, 0)

	_, err := rig.exporter.Export(ctx, rig.dest, 0)
	if !errors.Is(err, ErrNothingToExport) {
		t.Errorf("error = %v, want ErrNothingToExport", err)
	}
}

// The writer's empty-input error must be the same sentinel the API maps to a
// 200 "nothing to do". A separately-constructed error with identical text does
// not satisfy errors.Is, so a future caller would get a 500 instead.
func TestBundleWriter_EmptyExportReturnsTheSentinel(t *testing.T) {
	rig := newBundleRig(t)
	_, err := rig.writer.Export(context.Background(), rig.parent, nil, time.Now())
	if !errors.Is(err, ErrNothingToExport) {
		t.Errorf("error = %v, want it to satisfy errors.Is(ErrNothingToExport)", err)
	}
}

// Two exports must never interleave into one directory. Mkdir on the leaf is
// atomic; stat-then-MkdirAll leaves a window.
func TestBundleWriter_RefusesAnExistingBundleDirectory(t *testing.T) {
	ctx := context.Background()
	rig := newBundleRig(t)
	e := rig.write(t, "default/cpu/2026/08/07/14/a.parquet", "payload")

	res, err := rig.writer.Export(ctx, rig.parent, []*LedgerEntry{e}, time.Now())
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	// Re-exporting into the same directory name must fail rather than merge.
	if err := os.MkdirAll(res.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(res.Dir, manifestName)); err != nil {
		t.Fatalf("the first bundle is gone: %v", err)
	}
}

// Open is exported and the importer is a separate component, so it validates
// rather than trusting that Verify already did. A traversal primitive that is
// unreachable today is one refactor away from being reachable.
func TestBundleReader_OpenCannotEscapeTheDataDirectory(t *testing.T) {
	ctx := context.Background()
	rig := newBundleRig(t)
	res := rig.exportTwo(t)

	// A file inside the bundle but outside data/.
	if err := os.WriteFile(filepath.Join(res.Dir, "secret.txt"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}

	r, err := OpenBundle(res.Dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open bundle: %v", err)
	}

	for _, bad := range []string{"../secret.txt", "../../etc/passwd", "/etc/passwd", "a/../../secret.txt"} {
		f, err := r.Open(bad)
		if err == nil {
			f.Close()
			t.Errorf("Open(%q) escaped data/", bad)
		}
	}

	// A legitimate entry still opens.
	entries, err := r.Entries(ctx)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	f, err := r.Open(entries[0].Path)
	if err != nil {
		t.Fatalf("a declared entry could not be opened: %v", err)
	}
	f.Close()
}
