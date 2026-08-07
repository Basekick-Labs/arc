package edgesync

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// BundleVersion is the on-disk format version.
//
// Written into every manifest and checked on import. A reader that does not
// recognize a version must refuse rather than guess: a bundle is verified
// before anything is committed, and "verified" means nothing if the verifier
// misunderstood the layout.
const BundleVersion = 1

// Bundle file names. Fixed rather than configurable — an operator inspecting a
// drive at an air gap should find the same three names every time.
const (
	manifestName = "manifest.json"
	entriesName  = "entries.jsonl"
	dataDir      = "data"
)

// Manifest is the signed header of a bundle.
//
// Deliberately small and fixed-size: the entry list lives in a separate
// newline-delimited file so neither export nor import has to hold hundreds of
// thousands of entries in memory, and so a human can run `sha256sum
// entries.jsonl` — which is the point of a directory bundle over an archive.
type Manifest struct {
	Version int `json:"version"`

	// BundleID is a ULID, and the hub's replay key. Format-validated on import
	// because it is attacker-chosen: a compromised spoke signs whatever it
	// likes, and this string reaches a SQLite key and operator log lines.
	BundleID string `json:"bundle_id"`

	SpokeID string `json:"spoke_id"`

	// HubID is the hub this bundle is FOR. The import side must reject a
	// mismatch: the MAC alone does not, since a bundle for another hub
	// validates fine under the same spoke secret, and a scavenged drive would
	// otherwise import anywhere that spoke is registered.
	HubID string `json:"hub_id"`

	// CreatedAt is bound into the MAC but NOT enforced as a freshness window —
	// a bundle legitimately crosses an air gap over weeks. Surfaced to
	// operators so a 409 on a very old bundle is diagnosable.
	CreatedAt int64 `json:"created_at"`

	EntryCount int64 `json:"entry_count"`
	TotalBytes int64 `json:"total_bytes"`

	// EntriesSHA256 covers entries.jsonl as bytes, so a human with sha256sum
	// can check it without understanding the canonical entry encoding.
	EntriesSHA256 string `json:"entries_sha256"`

	// EntriesDigest is the canonical digest the MAC binds. Distinct from
	// EntriesSHA256: this one is order- and formatting-independent, so it
	// survives a reader that rewrites the file, while the raw hash does not.
	EntriesDigest string `json:"entries_digest"`

	MAC string `json:"mac"`
}

// BundleEntry is one file in a bundle, one JSON object per line.
type BundleEntry struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// NewBundleID returns a lexicographically-sortable, time-prefixed identifier.
//
// ULID-shaped: 48-bit millisecond timestamp then 80 bits of randomness, in
// Crockford base32. Sortable means a directory listing of bundles is in
// creation order, which is what an operator holding several drives wants.
func NewBundleID(now time.Time) (string, error) {
	var b [16]byte
	ms := uint64(now.UTC().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	if _, err := rand.Read(b[6:]); err != nil {
		return "", fmt.Errorf("edgesync: generate bundle ID: %w", err)
	}
	return crockford.EncodeToString(b[:]), nil
}

// crockford is base32 without I, L, O, or U — the characters most often
// misread when a human copies a bundle ID off a screen at an air gap.
var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// bundleIDLen is the encoded length of 16 bytes in unpadded base32.
const bundleIDLen = 26

// ValidateBundleID checks the format of an identifier that arrives from a file.
//
// The ID is attacker-chosen — a compromised spoke signs any manifest it likes —
// and it reaches a SQLite primary key, a log line, and (via the directory name)
// a filesystem path. An unbounded arbitrary string in all three is a bad idea
// regardless of whether a specific exploit is obvious.
func ValidateBundleID(id string) error {
	if len(id) != bundleIDLen {
		return fmt.Errorf("edgesync: bundle ID %q must be %d characters, got %d",
			truncateForError(id), bundleIDLen, len(id))
	}
	for _, r := range id {
		if !strings.ContainsRune("0123456789ABCDEFGHJKMNPQRSTVWXYZ", r) {
			return fmt.Errorf("edgesync: bundle ID %q contains %q, which is not Crockford base32",
				truncateForError(id), r)
		}
	}
	return nil
}

// truncateForError bounds an untrusted string before it reaches an error or log.
func truncateForError(s string) string {
	const max = 64
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// BundleWriterConfig configures an export.
type BundleWriterConfig struct {
	// Backend reads the spoke's Parquet files.
	Backend storage.Backend

	SpokeID string
	HubID   string

	// Secret is the hub-issued shared secret that signs the manifest.
	Secret string

	Logger zerolog.Logger
}

// BundleWriter exports ledger entries to a directory bundle.
type BundleWriter struct {
	backend storage.Backend
	spokeID string
	hubID   string
	secret  string
	logger  zerolog.Logger
}

// NewBundleWriter validates configuration and returns a ready writer.
func NewBundleWriter(cfg BundleWriterConfig) (*BundleWriter, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("edgesync: bundle writer requires a storage backend")
	}
	if err := validateSpokeID(cfg.SpokeID); err != nil {
		return nil, fmt.Errorf("edgesync: bundle writer spoke ID: %w", err)
	}
	if cfg.HubID == "" {
		return nil, fmt.Errorf("edgesync: bundle writer requires a hub ID")
	}
	if cfg.Secret == "" {
		// Without it the manifest cannot be signed, and an unsigned bundle is
		// not a bundle — the hub would reject every one.
		return nil, fmt.Errorf("edgesync: bundle writer requires a spoke secret")
	}
	return &BundleWriter{
		backend: cfg.Backend,
		spokeID: cfg.SpokeID,
		hubID:   cfg.HubID,
		secret:  cfg.Secret,
		logger:  cfg.Logger.With().Str("component", "edgesync-bundle").Logger(),
	}, nil
}

// ExportResult summarizes one export.
type ExportResult struct {
	BundleID  string
	Dir       string
	FileCount int
	Bytes     int64
	Duration  time.Duration
}

// Export writes entries to a new bundle directory under parent.
//
// Order matters and is the crash-safety property: data files first, then
// entries.jsonl, then the manifest LAST. A partial export therefore has no
// manifest and cannot be mistaken for a complete one.
//
// That ordering protects against a crash HERE. It does NOT survive the copy to
// removable media — `cp -r` walks in directory order and writes manifest.json
// before data/, so an interrupted copy leaves a complete manifest over a
// partial tree. The real completeness signal is BundleReader.Verify, which
// re-hashes every file. Do not treat the manifest's presence as sufficient.
func (w *BundleWriter) Export(ctx context.Context, parent string, entries []*LedgerEntry, now time.Time) (*ExportResult, error) {
	start := time.Now()

	if len(entries) == 0 {
		// The sentinel, not a look-alike: a caller distinguishing "nothing to
		// do" from a failure uses errors.Is, and a separately-constructed error
		// with identical text does not match it.
		return nil, ErrNothingToExport
	}

	bundleID, err := NewBundleID(now)
	if err != nil {
		return nil, err
	}

	// The spoke ID is in the directory name so a shared staging area holding
	// drives from several spokes is unambiguous without opening each manifest.
	dir := filepath.Join(parent, "bundle-"+w.spokeID+"-"+bundleID)

	// Mkdir on the leaf, not stat-then-MkdirAll: MkdirAll succeeds on an
	// existing directory, so a stat check leaves a window where two exports
	// could interleave into one bundle. Mkdir returns EEXIST atomically.
	// 0700: a bundle holds the spoke's telemetry and sits on removable media.
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, fmt.Errorf("edgesync: create bundle parent: %w", err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("edgesync: create bundle directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, dataDir), 0o700); err != nil {
		return nil, fmt.Errorf("edgesync: create bundle data directory: %w", err)
	}

	// Clean up a half-written bundle rather than leaving a partial tree that an
	// operator might copy. Only on the error path — success returns before this.
	committed := false
	defer func() {
		if !committed {
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				w.logger.Warn().Err(rmErr).Str("dir", dir).
					Msg("Could not remove a partially written bundle")
			}
		}
	}()

	written, err := w.writeData(ctx, dir, entries)
	if err != nil {
		return nil, err
	}

	digest, entriesSHA, total, err := w.writeEntries(dir, written)
	if err != nil {
		return nil, err
	}

	createdAt := now.UTC().Unix()
	mac, err := security.ComputeSyncBundleHMAC(w.secret, bundleID, w.spokeID, w.hubID, createdAt, digest)
	if err != nil {
		return nil, fmt.Errorf("edgesync: sign bundle: %w", err)
	}

	m := Manifest{
		Version:       BundleVersion,
		BundleID:      bundleID,
		SpokeID:       w.spokeID,
		HubID:         w.hubID,
		CreatedAt:     createdAt,
		EntryCount:    int64(len(written)),
		TotalBytes:    total,
		EntriesSHA256: entriesSHA,
		EntriesDigest: digest,
		MAC:           mac,
	}
	if err := writeJSONFile(filepath.Join(dir, manifestName), m); err != nil {
		return nil, err
	}

	committed = true
	w.logger.Info().
		Str("bundle_id", bundleID).
		Str("dir", dir).
		Int("files", len(written)).
		Int64("bytes", total).
		Msg("Bundle exported")

	return &ExportResult{
		BundleID:  bundleID,
		Dir:       dir,
		FileCount: len(written),
		Bytes:     total,
		Duration:  time.Since(start),
	}, nil
}

// writeData streams each entry's file into the bundle, re-hashing as it goes.
//
// Re-hashed rather than trusting the ledger's digest: the ledger records what
// the file was at discovery, and this verifies what is actually being written.
// A mismatch means the file changed underneath us, which must not be signed.
func (w *BundleWriter) writeData(ctx context.Context, dir string, entries []*LedgerEntry) ([]BundleEntry, error) {
	out := make([]BundleEntry, 0, len(entries))

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The entry path came from the spoke's own storage walk, but it lands
		// in a filesystem path here — validate before joining, not after.
		if err := validateSyncPath(e.Path); err != nil {
			return nil, fmt.Errorf("edgesync: entry %q: %w", truncateForError(e.Path), err)
		}

		dest := filepath.Join(dir, dataDir, filepath.FromSlash(e.Path))
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return nil, fmt.Errorf("edgesync: create bundle subdirectory: %w", err)
		}

		sha, n, err := w.copyOne(ctx, e.Path, dest)
		if err != nil {
			return nil, err
		}
		if sha != e.SHA256 {
			return nil, fmt.Errorf("edgesync: %q changed while exporting (ledger %s, read %s)",
				e.Path, e.SHA256, sha)
		}
		out = append(out, BundleEntry{Path: e.Path, SHA256: sha, SizeBytes: n})
	}
	return out, nil
}

// copyOne streams one file from the backend to dest, returning its digest.
func (w *BundleWriter) copyOne(ctx context.Context, src, dest string) (string, int64, error) {
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", 0, fmt.Errorf("edgesync: create %s: %w", dest, err)
	}
	defer f.Close()

	// Hash and write in one pass. Reading twice would double the I/O on a
	// constrained edge box and open a window for the file to change between.
	h := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(f, h)}
	if err := w.backend.ReadTo(ctx, src, counter); err != nil {
		return "", 0, fmt.Errorf("edgesync: read %s: %w", src, err)
	}
	if err := f.Sync(); err != nil {
		// Without the fsync a bundle can be reported complete while its bytes
		// are still in the page cache — and the next thing that happens to a
		// bundle is usually an unplugged drive.
		return "", 0, fmt.Errorf("edgesync: sync %s: %w", dest, err)
	}
	return hex.EncodeToString(h.Sum(nil)), counter.n, nil
}

// writeEntries writes entries.jsonl and returns the canonical digest, the
// file's own hash, and the total byte count.
func (w *BundleWriter) writeEntries(dir string, entries []BundleEntry) (digest, fileSHA string, total int64, err error) {
	paths := make([]string, 0, len(entries))
	shas := make([]string, 0, len(entries))
	sizes := make([]int64, 0, len(entries))
	for _, e := range entries {
		paths = append(paths, e.Path)
		shas = append(shas, e.SHA256)
		sizes = append(sizes, e.SizeBytes)
		total += e.SizeBytes
	}

	digest, err = security.BundleEntriesDigest(paths, shas, sizes)
	if err != nil {
		return "", "", 0, fmt.Errorf("edgesync: bundle entries digest: %w", err)
	}

	path := filepath.Join(dir, entriesName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", "", 0, fmt.Errorf("edgesync: create %s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	enc := json.NewEncoder(io.MultiWriter(f, h))
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return "", "", 0, fmt.Errorf("edgesync: write bundle entry: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		return "", "", 0, fmt.Errorf("edgesync: sync %s: %w", path, err)
	}
	return digest, hex.EncodeToString(h.Sum(nil)), total, nil
}

// writeJSONFile writes v as indented JSON, fsynced.
//
// Indented because a human at an air gap reads this file.
func writeJSONFile(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("edgesync: create %s: %w", path, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("edgesync: write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("edgesync: sync %s: %w", path, err)
	}
	return nil
}

// countingWriter counts bytes on their way through.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
