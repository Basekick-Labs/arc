package edgesync

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// ErrBundleInvalid marks a bundle that must not be imported.
//
// One error for every rejection reason so a caller maps it to a single status
// rather than leaking which specific check failed — a tampered bundle and a
// truncated one are both simply "do not import this".
var ErrBundleInvalid = errors.New("edgesync: bundle is invalid")

// maxManifestBytes bounds the manifest read.
//
// The manifest is a fixed set of short fields; anything larger is malformed or
// hostile. The entry list lives in a separate streamed file precisely so this
// bound can stay small.
const maxManifestBytes = 64 << 10

// BundleReader reads and verifies a bundle directory.
type BundleReader struct {
	dir      string
	manifest *Manifest
	logger   zerolog.Logger
}

// OpenBundle reads a bundle's manifest.
//
// Does NOT verify — call Verify before trusting anything here. Parsing the
// manifest first is what lets a caller look up the right spoke secret.
func OpenBundle(dir string, logger zerolog.Logger) (*BundleReader, error) {
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// The manifest is written last, so its absence is the signature of
			// an export that died partway.
			return nil, fmt.Errorf("%w: no %s (an interrupted export leaves none)",
				ErrBundleInvalid, manifestName)
		}
		return nil, fmt.Errorf("edgesync: read bundle manifest: %w", err)
	}
	if len(raw) > maxManifestBytes {
		return nil, fmt.Errorf("%w: manifest is %d bytes, over the %d-byte bound",
			ErrBundleInvalid, len(raw), maxManifestBytes)
	}

	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: manifest is not valid JSON: %v", ErrBundleInvalid, err)
	}
	if m.Version != BundleVersion {
		// Refuse rather than guess: "verified" means nothing if the verifier
		// misunderstood the layout.
		return nil, fmt.Errorf("%w: manifest version %d, this Arc understands %d",
			ErrBundleInvalid, m.Version, BundleVersion)
	}
	if err := ValidateBundleID(m.BundleID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBundleInvalid, err)
	}
	if err := validateSpokeID(m.SpokeID); err != nil {
		return nil, fmt.Errorf("%w: spoke ID: %v", ErrBundleInvalid, err)
	}
	if m.EntryCount < 0 || m.TotalBytes < 0 {
		return nil, fmt.Errorf("%w: negative entry count or size", ErrBundleInvalid)
	}

	return &BundleReader{
		dir:      dir,
		manifest: &m,
		logger:   logger.With().Str("component", "edgesync-bundle").Logger(),
	}, nil
}

// Manifest returns the parsed, not-yet-verified manifest.
func (r *BundleReader) Manifest() *Manifest { return r.manifest }

// Verify checks that this bundle is exactly what its spoke signed.
//
// Everything, before anything is committed:
//   - the MAC over (bundle ID, spoke, hub, created-at, entries digest)
//   - entries.jsonl's own hash, so a human's sha256sum agrees
//   - every declared entry's path, and its file's size and digest
//   - no file ANYWHERE in the bundle that the manifest does not declare
//
// That last check is what makes the directory format honest. Verifying only
// the declared direction would leave unsigned, unverified payload sitting on
// air-gap media inside something an operator has been told is verified — and
// it covers the whole directory, not just data/, because that is the whole of
// what a human is handed.
func (r *BundleReader) Verify(ctx context.Context, secret string) error {
	entries, digest, fileSHA, err := r.readEntries(ctx)
	if err != nil {
		return err
	}

	if fileSHA != r.manifest.EntriesSHA256 {
		return fmt.Errorf("%w: %s hashes to %s, manifest says %s",
			ErrBundleInvalid, entriesName, fileSHA, r.manifest.EntriesSHA256)
	}
	if digest != r.manifest.EntriesDigest {
		return fmt.Errorf("%w: entries digest is %s, manifest says %s",
			ErrBundleInvalid, digest, r.manifest.EntriesDigest)
	}
	if int64(len(entries)) != r.manifest.EntryCount {
		return fmt.Errorf("%w: %d entries, manifest declares %d",
			ErrBundleInvalid, len(entries), r.manifest.EntryCount)
	}

	if err := security.ValidateSyncBundleHMAC(secret, r.manifest.BundleID, r.manifest.SpokeID,
		r.manifest.HubID, r.manifest.CreatedAt, r.manifest.EntriesDigest, r.manifest.MAC); err != nil {
		return fmt.Errorf("%w: %v", ErrBundleInvalid, err)
	}

	declared := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.verifyOne(ctx, e); err != nil {
			return err
		}
		declared[filepath.Clean(filepath.FromSlash(e.Path))] = struct{}{}
	}

	return r.rejectUndeclared(declared)
}

// verifyOne checks one entry's file against its declared size and digest.
func (r *BundleReader) verifyOne(ctx context.Context, e BundleEntry) error {
	// Validated here rather than first at import: an invalid path must fail
	// verification, so the bundle is refused before a single byte is committed.
	if err := validateSyncPath(e.Path); err != nil {
		return fmt.Errorf("%w: entry %q: %v", ErrBundleInvalid, truncateForError(e.Path), err)
	}
	if !isHexSHA256(e.SHA256) {
		return fmt.Errorf("%w: entry %q has a malformed digest", ErrBundleInvalid, truncateForError(e.Path))
	}
	if e.SizeBytes < 0 {
		return fmt.Errorf("%w: entry %q has a negative size", ErrBundleInvalid, truncateForError(e.Path))
	}

	path := r.DataPath(e.Path)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %q is declared but missing (an interrupted copy)",
				ErrBundleInvalid, truncateForError(e.Path))
		}
		return fmt.Errorf("edgesync: stat bundle file: %w", err)
	}
	if info.Size() != e.SizeBytes {
		// The common case for a truncated transfer, and cheaper to catch here
		// than by hashing the whole file first.
		return fmt.Errorf("%w: %q is %d bytes, manifest declares %d (a truncated copy)",
			ErrBundleInvalid, truncateForError(e.Path), info.Size(), e.SizeBytes)
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("edgesync: open bundle file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("edgesync: hash bundle file: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != e.SHA256 {
		return fmt.Errorf("%w: %q hashes to %s, manifest declares %s",
			ErrBundleInvalid, truncateForError(e.Path), got, e.SHA256)
	}
	return nil
}

// rejectUndeclared fails if the bundle holds any file the manifest does not name.
//
// Walks the WHOLE bundle directory, not just data/. "Verified" has to mean the
// whole thing an operator is handed: an attacker with write access to media in
// transit could otherwise drop an autorun.inf, a decoy manifest.json.bak, or a
// shell script beside the data and the bundle would still verify clean. The
// premise of this format is that a human inspects what crosses an air gap, so
// the verifier must cover everything they would find.
func (r *BundleReader) rejectUndeclared(declared map[string]struct{}) error {
	dataPrefix := dataDir + string(filepath.Separator)

	return filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(r.dir, p)
		if relErr != nil {
			return fmt.Errorf("edgesync: walk bundle: %w", relErr)
		}
		rel = filepath.Clean(rel)
		if rel == "." {
			return nil
		}

		if d.IsDir() {
			// Only data/ and its subtree may contain directories.
			if rel != dataDir && !strings.HasPrefix(rel, dataPrefix) {
				return fmt.Errorf("%w: unexpected directory %q in the bundle",
					ErrBundleInvalid, truncateForError(rel))
			}
			return nil
		}

		// Symlinks are not regular files: a link could point anywhere, and its
		// target would be read as though it were bundle content.
		if !d.Type().IsRegular() {
			return fmt.Errorf("%w: %q is not a regular file", ErrBundleInvalid, truncateForError(rel))
		}

		// The two signed metadata files.
		if rel == manifestName || rel == entriesName {
			return nil
		}

		if !strings.HasPrefix(rel, dataPrefix) {
			return fmt.Errorf("%w: %q is in the bundle but is not %s, %s, or under %s/ — unsigned payload",
				ErrBundleInvalid, truncateForError(rel), manifestName, entriesName, dataDir)
		}
		if _, ok := declared[strings.TrimPrefix(rel, dataPrefix)]; !ok {
			return fmt.Errorf("%w: %q is under %s/ but not declared in %s — unsigned payload",
				ErrBundleInvalid, truncateForError(rel), dataDir, entriesName)
		}
		return nil
	})
}

// readEntries streams entries.jsonl, returning the entries, the canonical
// digest, and the file's own hash.
//
// Streamed, not buffered: a spoke returning from a long outage can present
// hundreds of thousands of entries, and this runs on a hub that may also be
// serving queries.
func (r *BundleReader) readEntries(ctx context.Context) ([]BundleEntry, string, string, error) {
	path := filepath.Join(r.dir, entriesName)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, "", "", fmt.Errorf("%w: no %s", ErrBundleInvalid, entriesName)
		}
		return nil, "", "", fmt.Errorf("edgesync: open %s: %w", entriesName, err)
	}
	defer f.Close()

	h := sha256.New()
	sc := bufio.NewScanner(io.TeeReader(f, h))
	// One entry per line; the default 64KB token bound is generous for a path
	// plus a digest, and a longer line is malformed.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		entries []BundleEntry
		paths   []string
		shas    []string
		sizes   []int64
	)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, "", "", err
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e BundleEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, "", "", fmt.Errorf("%w: malformed entry line: %v", ErrBundleInvalid, err)
		}
		entries = append(entries, e)
		paths = append(paths, e.Path)
		shas = append(shas, e.SHA256)
		sizes = append(sizes, e.SizeBytes)
	}
	if err := sc.Err(); err != nil {
		return nil, "", "", fmt.Errorf("%w: reading %s: %v", ErrBundleInvalid, entriesName, err)
	}

	// Rejects duplicate paths, which is how a conflict would otherwise smuggle
	// past "conflicts are reported, never overwritten".
	digest, err := security.BundleEntriesDigest(paths, shas, sizes)
	if err != nil {
		return nil, "", "", fmt.Errorf("%w: %v", ErrBundleInvalid, err)
	}

	return entries, digest, hex.EncodeToString(h.Sum(nil)), nil
}

// Entries streams the bundle's entries for an importer.
//
// Verify must have succeeded first; this does not re-check.
func (r *BundleReader) Entries(ctx context.Context) ([]BundleEntry, error) {
	entries, _, _, err := r.readEntries(ctx)
	return entries, err
}

// DataPath returns the on-disk location of one entry's file.
func (r *BundleReader) DataPath(entryPath string) string {
	return filepath.Join(r.dir, dataDir, filepath.FromSlash(entryPath))
}

// Open returns a reader over one entry's file, for an importer to stream.
func (r *BundleReader) Open(entryPath string) (io.ReadCloser, error) {
	return os.Open(r.DataPath(entryPath))
}
