package edgesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Discoverer walks local storage and tracks new files in the ledger.
//
// Separate from Agent because BOTH transports need it and they are
// independently enabled: a fully air-gapped spoke runs no agent at all, and
// without its own discovery its ledger would stay empty forever — every export
// answering "nothing to export" while files piled up on disk.
type Discoverer struct {
	ledger  *Ledger
	backend storage.Backend
	hubID   string
	logger  zerolog.Logger

	// namespaceExcluder, when set, returns the top-level path segments this
	// node holds ON BEHALF OF OTHERS — the spoke namespaces a dual-role
	// (hub+spoke) node received from other edges. Discovery skips them:
	// without this, a hub that is also a spoke re-discovers other spokes'
	// received files and forwards them, double-namespaced, to ITS upstream
	// hub — an undocumented, unbounded relay (2026-08-19 audit M2). Relaying
	// may become a real feature; today it is opt-nothing and surprising, so
	// it is off. An excluder ERROR aborts discovery (fail-safe: a transient
	// registry error must not silently start relaying).
	namespaceExcluder func(ctx context.Context) (map[string]struct{}, error)

	// compactionDeferEpoch, when non-zero, activates issue-#610 handling of
	// tier-suffixed compacted files that are NOT in the ledger: one whose
	// embedded timestamp is AFTER the epoch is a crash orphan — produced
	// under defer-until-synced, so its contents were delivered by
	// construction — and is tracked as a compacted output (never synced)
	// instead of as new data. One from BEFORE the epoch (or with an
	// unparseable name) is legacy: it may hold rows the hub never received,
	// so it syncs once. Ambiguity resolves toward syncing — a duplicate,
	// never a loss.
	compactionDeferEpoch time.Time
}

// SetCompactionDeferEpoch activates epoch-based compacted-file
// discrimination (issue #610). Zero deactivates it.
func (d *Discoverer) SetCompactionDeferEpoch(epoch time.Time) {
	d.compactionDeferEpoch = epoch
}

// SetNamespaceExcluder installs the received-namespace exclusion for
// dual-role nodes. nil deactivates it.
func (d *Discoverer) SetNamespaceExcluder(fn func(ctx context.Context) (map[string]struct{}, error)) {
	d.namespaceExcluder = fn
}

// NewDiscoverer validates configuration and returns a ready discoverer.
func NewDiscoverer(ledger *Ledger, backend storage.Backend, hubID string, logger zerolog.Logger) (*Discoverer, error) {
	if ledger == nil {
		return nil, errors.New("edgesync: discovery requires a ledger")
	}
	if backend == nil {
		return nil, errors.New("edgesync: discovery requires a storage backend")
	}
	if hubID == "" {
		hubID = DefaultHubID
	}
	return &Discoverer{
		ledger:  ledger,
		backend: backend,
		hubID:   hubID,
		logger:  logger.With().Str("component", "edgesync-discover").Logger(),
	}, nil
}

// Discover tracks every syncable local file not already in the ledger.
//
// Returns how many entries were newly tracked. Idempotent: a file already
// tracked is skipped before it is hashed, so running this every pass costs a
// listing rather than a re-read of the whole corpus.
func (d *Discoverer) Discover(ctx context.Context) (int, error) {
	lister, ok := d.backend.(storage.ObjectLister)
	if !ok {
		return 0, errors.New("edgesync: backend cannot list object metadata, so discovery is not possible")
	}

	objects, err := lister.ListObjects(ctx, "")
	if err != nil {
		return 0, fmt.Errorf("edgesync: list local files: %w", err)
	}

	var excluded map[string]struct{}
	if d.namespaceExcluder != nil {
		excluded, err = d.namespaceExcluder(ctx)
		if err != nil {
			// Fail-safe: proceeding without the exclusion set would relay
			// other spokes' received data upstream on a registry blip.
			return 0, fmt.Errorf("edgesync: list received namespaces for discovery exclusion: %w", err)
		}
	}

	entries := make([]*LedgerEntry, 0, len(objects))
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if !isSyncableFile(obj.Path) {
			continue
		}
		if len(excluded) > 0 {
			// First segment alone, NOT splitFirstTwo: that helper requires
			// three segments, but a spoke can legally sync a root-level file
			// the hub stores as {spokeID}/x.parquet — two segments, which
			// would escape the exclusion and relay upstream (deep-review
			// High on this change).
			if i := strings.IndexByte(obj.Path, '/'); i > 0 {
				if _, isReceived := excluded[obj.Path[:i]]; isReceived {
					// Another spoke's data, held here as a hub. Not this
					// node's telemetry; never forwarded upstream.
					continue
				}
			}
		}

		// Skip anything already tracked BEFORE hashing it. Discovery runs
		// every pass, and re-hashing the whole corpus each time would make the
		// cheap step the expensive one.
		if _, err := d.ledger.Get(ctx, d.hubID, obj.Path); err == nil {
			continue
		} else if !errors.Is(err, ErrNotFound) {
			return 0, fmt.Errorf("edgesync: check ledger for %q: %w", obj.Path, err)
		}

		// Issue #610: a compacted file not in the ledger is either a crash
		// orphan (observer insert lost — its contents are already on the
		// hub) or a legacy file (predates defer-until-synced — may hold
		// undelivered rows). The enablement epoch vs the filename's
		// embedded timestamp tells them apart.
		if !d.compactionDeferEpoch.IsZero() {
			if ts, isCompacted := compactedFileTimestamp(obj.Path); isCompacted && ts.After(d.compactionDeferEpoch) {
				if err := d.ledger.TrackCompactedOutput(ctx, d.hubID, obj.Path); err != nil {
					d.logger.Warn().Err(err).Str("path", obj.Path).
						Msg("Could not track an orphaned compacted output; will retry next pass")
					continue
				}
				d.logger.Info().Str("path", obj.Path).
					Msg("Recovered an orphaned compacted output into the ledger (will not sync; contents were delivered before compaction)")
				continue
			}
		}

		// ListObjects carries no digest, so compute one. This is the only full
		// read a file gets from discovery, ever — and the spoke has to read
		// those bytes to send them anyway. The digest is then reused for the
		// request HMAC and for the hub's verify-before-commit.
		sum, err := d.hashFile(ctx, obj.Path)
		if err != nil {
			// One unreadable file must not stop discovery: the rest of the
			// backlog is still syncable, and this one is retried next pass.
			d.logger.Warn().Err(err).Str("path", obj.Path).Msg("Skipping a file that could not be hashed")
			continue
		}

		e := &LedgerEntry{
			HubID:     d.hubID,
			Path:      obj.Path,
			SHA256:    sum,
			SizeBytes: obj.Size,
		}
		e.Database, e.Measurement, e.PartitionTime = parseArcPath(obj.Path)
		entries = append(entries, e)
	}

	if len(entries) == 0 {
		return 0, nil
	}

	inserted, err := d.ledger.TrackBatch(ctx, entries)
	if err != nil {
		return 0, fmt.Errorf("edgesync: track discovered files: %w", err)
	}
	return inserted, nil
}

// hashFile streams a file through SHA-256 without buffering it.
func (d *Discoverer) hashFile(ctx context.Context, path string) (string, error) {
	h := sha256.New()
	if err := d.backend.ReadTo(ctx, path, hasherWriter{h}); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// compactedFileTimestamp reports whether path names a compaction output
// (any tier) and, if so, the creation instant embedded in its filename.
//
// Output basenames are `{meas}_{YYYYMMDD_HHMMSS}_{unixnano}_b{batch}_{tier}.parquet`
// with tier "compacted" for hourly and the tier name otherwise (see
// compaction/job.go). The unix-nano field is parsed rather than the
// formatted timestamp: it is unambiguous and needs no layout. A name that
// merely LOOKS compacted but does not parse returns ok=false — the caller
// then treats the file as legacy data and syncs it. (That covers the
// parse-FAILURE direction; a false-positive match on a raw file cannot occur
// for ingest-produced names, whose final underscore token is numeric, never
// a tier suffix.)
func compactedFileTimestamp(path string) (time.Time, bool) {
	base := path
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	base, found := strings.CutSuffix(base, ".parquet")
	if !found {
		return time.Time{}, false
	}
	parts := strings.Split(base, "_")
	if len(parts) < 5 {
		return time.Time{}, false
	}
	switch parts[len(parts)-1] {
	case "compacted", "daily", "weekly", "monthly":
	default:
		return time.Time{}, false
	}
	if !strings.HasPrefix(parts[len(parts)-2], "b") {
		return time.Time{}, false
	}
	nanos, err := strconv.ParseInt(parts[len(parts)-3], 10, 64)
	if err != nil || nanos <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, nanos).UTC(), true
}
