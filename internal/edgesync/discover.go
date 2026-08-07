package edgesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

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

	entries := make([]*LedgerEntry, 0, len(objects))
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if !isSyncableFile(obj.Path) {
			continue
		}

		// Skip anything already tracked BEFORE hashing it. Discovery runs
		// every pass, and re-hashing the whole corpus each time would make the
		// cheap step the expensive one.
		if _, err := d.ledger.Get(ctx, d.hubID, obj.Path); err == nil {
			continue
		} else if !errors.Is(err, ErrNotFound) {
			return 0, fmt.Errorf("edgesync: check ledger for %q: %w", obj.Path, err)
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
