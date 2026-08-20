package edgesync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// ImportBatchSize bounds one manifest proposal.
//
// The Cluster Operations Checklist caps Raft batches at 1000 ops so a single
// log entry cannot grow unbounded. It matters more here than on the online
// path: an HTTP transfer naturally rate-limits proposals to one per request,
// whereas a bundle import is a tight loop, and a 10,000-file bundle would
// otherwise emit 10,000 individual proposals as fast as the disk allows.
const ImportBatchSize = 1000

// ImporterConfig configures bundle import.
type ImporterConfig struct {
	// Receiver commits each file: staging, hash, verify, promote, index. Reused
	// unchanged from the online path so both transports land bytes identically.
	//
	// IMPORTANT: this Receiver must be constructed with a RegisterFile that
	// COLLECTS rather than proposes — see CollectingRegistrar. The online
	// Receiver proposes to the Raft manifest once per file, which is fine at
	// one-file-per-HTTP-request but would emit 10,000 proposals in a tight
	// loop for a 10,000-file bundle.
	Receiver *Receiver

	// Collector is the sink the Receiver's RegisterFile writes into. The
	// importer drains it in batches of ImportBatchSize.
	Collector *CollectingRegistrar

	// Index is the dedup ledger.
	Index *BundleIndex

	// Registry resolves a spoke's secret to verify the manifest signature.
	Registry *Registry

	// HubID is this hub's identity. A bundle naming a different hub is
	// refused: the MAC alone does not stop that, since a bundle for another
	// hub validates fine under the same spoke's secret, and a scavenged drive
	// would otherwise import anywhere that spoke is registered.
	HubID string

	// MaxFiles refuses a manifest declaring more than this. Enforced
	// independently of the exporting spoke's own cap, which is advisory
	// against a hostile manifest.
	MaxFiles int64

	// FlushManifest commits a batch of registered files to the Raft manifest.
	// Nil in standalone mode, where there is no manifest.
	FlushManifest func(ctx context.Context, files []*ReceivedFile) error

	Logger zerolog.Logger
}

// Importer reads a verified bundle into hub storage.
//
// Imports are serialized. One collector and one staging area are shared across
// calls, and a hub takes one drive at a time by nature — but the endpoint is a
// concurrent Fiber handler, so nothing structural stops two operators (or one
// retry after a client timeout on a four-hour request) from overlapping.
// Without the lock they collide in the staging area: both stage the same path,
// one promotes, the other fails with "file not found" — and Reset truncates
// the other import's buffered registrations, leaving committed files outside
// the manifest with no error and no log.
type Importer struct {
	// mu serializes Import. Held for the whole call, which is the point:
	// the shared state is the staging area and the collector, and both span
	// the entire import.
	mu sync.Mutex

	receiver      *Receiver
	collector     *CollectingRegistrar
	index         *BundleIndex
	registry      *Registry
	hubID         string
	maxFiles      int64
	flushManifest func(ctx context.Context, files []*ReceivedFile) error
	logger        zerolog.Logger
}

// NewImporter validates configuration and returns a ready importer.
func NewImporter(cfg ImporterConfig) (*Importer, error) {
	if cfg.Receiver == nil {
		return nil, errors.New("edgesync: importer requires a receiver")
	}
	if cfg.Collector == nil {
		return nil, errors.New("edgesync: importer requires a collecting registrar")
	}
	if cfg.Index == nil {
		return nil, errors.New("edgesync: importer requires a bundle index")
	}
	if cfg.Registry == nil {
		// Without it there is no secret to verify against, and an unverified
		// bundle is an unauthenticated write to hub storage.
		return nil, errors.New("edgesync: importer requires a spoke registry")
	}
	if cfg.HubID == "" {
		return nil, errors.New("edgesync: importer requires a hub ID")
	}

	maxFiles := cfg.MaxFiles
	if maxFiles <= 0 {
		maxFiles = DefaultBundleMaxFiles
	}

	return &Importer{
		receiver:      cfg.Receiver,
		collector:     cfg.Collector,
		index:         cfg.Index,
		registry:      cfg.Registry,
		hubID:         cfg.HubID,
		maxFiles:      maxFiles,
		flushManifest: cfg.FlushManifest,
		logger:        cfg.Logger.With().Str("component", "edgesync-import").Logger(),
	}, nil
}

// ImportResult summarizes one import.
type ImportResult struct {
	BundleID  string
	SpokeID   string
	CreatedAt time.Time

	Committed      int
	AlreadyPresent int
	BytesWritten   int64

	// AckPaths are the paths the hub now holds, for the acknowledgment.
	// Committed and already-present both qualify: from the spoke's side they
	// are the same fact. Conflicted paths are excluded — the hub holds
	// DIFFERENT content there, so the spoke's copy was never delivered.
	AckPaths []string

	// AckWritten reports whether the acknowledgment reached the drive. False
	// means the import succeeded but the spoke cannot learn of it from this
	// drive, so its files stay `exported` and ride the next bundle.
	AckWritten bool

	// Conflicts are reported in full, not counted: each needs a human to
	// decide which copy is right, and a count alone would not say which.
	Conflicts []Conflict

	// Recorded reports whether the dedup ledger write succeeded. False means
	// the files are committed but a re-import of the same drive will not be
	// refused — harmless, since it resolves to already_present, but it makes
	// /history disagree with reality and the operator should know why.
	Recorded bool

	Duration time.Duration
}

// ErrImportInProgress means another bundle import is already running.
//
// One import at a time is the physical reality (one drive in one slot) and a
// hard requirement (the staging area and result collector are shared), so a
// concurrent request fails FAST with this sentinel rather than queueing
// silently behind a run that can legally take hours — the operator whose
// first request timed out client-side would otherwise re-POST and hang too
// (2026-08-19 audit M3).
var ErrImportInProgress = errors.New("edgesync: a bundle import is already in progress")

// Import verifies a bundle and commits its files.
//
// Order: open, identity checks, dedup, verify EVERYTHING, then commit. No byte
// reaches its final path until the whole bundle has been verified — the
// property that makes an artifact carried on removable media safe to trust.
func (i *Importer) Import(ctx context.Context, dir string) (*ImportResult, error) {
	if !i.mu.TryLock() {
		return nil, ErrImportInProgress
	}
	defer i.mu.Unlock()

	start := time.Now()

	r, err := OpenBundle(dir, i.logger)
	if err != nil {
		return nil, err
	}
	m := r.Manifest()

	// Identity before signature: a bundle for another hub is refused even if
	// it is perfectly signed, because the signing spoke may be registered on
	// both and the MAC would validate.
	if m.HubID != i.hubID {
		return nil, fmt.Errorf("%w: bundle is addressed to hub %q, this hub is %q",
			ErrBundleInvalid, truncateForError(m.HubID), i.hubID)
	}
	if m.EntryCount <= 0 {
		// An empty bundle is signable but meaningless, and recording one would
		// burn a bundle ID for nothing.
		return nil, fmt.Errorf("%w: bundle declares no entries", ErrBundleInvalid)
	}
	if m.EntryCount > i.maxFiles {
		return nil, fmt.Errorf("%w: bundle declares %d entries, over this hub's limit of %d",
			ErrBundleInvalid, m.EntryCount, i.maxFiles)
	}

	spoke, err := i.registry.Get(ctx, m.SpokeID)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown spoke %q", ErrBundleInvalid, truncateForError(m.SpokeID))
	}
	if !spoke.Enabled {
		return nil, fmt.Errorf("%w: spoke %q is disabled", ErrBundleInvalid, truncateForError(m.SpokeID))
	}

	// Dedup BEFORE verification: re-verifying a bundle already imported would
	// re-hash every file for a result that is already known.
	if prior, err := i.index.Seen(ctx, m.SpokeID, m.BundleID); err != nil {
		return nil, err
	} else if prior != nil {
		return nil, fmt.Errorf("%w: %s imported at %s (created %s, %d files)",
			ErrBundleAlreadyImported, m.BundleID,
			prior.ImportedAt.Format(time.RFC3339), prior.CreatedAt.Format(time.RFC3339),
			prior.FileCount)
	}

	secret, err := i.registry.Secret(ctx, m.SpokeID)
	if err != nil {
		return nil, fmt.Errorf("edgesync: read spoke secret: %w", err)
	}
	if err := r.Verify(ctx, secret); err != nil {
		return nil, err
	}

	entries, err := r.Entries(ctx)
	if err != nil {
		return nil, err
	}

	res := &ImportResult{
		BundleID:  m.BundleID,
		SpokeID:   m.SpokeID,
		CreatedAt: time.Unix(m.CreatedAt, 0).UTC(),
		Recorded:  true,
	}
	if err := i.commitAll(ctx, r, m.SpokeID, entries, res); err != nil {
		return nil, err
	}

	if err := i.index.Record(ctx, &ImportedBundle{
		SpokeID:    m.SpokeID,
		BundleID:   m.BundleID,
		CreatedAt:  res.CreatedAt,
		FileCount:  int64(res.Committed + res.AlreadyPresent),
		BytesTotal: res.BytesWritten,
		Conflicts:  int64(len(res.Conflicts)),
	}); err != nil {
		// The files are committed either way. Failing here would tell the
		// operator the import failed when it did not, and a retry would then
		// find everything already present — noise, not recovery.
		//
		// The two cases mean opposite things, so they are not logged alike: a
		// concurrent record means the row EXISTS and a re-import will be
		// refused; any other failure means it does not and a re-import will
		// not be.
		if errors.Is(err, ErrBundleAlreadyImported) {
			i.logger.Warn().
				Str("bundle_id", m.BundleID).
				Msg("Bundle recorded concurrently by another import; a re-import will be refused")
		} else {
			i.logger.Error().Err(err).
				Str("bundle_id", m.BundleID).
				Msg("Bundle imported but could not be recorded; a re-import will NOT be refused")
			res.Recorded = false
		}
	}

	// Written after the dedup row, so an ack only ever describes an import
	// this hub considers complete. Failure is logged and surfaced, not fatal:
	// the files are committed either way, and the spoke simply re-sends them
	// in a later bundle where the hub answers already_present.
	if err := i.writeAck(ctx, dir, m, res); err != nil {
		i.logger.Error().Err(err).
			Str("bundle_id", m.BundleID).
			Msg("Bundle imported but the acknowledgment could not be written; " +
				"the spoke will not learn of this import from this drive")
	} else {
		res.AckWritten = true
	}

	res.Duration = time.Since(start)
	i.logger.Info().
		Str("bundle_id", m.BundleID).
		Str("spoke_id", m.SpokeID).
		Int("committed", res.Committed).
		Int("already_present", res.AlreadyPresent).
		Int("conflicts", len(res.Conflicts)).
		Int64("bytes", res.BytesWritten).
		Dur("duration", res.Duration).
		Msg("Bundle imported")

	return res, nil
}

// commitAll streams every entry into storage, flushing manifest ops in batches.
//
// The Receiver registers each committed file through the collector rather than
// proposing directly, so this drains that buffer at ImportBatchSize. That is
// the difference between one Raft proposal per 1000 files and one per file.
func (i *Importer) commitAll(ctx context.Context, r *BundleReader, spokeID string, entries []BundleEntry, res *ImportResult) error {
	i.collector.Reset()

	flush := func() error {
		files := i.collector.Drain()
		if len(files) == 0 || i.flushManifest == nil {
			return nil
		}
		if err := i.flushManifest(ctx, files); err != nil {
			// The bytes are already committed by the receiver, so a failed
			// manifest update means files exist in storage the cluster cannot
			// see. Abort rather than continue: a Raft quorum loss is not
			// transient, and pressing on would widen the gap. The next import
			// of this bundle re-registers them, since Record never ran.
			return fmt.Errorf("%w: commit manifest batch: %w", ErrReceiveInternal, err)
		}
		return nil
	}

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		size, conflict, err := i.commitOne(ctx, r, spokeID, e)
		if err != nil {
			return err
		}
		switch {
		case conflict != nil:
			res.Conflicts = append(res.Conflicts, *conflict)
		case size < 0:
			res.AlreadyPresent++
			res.AckPaths = append(res.AckPaths, e.Path)
		default:
			res.Committed++
			res.BytesWritten += size
			res.AckPaths = append(res.AckPaths, e.Path)
		}

		// Checked after EVERY entry, not only after a commit. An
		// already-present file registers too — resolveExisting re-attempts it,
		// deliberately, so a file whose bytes landed but whose registration
		// failed is not stranded — so it also enters the collector. Skipping
		// the check on that path let a re-imported bundle accumulate past the
		// cap and emit one oversized proposal, defeating the batching this
		// exists to provide.
		if i.collector.Len() >= ImportBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// commitOne stages, verifies, and promotes a single entry.
//
// Returns (size, nil, nil) when it committed, (-1, nil, nil) when the hub
// already holds this exact content, and (0, conflict, nil) when it holds
// different content at the path.
func (i *Importer) commitOne(ctx context.Context, r *BundleReader, spokeID string, e BundleEntry) (int64, *Conflict, error) {
	body, err := r.Open(e.Path)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: open %q: %v", ErrBundleInvalid, truncateForError(e.Path), err)
	}
	defer body.Close()

	// offset 0: a bundle entry is a whole file, so the resume machinery is not
	// engaged and a non-appending backend is irrelevant here.
	out, err := i.receiver.Receive(ctx, spokeID, e.Path, e.SHA256, e.SizeBytes, 0, body)
	if err != nil {
		return 0, nil, err
	}

	switch out.Outcome {
	case OutcomeCommitted:
		return e.SizeBytes, nil, nil

	case OutcomeAlreadyPresent:
		return -1, nil, nil

	case OutcomeConflict:
		return 0, &Conflict{Path: e.Path, TheirSHA256: out.TheirSHA256}, nil

	case OutcomePartial, OutcomeChecksumMismatch:
		// Verify re-hashed this file moments ago, so a short read or a digest
		// mismatch NOW means the media changed underneath the import — a
		// failing drive, or a race with whoever holds it. Both are the same
		// diagnosis, and both are the operator's to act on: refuse the whole
		// bundle with 422 rather than report a generic internal error.
		return 0, nil, fmt.Errorf("%w: %q changed during import (%s); the media may be failing",
			ErrBundleInvalid, truncateForError(e.Path), out.Outcome)

	default:
		return 0, nil, fmt.Errorf("%w: unexpected outcome %q for %q",
			ErrReceiveInternal, out.Outcome, truncateForError(e.Path))
	}
}

// CollectingRegistrar buffers registered files instead of proposing them.
//
// The seam that makes batching possible without changing Receiver: the online
// hub passes a RegisterFile that proposes to Raft immediately, and the importer
// passes this, draining it at ImportBatchSize.
//
// Not safe for concurrent use. That is sound only because Importer.Import
// holds a mutex for the whole call: one collector is shared across every
// import, so without that lock two overlapping imports would append to the
// same buffer and Reset would truncate the other's pending registrations.
type CollectingRegistrar struct {
	files []*ReceivedFile
}

// NewCollectingRegistrar returns an empty collector.
func NewCollectingRegistrar() *CollectingRegistrar {
	return &CollectingRegistrar{files: make([]*ReceivedFile, 0, ImportBatchSize)}
}

// Register is the ReceiverConfig.RegisterFile hook.
func (c *CollectingRegistrar) Register(_ context.Context, f *ReceivedFile) error {
	c.files = append(c.files, f)
	return nil
}

// Len reports how many files are buffered.
func (c *CollectingRegistrar) Len() int { return len(c.files) }

// Drain returns the buffered files and empties the buffer.
func (c *CollectingRegistrar) Drain() []*ReceivedFile {
	out := c.files
	c.files = make([]*ReceivedFile, 0, ImportBatchSize)
	return out
}

// Reset discards anything buffered, so a new import does not inherit files
// left over from one that failed partway.
func (c *CollectingRegistrar) Reset() { c.files = c.files[:0] }

// writeAck signs and writes the acknowledgment into the bundle directory.
//
// The hub signs with the SAME per-spoke secret the spoke signs with — it is
// symmetric, so the key that lets a spoke prove authorship lets the hub prove
// receipt. No new key material, and a spoke that can make a bundle can check
// the answer.
func (i *Importer) writeAck(ctx context.Context, dir string, m *Manifest, res *ImportResult) error {
	secret, err := i.registry.Secret(ctx, m.SpokeID)
	if err != nil {
		return fmt.Errorf("read spoke secret: %w", err)
	}

	return WriteAck(dir, secret, &Ack{
		BundleID:   m.BundleID,
		SpokeID:    m.SpokeID,
		HubID:      i.hubID,
		ImportedAt: time.Now().UTC().Unix(),
		Paths:      res.AckPaths,
		Conflicts:  res.Conflicts,
	})
}
