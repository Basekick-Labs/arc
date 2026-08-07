package edgesync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// Bundle export defaults, applied when a config value is zero.
const (
	DefaultBundleMaxFiles = 10000
	DefaultBundleMaxBytes = int64(64) << 30 // 64 GiB, roughly a large drive
)

// ExporterConfig configures air-gap export.
type ExporterConfig struct {
	Ledger *Ledger
	Writer *BundleWriter
	Policy *DestinationPolicy

	// Discoverer finds new local files before an export selects from them.
	//
	// Required: an air-gapped spoke runs no sync agent, so without its own
	// discovery the ledger would never be populated and every export would
	// answer "nothing to export" while files piled up on disk.
	Discoverer *Discoverer

	HubID string

	// MaxFiles and MaxBytes cap one bundle. Zero uses the defaults above.
	MaxFiles int
	MaxBytes int64

	Logger zerolog.Logger
}

// Exporter writes pending ledger entries to an air-gap bundle.
//
// The export half of the sneakernet transport: it selects what to send, writes
// it, and advances the ledger. It deliberately does NOT delete anything — sync
// is a copy, and local retention stays the operator's decision.
type Exporter struct {
	ledger     *Ledger
	writer     *BundleWriter
	policy     *DestinationPolicy
	discoverer *Discoverer
	hubID      string

	// spokeID and secret are needed to VERIFY a returning acknowledgment. The
	// writer holds its own copies for signing; these are read-side only.
	spokeID string
	secret  string

	maxFiles int
	maxBytes int64
	logger   zerolog.Logger
}

// NewExporter validates configuration and returns a ready exporter.
func NewExporter(cfg ExporterConfig) (*Exporter, error) {
	if cfg.Ledger == nil {
		return nil, fmt.Errorf("edgesync: exporter requires a ledger")
	}
	if cfg.Writer == nil {
		return nil, fmt.Errorf("edgesync: exporter requires a bundle writer")
	}
	if cfg.Policy == nil {
		// Without a policy every operator-supplied path would reach the
		// filesystem unchecked. Refuse rather than default to permissive.
		return nil, fmt.Errorf("edgesync: exporter requires a destination policy")
	}
	if cfg.Discoverer == nil {
		return nil, fmt.Errorf("edgesync: exporter requires a discoverer")
	}

	hubID := cfg.HubID
	if hubID == "" {
		hubID = DefaultHubID
	}
	maxFiles := cfg.MaxFiles
	if maxFiles <= 0 {
		maxFiles = DefaultBundleMaxFiles
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultBundleMaxBytes
	}

	return &Exporter{
		ledger:     cfg.Ledger,
		writer:     cfg.Writer,
		policy:     cfg.Policy,
		discoverer: cfg.Discoverer,
		hubID:      hubID,
		spokeID:    cfg.Writer.spokeID,
		secret:     cfg.Writer.secret,
		maxFiles:   maxFiles,
		maxBytes:   maxBytes,
		logger:     cfg.Logger.With().Str("component", "edgesync-export").Logger(),
	}, nil
}

// Export writes one bundle to dest and marks its entries exported.
//
// The ledger advance happens AFTER the bundle is written and signed. The
// reverse order would mark files exported that a failed write never included,
// and only an operator noticing the gap would ever bring them back.
func (e *Exporter) Export(ctx context.Context, dest string, limit int) (*ExportResult, error) {
	resolved, err := e.policy.Resolve(dest)
	if err != nil {
		return nil, err
	}

	if limit <= 0 || limit > e.maxFiles {
		limit = e.maxFiles
	}

	// Discover before selecting. On an air-gapped spoke this is the ONLY thing
	// that ever populates the ledger — there is no sync agent doing it on a
	// tick — so skipping it would mean every export found nothing while files
	// accumulated on disk.
	discovered, err := e.discoverer.Discover(ctx)
	if err != nil {
		return nil, fmt.Errorf("edgesync: discover before export: %w", err)
	}
	if discovered > 0 {
		e.logger.Info().Int("discovered", discovered).Msg("Discovered new files before export")
	}

	entries, err := e.ledger.Unexported(ctx, e.hubID, limit)
	if err != nil {
		return nil, fmt.Errorf("edgesync: read unexported: %w", err)
	}
	if len(entries) == 0 {
		return nil, ErrNothingToExport
	}

	// Byte cap applied by truncating the selection, not by failing: a backlog
	// larger than one drive is the expected case, and the operator's next
	// bundle continues from where this one stopped. Entries are newest-first,
	// so a truncated bundle carries the freshest telemetry.
	//
	// At least one file is always included, even if it alone exceeds the cap —
	// otherwise a single large file would wedge the queue permanently.
	var (
		selected []*LedgerEntry
		total    int64
	)
	for i, entry := range entries {
		if i > 0 && total+entry.SizeBytes > e.maxBytes {
			break
		}
		selected = append(selected, entry)
		total += entry.SizeBytes
	}
	if len(selected) < len(entries) {
		e.logger.Info().
			Int("selected", len(selected)).
			Int("eligible", len(entries)).
			Int64("max_bytes", e.maxBytes).
			Msg("Bundle truncated at the byte cap; the remainder goes in the next one")
	}

	res, err := e.writer.Export(ctx, resolved, selected, time.Now())
	if err != nil {
		return nil, err
	}

	// A file that cannot be marked is NOT fatal: it is already in a signed,
	// verified bundle on disk. Leaving it pending means a later bundle carries
	// it again and the hub answers already_present — wasteful, not wrong.
	// Failing here would instead discard a bundle that is entirely valid.
	marked := 0
	for _, entry := range selected {
		if err := e.ledger.MarkExported(ctx, e.hubID, entry.Path, res.BundleID); err != nil {
			e.logger.Warn().Err(err).
				Str("path", entry.Path).
				Str("bundle_id", res.BundleID).
				Msg("Bundle written but the ledger entry could not be marked exported")
			continue
		}
		marked++
	}
	if marked != len(selected) {
		e.logger.Warn().
			Int("marked", marked).
			Int("files", len(selected)).
			Str("bundle_id", res.BundleID).
			Msg("Some entries stayed pending; a later bundle will include them again")
	}

	return res, nil
}

// ErrNothingToExport means no file is eligible for a bundle.
//
// Distinct from an error: a spoke with nothing new is the steady state once a
// backlog has drained, and an operator running a scheduled export should not
// see a failure for it.
var ErrNothingToExport = errors.New("edgesync: nothing to export")

// Revert returns a bundle's entries to pending, for a drive that never arrived.
func (e *Exporter) Revert(ctx context.Context, bundleID string) (int64, error) {
	if err := ValidateBundleID(bundleID); err != nil {
		return 0, err
	}
	n, err := e.ledger.RevertExported(ctx, e.hubID, bundleID)
	if err != nil {
		return 0, err
	}
	e.logger.Info().
		Str("bundle_id", bundleID).
		Int64("files", n).
		Msg("Bundle reverted to pending")
	return n, nil
}

// Status reports the ledger summary for this hub.
//
// Mirrors Agent.Status: both are pure ledger reads needing no transport, and
// an air-gap-only spoke has no agent to ask. Without this the operator who
// most needs the `exported` count — the one whose files are on a drive
// somewhere — would be the only one unable to see it.
func (e *Exporter) Status(ctx context.Context) (*Stats, error) {
	return e.ledger.Stats(ctx, e.hubID)
}

// UnfinishedEntries returns files that have not reached the hub, given-up ones
// first. Mirrors Agent.UnfinishedEntries.
func (e *Exporter) UnfinishedEntries(ctx context.Context, limit int) ([]*LedgerEntry, error) {
	return e.ledger.Unfinished(ctx, e.hubID, limit)
}

// ApplyAck reads an acknowledgment from a returned bundle directory and
// advances the ledger.
//
// The return leg. An operator plugs the drive back into the spoke after it has
// been to the hub, and this is what finally moves those files from `exported`
// to `synced` — making them prunable, which is the only thing that stops an
// air-gap ledger growing without bound.
//
// The destination policy applies here too: the path is operator-supplied and
// reaches the filesystem directly, exactly as on the export side.
func (e *Exporter) ApplyAck(ctx context.Context, dir string) (*AckResult, error) {
	resolved, err := e.policy.Resolve(dir)
	if err != nil {
		return nil, err
	}

	// ReadAck verifies before returning, so what reaches ApplyAck is already
	// proven to come from the configured hub and to name only paths this spoke
	// could have sent.
	ack, err := ReadAck(resolved, e.secret, e.spokeID, e.hubID)
	if err != nil {
		return nil, err
	}

	res, err := ApplyAck(ctx, e.ledger, ack, e.logger)
	if err != nil {
		return nil, err
	}

	e.logger.Info().
		Str("bundle_id", res.BundleID).
		Int("synced", res.Synced).
		Int("unknown", res.Unknown).
		Int("conflicts", len(res.Conflicts)).
		Msg("Acknowledgment applied")

	return res, nil
}
