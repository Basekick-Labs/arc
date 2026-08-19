package edgesync

// Issue #610: spoke compaction defers until edge sync has delivered the data.
//
// Local compaction rewrites raw Parquet into a compacted file and deletes the
// sources. On a syncing spoke that used to mean either duplication (raws
// synced, then the compacted file carrying the same rows synced too) or loss
// (raws consumed before they ever synced — since #617 they land `skipped`).
// The gate here closes both by construction: only DELIVERED files are
// eligible compaction inputs, and compacted outputs are recorded as
// never-to-sync. The hub receives every row exactly once, as raws.

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// NewCompactionEligibility returns the sync-eligibility gate for
// compaction.Manager.SetSyncEligibility.
//
// Eligible: state synced (delivered; acked, on the air-gap path), a
// compacted-output row (daily consumes hourly outputs), or a tier-suffixed
// file ABSENT from the ledger whose embedded timestamp is after epoch (a
// crash orphan: produced under this gate, so its inputs were delivered by
// construction). Everything else — pending, in_flight, exported (on a drive,
// unconfirmed), failed, legacy compacted files, and unknown raws — defers.
// The deferral detail is logged here, broken out by state, so a wedged spoke
// is diagnosable from its compaction log.
func NewCompactionEligibility(ledger *Ledger, hubID string, epoch time.Time, logger zerolog.Logger) func(ctx context.Context, paths []string) (map[string]bool, error) {
	log := logger.With().Str("component", "edgesync-compaction-gate").Logger()
	return func(ctx context.Context, paths []string) (map[string]bool, error) {
		states, err := ledger.DeliveryStates(ctx, hubID, paths)
		if err != nil {
			// The caller (compaction.Manager) fails safe on error: the whole
			// partition defers this cycle.
			return nil, err
		}

		out := make(map[string]bool, len(paths))
		deferredByReason := make(map[string]int)
		for _, p := range paths {
			ds, tracked := states[p]
			switch {
			case tracked && ds.State == StateSynced:
				out[p] = true
			case tracked && ds.State == StateSkipped && ds.Note == NoteCompactedOutput:
				out[p] = true
			case tracked && ds.State == StateSkipped && ds.Note == NoteOperatorDismissed:
				// The operator explicitly renounced delivery of this file
				// (POST /spoke-sync/ledger/dismiss), so consuming it locally
				// destroys nothing the hub is still owed — and leaving it
				// ineligible would wedge its partition's compaction forever
				// on a file the operator already wrote off.
				out[p] = true
			case !tracked:
				if ts, isCompacted := compactedFileTimestamp(p); isCompacted && ts.After(epoch) {
					// Post-epoch orphan: the observer insert was lost to a
					// crash, but under this gate its inputs were all synced,
					// so the content is delivered. Discovery re-tracks it as
					// a compacted output on its next pass.
					out[p] = true
				} else if isCompacted {
					deferredByReason["legacy_compacted_unsynced"]++
				} else {
					deferredByReason["not_yet_discovered"]++
				}
			default:
				deferredByReason[string(ds.State)]++
			}
		}

		if len(deferredByReason) > 0 {
			// Debug, not Info: this fires once per deferred candidate per
			// cycle, and a backlogged spoke can have hundreds of candidates
			// twice an hour. The manager's single per-candidate line carries
			// the counts at Info; this adds the by-state detail when needed.
			evt := log.Debug()
			for reason, n := range deferredByReason {
				evt = evt.Int(reason, n)
			}
			evt.Msg("Compaction deferral detail: files await delivery (or an ack) before they may be compacted; " +
				"exported means on a drive awaiting its ack, failed/conflicted need operator attention")
		}
		return out, nil
	}
}

// NewCompactedOutputObserver returns the compacted-output recorder for
// compaction.Manager.SetOnCompactedOutput. Errors are logged, not returned —
// the compaction that produced the output has already succeeded, and a lost
// insert is recovered by discovery's epoch rule on its next pass.
//
// The epoch check exists for the manifest-RECOVERY path: recovery can keep an
// output from a crash that happened BEFORE the gate was active (an upgrade
// with an orphaned manifest, or an ungated period), whose inputs were not
// necessarily delivered. Marking that never-sync would be silent loss — so a
// pre-epoch output is left untracked and discovery's legacy rule syncs it
// once instead. Outputs of gated runs are post-epoch by construction.
func NewCompactedOutputObserver(ledger *Ledger, hubID string, epoch time.Time, logger zerolog.Logger) func(storageKey string) {
	log := logger.With().Str("component", "edgesync-compaction-gate").Logger()
	return func(storageKey string) {
		if ts, ok := compactedFileTimestamp(storageKey); ok && !ts.After(epoch) {
			log.Info().Str("path", storageKey).
				Msg("Recovered compacted output predates the delivery gate; leaving it for discovery to sync once (its inputs may never have been delivered)")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := ledger.TrackCompactedOutput(ctx, hubID, storageKey); err != nil {
			log.Warn().Err(err).Str("path", storageKey).
				Msg("Could not record a compacted output in the sync ledger; discovery's epoch rule recovers it next pass")
			return
		}
		log.Debug().Str("path", storageKey).Msg("Recorded compacted output; it will not sync")
	}
}
