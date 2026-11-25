package edgesync

import (
	"context"
	"errors"
	"time"
)

// Relay bucket layout (edge sync 3d, #1685). Keys are relative to the relay
// backend, whose own prefix (s3_prefix, azure_prefix, gcs_prefix, or local_path) is the
// relay's root: there is no second prefix to compose with it, so a key is
// built in exactly one place.
//
//	<spoke_id>/outbox/<bundle_id>/manifest.json   spoke writes, hub reads
//	<spoke_id>/outbox/<bundle_id>/entries.jsonl
//	<spoke_id>/outbox/<bundle_id>/data/<path>
//	<spoke_id>/acks/<bundle_id>.json              hub writes, spoke reads
//	<spoke_id>/rejected/<bundle_id>.json          hub writes, spoke reads
//
// A bundle is complete only once its manifest exists: the writer puts it
// last. Every function here takes IDs the caller has validated
// (validateSpokeID, ValidateBundleID); they are joined, not sanitized.
const (
	relayOutbox   = "outbox"
	relayAcks     = "acks"
	relayRejected = "rejected"
)

// RelaySpokePrefix is one spoke's root in the relay bucket, with a trailing
// separator.
func RelaySpokePrefix(spokeID string) string { return spokeID + "/" }

// RelayOutboxDir is the prefix listing a spoke's outbox bundles.
func RelayOutboxDir(spokeID string) string { return spokeID + "/" + relayOutbox + "/" }

// RelayOutboxPrefix is one bundle's prefix in a spoke's outbox, with a
// trailing separator, so a prefix match cannot reach a sibling bundle.
func RelayOutboxPrefix(spokeID, bundleID string) string {
	return RelayOutboxDir(spokeID) + bundleID + "/"
}

// RelayAckDir is the prefix listing a spoke's acknowledgments.
func RelayAckDir(spokeID string) string { return spokeID + "/" + relayAcks + "/" }

// RelayAckKey is one bundle's acknowledgment object.
func RelayAckKey(spokeID, bundleID string) string { return RelayAckDir(spokeID) + bundleID + ".json" }

// RelayRejectedDir is the prefix listing a spoke's refused bundles.
func RelayRejectedDir(spokeID string) string { return spokeID + "/" + relayRejected + "/" }

// RelayRejectedKey is the hub's refusal note for one bundle.
func RelayRejectedKey(spokeID, bundleID string) string {
	return RelayRejectedDir(spokeID) + bundleID + ".json"
}

// relayRunner adapts a relay pass to the scheduler's SyncRunner, so the
// relay reuses its per-tick licence and writer gates and its backoff rather
// than a third ticker loop. failed counts bundles a pass could not process:
// non-zero makes the scheduler back off.
type relayRunner func(ctx context.Context) (failed int, err error)

func (r relayRunner) Run(ctx context.Context) (*RunResult, error) {
	start := time.Now()
	failed, err := r(ctx)
	if errors.Is(err, ErrRelayInProgress) {
		// A manual pass holds the lock: the scheduler skips this tick.
		return nil, ErrAgentRunInProgress
	}
	if err != nil {
		return nil, err
	}
	return &RunResult{Failed: failed, Duration: time.Since(start)}, nil
}

// Runner returns the spoke relay pass as a scheduler SyncRunner.
func (r *RelaySpoke) Runner() SyncRunner {
	return relayRunner(func(ctx context.Context) (int, error) {
		res, err := r.Run(ctx)
		if err != nil {
			return 0, err
		}
		if res.Exported != nil || res.AcksApplied > 0 || res.Refused > 0 || res.TimedOut > 0 {
			r.logger.Info().Int("acks_applied", res.AcksApplied).Int("synced", res.Synced).
				Int("refused", res.Refused).Int("timed_out", res.TimedOut).
				Bool("exported", res.Exported != nil).Msg("Relay pass")
		}
		return 0, nil
	})
}

// Runner returns the hub relay poll as a scheduler SyncRunner.
func (p *RelayPoller) Runner() SyncRunner {
	return relayRunner(func(ctx context.Context) (int, error) {
		res, err := p.Run(ctx)
		if err != nil {
			return 0, err
		}
		if res.Imported+res.Reacked+res.Refused > 0 {
			p.logger.Info().Int("imported", res.Imported).Int("reacked", res.Reacked).
				Int("refused", res.Refused).Int("failed", res.Failed).Msg("Relay poll")
		}
		return res.Failed, nil
	})
}
