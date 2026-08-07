package edgesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// ackName is the acknowledgment file, written into the bundle directory it
// answers.
//
// Inside the bundle rather than beside it so the drive carries one
// self-describing artifact: an operator hands back the same directory they
// were given, and the ack cannot be separated from the bundle it refers to.
const ackName = "ack.json"

// ErrAckInvalid marks an acknowledgment that must not be trusted.
var ErrAckInvalid = errors.New("edgesync: acknowledgment is invalid")

// ErrNoAck means the bundle directory carries no acknowledgment.
//
// Distinct from an invalid one: a drive that has not yet been to the hub is
// the normal case on the outbound leg, not a problem.
var ErrNoAck = errors.New("edgesync: bundle carries no acknowledgment")

// Ack is a hub's signed statement that it holds a bundle's files.
//
// The return leg of the air-gap transport. Without it a spoke has no terminal
// state: `synced` is unreachable, so PruneSynced never prunes and the ledger
// grows without bound on the box least able to receive a site visit.
type Ack struct {
	Version int `json:"version"`

	BundleID string `json:"bundle_id"`
	SpokeID  string `json:"spoke_id"`

	// HubID is the hub that imported the bundle. The spoke checks it against
	// its own configured hub: an ack from somewhere else names files this
	// spoke never sent there.
	HubID string `json:"hub_id"`

	// ImportedAt is when the hub committed the bundle. Bound into the MAC and
	// surfaced to operators, not enforced as a freshness window — an ack rides
	// the same drive back and is subject to the same weeks-long latency.
	ImportedAt int64 `json:"imported_at"`

	// Paths are the spoke-relative paths the hub now holds. Committed and
	// already-present files both appear: from the spoke's point of view they
	// are the same fact — the hub has this file — and only that fact licenses
	// advancing the ledger.
	//
	// Conflicted paths are deliberately ABSENT. A conflict means the hub holds
	// DIFFERENT content at that path, so the spoke's copy has not been
	// delivered and must not be marked synced.
	Paths []string `json:"paths"`

	// Conflicts are reported so the spoke can surface them to an operator
	// without a network round-trip. Not acknowledged, only described.
	Conflicts []Conflict `json:"conflicts,omitempty"`

	// PathsDigest is the canonical digest the MAC binds.
	PathsDigest string `json:"paths_digest"`

	MAC string `json:"mac"`
}

// WriteAck signs an acknowledgment and writes it into the bundle directory.
//
// Written by the hub after a successful import. A failure here is not fatal to
// the import — the files are committed either way — but it does cost the spoke
// its chance to advance, so the caller should say so plainly.
func WriteAck(dir, secret string, a *Ack) error {
	if a == nil {
		return errors.New("edgesync: write ack: nil acknowledgment")
	}
	if err := ValidateBundleID(a.BundleID); err != nil {
		return err
	}

	digest, err := security.AckPathsDigest(a.Paths)
	if err != nil {
		return fmt.Errorf("edgesync: ack paths digest: %w", err)
	}
	a.PathsDigest = digest
	a.Version = BundleVersion

	mac, err := security.ComputeSyncAckHMAC(secret, a.BundleID, a.SpokeID, a.HubID, a.ImportedAt, digest)
	if err != nil {
		return fmt.Errorf("edgesync: sign ack: %w", err)
	}
	a.MAC = mac

	return writeJSONFile(filepath.Join(dir, ackName), a)
}

// ReadAck reads and verifies an acknowledgment from a bundle directory.
//
// Verifies before returning: a caller that gets an Ack back can advance its
// ledger on the strength of it, so an unverified one must never escape.
func ReadAck(dir, secret, expectSpokeID, expectHubID string) (*Ack, error) {
	raw, err := os.ReadFile(filepath.Join(dir, ackName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNoAck
		}
		return nil, fmt.Errorf("edgesync: read ack: %w", err)
	}
	// The same bound the manifest gets: a fixed set of short fields plus a
	// path list already capped by the bundle's own file limit.
	if len(raw) > maxAckBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, over the %d-byte bound",
			ErrAckInvalid, ackName, len(raw), maxAckBytes)
	}

	var a Ack
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, fmt.Errorf("%w: not valid JSON: %v", ErrAckInvalid, err)
	}
	if a.Version != BundleVersion {
		return nil, fmt.Errorf("%w: version %d, this Arc understands %d",
			ErrAckInvalid, a.Version, BundleVersion)
	}
	if err := ValidateBundleID(a.BundleID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAckInvalid, err)
	}

	// Identity checks before the MAC, so a mismatch reports what is actually
	// wrong rather than a generic signature failure.
	if a.SpokeID != expectSpokeID {
		return nil, fmt.Errorf("%w: acknowledges spoke %q, this spoke is %q",
			ErrAckInvalid, truncateForError(a.SpokeID), expectSpokeID)
	}
	if a.HubID != expectHubID {
		// An ack from another hub names files this spoke never sent there.
		return nil, fmt.Errorf("%w: signed by hub %q, this spoke syncs to %q",
			ErrAckInvalid, truncateForError(a.HubID), expectHubID)
	}

	// The digest is RECOMPUTED from the paths rather than trusted: the MAC
	// binds the digest, so a tampered path list with a matching stale digest
	// would otherwise validate and license advancing files the hub never got.
	digest, err := security.AckPathsDigest(a.Paths)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAckInvalid, err)
	}
	if digest != a.PathsDigest {
		return nil, fmt.Errorf("%w: paths digest is %s, ack declares %s",
			ErrAckInvalid, digest, a.PathsDigest)
	}

	if err := security.ValidateSyncAckHMAC(secret, a.BundleID, a.SpokeID, a.HubID,
		a.ImportedAt, a.PathsDigest, a.MAC); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAckInvalid, err)
	}

	// Every acknowledged path must be one this spoke could have sent.
	for _, p := range a.Paths {
		if err := validateSyncPath(p); err != nil {
			return nil, fmt.Errorf("%w: acknowledged path %q: %v",
				ErrAckInvalid, truncateForError(p), err)
		}
	}

	return &a, nil
}

// maxAckBytes bounds the acknowledgment read.
//
// An ack names every acknowledged path, so it scales with the bundle. Sized
// for the default 10,000-file cap at a generous path length, which is the same
// order as the entry list it answers.
const maxAckBytes = 8 << 20

// AckResult summarizes applying one acknowledgment.
type AckResult struct {
	BundleID   string
	HubID      string
	ImportedAt time.Time

	// Synced is how many entries advanced from exported to synced.
	Synced int

	// AlreadySynced is how many acknowledged paths were already synced. The
	// benign replay case: a drive plugged in twice.
	AlreadySynced int

	// Untracked is how many acknowledged paths this ledger does not know. A
	// spoke restored from a backup legitimately produces these.
	Untracked int

	// Discrepancies is how many acknowledged paths this ledger holds in a
	// state the ack cannot advance — in practice, terminally failed. The hub
	// says it HOLDS a file this spoke gave up on, which an operator should
	// see. Counted apart from the two benign cases because collapsing all
	// three hides the only one that means something is wrong.
	Discrepancies int

	Conflicts []Conflict
}

// ApplyAck advances a ledger using a verified acknowledgment.
//
// The caller must have obtained the Ack from ReadAck, which verifies it. This
// function trusts what it is given, so handing it an unverified ack would let a
// tampered path list mark files synced that no hub ever received.
//
// An acknowledged entry that is still `pending` — one the spoke never put on
// the drive, queued for the network path instead — IS advanced, deliberately.
// ReadAck has proven the hub holds that exact path, so `synced` is factually
// true however it got there, and re-sending it over a contact window would
// spend link budget on a file already delivered. The effect is that a drive can
// satisfy work queued for the network, which is the right outcome when both
// transports are enabled.
func ApplyAck(ctx context.Context, l *Ledger, a *Ack, logger zerolog.Logger) (*AckResult, error) {
	if l == nil {
		return nil, errors.New("edgesync: apply ack: nil ledger")
	}
	if a == nil {
		return nil, errors.New("edgesync: apply ack: nil acknowledgment")
	}

	res := &AckResult{
		BundleID:   a.BundleID,
		HubID:      a.HubID,
		ImportedAt: time.Unix(a.ImportedAt, 0).UTC(),
		Conflicts:  a.Conflicts,
	}

	for _, p := range a.Paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := l.MarkSynced(ctx, a.HubID, p); err != nil {
			// Not fatal: an entry the ack names but this ledger cannot advance
			// is something to report, not a reason to abandon the rest of a
			// valid acknowledgment.
			//
			// Classified rather than lumped together. The ledger already
			// distinguishes "no such row" from "wrong state", and the two mean
			// opposite things to an operator.
			switch {
			case errors.Is(err, ErrNotFound):
				res.Untracked++
				logger.Debug().Str("path", p).Msg("Acknowledged path is not tracked by this ledger")
			default:
				// Wrong state. Already-synced is the benign replay case;
				// anything else — in practice a terminally failed entry — is a
				// real discrepancy, because the hub says it holds a file this
				// spoke gave up on.
				entry, getErr := l.Get(ctx, a.HubID, p)
				if getErr == nil && entry.State == StateSynced {
					res.AlreadySynced++
					continue
				}
				res.Discrepancies++
				logger.Warn().Err(err).Str("path", p).
					Msg("The hub acknowledges a file this spoke cannot advance; investigate")
			}
			continue
		}
		res.Synced++
	}

	return res, nil
}
