package edgesync

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrBundleRefused marks a relay bundle this hub refuses for good (#1685):
// the signature does not verify, the bundle is malformed, or it is over this
// hub's limits. Re-sending the same bundle cannot fix any of these, so the
// relay poller writes a refusal note and stops looking at it. Wraps
// ErrBundleInvalid.
//
// Deliberately narrower than ErrBundleInvalid, which also covers an unknown
// or disabled spoke and a bundle addressed to another hub. Those can be one
// registration away from importing, or belong to another hub sharing the
// bucket, so the poller skips them without a note.
var ErrBundleRefused = fmt.Errorf("%w: refused", ErrBundleInvalid)

// errBundleNotOurs marks a relay bundle the poller leaves alone without a
// refusal note: an unknown or disabled spoke, or a bundle for another hub.
var errBundleNotOurs = errors.New("edgesync: relay bundle is not for this hub")

// RelayPrecheck is what a relay hub knows about a bundle once its signed
// metadata has verified, before any data is downloaded.
type RelayPrecheck struct {
	Manifest *Manifest
	// Entries passed VerifyManifest: the spoke signed exactly these.
	Entries []BundleEntry
	// Prior is the dedup record when this bundle was already imported, nil
	// otherwise. A relay hub then rebuilds the ack instead of downloading.
	Prior *ImportedBundle
}

// PrecheckRelay authenticates a bundle directory holding only manifest.json
// and entries.jsonl (#1685): identity, limits, the spoke's registration and
// the MAC over the entries, so nothing an unauthenticated party wrote to the
// bucket is downloaded. Then, and only then, the dedup check.
//
// Errors: ErrBundleRefused (permanent), errBundleNotOurs (skip), anything
// else is transient.
func (i *Importer) PrecheckRelay(ctx context.Context, dir string) (*RelayPrecheck, error) {
	r, err := OpenBundle(dir, i.logger)
	if err != nil {
		if errors.Is(err, ErrBundleInvalid) {
			return nil, fmt.Errorf("%w: %v", ErrBundleRefused, err)
		}
		return nil, err
	}
	m := r.Manifest()
	if m.HubID != i.hubID {
		return nil, fmt.Errorf("%w: addressed to hub %q", errBundleNotOurs, truncateForError(m.HubID))
	}
	if m.EntryCount <= 0 {
		return nil, fmt.Errorf("%w: bundle declares no entries", ErrBundleRefused)
	}
	if m.EntryCount > i.maxFiles {
		return nil, fmt.Errorf("%w: bundle declares %d entries, over this hub's limit of %d",
			ErrBundleRefused, m.EntryCount, i.maxFiles)
	}
	secret, err := i.registry.Secret(ctx, m.SpokeID)
	if err != nil {
		if errors.Is(err, ErrSpokeNotFound) || errors.Is(err, ErrSpokeDisabled) || errors.Is(err, ErrSpokeIDCollision) {
			return nil, fmt.Errorf("%w: spoke %q: %w", errBundleNotOurs, m.SpokeID, err)
		}
		return nil, fmt.Errorf("edgesync: read spoke secret: %w", err)
	}
	entries, err := r.VerifyManifest(ctx, secret)
	if err != nil {
		if errors.Is(err, ErrBundleInvalid) {
			return nil, fmt.Errorf("%w: %v", ErrBundleRefused, err)
		}
		return nil, err
	}
	// Every declared path must be one this spoke may write, before any of
	// them becomes a download key or a staging path.
	for _, e := range entries {
		if err := validateSyncPath(e.Path); err != nil {
			return nil, fmt.Errorf("%w: entry %q: %v", ErrBundleRefused, truncateForError(e.Path), err)
		}
		if !isHexSHA256(e.SHA256) || e.SizeBytes < 0 {
			return nil, fmt.Errorf("%w: entry %q has a malformed digest or size", ErrBundleRefused, truncateForError(e.Path))
		}
	}
	prior, err := i.index.Seen(ctx, m.SpokeID, m.BundleID)
	if err != nil {
		return nil, err
	}
	return &RelayPrecheck{Manifest: m, Entries: entries, Prior: prior}, nil
}

// RebuildAck re-creates the acknowledgment for a bundle this hub already
// imported, whose ack never reached the relay bucket (B1, #1685): the hub
// crashed between commit and ack upload, or the upload failed.
//
// The import's own ack named every path the hub then held with the spoke's
// content, committed or already present, and reported the rest as
// conflicts. That is rebuilt from the hub's receipts, which outlive the
// import: a receipt with the spoke's digest is acknowledged (whatever
// happened to the file since: compaction and removal leave the receipt
// vouching for it, as receive does), a different digest is a conflict, and
// no receipt at all (an admin forget) is left out so the spoke re-sends it.
// The held attestation is the verify answer, as at import.
//
// pre must come from PrecheckRelay on the same bundle, with Prior set.
func (i *Importer) RebuildAck(ctx context.Context, pre *RelayPrecheck) ([]byte, error) {
	if pre == nil || pre.Prior == nil {
		return nil, errors.New("edgesync: rebuild ack: the bundle has not been imported")
	}
	m := pre.Manifest
	paths := make([]string, 0, len(pre.Entries))
	for _, e := range pre.Entries {
		paths = append(paths, e.Path)
	}
	held, err := i.receiver.index.Lookup(ctx, m.SpokeID, paths)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReceiveInternal, err)
	}
	res := &ImportResult{BundleID: m.BundleID, SpokeID: m.SpokeID}
	for _, e := range pre.Entries {
		hf, ok := held[e.Path]
		switch {
		case !ok:
		case hf.SHA256 == e.SHA256:
			res.AckPaths = append(res.AckPaths, e.Path)
		default:
			res.Conflicts = append(res.Conflicts, Conflict{Path: e.Path, TheirSHA256: hf.SHA256})
		}
	}
	ack, secret, err := i.buildAck(ctx, m, pre.Entries, res, pre.Prior.ImportedAt)
	if err != nil {
		return nil, err
	}
	return EncodeAck(secret, ack)
}

// buildAck is writeAck's body without the file: the signed ack for an import
// result, with the held attestation when the hub can verify.
func (i *Importer) buildAck(ctx context.Context, m *Manifest, entries []BundleEntry, res *ImportResult, importedAt time.Time) (*Ack, string, error) {
	secret, err := i.registry.Secret(ctx, m.SpokeID)
	if err != nil {
		return nil, "", fmt.Errorf("read spoke secret: %w", err)
	}

	ack := &Ack{
		BundleID:   m.BundleID,
		SpokeID:    m.SpokeID,
		HubID:      i.hubID,
		ImportedAt: importedAt.UTC().Unix(),
		Paths:      res.AckPaths,
		Conflicts:  res.Conflicts,
	}
	if i.verify != nil && len(res.AckPaths) > 0 {
		if err := i.attestHeld(ctx, m.SpokeID, entries, ack); err != nil {
			// The ack goes out without an attestation: the spoke advances
			// its ledger as before and deletes nothing on its strength.
			i.logger.Warn().Err(err).Str("bundle_id", m.BundleID).
				Msg("Could not verify what this hub holds of the bundle; the acknowledgment carries no held attestation")
			ack.Held, ack.HeldCompacted, ack.VerifiedAt = nil, nil, 0
		}
		if ack.VerifiedAt != 0 && !ackFits(ack) {
			// The held lists roughly double the ack. Past the bound a spoke
			// would reject the WHOLE ack, losing the sync advance too, so
			// the attestation goes and the ack stays.
			i.logger.Warn().Str("bundle_id", m.BundleID).Int("files", len(ack.Paths)).
				Msg("The acknowledgment with a held attestation would exceed the size a spoke accepts; written without it")
			ack.Held, ack.HeldCompacted, ack.VerifiedAt = nil, nil, 0
		}
	}
	return ack, secret, nil
}
