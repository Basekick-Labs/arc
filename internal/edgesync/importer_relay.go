package edgesync

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrBundleRefused marks a relay bundle this hub refuses for good (#1685):
// its signed metadata does not verify, is malformed, names another key than
// the one it was found under, or is over this hub's limits. All are facts
// about what the spoke signed, so re-sending the same bundle cannot change
// them; the relay poller writes a signed refusal note and stops looking at
// it. Wraps ErrBundleInvalid.
//
// Deliberately narrower than ErrBundleInvalid. Nothing found AFTER the
// metadata verified is a refusal: a data object that is missing, the wrong
// size or the wrong content, a staging-disk error, or a spoke disabled
// between precheck and import can all be cured by retrying or by the spoke
// re-sending (its ack_timeout), and a refusal would instead fail the rows
// until an operator requeues them.
var ErrBundleRefused = fmt.Errorf("%w: refused", ErrBundleInvalid)

// errBundleNotOurs marks a relay bundle the poller leaves alone without a
// refusal note: an unknown or disabled spoke, or a bundle for another hub.
// Either can be one registration away from importing, or belong to another
// hub sharing the bucket.
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
	// Secret is the spoke's, for signing a refusal or an ack.
	Secret string
}

// relaySpokeSecret is the first check on a relay key, before anything is
// fetched: a prefix that names no enabled, registered spoke costs one
// registry lookup and no download.
func (i *Importer) relaySpokeSecret(ctx context.Context, spokeID string) (string, error) {
	secret, err := i.registry.Secret(ctx, spokeID)
	if err != nil {
		if errors.Is(err, ErrSpokeNotFound) || errors.Is(err, ErrSpokeDisabled) || errors.Is(err, ErrSpokeIDCollision) {
			return "", fmt.Errorf("%w: spoke %q: %w", errBundleNotOurs, spokeID, err)
		}
		return "", fmt.Errorf("edgesync: read spoke secret: %w", err)
	}
	return secret, nil
}

// checkRelayManifest checks a staged manifest.json alone, before the entry
// list is fetched: the hub it names (not ours: skip), the key it was found
// under, and the declared size.
func (i *Importer) checkRelayManifest(dir, spokeID, bundleID string) (*Manifest, error) {
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
	// The key the bundle was found under must be the bundle it signs: a
	// manifest copied into another prefix would otherwise be acknowledged
	// where its spoke never looks.
	if m.SpokeID != spokeID || m.BundleID != bundleID {
		return nil, fmt.Errorf("%w: found under %s/%s but signed as spoke %q bundle %q",
			ErrBundleRefused, spokeID, bundleID, truncateForError(m.SpokeID), truncateForError(m.BundleID))
	}
	if m.EntryCount <= 0 {
		return nil, fmt.Errorf("%w: bundle declares no entries", ErrBundleRefused)
	}
	if m.EntryCount > i.maxFiles {
		return nil, fmt.Errorf("%w: bundle declares %d entries, over this hub's limit of %d",
			ErrBundleRefused, m.EntryCount, i.maxFiles)
	}
	return m, nil
}

// PrecheckRelay authenticates a bundle directory holding only manifest.json
// and entries.jsonl (#1685), after checkRelayManifest passed: the MAC over the
// entries, so nothing an unauthenticated party wrote is downloaded, and every
// declared path. Then, and only then, the dedup check.
//
// Errors: ErrBundleRefused (permanent), errBundleNotOurs (skip), anything
// else is transient.
func (i *Importer) PrecheckRelay(ctx context.Context, dir, secret string) (*RelayPrecheck, error) {
	r, err := OpenBundle(dir, i.logger)
	if err != nil {
		if errors.Is(err, ErrBundleInvalid) {
			return nil, fmt.Errorf("%w: %v", ErrBundleRefused, err)
		}
		return nil, err
	}
	m := r.Manifest()
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
	return &RelayPrecheck{Manifest: m, Entries: entries, Prior: prior, Secret: secret}, nil
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
