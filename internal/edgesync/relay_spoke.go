package edgesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// DefaultRelayAckTimeout is how long a relay bundle may wait for its ack
// before the spoke takes it back and re-sends its files.
const DefaultRelayAckTimeout = 72 * time.Hour

// RelaySpokeConfig configures the spoke side of the relay transport.
type RelaySpokeConfig struct {
	Exporter *Exporter
	// Relay is the bucket shared with the hub. Must list directories.
	Relay storage.Backend
	// AckTimeout: a bundle with no ack and no refusal this long after its
	// export is reverted (its files go back to pending, re-sent in the next
	// bundle) and removed from the outbox (B2). Zero means 72h.
	AckTimeout time.Duration
	Logger     zerolog.Logger
}

// RelaySpoke runs relay passes: apply acks, act on refusals, take back
// bundles that timed out, sweep partial uploads, then export.
type RelaySpoke struct {
	exporter   *Exporter
	relay      storage.Backend
	lister     storage.DirectoryLister
	ackTimeout time.Duration
	now        func() time.Time
	logger     zerolog.Logger

	// mu single-flights passes: the sweep of manifest-less prefixes is only
	// sound when no export of this node is in flight.
	mu sync.Mutex
}

// NewRelaySpoke validates configuration.
func NewRelaySpoke(cfg RelaySpokeConfig) (*RelaySpoke, error) {
	if cfg.Exporter == nil {
		return nil, errors.New("edgesync: relay spoke requires a bundle exporter")
	}
	if cfg.Relay == nil {
		return nil, errors.New("edgesync: relay spoke requires a relay backend")
	}
	lister, ok := cfg.Relay.(storage.DirectoryLister)
	if !ok {
		return nil, fmt.Errorf("edgesync: relay backend %q cannot list directories", cfg.Relay.Type())
	}
	timeout := cfg.AckTimeout
	if timeout <= 0 {
		timeout = DefaultRelayAckTimeout
	}
	return &RelaySpoke{
		exporter:   cfg.Exporter,
		relay:      cfg.Relay,
		lister:     lister,
		ackTimeout: timeout,
		now:        time.Now,
		logger:     cfg.Logger.With().Str("component", "edgesync-relay-spoke").Logger(),
	}, nil
}

// RelaySpokeResult summarizes one pass.
type RelaySpokeResult struct {
	AcksApplied  int `json:"acks_applied"`
	Synced       int `json:"synced"`
	Attested     int `json:"attested"`
	LocalDeleted int `json:"local_deleted"`
	Conflicts    int `json:"conflicts"`
	// Reverted counts files put back to pending: unacknowledged by an
	// applied ack, or in a bundle that timed out.
	Reverted int `json:"reverted"`
	// Refused counts bundles the hub refused; their files are failed.
	Refused  int `json:"refused"`
	TimedOut int `json:"timed_out"`
	Swept    int `json:"swept"`

	Exported *ExportResult `json:"exported,omitempty"`
}

// Run performs one relay pass. Step failures are logged and the pass goes on,
// so a bad ack cannot stop exports; listing the outbox is the one failure
// that ends it, since every later step depends on it.
func (r *RelaySpoke) Run(ctx context.Context) (*RelaySpokeResult, error) {
	if !r.mu.TryLock() {
		return nil, ErrRelayInProgress
	}
	defer r.mu.Unlock()

	spokeID := r.exporter.spokeID
	res := &RelaySpokeResult{}

	dirs, err := r.lister.ListDirectories(ctx, RelayOutboxDir(spokeID))
	if err != nil {
		return nil, fmt.Errorf("edgesync: list relay outbox: %w", err)
	}
	complete := map[string]bool{}
	var partial []string
	for _, id := range dirs {
		if ValidateBundleID(id) != nil {
			continue
		}
		ok, err := r.relay.Exists(ctx, RelayOutboxPrefix(spokeID, id)+manifestName)
		if err != nil {
			return nil, fmt.Errorf("edgesync: read relay outbox: %w", err)
		}
		if ok {
			complete[id] = true
		} else {
			partial = append(partial, id)
		}
	}

	if acks, err := r.relay.List(ctx, RelayAckDir(spokeID)); err != nil {
		r.logger.Warn().Err(err).Msg("Relay: could not list acknowledgments; retrying next pass")
	} else {
		for id := range idsFromKeys(acks, RelayAckDir(spokeID), ".json") {
			if !complete[id] {
				continue // applied already: the outbox goes only after that
			}
			if r.applyAck(ctx, spokeID, id, res) {
				delete(complete, id)
			}
		}
	}

	if notes, err := r.relay.List(ctx, RelayRejectedDir(spokeID)); err != nil {
		r.logger.Warn().Err(err).Msg("Relay: could not list refusals; retrying next pass")
	} else {
		for id := range idsFromKeys(notes, RelayRejectedDir(spokeID), ".json") {
			if !complete[id] {
				continue
			}
			if r.applyRefusal(ctx, spokeID, id, res) {
				delete(complete, id)
			}
		}
	}

	// B2: a bundle nobody answered. The bundle ID carries its export time.
	for id := range complete {
		created, err := bundleIDTime(id)
		if err != nil || r.now().Sub(created) < r.ackTimeout {
			continue
		}
		n, err := r.exporter.ledger.RevertExported(ctx, r.exporter.hubID, id)
		if err != nil {
			r.logger.Warn().Err(err).Str("bundle_id", id).Msg("Relay: could not take back a bundle with no acknowledgment")
			continue
		}
		res.TimedOut++
		res.Reverted += int(n)
		r.logger.Warn().Str("bundle_id", id).Int64("files", n).Dur("ack_timeout", r.ackTimeout).
			Msg("Relay: no acknowledgment within edge_sync.spoke.relay.ack_timeout; its files go in the next bundle")
		r.removeBundle(ctx, spokeID, id)
		_ = r.exporter.ledger.ForgetRelayBundle(ctx, id)
	}

	// Single-flight makes every manifest-less prefix a leftover: an export
	// that failed partway (no row marked yet), or a removal that crashed
	// after its manifest (rows already settled), or a manifest something
	// else deleted. Reverting is a no-op for the first two and the recovery
	// for the third.
	for _, id := range partial {
		if n, err := r.exporter.ledger.RevertExported(ctx, r.exporter.hubID, id); err == nil {
			res.Reverted += int(n)
		}
		if r.removeBundle(ctx, spokeID, id) {
			res.Swept++
		}
	}

	// A relay bundle that vanished from the bucket entirely (a lifecycle
	// rule, an operator): nothing lists it, so only the ledger knows. Its
	// rows would otherwise stay exported for good. Scoped to bundles this
	// spoke recorded as relay bundles, so a drive in transit is untouched.
	listed := map[string]bool{}
	for _, id := range dirs {
		listed[id] = true
	}
	if recorded, err := r.exporter.ledger.RelayBundles(ctx); err != nil {
		r.logger.Warn().Err(err).Msg("Relay: could not read the relay bundle record; retrying next pass")
	} else {
		for _, id := range recorded {
			if listed[id] {
				continue
			}
			n, err := r.exporter.ledger.RevertExported(ctx, r.exporter.hubID, id)
			if err != nil {
				continue
			}
			if n > 0 {
				res.Reverted += int(n)
				r.logger.Warn().Str("bundle_id", id).Int64("files", n).
					Msg("Relay: a bundle disappeared from the bucket before it was answered; its files go in the next bundle")
			}
			_ = r.exporter.ledger.ForgetRelayBundle(ctx, id)
		}
	}

	exp, err := r.exporter.ExportToRelay(ctx, r.relay, 0)
	switch {
	case err == nil:
		res.Exported = exp
		if err := r.exporter.ledger.RecordRelayBundle(ctx, exp.BundleID); err != nil {
			r.logger.Warn().Err(err).Str("bundle_id", exp.BundleID).
				Msg("Relay: could not record the bundle; if it vanishes from the bucket its files need POST /api/v1/spoke-sync/export/{id}/revert")
		}
	case errors.Is(err, ErrNothingToExport):
	default:
		return res, fmt.Errorf("edgesync: relay export: %w", err)
	}
	return res, nil
}

// applyAck verifies and applies one ack, settles what it did not advance,
// and removes the bundle. Reports whether the bundle is done.
func (r *RelaySpoke) applyAck(ctx context.Context, spokeID, bundleID string, res *RelaySpokeResult) bool {
	log := r.logger.With().Str("bundle_id", bundleID).Logger()
	raw, err := readBounded(ctx, r.relay, RelayAckKey(spokeID, bundleID), MaxAckBytes)
	if err != nil {
		log.Warn().Err(err).Msg("Relay: could not read an acknowledgment; retrying next pass")
		return false
	}
	ack, err := DecodeAck(raw, r.exporter.secret, spokeID, r.exporter.hubID)
	if err == nil && ack.BundleID != bundleID {
		err = fmt.Errorf("%w: stored as %s but acknowledges %s", ErrAckInvalid, bundleID, truncateForError(ack.BundleID))
	}
	if err != nil {
		// Not trusted, not acted on. The bundle waits; ack_timeout takes it
		// back if no valid ack ever comes.
		log.Error().Err(err).Msg("Relay: acknowledgment failed verification; ignoring it")
		return false
	}

	// Only paths that left on THIS bundle. A signed ack is replayable by
	// anyone with bucket write, and ApplyAck would otherwise advance any row
	// at a named path, pending ones included (a file the network verify
	// re-queued because the hub forgot it would then never be re-sent).
	onBundle, err := r.exporter.ledger.ExportedOnBundle(ctx, r.exporter.hubID, bundleID)
	if err != nil {
		log.Warn().Err(err).Msg("Relay: could not read the bundle's rows; retrying next pass")
		return false
	}
	scoped := *ack
	scoped.Paths = keepOnBundle(ack.Paths, onBundle)
	scoped.Held = keepOnBundle(ack.Held, onBundle)
	scoped.HeldCompacted = keepOnBundle(ack.HeldCompacted, onBundle)

	ar, err := r.exporter.applyVerifiedAck(ctx, &scoped)
	if err != nil {
		log.Warn().Err(err).Msg("Relay: could not apply an acknowledgment; retrying next pass")
		return false
	}
	res.AcksApplied++
	res.Synced += ar.Synced
	res.Attested += ar.Attested
	res.LocalDeleted += ar.LocalDeleted

	// What the ack did not advance must not stay exported on a bundle about
	// to be deleted: nothing would ever move it, and with
	// defer_compaction_until_synced it would block compaction for good.
	// A conflict is terminal (as on the network path); anything else goes
	// back to pending and rides the next bundle.
	if len(ack.Conflicts) > 0 {
		paths := make([]string, 0, len(ack.Conflicts))
		for _, c := range ack.Conflicts {
			paths = append(paths, c.Path)
		}
		n, err := r.exporter.ledger.FailExported(ctx, r.exporter.hubID, bundleID, paths, "conflict: hub holds different content")
		if err != nil {
			log.Warn().Err(err).Msg("Relay: could not mark conflicted files failed; retrying next pass")
			return false
		}
		res.Conflicts += int(n)
	}
	n, err := r.exporter.ledger.RevertExported(ctx, r.exporter.hubID, bundleID)
	if err != nil {
		log.Warn().Err(err).Msg("Relay: could not put unacknowledged files back to pending; retrying next pass")
		return false
	}
	res.Reverted += int(n)
	r.removeBundle(ctx, spokeID, bundleID)
	_ = r.exporter.ledger.ForgetRelayBundle(ctx, bundleID)
	return true
}

func keepOnBundle(paths []string, on map[string]bool) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if on[p] {
			out = append(out, p)
		}
	}
	return out
}

// applyRefusal fails a refused bundle's files, so they are not re-sent until
// an operator requeues them, and removes the bundle.
func (r *RelaySpoke) applyRefusal(ctx context.Context, spokeID, bundleID string, res *RelaySpokeResult) bool {
	log := r.logger.With().Str("bundle_id", bundleID).Logger()
	raw, err := readBounded(ctx, r.relay, RelayRejectedKey(spokeID, bundleID), maxRelayNoteBytes)
	if err != nil {
		log.Warn().Err(err).Msg("Relay: could not read a refusal note; retrying next pass")
		return false
	}
	var note RelayRefusal
	err = json.Unmarshal(raw, &note)
	if err == nil {
		err = note.verify(r.exporter.secret, spokeID, r.exporter.hubID, bundleID)
	}
	if err != nil {
		// Not from the hub, or not for this bundle: failing the rows on it
		// would need an operator to undo. The bundle waits; ack_timeout takes
		// it back if the hub never answers.
		log.Error().Err(err).Msg("Relay: refusal note failed verification; ignoring it")
		return false
	}
	reason := truncateReason(strings.TrimSpace(note.Reason))
	n, err := r.exporter.ledger.FailExported(ctx, r.exporter.hubID, bundleID, nil, "relay: hub refused the bundle: "+reason)
	if err != nil {
		log.Warn().Err(err).Msg("Relay: could not mark a refused bundle's files failed; retrying next pass")
		return false
	}
	res.Refused++
	log.Error().Str("reason", reason).Int64("files", n).
		Msg("Relay: the hub refused this bundle; its files are failed. Fix the cause, then requeue them (POST /api/v1/spoke-sync/ledger/requeue)")
	r.removeBundle(ctx, spokeID, bundleID)
	_ = r.exporter.ledger.ForgetRelayBundle(ctx, bundleID)
	return true
}

// removeBundle deletes one outbox bundle, manifest first: once it is gone the
// hub no longer sees a complete bundle, whatever is left. Reports whether
// anything was deleted.
func (r *RelaySpoke) removeBundle(ctx context.Context, spokeID, bundleID string) bool {
	prefix := RelayOutboxPrefix(spokeID, bundleID)
	keys, err := r.relay.List(ctx, prefix)
	if err != nil {
		r.logger.Warn().Err(err).Str("bundle_id", bundleID).Msg("Relay: could not list a bundle to remove it; retrying next pass")
		return false
	}
	manifest := prefix + manifestName
	sort.SliceStable(keys, func(i, j int) bool { return keys[i] == manifest && keys[j] != manifest })
	deleted := false
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if err := r.relay.Delete(ctx, k); err != nil && !storage.IsNotFound(err) {
			r.logger.Warn().Err(err).Str("key", k).Msg("Relay: could not delete a bundle object; retrying next pass")
			return deleted
		}
		deleted = true
	}
	// A local relay stages uploads as hidden partials that List does not
	// show; an upload that died mid-write leaves one behind.
	if si, ok := r.relay.(storage.StagingInspector); ok {
		if staged, err := si.ListStaged(ctx, prefix); err == nil {
			for _, o := range staged {
				if strings.HasPrefix(o.Path, prefix) && si.DeleteStaged(ctx, o.Path) == nil {
					deleted = true
				}
			}
		}
	}
	pruneEmptyLocalDirs(r.relay, prefix)
	return deleted
}

// pruneEmptyLocalDirs removes the directories a local relay backend leaves
// behind once a bundle's files are deleted, so a listing does not keep
// showing an empty bundle. Object stores have no directories.
func pruneEmptyLocalDirs(b storage.Backend, prefix string) {
	lb, ok := b.(interface{ GetBasePath() string })
	if !ok {
		return
	}
	root := filepath.Join(lb.GetBasePath(), filepath.FromSlash(strings.TrimSuffix(prefix, "/")))
	var dirs []string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i]) // fails, harmlessly, on a non-empty one
	}
}

// readBounded reads a small object, refusing one over max bytes before and
// during the read.
func readBounded(ctx context.Context, b storage.Backend, key string, max int64) ([]byte, error) {
	size, err := b.StatFile(ctx, key)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, fmt.Errorf("edgesync: %s does not exist", key)
	}
	if size > max {
		return nil, fmt.Errorf("edgesync: %s is %d bytes, over the %d-byte bound", key, size, max)
	}
	var buf boundedBuffer
	buf.max = max
	if err := b.ReadTo(ctx, key, &buf); err != nil {
		return nil, err
	}
	return buf.b, nil
}

type boundedBuffer struct {
	b   []byte
	max int64
}

func (w *boundedBuffer) Write(p []byte) (int, error) {
	if int64(len(w.b)+len(p)) > w.max {
		return 0, errors.New("edgesync: object larger than its bound")
	}
	w.b = append(w.b, p...)
	return len(p), nil
}

// bundleIDTime decodes the millisecond timestamp a bundle ID starts with.
func bundleIDTime(id string) (time.Time, error) {
	if err := ValidateBundleID(id); err != nil {
		return time.Time{}, err
	}
	b, err := crockford.DecodeString(id)
	if err != nil || len(b) < 6 {
		return time.Time{}, fmt.Errorf("edgesync: bundle ID %q has no timestamp", id)
	}
	ms := int64(b[0])<<40 | int64(b[1])<<32 | int64(b[2])<<24 | int64(b[3])<<16 | int64(b[4])<<8 | int64(b[5])
	return time.UnixMilli(ms).UTC(), nil
}
