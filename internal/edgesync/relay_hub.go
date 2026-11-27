package edgesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// maxRelayEntriesBytes bounds the entries.jsonl download, before its MAC can
// be checked. A line is a path, a digest and a size; at the 1 MiB line cap
// the reader already enforces, the default 10,000-file bundle is far below
// this. It is the bound on what an unauthenticated party can make the hub
// fetch per bundle per poll.
const maxRelayEntriesBytes = 64 << 20

// maxRelayNoteBytes bounds a refusal note read by the spoke.
const maxRelayNoteBytes = 64 << 10

// ErrRelayInProgress means a relay pass is already running on this node.
var ErrRelayInProgress = errors.New("edgesync: a relay pass is already in progress")

// RelayRefusal is the note a hub writes to <spoke>/rejected/<bundle>.json
// when it refuses a relay bundle for good (#1685). Signed with the spoke's
// secret: a spoke fails the bundle's rows on it, which takes an operator to
// undo, so bucket write access alone must not be able to plant one. A spoke
// ignores a note that does not verify, and ack_timeout takes the bundle back.
type RelayRefusal struct {
	BundleID  string `json:"bundle_id"`
	SpokeID   string `json:"spoke_id"`
	HubID     string `json:"hub_id"`
	Reason    string `json:"reason"`
	RefusedAt int64  `json:"refused_at"`
	MAC       string `json:"mac"`
}

func (n *RelayRefusal) sign(secret string) error {
	mac, err := security.ComputeSyncRelayRefusalHMAC(secret, n.BundleID, n.SpokeID, n.HubID, n.RefusedAt, n.Reason)
	if err != nil {
		return err
	}
	n.MAC = mac
	return nil
}

// verify checks the note's MAC and that it names this spoke, hub and bundle.
func (n *RelayRefusal) verify(secret, spokeID, hubID, bundleID string) error {
	if n.SpokeID != spokeID || n.HubID != hubID || n.BundleID != bundleID {
		return fmt.Errorf("refusal names spoke %q hub %q bundle %q", truncateForError(n.SpokeID),
			truncateForError(n.HubID), truncateForError(n.BundleID))
	}
	return security.ValidateSyncRelayRefusalHMAC(secret, n.BundleID, n.SpokeID, n.HubID, n.RefusedAt, n.Reason, n.MAC)
}

// RelayPollerConfig configures the hub side of the relay transport.
type RelayPollerConfig struct {
	// Relay is the bucket spokes write bundles to. Must list directories.
	Relay storage.Backend
	// Importer commits bundles exactly as a drive import does.
	Importer *Importer
	// StagingDir holds one bundle at a time while it is verified and
	// imported. Created 0700; each bundle gets a fresh subdirectory, removed
	// on every exit.
	StagingDir string
	HubID      string
	Logger     zerolog.Logger
}

// RelayPoller imports relay bundles and writes their acks.
//
// One download per bundle, staged locally, so the importer's filesystem
// defences and verify-before-commit apply unchanged: the signed metadata is
// fetched and its MAC checked first, then every declared data object is
// downloaded (capped at its declared size), then Import runs as for a drive.
type RelayPoller struct {
	relay    storage.Backend
	lister   storage.DirectoryLister
	importer *Importer
	staging  string
	hubID    string
	logger   zerolog.Logger

	mu sync.Mutex
	// notOurs remembers bundles addressed to another hub ("spoke/bundle"),
	// so a bucket shared by several hubs costs one manifest fetch per foreign
	// bundle, not one per poll. Pruned to the bundles still listed.
	notOurs map[string]bool
}

// NewRelayPoller validates configuration and prepares the staging directory,
// sweeping anything a previous process left there.
func NewRelayPoller(cfg RelayPollerConfig) (*RelayPoller, error) {
	if cfg.Relay == nil {
		return nil, errors.New("edgesync: relay poller requires a relay backend")
	}
	lister, ok := cfg.Relay.(storage.DirectoryLister)
	if !ok {
		return nil, fmt.Errorf("edgesync: relay backend %q cannot list directories", cfg.Relay.Type())
	}
	if cfg.Importer == nil {
		return nil, errors.New("edgesync: relay poller requires a bundle importer")
	}
	if cfg.StagingDir == "" {
		return nil, errors.New("edgesync: relay poller requires a staging directory")
	}
	if cfg.HubID == "" {
		return nil, errors.New("edgesync: relay poller requires a hub ID")
	}
	if err := os.MkdirAll(cfg.StagingDir, 0o700); err != nil {
		return nil, fmt.Errorf("edgesync: create relay staging directory: %w", err)
	}
	// Leftovers from a crash are bundles mid-download: never imported from
	// here, and re-fetched on the next poll.
	if left, err := filepath.Glob(filepath.Join(cfg.StagingDir, "relay-*")); err == nil {
		for _, d := range left {
			_ = os.RemoveAll(d)
		}
	}
	return &RelayPoller{
		relay:    cfg.Relay,
		lister:   lister,
		importer: cfg.Importer,
		staging:  cfg.StagingDir,
		hubID:    cfg.HubID,
		logger:   cfg.Logger.With().Str("component", "edgesync-relay-hub").Logger(),
		notOurs:  map[string]bool{},
	}, nil
}

// RelayPollResult summarizes one poll.
type RelayPollResult struct {
	Imported   int `json:"imported"`
	Reacked    int `json:"reacked"`
	Refused    int `json:"refused"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
	AcksPruned int `json:"acks_pruned"`
}

// Run polls the relay bucket once. A failure on one bundle is logged and
// counted, and the poll moves on; only a failure to list the bucket root is
// returned.
func (p *RelayPoller) Run(ctx context.Context) (*RelayPollResult, error) {
	if !p.mu.TryLock() {
		return nil, ErrRelayInProgress
	}
	defer p.mu.Unlock()

	spokes, err := p.lister.ListDirectories(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("edgesync: list relay bucket: %w", err)
	}
	res := &RelayPollResult{}
	for _, spokeID := range spokes {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if validateSpokeID(spokeID) != nil {
			continue
		}
		if err := p.pollSpoke(ctx, spokeID, res); err != nil {
			res.Failed++
			p.logger.Warn().Err(err).Str("spoke_id", spokeID).
				Msg("Relay: could not read this spoke's prefix; retrying next poll")
		}
	}
	return res, nil
}

// listIDs returns the bundle IDs named by keys "<dir><id><suffix>".
func (p *RelayPoller) listIDs(ctx context.Context, dir, suffix string) (map[string]bool, error) {
	keys, err := p.relay.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	return idsFromKeys(keys, dir, suffix), nil
}

func idsFromKeys(keys []string, dir, suffix string) map[string]bool {
	out := map[string]bool{}
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, dir)
		if !ok {
			continue
		}
		id, ok := strings.CutSuffix(rest, suffix)
		if !ok || ValidateBundleID(id) != nil {
			continue
		}
		out[id] = true
	}
	return out
}

func (p *RelayPoller) pollSpoke(ctx context.Context, spokeID string, res *RelayPollResult) error {
	bundles, err := p.lister.ListDirectories(ctx, RelayOutboxDir(spokeID))
	if err != nil {
		return err
	}
	acked, err := p.listIDs(ctx, RelayAckDir(spokeID), ".json")
	if err != nil {
		return err
	}
	refused, err := p.listIDs(ctx, RelayRejectedDir(spokeID), ".json")
	if err != nil {
		return err
	}

	complete := map[string]bool{}
	for _, id := range bundles {
		if ValidateBundleID(id) != nil {
			continue
		}
		ok, err := p.relay.Exists(ctx, RelayOutboxPrefix(spokeID, id)+manifestName)
		if err != nil {
			return err
		}
		if ok {
			complete[id] = true
		}
	}

	// GC (H4): an ack or refusal note goes once its bundle's manifest is
	// gone, which the spoke removes first and only after acting on the
	// note. The hub never deletes outbox objects.
	for id := range acked {
		if !complete[id] {
			if err := p.relay.Delete(ctx, RelayAckKey(spokeID, id)); err == nil {
				res.AcksPruned++
			}
		}
	}
	for id := range refused {
		if !complete[id] {
			_ = p.relay.Delete(ctx, RelayRejectedKey(spokeID, id))
		}
	}

	for id := range complete {
		if acked[id] || refused[id] || p.notOurs[spokeID+"/"+id] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		p.processBundle(ctx, spokeID, id, res)
	}
	for k := range p.notOurs {
		if sp, id, _ := strings.Cut(k, "/"); sp == spokeID && !complete[id] {
			delete(p.notOurs, k)
		}
	}
	return nil
}

func (p *RelayPoller) processBundle(ctx context.Context, spokeID, bundleID string, res *RelayPollResult) {
	log := p.logger.With().Str("spoke_id", spokeID).Str("bundle_id", bundleID).Logger()
	secret, err := p.importOne(ctx, spokeID, bundleID, res)
	switch {
	case err == nil:
	case errors.Is(err, errBundleNotOurs):
		res.Skipped++
		log.Debug().Err(err).Msg("Relay: bundle is not for this hub; leaving it")
	case errors.Is(err, ErrBundleRefused):
		res.Refused++
		log.Error().Err(err).Msg("Relay: bundle refused; the spoke is told and stops re-sending it")
		p.refuse(ctx, spokeID, bundleID, secret, err, log)
	case errors.Is(err, ErrImportInProgress):
		// A drive import holds the importer: not a failure, the next poll.
		log.Debug().Msg("Relay: a bundle import is in progress; this bundle waits for the next poll")
	default:
		res.Failed++
		log.Warn().Err(err).Msg("Relay: bundle import failed; retrying next poll")
	}
}

func (p *RelayPoller) refuse(ctx context.Context, spokeID, bundleID, secret string, cause error, log zerolog.Logger) {
	note := RelayRefusal{
		BundleID:  bundleID,
		SpokeID:   spokeID,
		HubID:     p.hubID,
		Reason:    truncateReason(strings.ReplaceAll(cause.Error(), "\x00", "")),
		RefusedAt: time.Now().UTC().Unix(),
	}
	if err := note.sign(secret); err != nil {
		log.Warn().Err(err).Msg("Relay: could not sign the refusal note")
		return
	}
	raw, err := json.Marshal(note)
	if err != nil {
		return
	}
	if _, err := writeIfAbsent(ctx, p.relay, RelayRejectedKey(spokeID, bundleID), raw); err != nil {
		log.Warn().Err(err).Msg("Relay: could not write the refusal note; the bundle is refused again next poll")
	}
}

func truncateReason(s string) string {
	const max = 1024
	if len(s) > max {
		return s[:max]
	}
	return s
}

// writeIfAbsent writes data at key unless an object is there already, where
// the backend can compare-and-set; elsewhere it overwrites. Overwriting is
// harmless for what the relay writes this way: two hubs after a failover
// write equivalent, independently signed acks for the same bundle, and a
// refusal note says the same thing twice.
//
// A conditional write that ERRORS falls back to a plain one. S3-compatible
// stores disagree on If-None-Match: * for PUT — MinIO 2025-09 answers it on
// a missing key with 404 NoSuchKey — and the condition is an optimisation
// here, not the safety property, so a store that cannot honour it must not
// stop acks from being written.
func writeIfAbsent(ctx context.Context, b storage.Backend, key string, data []byte) (bool, error) {
	if cw, ok := b.(storage.ConditionalWriter); ok {
		created, err := cw.WriteIfAbsent(ctx, key, data)
		if err == nil {
			return created, nil
		}
	}
	if err := b.Write(ctx, key, data); err != nil {
		return false, err
	}
	return true, nil
}

// importOne takes one complete bundle through the checks, cheapest and least
// trusting first, so nothing an unauthenticated party wrote costs more than a
// small bounded fetch:
//
//  1. the spoke prefix names an enabled, registered spoke (no fetch at all);
//  2. manifest.json (64 KiB cap): addressed to this hub, signed for this key,
//     within limits;
//  3. entries.jsonl (64 MiB cap) and the MAC over both;
//  4. dedup: already imported means rebuild the ack, download nothing;
//  5. with the importer lock held, every signed data object at its declared
//     size, then Import.
//
// It returns the spoke's secret once known, for signing a refusal. Only steps
// 2 and 3 refuse (ErrBundleRefused): anything wrong after the MAC verified is
// transient, since a retry or the spoke re-sending can cure it.
func (p *RelayPoller) importOne(ctx context.Context, spokeID, bundleID string, res *RelayPollResult) (string, error) {
	secret, err := p.importer.relaySpokeSecret(ctx, spokeID)
	if err != nil {
		return "", err
	}

	stage, err := os.MkdirTemp(p.staging, "relay-")
	if err != nil {
		return secret, fmt.Errorf("edgesync: create relay staging: %w", err)
	}
	defer os.RemoveAll(stage)

	prefix := RelayOutboxPrefix(spokeID, bundleID)
	if err := p.fetch(ctx, prefix+manifestName, filepath.Join(stage, manifestName), maxManifestBytes, false); err != nil {
		return secret, err
	}
	if _, err := p.importer.checkRelayManifest(stage, spokeID, bundleID); err != nil {
		if errors.Is(err, errBundleNotOurs) {
			p.notOurs[spokeID+"/"+bundleID] = true
		}
		return secret, err
	}
	if err := p.fetch(ctx, prefix+entriesName, filepath.Join(stage, entriesName), maxRelayEntriesBytes, false); err != nil {
		return secret, err
	}
	pre, err := p.importer.PrecheckRelay(ctx, stage, secret)
	if err != nil {
		return secret, err
	}

	if pre.Prior != nil {
		// Imported before, ack never landed (B1): rebuild it, download nothing.
		raw, err := p.importer.RebuildAck(ctx, pre)
		if err != nil {
			return secret, err
		}
		if _, err := writeIfAbsent(ctx, p.relay, RelayAckKey(spokeID, bundleID), raw); err != nil {
			return secret, fmt.Errorf("edgesync: write rebuilt ack: %w", err)
		}
		res.Reacked++
		p.logger.Info().Str("spoke_id", spokeID).Str("bundle_id", bundleID).
			Msg("Relay: bundle was already imported; its acknowledgment was rebuilt and written")
		return secret, nil
	}

	// The importer lock BEFORE the download: a drive import running now is
	// found before a whole bundle is fetched for nothing.
	unlock, ok := p.importer.tryLock()
	if !ok {
		return secret, ErrImportInProgress
	}
	defer unlock()

	// Download what the spoke SIGNED, not what a listing shows: an extra
	// object in the prefix is never fetched, and a declared one that is
	// missing or the wrong size is caught here, before Import re-verifies.
	if err := os.MkdirAll(filepath.Join(stage, dataDir), 0o700); err != nil {
		return secret, fmt.Errorf("edgesync: create relay staging: %w", err)
	}
	for _, e := range pre.Entries {
		if err := ctx.Err(); err != nil {
			return secret, err
		}
		dest := filepath.Join(stage, dataDir, filepath.FromSlash(e.Path))
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return secret, fmt.Errorf("edgesync: create relay staging: %w", err)
		}
		if err := p.fetch(ctx, prefix+dataDir+"/"+e.Path, dest, e.SizeBytes, true); err != nil {
			return secret, err
		}
	}

	out, err := p.importer.importLocked(ctx, stage)
	if err != nil {
		// After the MAC verified, never a refusal (see ErrBundleRefused):
		// strip the sentinel so a content or staging error stays transient.
		if errors.Is(err, ErrBundleInvalid) {
			return secret, fmt.Errorf("edgesync: import of a verified relay bundle failed (retried; the spoke re-sends after its ack_timeout): %s", err.Error())
		}
		return secret, err
	}
	res.Imported++
	if !out.AckWritten {
		return secret, errors.New("edgesync: bundle imported but its acknowledgment could not be written; rebuilt next poll")
	}
	raw, err := os.ReadFile(filepath.Join(stage, ackName))
	if err != nil {
		return secret, fmt.Errorf("edgesync: read staged ack (rebuilt next poll): %w", err)
	}
	if _, err := writeIfAbsent(ctx, p.relay, RelayAckKey(spokeID, bundleID), raw); err != nil {
		return secret, fmt.Errorf("edgesync: upload ack (rebuilt next poll): %w", err)
	}
	return secret, nil
}

// fetch downloads key to dest (0600), refusing more than max bytes. With
// exact, the object must be exactly max bytes: a data object's declared size.
//
// The size is checked before reading (StatFile) and enforced during it, since
// the object can be replaced between the two. A missing object is transient:
// the spoke may be removing the bundle (an ack timeout) as this poll runs.
func (p *RelayPoller) fetch(ctx context.Context, key, dest string, max int64, exact bool) error {
	size, err := p.relay.StatFile(ctx, key)
	if err != nil {
		return fmt.Errorf("edgesync: stat %s: %w", key, err)
	}
	if size < 0 {
		return fmt.Errorf("edgesync: %s vanished during the poll", key)
	}
	if size > max || (exact && size != max) {
		// Metadata over its bound is the spoke's signed shape: refused. A data
		// object of the wrong size is not what the spoke signed and may be
		// re-sent correctly: transient.
		return fmt.Errorf("%w: %s is %d bytes, expected %s %d", oversize(exact), key, size,
			map[bool]string{true: "exactly", false: "at most"}[exact], max)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("edgesync: create %s: %w", dest, err)
	}
	defer f.Close()
	lw := &limitWriter{w: f, left: max}
	if err := p.relay.ReadTo(ctx, key, lw); err != nil {
		if lw.over {
			return fmt.Errorf("%w: %s is larger than %d bytes", oversize(exact), key, max)
		}
		if storage.IsNotFound(err) {
			return fmt.Errorf("edgesync: %s vanished during the poll", key)
		}
		return fmt.Errorf("edgesync: download %s: %w", key, err)
	}
	if exact && lw.n != max {
		return fmt.Errorf("%w: %s is %d bytes, declared %d", oversize(exact), key, lw.n, max)
	}
	return nil
}

var errRelayObjectSize = errors.New("edgesync: relay data object is not the size the spoke signed")

func oversize(data bool) error {
	if data {
		return errRelayObjectSize
	}
	return ErrBundleRefused
}

// limitWriter fails a write that would take the total past left.
type limitWriter struct {
	w    io.Writer
	left int64
	n    int64
	over bool
}

func (l *limitWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > l.left {
		l.over = true
		return 0, errors.New("edgesync: object larger than its bound")
	}
	n, err := l.w.Write(b)
	l.left -= int64(n)
	l.n += int64(n)
	return n, err
}
