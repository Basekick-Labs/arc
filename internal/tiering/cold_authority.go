package tiering

// Whether this node's cold tier rows can stand in for a listing of the cold
// store (#1550).
//
// When the cold tier cannot be listed, the query path may leave out the cold
// partitions of a read whose range has no cold rows, so a hot-only query
// keeps working through a cold-tier outage. That is only safe while the rows
// are a complete record of what the cold store holds. The rows are trusted
// only when all of these hold:
//
//   - The node is not part of a cluster deployment (cluster.enabled or
//     cluster.shared_storage_mode, whether or not a coordinator is running),
//     and has no cluster manifest. A clustered node learns of other nodes'
//     cold files through replication events and the cold sync, and several of
//     those record nothing while the cold store is unreachable (#1552).
//   - A cold-tier metadata sync that recorded a row for every listed cold
//     object has finished against THIS database: the tier_state flag
//     (metadata.go). A fresh, lost or moved database has no flag, so its rows
//     are not trusted before a sync has filled them.
//   - No marker file says rows may be missing since then. The marker, beside
//     the SQLite database, is written:
//   - at boot, before a staged metadata restore replaces the database
//     (main.go): the restored rows predate every file migrated since the
//     backup, and the restored database carries the backup's flag;
//   - when a restore that may write cold objects starts (BeginColdRestore),
//     before its first cold write, so a crash mid-restore leaves it set;
//   - when a cold sync lists objects it cannot record.
//     It is a file, not a row, because a metadata restore replaces the database
//     and with it any row that could say so.
//
// Only a complete cold sync that started after the last marking, with no
// restore in progress, sets the flag and removes the marker.
//
// A cold sync that fails, or stops early, does not mark anything on a
// standalone node: nothing else writes cold objects there without a row. The
// migrator records a file's cold row before it releases the hot copy
// (migrator.go), and a restore is covered by BeginColdRestore.
//
// Readers (every tiered read asks) only load atomics. writeMu serializes the
// writers — mark, clear, restore begin/end — across their file and SQLite
// I/O, so a marking can never land between a clear's generation check and
// its state change; no reader ever takes it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ColdMetadataUnverifiedSuffix names the marker file, next to the SQLite
// database that holds tier_files, recording that this node's cold tier rows
// may be missing files the cold store holds.
const ColdMetadataUnverifiedSuffix = ".cold-metadata-unverified"

// ColdMetadataMarkerPath is the marker path for the SQLite database at
// dbPath, or "" without one.
func ColdMetadataMarkerPath(dbPath string) string {
	if dbPath == "" {
		return ""
	}
	return dbPath + ColdMetadataUnverifiedSuffix
}

// MarkColdMetadataUnverified writes the marker for the database at dbPath.
// main.go calls it at boot, before a staged metadata restore replaces that
// database, so no crash between the swap and the marking can lose it.
func MarkColdMetadataUnverified(dbPath, reason string) error {
	path := ColdMetadataMarkerPath(dbPath)
	if path == "" {
		return nil
	}
	return writeColdMarker(path, reason)
}

func writeColdMarker(path, reason string) error {
	if err := os.WriteFile(path, []byte(reason+"\n"), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// Reasons reported by ColdMetadataAuthority.
const (
	// ColdMetadataReasonRestored is the boot marker's reason.
	ColdMetadataReasonRestored = "tier metadata was restored from a backup"

	coldAuthorityReasonCluster    = "cluster deployment: cold rows arrive from other nodes and can lag the cold store"
	coldAuthorityReasonNoSync     = "no cold-tier metadata sync has recorded every cold object against this database yet"
	coldAuthorityReasonRestore    = "a restore wrote to the cold tier"
	coldAuthorityReasonUnrecorded = "a cold-tier metadata sync could not record every cold object"
	coldAuthorityReasonUnreadable = "the incomplete-cold-rows marker could not be read"
)

// coldAuthority is one manager's trust state.
type coldAuthority struct {
	writeMu    sync.Mutex
	markerPath string
	clustered  bool

	complete atomic.Bool // tier_state flag, mirrored
	marked   atomic.Bool // marker file present, mirrored
	reason   atomic.Value
	// gen counts markings and restore starts. A sync clears only when it is
	// unchanged since the sync began: what changed may not be in its listing.
	gen atomic.Uint64
	// restoring counts restores in progress; no sync clears while one runs.
	restoring atomic.Int32
}

func (a *coldAuthority) setReason(r string) { a.reason.Store(r) }

func (a *coldAuthority) reasonText() string {
	r, _ := a.reason.Load().(string)
	return r
}

// load reads the marker file and the tier_state flag at startup.
func (a *coldAuthority) load(ctx context.Context, store *MetadataStore, markerPath string, clustered bool) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	a.markerPath = markerPath
	a.clustered = clustered

	var errs []error
	if store != nil {
		complete, err := store.coldRowsComplete(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		a.complete.Store(complete)
	}
	if markerPath != "" {
		b, err := os.ReadFile(markerPath)
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			// Unreadable is not absent.
			a.marked.Store(true)
			a.setReason(coldAuthorityReasonUnreadable)
			errs = append(errs, fmt.Errorf("read %s: %w", markerPath, err))
		default:
			a.marked.Store(true)
			reason := strings.TrimSpace(string(b))
			if reason == "" {
				reason = ColdMetadataReasonRestored
			}
			a.setReason(reason)
		}
	}
	return errors.Join(errs...)
}

// mark records that rows may be missing: the marker file, and the tier_state
// flag forgotten, so a marker that could not be written still leaves the rows
// untrusted after a restart. Failures are returned; the in-memory state is
// set first regardless.
func (a *coldAuthority) mark(ctx context.Context, store *MetadataStore, reason string) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	a.gen.Add(1)
	a.marked.Store(true)
	a.setReason(reason)
	var errs []error
	if a.markerPath != "" {
		if err := writeColdMarker(a.markerPath, reason); err != nil {
			errs = append(errs, err)
		}
	}
	if store != nil {
		if err := store.setColdRowsComplete(ctx, false); err != nil {
			errs = append(errs, err)
		}
	}
	a.complete.Store(false)
	return errors.Join(errs...)
}

// clear records a complete cold sync that began at generation startGen. It
// reports whether the rows became trusted; a marking or a restore since the
// sync began, or a write that fails, leaves them as they were.
func (a *coldAuthority) clear(ctx context.Context, store *MetadataStore, startGen uint64) (bool, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if a.gen.Load() != startGen || a.restoring.Load() > 0 {
		return false, nil
	}
	if store != nil {
		if err := store.setColdRowsComplete(ctx, true); err != nil {
			return false, err
		}
	}
	a.complete.Store(true)
	if a.markerPath != "" {
		if err := os.Remove(a.markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("remove %s: %w", a.markerPath, err)
		}
	}
	a.marked.Store(false)
	a.setReason("")
	return true, nil
}

// initColdAuthority loads the trust state from cfg at construction.
func (m *Manager) initColdAuthority(cfg *ManagerConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), coldAuthTimeout)
	defer cancel()
	if err := m.coldAuth.load(ctx, m.metadata, cfg.ColdMetadataMarkerPath, cfg.ClusteredDeployment); err != nil {
		m.logger.Error().Err(err).Msg("Could not read whether cold tier rows are complete; treating them as incomplete")
	}
	if m.coldAuth.marked.Load() {
		m.logger.Warn().Str("reason", m.coldAuth.reasonText()).
			Msg("Cold tier rows may be missing cold files until a cold-tier metadata sync records every cold object; until then a read the cold tier cannot list fails rather than answer from these rows")
	}
}

// ColdMetadataAuthority reports whether this node's cold tier rows are a
// complete record of the cold store, so a read whose cold listing failed may
// trust them to say a range holds no cold data, and if not, why (#1550).
// Nil-receiver safe.
func (m *Manager) ColdMetadataAuthority() (authoritative bool, reason string) {
	if m == nil {
		return false, "tiering is not running"
	}
	if m.coldAuth.clustered || m.manifest != nil {
		return false, coldAuthorityReasonCluster
	}
	if m.coldAuth.marked.Load() {
		return false, m.coldAuth.reasonText() + "; cleared by the next cold-tier metadata sync that records every cold object"
	}
	if !m.coldAuth.complete.Load() {
		return false, coldAuthorityReasonNoSync
	}
	return true, ""
}

// ColdMetadataAuthoritative is ColdMetadataAuthority without the reason.
func (m *Manager) ColdMetadataAuthoritative() bool {
	ok, _ := m.ColdMetadataAuthority()
	return ok
}

// coldAuthTimeout bounds the SQLite write a marking or a clear makes.
const coldAuthTimeout = 10 * time.Second

// markColdMetadataUnverified records that cold rows may be missing.
func (m *Manager) markColdMetadataUnverified(reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), coldAuthTimeout)
	defer cancel()
	if err := m.coldAuth.mark(ctx, m.metadata, reason); err != nil {
		m.logger.Error().Err(err).Str("reason", reason).
			Msg("Could not fully persist that cold tier rows may be incomplete; this process treats them as incomplete regardless")
	}
}

// BeginColdRestore is called by a restore before it may write to the cold
// tier. It marks the rows incomplete, persistently, and keeps any cold sync
// from clearing that until EndColdRestore: objects the restore writes after a
// sync has listed would otherwise lose their only record if the restore then
// died before recording their rows. Nil-receiver safe.
func (m *Manager) BeginColdRestore() {
	if m == nil {
		return
	}
	m.coldAuth.restoring.Add(1)
	m.markColdMetadataUnverified(coldAuthorityReasonRestore)
}

// EndColdRestore ends a BeginColdRestore. The next complete cold sync then
// records the restored objects and clears the marking.
func (m *Manager) EndColdRestore() {
	if m == nil {
		return
	}
	m.coldAuth.restoring.Add(-1)
}

// coldSyncCompleted is called by a cold-tier metadata sync that listed the
// whole cold store and returned no error. notRecorded counts the objects it
// listed but could not record, sample names a few of them. A failure that
// repeats every cycle keeps the rows untrusted until it is fixed; the sample
// is how an operator finds what is pinning them.
func (m *Manager) coldSyncCompleted(startGen uint64, notRecorded int, sample []string) {
	if notRecorded > 0 {
		m.logger.Warn().Int("objects_not_recorded", notRecorded).Strs("sample", sample).
			Msg("Cold tier metadata sync could not record every cold object; cold tier rows are marked incomplete until a sync records them all, so a read the cold tier cannot list fails rather than answer from them")
		m.markColdMetadataUnverified(coldAuthorityReasonUnrecorded)
		return
	}
	wasTrusted := m.ColdMetadataAuthoritative()
	ctx, cancel := context.WithTimeout(context.Background(), coldAuthTimeout)
	defer cancel()
	cleared, err := m.coldAuth.clear(ctx, m.metadata, startGen)
	if err != nil {
		m.logger.Error().Err(err).Msg("Could not record a complete cold sync; cold tier rows stay marked incomplete")
		return
	}
	if cleared && !wasTrusted && m.ColdMetadataAuthoritative() {
		m.logger.Info().Msg("Cold tier metadata sync recorded every cold object; cold tier rows are a complete record again")
	}
}
