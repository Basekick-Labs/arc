package tiering

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"
)

// Migrator handles file migration between tiers
type Migrator struct {
	manager       *Manager
	maxConcurrent int
	batchSize     int
	logger        zerolog.Logger
}

// MigratorConfig holds configuration for creating a migrator
type MigratorConfig struct {
	Manager       *Manager
	MaxConcurrent int
	BatchSize     int
	Logger        zerolog.Logger
}

func isDailyCompactedPath(path string) bool {
	return strings.HasSuffix(storage.StripRewriteSuffix(path), "_daily.parquet")
}

// ErrCandidateQuarantined is returned by MigrateFile when the candidate's
// storage key turned out to be permanently unusable and the file index row
// was marked so it is never selected again (#758). It wraps the backend's
// ErrInvalidPath, so errors.Is works for either. Callers count it as a failed
// migration for the cycle that discovered it, but it is not a retryable one.
var ErrCandidateQuarantined = errors.New("tiering: candidate quarantined, its storage key is permanently unusable")

// quarantineReasonInvalidPath is what the file index row records. It is a
// fixed string rather than the wrapped error so the column stays greppable
// and does not carry the per-backend spelling of the same condition.
const quarantineReasonInvalidPath = "storage key is permanently unusable by every storage backend (storage.ErrInvalidPath)"

// NewMigrator creates a new migrator
func NewMigrator(cfg *MigratorConfig) *Migrator {
	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	return &Migrator{
		manager:       cfg.Manager,
		maxConcurrent: maxConcurrent,
		batchSize:     batchSize,
		logger:        cfg.Logger.With().Str("component", "tiering-migrator").Logger(),
	}
}

// MigrateTier migrates eligible files from one tier to another
// Returns the number of files migrated and the number of errors
func (m *Migrator) MigrateTier(ctx context.Context, fromTier, toTier Tier) (int, int) {
	m.logger.Info().
		Str("from_tier", string(fromTier)).
		Str("to_tier", string(toTier)).
		Msg("Starting tier migration")

	// Find candidates for migration
	candidates, err := m.FindCandidates(ctx, fromTier, toTier)
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to find migration candidates")
		return 0, 1
	}

	if len(candidates) == 0 {
		m.logger.Info().Msg("No candidates for migration")
		return 0, 0
	}

	m.logger.Info().Int("candidates", len(candidates)).Msg("Found migration candidates")

	// Process in batches
	migrated := 0
	failed := 0

	for i := 0; i < len(candidates); i += m.batchSize {
		end := i + m.batchSize
		if end > len(candidates) {
			end = len(candidates)
		}

		batch := candidates[i:end]
		batchMigrated, batchFailed, releaseErr := m.migrateBatch(ctx, batch)
		migrated += batchMigrated
		failed += batchFailed
		if releaseErr != nil {
			// A manifest failure is not transient (Cluster Operations
			// Checklist item 3): no further batch is copied this cycle.
			break
		}
	}

	return migrated, failed
}

// FindCandidates finds files eligible for migration from one tier to another
func (m *Migrator) FindCandidates(ctx context.Context, fromTier, toTier Tier) ([]MigrationCandidate, error) {
	// Only support Hot -> Cold migration in 2-tier system
	if fromTier != TierHot || toTier != TierCold {
		return nil, fmt.Errorf("unsupported migration: %s -> %s (only hot -> cold supported)", fromTier, toTier)
	}

	// Hot -> Cold: files older than hot_max_age_days
	maxAge := time.Duration(m.manager.config.DefaultHotMaxAgeDays) * 24 * time.Hour

	// Get files older than max age in the source tier
	files, err := m.manager.metadata.GetFilesOlderThan(ctx, fromTier, maxAge)
	if err != nil {
		return nil, fmt.Errorf("failed to get old files: %w", err)
	}

	// A file a DELETE replaced but has not removed yet (its rewrite is
	// published, the journal or the manifest says so) holds the rows the
	// DELETE took out. Copied to cold, it would outlive its hot copy as a
	// cold object, which the cold sync records, and those rows would be read
	// again for good. Without a journal listing it cannot be told apart from
	// a live file, so the cycle migrates nothing rather than guess.
	retired, err := storage.ReadRetired(ctx, m.manager.hotBackend)
	if err != nil {
		return nil, fmt.Errorf("failed to read the DELETE journal: %w", err)
	}

	// Filter by per-database policies
	var candidates []MigrationCandidate
	now := time.Now().UTC()

	for _, file := range files {
		// Check if database is excluded from tiering
		if m.manager.IsHotOnly(ctx, file.Database) {
			continue
		}

		// Get effective policy for this database
		policy := m.manager.GetEffectivePolicy(ctx, file.Database)

		// Calculate age threshold based on policy
		ageThreshold := time.Duration(policy.HotMaxAgeDays) * 24 * time.Hour

		// Check if file is old enough based on its policy
		fileAge := now.Sub(file.PartitionTime)
		if fileAge < ageThreshold {
			continue
		}

		// Only migrate daily-compacted files to cold tier. Immutable DELETE
		// rewrites retain the logical suffix after normalization.
		if !isDailyCompactedPath(file.Path) {
			continue
		}

		// Spoke-namespace files register for visibility but do NOT migrate
		// yet: legacy spoke-side compacted files sync once and carry
		// sync_received receipts, and deleting their hot copy would make
		// confirmPresent forget the receipt — the spoke then re-offers the
		// file and the hub re-accepts a duplicate next to the cold copy (the
		// #611 hazard class, #687). The path shape decides, via the same
		// parser that registers files: content-based heuristics (numeric
		// spoke measurement names) and metadata-based reconstruction
		// (synthetic or legacy PartitionTime values) both misclassify.
		// An unparseable path is skipped conservatively.
		info, err := m.manager.parseFilePath(file.Path)
		if err != nil {
			m.manager.logger.Debug().Str("path", file.Path).
				Msg("Skipping unrecognized file path for cold migration")
			continue
		}
		if info.SpokeNamespaced && !m.manager.hasHotFileRemovalHook() {
			// Without receipt marking wired, deleting a spoke file's hot copy
			// would make confirmPresent forget its sync receipt and re-accept
			// a duplicate upload (#687).
			m.manager.logger.Debug().Str("path", file.Path).
				Msg("Skipping spoke-namespace file for cold migration (receipt marking not wired, #687)")
			continue
		}

		if m.manager.heldByCompaction(file.Path) {
			continue
		}
		if m.supersededByDelete(file.Path, retired[file.Path]) {
			m.logger.Debug().Str("path", file.Path).
				Msg("Skipping a file a DELETE replaced for cold migration")
			continue
		}

		candidates = append(candidates, MigrationCandidate{
			Path:          file.Path,
			Database:      file.Database,
			Measurement:   file.Measurement,
			PartitionTime: file.PartitionTime,
			SizeBytes:     file.SizeBytes,
			CurrentTier:   fromTier,
			TargetTier:    toTier,
			Age:           fileAge,
		})
	}

	return candidates, nil
}

// dropTakenOver removes from files those a compaction committed over since
// their copy (#1207 d2a2b): their rows are in the compaction's output, so the
// cold copy goes and the hot copy is the finisher's to delete.
func (m *Migrator) dropTakenOver(ctx context.Context, files []MigrationCandidate) []MigrationCandidate {
	kept := files[:0:0]
	for _, c := range files {
		if !m.manager.heldByCompaction(c.Path) {
			kept = append(kept, c)
			continue
		}
		if dst := m.manager.GetBackendForTier(c.TargetTier); dst != nil {
			if err := dst.Delete(ctx, c.Path); err != nil && !storage.IsNotFound(err) {
				m.logger.Warn().Err(err).Str("path", c.Path).Msg("Could not remove the cold copy of a file a compaction took over")
			}
		}
		if err := m.manager.metadata.UpdateTier(ctx, c.Path, c.CurrentTier); err != nil {
			m.logger.Warn().Err(err).Str("path", c.Path).Msg("Could not record a file a compaction took over as hot again")
		}
		m.logger.Info().Str("path", c.Path).Msg("A compaction took over a file during its migration; its cold copy was removed")
	}
	return kept
}

// errHeldByCompaction: a compaction took the file over during its migration.
var errHeldByCompaction = errors.New("a compaction took the file over during its migration")

// dropSupersededCopy deletes the cold copy of a file whose rows a DELETE took
// out. If it stays, the next cold sync records it as a cold file and the
// deleted rows are read again; nothing else removes it.
func (m *Migrator) dropSupersededCopy(ctx context.Context, cold storage.Backend, path, why string) {
	if err := cold.Delete(ctx, path); err != nil && !storage.IsNotFound(err) {
		m.logger.Error().Err(err).Str("path", path).Str("why", why).
			Msg("Could not remove the cold copy of a file whose rows a DELETE removed; the next cold sync will record it and those rows will be read again until it is deleted by hand")
		return
	}
	m.logger.Warn().Str("path", path).Str("why", why).Msg("Removed the cold copy of a file whose rows a DELETE removed")
}

// errNotPrimary: this node lost the writer role during the copy.
var errNotPrimary = errors.New("this node is no longer the primary writer; the flip is left to the new one")

// errReplacedByDelete: a DELETE replaced the file during its migration.
var errReplacedByDelete = errors.New("a DELETE replaced the file during its migration")

// errTierChanged: the file's tier row stopped saying hot during the copy.
var errTierChanged = errors.New("the file's tier row changed during its migration")

// RewriteSiblingChecker is a ManifestCoordinator that can tell a registered
// member of a file's rewrite family (#1340). Without it a cluster's DELETE
// leftovers are known to the migrator only through the journal.
type RewriteSiblingChecker interface {
	HasRewriteSibling(path string) bool
}

// supersededByDelete reports a file a DELETE replaced and has not removed
// yet, on positive evidence only: the DELETE journal names it, or, in a
// cluster, the manifest no longer lists it while it lists another member of
// its rewrite family.
func (m *Migrator) supersededByDelete(path string, journaled bool) bool {
	if journaled {
		return true
	}
	mc := m.manager.manifest
	if mc == nil {
		return false
	}
	sc, ok := mc.(RewriteSiblingChecker)
	if !ok {
		return false
	}
	if _, registered := mc.ManifestEntry(path); registered {
		return false
	}
	return sc.HasRewriteSibling(path)
}

// supersededNow is supersededByDelete with the journal record for path read
// now rather than from the cycle's listing.
func (m *Migrator) supersededNow(ctx context.Context, path string) (bool, error) {
	key := storage.RetiredKey(path)
	data, err := m.manager.hotBackend.Read(ctx, key)
	journaled := false
	switch {
	case err == nil:
		_, journaled = storage.ParseRetired(key, data)
	case storage.IsNotFound(err):
	default:
		return false, err
	}
	return m.supersededByDelete(path, journaled), nil
}

// MigrateBatch migrates a batch of files concurrently
func (m *Migrator) MigrateBatch(ctx context.Context, candidates []MigrationCandidate) (int, int) {
	migrated, failed, _ := m.migrateBatch(ctx, candidates)
	return migrated, failed
}

// migrateBatch is MigrateBatch, also returning the release's manifest
// failure, which stops the cycle.
func (m *Migrator) migrateBatch(ctx context.Context, candidates []MigrationCandidate) (int, int, error) {
	if len(candidates) == 0 {
		return 0, 0, nil
	}
	// Mark sync receipts for the WHOLE batch before any file work (#687):
	// one chunked UPDATE on the shared sync DB instead of per-file calls
	// from concurrent goroutines. Ordering is load-bearing — marking must
	// precede each file's tier flip and delete, and pre-marking the batch
	// satisfies that for every member; a marked receipt whose file does not
	// migrate (later copy failure) is documented harmless. A mark failure
	// aborts the batch so no file's hot copy can be removed unmarked.
	paths := make([]string, len(candidates))
	for i, c := range candidates {
		paths[i] = c.Path
	}
	if err := m.manager.notifyHotFilesRemoved(paths); err != nil {
		m.logger.Warn().Err(err).Int("candidates", len(candidates)).
			Msg("Could not mark sync receipts; aborting migration batch")
		return 0, len(candidates), nil
	}

	if len(candidates) == 0 {
		return 0, 0, nil
	}

	// Each file is claimed before its copy so a DELETE cannot rewrite it
	// meanwhile (ClaimForDelete), and kept claimed until the batch ends: a
	// file whose hot copy outlives the batch (a manifest failure) still has
	// a cold copy, which ClaimForDelete refuses on its own.
	var claimed []string
	defer func() {
		for _, p := range claimed {
			m.manager.claims.releaseMove(p)
		}
	}()

	// Copy and flip concurrently, and release the hot copies as files flip,
	// a chunk at a time, rather than after the whole batch: between a flip
	// and its release both copies are read. A manifest failure, or this
	// node losing the writer role, stops new copies; the ones in flight
	// still finish and flip.
	sem := semaphore.NewWeighted(int64(m.maxConcurrent))
	var wg sync.WaitGroup
	var mu sync.Mutex
	migrated, failed := 0, 0
	var stop atomic.Bool
	flippedCh := make(chan MigrationCandidate, len(candidates))
	releaseErr := make(chan error, 1)
	go func() {
		releaseErr <- m.streamRelease(ctx, flippedCh, &stop)
	}()

	for _, candidate := range candidates {
		if err := sem.Acquire(ctx, 1); err != nil {
			m.logger.Error().Err(err).Msg("Failed to acquire semaphore")
			break
		}
		// Asked once a slot is free: the wait for it can outlast a manifest
		// failure or the writer role.
		if stop.Load() {
			sem.Release(1)
			break
		}
		if m.manager.roleGated() {
			sem.Release(1)
			m.logger.Warn().Msg("This node is no longer the primary writer; no further files are copied this cycle")
			break
		}
		if !m.manager.claims.claimMove(candidate.Path) {
			// A DELETE is rewriting it; the next cycle sees the result.
			sem.Release(1)
			continue
		}
		claimed = append(claimed, candidate.Path)

		wg.Add(1)
		go func(c MigrationCandidate) {
			defer wg.Done()
			defer sem.Release(1)

			if err := m.copyAndFlip(ctx, c); err != nil {
				// A quarantined candidate already logged its own, definitive
				// line at Error; repeating it here would read as a second
				// failure of the same file. It still counts as failed for this
				// cycle: nothing was migrated, and the cycle summary should
				// say so the one time it happens.
				// Not-primary, replaced and changed-row outcomes logged their
				// own Warn; they are not failures of the copy.
				if errors.Is(err, errNotPrimary) || errors.Is(err, errReplacedByDelete) || errors.Is(err, errTierChanged) {
					mu.Lock()
					failed++
					mu.Unlock()
					return
				}
				if !errors.Is(err, ErrCandidateQuarantined) {
					m.logger.Error().
						Err(err).
						Str("path", c.Path).
						Str("from", string(c.CurrentTier)).
						Str("to", string(c.TargetTier)).
						Msg("Failed to migrate file")
				}

				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			mu.Lock()
			migrated++
			mu.Unlock()
			flippedCh <- c
		}(candidate)
	}

	wg.Wait()
	close(flippedCh)
	// A manifest failure counts once for the cycle; the files are migrated
	// regardless and reconciliation finishes the hot cleanup.
	err := <-releaseErr
	if err != nil {
		failed++
	}
	return migrated, failed, err
}

// releaseWindow bounds how long a flipped file waits for its chunk to start
// releasing. The double-read window of a file is this plus its chunk's
// release, which the manager paces at one manifest proposal per second
// (manifestChunkPause): when copies outrun the releases, files queue for a
// second per 200.
const releaseWindow = time.Second

// onReleaseStopped is a test seam: called once a manifest failure has
// stopped new copies.
var onReleaseStopped = func() {}

// streamRelease releases hot copies as files arrive on flipped, in chunks of
// manifestChunk or every releaseWindow, whichever comes first. A manifest
// failure sets stop, so no further copies start, and keeps every later
// hot copy for reconciliation.
func (m *Migrator) streamRelease(ctx context.Context, flipped <-chan MigrationCandidate, stop *atomic.Bool) error {
	var pending []MigrationCandidate
	var failure error
	kept := 0
	ticker := time.NewTicker(releaseWindow)
	defer ticker.Stop()
	flush := func() {
		if len(pending) == 0 {
			return
		}
		chunk := pending
		pending = nil
		if failure != nil {
			// Kept hot for reconciliation, but a file a compaction took over
			// since its flip still loses its cold copy, as releaseHotCopies
			// would have done: the compaction's output holds its rows.
			kept += len(m.dropTakenOver(ctx, chunk))
			return
		}
		if err := m.releaseHotCopies(ctx, chunk, manifestReasonMigrated); err != nil {
			failure = err
			stop.Store(true)
			onReleaseStopped()
		}
	}
	for {
		select {
		case c, ok := <-flipped:
			if !ok {
				flush()
				if kept > 0 {
					m.logger.Warn().Err(failure).Int("kept_hot_copies", kept).
						Msg("Files that flipped after the manifest failure keep their hot copies for reconciliation")
				}
				return failure
			}
			pending = append(pending, c)
			if len(pending) >= manifestChunk {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// MigrateFile migrates a single file from one tier to another: receipts,
// copy, flip, then release the hot copy through the manifest — MigrateBatch
// for one file.
func (m *Migrator) MigrateFile(ctx context.Context, candidate MigrationCandidate) error {
	if err := m.manager.notifyHotFilesRemoved([]string{candidate.Path}); err != nil {
		return fmt.Errorf("mark sync receipts: %w", err)
	}
	if !m.manager.claims.claimMove(candidate.Path) {
		return fmt.Errorf("%s is being rewritten by a DELETE; it migrates on a later cycle", candidate.Path)
	}
	defer m.manager.claims.releaseMove(candidate.Path)
	if err := m.copyAndFlip(ctx, candidate); err != nil {
		return err
	}
	return m.releaseHotCopies(ctx, []MigrationCandidate{candidate}, manifestReasonMigrated)
}

// releaseHotCopies removes migrated files' hot copies in chunks: each chunk
// leaves the cluster manifest first (one proposal, paced by the manager so
// every node's unlink queue drains between proposals), then its hot copies
// are deleted. On a local-storage cluster the manifest delete is what
// unlinks the replicas on every other node; on a shared bucket it is
// bookkeeping and the delete here removes the object. A manifest failure
// returns with the remaining hot copies in place — their rows
// already say cold, and reconciliation finishes them, manifest-first,
// within its window.
func (m *Migrator) releaseHotCopies(ctx context.Context, files []MigrationCandidate, reason string) error {
	files = m.dropTakenOver(ctx, files)
	for i := 0; i < len(files); i += manifestChunk {
		chunk := files[i:min(i+manifestChunk, len(files))]
		paths := make([]string, len(chunk))
		for j, c := range chunk {
			paths[j] = c.Path
		}
		// Before the hot copies go, a cold listing cached before the copy
		// (no file that day) must not answer for them: that read would find
		// neither copy. After, a cached hot glob naming them must not either.
		m.manager.notifyReadCachesStale()
		// The adapter logged the cause at Error; this is the consequence.
		if err := m.manager.deleteFromManifest(ctx, paths, reason); err != nil {
			m.logger.Warn().Err(err).Int("kept_hot_copies", len(files)-i).
				Msg("Manifest update failed; keeping the remaining hot copies for reconciliation")
			return err
		}
		for _, c := range chunk {
			// Always the HOT backend in the only production caller
			// (MigrateTier(ctx, TierHot, TierCold)), but GetBackendForTier
			// can answer nil for cold since #1143, so a cold-to-hot path
			// added later finds a guard rather than a panic.
			//
			// Nothing re-runs this delete, and the comment must not pretend
			// otherwise (Cluster Operations Checklist item 5): the manifest
			// entry is already gone for this chunk, and the orphan sweep only
			// ever removes HOT copies, so a leftover source copy in cold is
			// an orphan in storage with no manifest entry. The opt-in
			// reconciliation sweep would delete it after its grace window;
			// nothing else will.
			src := m.manager.GetBackendForTier(c.CurrentTier)
			if src == nil {
				m.logger.Error().Str("path", c.Path).Str("tier", string(c.CurrentTier)).
					Msg("No usable backend for the migration source tier, so the copy in it cannot be removed; its manifest entry is already gone and nothing re-runs this delete, leaving an orphan in that tier until an operator or the opt-in reconciliation sweep removes it")
				continue
			}
			if err := src.Delete(ctx, c.Path); err != nil {
				m.logger.Warn().Err(err).Str("path", c.Path).Msg("Failed to delete source file after migration")
				// Don't fail the migration - file is in destination, just source cleanup failed
				continue
			}
			// Clean up empty parent directories after successful delete
			m.CleanupEmptyDirectories(ctx, c.Path)
		}
		m.manager.notifyReadCachesStale()
	}
	return nil
}

// copyAndFlip streams the candidate to its target tier and flips its row.
// After this the file is migrated: the cold copy is canonical and stays even
// if a later step fails. The hot copy is released separately.
func (m *Migrator) copyAndFlip(ctx context.Context, candidate MigrationCandidate) error {
	// .UTC() so StartedAt persists in a stable timezone (matches the
	// rest of internal/tiering/metadata.go). time.Since(startTime) is
	// location-independent so the elapsed-duration math is unaffected.
	// Issue #460.
	startTime := time.Now().UTC()

	// Get source and destination backends
	srcBackend := m.manager.GetBackendForTier(candidate.CurrentTier)
	dstBackend := m.manager.GetBackendForTier(candidate.TargetTier)

	if srcBackend == nil {
		return fmt.Errorf("source backend not available for tier: %s", candidate.CurrentTier)
	}
	if dstBackend == nil {
		return fmt.Errorf("destination backend not available for tier: %s", candidate.TargetTier)
	}
	if m.manager.heldByCompaction(candidate.Path) {
		return fmt.Errorf("%s is being compacted; it migrates on a later cycle", candidate.Path)
	}

	// The row's size is what the last scan recorded; a file rewritten in
	// place since (the delete API does that) is longer or shorter, and an
	// object-store destination now enforces the declared length. Ask the
	// source, falling back to the row when it cannot say.
	size := candidate.SizeBytes
	if n, err := srcBackend.StatFile(ctx, candidate.Path); err == nil && n > 0 {
		size = n
	}

	// Record migration start
	record := &MigrationRecord{
		FilePath:  candidate.Path,
		Database:  candidate.Database,
		FromTier:  candidate.CurrentTier,
		ToTier:    candidate.TargetTier,
		SizeBytes: size,
		StartedAt: startTime,
	}
	migrationID, err := m.manager.metadata.RecordMigration(ctx, record)
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to record migration start")
	}

	// Perform the migration using streaming to avoid loading entire files into memory
	migrationErr := m.copyFileStreaming(ctx, srcBackend, dstBackend, candidate.Path, size)

	if migrationErr != nil {
		// Record failure
		if migrationID > 0 {
			m.manager.metadata.CompleteMigration(ctx, migrationID, migrationErr)
		}
		if errors.Is(migrationErr, storage.ErrInvalidPath) {
			return m.quarantineCandidate(ctx, candidate, migrationErr)
		}
		return migrationErr
	}

	// Update tier metadata. Sync receipts were already marked for the whole
	// batch by MigrateBatch before any file work began (#687) — marking must
	// precede this flip, and a marked receipt for a file that ends up not
	// migrating is documented harmless by the receive path.
	// A compaction may have committed over the file during the copy: its
	// rows are in the compaction's output now, and a cold copy would hold
	// them twice (#1207 d2a2b).
	if m.manager.heldByCompaction(candidate.Path) {
		if migrationID > 0 {
			m.manager.metadata.CompleteMigration(ctx, migrationID, errHeldByCompaction)
		}
		if err := dstBackend.Delete(ctx, candidate.Path); err != nil && !storage.IsNotFound(err) {
			m.logger.Warn().Err(err).Str("path", candidate.Path).Msg("Could not remove the cold copy of a file a compaction took over")
		}
		return fmt.Errorf("%s: %w", candidate.Path, errHeldByCompaction)
	}
	// The copy is done; the flip and its bookkeeping run even if the cycle
	// is cancelled now, or the cold copy would sit next to a hot row.
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	fail := func(cause error) error {
		if migrationID > 0 {
			m.manager.metadata.CompleteMigration(bctx, migrationID, cause)
		}
		return cause
	}
	// Demoted during the copy: the new primary's cold sync and its own
	// cycle take it from here. Flipping would race that node's DELETEs,
	// which this node's claims cannot see.
	if m.manager.roleGated() {
		return fail(fmt.Errorf("%s: %w", candidate.Path, errNotPrimary))
	}
	// A DELETE replaced the file before its claim was taken here (the
	// journal or the manifest says so): the copy holds the rows it took out.
	superseded, err := m.supersededNow(bctx, candidate.Path)
	if err != nil {
		m.logger.Warn().Err(err).Str("path", candidate.Path).Msg("Could not read the DELETE journal before the flip; the cold copy stays and the file is retried next cycle")
		return fail(fmt.Errorf("check the DELETE journal: %w", err))
	}
	if superseded {
		m.dropSupersededCopy(bctx, dstBackend, candidate.Path, "a DELETE replaced the file")
		return fail(fmt.Errorf("%s: %w", candidate.Path, errReplacedByDelete))
	}
	// Standalone, nothing but this node's DELETE (or its retention) removes
	// a hot file, so a source gone during the copy means its rows were
	// deleted: a leftover whose journal record was cleared mid-copy, or a
	// file retention removed. Clusters cannot tell that from another node's
	// move, so there only the evidence above counts.
	if m.manager.manifest == nil && m.manager.clusterGate == nil {
		size, err := m.manager.hotBackend.StatFile(bctx, candidate.Path)
		if err != nil {
			m.logger.Warn().Err(err).Str("path", candidate.Path).Msg("Could not check the hot copy before the flip; the cold copy stays and the file is retried next cycle")
			return fail(fmt.Errorf("check the hot copy: %w", err))
		}
		if size < 0 {
			m.dropSupersededCopy(bctx, dstBackend, candidate.Path, "the hot copy was removed during the copy")
			return fail(fmt.Errorf("%s: %w", candidate.Path, errReplacedByDelete))
		}
	}
	flippedNow, err := m.manager.metadata.UpdateTierIfCurrent(bctx, candidate.Path, candidate.CurrentTier, candidate.TargetTier)
	if err != nil {
		// The cold copy stays. Another node's metadata sync may already have
		// recorded it as cold, and that node's reconciliation would then
		// remove the hot copy on the strength of it — deleting the cold copy
		// here could leave no copy anywhere. Left in place it finishes the
		// move later: the next cold sync records it (RecordColdFile flips
		// this row to cold) and reconciliation then removes the hot copy, or
		// the next cycle copies over it and flips. A DELETE refuses the file
		// meanwhile (ClaimForDelete finds the cold copy).
		m.logger.Warn().Err(err).Str("path", candidate.Path).
			Msg("Failed to update tier metadata after copy; cold copy left in place for the next cycle")
		return fail(fmt.Errorf("failed to update tier metadata: %w", err))
	}
	if !flippedNow {
		// The row is no longer hot: already cold (another node's flip,
		// replicated here), quarantined, or gone. The hot copy being gone
		// too does not mean a DELETE took it — another node's migration or
		// reconciliation does that, and then this cold copy may be the only
		// one left. It stays.
		m.logger.Warn().Str("path", candidate.Path).
			Msg("Tier row changed during the copy; cold copy left in place")
		return fail(fmt.Errorf("%s: %w", candidate.Path, errTierChanged))
	}

	// Record success
	if migrationID > 0 {
		m.manager.metadata.CompleteMigration(bctx, migrationID, nil)
	}

	duration := time.Since(startTime)
	m.logger.Debug().
		Str("path", candidate.Path).
		Str("from", string(candidate.CurrentTier)).
		Str("to", string(candidate.TargetTier)).
		Int64("size_bytes", size).
		Dur("duration", duration).
		Msg("File migrated successfully")

	return nil
}

// quarantineCandidate handles a migration that failed because no backend can
// address the candidate's key (#758). The failure is permanent: the same key
// is refused by every backend on every attempt, so leaving the row in the hot
// tier would make FindCandidates re-select it next cycle, write another
// failed-migration row, and log the same error, forever.
//
// The row is marked rather than deleted or re-tiered. On local storage the
// file is a real data file inside the partition glob, so the query path still
// serves it and the index should keep saying it exists in hot. What changes is
// that the two work-set queries stop returning it. The marked row is the
// operator's record; the only remedy is renaming the object, after which the
// next scan registers the new key as a fresh row.
//
// If the mark itself cannot be persisted the original error is returned as a
// plain failure, so the candidate IS retried next cycle. That is right: the
// persistence failure is the transient one, and a retry of it is a single
// UPDATE, not a copy.
func (m *Migrator) quarantineCandidate(ctx context.Context, candidate MigrationCandidate, cause error) error {
	if err := m.manager.metadata.QuarantineFile(ctx, candidate.Path, quarantineReasonInvalidPath); err != nil {
		m.logger.Error().Err(err).
			Str("path", candidate.Path).
			AnErr("cause", cause).
			Msg("Migration failed on a permanently unusable storage key and the quarantine mark could not be persisted; the candidate will be re-selected next cycle")
		return cause
	}
	metrics.Get().IncStorageInvalidPathQuarantined()
	m.logger.Error().Err(cause).
		Str("path", candidate.Path).
		Str("from", string(candidate.CurrentTier)).
		Str("to", string(candidate.TargetTier)).
		Msg("Migration candidate has a permanently unusable storage key; quarantined so it is never selected again. Its tier is unchanged and the file is not deleted. Rename the object by hand to make it migratable")
	return fmt.Errorf("%w: %w", ErrCandidateQuarantined, cause)
}

// copyFile copies a file from source to destination backend
func (m *Migrator) copyFile(ctx context.Context, src, dst interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
}, path string, expectedSize int64) error {

	// For now, use simple read/write
	// TODO: Use streaming (ReadTo/WriteReader) for large files

	data, err := src.Read(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to read from source: %w", err)
	}

	// Verify size matches
	if int64(len(data)) != expectedSize && expectedSize > 0 {
		return fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, len(data))
	}

	if err := dst.Write(ctx, path, data); err != nil {
		return fmt.Errorf("failed to write to destination: %w", err)
	}

	return nil
}

// StreamingBackend is an interface for backends that support streaming
type StreamingBackend interface {
	ReadTo(ctx context.Context, path string, w io.Writer) error
	WriteReader(ctx context.Context, path string, r io.Reader, size int64) error
}

// copyFileStreaming copies a file using streaming for memory efficiency
// This is used for large files to avoid loading them entirely into memory
func (m *Migrator) copyFileStreaming(ctx context.Context, src, dst StreamingBackend, path string, size int64) error {
	// Create a pipe to stream data from source to destination
	pr, pw := io.Pipe()

	errCh := make(chan error, 2)

	// Read from source in a goroutine
	go func() {
		err := src.ReadTo(ctx, path, pw)
		pw.CloseWithError(err)
		errCh <- err
	}()

	// Write to destination in main goroutine
	go func() {
		err := dst.WriteReader(ctx, path, pr, size)
		pr.CloseWithError(err)
		errCh <- err
	}()

	// Wait for both operations to complete. The first error to arrive is the
	// one reported, except that a classified error wins over whatever arrived
	// first: which side fails first is a goroutine race (the pipe closes with
	// one side's error, and the other may report that or its own), the
	// caller branches on ErrInvalidPath to quarantine the candidate (#758),
	// and a length mismatch (ErrBodyLength) must be logged as such rather
	// than as the closed pipe it causes on the reading side.
	classified := func(err error) bool {
		return errors.Is(err, storage.ErrInvalidPath) || errors.Is(err, storage.ErrBodyLength)
	}
	var firstErr error
	for i := 0; i < 2; i++ {
		err := <-errCh
		if err == nil {
			continue
		}
		if firstErr == nil || (classified(err) && !classified(firstErr)) {
			firstErr = err
		}
	}

	if firstErr != nil {
		return fmt.Errorf("streaming copy failed: %w", firstErr)
	}

	return nil
}

// ReconcileOrphanedFiles finds and deletes files that exist in hot storage
// but are tracked as cold in metadata (orphaned after failed hot deletion during migration).
// Only checks files migrated within the last 48 hours to limit I/O.
func (m *Migrator) ReconcileOrphanedFiles(ctx context.Context) (orphansFound, deleted, failed int) {
	const reconcileWindow = 48 * time.Hour

	coldFiles, err := m.manager.metadata.GetRecentlyMigratedFiles(ctx, TierCold, reconcileWindow)
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to get recently migrated files for reconciliation")
		return 0, 0, 1
	}

	if len(coldFiles) == 0 {
		return 0, 0, 0
	}

	hotBackend := m.manager.GetBackendForTier(TierHot)
	if hotBackend == nil {
		m.logger.Error().Msg("Hot backend not available for orphaned file reconciliation")
		return 0, 0, 1
	}

	// Pass 1: find the orphans whose cold copy is verified. Pass 2 removes
	// them, manifest first.
	var orphans []FileMetadata
	for _, file := range coldFiles {
		select {
		case <-ctx.Done():
			return orphansFound, deleted, failed
		default:
		}

		exists, err := hotBackend.Exists(ctx, file.Path)
		if errors.Is(err, storage.ErrInvalidPath) {
			// Permanent: Exists fails identically on every cycle until the
			// 48-hour window closes, and Delete would fail the same way, so
			// there is nothing this loop can ever do for the row (#758). Mark
			// it so GetRecentlyMigratedFiles stops returning it. The cold
			// row is left as cold: this sweep only ever removes hot copies,
			// and it cannot even establish whether one exists here.
			//
			// A persistence failure is counted and retried next cycle, which
			// is a retry of one UPDATE, not a storm.
			qErr := m.manager.metadata.QuarantineFile(ctx, file.Path, quarantineReasonInvalidPath)
			if qErr != nil {
				m.logger.Error().Err(qErr).Str("path", file.Path).AnErr("cause", err).
					Msg("Cold file's storage key is permanently unusable and the quarantine mark could not be persisted; reconciliation will retry it next cycle")
				failed++
				continue
			}
			metrics.Get().IncStorageInvalidPathQuarantined()
			m.logger.Error().Err(err).Str("path", file.Path).
				Msg("Cold file's storage key is permanently unusable, so its hot copy can be neither checked nor removed; quarantined so reconciliation stops retrying it. Remove any hot copy by hand")
			failed++
			continue
		}
		if err != nil {
			m.logger.Warn().Err(err).Str("path", file.Path).Msg("Failed to check hot existence during reconciliation")
			failed++
			continue
		}

		if !exists {
			continue
		}

		// Orphan found — metadata says cold, but a hot copy is still there.
		orphansFound++

		// A cold row written by this node's own migration is proof of a cold
		// copy; a cold row written by the metadata sync is not, and since
		// #1179 that sync runs on every node with a usable cold tier, not
		// only on shared storage. The sync records rows from a cold LISTING,
		// and a listing says an object is there, not that it is the complete
		// and current copy of that path: copyAndFlip leaves the cold object
		// in place when its UpdateTier fails (see the comment there, which
		// says why removing it would be worse), so an object can sit in cold
		// under a row that still says hot until some later pass reconciles
		// them. Deleting the hot copy on the strength of a listing alone
		// would lose the file, so require the cold object first; a row whose
		// cold object is gone goes back to hot so
		// the primary migrates it again instead of leaving a cold row that
		// points at nothing.
		coldBackend := m.manager.GetBackendForTier(TierCold)
		if coldBackend == nil {
			m.logger.Error().Str("path", file.Path).
				Msg("Cold backend not available; keeping orphaned hot file since its cold copy cannot be verified")
			failed++
			continue
		}
		coldExists, err := coldBackend.Exists(ctx, file.Path)
		if err != nil {
			m.logger.Warn().Err(err).Str("path", file.Path).
				Msg("Failed to check cold existence during reconciliation; keeping orphaned hot file")
			failed++
			continue
		}
		if !coldExists {
			m.logger.Warn().Str("path", file.Path).
				Msg("Cold row has no cold object; reverting it to hot so the file is migrated again rather than deleting the only copy")
			if err := m.manager.metadata.UpdateTier(ctx, file.Path, TierHot); err != nil {
				m.logger.Error().Err(err).Str("path", file.Path).Msg("Failed to revert cold row to hot")
			}
			failed++
			continue
		}

		orphans = append(orphans, file)
	}

	if len(orphans) == 0 {
		return orphansFound, deleted, failed
	}
	paths := make([]string, len(orphans))
	for i, file := range orphans {
		paths[i] = file.Path
	}

	// Mark sync receipts for every orphan before any can disappear (#687).
	// Reconciliation is the crash-recovery path and must uphold the same
	// invariant as migration; and on a local-storage cluster the manifest
	// delete below unlinks the copies on every node, this one included, so
	// marking in front of each hot delete would come too late.
	if err := m.manager.notifyHotFilesRemoved(paths); err != nil {
		m.logger.Warn().Err(err).Int("orphans", len(orphans)).
			Msg("Could not mark sync receipts; keeping orphaned hot files for the next cycle")
		return orphansFound, deleted, failed + len(orphans)
	}

	// Pass 2: manifest first, then the hot copies, chunk by chunk, as
	// releaseHotCopies does for a migration batch.
	for i := 0; i < len(orphans); i += manifestChunk {
		end := min(i+manifestChunk, len(orphans))
		if err := m.manager.deleteFromManifest(ctx, paths[i:end], manifestReasonReconcile); err != nil {
			m.logger.Warn().Err(err).Int("kept_hot_copies", len(orphans)-i).
				Msg("Manifest update failed; keeping orphaned hot files for the next cycle")
			return orphansFound, deleted, failed + len(orphans) - i
		}
		for _, file := range orphans[i:end] {
			m.logger.Info().
				Str("path", file.Path).
				Str("database", file.Database).
				Str("measurement", file.Measurement).
				Int64("size_bytes", file.SizeBytes).
				Msg("Found orphaned hot file (metadata says cold), deleting from hot")

			if err := hotBackend.Delete(ctx, file.Path); err != nil {
				m.logger.Warn().Err(err).Str("path", file.Path).Msg("Failed to delete orphaned hot file")
				failed++
				continue
			}

			deleted++
			m.CleanupEmptyDirectories(ctx, file.Path)
		}
	}

	return orphansFound, deleted, failed
}

// ReconcileManifest removes cluster-manifest entries for files that are
// already in cold: files migrated before tiering kept the manifest in step,
// and files whose manifest step failed after their copy. While such an entry
// exists, peer replication pulls the file back onto every node and a
// restarted node fails catch-up on it. Only settled rows are considered —
// migrated_at older than manifestSettle; the sync stamps it from the
// object's own timestamp, so a copy still in flight on another node is never
// swept — and the cold object must exist with the size the manifest
// recorded: a row the sync took from a listing is not proof of a durable
// copy, and a foreign object at the path does not count. Returns how many
// entries were removed.
func (m *Migrator) ReconcileManifest(ctx context.Context, coldRows []FileMetadata) (int, error) {
	if m.manager.manifest == nil {
		return 0, nil
	}
	coldBackend := m.manager.GetBackendForTier(TierCold)
	if coldBackend == nil {
		return 0, nil
	}

	settled := time.Now().Add(-manifestSettle)
	var paths, unverified []string
	for _, row := range coldRows {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if row.Tier != TierCold || row.QuarantinedAt != nil || row.MigratedAt == nil || row.MigratedAt.After(settled) {
			continue
		}
		want, ok := m.manager.manifest.ManifestEntry(row.Path)
		if !ok {
			continue
		}
		got, err := coldBackend.StatFile(ctx, row.Path)
		if err != nil {
			m.logger.Warn().Err(err).Str("path", row.Path).Msg("Failed to stat cold object during manifest reconciliation")
			unverified = append(unverified, row.Path)
			continue
		}
		if got != want {
			// Absent, or not this file.
			unverified = append(unverified, row.Path)
			continue
		}
		paths = append(paths, row.Path)
	}
	if len(unverified) > 0 {
		m.logger.Warn().
			Int("count", len(unverified)).
			Strs("sample", unverified[:min(5, len(unverified))]).
			Msg("Cold rows still in the cluster manifest whose cold object is missing or differs in size; manifest entries kept and their hot copies still replicate. Restore or re-migrate the object, or let the reconciler remove the entry")
	}
	if len(paths) == 0 {
		return 0, nil
	}

	// Receipts before the entries go (#687): the manifest delete unlinks hot
	// copies cluster-wide.
	if err := m.manager.notifyHotFilesRemoved(paths); err != nil {
		return 0, fmt.Errorf("mark sync receipts: %w", err)
	}

	removed := 0
	for _, chunk := range chunkPaths(paths, manifestChunk) {
		if err := m.manager.deleteFromManifest(ctx, chunk, manifestReasonSweep); err != nil {
			return removed, err
		}
		removed += len(chunk)
	}
	m.logger.Info().Int("entries", removed).
		Msg("Removed cluster manifest entries for files already in cold; their replicas are unlinked cluster-wide, and a node without a cold tier can no longer read them")
	return removed, nil
}

// CleanupEmptyDirectories removes empty directories after file migration
// Walks up from the file's hour directory, removing empty dirs until hitting a non-empty one
// Path format: {database}/{measurement}/{year}/{month}/{day}/{hour}/
func (m *Migrator) CleanupEmptyDirectories(ctx context.Context, filePath string) {
	// Get DirectoryRemover interface from hot backend
	dirRemover, ok := m.manager.hotBackend.(storage.DirectoryRemover)
	if !ok {
		return // Backend doesn't support directory removal
	}

	// Extract directory path from file path
	dir := filepath.Dir(filePath)

	// Walk up the tree: hour -> day -> month -> year -> measurement -> database
	// Stop when we hit a non-empty directory or reach the root
	// Maximum depth of 6 prevents accidentally climbing too far
	for depth := 0; depth < 6; depth++ {
		if dir == "" || dir == "." {
			break
		}

		// Try to remove - will fail silently if not empty (os.Remove only removes empty dirs)
		err := dirRemover.RemoveDirectory(ctx, dir)
		if err != nil {
			// Directory not empty or other error - stop climbing
			break
		}

		m.logger.Debug().Str("dir", dir).Msg("Removed empty directory")
		dir = filepath.Dir(dir)
	}
}
