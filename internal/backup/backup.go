package backup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// errBackupRead marks a failure reading the SOURCE file from data storage.
//
// Only source-read failures are skippable: the file may legitimately have been
// deleted by compaction or retention between the listing and the copy, and
// aborting the whole backup for that benign race would be wrong.
//
// Every other failure — temp file creation, seek, backup-storage write — is
// fatal. Those indicate a broken environment (no temp space, unwritable or
// unreachable backup storage), not a race, and continuing would produce a
// backup that silently omits files while reporting success.
//
// Classification is deliberately positive rather than by exclusion: a failure
// mode added here later is fatal by default until someone marks it skippable.
var errBackupRead = errors.New("backup source read failed")

// maxSkipRatio is the fraction of data files that may be skipped before the
// backup is treated as failed rather than merely incomplete.
//
// Skipping exists to tolerate one narrow race: a file removed by compaction or
// retention between the listing and the copy. That affects a small number of
// files at the tail of a run, so a low ceiling is enough to absorb it. Anything
// above this is a different event — throttling, credential expiry, a storage
// outage — where returning a fraction of the data as a "successful" backup
// hides the gap until a restore needs it.
//
// Deliberately a constant, not a config key: no operator has needed to tune it,
// and a knob nobody sets is a knob nobody tests.
const maxSkipRatio = 0.10

// icebergCatalogDBName is the filename the Iceberg SQL catalog is stored under
// inside a backup's metadata/ directory, when it is a separate database from the
// shared one (which is stored as arc.db).
const icebergCatalogDBName = "iceberg-catalog.db"

func isSourceReadError(err error) bool {
	return errors.Is(err, errBackupRead)
}

// BackupOptions controls what gets backed up and where.
type BackupOptions struct {
	IncludeMetadata bool // back up the SQLite database
	IncludeConfig   bool // back up arc.toml
}

// BackupResult is returned when a backup completes.
type BackupResult struct {
	Manifest *Manifest
	Duration time.Duration
}

// CreateBackup performs a full backup. It runs synchronously; the API layer
// launches it in a goroutine and exposes progress via GetProgress().
func (m *Manager) CreateBackup(ctx context.Context, opts BackupOptions) (*BackupResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	backupID := generateBackupID()
	startTime := time.Now()

	progress := &Progress{
		Operation: "backup",
		BackupID:  backupID,
		Status:    "running",
		StartedAt: startTime,
	}
	m.setProgress(progress)
	defer func() {
		now := time.Now()
		progress.CompletedAt = &now
		m.setProgress(progress)
	}()

	m.logger.Info().Str("backup_id", backupID).Msg("Starting backup")

	// ── 1. Discover data files ──────────────────────────────────────────
	objectLister, ok := m.dataStorage.(storage.ObjectLister)
	if !ok {
		progress.Status = "failed"
		progress.Error = "storage backend does not support listing objects"
		return nil, fmt.Errorf("storage backend does not support ListObjects")
	}

	// Enumerate the hidden set FIRST, and the inventory second. The two are
	// separate passes, so a file renamed between them is seen by one or the
	// other depending on the order: this way a key fixed mid-backup is reported
	// as unaddressable AND copied, which is a spurious warning. The reverse
	// order loses it from both, which is silently the very bug this guards
	// against (#756).
	unaddressable := m.findUnaddressable(ctx, progress)

	objects, err := objectLister.ListObjects(ctx, "")
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to list data files: %w", err)
	}

	// Filter to .parquet data files (the manifest inventory), and separately collect Iceberg
	// warehouse metadata (metadata.json / .avro / version-hint.text under an Iceberg table's
	// metadata/ dir). The Iceberg metadata is NOT parquet, so the old .parquet-only filter
	// silently dropped it — a restore then lost the Iceberg tables (the referenced parquet
	// survived, but the table metadata pointing at it did not). It is copied via the same
	// mechanism but kept out of the db/measurement inventory below.
	var parquetFiles []storage.ObjectInfo
	var icebergMetaFiles []storage.ObjectInfo
	for _, obj := range objects {
		switch {
		case strings.HasSuffix(obj.Path, ".parquet"):
			parquetFiles = append(parquetFiles, obj)
		case isIcebergMetadata(obj.Path):
			icebergMetaFiles = append(icebergMetaFiles, obj)
		}
	}

	// MEDIUM: decide the fatal case before doing any work. Copying every
	// Iceberg metadata file and only then failing would leave a half-written
	// backupID/data/... tree in backup storage with no manifest, which
	// ListBackups keys on and therefore can neither show nor clean up.
	if len(unaddressable) > 0 && len(parquetFiles) == 0 {
		err := fmt.Errorf("backup failed: all %d data file(s) in source storage have a key that cannot be addressed, so the backup would contain no data; rename them to conform to the storage key rules (see the log for paths)", len(unaddressable))
		m.logger.Error().Int("unaddressable", len(unaddressable)).
			Strs("sample", sampleUnaddressable(unaddressable)).Msg(err.Error())
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// Build manifest inventory
	manifest := &Manifest{
		Version:    "dev",
		BackupID:   backupID,
		CreatedAt:  startTime.UTC(),
		BackupType: "full",
	}

	dbMap := make(map[string]*DatabaseInfo)
	for _, obj := range parquetFiles {
		manifest.TotalFiles++
		manifest.TotalSizeBytes += obj.Size

		db, meas := parseDBMeasurement(obj.Path)
		di, exists := dbMap[db]
		if !exists {
			di = &DatabaseInfo{Name: db}
			dbMap[db] = di
		}
		di.FileCount++
		di.SizeBytes += obj.Size

		// Find or create measurement entry
		found := false
		for i := range di.Measurements {
			if di.Measurements[i].Name == meas {
				di.Measurements[i].FileCount++
				di.Measurements[i].SizeBytes += obj.Size
				found = true
				break
			}
		}
		if !found {
			di.Measurements = append(di.Measurements, MeasurementInfo{
				Name:      meas,
				FileCount: 1,
				SizeBytes: obj.Size,
			})
		}
	}
	for _, di := range dbMap {
		manifest.Databases = append(manifest.Databases, *di)
	}

	// Progress total includes Iceberg metadata files (copied in step 2b) so ProcessedFiles
	// never exceeds TotalFiles. The manifest inventory (TotalFiles) counts only data files.
	progress.TotalFiles = manifest.TotalFiles + int64(len(icebergMetaFiles))
	progress.TotalBytes = manifest.TotalSizeBytes
	m.setProgress(progress)

	// ── 2. Copy parquet files ───────────────────────────────────────────
	if err := m.copyDataFiles(ctx, backupID, parquetFiles, progress); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// ── 2b. Copy Iceberg warehouse metadata (if any) ────────────────────
	// Same copy mechanism + path preservation as data files, so restore round-trips them to
	// their original locations and the SQLite catalog's metadata pointers still resolve. The
	// referenced parquet data is already copied above; only the Iceberg metadata is added here.
	if len(icebergMetaFiles) > 0 {
		if err := m.copyDataFiles(ctx, backupID, icebergMetaFiles, progress); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		m.logger.Info().Int("files", len(icebergMetaFiles)).Msg("Backed up Iceberg warehouse metadata")
	}

	// Evaluate the skip ratio once, over every file group above.
	if err := m.checkSkipRatio(progress, len(parquetFiles)+len(icebergMetaFiles)); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// Record files that could never be copied because no listing returns them.
	// The fatal case was decided before any copying began.
	m.recordUnaddressable(manifest, unaddressable, len(parquetFiles))

	// ── 3. Copy SQLite metadata ─────────────────────────────────────────
	if opts.IncludeMetadata && m.sqliteDBPath != "" {
		if err := m.backupSQLite(ctx, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup SQLite database")
			// Non-fatal: continue with backup
		} else {
			manifest.HasMetadata = true
		}

		// The Iceberg SQL catalog, when the operator put it in its own file.
		// It holds every Iceberg table's schema and snapshot pointers: without
		// it a restore brings back the Parquet and the warehouse metadata but
		// the tables no longer resolve. Empty when it lives in the shared
		// database, which the copy above already covers.
		if m.icebergCatalogDBPath != "" {
			if err := m.backupSQLiteFile(ctx, backupID, m.icebergCatalogDBPath, icebergCatalogDBName); err != nil {
				m.logger.Warn().Err(err).
					Str("path", m.icebergCatalogDBPath).
					Msg("Failed to backup Iceberg catalog database")
			} else {
				manifest.HasIcebergCatalog = true
			}
		}
	}

	// ── 4. Copy config ──────────────────────────────────────────────────
	if opts.IncludeConfig && m.configPath != "" {
		if err := m.backupConfig(ctx, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup config file")
		} else {
			manifest.HasConfig = true
		}
	}

	// ── 5. Write manifest ───────────────────────────────────────────────
	// Record files that were inventoried but proved unreadable, so the manifest
	// does not claim contents the backup does not actually hold.
	manifest.SkippedFiles = atomic.LoadInt64(&progress.SkippedFiles)

	manifestData, err := MarshalManifest(manifest)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}
	manifestPath := fmt.Sprintf("%s/manifest.json", backupID)
	if err := m.backupStorage.Write(ctx, manifestPath, manifestData); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}

	progress.Status = "completed"
	duration := time.Since(startTime)

	m.logger.Info().
		Str("backup_id", backupID).
		Int64("files", manifest.TotalFiles).
		Int64("bytes", manifest.TotalSizeBytes).
		Int64("skipped", manifest.SkippedFiles).
		Dur("duration", duration).
		Msg("Backup completed")

	return &BackupResult{Manifest: manifest, Duration: duration}, nil
}

// copyDataFiles copies parquet files from data storage to backup storage.
//
// Files whose source cannot be read are skipped (see errBackupRead) and added to
// progress.SkippedFiles. Every other failure aborts the backup immediately.
//
// The skip-ratio check is NOT applied here, because CreateBackup calls this more
// than once (data files, then Iceberg warehouse metadata) and the ratio is only
// meaningful over the whole backup: a handful of stale entries in a small
// metadata set is a large fraction of that set but a negligible fraction of the
// backup. The caller evaluates the ratio once via checkSkipRatio.
func (m *Manager) copyDataFiles(ctx context.Context, backupID string, files []storage.ObjectInfo, progress *Progress) error {
	var skipped int64

	for _, obj := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		destPath := fmt.Sprintf("%s/data/%s", backupID, obj.Path)
		written, err := m.streamBackupFile(ctx, obj.Path, destPath)
		if err != nil {
			// Only a source-read failure is skippable — the file may have been
			// deleted by compaction/retention between listing and copy. Anything
			// else (temp file, seek, backup write) means the environment is broken
			// and continuing would silently drop files from the backup.
			if !isSourceReadError(err) {
				return fmt.Errorf("failed to back up %s: %w", obj.Path, err)
			}
			skipped++
			m.logger.Warn().Str("path", obj.Path).Err(err).Msg("Failed to read data file, skipping")
			continue
		}

		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, written)
		// Republish so /status polling sees live counters — published Progress
		// values are immutable snapshots, not the struct being mutated here.
		m.setProgress(progress)

		if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
			m.logger.Info().
				Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
				Int64("total", progress.TotalFiles).
				Msg("Backup progress")
		}
	}

	// Accumulate rather than overwrite: CreateBackup calls this once per file
	// group, and a later group must not erase an earlier group's skips.
	atomic.AddInt64(&progress.SkippedFiles, skipped)
	m.setProgress(progress)

	if skipped == 0 {
		return nil
	}

	copied := atomic.LoadInt64(&progress.ProcessedFiles)

	m.logger.Warn().
		Int64("skipped", skipped).
		Int64("copied", copied).
		Msg("Files skipped during backup — backup will be incomplete")

	return nil
}

// findUnaddressable inventories data files that exist in source storage but
// that no listing returns, so nothing driven by a listing could copy them.
//
// Filtered with the same rule the inventory uses, because the storage layer
// reports everything a listing hid and only what a backup would have carried is
// the operator's loss: OS debris such as .DS_Store is hidden for good reason and
// naming it here would cry wolf on every macOS deployment.
//
// A backend that cannot enumerate them contributes nothing, which is correct
// rather than optimistic: it is the same position every caller was in before.
func (m *Manager) findUnaddressable(ctx context.Context, progress *Progress) []storage.UnusableObject {
	lister, ok := m.dataStorage.(storage.UnusableLister)
	if !ok {
		// Not clean, unchecked. Said out loud so a zero in the manifest is not
		// read as a guarantee.
		m.logger.Debug().Msg("Storage backend cannot enumerate hidden files; the backup cannot confirm it is complete")
		return nil
	}
	hidden, err := lister.ListUnusable(ctx, "")
	if err != nil {
		// Not fatal: failing the backup because the diagnostic failed would be
		// worse than the gap it reports. Loud, because the count is now part of
		// whether the backup can call itself complete.
		m.logger.Warn().Err(err).Msg("Could not check for unaddressable files; the backup cannot confirm it is complete")
		return nil
	}
	var out []storage.UnusableObject
	for _, o := range hidden {
		if isBackupPayload(o.Path) {
			out = append(out, o)
		}
	}
	// Set unconditionally, including 0: the gauge describes the store as of the
	// backup that just ran, so a deployment that fixed its keys must see it
	// fall back to zero rather than stay latched on the first bad file.
	metrics.Get().SetStorageUnaddressableFiles(int64(len(out)))
	if len(out) > 0 {
		atomic.AddInt64(&progress.UnaddressableFiles, int64(len(out)))
	}
	return out
}

// isBackupPayload reports whether a path is something CreateBackup would have
// copied had it been addressable, and therefore something whose absence is the
// operator's loss.
//
// It mirrors the inventory split above, and deliberately covers more than
// ".parquet". Iceberg metadata is the reason that split exists at all: losing a
// metadata.json or .avro loses a whole table even when every Parquet file it
// references survives, so an unaddressable one is worse than an unaddressable
// data file, not lesser. And a ".parquet.part" key on an object store is an
// ordinary committed object there (S3 and Azure do not stage), which the
// storage layer reports precisely because nothing else can name it.
func isBackupPayload(p string) bool {
	if isIcebergMetadata(p) {
		return true
	}
	return strings.HasSuffix(strings.TrimSuffix(p, storage.PartSuffix), ".parquet")
}

// unaddressableSampleCap bounds how many paths land in the manifest. The
// manifest is one JSON blob written to storage, and the over-length key shape
// makes each path up to a kilobyte, so an unbounded list could dwarf the
// manifest it is reported in.
const unaddressableSampleCap = 32

// recordUnaddressable puts the finding in the manifest and decides whether the
// run may still call itself a complete backup.
//
// Deliberately NOT folded into checkSkipRatio. That guard exists for a
// transient race (a file compaction removed between listing and copy), its
// denominator is the addressable inventory, and it short-circuits when
// SkippedFiles is zero, which is exactly the case here. Worse, its ratio would
// hard-fail a nine-file deployment with one legacy file forever, and its
// message sends the operator to diagnose storage rather than rename a file.
func (m *Manager) recordUnaddressable(manifest *Manifest, unaddressable []storage.UnusableObject, addressableFiles int) {
	if len(unaddressable) == 0 {
		return
	}

	manifest.UnaddressableFiles = int64(len(unaddressable))
	for i, o := range unaddressable {
		if i == unaddressableSampleCap {
			break
		}
		manifest.UnaddressableSample = append(manifest.UnaddressableSample, o.Path)
	}

	m.logger.Warn().
		Int("unaddressable", len(unaddressable)).
		Int("addressable", addressableFiles).
		Strs("sample", manifest.UnaddressableSample).
		Msg("Backup is incomplete: these files exist in storage but their key cannot be addressed, so they were not copied; rename them to conform to the storage key rules")
}

// sampleUnaddressable bounds a path list for logging, same cap as the manifest.
func sampleUnaddressable(objs []storage.UnusableObject) []string {
	out := make([]string, 0, unaddressableSampleCap)
	for i, o := range objs {
		if i == unaddressableSampleCap {
			break
		}
		out = append(out, o.Path)
	}
	return out
}

// checkSkipRatio fails the backup when too large a fraction of it was unreadable.
//
// Skipping tolerates one specific thing: a file removed by compaction or retention
// between the listing and the copy. That race touches a handful of files at the
// tail of a run. A large fraction of the backup failing to read is a different
// event — throttling, credential expiry, a storage outage — and silently returning
// a fraction of the data as a successful backup is how an operator discovers the
// gap at restore time instead of at backup time.
//
// Evaluated once over every file group, not per group: a stale entry or two in a
// small Iceberg metadata set is a large fraction of that set but a negligible
// fraction of the backup, and must not abort a run whose data files all copied.
func (m *Manager) checkSkipRatio(progress *Progress, totalFiles int) error {
	skipped := atomic.LoadInt64(&progress.SkippedFiles)
	if skipped == 0 || totalFiles == 0 {
		return nil
	}
	if float64(skipped) > maxSkipRatio*float64(totalFiles) {
		return fmt.Errorf("backup failed: %d of %d files were unreadable (>%.0f%%), source storage may be degraded",
			skipped, totalFiles, maxSkipRatio*100)
	}
	return nil
}

// streamBackupFile streams a file from data storage to backup storage via a temp file,
// avoiding loading the entire file into memory (important for large Parquet files).
// It returns the number of bytes actually copied.
//
// Only a source-read failure is wrapped with errBackupRead (making it skippable by
// the caller); temp file, seek, and write failures are returned unwrapped and are
// fatal to the backup.
func (m *Manager) streamBackupFile(ctx context.Context, srcPath, destPath string) (int64, error) {
	tmpFile, err := os.CreateTemp("", "arc-backup-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// Stream from data storage to temp file
	if err := m.dataStorage.ReadTo(ctx, srcPath, tmpFile); err != nil {
		return 0, fmt.Errorf("failed to read from data storage: %w: %w", errBackupRead, err)
	}

	// Size the upload from the temp file rather than the listing: the listing is a
	// point-in-time snapshot that compaction or retention may have invalidated, and
	// a declared size that disagrees with the reader can truncate the upload on
	// backends that send it as Content-Length. Matches streamRestoreFile.
	info, err := tmpFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat temp file: %w", err)
	}
	size := info.Size()

	// Rewind for upload
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Stream from temp file to backup storage
	if err := m.backupStorage.WriteReader(ctx, destPath, tmpFile, size); err != nil {
		m.cleanupPartialWrite(ctx, destPath)
		return 0, fmt.Errorf("failed to write to backup storage: %w", err)
	}

	return size, nil
}

// cleanupPartialWrite removes the staging file a failed WriteReader leaves behind.
//
// LocalBackend.WriteReader deliberately preserves "<path>.part" on failure so the
// file-replication puller can resume from the last committed byte. Backup has no
// resume path — a retried backup starts over under a fresh backup ID — so that
// staging file is unreferenced garbage: never read, never listed as a backup
// (it has no manifest.json), and holding disk equal to the bytes transferred
// before the failure.
//
// Best-effort by design: the write already failed, so the cleanup very likely
// fails too (unwritable volume, storage unreachable). A cleanup failure must not
// mask the real error, so it is logged at debug and discarded.
func (m *Manager) cleanupPartialWrite(ctx context.Context, destPath string) {
	// Addressed through the staging API rather than by appending the suffix to
	// the key. That suffix is reserved now, because the staging file of key
	// "x" used to BE the committed object "x.part" (#744). A backend that does
	// not stage never leaves a partial, so there is nothing to clean up.
	si, ok := m.backupStorage.(storage.StagingInspector)
	if !ok {
		return
	}
	if err := si.DeleteStaged(ctx, destPath); err != nil {
		m.logger.Debug().
			Str("path", destPath).
			Err(err).
			Msg("Could not remove partial backup staging file")
	}
}

// backupSQLite copies the shared SQLite database into the backup.
func (m *Manager) backupSQLite(ctx context.Context, backupID string) error {
	return m.backupSQLiteFile(ctx, backupID, m.sqliteDBPath, "arc.db")
}

// snapshotSQLite writes a consistent snapshot of the live database at dbPath to
// a fresh temporary file and returns its path. The caller must remove it.
//
// VACUUM INTO reads the database (including un-checkpointed WAL frames) under
// one read transaction, so concurrently committing writers cannot interleave
// pages into the snapshot — the failure mode of checkpoint-then-copy, where a
// checkpoint between the copy's start and end rewrites the main file mid-read
// once the WAL passes the auto-checkpoint threshold.
func snapshotSQLite(ctx context.Context, dbPath string) (string, error) {
	dir, err := os.MkdirTemp(filepath.Dir(dbPath), ".arc-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	snapshotPath := filepath.Join(dir, "snapshot.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to open SQLite for snapshot: %w", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", snapshotPath); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("SQLite snapshot failed: %w", err)
	}
	if err := os.Chmod(snapshotPath, 0600); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to restrict SQLite snapshot: %w", err)
	}
	return snapshotPath, nil
}

// backupSQLiteFile copies one SQLite database file into the backup under
// metadata/<destName>, as a consistent snapshot of the live database streamed
// via the storage backend rather than a file read of the live path.
func (m *Manager) backupSQLiteFile(ctx context.Context, backupID, dbPath, destName string) error {
	snapshotPath, err := snapshotSQLite(ctx, dbPath)
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Dir(snapshotPath))

	// Get file size for WriteReader
	info, err := os.Stat(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to stat SQLite snapshot: %w", err)
	}
	size := info.Size()

	// Stream via temp file to avoid loading entire DB in memory
	f, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite snapshot: %w", err)
	}
	defer f.Close()

	destPath := fmt.Sprintf("%s/metadata/%s", backupID, destName)
	if err := m.backupStorage.WriteReader(ctx, destPath, f, size); err != nil {
		return fmt.Errorf("failed to write SQLite backup: %w", err)
	}

	m.logger.Info().
		Str("backup_id", backupID).
		Str("database", destName).
		Int64("bytes", size).
		Msg("SQLite database backed up")
	return nil
}

// backupConfig copies the arc.toml config file into the backup.
//
// SECURITY: arc.toml typically contains plaintext credentials (S3 secret key,
// Azure account key, cluster shared secret). The backup storage must be treated
// as secret material with the same access controls as the live config.
// Operators who cannot secure backup storage should set backup.include_config
// to false in arc.toml.
func (m *Manager) backupConfig(ctx context.Context, backupID string) error {
	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	destPath := fmt.Sprintf("%s/config/arc.toml", backupID)
	if err := m.backupStorage.Write(ctx, destPath, data); err != nil {
		return fmt.Errorf("failed to write config backup: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file backed up")
	return nil
}

// isIcebergMetadata reports whether a storage path is an Iceberg warehouse metadata file that
// must be backed up alongside data. Iceberg tables written by Arc's exporter live at
// {nsPrefix}_{db}.db/{measurement}/metadata/*, containing table metadata (*.metadata.json,
// incl. our v<N>.metadata.json reader copies), manifest lists + manifests (*.avro), and
// version-hint.text (current-version pointer for directory-based readers).
//
// Deliberately a catch-all — ANY non-parquet file under a "/metadata/" segment — rather than an
// allowlist of today's extensions. Iceberg keeps adding metadata file types (e.g. Puffin
// .puffin statistics/index files); an allowlist silently drops them from the backup and loses
// them on restore. Over-copying a stray file is cheap; losing table metadata is not.
//
// Safe against a user measurement literally named "metadata": its files are .parquet, and the
// caller's switch tests the .parquet branch FIRST, so data files never reach this predicate.
// The referenced parquet DATA files are backed up normally.
func isIcebergMetadata(p string) bool {
	p = filepath.ToSlash(p)
	if !strings.Contains(p, "/metadata/") {
		return false
	}
	return !strings.HasSuffix(p, ".parquet")
}

// parseDBMeasurement extracts the database and measurement from a storage path.
// Path format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.parquet
func parseDBMeasurement(path string) (database, measurement string) {
	path = filepath.ToSlash(path)
	parts := strings.SplitN(path, "/", 3)
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	if len(parts) == 1 {
		return parts[0], "unknown"
	}
	return "unknown", "unknown"
}
