package backup

// Apply-at-boot metadata restore (#635, second half).
//
// The API restore path STAGES a restored SQLite database next to the live one
// and never touches the live file: a running server keeps serving consistent
// pre-restore data, and the staged file is applied here, at boot, before any
// subsystem has opened the database. This removes the live-swap hazards the
// first half of #635 documented (stale-sidecar corruption, connection pools
// splitting across inodes) instead of merely narrowing them.
//
// Crash convergence: every step below is a remove or rename, ordered so a
// crash at ANY point converges on the next boot. The one non-obvious rule is
// resume-on-missing-destination: a crash between the safety rename and the
// final rename leaves no live database with the pending file still staged —
// the next boot MUST apply the pending file rather than skip, or the server
// would boot onto a fresh empty database and the restore (and, for auth.db,
// every credential) would be silently lost.

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"

	"github.com/rs/zerolog"
)

// PendingRestoreSuffix is appended to a database path to name its staged
// restore. The suffix survives crashes; boot applies or quarantines it.
const PendingRestoreSuffix = ".pending-restore"

// sqliteMagic is the 16-byte header prefix of every valid SQLite database.
var sqliteMagic = []byte("SQLite format 3\x00")

// StagePath returns the pending-restore path for a database path.
func StagePath(dbPath string) string { return dbPath + PendingRestoreSuffix }

// ApplyPendingRestores applies staged restores for the given database paths.
// MUST be called at boot before anything opens those databases. Duplicate or
// empty paths are tolerated (the shared-database layout passes the same file
// for several roles). Errors are contained per path: a failure to apply one
// staged restore quarantines or leaves it and lets the boot continue on the
// current database, logged loudly — a staged restore must never boot-loop the
// server or apply garbage over a working database.
func ApplyPendingRestores(logger zerolog.Logger, dbPaths ...string) {
	seen := make(map[string]bool, len(dbPaths))
	for _, dbPath := range dbPaths {
		if dbPath == "" || seen[dbPath] {
			continue
		}
		seen[dbPath] = true
		applyPendingRestore(logger, dbPath)
	}
}

func applyPendingRestore(logger zerolog.Logger, dbPath string) {
	pending := StagePath(dbPath)
	if _, err := os.Stat(pending); err != nil {
		return
	}
	log := logger.With().Str("db", dbPath).Logger()

	// Never rename garbage over a working database: a crash during staging or
	// disk truncation can leave a partial file. Quarantine instead of apply.
	if err := validatePendingSQLite(pending); err != nil {
		rejected := pending + ".rejected"
		if renameErr := os.Rename(pending, rejected); renameErr != nil {
			log.Error().Err(renameErr).Msg("Staged restore is invalid and could not be quarantined; leaving it — it will be re-checked next boot")
		} else {
			log.Error().Err(err).Str("quarantined_to", rejected).
				Msg("Staged restore failed validation; quarantined — booting with the current database")
		}
		return
	}

	before := dbPath + ".before-restore"
	if _, err := os.Stat(dbPath); err == nil {
		// Fold un-checkpointed WAL commits into the main file so the safety
		// copy is complete. Nothing else has the database open at boot.
		ckErr := checkpointSQLite(dbPath)

		// Remove an OLDER restore's safety sidecars: after this run the
		// safety copy is either checkpointed (no sidecars) or gets this run's
		// sidecars moved in below — stale ones from a previous restore beside
		// it are exactly the mismatched-WAL hazard #635 documented.
		removeIgnoreMissing(log, before+"-wal")
		removeIgnoreMissing(log, before+"-shm")

		if ckErr == nil {
			removeIgnoreMissing(log, dbPath+"-wal")
			removeIgnoreMissing(log, dbPath+"-shm")
		} else {
			// Could not fold the WAL in (locked or damaged database). Keep
			// the sidecars WITH the safety copy so it remains recoverable.
			log.Warn().Err(ckErr).Msg("Checkpoint before restore apply failed; moving WAL sidecars alongside the safety copy")
			renameIgnoreMissing(log, dbPath+"-wal", before+"-wal")
			renameIgnoreMissing(log, dbPath+"-shm", before+"-shm")
		}

		if err := os.Rename(dbPath, before); err != nil {
			log.Error().Err(err).Msg("Could not move the current database aside; staged restore NOT applied — booting with the current database")
			return
		}
		log.Info().Str("safety_copy", before).Msg("Current database moved aside before restore apply")
	}
	// else: no current database — either a fresh node, or the
	// resume-on-missing-destination case after a crash mid-apply.

	if err := os.Rename(pending, dbPath); err != nil {
		log.Error().Err(err).Msg("Could not move the staged restore into place; it remains staged for the next boot")
		return
	}
	log.Warn().Msg("Staged restore applied at boot; the previous database (if any) is at the .before-restore path")
}

// validatePendingSQLite refuses files that are not plausibly a complete
// SQLite database: header magic, minimum size, and an integrity quick_check
// on a read-only connection (nothing else is running at boot).
func validatePendingSQLite(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	header := make([]byte, len(sqliteMagic))
	_, readErr := f.Read(header)
	info, statErr := f.Stat()
	f.Close()
	if readErr != nil {
		return fmt.Errorf("read header: %w", readErr)
	}
	if !bytes.Equal(header, sqliteMagic) {
		return fmt.Errorf("not a SQLite database (bad header magic)")
	}
	if statErr == nil && info.Size() < 512 {
		return fmt.Errorf("implausibly small for a SQLite database (%d bytes)", info.Size())
	}

	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open for quick_check: %w", err)
	}
	defer db.Close()
	var result string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&result); err != nil {
		return fmt.Errorf("quick_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("quick_check: %s", result)
	}
	return nil
}

// checkpointSQLite folds the WAL into the main database file.
func checkpointSQLite(path string) error {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

func removeIgnoreMissing(log zerolog.Logger, path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("path", path).Msg("Could not remove file during restore apply")
	}
}

func renameIgnoreMissing(log zerolog.Logger, from, to string) {
	if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("from", from).Msg("Could not move file during restore apply")
	}
}
