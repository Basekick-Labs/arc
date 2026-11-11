package backup

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/storage"
)

// icebergHintName is the Hadoop-convention pointer Arc publishes in every
// in-root Iceberg table's metadata directory: the bare number of the newest
// published v<N>.metadata.json.
const icebergHintName = "version-hint.text"

// listInRootIcebergMetadata lists the in-root Iceberg metadata, classified as
// the inventory listing classifies it (step 1). Step 3b lists it again AFTER
// the SQLite snapshot (#1613): the export keeps committing through the data
// copy, and a list taken before the snapshot misses the metadata file and
// v<N> a snapshotted catalog row may point at.
func listInRootIcebergMetadata(ctx context.Context, lister storage.ObjectLister) ([]storage.ObjectInfo, error) {
	objects, err := lister.ListObjects(ctx, "")
	if err != nil {
		return nil, err
	}
	var out []storage.ObjectInfo
	for _, obj := range objects {
		if isStagedCompaction(obj.Path) || isCompactionState(obj.Path) {
			continue
		}
		if isIcebergMetadata(obj.Path) {
			out = append(out, obj)
		}
	}
	return out, nil
}

// splitIcebergHints separates the version hints from the rest of the metadata.
func splitIcebergHints(files []storage.ObjectInfo) (rest, hints []storage.ObjectInfo) {
	for _, f := range files {
		if path.Base(f.Path) == icebergHintName {
			hints = append(hints, f)
		} else {
			rest = append(rest, f)
		}
	}
	return rest, hints
}

// publishedVersionsByDir maps each metadata directory to the v<N> numbers in
// files.
func publishedVersionsByDir(files []storage.ObjectInfo) map[string]map[int]bool {
	out := make(map[string]map[int]bool)
	for _, f := range files {
		base := path.Base(f.Path)
		if !strings.HasPrefix(base, "v") || !strings.HasSuffix(base, ".metadata.json") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(base, "v"), ".metadata.json"))
		if err != nil || n < 0 {
			continue
		}
		dir := path.Dir(f.Path)
		if out[dir] == nil {
			out[dir] = make(map[int]bool)
		}
		out[dir][n] = true
	}
	return out
}

// backupHintBody returns the hint bytes to store for a table whose live hint
// reads live, given the v<N> listed in its directory. A hint is a mutable key
// read at copy time, after the listing, so it can name a v<N> newer than any
// the backup holds (#1613). Then it is pointed at the newest one the backup
// does hold, so DuckDB and Spark reading the restored directory resolve a
// table, and a restored catalog row adopts a version that exists. ok is false
// when the backup holds no v<N> for the table: the hint is left out, and a
// restore finds the table by its catalog row alone. A hint that is not a
// number is copied as it is: it is not this function's to repair.
func backupHintBody(live []byte, listed map[int]bool) (body []byte, rewritten, ok bool) {
	n, err := strconv.Atoi(strings.TrimSpace(string(live)))
	if err != nil || n < 0 || listed[n] {
		return live, false, true
	}
	newest := -1
	for v := range listed {
		newest = max(newest, v)
	}
	if newest < 0 {
		return nil, false, false
	}
	return []byte(strconv.Itoa(newest)), true, true
}

// copyIcebergHints copies each table's version hint after its v<N> files, so
// the hint stored names a v<N> the backup holds (backupHintBody). It returns
// the number of hints skipped.
func (m *Manager) copyIcebergHints(ctx context.Context, leg *backupLeg, hints []storage.ObjectInfo, versions map[string]map[int]bool) (int64, error) {
	run := leg.run
	progress := run.progress
	var skipped int64
	for _, h := range hints {
		if err := ctx.Err(); err != nil {
			return skipped, err
		}
		destPath := run.id + "/data/" + h.Path
		live, err := m.dataStorage.Read(ctx, h.Path)
		if err != nil {
			if storage.IsNotFound(err) {
				// Dropped since the listing: a restore finds no table there,
				// as the live warehouse does.
				skipped++
				continue
			}
			return skipped, fmt.Errorf("failed to read Iceberg version hint %s: %w", h.Path, err)
		}
		body, rewritten, ok := backupHintBody(live, versions[path.Dir(h.Path)])
		if !ok || leg.target.keyTooLong(destPath) {
			skipped++
			continue
		}
		if rewritten {
			m.logger.Info().Str("path", h.Path).Str("live", strings.TrimSpace(string(live))).Str("stored", string(body)).
				Msg("Iceberg version hint is ahead of the metadata this backup listed (the export committed during the backup); storing the newest version the backup holds")
		}
		if err := leg.target.backend.Write(ctx, destPath, body); err != nil {
			m.cleanupPartialBackupWrite(ctx, leg.target, destPath)
			return skipped, fmt.Errorf("failed to write Iceberg version hint to %s: %w", leg.target.describe(), err)
		}
		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, int64(len(body)))
		atomic.AddInt64(&leg.files, 1)
		atomic.AddInt64(&leg.bytes, int64(len(body)))
	}
	if skipped > 0 {
		atomic.AddInt64(&progress.SkippedFiles, skipped)
		atomic.AddInt64(&leg.skipped, skipped)
	}
	run.publishTargets()
	return skipped, nil
}
