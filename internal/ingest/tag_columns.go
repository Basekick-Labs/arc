package ingest

import (
	"strings"
	"time"
)

// writableTagColumns returns the tag names that may go into a file's arc:tags
// metadata, in their original order, and the names it leaves out.
//
// Compaction reads arc:tags back, unions it across a partition's files and
// keys its dedup on those names (internal/compaction/dedup.go). A name that is
// not a column of the file fails to bind there, on every cycle, so the
// partition never compacts (#1258). Names come from clients — MessagePack
// tag_keys and line-protocol tag keys — so each is checked against the batch:
//   - empty, or "time" in any case: time is always part of the dedup key;
//   - containing ',': arc:tags is comma-joined, so "a,b" reads back as a and b;
//   - starting with '_': inferSchema leaves those columns out of the file;
//   - not a column of the batch, or a repeat.
//
// Other characters are kept: compaction already skips dedup for a name it
// does not accept as an identifier (a dotted OTel key, say), which is safe.
func writableTagColumns(columns map[string]interface{}, tags []string) (kept, dropped []string) {
	if len(tags) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(tags))
	kept = make([]string, 0, len(tags))
	for _, name := range tags {
		_, repeat := seen[name]
		_, present := columns[name]
		if name == "" || strings.EqualFold(name, "time") || strings.Contains(name, ",") ||
			name[0] == '_' || !present || repeat {
			dropped = append(dropped, name)
			continue
		}
		seen[name] = struct{}{}
		kept = append(kept, name)
	}
	return kept, dropped
}

// fileTagColumns applies writableTagColumns to a flush's merged batch and
// logs, sampled, the names it leaves out. The rows are written either way.
func (b *ArrowBuffer) fileTagColumns(database, measurement string, merged *TypedColumnBatch) []string {
	kept, dropped := writableTagColumns(merged.Data, merged.TagColumns)
	if len(dropped) > 0 {
		now := time.Now().UnixNano()
		last := b.tagDropLastLogNano.Load()
		if now-last >= walDropLogIntervalNano && b.tagDropLastLogNano.CompareAndSwap(last, now) {
			b.logger.Warn().
				Str("database", database).
				Str("measurement", measurement).
				Strs("dropped_tags", dropped).
				Strs("kept_tags", kept).
				Msg("Tag names that are not usable columns were left out of the file's tag metadata; the rows are written")
		}
	}
	return kept
}
