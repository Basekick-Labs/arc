package rehome

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// A job whose wait for the Iceberg export was waived (#1845 item 1) ends
// before the published table stops naming its old file. The pass that later
// removes the old file from the table must still commit that removal with
// the files that hold its rows, or a table published part way through that
// pass would lack some of them. The job's record goes at its end, so a
// separate record keeps what the export needs: the old file's key and its
// replacements'.
//
// It lives in a subdirectory of the journal, which Seed has never read as a
// job (a name with a "/" is skipped, in this build and in older ones): it
// owns no file, holds no job slot, and an older build ignores it. The
// export drops it once the published table does not name the old file
// (ReplacedDone).

// ReplacedRoot holds one record per waived job: _rehome/replaced/<id>.json.
const ReplacedRoot = JournalRoot + "/replaced"

// ReplacedRecord is what a waived job leaves for the export.
type ReplacedRecord struct {
	ID          string    `json:"id"`
	Database    string    `json:"database"`
	Measurement string    `json:"measurement"`
	Source      string    `json:"source"`
	Outputs     []string  `json:"outputs"`
	EndedAt     time.Time `json:"ended_at"`
}

func replacedKey(id string) string { return ReplacedRoot + "/" + id + ".json" }

// ErrReplacedInvalid: the job cannot be written as a replacement record (a
// key that is not a data file of its measurement); trying again cannot help.
var ErrReplacedInvalid = errors.New("the job cannot be recorded for the export")

// validate reports whether every key of the record is a data file of its
// database and measurement.
func (rec *ReplacedRecord) validate(id string) error {
	if !validID(id) || rec.ID != id || rec.Database == "" || rec.Measurement == "" || len(rec.Outputs) == 0 {
		return fmt.Errorf("incomplete record")
	}
	for _, k := range append([]string{rec.Source}, rec.Outputs...) {
		src, err := ParseKey(k)
		if err != nil {
			return err
		}
		if src.Database != rec.Database || src.Measurement != rec.Measurement {
			return fmt.Errorf("%s is not a file of %s.%s", k, rec.Database, rec.Measurement)
		}
	}
	return nil
}

// KeepReplaced writes the record of a job whose export wait was waived, made
// durable before the caller removes the old file. Call it before the
// removal.
func (r *Registry) KeepReplaced(ctx context.Context, j *Job) error {
	if r == nil || j == nil {
		return nil
	}
	rec := ReplacedRecord{ID: j.ID, Database: j.Database, Measurement: j.Measurement, Source: j.Source.Key, EndedAt: time.Now().UTC()}
	for _, o := range j.Outputs {
		rec.Outputs = append(rec.Outputs, o.Key)
	}
	if err := rec.validate(j.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrReplacedInvalid, err)
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := r.store.Write(ctx, replacedKey(j.ID), data); err != nil {
		return fmt.Errorf("write the job's replacement record: %w", err)
	}
	// In use from here: the file is written, and a restart reads it.
	r.mu.Lock()
	r.replacedAfter[rec.ID] = rec
	r.mu.Unlock()
	// Local storage: make the record durable before the old file goes. An
	// object store has nothing to sync.
	if syncer, ok := r.store.(interface {
		Sync(ctx context.Context, dir string) error
	}); ok {
		for _, dir := range []string{ReplacedRoot, JournalRoot} {
			if err := syncer.Sync(ctx, dir); err != nil && !storage.IsNotFound(err) && !os.IsNotExist(err) {
				return fmt.Errorf("the job's replacement record is written and in use, but could not be made durable: %w", err)
			}
		}
	}
	return nil
}

// ReplacedWaived returns, for the waived jobs under prefix ("db/measurement/"),
// the key of each old file with the keys of the files that hold its rows.
func (r *Registry) ReplacedWaived(prefix string) map[string][]string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out map[string][]string
	for _, rec := range r.replacedAfter {
		if rec.Database+"/"+rec.Measurement+"/" != prefix {
			continue
		}
		if out == nil {
			out = map[string][]string{}
		}
		out[rec.Source] = append([]string(nil), rec.Outputs...)
	}
	return out
}

// ReplacedDone drops the records of the waived jobs whose old file was source
// in database.measurement: the published table no longer names it. A record
// whose file cannot be deleted stays, and the next pass tries again.
func (r *Registry) ReplacedDone(ctx context.Context, database, measurement, source string) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	var ids []string
	for id, rec := range r.replacedAfter {
		if rec.Database == database && rec.Measurement == measurement && rec.Source == source {
			ids = append(ids, id)
		}
	}
	r.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		if err := r.store.Delete(ctx, replacedKey(id)); err != nil && !storage.IsNotFound(err) {
			return fmt.Errorf("remove the replacement record of job %s: %w", id, err)
		}
		r.mu.Lock()
		delete(r.replacedAfter, id)
		r.mu.Unlock()
	}
	return nil
}

// ReplacedSkipped names the replacement records Seed could not read or found
// not valid. They are left in place and not used: a lost record only brings
// back the gap a waived job had before #1845.
func (r *Registry) ReplacedSkipped() []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.replacedSkipped...)
}

// seedReplaced reads a record under ReplacedRoot (name is relative to
// JournalRoot) into after, or notes it skipped.
func (r *Registry) seedReplaced(ctx context.Context, key, name string, after map[string]ReplacedRecord, skipped *[]string) {
	id, ok := strings.CutSuffix(strings.TrimPrefix(name, "replaced/"), ".json")
	if !ok || strings.Contains(id, "/") || id == "" {
		return // a writer's temporary file
	}
	data, err := r.store.Read(ctx, key)
	if err != nil {
		if !storage.IsNotFound(err) {
			*skipped = append(*skipped, fmt.Sprintf("%s (%v)", key, err))
		}
		return
	}
	var rec ReplacedRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		*skipped = append(*skipped, fmt.Sprintf("%s (%v)", key, err))
		return
	}
	if err := rec.validate(id); err != nil {
		*skipped = append(*skipped, fmt.Sprintf("%s (%v)", key, err))
		return
	}
	after[id] = rec
}
