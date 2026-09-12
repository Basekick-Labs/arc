package compaction

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Tests for #744. The manifest filename used to repeat the folded partition
// path, which jobID already contains, so the name grew with the database and
// measurement lengths and blew past the storage key segment limit.

func jobIDFor(database, partitionPath string) string {
	// Mirrors manager.go's construction.
	return fmt.Sprintf("%s_%s_%d_b%d",
		sanitizeDBForName(database),
		strings.ReplaceAll(partitionPath, "/", "_"),
		time.Now().UnixNano(),
		0,
	)
}

// TestManifestPathFitsTheKeyContract is the regression. At db=30/meas=60 the
// old format produced a 270-byte filename; the storage contract caps a segment
// at 255, so WriteManifest failed and that partition never compacted again.
func TestManifestPathFitsTheKeyContract(t *testing.T) {
	m := &ManifestManager{}

	for _, tt := range []struct {
		name    string
		dbLen   int
		measLen int
	}{
		{"short", 4, 3},
		{"moderate", 20, 40},
		{"over the old limit", 30, 60},
		{"maximum permitted", 64, 128},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := strings.Repeat("d", tt.dbLen)
			meas := strings.Repeat("m", tt.measLen)
			partition := filepath.Join(db, meas, "2026", "09", "12", "14")
			path := m.GenerateManifestPath("hourly", db, partition, jobIDFor(db, partition))

			if err := storage.ValidateKey(path); err != nil {
				t.Fatalf("manifest path is not a usable storage key: %v", err)
			}
			for _, seg := range strings.Split(path, "/") {
				if len(seg) > storage.MaxKeySegmentLen {
					t.Errorf("segment %q is %d bytes, over the %d-byte limit", seg, len(seg), storage.MaxKeySegmentLen)
				}
			}
			if !strings.HasSuffix(path, ".json") {
				t.Errorf("manifest path %q must end in .json; ListManifests filters on it", path)
			}
			if !strings.HasPrefix(path, ManifestBasePath+"/") {
				t.Errorf("manifest path %q must live under %q", path, ManifestBasePath)
			}
		})
	}
}

// TestManifestPathsAreDistinct pins where the distinctness actually comes from,
// which is NOT the filename.
//
// Worth being precise, because an earlier version of this comment was wrong:
// the adversarial pair below produces an IDENTICAL filename under both the old
// and the new format, because jobID itself folds them together
// (a + a_a_cpu and a_a + cpu give one jobID). What keeps the two manifests
// apart is the {database} path segment, and that was true before this change
// too. The filename contributes uniqueness per job, not per partition.
func TestManifestPathsAreDistinct(t *testing.T) {
	m := &ManifestManager{}

	// The pair that folds identically: database and measurement can both
	// contain "_", and the partition path already starts with the database.
	type tuple struct{ db, meas string }
	tuples := []tuple{
		{"a", "a_a_cpu"},
		{"a_a", "cpu"},
		{"a", "b_c"},
		{"a_b", "c"},
		{"prod", "cpu"},
	}

	seen := make(map[string]tuple, len(tuples))
	for _, tu := range tuples {
		partition := filepath.Join(tu.db, tu.meas, "2026", "09", "12", "14")
		// jobID held CONSTANT across tuples: a nanosecond timestamp would make
		// any encoding look injective.
		jobID := fmt.Sprintf("%s_%s_%d_b%d", sanitizeDBForName(tu.db),
			strings.ReplaceAll(partition, "/", "_"), 1757000000000000000, 0)
		path := m.GenerateManifestPath("hourly", tu.db, partition, jobID)
		if prev, clash := seen[path]; clash {
			t.Errorf("tuples %+v and %+v both produce %q", prev, tu, path)
		}
		seen[path] = tu
		// And the guarantee the filename itself owes: it must be one path
		// segment, so the database segment above can do its job.
		if strings.Contains(filepath.Base(path), "/") {
			t.Errorf("manifest filename %q spans more than one segment", filepath.Base(path))
		}
	}
}

// TestManifestPathRejectsAnEmptyJobID pins the hidden-filename guard.
func TestManifestPathRejectsAnEmptyJobID(t *testing.T) {
	m := &ManifestManager{}
	path := m.GenerateManifestPath("hourly", "db", "db/cpu/2026/09/12/14", "")
	if base := filepath.Base(path); strings.HasPrefix(base, ".") {
		t.Errorf("manifest filename %q starts with a dot; List hides it, so the manifest would never be recovered", base)
	}
}
