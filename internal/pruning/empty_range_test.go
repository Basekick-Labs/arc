package pruning

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestIsExactTimeRange(t *testing.T) {
	exact := []string{
		"SELECT v FROM cpu WHERE time >= '2026-02-01T00:00:00Z' AND time < '2026-02-02T00:00:00Z'",
		"SELECT v FROM db.cpu WHERE host = 'a' AND time >= '2026-02-01' AND time < '2026-02-02' ORDER BY time LIMIT 10",
		"SELECT v FROM cpu c WHERE (c.time >= '2026-02-01' AND c.time < '2026-02-02') AND host = 'x'",
		"SELECT v FROM cpu WHERE host='a' AND time >= '2026-02-01' AND time < '2026-02-02'",
		"SELECT v FROM cpu WHERE time >= NOW() - INTERVAL 6 HOUR AND time < NOW() - INTERVAL 1 HOUR",
		"SELECT v FROM cpu WHERE time >= NOW() - INTERVAL '6 hours' AND time < NOW() + INTERVAL '1 hour' GROUP BY host",
		"SELECT avg(v) FROM cpu WHERE time >= '2026-02-01' AND time < '2026-02-02' AND message LIKE '%GROUP BY%'",
	}
	inexact := []string{
		// A JOIN applies one side's predicate to both tables.
		"SELECT c.v FROM cpu c JOIN meta m ON c.host = m.host WHERE c.time >= '2030-01-01' AND c.time < '2030-01-02'",
		"SELECT v FROM cpu WHERE time >= '2030-01-01' OR host = 'x'",
		"SELECT v FROM cpu WHERE NOT (time >= '2030-01-01' AND time < '2030-01-02')",
		"SELECT v FROM cpu WHERE host IN (SELECT host FROM meta WHERE time >= '2030-01-01')",
		"SELECT v FROM cpu WHERE time >= '2030-01-08'::TIMESTAMP - INTERVAL 7 DAY AND time < '2030-01-10'",
		"SELECT v FROM cpu WHERE time >= '2030-01-08' + INTERVAL 1 DAY",
		"SELECT v FROM cpu WHERE timestamp >= '2030-01-01' AND timestamp < '2030-01-02'",
		"WITH w AS (SELECT * FROM cpu) SELECT * FROM w WHERE time >= '2030-01-01'",
		"SELECT v FROM cpu WHERE time >= '2030-01-01' UNION ALL SELECT v FROM mem WHERE time >= '2030-01-01'",
		"SELECT v FROM cpu WHERE time = '2030-01-01'",
		"SELECT v FROM cpu WHERE time >= '2030-01-01' AND time <> '2030-01-05'",
		"SELECT v FROM cpu WHERE host = 'x'",
		"SELECT v FROM cpu WHERE time < '2030-01-02'",                                 // end only: start is assumed
		"SELECT v FROM cpu WHERE time >= '2030-01-02'",                                // start only: end is assumed
		"SELECT v FROM cpu WHERE time >= NOW() - INTERVAL '6 hours' AND time < NOW()", // bare NOW() is not a bound the extractor reads
		"SELECT v FROM cpu WHERE CASE WHEN time >= '2030-01-01' THEN 1 ELSE 0 END = 1",
		"SELECT v FROM cpu WHERE last_time >= '2030-01-01' AND time < '2030-01-02'",
		// A keyword glued to a closing quote is still a keyword to DuckDB.
		"SELECT v FROM cpu WHERE time >= '2026-02-10' AND time < '2026-02-11' AND host='x'OR host='y'",
		"SELECT v FROM cpu WHERE time >= '2026-02-10' AND time < '2026-02-11' AND v=1OR v=2",
		// Inclusive ends reach the next hour directory.
		"SELECT v FROM cpu WHERE time >= '2026-01-31T23:00:00Z' AND time <= '2026-02-01T00:00:00Z'",
		"SELECT v FROM cpu WHERE time BETWEEN '2026-02-01' AND '2026-02-02'",
		// Literals DuckDB accepts but the extractor cannot parse make it
		// assume an end of now plus one day.
		"SELECT v FROM cpu WHERE time >= '2026-02-01' AND time < '2026-02-04T23:00:00'",
		"SELECT v FROM cpu WHERE time >= NOW() - INTERVAL 1 HOUR AND time < NOW() + INTERVAL 1 YEAR",
		"SELECT v FROM cpu WHERE time >= NOW() - INTERVAL 1 HOUR AND time < NOW() + INTERVAL '1 day 12 hours'",
	}
	for _, q := range exact {
		if !IsExactTimeRange(q) {
			t.Errorf("expected exact: %s", q)
		}
	}
	for _, q := range inexact {
		if IsExactTimeRange(q) {
			t.Errorf("expected inexact: %s", q)
		}
	}
}

// countingRemote is a remote-shaped backend that records listings and can
// fail day-level file checks.
type countingRemote struct {
	mockS3Backend
	dayFiles     map[string][]string // day prefix -> direct parquet files
	listDirCalls int
	listCalls    int
	failList     bool
}

func (c *countingRemote) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	c.listDirCalls++
	return c.mockS3Backend.ListDirectories(ctx, prefix)
}

func (c *countingRemote) List(ctx context.Context, prefix string) ([]string, error) {
	c.listCalls++
	if c.failList {
		return nil, errors.New("simulated list failure")
	}
	var out []string
	for _, f := range c.dayFiles[prefix] {
		out = append(out, prefix+f)
	}
	return out, nil
}

func (c *countingRemote) Type() string                                             { return "s3" }
func (c *countingRemote) ReadToAt(context.Context, string, io.Writer, int64) error { return nil }
func (c *countingRemote) StatFile(context.Context, string) (int64, error)          { return -1, nil }

func TestEmptyRangeProofRemote(t *testing.T) {
	newPruner := func(remote *countingRemote) *PartitionPruner {
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(remote)
		return p
	}
	glob := "s3://bucket/db/cpu/**/*.parquet"
	sql := "SELECT v FROM db.cpu WHERE time >= '2026-02-01T00:00:00Z' AND time < '2026-02-03T00:00:00Z'"

	t.Run("proven when layout standard and listings verified", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/":         {"2026"},
			"db/cpu/2026/01/": {"01"},
		}}}
		p := newPruner(remote)
		ctx, vol := WithVolatileResult(context.Background())
		_, optimized, empty := p.OptimizeTablePathVerdict(ctx, glob, sql)
		if !empty || optimized {
			t.Fatalf("empty=%v optimized=%v", empty, optimized)
		}
		if !vol.Volatile {
			t.Fatal("a proven-empty verdict must be volatile")
		}
		// Two covered days: one parent listing per day for the hour paths,
		// one month listing for the two day-level paths, one layout listing.
		// No day directory exists, so no day-level file check ran. The
		// first pass had no cache hits, so no second pass ran.
		if remote.listDirCalls != 4 || remote.listCalls != 0 {
			t.Fatalf("listDir=%d list=%d", remote.listDirCalls, remote.listCalls)
		}
		if _, _, ok := p.partitionCache.get(p.partitionCache.cacheKey(glob, sql)); ok {
			t.Fatal("proven-empty verdict must not be cached")
		}
		// A second identical query re-proves from listings younger than
		// the freshness window: the cache answers, no new calls.
		_, _, empty = p.OptimizeTablePathVerdict(context.Background(), glob, sql)
		if !empty || remote.listDirCalls != 4 || remote.listCalls != 0 {
			t.Fatalf("second proof: empty=%v listDir=%d list=%d", empty, remote.listDirCalls, remote.listCalls)
		}
		// Once the cached listings are older than the window, the second
		// pass really lists again (three new parent listings; the layout
		// entry is still valid), and nothing changed: still proven.
		age := func() {
			p.globCache.mu.Lock()
			for k, e := range p.globCache.entries {
				if !strings.HasPrefix(k, "layout:") {
					e.setAt = e.setAt.Add(-3 * time.Second)
					p.globCache.entries[k] = e
				}
			}
			p.globCache.mu.Unlock()
		}
		age()
		_, _, empty = p.OptimizeTablePathVerdict(context.Background(), glob, sql)
		if !empty || remote.listDirCalls != 7 {
			t.Fatalf("stale cache must be re-listed: empty=%v listDir=%d", empty, remote.listDirCalls)
		}
		// A directory that appeared meanwhile flips the verdict.
		age()
		remote.existingDirs["db/cpu/2026/02/"] = []string{"01"}
		remote.existingDirs["db/cpu/2026/02/01/"] = []string{"05"}
		_, _, empty = p.OptimizeTablePathVerdict(context.Background(), glob, sql)
		if empty {
			t.Fatal("a partition created after the cached listing must defeat the proof")
		}
	})
	t.Run("not proven when a day-level listing fails", func(t *testing.T) {
		// The day directory exists (so the day-level file check runs) but
		// its listing fails: absence is not verified.
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{"db/cpu/": {"2026"}, "db/cpu/2026/02/": {"01"}}}, failList: true}
		p := newPruner(remote)
		if _, _, empty := p.OptimizeTablePathVerdict(context.Background(), glob, sql); empty {
			t.Fatal("listing failure must fail open")
		}
	})
	t.Run("not proven on a spoke namespace layout", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{"db/cpu/": {"metrics", "events"}}}}
		p := newPruner(remote)
		if _, _, empty := p.OptimizeTablePathVerdict(context.Background(), glob, sql); empty {
			t.Fatal("measurement-name children mean the generated paths are one level shallow")
		}
	})
	t.Run("not proven for a missing measurement", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{}}}
		p := newPruner(remote)
		if _, _, empty := p.OptimizeTablePathVerdict(context.Background(), glob, sql); empty {
			t.Fatal("a measurement with no directory keeps DuckDB's no-files handling")
		}
	})
	t.Run("not proven beyond the span bound or for inexact SQL", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{"db/cpu/": {"2026"}}}}
		p := newPruner(remote)
		// A second pruner on a second counter runs plain pruning for the
		// same queries: the gated proof must cost no listing beyond that.
		plainRemote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{"db/cpu/": {"2026"}}}}
		plain := newPruner(plainRemote)
		for name, q := range map[string]string{
			"9-day range":   "SELECT v FROM db.cpu WHERE time >= '2026-02-01' AND time < '2026-02-10'",
			"JOIN":          "SELECT c.v FROM db.cpu c JOIN db.meta m ON c.host = m.host WHERE c.time >= '2026-02-01' AND c.time < '2026-02-02'",
			"assumed start": "SELECT v FROM db.cpu WHERE time < '2026-02-02'",
			"assumed end":   "SELECT v FROM db.cpu WHERE time >= '2026-02-01'",
			"inclusive end": "SELECT v FROM db.cpu WHERE time >= '2026-02-01' AND time <= '2026-02-02'",
			"glued keyword": "SELECT v FROM db.cpu WHERE time >= '2026-02-01' AND time < '2026-02-02' AND host='a'OR host='b'",
		} {
			if _, _, empty := p.OptimizeTablePathVerdict(context.Background(), glob, q); empty {
				t.Fatalf("%s must not be proven", name)
			}
			plain.OptimizeTablePath(context.Background(), glob, q)
		}
		if remote.listDirCalls != plainRemote.listDirCalls || remote.listCalls != plainRemote.listCalls {
			t.Fatalf("gated proof must not list beyond plain pruning: proof listDir=%d list=%d, plain listDir=%d list=%d",
				remote.listDirCalls, remote.listCalls, plainRemote.listDirCalls, plainRemote.listCalls)
		}
	})
	t.Run("plain OptimizeTablePath is unchanged and cached", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{"db/cpu/": {"2026"}}}}
		p := newPruner(remote)
		result, optimized := p.OptimizeTablePath(context.Background(), glob, sql)
		if optimized || result != glob {
			t.Fatalf("result=%v optimized=%v", result, optimized)
		}
		if _, _, ok := p.partitionCache.get(p.partitionCache.cacheKey(glob, sql)); !ok {
			t.Fatal("the plain fallback verdict is still cached")
		}
		// A cached fallback verdict does not stand in for a proof.
		if _, _, empty := p.OptimizeTablePathVerdict(context.Background(), glob, sql); !empty {
			t.Fatal("proof must re-derive past a cached fallback")
		}
	})
}

func TestEmptyRangeProofLocal(t *testing.T) {
	root := t.TempDir()
	meas := filepath.Join(root, "db", "cpu")
	if err := os.MkdirAll(filepath.Join(meas, "2026", "01", "01", "00"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(meas, "2026", "01", "01", "00", "a.parquet"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Local backend for the layout check target only; local existence uses Glob.
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(backend)
	glob := filepath.Join(root, "db", "cpu", "**", "*.parquet")
	sql := "SELECT v FROM db.cpu WHERE time >= '2026-02-01T00:00:00Z' AND time < '2026-02-02T00:00:00Z'"
	ctx, vol := WithVolatileResult(context.Background())
	if _, _, empty := p.OptimizeTablePathVerdict(ctx, glob, sql); !empty || !vol.Volatile {
		t.Fatalf("expected a proven-empty February: empty=%v volatile=%v", empty, vol.Volatile)
	}
	// A file flushed into February after the cached (empty) glob results:
	// within the freshness window the proof still stands, past it the new
	// directory is seen.
	feb := filepath.Join(meas, "2026", "02", "01", "03")
	if err := os.MkdirAll(feb, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(feb, "b.parquet"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p.globCache.mu.Lock()
	for k, e := range p.globCache.entries {
		if strings.HasPrefix(k, "layout:") {
			continue
		}
		e.setAt = e.setAt.Add(-3 * time.Second)
		p.globCache.entries[k] = e
	}
	p.globCache.mu.Unlock()
	if _, _, empty := p.OptimizeTablePathVerdict(context.Background(), glob, sql); empty {
		t.Fatal("fresh February file not seen")
	}
	// A stray file and a dot entry in the measurement directory do not make
	// the layout non-standard; a name-shaped child does.
	if err := os.WriteFile(filepath.Join(meas, ".DS_Store"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	p.globCache.invalidate()
	if !p.layoutIsStandard(context.Background(), root, "db", "cpu") {
		t.Fatal("dot entries must be ignored")
	}
	if err := os.MkdirAll(filepath.Join(meas, "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	p.globCache.invalidate()
	if p.layoutIsStandard(context.Background(), root, "db", "cpu") {
		t.Fatal("a measurement-name child must fail the layout check")
	}
}
