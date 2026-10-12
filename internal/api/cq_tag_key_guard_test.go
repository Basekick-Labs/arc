package api

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A real scheduled run (SQLite definitions, DuckDB, LocalBackend, ArrowBuffer)
// of a continuous query that groups by host AND region. Declared tag_columns
// of [host] alone would let compaction keep one row per (host, time) and
// delete the other regions' rows — the state #1023 makes easy to reach, since
// an update that leaves tag_columns out now keeps the stored list while the
// query changes. The run must fail, write nothing, record the failure with a
// reason naming tag_columns, and leave the watermark where it was so the
// window re-runs once the definition is fixed. With the full grouping
// declared the same run succeeds, and so does a raw copy whose duplicate rows
// are identical in every column.
func TestExecuteCQ_RefusesTagColumnsThatDoNotIdentifyARow(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dataDir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buf.Close() })

	ctx := context.Background()
	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	us := hour.UnixMicro()
	write := func(meas string, cols map[string][]interface{}) {
		t.Helper()
		if err := buf.WriteColumnarDirectNoWAL(ctx, "db1", meas, cols); err != nil {
			t.Fatal(err)
		}
	}
	write("cpu", map[string][]interface{}{
		"time":   {us, us + 1, us + 2, us + 3},
		"host":   {"a", "a", "a", "a"},
		"region": {"eu", "eu", "us", "us"},
		"usage":  {1.0, 2.0, 3.0, 4.0},
	})
	// The same reading twice, as a client retry leaves it before compaction.
	write("dup", map[string][]interface{}{
		"time":  {us, us},
		"host":  {"a", "a"},
		"usage": {1.0, 1.0},
	})
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	h := newSQLiteOnlyCQHandler(t, filepath.Join(root, "cq.db"))
	t.Cleanup(func() { h.sqliteDB.Close() })
	h.db, h.storage, h.arrowBuffer = duck, backend, buf

	watermark := hour.Add(-time.Hour).Format(time.RFC3339)
	const grouped = "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, region, avg(usage) AS usage " +
		"FROM db1.cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2, 3"
	const rawCopy = "SELECT time, host, usage FROM db1.dup WHERE time >= {start_time} AND time < {end_time}"
	seed := func(id int, name, query, dest, tags string) {
		t.Helper()
		if _, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries
			(id, name, database, source_measurement, destination_measurement, query, interval, tag_columns, is_active, last_processed_time)
			VALUES (?, ?, 'db1', ?, ?, ?, '1h', ?, 1, ?)`, id, name, map[int]string{1: "cpu", 2: "cpu", 3: "dup"}[id], dest, query, tags, watermark); err != nil {
			t.Fatal(err)
		}
	}
	seed(1, "stale_tags", grouped, "cpu_stale", `["host"]`)
	seed(2, "full_tags", grouped, "cpu_full", `["host","region"]`)
	seed(3, "raw_copy", rawCopy, "dup_copy", `["host"]`)

	destFiles := func(meas string) int {
		t.Helper()
		if err := buf.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
		n := 0
		_ = filepath.Walk(filepath.Join(dataDir, "db1", meas), func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && strings.HasSuffix(p, ".parquet") {
				n++
			}
			return nil
		})
		return n
	}

	_, err = h.ExecuteCQ(ctx, 1)
	if err == nil {
		t.Fatal("a run whose tag_columns do not identify a row was written")
	}
	if !errors.Is(err, errCQTagKeyNotUnique) {
		t.Errorf("failed for the wrong reason: %v", err)
	}
	if n := destFiles("cpu_stale"); n != 0 {
		t.Errorf("a refused run wrote %d file(s)", n)
	}
	var status, reason string
	if err := h.sqliteDB.QueryRow(`SELECT status, error_message FROM continuous_query_executions WHERE query_id = 1`).
		Scan(&status, &reason); err != nil {
		t.Fatalf("no execution recorded for the refused run: %v", err)
	}
	if status != "failed" || !strings.Contains(reason, "tag_columns") {
		t.Errorf("execution recorded as %q with %q, want failed naming tag_columns", status, reason)
	}
	if strings.Contains(reason, "'") {
		t.Errorf("reason carries an apostrophe, which the log masking hook treats as a quote: %q", reason)
	}
	var stored string
	if err := h.sqliteDB.QueryRow(`SELECT last_processed_time FROM continuous_queries WHERE id = 1`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if got, _ := time.Parse(time.RFC3339, stored); !got.Equal(hour.Add(-time.Hour)) {
		t.Errorf("watermark moved to %q after a refused run, want %q", stored, watermark)
	}

	// The admin execute endpoint reports the same refusal as a definition
	// problem (422), not a server fault.
	app := fiber.New()
	app.Post("/continuous-queries/:id/execute", h.handleExecute)
	code, body := sendCQ(t, app, "POST", "/continuous-queries/1/execute",
		`{"start_time":"`+hour.Format(time.RFC3339)+`","end_time":"`+hour.Add(time.Hour).Format(time.RFC3339)+`"}`)
	if code != fiber.StatusUnprocessableEntity || !strings.Contains(body, "tag_columns") {
		t.Errorf("manual execute: expected 422 naming tag_columns, got %d: %s", code, body)
	}

	resp, err := h.ExecuteCQ(ctx, 2)
	if err != nil {
		t.Fatalf("a run whose tag_columns cover its grouping failed: %v", err)
	}
	if resp.RecordsWritten != 2 {
		t.Errorf("wrote %d rows, want 2 (one per region)", resp.RecordsWritten)
	}
	if n := destFiles("cpu_full"); n == 0 {
		t.Error("an accepted run wrote no file")
	}

	if _, err := h.ExecuteCQ(ctx, 3); err != nil {
		t.Fatalf("a raw copy whose duplicate rows are identical failed: %v", err)
	}
}

func TestTagTimeKeyIsUnique(t *testing.T) {
	cases := []struct {
		name   string
		data   map[string][]interface{}
		tags   []string
		unique bool
	}{
		{"distinct tags", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {"a", "b"}}, []string{"h"}, true},
		{"repeated tag and time", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {"a", "a"}, "v": {1.0, 2.0}}, []string{"h"}, false},
		{"same tag, different time", map[string][]interface{}{"time": {int64(1), int64(2)}, "h": {"a", "a"}}, []string{"h"}, true},
		// DuckDB groups NULLs together and apart from "": nil, "" and the
		// string a naive fmt.Sprint(nil) would produce are three keys. The
		// cases that must stay distinct carry a differing column v, so a
		// weakened encoding that merged their keys would be reported.
		{"nil vs empty vs <nil>", map[string][]interface{}{"time": {int64(1), int64(1), int64(1)}, "h": {nil, "", "<nil>"}, "v": {1.0, 2.0, 3.0}}, []string{"h"}, true},
		{"two nils", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {nil, nil}, "v": {1.0, 2.0}}, []string{"h"}, false},
		// A boundary in the concatenation must not merge two keys.
		{"ab|c vs a|bc", map[string][]interface{}{"time": {int64(1), int64(1)}, "x": {"ab", "a"}, "y": {"c", "bc"}, "v": {1.0, 2.0}}, []string{"x", "y"}, true},
		// Without a length on each value, a tag value that ends in the bytes
		// of the next value's type header shifts the boundary and two
		// different rows encode identically.
		{"value carrying a type header", map[string][]interface{}{"time": {int64(1), int64(1)}, "x": {"a\x06string", "a"}, "y": {"b", "\x06stringb"}, "v": {1.0, 2.0}}, []string{"x", "y"}, true},
		{"[]byte values", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {[]byte("a"), []byte("a")}, "v": {1.0, 2.0}}, []string{"h"}, false},
		{"-0 and 0 are one value", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {0.0, negZero()}, "v": {1.0, 2.0}}, []string{"h"}, false},
		{"type differs", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {int64(1), "1"}, "v": {1.0, 2.0}}, []string{"h"}, true},
		{"unparsed string time is refused", map[string][]interface{}{"time": {"x", "y"}, "h": {"a", "b"}}, []string{"h"}, false},
		{"nil time is refused", map[string][]interface{}{"time": {nil, int64(1)}, "h": {"a", "b"}}, []string{"h"}, false},
		// The writer accepts any integer or float time kind, so the guard does.
		{"int32 times with distinct keys", map[string][]interface{}{"time": {int32(1), int32(1)}, "h": {"a", "b"}}, []string{"h"}, true},
		{"int32 times repeating a key", map[string][]interface{}{"time": {int32(1), int32(1)}, "h": {"a", "a"}, "v": {1.0, 2.0}}, []string{"h"}, false},
		// Rows identical in every column lose nothing to dedup.
		{"identical rows", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {"a", "a"}, "v": {1.0, 1.0}}, []string{"h"}, true},
		{"same key, other column differs", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {"a", "a"}, "v": {1.0, 2.0}}, []string{"h"}, false},
		{"float32 -0 and 0 are one value", map[string][]interface{}{"time": {int64(1), int64(1)}, "h": {float32(0), negZero32()}, "v": {1.0, 2.0}}, []string{"h"}, false},
		{"single row", map[string][]interface{}{"time": {"x"}, "h": {"a"}}, []string{"h"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tagTimeKeyIsUnique(tc.data, tc.tags)
			if (err == nil) != tc.unique {
				t.Errorf("unique = %v (err %v), want %v", err == nil, err, tc.unique)
			}
		})
	}
}

func negZero32() float32 {
	z := float32(0)
	return -z
}

func negZero() float64 {
	z := 0.0
	return -z
}
