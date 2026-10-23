package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
)

// #1364: a measurement no tier holds a file of any more (every file deleted
// while its tier rows stayed) was read as "(SELECT * WHERE 1=0)", which DuckDB
// refuses to bind, so even count(*) answered "* expression without FROM
// clause". With the schema anchor it reads as zero rows with the
// measurement's columns.
func TestNoTierReadWithAnAnchorIsEmptyWithColumns(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "gone", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	e.mustRows("SELECT count(*) FROM sch.gone") // builds the anchor

	ctx := context.Background()
	from := e.h.noTierRead(ctx, "sch", "gone", "FROM", buildReadParquetOptions())
	var n int64
	if err := e.duck.DB().QueryRowContext(ctx, "SELECT count(*) "+from).Scan(&n); err != nil {
		t.Fatalf("count(*) %s: %v", from, err)
	}
	if n != 0 {
		t.Fatalf("count = %d, want 0 from the anchor alone", n)
	}
	var avg any
	if err := e.duck.DB().QueryRowContext(ctx, "SELECT avg(v) "+from+" WHERE host = 'a'").Scan(&avg); err != nil {
		t.Fatalf("a query naming the measurement's columns does not bind: %v", err)
	}
}

// Without an anchor it reads as the hot glob over nothing: the "No files
// found" the handler answers with an empty result, as for a measurement that
// never had a file.
func TestNoTierReadWithoutAnAnchorIsTheEmptyMeasurementRead(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, false)
	e.h.SetFieldSchema(nil)
	ctx := context.Background()
	from := e.h.noTierRead(ctx, "sch", "never", "FROM", buildReadParquetOptions())
	_, err := e.duck.DB().ExecContext(ctx, "SELECT count(*) "+from)
	if err == nil {
		t.Fatal("a glob over no files read without error")
	}
	if kind := e.h.classifyNoFiles(err); kind != noFilesEmpty {
		t.Fatalf("classified %v (%v), want noFilesEmpty: an empty result, not an error", kind, err)
	}
}
