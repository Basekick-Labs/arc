package fieldschema

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func names(s *arrow.Schema) string {
	var out []string
	for _, f := range s.Fields() {
		out = append(out, f.Name)
	}
	return strings.Join(out, ",")
}

// #1342: the anchor drops field names Arc cannot read back.
func TestCleanDropsUnreadableNames(t *testing.T) {
	cases := []struct {
		in   *arrow.Schema
		want string
	}{
		// Bootstrap DESCRIBE of a file with Time first: time_1 is the real time.
		{fields("Time", tFloat, "time_1", tTsTZ, "v", tFloat), "time,v"},
		{fields("time", tTsTZ, "Time", tFloat, "v", tFloat), "time,v"},
		{fields("time", tTsTZ, "Temp", tFloat, "temp", tFloat), "time,Temp"},
		{fields("time", tTsTZ, "Temp", tFloat, "temp_1", tFloat), "time,Temp"}, // DESCRIBE's rename of temp
		{fields("time", tTsTZ, "cpu", tFloat, "cpu_1", tFloat), "time,cpu,cpu_1"},
		{fields("time", tTsTZ, "t\xb0", tFloat, "v", tFloat), "time,v"},
		{fields("TIME", tFloat, "v", tFloat), "time,v"},
	}
	for _, c := range cases {
		got, _ := Clean(c.in)
		if names(got) != c.want {
			t.Errorf("Clean(%s) = %s, want %s", names(c.in), names(got), c.want)
		}
	}
	got, _ := Clean(fields("Time", tFloat, "time_1", tTsTZ))
	if !arrow.TypeEqual(got.Field(0).Type, tTsTZ) {
		t.Errorf("time rebuilt as %v, want the renamed column's type", got.Field(0).Type)
	}
}

// A stored anchor written before #1342 is cleaned on load, rewritten on the
// next flush, and a query of the anchor alone binds time again.
func TestDirtyStoredAnchorIsHealed(t *testing.T) {
	r, b, _ := newTestRegistry(t, Options{})
	ctx := context.Background()
	dirty := arrow.NewSchema([]arrow.Field{
		{Name: "Time", Type: tFloat, Nullable: true},
		{Name: "time_1", Type: tTsTZ, Nullable: true},
		{Name: "v", Type: tFloat, Nullable: true},
	}, nil)
	data, err := EncodeAnchor(dirty)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, AnchorKey("db", "m"), data); err != nil {
		t.Fatal(err)
	}
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "v", tFloat), nil); err != nil {
		t.Fatal(err)
	}
	data, err = b.Read(ctx, AnchorKey("db", "m"))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := DecodeAnchor(data)
	if err != nil {
		t.Fatal(err)
	}
	if names(stored) != "time,v" {
		t.Fatalf("stored anchor = %s, want it rewritten clean", names(stored))
	}
	path, ok := r.Resolve(ctx, "db", "m")
	if !ok {
		t.Fatal("no local anchor")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	q := "SELECT count(*) FROM read_parquet('" + filepath.ToSlash(path) + "', union_by_name=true) WHERE time > TIMESTAMPTZ '2020-01-01'"
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("query of the anchor alone: %v", err)
	}
}

// KnownColumnFolds answers from memory, the first spelling winning.
func TestKnownColumnFolds(t *testing.T) {
	r, _, _ := newTestRegistry(t, Options{})
	if r.KnownColumnFolds("db", "m") != nil {
		t.Fatal("an unknown measurement must answer nil")
	}
	if err := r.Ensure(context.Background(), "db", "m", fields("time", tTsTZ, "Temp", tFloat), nil); err != nil {
		t.Fatal(err)
	}
	if got := r.KnownColumnFolds("db", "m")["temp"]; got != "Temp" {
		t.Fatalf("folds[temp] = %q", got)
	}
}
