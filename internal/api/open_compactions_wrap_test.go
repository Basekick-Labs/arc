package api

import (
	"context"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The continuous query source read and the transform share one wrapper: a
// read_parquet(...) becomes a subquery that leaves the named files out, and a
// read with nothing open is unchanged.
func TestWrapOpenCompactions(t *testing.T) {
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	oc := &fakeOpenCompactionsPlain{keys: []string{"db/m/2026/10/20/10/a.parquet"}}
	expr := "read_parquet('/x/db/m/**/*.parquet', union_by_name=true)"
	got := wrapOpenCompactions(context.Background(), oc, b, expr, "db", "m")
	for _, want := range []string{
		"(SELECT * EXCLUDE (__arc_file) FROM read_parquet('/x/db/m/**/*.parquet', union_by_name=true, filename='__arc_file') WHERE __arc_file NOT IN (",
		"/db/m/2026/10/20/10/a.parquet')",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("wrapped read %q does not contain %q", got, want)
		}
	}
	if got := wrapOpenCompactions(context.Background(), oc, b, expr, "db", "other"); got != expr {
		t.Fatalf("a measurement with nothing open was rewritten: %q", got)
	}
	if got := wrapOpenCompactions(context.Background(), nil, b, expr, "db", "m"); got != expr {
		t.Fatalf("no registry rewrote the read: %q", got)
	}
	if got := wrapOpenCompactions(context.Background(), oc, b, "FROM "+expr, "db", "m"); !strings.HasPrefix(got, "FROM (SELECT * EXCLUDE") {
		t.Fatalf("keyword not kept in front: %q", got)
	}
}

type fakeOpenCompactionsPlain struct{ keys []string }

func (f *fakeOpenCompactionsPlain) ExcludedFor(_ context.Context, prefix string) []string {
	var out []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}
func (f *fakeOpenCompactionsPlain) Generation() uint64 { return 1 }
