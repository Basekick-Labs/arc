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
func (f *fakeOpenCompactionsPlain) Open(prefix string) bool {
	return len(f.ExcludedFor(context.Background(), prefix)) > 0
}

// A continuous query reads its source through the same exclusion.
func TestContinuousQuerySourceReadExcludesOpenCompactions(t *testing.T) {
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h := &ContinuousQueryHandler{storage: b}
	h.SetOpenCompactions(&fakeOpenCompactionsPlain{keys: []string{"db/m/2026/10/20/10/a.parquet"}})
	got := h.sourceRead(context.Background(), &ContinuousQuery{Database: "db", SourceMeasurement: "m"}, "/x/db/m/**/*.parquet")
	if !strings.Contains(got, "WHERE __arc_file NOT IN (") || !strings.Contains(got, "a.parquet") {
		t.Fatalf("continuous query source read %q does not leave the open compaction's files out", got)
	}
}

// tieredFake names some of its keys as files in cold storage, and counts how
// often it is asked.
type tieredFake struct {
	fakeOpenCompactionsPlain
	cold        []string
	tiered, old int
}

func (f *tieredFake) ExcludedTiered(ctx context.Context, prefix string) ([]string, []string) {
	f.tiered++
	return f.fakeOpenCompactionsPlain.ExcludedFor(ctx, prefix), f.cold
}

func (f *tieredFake) ExcludedFor(ctx context.Context, prefix string) []string {
	f.old++
	return f.fakeOpenCompactionsPlain.ExcludedFor(ctx, prefix)
}

// #1825: a file in cold storage is left out by its address there, the name a
// read over both tiers reports it under; every key keeps its hot address.
// The keys and which of them are cold come from one call.
func TestExclusionsNameAColdFileByItsColdAddress1825(t *testing.T) {
	hot, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cold, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const coldKey, hotKey = "db/m/2026/01/05/x_daily.parquet", "db/m/2026/01/06/o_daily.parquet"
	src := &tieredFake{fakeOpenCompactionsPlain: fakeOpenCompactionsPlain{keys: []string{coldKey, hotKey}}, cold: []string{coldKey}}
	// Through the combination, as the query handler holds it.
	oc := CombineOpenCompactions(&fakeOpenCompactionsPlain{keys: []string{"db/m/2026/01/07/c.parquet"}}, src)
	excl := snapshotOpenCompactions(context.Background(), oc, "db", "m")
	if src.tiered != 1 || src.old != 0 {
		t.Fatalf("the source was asked %d time(s) for the tiered answer and %d for the plain one; want once and never", src.tiered, src.old)
	}
	if len(excl.keys) != 3 || len(excl.coldKeys) != 1 || excl.coldKeys[0] != coldKey {
		t.Fatalf("snapshot: %+v", excl)
	}
	expr := "read_parquet(['/hot/db/m/**/*.parquet', '/cold/db/m/**/*.parquet'], union_by_name=true)"
	coldURI, _ := storage.ObjectURI(cold, coldKey)
	hotOfCold, _ := storage.ObjectURI(hot, coldKey)
	hotURI, _ := storage.ObjectURI(hot, hotKey)
	coldOfHot, _ := storage.ObjectURI(cold, hotKey)

	got := excl.apply(hot, cold, expr)
	for _, want := range []string{"'" + coldURI + "'", "'" + hotOfCold + "'", "'" + hotURI + "'"} {
		if !strings.Contains(got, want) {
			t.Fatalf("the read %q does not leave out %s", got, want)
		}
	}
	if strings.Contains(got, coldOfHot) {
		t.Fatalf("a hot file was left out by a cold address too: %q", got)
	}
	// With no cold tier there is no cold glob to filter.
	if got := excl.apply(hot, nil, expr); strings.Contains(got, coldURI) {
		t.Fatalf("with no cold backend: %q", got)
	}
}
