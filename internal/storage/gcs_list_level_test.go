//go:build !fips

package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestGCSListLevel(t *testing.T) {
	s := &gcsStub{objects: []string{
		"inst/db/cpu/2026/01/15/cpu_daily.parquet",
		"inst/db/cpu/2026/01/15/cpu_daily2.parquet",
		"inst/db/cpu/2026/01/15/10/a.parquet",
		"inst/db/cpu/2026/01/15/11/b.parquet",
		"inst/db/cpu/2026/01/16/10/c.parquet",
		"other/db/cpu/2026/01/15/z.parquet",
	}}
	b, err := stubGCSBackend(t, s, "inst", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// No trailing separator from the caller: the method adds one.
	dirs, files, err := b.ListLevel(ctx, "db/cpu/2026/01/15")
	assertLevel(t, dirs, files, err, "10,11", "cpu_daily.parquet,cpu_daily2.parquet")

	dirs, files, err = b.ListLevel(ctx, "db/cpu/2026/01/16/")
	assertLevel(t, dirs, files, err, "10", "")

	for _, p := range s.seenPrefixes {
		if !strings.HasPrefix(p, "inst/") {
			t.Fatalf("a listing asked for %q, outside the backend prefix", p)
		}
	}
}
