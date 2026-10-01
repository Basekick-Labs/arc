package fieldschema

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

func TestAnchorCompletenessIssue928(t *testing.T) {
	r, b, _ := newTestRegistry(t, Options{Bootstrap: true, BootstrapMaxFiles: 2})
	ctx := context.Background()

	// Round trip of the flag.
	data, err := EncodeAnchorComplete(fields("time", tTsTZ, "v", tInt64), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, complete, err := DecodeAnchorComplete(data); err != nil || !complete {
		t.Fatalf("complete flag lost: %v %v", complete, err)
	}
	if s, _ := DecodeAnchor(data); s.Metadata().Len() != 0 {
		t.Fatal("DecodeAnchor must strip metadata")
	}

	// Ingest creates the anchor for a measurement that has no files yet:
	// complete, and it stays complete as columns are added.
	// The file is written before the registry hears about it, as ingest
	// does; the key tells the registry it is the measurement's only file.
	if err := b.Write(ctx, "db/fresh/2026/01/01/00/first.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.EnsureFile(ctx, "db", "fresh", fields("time", tTsTZ, "v", tInt64), nil, "db/fresh/2026/01/01/00/first.parquet"); err != nil {
		t.Fatal(err)
	}
	if !r.IsComplete("db", "fresh") {
		t.Fatal("anchor for a new measurement must be complete")
	}
	// Without the key the registry cannot tell and stays cautious.
	if err := b.Write(ctx, "db/unknown/2026/01/01/00/first.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.Ensure(ctx, "db", "unknown", fields("time", tTsTZ, "v", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	if r.IsComplete("db", "unknown") {
		t.Fatal("an anchor created without the written key must be incomplete")
	}
	if err := r.Ensure(ctx, "db", "fresh", fields("time", tTsTZ, "w", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	if !r.IsComplete("db", "fresh") {
		t.Fatal("adding a column must not lose completeness")
	}
	// Another process reads the flag from storage.
	other := New(b, nil, Options{Enabled: true, LocalDir: t.TempDir()}, zerolog.Nop())
	if _, ok := other.Resolve(ctx, "db", "fresh"); !ok || !other.IsComplete("db", "fresh") {
		t.Fatal("completeness must be read from the stored anchor")
	}

	// Ingest creating the anchor for a measurement with existing files
	// (written before the registry existed) cannot claim completeness.
	if err := b.Write(ctx, "db/legacy/2025/01/01/00/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, "db/legacy/2026/02/01/00/b.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.EnsureFile(ctx, "db", "legacy", fields("time", tTsTZ, "v", tInt64), nil, "db/legacy/2026/02/01/00/b.parquet"); err != nil {
		t.Fatal(err)
	}
	if r.IsComplete("db", "legacy") {
		t.Fatal("anchor over pre-existing files must be incomplete")
	}

	// Bootstrap: complete only when every file was sampled and mapped.
	describe := func(rows [][2]string) describeFunc {
		return func(context.Context, []string) ([][2]string, error) { return rows, nil }
	}
	for _, k := range []string{"db/small/2026/01/01/00/a.parquet", "db/small/2026/01/02/00/b.parquet"} {
		if err := b.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	r.describe = describe([][2]string{{"time", "TIMESTAMP WITH TIME ZONE"}, {"v", "BIGINT"}})
	r.Resolve(ctx, "db", "small")
	r.runBootstrap(ctx, <-r.queue)
	if !r.IsComplete("db", "small") {
		t.Fatal("a bootstrap that sampled every file must be complete")
	}
	for _, k := range []string{"db/big/2026/01/01/00/a.parquet", "db/big/2026/01/02/00/b.parquet", "db/big/2026/01/03/00/c.parquet"} {
		if err := b.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	r.Resolve(ctx, "db", "big")
	r.runBootstrap(ctx, <-r.queue)
	if r.IsComplete("db", "big") {
		t.Fatal("a capped sample must not be complete")
	}
	if err := b.Write(ctx, "db/odd/2026/01/01/00/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	r.describe = describe([][2]string{{"time", "TIMESTAMP WITH TIME ZONE"}, {"v", "STRUCT(a INTEGER)"}})
	r.Resolve(ctx, "db", "odd")
	r.runBootstrap(ctx, <-r.queue)
	if r.IsComplete("db", "odd") {
		t.Fatal("an unmapped field must leave the anchor incomplete")
	}
}
