package fieldschema

import (
	"context"
	"strings"
	"testing"
)

// A deployment whose storage root sat under a `key=value` directory recorded
// the inferred column in its field-schema anchors (#1005). Disabling the
// inference stops new ones; it does not remove the ones already written. The
// release note tells operators to delete the measurement's anchor object under
// `_schema/` and rebuild afterwards, and says in so many words that a rebuild
// on its own will not do it. Both halves of that claim are pinned here,
// because an operator following a recovery procedure during an incident has no
// way to discover that it silently did nothing.
func TestPhantomHiveColumnSurvivesRebuildAndNeedsTheAnchorDeleted(t *testing.T) {
	r, b, _ := newTestRegistry(t, Options{Bootstrap: true, BootstrapMaxFiles: 10})
	ctx := context.Background()
	if err := b.Write(ctx, "db/cpu/2026/01/01/00/raw_a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	// What the files actually hold, now that the inference is off.
	r.describe = func(context.Context, []string) ([][2]string, error) {
		return [][2]string{
			{"time", "TIMESTAMP WITH TIME ZONE"},
			{"host", "VARCHAR"},
			{"usage", "DOUBLE"},
		}, nil
	}
	// The anchor as it was written while the inference was live: `tenant`
	// came from the directory name, never from a file.
	if err := r.Ensure(ctx, "db", "cpu", fields("time", tTsTZ, "host", tStr, "usage", tFloat, "tenant", tStr), nil); err != nil {
		t.Fatal(err)
	}
	if got := anchorFieldNames(t, b); got != "time,host,tenant,usage" {
		t.Fatalf("precondition: anchor = %s", got)
	}

	// Rebuild alone: Merge is a union, so the phantom stays.
	rebuild(t, r, ctx)
	if got := anchorFieldNames(t, b); got != "time,host,tenant,usage" {
		t.Fatalf("a rebuild dropped a field — the release note says it never does, so one of the two is now wrong: anchor = %s", got)
	}

	// Delete the anchor object first, then rebuild: gone, including from the
	// registry's warm in-memory copy (publishLocked drops the cached schema
	// when nothing is stored).
	if err := b.Delete(ctx, AnchorKey("db", "cpu")); err != nil {
		t.Fatal(err)
	}
	rebuild(t, r, ctx)
	if got := anchorFieldNames(t, b); got != "time,host,usage" {
		t.Fatalf("delete-then-rebuild did not clear the phantom column — the recovery procedure in the release note does not work: anchor = %s", got)
	}
	fs, ok, err := r.Fields(ctx, "db", "cpu")
	if err != nil || !ok {
		t.Fatalf("Fields ok=%v err=%v", ok, err)
	}
	for _, f := range fs {
		if f.Name == "tenant" {
			t.Fatalf("the registry still serves the phantom column: %v", fs)
		}
	}
}

func rebuild(t *testing.T, r *Registry, ctx context.Context) {
	t.Helper()
	if st := r.Rebuild("db", "cpu"); st != RebuildQueued {
		t.Fatalf("Rebuild = %v, want RebuildQueued", st)
	}
	r.runBootstrap(ctx, <-r.queue)
}

func anchorFieldNames(t *testing.T, b interface {
	Read(context.Context, string) ([]byte, error)
}) string {
	t.Helper()
	data, err := b.Read(context.Background(), AnchorKey("db", "cpu"))
	if err != nil {
		t.Fatalf("read anchor: %v", err)
	}
	s, err := DecodeAnchor(data)
	if err != nil {
		t.Fatalf("decode anchor: %v", err)
	}
	names := make([]string, 0, s.NumFields())
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	return strings.Join(names, ",")
}
