package fieldschema

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// KnownTextBinaryColumns answers, from memory, which STRING and BINARY
// fields are BINARY; other fields are absent (#440).
func TestKnownTextBinaryColumns(t *testing.T) {
	r, _, _ := newTestRegistry(t, Options{})
	ctx := context.Background()
	if r.KnownTextBinaryColumns("db", "m") != nil {
		t.Fatal("an unknown measurement must answer nil")
	}
	tBin := arrow.BinaryTypes.Binary
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "geom", tBin, "name", tStr, "v", tFloat), nil); err != nil {
		t.Fatal(err)
	}
	got := r.KnownTextBinaryColumns("db", "m")
	if isBin, ok := got["geom"]; !ok || !isBin {
		t.Errorf("geom = %v,%v, want binary", isBin, ok)
	}
	if isBin, ok := got["name"]; !ok || isBin {
		t.Errorf("name = %v,%v, want string", isBin, ok)
	}
	for _, absent := range []string{"time", "v"} {
		if _, ok := got[absent]; ok {
			t.Errorf("%s is tracked; only string and binary fields are", absent)
		}
	}
}

// A flip that got past the check (a writer that had not loaded the anchor)
// leaves the merged field at no opinion, not at either side: from then on
// the column is mixed and neither kind is refused.
func TestKnownTextBinaryColumnsAfterAFlipLanded(t *testing.T) {
	r, _, _ := newTestRegistry(t, Options{})
	ctx := context.Background()
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "geom", tStr), nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "geom", arrow.BinaryTypes.Binary), nil); err != nil {
		t.Fatal(err)
	}
	if isBin, ok := r.KnownTextBinaryColumns("db", "m")["geom"]; ok {
		t.Fatalf("geom after a landed flip = binary:%v; want no opinion", isBin)
	}
}
