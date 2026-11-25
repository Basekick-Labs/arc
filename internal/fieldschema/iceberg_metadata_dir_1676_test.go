package fieldschema

import (
	"reflect"
	"testing"
)

// #1676: a colocated Iceberg table's metadata directory is not data.
func TestWithoutIcebergMetadataDir(t *testing.T) {
	got := withoutIcebergMetadataDir([]string{"db/cpu/2026/", "db/cpu/metadata/", "metadata"})
	if want := []string{"db/cpu/2026/"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := withoutIcebergMetadataDir(nil); len(got) != 0 {
		t.Fatalf("nil input gave %v", got)
	}
}
