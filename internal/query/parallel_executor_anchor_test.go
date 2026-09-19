package query

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestBuildPartitionQueryListsAnchorFirst(t *testing.T) {
	e := NewParallelExecutor(nil, nil, zerolog.Nop())
	got := e.buildPartitionQuery("SELECT x FROM {PARTITION_PATH} WHERE 1=1", "/data/db/m/2026/01/01/00/*.parquet", "union_by_name=true", "/tmp/schema/abc.parquet")
	want := "SELECT x FROM read_parquet(['/tmp/schema/abc.parquet', '/data/db/m/2026/01/01/00/*.parquet'], union_by_name=true) WHERE 1=1"
	if got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	got = e.buildPartitionQuery("SELECT x FROM {PARTITION_PATH}", "/data/p/*.parquet", "union_by_name=true", "")
	if strings.Contains(got, "[") {
		t.Fatalf("no anchor must keep the bare path: %s", got)
	}
}
