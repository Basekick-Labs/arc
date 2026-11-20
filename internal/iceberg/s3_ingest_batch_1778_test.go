//go:build objectstore

package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1778 on an S3 API: the appends of several tables at once, each with its
// own reads of the store and its own exporter state. Every file ends in its
// table once, nothing falls back, and each append still makes the requests
// of one append (11, s3_requests_1778_test.go).
func TestS3IngestBatch_SeveralTablesAtOnce(t *testing.T) {
	b, tr := s3TraceRig(t, 0)
	ctx := context.Background()
	local := filepath.Join(t.TempDir(), "f.parquet")
	writeArcStyleParquet(t, local, 1_699_920_000_000_000, 20000)
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	const tables, rounds = 6, 4
	exp, _, _ := s3Exporter(t, b)
	exp.SetVersionRetention(120*time.Second, 5*time.Second)
	exp.SetWarehouseAuthoritative(true)
	src := &recordingSource{StorageWalkSource: NewStorageWalkSource(b, "arc", zerolog.Nop()), fail: map[string]bool{}}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Interval: time.Hour, Logger: zerolog.Nop(),
		IngestInterval: time.Millisecond, IngestAppend: true, IngestConcurrency: tables})
	sched.started = time.Now().Add(-time.Second)
	seq := 0
	want := map[string][]string{}
	report := func(m string) {
		seq++
		key := fmt.Sprintf("mydb/%s/2023/11/14/22/%s", m, ingestName(m, time.Now().Add(time.Duration(seq)*time.Microsecond)))
		if err := b.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		want[m] = append(want[m], key)
		sched.RequestIngestAppend("mydb", m, key, int64(len(data)))
	}
	serve := func() {
		time.Sleep(5 * time.Millisecond)
		sched.serviceIngest(ctx, 4*tables)
	}
	name := func(i int) string { return fmt.Sprintf("t%02d", i) }
	// First exports (passes), then one append each so every table's manifest
	// count is known.
	for round := 0; round < 2; round++ {
		for i := 0; i < tables; i++ {
			report(name(i))
		}
		serve()
	}
	listings := func() int {
		if o := src.order(); o != "" {
			return strings.Count(o, ",") + 1
		}
		return 0
	}
	before := listings()
	for round := 0; round < rounds; round++ {
		for i := 0; i < tables; i++ {
			report(name(i))
		}
		tr.take()
		tr.takeData()
		serve()
		reqs, peak := tr.take()
		_, whole := tr.takeData()
		if whole != 0 {
			t.Errorf("round %d: %d GETs asked for a whole data file", round, whole)
		}
		if got := countOf(reqs, "HEAD data-file"); got != 0 {
			t.Errorf("round %d: %d HEADs of data files", round, got)
		}
		if got := countOf(reqs, "GET(range) data-file"); got != tables {
			t.Errorf("round %d: %d data-file reads for %d appends of one file", round, got, tables)
		}
		if got := countOf(reqs, "GET version-hint"); got != tables {
			t.Errorf("round %d: %d hint reads for %d appends", round, got, tables)
		}
		if peak < 2 {
			t.Errorf("round %d: at most %d request at a time: the appends did not overlap", round, peak)
		}
	}
	if listings() != before {
		t.Fatalf("%d listings during the batched appends, want none: an append fell back to a pass", listings()-before)
	}
	for i := 0; i < tables; i++ {
		m := name(i)
		ictx := exp.withAWS(ctx)
		lt, err := exp.catalog.LoadTable(ictx, exp.tableIdent("mydb", m))
		if err != nil {
			t.Fatal(err)
		}
		tasks, err := lt.Scan().PlanFiles(ictx)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]int{}
		for _, task := range tasks {
			seen[task.File.FilePath()]++
		}
		if len(tasks) != len(want[m]) || len(seen) != len(want[m]) {
			t.Fatalf("table %s: %d entries for %d files, want %d each", m, len(tasks), len(seen), len(want[m]))
		}
		for _, key := range want[m] {
			found := false
			for p := range seen {
				if strings.HasSuffix(p, "/"+key) {
					found = true
				}
			}
			if !found {
				t.Fatalf("table %s lacks %s", m, key)
			}
		}
	}
}
