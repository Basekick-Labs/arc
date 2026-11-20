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

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// requestRig is a table on the traced S3 server, settled so that it takes
// appends, with data files larger than the footer window (so "the whole
// file" and "its tail" are different requests) and the exporter set up as
// every production node is.
type requestRig struct {
	t     *testing.T
	b     *storage.S3Backend
	tr    *s3Trace
	exp   *Exporter
	src   *recordingSource
	sched *Scheduler
	data  []byte
	seq   int
}

const requestDir = "mydb/cpu/2023/11/14/22/"

func newRequestRig(t *testing.T) *requestRig {
	t.Helper()
	b, tr := s3TraceRig(t, 0)
	local := filepath.Join(t.TempDir(), "f.parquet")
	writeArcStyleParquet(t, local, 1_699_920_000_000_000, 20000) // from 00:00 UTC: the rows stay within one day
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) <= 2*footerWindow {
		t.Fatalf("test file is %d bytes; it must be well over the %d-byte footer window", len(data), footerWindow)
	}
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if err := b.Write(ctx, fmt.Sprintf("%sseed_%04d.parquet", requestDir, i), data); err != nil {
			t.Fatal(err)
		}
	}
	exp, _, _ := s3Exporter(t, b)
	exp.SetVersionRetention(120*time.Second, 5*time.Second)
	exp.SetWarehouseAuthoritative(true)
	src := &recordingSource{StorageWalkSource: NewStorageWalkSource(b, "arc", zerolog.Nop()), fail: map[string]bool{}}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Interval: time.Hour, Logger: zerolog.Nop(),
		IngestInterval: time.Millisecond, IngestAppend: true})
	sched.started = time.Now().Add(-time.Second)
	r := &requestRig{t: t, b: b, tr: tr, exp: exp, src: src, sched: sched, data: data}
	// The first export is a pass (it creates the table); the one after it is
	// the first append, which counts the table's manifests once.
	r.report(r.newFile())
	r.serve()
	r.report(r.newFile())
	r.serve()
	if got := len(liveFiles(t, exp)); got != 8 {
		t.Fatalf("%d files in the table after the setup, want 8", got)
	}
	return r
}

func (r *requestRig) newFile() string {
	r.t.Helper()
	r.seq++
	key := requestDir + ingestName("cpu", time.Now().Add(time.Duration(r.seq)*time.Microsecond))
	if err := r.b.Write(context.Background(), key, r.data); err != nil {
		r.t.Fatal(err)
	}
	return key
}

func (r *requestRig) report(keys ...string) {
	for _, k := range keys {
		r.sched.RequestIngestAppend("mydb", "cpu", k, int64(len(r.data)))
	}
}

func (r *requestRig) serve() {
	time.Sleep(5 * time.Millisecond)
	r.sched.serviceIngest(context.Background(), ingestBatch)
}

func (r *requestRig) listings() int {
	if o := r.src.order(); o != "" {
		return strings.Count(o, ",") + 1
	}
	return 0
}

// traced runs fn and returns the requests it made.
func (r *requestRig) traced(fn func()) (reqs []string, maxDataBytes int64, wholeGets int) {
	r.tr.take()
	r.tr.takeData()
	fn()
	reqs, _ = r.tr.take()
	maxDataBytes, wholeGets = r.tr.takeData()
	return reqs, maxDataBytes, wholeGets
}

// #1778: what an append of one file asks the store. Before: 17 requests, among
// them a HEAD of the file, its footer twice, a GET of the whole object, the
// hint twice and the manifest list twice.
func TestS3Append_RequestsForOneNewFile(t *testing.T) {
	r := newRequestRig(t)
	before := r.listings()
	key := r.newFile()
	reqs, maxData, whole := r.traced(func() { r.report(key); r.serve() })
	t.Logf("%d requests: %s", len(reqs), summarize(reqs))
	if r.listings() != before {
		t.Fatal("the export was a pass, not an append")
	}
	if got := len(liveFiles(t, r.exp)); got != 9 {
		t.Fatalf("%d files in the table, want 9", got)
	}
	for kind, want := range map[string]int{
		"HEAD data-file":         0, // the size is the flush's
		"GET data-file":          0, // never the whole object
		"GET(range) data-file":   1, // its tail, once, for the schema and for iceberg-go
		"GET version-hint":       1, // the adopt check's read serves the pass start
		"GET manifest-list":      1, // iceberg-go's own; the count is kept between appends
		"GET manifest":           0,
		"GET LIST":               0,
		"PUT manifest":           1,
		"PUT manifest-list":      1,
		"PUT metadata.json":      1,
		"PUT v<N>.metadata.json": 1,
		"PUT version-hint":       1,
	} {
		if got := countOf(reqs, kind); got != want {
			t.Errorf("%d × %q, want %d", got, kind, want)
		}
	}
	if len(reqs) > 11 {
		t.Errorf("%d requests for an append of one file, want at most 11", len(reqs))
	}
	if whole != 0 {
		t.Errorf("%d GETs asked for a whole data file", whole)
	}
	if maxData > footerWindow {
		t.Errorf("a data-file read answered %d bytes of a %d-byte file, want at most the %d-byte footer window", maxData, len(r.data), footerWindow)
	}
	if got := tableRows(t, r.exp); got != 9*20000 {
		t.Errorf("%d rows through the S3 FileIO, want %d", got, 9*20000)
	}
}

// Each new file costs one request, whether an append or a pass adds it.
func TestS3Export_OneRequestPerNewFile(t *testing.T) {
	r := newRequestRig(t)
	const n = 7
	keys := make([]string, n)
	for i := range keys {
		keys[i] = r.newFile()
	}
	before := r.listings()
	reqs, maxData, whole := r.traced(func() { r.report(keys...); r.serve() })
	if r.listings() != before {
		t.Fatal("the export was a pass, not an append")
	}
	if got := countOf(reqs, "GET(range) data-file") + countOf(reqs, "GET data-file") + countOf(reqs, "HEAD data-file"); got != n {
		t.Errorf("an append of %d files made %d data-file requests (%s), want %d", n, got, summarize(reqs), n)
	}
	if whole != 0 || maxData > footerWindow {
		t.Errorf("append: %d whole-object GETs, largest answer %d bytes", whole, maxData)
	}
	// The same through a reconcile pass, which finds its files by listing.
	for i := range keys {
		keys[i] = r.newFile()
	}
	reqs, maxData, whole = r.traced(func() {
		if _, err := r.sched.reconcileOneRecovered(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"); err != nil {
			t.Fatal(err)
		}
	})
	if got := countOf(reqs, "GET(range) data-file") + countOf(reqs, "GET data-file") + countOf(reqs, "HEAD data-file"); got != n {
		t.Errorf("a pass adding %d files made %d data-file requests (%s), want %d", n, got, summarize(reqs), n)
	}
	if whole != 0 || maxData > footerWindow {
		t.Errorf("pass: %d whole-object GETs, largest answer %d bytes", whole, maxData)
	}
	if got := countOf(reqs, "GET version-hint"); got != 1 {
		t.Errorf("a pass read the hint %d times, want 1", got)
	}
	if got := len(liveFiles(t, r.exp)); got != 8+2*n {
		t.Fatalf("%d files in the table, want %d", got, 8+2*n)
	}
}

// A reported file that left storage before the export is left out and the
// rest are appended: no HEAD told the export it was gone, its footer read did.
func TestS3Append_AReportedFileThatIsGoneIsLeftOut(t *testing.T) {
	r := newRequestRig(t)
	kept, gone := r.newFile(), r.newFile()
	if err := r.b.Delete(context.Background(), gone); err != nil {
		t.Fatal(err)
	}
	before := r.listings()
	r.report(kept, gone)
	r.serve()
	if r.listings() != before {
		t.Fatal("a file that was gone made the export a pass")
	}
	live := liveFiles(t, r.exp)
	if len(live) != 9 {
		t.Fatalf("%d files in the table, want 9", len(live))
	}
	for p := range live {
		if strings.HasSuffix(p, "/"+gone) {
			t.Fatalf("the file that is gone is in the table: %s", p)
		}
	}
}

// A reported size that is not the stored object's: the footer read refuses
// it, the export runs a pass, and the table holds the file once.
func TestS3Append_AWrongReportedSizeRunsAPass(t *testing.T) {
	r := newRequestRig(t)
	key := r.newFile()
	before := r.listings()
	r.sched.RequestIngestAppend("mydb", "cpu", key, int64(len(r.data))-100)
	r.serve()
	if r.listings() != before+1 {
		t.Fatalf("%d listings after a report with the wrong size, want %d: the file was appended on a size the store does not have", r.listings(), before+1)
	}
	live := liveFiles(t, r.exp)
	if len(live) != 9 {
		t.Fatalf("%d files in the table, want 9", len(live))
	}
	for p, size := range live {
		if strings.HasSuffix(p, "/"+key) && size != int64(len(r.data)) {
			t.Fatalf("the table records %d bytes for the file, the store has %d", size, len(r.data))
		}
	}
	// A report with no size asks the store, and appends.
	key = r.newFile()
	before = r.listings()
	var reqs []string
	reqs, _, _ = r.traced(func() { r.sched.RequestIngestAppend("mydb", "cpu", key, 0); r.serve() })
	if r.listings() != before {
		t.Fatal("a report without a size made the export a pass")
	}
	if got := countOf(reqs, "HEAD data-file"); got != 1 {
		t.Errorf("%d HEADs for a file reported without a size, want 1", got)
	}
	if got := len(liveFiles(t, r.exp)); got != 10 {
		t.Fatalf("%d files in the table, want 10", got)
	}
}
