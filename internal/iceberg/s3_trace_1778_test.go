//go:build objectstore

package iceberg

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// s3Trace is a reverse proxy in front of the test S3 server that records
// every request and can hold each one for a fixed time, so a run on one
// machine shows what an export costs with a network in the way (#1778).
type s3Trace struct {
	mu    sync.Mutex
	reqs  []string
	inFly int
	peak  int
	delay time.Duration
	// maxDataBytes is the largest answer to a data-file read, and wholeGets
	// the data-file GETs that asked for the whole object.
	maxDataBytes int64
	wholeGets    int
}

func (tr *s3Trace) take() (reqs []string, peak int) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	reqs, peak = tr.reqs, tr.peak
	tr.reqs, tr.peak = nil, 0
	return reqs, peak
}

// takeData returns and resets what the data-file reads asked for.
func (tr *s3Trace) takeData() (maxBytes int64, wholeGets int) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	maxBytes, wholeGets = tr.maxDataBytes, tr.wholeGets
	tr.maxDataBytes, tr.wholeGets = 0, 0
	return maxBytes, wholeGets
}

func countOf(reqs []string, kind string) int {
	n := 0
	for _, r := range reqs {
		if r == kind {
			n++
		}
	}
	return n
}

// kind names a request by what it is for, not by its key.
func traceKind(r *http.Request) string {
	p := r.URL.Path
	q := r.URL.Query()
	what := "other"
	switch {
	case q.Has("list-type") || (r.Method == http.MethodGet && strings.Count(strings.Trim(p, "/"), "/") == 0):
		what = "LIST"
	case strings.HasSuffix(p, ".parquet"):
		what = "data-file"
	case strings.HasSuffix(p, "version-hint.text"):
		what = "version-hint"
	case strings.Contains(p, "/metadata/v") && strings.HasSuffix(p, ".metadata.json"):
		what = "v<N>.metadata.json"
	case strings.HasSuffix(p, ".metadata.json"):
		what = "metadata.json"
	case strings.Contains(p, "/snap-") && strings.HasSuffix(p, ".avro"):
		what = "manifest-list"
	case strings.HasSuffix(p, ".avro"):
		what = "manifest"
	}
	m := r.Method
	if m == http.MethodGet && r.Header.Get("Range") != "" {
		m = "GET(range)"
	}
	if q.Has("uploads") || q.Has("uploadId") {
		m += "(multipart)"
	}
	return m + " " + what
}

func s3TraceRig(t *testing.T, delay time.Duration) (*storage.S3Backend, *s3Trace) {
	t.Helper()
	bucket := os.Getenv("ARC_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("ARC_TEST_S3_BUCKET not set")
	}
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	tr := &s3Trace{delay: delay}
	rp := httputil.NewSingleHostReverseProxy(target)
	// Keep the Host the client signed: SigV4 covers it.
	director := rp.Director
	rp.Director = func(r *http.Request) { host := r.Host; director(r); r.Host = host }
	// What the data-file reads asked for and got.
	rp.ModifyResponse = func(resp *http.Response) error {
		if strings.HasSuffix(resp.Request.URL.Path, ".parquet") && resp.Request.Method != http.MethodPut {
			tr.mu.Lock()
			if resp.ContentLength > tr.maxDataBytes {
				tr.maxDataBytes = resp.ContentLength
			}
			if resp.Request.Method == http.MethodGet && resp.Request.Header.Get("Range") == "" {
				tr.wholeGets++
			}
			tr.mu.Unlock()
		}
		return nil
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tr.mu.Lock()
		tr.reqs = append(tr.reqs, traceKind(r))
		tr.inFly++
		if tr.inFly > tr.peak {
			tr.peak = tr.inFly
		}
		d := tr.delay
		tr.mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
		rp.ServeHTTP(w, r)
		tr.mu.Lock()
		tr.inFly--
		tr.mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	prefix := fmt.Sprintf("iceberg-1778/%s-%d/", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
	b, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: srv.URL, Prefix: prefix,
		AccessKey: objTestSecret, SecretKey: objTestSecret, PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("S3 backend: %v", err)
	}
	t.Cleanup(func() {
		tr.mu.Lock()
		tr.delay = 0
		tr.mu.Unlock()
		ctx := context.Background()
		keys, _ := b.List(ctx, "")
		for _, k := range keys {
			_ = b.Delete(ctx, k)
		}
		b.Close()
	})
	return b, tr
}

func summarize(reqs []string) string {
	n := map[string]int{}
	for _, r := range reqs {
		n[r]++
	}
	keys := make([]string, 0, len(n))
	for k := range n {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s ×%d; ", k, n[k])
	}
	return sb.String()
}

// TestMeasure_S3RequestsPerExport prints the S3 requests one ingest export
// makes, as an append and as a pass, with one new file and with many, and
// how long each takes with ARC_TEST_S3_DELAY_MS added to every request.
// ARC_TEST_S3_TRACE_TABLES > 1 exports that many tables in a row.
func TestMeasure_S3RequestsPerExport(t *testing.T) {
	if os.Getenv("ARC_TEST_S3_TRACE") == "" {
		t.Skip("ARC_TEST_S3_TRACE not set: a measurement, not a check")
	}
	delayMS, _ := strconv.Atoi(os.Getenv("ARC_TEST_S3_DELAY_MS"))
	b, tr := s3TraceRig(t, 0)
	ctx := context.Background()
	base := int64(1_700_000_000_000_000)
	local := filepath.Join(t.TempDir(), "f.parquet")
	writeArcStyleParquet(t, local, base, traceRows())
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	dir := "mydb/cpu/2023/11/14/22/"
	const seed = 48
	for i := 0; i < seed; i++ {
		if err := b.Write(ctx, fmt.Sprintf("%sseed_%04d.parquet", dir, i), data); err != nil {
			t.Fatal(err)
		}
	}
	exp, _, _ := s3Exporter(t, b)
	exp.SetVersionRetention(120*time.Second, 5*time.Second)
	exp.SetWarehouseAuthoritative(true) // as every production node is (main.go)
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Interval: time.Hour, Logger: zerolog.Nop(),
		IngestInterval: time.Millisecond, IngestAppend: true})
	sched.started = time.Now().Add(-time.Second)
	seq := 0
	files := seed + 1 // in the table once the setup below has run
	newFiles := func(n int) []string {
		keys := make([]string, n)
		for i := range keys {
			seq++
			keys[i] = dir + ingestName("cpu", time.Now().Add(time.Duration(seq)*time.Microsecond))
			if err := b.Write(ctx, keys[i], data); err != nil {
				t.Fatal(err)
			}
		}
		return keys
	}
	setDelay := func(d time.Duration) { tr.mu.Lock(); tr.delay = d; tr.mu.Unlock() }
	serve := func(label string, n int, viaPass bool) {
		keys := newFiles(n)
		tr.take()
		setDelay(time.Duration(delayMS) * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		start := time.Now()
		if viaPass {
			if _, err := sched.reconcileOneRecovered(ctx, Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"); err != nil {
				t.Fatal(err)
			}
		} else {
			for _, k := range keys {
				sched.RequestIngestAppend("mydb", "cpu", k, int64(len(data)))
			}
			time.Sleep(5 * time.Millisecond)
			sched.serviceIngest(ctx, ingestBatch)
		}
		took := time.Since(start)
		setDelay(0)
		// A file left out (rows across a UTC day, say) would make the trace
		// that of an export that committed nothing.
		files += n
		if got := len(liveFiles(t, exp)); got != files {
			t.Fatalf("%s: %d files in the table, want %d", label, got, files)
		}
		reqs, peak := tr.take()
		t.Logf("%-28s %4d requests, %7.0f ms at +%d ms each, most at once %d\n    %s", label, len(reqs), float64(took.Microseconds())/1000, delayMS, peak, summarize(reqs))
		if os.Getenv("ARC_TEST_S3_TRACE_ORDER") != "" {
			t.Logf("    in order: %s", strings.Join(reqs, " | "))
		}
	}
	// Create the table and settle it, so the next request is an append.
	sched.RequestIngestAppend("mydb", "cpu", newFiles(1)[0], 0)
	time.Sleep(5 * time.Millisecond)
	sched.serviceIngest(ctx, ingestBatch)
	if _, err := sched.reconcileOneRecovered(ctx, Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"); err != nil {
		t.Fatal(err)
	}
	serve("append, 1 new file", 1, false)
	serve("append, 1 new file (again)", 1, false)
	serve("append, 19 new files", 19, false)
	serve("pass, 1 new file", 1, true)
	serve("pass, 19 new files", 19, true)
	serve("pass, nothing new", 0, true)
}

func traceRows() int {
	if n, _ := strconv.Atoi(os.Getenv("ARC_TEST_S3_TRACE_ROWS")); n > 0 {
		return n
	}
	return 10
}
