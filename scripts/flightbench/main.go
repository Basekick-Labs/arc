// Command flightbench measures what Arc's Flight SQL listener costs over its
// HTTP Arrow endpoint, and how much of that is the internal bridge
// (internal/api/flight_sql.go: Flight SQL calls /api/v1/query/arrow over a
// loopback HTTP hop, decodes the Arrow stream and re-encodes it as Flight).
//
// Arms, each run against the same server and the same SQL:
//
//	http    POST /api/v1/query/arrow, read every batch and the trailers.
//	flight  Flight SQL GetFlightInfo + DoGet, through the bridge.
//	direct  Flight SQL with the x-arc-bench-direct call header: the server
//	        serves the statement from DuckDB in-process, without the bridge.
//	        Needs a server built with -tags=duckdb_arrow,flightbench; that arm
//	        skips RBAC, governance and audit and is never in a release build.
//	        direct − flight is the most any bridge change can save.
//	floor   Flight SQL GetTableTypes + DoGet: two RPCs, no query, no bridge.
//	        The per-statement cost of the protocol alone.
//
// Repeat an arm in -arms (http,http) for an A/A noise floor; the order rotates
// every round. Per run it reports req/s, latency, rows/bytes/batches per
// request (a parity check across arms), and, given -pid and -pprof, the
// server's CPU, allocated bytes and GC cycles per request.
//
// Server side, for allocation figures: ARC_DEBUG_PPROF=1 (localhost:6060).
//
//	go build -tags=duckdb_arrow,flightbench -o arc-bench ./cmd/arc
//	ARC_FLIGHT_SQL_ENABLED=true ARC_DEBUG_PPROF=1 ./arc-bench
//	go run ./scripts/flightbench seed -http http://127.0.0.1:8000 -token $ADMIN -rows 10000000
//	go run ./scripts/flightbench run -token $TOKEN -pid $(pgrep -f arc-bench) \
//	    -arms http,flight,direct,floor,http,flight -c 1,8 -dur 15s -rounds 5
//
// Run the client on another machine, or pin it to other cores, for figures
// meant for publication: on one machine the client competes with the server.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: flightbench seed|run [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "seed":
		seed(os.Args[2:])
	case "run":
		run(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "usage: flightbench seed|run [flags]")
		os.Exit(2)
	}
}

// ---------------------------------------------------------------- seed

func seed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	httpURL := fs.String("http", "http://127.0.0.1:8000", "Arc HTTP base URL")
	token := fs.String("token", "", "token with write permission")
	db := fs.String("db", "bench", "database")
	measurement := fs.String("m", "m", "measurement")
	rows := fs.Int("rows", 10_000_000, "rows to write")
	hours := fs.Int("hours", 24, "hours the rows spread over (one partition each)")
	_ = fs.Parse(args)

	client := &http.Client{Timeout: 5 * time.Minute}
	start := time.Unix(1_760_000_000, 0).UTC()
	perHour := (*rows + *hours - 1) / *hours
	var buf bytes.Buffer
	flush := func() {
		if buf.Len() == 0 {
			return
		}
		req, _ := http.NewRequest(http.MethodPost, *httpURL+"/api/v1/write/line-protocol", bytes.NewReader(buf.Bytes()))
		req.Header.Set("Authorization", "Bearer "+*token)
		req.Header.Set("x-arc-database", *db)
		resp, err := client.Do(req)
		if err != nil {
			fatal("write: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			fatal("write: HTTP %d", resp.StatusCode)
		}
		buf.Reset()
	}
	for i := 0; i < *rows; i++ {
		ts := start.Add(time.Duration(i/perHour)*time.Hour + time.Duration(i%perHour)*time.Millisecond)
		fmt.Fprintf(&buf, "%s,host=h%d v=%di,f=%g %d\n", *measurement, i%16, i, float64(i)*0.5, ts.UnixNano())
		if buf.Len() > 8<<20 {
			flush()
		}
	}
	flush()
	req, _ := http.NewRequest(http.MethodPost, *httpURL+"/api/v1/write/line-protocol/flush", nil)
	req.Header.Set("Authorization", "Bearer "+*token)
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
	}
	fmt.Printf("wrote %d rows to %s.%s over %d hours\n", *rows, *db, *measurement, *hours)
}

// ---------------------------------------------------------------- run

type workload struct{ name, sql string }

type sample struct {
	latency time.Duration
	rows    int64
	bytes   int64
	batches int64
	err     error
}

type result struct {
	arm, workload string
	conc          int
	round         int
	reqs          int
	errs          int
	firstErr      error
	elapsed       time.Duration
	p50, p99      time.Duration
	rowsPerReq    float64
	bytesPerReq   float64
	batchesPerReq float64
	cpuMsPerReq   float64 // -1 when unknown
	allocMBPerReq float64 // -1 when unknown
	gcPerKReq     float64 // -1 when unknown
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	httpURL := fs.String("http", "http://127.0.0.1:8000", "Arc HTTP base URL")
	flightAddr := fs.String("flight", "127.0.0.1:8081", "Arc Flight SQL address")
	token := fs.String("token", "", "token with read permission")
	db := fs.String("db", "bench", "database")
	measurement := fs.String("m", "m", "measurement")
	armsFlag := fs.String("arms", "http,flight,direct,floor,http,flight", "arms in order; repeat one for an A/A")
	workFlag := fs.String("workloads", "agg,scan100k,scan1m,scan10m", "workloads")
	concFlag := fs.String("c", "1,8", "concurrency levels")
	flightConns := fs.Int("flight-conns", 1, "gRPC connections the Flight arms spread requests over")
	dur := fs.Duration("dur", 15*time.Second, "measured duration per run")
	warmup := fs.Duration("warmup", 2*time.Second, "unmeasured warmup per run")
	rounds := fs.Int("rounds", 5, "rounds; arm order rotates every round")
	pid := fs.Int("pid", 0, "server PID, for CPU per request")
	pprofURL := fs.String("pprof", "http://127.0.0.1:6060", "server pprof base URL (ARC_DEBUG_PPROF=1), for allocations; empty to skip")
	_ = fs.Parse(args)

	table := fmt.Sprintf("%q.%q", *db, *measurement)
	all := map[string]workload{
		"agg":      {"agg", "SELECT count(*) AS n, max(f) AS mf, min(v) AS mv FROM " + table},
		"scan100k": {"scan100k", "SELECT time, host, v, f FROM " + table + " LIMIT 100000"},
		"scan1m":   {"scan1m", "SELECT time, host, v, f FROM " + table + " LIMIT 1000000"},
		"scan10m":  {"scan10m", "SELECT time, host, v, f FROM " + table + " LIMIT 10000000"},
	}
	var works []workload
	for _, w := range splitList(*workFlag) {
		wl, ok := all[w]
		if !ok {
			fatal("unknown workload %q", w)
		}
		works = append(works, wl)
	}
	arms := splitList(*armsFlag)
	var concs []int
	for _, c := range splitList(*concFlag) {
		n, err := strconv.Atoi(c)
		if err != nil || n < 1 {
			fatal("bad concurrency %q", c)
		}
		concs = append(concs, n)
	}

	h := newHTTPArm(*httpURL, *token, maxInt(concs))
	f := newFlightArm(*flightAddr, *token, *flightConns)
	defer f.close()

	fmt.Printf("# flightbench %s  arms=%s  dur=%s rounds=%d flight-conns=%d\n", time.Now().UTC().Format(time.RFC3339), *armsFlag, *dur, *rounds, *flightConns)
	fmt.Println("round\tconc\tworkload\tarm\treq/s\tp50_ms\tp99_ms\trows/req\tKB/req\tbatches/req\tsrv_cpu_ms/req\tsrv_alloc_MB/req\tgc/1k_req\terrors")
	var results []result
	for _, w := range works {
		for _, c := range concs {
			for r := 0; r < *rounds; r++ {
				for i := range arms {
					arm := arms[(i+r)%len(arms)]
					do := armFunc(arm, h, f, w.sql)
					res := measure(do, c, *warmup, *dur, *pid, *pprofURL)
					res.arm, res.workload, res.conc, res.round = arm, w.name, c, r
					results = append(results, res)
					printResult(res)
				}
			}
		}
	}
	summarize(results)
}

func armFunc(arm string, h *httpArm, f *flightArm, sql string) func(context.Context) sample {
	switch arm {
	case "http":
		return func(ctx context.Context) sample { return h.query(ctx, sql) }
	case "flight":
		return func(ctx context.Context) sample { return f.query(ctx, sql, false) }
	case "direct":
		return func(ctx context.Context) sample { return f.query(ctx, sql, true) }
	case "floor":
		return func(ctx context.Context) sample { return f.floor(ctx) }
	}
	fatal("unknown arm %q", arm)
	return nil
}

func measure(do func(context.Context) sample, conc int, warmup, dur time.Duration, pid int, pprofURL string) result {
	drive := func(d time.Duration, collect bool) ([]sample, time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), d+2*time.Minute)
		defer cancel()
		deadline := time.Now().Add(d)
		var mu sync.Mutex
		var out []sample
		var wg sync.WaitGroup
		start := time.Now()
		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					s := do(ctx)
					if collect {
						mu.Lock()
						out = append(out, s)
						mu.Unlock()
					}
				}
			}()
		}
		wg.Wait()
		return out, time.Since(start)
	}
	if warmup > 0 {
		drive(warmup, false)
	}
	cpu0 := processCPU(pid)
	mem0 := serverMem(pprofURL)
	samples, elapsed := drive(dur, true)
	cpu1 := processCPU(pid)
	mem1 := serverMem(pprofURL)

	res := result{reqs: len(samples), elapsed: elapsed, cpuMsPerReq: -1, allocMBPerReq: -1, gcPerKReq: -1}
	var lats []time.Duration
	var rows, byts, batches int64
	ok := 0
	for _, s := range samples {
		if s.err != nil {
			res.errs++
			if res.firstErr == nil {
				res.firstErr = s.err
			}
			continue
		}
		ok++
		lats = append(lats, s.latency)
		rows += s.rows
		byts += s.bytes
		batches += s.batches
	}
	if ok > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		res.p50 = lats[len(lats)/2]
		res.p99 = lats[min(len(lats)-1, len(lats)*99/100)]
		res.rowsPerReq = float64(rows) / float64(ok)
		res.bytesPerReq = float64(byts) / float64(ok)
		res.batchesPerReq = float64(batches) / float64(ok)
	}
	if res.reqs > 0 {
		if cpu0 >= 0 && cpu1 >= 0 {
			res.cpuMsPerReq = (cpu1 - cpu0) * 1000 / float64(res.reqs)
		}
		if mem0.ok && mem1.ok {
			res.allocMBPerReq = float64(mem1.totalAlloc-mem0.totalAlloc) / (1 << 20) / float64(res.reqs)
			res.gcPerKReq = float64(mem1.numGC-mem0.numGC) * 1000 / float64(res.reqs)
		}
	}
	return res
}

func printResult(r result) {
	errText := ""
	if r.errs > 0 {
		errText = fmt.Sprintf("%d (%v)", r.errs, r.firstErr)
	}
	fmt.Printf("%d\t%d\t%s\t%s\t%.1f\t%.2f\t%.2f\t%.0f\t%.1f\t%.1f\t%s\t%s\t%s\t%s\n",
		r.round, r.conc, r.workload, r.arm,
		float64(r.reqs)/r.elapsed.Seconds(), ms(r.p50), ms(r.p99),
		r.rowsPerReq, r.bytesPerReq/1024, r.batchesPerReq,
		optional(r.cpuMsPerReq, "%.2f"), optional(r.allocMBPerReq, "%.2f"), optional(r.gcPerKReq, "%.1f"), errText)
}

// summarize prints the median of every figure per (workload, concurrency,
// arm), the row/byte parity check, and each arm's distance from http.
func summarize(results []result) {
	type key struct {
		w   string
		c   int
		arm string
	}
	groups := map[key][]result{}
	var keys []key
	for _, r := range results {
		k := key{r.workload, r.conc, r.arm}
		if _, seen := groups[k]; !seen {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], r)
	}
	fmt.Println("\n# medians across rounds")
	fmt.Println("workload\tconc\tarm\treq/s\tp50_ms\tsrv_cpu_ms/req\tsrv_alloc_MB/req\trows/req\tKB/req\tvs_http_cpu")
	httpCPU := map[[2]interface{}]float64{}
	for _, k := range keys {
		if k.arm == "http" {
			httpCPU[[2]interface{}{k.w, k.c}] = median(groups[k], func(r result) float64 { return r.cpuMsPerReq })
		}
	}
	for _, k := range keys {
		g := groups[k]
		cpu := median(g, func(r result) float64 { return r.cpuMsPerReq })
		vs := ""
		if base, ok := httpCPU[[2]interface{}{k.w, k.c}]; ok && base > 0 && cpu >= 0 {
			vs = fmt.Sprintf("%+.1f%%", (cpu-base)/base*100)
		}
		fmt.Printf("%s\t%d\t%s\t%.1f\t%.2f\t%s\t%s\t%.0f\t%.1f\t%s\n", k.w, k.c, k.arm,
			median(g, func(r result) float64 { return float64(r.reqs) / r.elapsed.Seconds() }),
			median(g, func(r result) float64 { return ms(r.p50) }),
			optional(cpu, "%.2f"),
			optional(median(g, func(r result) float64 { return r.allocMBPerReq }), "%.2f"),
			median(g, func(r result) float64 { return r.rowsPerReq }),
			median(g, func(r result) float64 { return r.bytesPerReq / 1024 }), vs)
	}
	// Parity: the query arms must return the same rows.
	for _, k := range keys {
		if k.arm != "flight" && k.arm != "direct" {
			continue
		}
		base, ok := groups[key{k.w, k.c, "http"}]
		if !ok {
			continue
		}
		a := median(groups[k], func(r result) float64 { return r.rowsPerReq })
		b := median(base, func(r result) float64 { return r.rowsPerReq })
		if a != b {
			fmt.Printf("# PARITY: %s c=%d %s returned %.0f rows/req, http %.0f\n", k.w, k.c, k.arm, a, b)
		}
	}
}

// ---------------------------------------------------------------- arms

type httpArm struct {
	url, token string
	client     *http.Client
}

func newHTTPArm(base, token string, conc int) *httpArm {
	// One idle connection per worker, so concurrency does not churn
	// connections (Go's default keeps 2 per host).
	tr := &http.Transport{MaxIdleConnsPerHost: conc + 1, MaxConnsPerHost: 0, DisableCompression: true}
	return &httpArm{url: base + "/api/v1/query/arrow", token: token, client: &http.Client{Transport: tr}}
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (h *httpArm) query(ctx context.Context, sql string) sample {
	start := time.Now()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := h.client.Do(req)
	if err != nil {
		return sample{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return sample{err: fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))}
	}
	cr := &countingReader{r: resp.Body}
	rdr, err := ipc.NewReader(cr)
	if err != nil {
		return sample{err: err}
	}
	var rows, batches int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
		batches++
	}
	err = rdr.Err()
	rdr.Release()
	if err != nil {
		return sample{err: err}
	}
	// Finish the body so the trailers arrive, as the Flight bridge does.
	_, _ = io.Copy(io.Discard, cr)
	if t := resp.Trailer.Get("Arc-Stream-Truncated"); t != "" {
		return sample{err: errors.New("truncated: " + t)}
	}
	return sample{latency: time.Since(start), rows: rows, bytes: cr.n, batches: batches}
}

type flightArm struct {
	clients []*flightsql.Client
	next    atomic.Uint64
	counter *bytesCounter
	token   string
}

// bytesCounter counts received gRPC payload bytes per goroutine-free total;
// per-request bytes come from the difference around one request, so it is
// exact at concurrency 1 and an average above it.
type bytesCounter struct{ n atomic.Int64 }

func (b *bytesCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (b *bytesCounter) HandleRPC(_ context.Context, s stats.RPCStats) {
	if in, ok := s.(*stats.InPayload); ok {
		b.n.Add(int64(in.WireLength))
	}
}
func (b *bytesCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (b *bytesCounter) HandleConn(context.Context, stats.ConnStats) {}

func newFlightArm(addr, token string, conns int) *flightArm {
	f := &flightArm{counter: &bytesCounter{}, token: token}
	for i := 0; i < max(1, conns); i++ {
		c, err := flightsql.NewClient(addr, nil, nil,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(f.counter))
		if err != nil {
			fatal("flight client: %v", err)
		}
		f.clients = append(f.clients, c)
	}
	return f
}

func (f *flightArm) close() {
	for _, c := range f.clients {
		_ = c.Client.Close()
	}
}

func (f *flightArm) pick() *flightsql.Client {
	return f.clients[int(f.next.Add(1))%len(f.clients)]
}

func (f *flightArm) ctx(ctx context.Context, direct bool) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+f.token)
	if direct {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-arc-bench-direct", "1")
	}
	return ctx
}

func (f *flightArm) query(ctx context.Context, sql string, direct bool) sample {
	start := time.Now()
	before := f.counter.n.Load()
	c := f.pick()
	ctx = f.ctx(ctx, direct)
	info, err := c.Execute(ctx, sql)
	if err != nil {
		return sample{err: err}
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return sample{err: err}
	}
	var rows, batches int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
		batches++
	}
	err = rdr.Err()
	rdr.Release()
	if err != nil {
		return sample{err: err}
	}
	return sample{latency: time.Since(start), rows: rows, bytes: f.counter.n.Load() - before, batches: batches}
}

func (f *flightArm) floor(ctx context.Context) sample {
	start := time.Now()
	c := f.pick()
	ctx = f.ctx(ctx, false)
	info, err := c.GetTableTypes(ctx)
	if err != nil {
		return sample{err: err}
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return sample{err: err}
	}
	var rows, batches int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
		batches++
	}
	err = rdr.Err()
	rdr.Release()
	if err != nil {
		return sample{err: err}
	}
	return sample{latency: time.Since(start), rows: rows, batches: batches}
}

// ---------------------------------------------------------------- server stats

// processCPU returns the process's user+system CPU seconds, -1 if unknown.
// Linux reads /proc (10 ms ticks); elsewhere ps (10 ms on macOS).
func processCPU(pid int) float64 {
	if pid <= 0 {
		return -1
	}
	if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		// Fields after the ")" of the command name; utime and stime are 14 and 15.
		s := string(b)
		if i := strings.LastIndexByte(s, ')'); i >= 0 {
			f := strings.Fields(s[i+1:])
			if len(f) > 13 {
				u, _ := strconv.ParseFloat(f[11], 64)
				st, _ := strconv.ParseFloat(f[12], 64)
				return (u + st) / 100 // USER_HZ is 100 on every Linux Arc ships for
			}
		}
	}
	out, err := exec.Command("ps", "-o", "time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return -1
	}
	return parsePSTime(strings.TrimSpace(string(out)))
}

// parsePSTime parses [[dd-]hh:]mm:ss[.ss].
func parsePSTime(s string) float64 {
	days := 0.0
	if i := strings.IndexByte(s, '-'); i >= 0 {
		d, _ := strconv.ParseFloat(s[:i], 64)
		days, s = d, s[i+1:]
	}
	parts := strings.Split(s, ":")
	total := 0.0
	for _, p := range parts {
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return -1
		}
		total = total*60 + v
	}
	return days*86400 + total
}

type memStats struct {
	ok                bool
	totalAlloc, numGC uint64
}

// serverMem reads TotalAlloc and NumGC from the runtime.MemStats section of
// the server's heap profile text (pprof heap?debug=1).
func serverMem(base string) memStats {
	if base == "" {
		return memStats{}
	}
	resp, err := http.Get(base + "/debug/pprof/heap?debug=1")
	if err != nil {
		return memStats{}
	}
	defer resp.Body.Close()
	var m memStats
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "# TotalAlloc = "); ok {
			m.totalAlloc, _ = strconv.ParseUint(v, 10, 64)
			m.ok = true
		} else if v, ok := strings.CutPrefix(line, "# NumGC = "); ok {
			m.numGC, _ = strconv.ParseUint(v, 10, 64)
		}
	}
	return m
}

// ---------------------------------------------------------------- helpers

func median(rs []result, f func(result) float64) float64 {
	var v []float64
	for _, r := range rs {
		if x := f(r); x >= 0 {
			v = append(v, x)
		}
	}
	if len(v) == 0 {
		return -1
	}
	sort.Float64s(v)
	return v[len(v)/2]
}

func optional(v float64, format string) string {
	if v < 0 {
		return "-"
	}
	return fmt.Sprintf(format, v)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func maxInt(v []int) int {
	m := 1
	for _, x := range v {
		m = max(m, x)
	}
	return m
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "flightbench: "+format+"\n", args...)
	os.Exit(1)
}
