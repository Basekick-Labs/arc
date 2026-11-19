package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// tracker holds, per marker, when its write was acknowledged and the two
// probes that bracket the moment it became visible.
type tracker struct {
	mu      sync.Mutex
	ack     map[int64]time.Time
	hit     map[int64]bracket
	early   map[int64]bracket // seen by a probe before its ack was recorded
	waiting int
}

// bracket is what the probes know about when a marker became visible: after
// the last probe that missed it was sent, and before the first that returned
// it was received.
type bracket struct{ lastMissSent, firstHitRecv time.Time }

func newTracker() *tracker {
	return &tracker{ack: map[int64]time.Time{}, hit: map[int64]bracket{}, early: map[int64]bracket{}}
}

// acked records a write's acknowledgement.
func (t *tracker) acked(marker int64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ack[marker] = at
	if b, ok := t.early[marker]; ok {
		// A probe returned the row before the writer goroutine got here.
		delete(t.early, marker)
		t.hit[marker] = b
		return
	}
	t.waiting++
}

// seen records the markers one probe returned. lastMissSent is when the
// previous successful probe of the same measurement was sent (zero for the
// first one).
func (t *tracker) seen(markers []int64, lastMissSent, recv time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range markers {
		if _, done := t.hit[m]; done {
			continue
		}
		b := bracket{lastMissSent: lastMissSent, firstHitRecv: recv}
		if _, ok := t.ack[m]; !ok {
			if _, dup := t.early[m]; !dup {
				t.early[m] = b
			}
			continue
		}
		t.hit[m] = b
		t.waiting--
	}
}

func (t *tracker) pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.waiting
}

// lags returns, for every visible marker, the upper bound of its lag (ack to
// the first probe that returned it) and the lower bound (ack to the last
// probe sent that missed it), never below zero.
func (t *tracker) lags() (upper, lower []time.Duration, tracked, visible int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for m, ack := range t.ack {
		tracked++
		b, ok := t.hit[m]
		if !ok {
			continue
		}
		visible++
		upper = append(upper, max(b.firstHitRecv.Sub(ack), 0))
		lo := time.Duration(0)
		if !b.lastMissSent.IsZero() {
			lo = max(b.lastMissSent.Sub(ack), 0)
		}
		lower = append(lower, lo)
	}
	return upper, lower, tracked, visible
}

// durations is a goroutine-safe list of durations.
type durations struct {
	mu sync.Mutex
	d  []time.Duration
}

func (d *durations) add(v time.Duration) {
	d.mu.Lock()
	d.d = append(d.d, v)
	d.mu.Unlock()
}

func (d *durations) merge(o *durations) {
	o.mu.Lock()
	defer o.mu.Unlock()
	d.mu.Lock()
	d.d = append(d.d, o.d...)
	d.mu.Unlock()
}

func (d *durations) quantiles() quantiles {
	d.mu.Lock()
	defer d.mu.Unlock()
	return quantilesOf(d.d)
}

type quantiles struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func (q quantiles) String() string {
	if q.N == 0 {
		return "n=0"
	}
	return fmt.Sprintf("n=%d p50=%.1f p95=%.1f p99=%.1f max=%.1f", q.N, q.P50, q.P95, q.P99, q.Max)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// quantilesOf uses the nearest-rank method: the value at rank ceil(p*n).
func quantilesOf(in []time.Duration) quantiles {
	if len(in) == 0 {
		return quantiles{}
	}
	d := append([]time.Duration(nil), in...)
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	rank := func(p float64) float64 {
		i := int(p*float64(len(d))+0.999999999) - 1
		return ms(d[min(max(i, 0), len(d)-1)])
	}
	return quantiles{N: len(d), P50: rank(0.50), P95: rank(0.95), P99: rank(0.99), Max: ms(d[len(d)-1])}
}

// serverMetrics is what the harness reads from Arc's /metrics.
type serverMetrics struct {
	flushes      float64
	publishCount float64
	publishSum   float64
	publishMax   float64
	buckets      []bucket // cumulative, in exposition order
}

type bucket struct {
	le    string
	count float64
}

// serverDelta is the change over a run.
type serverDelta struct {
	Flushes      int64   `json:"files_flushed"`
	PublishCount int64   `json:"publish_delay_files"`
	PublishSumS  float64 `json:"publish_delay_sum_s"`
	// PublishMaxS is the gauge's value after the run. It is the largest
	// since the server started, not since the run did.
	PublishMaxS float64       `json:"publish_delay_max_since_start_s"`
	Buckets     []bucketDelta `json:"publish_delay_buckets,omitempty"`
}

type bucketDelta struct {
	LE    string `json:"le"`
	Files int64  `json:"files"` // in this bucket alone, not cumulative
}

func (d serverDelta) meanMS() float64 {
	if d.PublishCount == 0 {
		return 0
	}
	return d.PublishSumS / float64(d.PublishCount) * 1000
}

func (d serverDelta) bucketLine() string {
	var parts []string
	for _, b := range d.Buckets {
		if b.Files != 0 {
			parts = append(parts, fmt.Sprintf("%s: %d", b.LE, b.Files))
		}
	}
	return strings.Join(parts, "  ")
}

func (after serverMetrics) minus(before serverMetrics) serverDelta {
	d := serverDelta{
		Flushes:      int64(after.flushes - before.flushes),
		PublishCount: int64(after.publishCount - before.publishCount),
		PublishSumS:  after.publishSum - before.publishSum,
		PublishMaxS:  after.publishMax,
	}
	prev := map[string]float64{}
	for _, b := range before.buckets {
		prev[b.le] = b.count
	}
	var below float64
	for _, b := range after.buckets {
		cum := b.count - prev[b.le]
		d.Buckets = append(d.Buckets, bucketDelta{LE: b.le, Files: int64(cum - below)})
		below = cum
	}
	return d
}

func (c *client) scrape(ctx context.Context) (serverMetrics, error) {
	resp, err := c.do(ctx, http.MethodGet, "/metrics", nil, nil)
	if err != nil {
		return serverMetrics{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return serverMetrics{}, fmt.Errorf("/metrics: HTTP %d", resp.StatusCode)
	}
	return parseMetrics(resp.Body)
}

func parseMetrics(r io.Reader) (serverMetrics, error) {
	var m serverMetrics
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		name := line[:sp]
		v, err := strconv.ParseFloat(line[sp+1:], 64)
		if err != nil {
			continue
		}
		switch {
		case name == "arc_buffer_flushes_total":
			m.flushes = v
		case name == "arc_ingest_publish_delay_seconds_count":
			m.publishCount = v
		case name == "arc_ingest_publish_delay_seconds_sum":
			m.publishSum = v
		case name == "arc_ingest_publish_delay_max_seconds":
			m.publishMax = v
		case strings.HasPrefix(name, `arc_ingest_publish_delay_seconds_bucket{le="`):
			le := strings.TrimSuffix(strings.TrimPrefix(name, `arc_ingest_publish_delay_seconds_bucket{le="`), `"}`)
			m.buckets = append(m.buckets, bucket{le: le, count: v})
		}
	}
	return m, sc.Err()
}
