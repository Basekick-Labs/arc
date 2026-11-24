// Command freshness measures how long a row written to Arc takes to show up
// in a query: the time from the write's acknowledgement (HTTP 204) to the
// first query that returns the row.
//
// Two modes:
//
//	steady  writes marker rows at a fixed rate and polls a query for them.
//	        Reports the lag distribution, next to the server's own
//	        arc_ingest_publish_delay_seconds for the same period.
//
//	late    writes ONE row stamped in another hour (late or future data) into
//	        an hour that a query has just found empty, flushes, and times how
//	        long queries keep answering without it (#1757).
//
// Run it against a built binary:
//
//	go run ./scripts/freshness steady -http http://127.0.0.1:8000 -token $TOKEN
//	go run ./scripts/freshness late   -http http://127.0.0.1:8000 -token $ADMIN
//
// See README.md for what each number means and what it does not measure.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	var err error
	switch os.Args[1] {
	case "steady":
		err = steadyCmd(ctx, os.Args[2:], os.Stdout)
	case "late":
		err = lateCmd(ctx, os.Args[2:], os.Stdout)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "freshness:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: freshness steady|late [flags]   (-h after the mode lists its flags)")
}

// common are the flags both modes take.
type common struct {
	baseURL  string
	token    string
	database string
	poll     time.Duration
	timeout  time.Duration
	out      string
	// probeURL: where the late mode's queries go, when not -http (a cluster
	// reader probed while the writer takes the writes, #1763).
	probeURL string
}

func (c *common) register(fs *flag.FlagSet, timeout time.Duration) {
	fs.StringVar(&c.baseURL, "http", "http://127.0.0.1:8000", "Arc base URL")
	fs.StringVar(&c.token, "token", os.Getenv("ARC_TOKEN"), "API token (default $ARC_TOKEN)")
	fs.StringVar(&c.database, "database", "freshness", "database to write to and query")
	fs.DurationVar(&c.poll, "poll", 5*time.Millisecond, "pause between probe queries; with the probe round trip, this is the measurement's resolution")
	fs.DurationVar(&c.timeout, "timeout", timeout, "how long to keep probing for rows that have not shown up")
	fs.StringVar(&c.out, "out", "", "also write the report as JSON to this file")
}

func (c *common) client() *client {
	return &client{
		base:  strings.TrimRight(c.baseURL, "/"),
		token: c.token,
		http: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second},
		},
	}
}

func writeReport(path string, v any) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// timeLiteral is a timestamp in the one form both the query engine and Arc's
// partition pruner read: a quoted literal as a bare conjunct on time.
func timeLiteral(t time.Time) string {
	return "'" + t.UTC().Format("2006-01-02T15:04:05Z") + "'"
}

// ---------------------------------------------------------------- steady

type steadyReport struct {
	Mode           string        `json:"mode"`
	Run            string        `json:"run"`
	Endpoint       string        `json:"endpoint"`
	SQL            string        `json:"sql"`
	Rate           float64       `json:"rate_per_s"`
	Batch          int           `json:"filler_rows_per_request"`
	Measurements   int           `json:"measurements"`
	Probed         int           `json:"measurements_probed"`
	PollMS         float64       `json:"poll_ms"`
	DurationS      float64       `json:"duration_s"`
	Sent           int           `json:"markers_sent"`
	Acked          int           `json:"markers_acked"`
	WriteErrors    int           `json:"write_errors"`
	Skipped        int64         `json:"ticks_skipped"`
	Tracked        int           `json:"markers_on_probed_measurements"`
	Visible        int           `json:"markers_visible"`
	NeverVisible   int           `json:"markers_never_visible"`
	LagMS          quantiles     `json:"lag_ms"`
	LagLowerMS     quantiles     `json:"lag_lower_bound_ms"`
	WriteRTTMS     quantiles     `json:"write_round_trip_ms"`
	ProbeRTTMS     quantiles     `json:"probe_round_trip_ms"`
	Probes         int           `json:"probes"`
	ProbeErrors    int           `json:"probe_errors"`
	FirstProbeErr  string        `json:"first_probe_error,omitempty"`
	Server         *serverDelta  `json:"server,omitempty"`
	ServerScrapeEr string        `json:"server_scrape_error,omitempty"`
	Elapsed        time.Duration `json:"-"`
}

func steadyCmd(ctx context.Context, args []string, w io.Writer) error {
	fs := flag.NewFlagSet("steady", flag.ExitOnError)
	var c common
	c.register(fs, 60*time.Second)
	prefix := fs.String("measurement", "fresh", "measurement name (prefix when -measurements > 1)")
	nMeas := fs.Int("measurements", 1, "spread the rows over this many measurements")
	probeSample := fs.Int("probe-sample", 4, "probe at most this many of the measurements (each is polled on its own); the others are written to but not timed")
	rate := fs.Float64("rate", 20, "write requests per second, one marker row each")
	batch := fs.Int("batch", 0, "extra filler rows in each write request (to reach ingest.max_buffer_size and take the size-triggered flush)")
	duration := fs.Duration("duration", 60*time.Second, "how long to write")
	lookback := fs.Duration("lookback", time.Minute, "the probe's time range starts this long before the run; over an hour makes it span hour partitions, which Arc plans (and caches) differently from a range inside the current hour")
	offset := fs.Duration("time-offset", 0, "stamp rows this far from now (negative: late data)")
	endpoint := fs.String("endpoint", "json", "query endpoint to probe: json, msgpack, arrow, measurement")
	prequery := fs.Bool("prequery", false, "run each probe query once before the first write")
	_ = fs.Parse(args)
	if c.token == "" {
		return fmt.Errorf("-token is required")
	}
	if *rate <= 0 || *nMeas < 1 || *probeSample < 1 || *batch < 0 || c.poll < 0 || *lookback < 0 {
		return fmt.Errorf("-rate must be > 0, -measurements and -probe-sample >= 1, -batch, -poll and -lookback >= 0")
	}
	cl := c.client()
	run := fmt.Sprintf("r%x", time.Now().UnixNano())
	start := time.Now()
	// One fixed range for the whole run, so the SQL text never changes, as a
	// dashboard panel's does not between refreshes. Whether Arc caches the
	// plan depends on the range: one that starts in the current hour (the
	// default lookback) is re-listed on every query, one that spans hour
	// partitions (-lookback over an hour) is the cached shape.
	rangeFrom := start.Add(*offset).Add(-*lookback)
	rangeTo := start.Add(*offset).Add(*duration).Add(c.timeout).Add(time.Minute)

	names := make([]string, *nMeas)
	for i := range names {
		names[i] = *prefix
		if *nMeas > 1 {
			names[i] = fmt.Sprintf("%s_%d", *prefix, i)
		}
	}
	probed := min(*probeSample, *nMeas)
	probes := make([]probe, probed)
	for i := range probes {
		p, err := newProbe(*endpoint, c.database, names[i], run, rangeFrom, rangeTo)
		if err != nil {
			return err
		}
		probes[i] = p
	}

	before, scrapeErr := cl.scrape(ctx)
	if *prequery {
		for _, p := range probes {
			if _, err := p.run(ctx, cl); err != nil {
				return fmt.Errorf("prequery: %w", err)
			}
		}
	}

	tr := newTracker()
	var wg sync.WaitGroup
	probeCtx, stopProbes := context.WithCancel(ctx)
	defer stopProbes()
	stats := make([]*probeStats, probed)
	for i, p := range probes {
		stats[i] = &probeStats{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			probeLoop(probeCtx, cl, p, tr, c.poll, stats[i])
		}()
	}

	// Writer. A tick whose request slot is still busy is skipped and counted,
	// never queued: a stalled server must not turn into a burst afterwards.
	var (
		sent, writeErrs atomic.Int64
		skipped         atomic.Int64
		writeRTT        durations
		slots           = make(chan struct{}, 32)
		writers         sync.WaitGroup
	)
	ticker := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	deadline := time.After(*duration)
	marker := int64(0)
writing:
	for {
		select {
		case <-ctx.Done():
			break writing
		case <-deadline:
			break writing
		case <-ticker.C:
			select {
			case slots <- struct{}{}:
			default:
				skipped.Add(1)
				continue
			}
			marker++
			m, meas := marker, int(marker%int64(*nMeas))
			tracked := meas < probed
			writers.Add(1)
			go func() {
				defer writers.Done()
				defer func() { <-slots }()
				body := lineProtocol(names[meas], run, m, *batch, time.Now().Add(*offset))
				sent.Add(1)
				t0 := time.Now()
				err := cl.write(ctx, c.database, body)
				ack := time.Now()
				if err != nil {
					writeErrs.Add(1)
					return
				}
				writeRTT.add(ack.Sub(t0))
				if tracked {
					tr.acked(m, ack)
				}
			}()
		}
	}
	ticker.Stop()
	writers.Wait()
	wrote := time.Since(start)

	// Keep probing until every tracked marker has shown up, or the timeout.
	waitUntil := time.Now().Add(c.timeout)
	for tr.pending() > 0 && time.Now().Before(waitUntil) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	stopProbes()
	wg.Wait()
	after, scrapeErr2 := cl.scrape(context.WithoutCancel(ctx))

	upper, lower, tracked, visible := tr.lags()
	rep := steadyReport{
		Mode: "steady", Run: run, Endpoint: *endpoint, SQL: probes[0].describe(),
		Rate: *rate, Batch: *batch, Measurements: *nMeas, Probed: probed,
		PollMS: ms(c.poll), DurationS: wrote.Seconds(),
		Sent: int(sent.Load()), Acked: int(sent.Load() - writeErrs.Load()), WriteErrors: int(writeErrs.Load()),
		Skipped: skipped.Load(), Tracked: tracked, Visible: visible, NeverVisible: tracked - visible,
		LagMS: quantilesOf(upper), LagLowerMS: quantilesOf(lower), WriteRTTMS: writeRTT.quantiles(),
	}
	var rtt durations
	for _, s := range stats {
		rtt.merge(&s.rtt)
		rep.Probes += s.n
		rep.ProbeErrors += s.errs
		if rep.FirstProbeErr == "" {
			rep.FirstProbeErr = s.firstErr
		}
	}
	rep.ProbeRTTMS = rtt.quantiles()
	switch {
	case scrapeErr != nil:
		rep.ServerScrapeEr = scrapeErr.Error()
	case scrapeErr2 != nil:
		rep.ServerScrapeEr = scrapeErr2.Error()
	default:
		d := after.minus(before)
		rep.Server = &d
	}
	printSteady(w, rep)
	if err := writeReport(c.out, rep); err != nil {
		return err
	}
	// A run that could not time its rows is a failed run, not a fast one.
	switch {
	case rep.Probes > 0 && rep.ProbeErrors == rep.Probes:
		return fmt.Errorf("every probe failed: %s", rep.FirstProbeErr)
	case rep.NeverVisible > 0:
		return fmt.Errorf("%d of %d rows did not show up within %s", rep.NeverVisible, rep.Tracked, c.timeout)
	}
	return nil
}

func printSteady(w io.Writer, r steadyReport) {
	fmt.Fprintf(w, "steady  run=%s endpoint=%s rate=%.0f/s batch=%d measurements=%d (probed %d) poll=%.0fms wrote for %.1fs\n",
		r.Run, r.Endpoint, r.Rate, r.Batch, r.Measurements, r.Probed, r.PollMS, r.DurationS)
	fmt.Fprintf(w, "probe   %s\n", r.SQL)
	fmt.Fprintf(w, "writes  sent=%d acked=%d errors=%d ticks_skipped=%d   round trip ms %s\n", r.Sent, r.Acked, r.WriteErrors, r.Skipped, r.WriteRTTMS)
	fmt.Fprintf(w, "probes  n=%d errors=%d   round trip ms %s\n", r.Probes, r.ProbeErrors, r.ProbeRTTMS)
	if r.FirstProbeErr != "" {
		fmt.Fprintf(w, "        first probe error: %s\n", r.FirstProbeErr)
	}
	fmt.Fprintf(w, "rows    timed=%d visible=%d never_visible=%d\n", r.Tracked, r.Visible, r.NeverVisible)
	fmt.Fprintf(w, "lag ms  (ack -> first probe that returned the row)  %s\n", r.LagMS)
	fmt.Fprintf(w, "        lower bound (ack -> last probe sent that missed it)  %s\n", r.LagLowerMS)
	switch {
	case r.Server != nil:
		fmt.Fprintf(w, "server  files_flushed=%d  publish_delay: files=%d mean=%.0fms max_since_start=%.0fms\n",
			r.Server.Flushes, r.Server.PublishCount, r.Server.meanMS(), r.Server.PublishMaxS*1000)
		if b := r.Server.bucketLine(); b != "" {
			fmt.Fprintf(w, "        publish_delay buckets (le seconds: files)  %s\n", b)
		}
	default:
		fmt.Fprintf(w, "server  metrics not read: %s\n", r.ServerScrapeEr)
	}
}

// probeStats is one probe loop's own tally; read after the loop has ended.
type probeStats struct {
	rtt      durations
	n, errs  int
	firstErr string
}

// probeLoop polls one measurement until ctx ends.
func probeLoop(ctx context.Context, cl *client, p probe, tr *tracker, poll time.Duration, st *probeStats) {
	var lastMissSent time.Time
	for ctx.Err() == nil {
		sentAt := time.Now()
		markers, err := p.run(ctx, cl)
		recvAt := time.Now()
		if ctx.Err() != nil {
			return
		}
		st.n++
		if err != nil {
			// Counted, never read as lag: a probe that failed says nothing
			// about what was visible.
			st.errs++
			if st.firstErr == "" {
				st.firstErr = err.Error()
			}
		} else {
			st.rtt.add(recvAt.Sub(sentAt))
			tr.seen(markers, lastMissSent, recvAt)
			lastMissSent = sentAt
		}
		if poll > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(poll):
			}
		}
	}
}

// lineProtocol is one write request: the marker row, then filler rows that
// the probe's "marker > 0" leaves out.
func lineProtocol(measurement, run string, marker int64, filler int, at time.Time) []byte {
	ns := at.UnixNano()
	var b strings.Builder
	fmt.Fprintf(&b, "%s,run=%s marker=%di %d\n", measurement, run, marker, ns)
	for i := 0; i < filler; i++ {
		fmt.Fprintf(&b, "%s,run=%s marker=0i,f=%di %d\n", measurement, run, i, ns)
	}
	return []byte(b.String())
}

// ---------------------------------------------------------------- late

type lateTrial struct {
	Measurement string  `json:"measurement"`
	OffsetS     float64 `json:"offset_s"`
	DelayS      float64 `json:"prequery_to_write_s"`
	// Seconds from the flush returning (the file is in storage) to the first
	// query that returned the row. -1: not within the timeout.
	SameSQLS   float64 `json:"same_sql_s"`
	UniqueSQLS float64 `json:"unique_sql_s"`
	// Discard is why the trial says nothing about the caches, when it does not.
	Discard     string `json:"discard,omitempty"`
	QueryErrors int    `json:"query_errors"`
}

type lateReport struct {
	Mode    string      `json:"mode"`
	Run     string      `json:"run"`
	PollMS  float64     `json:"poll_ms"`
	Trials  []lateTrial `json:"trials"`
	SameSQL quantiles   `json:"same_sql_ms"`
	Unique  quantiles   `json:"unique_sql_ms"`
	Missed  int         `json:"trials_not_visible_within_timeout"`
	// Discarded trials are listed and left out of the numbers above.
	Discarded int `json:"trials_discarded"`
}

func lateCmd(ctx context.Context, args []string, w io.Writer) error {
	fs := flag.NewFlagSet("late", flag.ExitOnError)
	var c common
	c.register(fs, 180*time.Second)
	prefix := fs.String("measurement", "fresh_late", "measurement name prefix; each trial uses a new measurement")
	trials := fs.Int("trials", 6, "number of trials")
	offset := fs.Duration("offset", -3*time.Hour, "how far from now the late row is stamped (negative: the past; positive: the future)")
	delays := fs.String("delays", "0s,5s,15s,35s", "time between the query that finds the hour empty and the late write, cycled over the trials")
	fs.StringVar(&c.probeURL, "probe", "", "Arc base URL the queries go to (default: -http); on a cluster, a reader while -http is the writer")
	_ = fs.Parse(args)
	if c.token == "" {
		return fmt.Errorf("-token is required (admin: this mode calls the flush endpoint)")
	}
	if off := *offset; off > -time.Hour && off < time.Hour {
		return fmt.Errorf("-offset must be at least an hour from now, so the row lands in another hour partition")
	}
	var waits []time.Duration
	for _, s := range strings.Split(*delays, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return fmt.Errorf("-delays: %w", err)
		}
		waits = append(waits, d)
	}
	cl := c.client()
	run := fmt.Sprintf("r%x", time.Now().UnixNano())
	rep := lateReport{Mode: "late", Run: run, PollMS: ms(c.poll)}
	var same, unique []time.Duration
	for i := 0; i < *trials && ctx.Err() == nil; i++ {
		tr, err := lateTrialRun(ctx, cl, c, fmt.Sprintf("%s_%s_%d", *prefix, run, i), run, *offset, waits[i%len(waits)])
		if err != nil {
			return fmt.Errorf("trial %d: %w", i, err)
		}
		rep.Trials = append(rep.Trials, tr)
		note := ""
		if tr.Discard != "" {
			note = "  (discard: " + tr.Discard + ")"
		}
		fmt.Fprintf(w, "trial %d  offset=%s wait=%.0fs  same_sql=%s  unique_sql=%s%s\n", i, *offset, tr.DelayS,
			secs(tr.SameSQLS), secs(tr.UniqueSQLS), note)
		if tr.Discard != "" {
			rep.Discarded++
			continue
		}
		if tr.SameSQLS < 0 || tr.UniqueSQLS < 0 {
			rep.Missed++
		}
		if tr.SameSQLS >= 0 {
			same = append(same, time.Duration(tr.SameSQLS*float64(time.Second)))
		}
		if tr.UniqueSQLS >= 0 {
			unique = append(unique, time.Duration(tr.UniqueSQLS*float64(time.Second)))
		}
	}
	rep.SameSQL, rep.Unique = quantilesOf(same), quantilesOf(unique)
	fmt.Fprintf(w, "late    flush returned -> first query that returned the row, ms\n")
	fmt.Fprintf(w, "        same SQL text as the query that found the hour empty   %s\n", rep.SameSQL)
	fmt.Fprintf(w, "        SQL text never sent before                             %s\n", rep.Unique)
	fmt.Fprintf(w, "        not visible within %s: %d of %d trials; discarded: %d\n", c.timeout, rep.Missed, len(rep.Trials)-rep.Discarded, rep.Discarded)
	return writeReport(c.out, rep)
}

func secs(s float64) string {
	if s < 0 {
		return "not seen"
	}
	return fmt.Sprintf("%.3fs", s)
}

// lateTrialRun is one reproduction attempt of #1757, on a measurement of its own:
//
//  1. a row in the current hour, flushed: the measurement now has one hour
//     partition, so a ranged query is pruned to a list of partitions (with
//     none, Arc falls back to reading everything and nothing is cached);
//  2. a query over a range that includes the hour the late row will land in,
//     which has no directory yet: "that hour is empty" is now cached;
//  3. after the wait, the late row, flushed: its file is in storage;
//  4. the same query text, and a query text never sent before, polled until
//     each returns the row.
func lateTrialRun(ctx context.Context, cl *client, c common, measurement, run string, offset, wait time.Duration) (lateTrial, error) {
	qcl := cl
	if c.probeURL != "" {
		q := *cl
		q.base = strings.TrimRight(c.probeURL, "/")
		qcl = &q
	}
	tr := lateTrial{Measurement: measurement, OffsetS: offset.Seconds(), DelayS: wait.Seconds(), SameSQLS: -1, UniqueSQLS: -1}
	now := time.Now()
	hour := hourClock().UTC().Truncate(time.Hour)
	// For its first minutes an hour keeps the previous one "live" (liveGrace,
	// internal/pruning/live_hours.go): a query that finds a live hour empty is
	// not cached at all, so there is nothing for the late row to hide behind.
	// The trial's range always includes the previous hour.
	if sinceHour := hourClock().UTC().Sub(hour); sinceHour < liveGrace {
		tr.Discard = fmt.Sprintf("started %s into the hour, inside the %s in which Arc caches no plan that includes the previous hour", sinceHour.Round(time.Second), liveGrace)
	}
	from, to := now.Add(-time.Hour), now.Add(time.Hour)
	if offset < 0 {
		from = now.Add(offset).Add(-time.Hour)
	} else {
		to = now.Add(offset).Add(time.Hour)
	}
	sql := fmt.Sprintf("SELECT marker FROM %s.%s WHERE time >= %s AND time < %s AND run = '%s' AND marker > 0",
		c.database, measurement, timeLiteral(from), timeLiteral(to), run)

	if err := cl.write(ctx, c.database, lineProtocol(measurement, run, 1, 0, now)); err != nil {
		return tr, fmt.Errorf("seed write: %w", err)
	}
	if err := cl.flush(ctx); err != nil {
		return tr, err
	}
	// Wait for the seed row through the very query text used later: its last
	// run is the one that finds the late row's hour empty.
	seedBy := time.Now().Add(c.timeout)
	for {
		got, err := qcl.queryMarkers(ctx, sql)
		if err != nil {
			return tr, fmt.Errorf("prequery: %w", err)
		}
		if contains(got, 1) {
			break
		}
		if time.Now().After(seedBy) {
			return tr, fmt.Errorf("the seed row never became visible")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-ctx.Done():
		return tr, ctx.Err()
	case <-time.After(wait):
	}
	if err := cl.write(ctx, c.database, lineProtocol(measurement, run, 2, 0, time.Now().Add(offset))); err != nil {
		return tr, fmt.Errorf("late write: %w", err)
	}
	if err := cl.flush(ctx); err != nil {
		return tr, err
	}
	published := time.Now()
	deadline := published.Add(c.timeout)
	for n := 0; (tr.SameSQLS < 0 || tr.UniqueSQLS < 0) && time.Now().Before(deadline) && ctx.Err() == nil; n++ {
		// A failed query is counted and the trial goes on: it says nothing
		// about what was visible.
		if tr.SameSQLS < 0 {
			if got, err := qcl.queryMarkers(ctx, sql); err != nil {
				tr.QueryErrors++
			} else if contains(got, 2) {
				tr.SameSQLS = time.Since(published).Seconds()
			}
		}
		if tr.UniqueSQLS < 0 {
			// A predicate that changes the text and nothing else.
			if got, err := qcl.queryMarkers(ctx, fmt.Sprintf("%s AND marker <> -%d", sql, n+1)); err != nil {
				tr.QueryErrors++
			} else if contains(got, 2) {
				tr.UniqueSQLS = time.Since(published).Seconds()
			}
		}
		if c.poll > 0 {
			time.Sleep(c.poll)
		}
	}
	// The caches are keyed by the current hour, so a trial that crossed an
	// hour boundary measured the rollover, not the cache.
	if tr.Discard == "" && !hourClock().UTC().Truncate(time.Hour).Equal(hour) {
		tr.Discard = "the hour rolled over during the trial"
	}
	return tr, ctx.Err()
}

// liveGrace mirrors internal/pruning's: how long the previous hour stays
// "live" after an hour starts.
const liveGrace = 5 * time.Minute

// hourClock is the clock the discard rules read; a test replaces it.
var hourClock = time.Now

func contains(xs []int64, x int64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
