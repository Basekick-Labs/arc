package api

// #1383 follow-up: the remote write limits follow server.max_payload_size,
// the WAL reservation is what the rows append, a label spelled two ways
// drops only the series using the other spelling, and remote write has its
// own counters.

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/stretchr/testify/require"
)

// rwSeries is a series of samples samples at increasing timestamps.
func rwSeries(labels []remoteWriteTestLabel, samples int) []byte {
	s := make([]remoteWriteTestSample, samples)
	for i := range s {
		s[i] = remoteWriteTestSample{value: float64(i), time: 1_763_037_600_000 + int64(i)}
	}
	return remoteWriteTestSeries(labels, s, false, false)
}

// rwDecodedOf is a request of one series whose decoded size is at least n.
func rwDecodedOf(n int) []byte {
	one := len(remoteWriteTestRequest(rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"job", "j"}}, 1)))
	samples := n / (one - 8)
	for {
		req := remoteWriteTestRequest(rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"job", "j"}}, samples))
		if len(req) >= n {
			return req
		}
		samples += 64
	}
}

func TestPrometheusRemoteWritePayloadLimitFollowsMaxPayloadSize(t *testing.T) {
	h := &LineProtocolHandler{}
	require.Equal(t, maxRemoteWritePayloadBytes, h.remoteWritePayloadLimit(), "unset")
	h.SetMaxPayloadSize(1 << 30) // the 1GB default
	require.Equal(t, maxRemoteWritePayloadBytes, h.remoteWritePayloadLimit())
	h.SetMaxPayloadSize(maxRemoteWritePayloadBytes + 1)
	require.Equal(t, maxRemoteWritePayloadBytes, h.remoteWritePayloadLimit())
	h.SetMaxPayloadSize(maxRemoteWritePayloadBytes)
	require.Equal(t, maxRemoteWritePayloadBytes, h.remoteWritePayloadLimit())
	h.SetMaxPayloadSize(10 << 20)
	require.Equal(t, 10<<20, h.remoteWritePayloadLimit())
}

// A small body that decodes past server.max_payload_size is refused 413, as
// msgpack refuses its decompressed body; below it, it is written.
func TestPrometheusRemoteWriteDecodedSizeFollowsMaxPayloadSize(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int64
		decoded int
		status  int
		message string
	}{
		{"over 1MB", 1 << 20, 1<<20 + 1, http.StatusRequestEntityTooLarge, "remote write payload exceeds 1MB limit"},
		{"under 1MB", 1 << 20, 1<<20 - 4096, http.StatusNoContent, ""},
		{"over 1.5MB", 3 << 19, 3<<19 + 1, http.StatusRequestEntityTooLarge, "remote write payload exceeds 1.5MB limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newLPWriteRig(t, nil)
			r.h.SetMaxPayloadSize(tc.limit)
			req := rwDecodedOf(tc.decoded)
			if tc.status == http.StatusNoContent {
				require.LessOrEqual(t, len(req), int(tc.limit))
			}
			body := remoteWriteTestBody(req)
			require.Less(t, len(body), int(tc.limit), "the body itself is under the limit")
			status, out := remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
			require.Equal(t, tc.status, status, out)
			require.Contains(t, out, tc.message)
			rows, _ := r.stored(t)
			if tc.status != http.StatusNoContent {
				require.Empty(t, rows)
			}
		})
	}
	// Exactly at the limit is accepted.
	req := rwDecodedOf(1 << 19)
	_, size, err := decodePrometheusRemoteWrite(remoteWriteTestBody(req), len(req))
	require.NoError(t, err)
	require.Equal(t, len(req), size)
	_, _, err = decodePrometheusRemoteWrite(remoteWriteTestBody(req), len(req)-1)
	require.EqualError(t, err, remoteWriteLimitError{kind: remoteWriteLimitPayload, limit: len(req) - 1}.Error())
}

// Label names are repeated on every sample row as their values are, and
// have no length limit of their own: they count in the repeated-bytes limit,
// which follows server.max_payload_size.
func TestPrometheusRemoteWriteRepeatedLabelNamesCount(t *testing.T) {
	long1 := "a" + strings.Repeat("n", 10_000)
	long2 := "b" + strings.Repeat("n", 10_000)
	labels := []remoteWriteTestLabel{{"__name__", "up"}, {long1, "v"}, {long2, "v"}}
	r := newLPWriteRig(t, nil)
	r.h.SetMaxPayloadSize(1 << 20)
	body := remoteWriteTestBody(remoteWriteTestRequest(rwSeries(labels, 60))) // 60 x 20 KB > 1 MiB
	status, out := remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
	require.Equal(t, http.StatusRequestEntityTooLarge, status, out)
	require.Contains(t, out, "repeats more than 1MB of label names and values")

	body = remoteWriteTestBody(remoteWriteTestRequest(rwSeries(labels, 40))) // 40 x 20 KB < 1 MiB
	status, out = remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
	require.Equal(t, http.StatusNoContent, status, out)
}

// recordingWAL records the size of each WAL append.
type recordingWAL struct {
	mu       sync.Mutex
	appended int64
}

func (w *recordingWAL) Append([]map[string]interface{}) error { return nil }
func (w *recordingWAL) AppendRaw(p []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.appended += int64(len(p))
	return nil
}
func (w *recordingWAL) AppendRawWithMeta(_ string, p []byte) error { return w.AppendRaw(p) }
func (w *recordingWAL) Stats() map[string]interface{}              { return nil }
func (w *recordingWAL) Close() error                               { return nil }

// reservingGate records what is reserved and what is still reserved, and
// optionally has a structural maximum (wal.DiskMonitor.MaxReservable) or no
// room at all.
type reservingGate struct {
	mu       sync.Mutex
	reserved []int64
	held     int64
	most     int64
	full     bool
}

func (g *reservingGate) Pressure() bool { return false }
func (g *reservingGate) Reserve(n int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.full {
		return false
	}
	g.reserved = append(g.reserved, n)
	g.held += n
	return true
}
func (g *reservingGate) Release(n int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.held -= n
}
func (g *reservingGate) MaxReservable() int64 { return g.most }

// The request reserves exactly what its rows append to the WAL: every sample
// row repeats its series' labels, so with several samples per series that is
// far more than the body, and with one it is a little less.
func TestPrometheusRemoteWriteReservesItsWALBytes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		series, samples int
		labels          int
	}{
		{"50 series x 20 samples x 10 labels", 50, 20, 10},
		{"2000 series x 1 sample", 2000, 1, 6},
		{"wide rows: 20 labels, values to 300 bytes", 30, 5, 20},
		{"70000 rows in one measurement", 1, 70000, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newLPWriteRig(t, nil)
			w := &recordingWAL{}
			r.buf.SetWAL(w)
			g := &reservingGate{}
			r.buf.SetWALDiskGate(g)
			var series [][]byte
			for i := 0; i < tc.series; i++ {
				labels := []remoteWriteTestLabel{{"__name__", []string{"up", "node_load1", "http_requests_total"}[i%3]}}
				for k := 0; k < tc.labels; k++ {
					labels = append(labels, remoteWriteTestLabel{name: "l" + strings.Repeat("x", k*7), value: strings.Repeat("v", (i+k*15)%301)})
				}
				series = append(series, rwSeries(labels, tc.samples))
			}
			req := remoteWriteTestRequest(series...)
			status, out := remoteWriteTestPost(t, r, remoteWriteTestBody(req), remoteWriteTestHeaders())
			require.Equal(t, http.StatusNoContent, status, out)
			require.Len(t, g.reserved, 1)
			require.Equal(t, w.appended, g.reserved[0], "reserved vs appended (body %d bytes)", len(req))
			require.Zero(t, g.held)
		})
	}
}

// A request the WAL directory can never hold is 413, which Prometheus does
// not retry; one that does not fit only now is 503, retried.
func TestPrometheusRemoteWriteTooLargeForTheWALDirectory(t *testing.T) {
	body := remoteWriteTestBody(remoteWriteTestRequest(rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"job", "j"}}, 1000)))

	r := newLPWriteRig(t, nil)
	r.buf.SetWAL(&recordingWAL{})
	r.buf.SetWALDiskGate(&reservingGate{most: 1000})
	status, out := remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
	require.Equal(t, http.StatusRequestEntityTooLarge, status, out)
	require.Contains(t, out, "of WAL, more than wal.max_directory_mb allows")

	r = newLPWriteRig(t, nil)
	r.buf.SetWAL(&recordingWAL{})
	r.buf.SetWALDiskGate(&reservingGate{most: 1 << 30, full: true})
	status, out = remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
	require.Equal(t, http.StatusServiceUnavailable, status, out)

	r = newLPWriteRig(t, nil)
	r.buf.SetWAL(&recordingWAL{})
	r.buf.SetWALDiskGate(&reservingGate{}) // no directory cap
	status, out = remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
	require.Equal(t, http.StatusNoContent, status, out)
}

func rwTwinRequest(reverse bool) []byte {
	series := [][]byte{
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "i1"}}, 1),
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "i2"}}, 1),
		rwSeries([]remoteWriteTestLabel{{"Instance", "I1"}, {"__name__", "up"}}, 1),
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"host", "h1"}}, 1),
	}
	if reverse {
		for i, j := 0, len(series)-1; i < j; i, j = i+1, j-1 {
			series[i], series[j] = series[j], series[i]
		}
	}
	return remoteWriteTestRequest(series...)
}

// A label spelled two ways drops the series using the other spelling, not
// every sample of the metric: the byte-smallest spelling in the request wins
// whatever the series order, so Prometheus shards sending the same series
// keep the same one.
func TestPrometheusRemoteWriteCaseTwinDropsOnlyTheSeries(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		r := newLPWriteRig(t, nil)
		status, out := remoteWriteTestPost(t, r, remoteWriteTestBody(rwTwinRequest(reverse)), remoteWriteTestHeaders())
		require.Equal(t, http.StatusBadRequest, status, out)
		require.Contains(t, out, `partial write: 2 points dropped: 2 in \"up\" column \"instance\": `+ingest.ColumnNameCaseTwin)
		require.Contains(t, out, "the other points were written")
		rows, types := r.stored(t)
		require.Equal(t, map[string]int64{"up": 2}, rows, "reverse=%v", reverse)
		require.Contains(t, types["up"], "Instance")
		require.Contains(t, types["up"], "host")
		require.NotContains(t, types["up"], "instance")
	}
}

// A stored column's spelling wins over the request's, whatever their byte
// order; the token sees it only where it may write.
func TestPrometheusRemoteWriteCaseTwinStoredSpellingWins(t *testing.T) {
	stored := map[string]string{"host": "host", "value": "value"}
	req := remoteWriteTestRequest(
		rwSeries([]remoteWriteTestLabel{{"Host", "a"}, {"__name__", "up"}}, 2),
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"host", "b"}}, 1),
	)
	batch, err := remoteWriteBuild(remoteWriteTestBody(req), "d", func(string) map[string]string { return stored }, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Equal(t, 1, batch.rows)
	require.Equal(t, []ingest.LPDrop{{Measurement: "up", Column: "Host", Reason: ingest.ColumnNameCaseTwin, Points: 2}}, batch.drops)
	require.Contains(t, batch.records["up"].Data, "host")

	// A stored "Value" (a line-protocol field) leaves no room for the sample
	// column: every series of the measurement is dropped, naming it, and the
	// name filter behind it finds nothing more.
	stored = map[string]string{"value": "Value"}
	batch, err = remoteWriteBuild(remoteWriteTestBody(req), "d", func(string) map[string]string { return stored }, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Zero(t, batch.rows)
	require.Equal(t, []ingest.LPDrop{{Measurement: "up", Column: "value", Reason: ingest.ColumnNameCaseTwin, Points: 3}}, batch.drops)
}

// A series carrying both spellings itself is dropped whole; the others stay.
func TestPrometheusRemoteWriteCaseTwinWithinOneSeries(t *testing.T) {
	req := remoteWriteTestRequest(
		rwSeries([]remoteWriteTestLabel{{"Host", "a"}, {"__name__", "up"}, {"host", "a"}}, 1),
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"job", "j"}}, 1),
	)
	batch, err := remoteWriteBuild(remoteWriteTestBody(req), "d", nil, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Equal(t, 1, batch.rows)
	require.Equal(t, []ingest.LPDrop{{Measurement: "up", Column: "host", Reason: ingest.ColumnNameCaseTwin, Points: 1}}, batch.drops)
	require.NotContains(t, batch.records["up"].Data, "Host")
	require.NotContains(t, batch.records["up"].Data, "host")
}

// A dropped series takes no budget: a twin series that alone would pass the
// repeated-bytes limit does not refuse the request.
func TestPrometheusRemoteWriteDroppedSeriesTakesNoBudget(t *testing.T) {
	big := strings.Repeat("x", 4096)
	req := remoteWriteTestRequest(
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"Host", "small"}}, 10), // "Host" < "host": kept
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"host", big}}, 300),    // 300 x 4 KB > 1 MiB, dropped
	)
	batch, err := remoteWriteBuild(remoteWriteTestBody(req), "d", nil, 1<<20)
	require.NoError(t, err)
	require.Equal(t, 10, batch.rows)
	require.Equal(t, 300, batch.dropped)
}

func rwCounters() [4]int64 {
	s := metrics.Get().Snapshot()
	return [4]int64{
		s["prometheus_remote_write_requests_total"].(int64),
		s["prometheus_remote_write_errors_total"].(int64),
		s["prometheus_remote_write_records_total"].(int64),
		s["prometheus_remote_write_bytes_total"].(int64),
	}
}

// Remote write has its own counters, and leaves line protocol's alone.
func TestPrometheusRemoteWriteCounters(t *testing.T) {
	r := newLPWriteRig(t, nil)
	lpBefore := r.h.GetStats()
	ok := remoteWriteTestRequest(rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"job", "j"}}, 3))

	before := rwCounters()
	status, out := remoteWriteTestPost(t, r, remoteWriteTestBody(ok), remoteWriteTestHeaders())
	require.Equal(t, http.StatusNoContent, status, out)
	require.Equal(t, [4]int64{1, 0, 3, int64(len(ok))}, sub(rwCounters(), before))

	before = rwCounters()
	status, out = remoteWriteTestPost(t, r, remoteWriteTestBody(rwTwinRequest(false)), remoteWriteTestHeaders())
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Equal(t, [4]int64{1, 1, 2, int64(len(rwTwinRequest(false)))}, sub(rwCounters(), before), "partial write")

	before = rwCounters()
	headers := remoteWriteTestHeaders()
	delete(headers, fiberHeaderUserAgent)
	status, _ = remoteWriteTestPost(t, r, remoteWriteTestBody(ok), headers)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, [4]int64{1, 1, 0, 0}, sub(rwCounters(), before), "refused before decoding")

	r.h.SetMaxPayloadSize(64)
	before = rwCounters()
	status, _ = remoteWriteTestPost(t, r, remoteWriteTestBody(ok), remoteWriteTestHeaders())
	require.Equal(t, http.StatusRequestEntityTooLarge, status)
	require.Equal(t, [4]int64{1, 1, 0, 0}, sub(rwCounters(), before), "413")
	r.h.SetMaxPayloadSize(0)

	r.buf.SetWAL(&recordingWAL{})
	r.buf.SetWALDiskGate(&reservingGate{full: true})
	before = rwCounters()
	status, _ = remoteWriteTestPost(t, r, remoteWriteTestBody(ok), remoteWriteTestHeaders())
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, [4]int64{1, 1, 0, int64(len(ok))}, sub(rwCounters(), before), "503")
	r.buf.SetWALDiskGate(nil)

	require.Equal(t, lpBefore, r.h.GetStats(), "line-protocol stats moved")
	require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
}

// A write stopped part-way counts the samples it wrote.
func TestPrometheusRemoteWriteCountersStoppedPartWay(t *testing.T) {
	r := newLPWriteRig(t, nil)
	failLPAt(t, "up", ingest.ErrSchemaChurnExceeded)
	req := remoteWriteTestRequest(
		rwSeries([]remoteWriteTestLabel{{"__name__", "node_load1"}, {"job", "j"}}, 4),
		rwSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"job", "j"}}, 2),
	)
	before := rwCounters()
	status, out := remoteWriteTestPost(t, r, remoteWriteTestBody(req), remoteWriteTestHeaders())
	require.GreaterOrEqual(t, status, 400, out)
	require.Contains(t, out, "node_load1")
	require.Equal(t, [4]int64{1, 1, 4, int64(len(req))}, sub(rwCounters(), before))
}

func sub(a, b [4]int64) [4]int64 {
	return [4]int64{a[0] - b[0], a[1] - b[1], a[2] - b[2], a[3] - b[3]}
}
