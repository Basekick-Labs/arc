package api

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/klauspost/compress/snappy"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

type remoteWriteTestLabel struct {
	name  string
	value string
}

type remoteWriteTestSample struct {
	value float64
	time  int64
}

func remoteWriteTestRequest(series ...[]byte) []byte {
	var request []byte
	for _, timeSeries := range series {
		request = protowire.AppendTag(request, 1, protowire.BytesType)
		request = protowire.AppendBytes(request, timeSeries)
	}
	return request
}

func remoteWriteTestSeries(labels []remoteWriteTestLabel, samples []remoteWriteTestSample, exemplars, histograms bool) []byte {
	var series []byte
	for _, label := range labels {
		var message []byte
		message = protowire.AppendTag(message, 1, protowire.BytesType)
		message = protowire.AppendString(message, label.name)
		message = protowire.AppendTag(message, 2, protowire.BytesType)
		message = protowire.AppendString(message, label.value)
		series = protowire.AppendTag(series, 1, protowire.BytesType)
		series = protowire.AppendBytes(series, message)
	}
	for _, sample := range samples {
		var message []byte
		message = protowire.AppendTag(message, 1, protowire.Fixed64Type)
		message = protowire.AppendFixed64(message, math.Float64bits(sample.value))
		message = protowire.AppendTag(message, 2, protowire.VarintType)
		message = protowire.AppendVarint(message, uint64(sample.time))
		series = protowire.AppendTag(series, 2, protowire.BytesType)
		series = protowire.AppendBytes(series, message)
	}
	if exemplars {
		series = protowire.AppendTag(series, 3, protowire.BytesType)
		series = protowire.AppendBytes(series, nil)
	}
	if histograms {
		series = protowire.AppendTag(series, 4, protowire.BytesType)
		series = protowire.AppendBytes(series, nil)
	}
	return series
}

func remoteWriteTestBody(request []byte) []byte {
	return snappy.Encode(nil, request)
}

func remoteWriteTestHeaders() map[string]string {
	return map[string]string{
		fiberHeaderContentEncoding:          "snappy",
		fiberHeaderContentType:              "application/x-protobuf",
		"X-Prometheus-Remote-Write-Version": prometheusRemoteWriteVersion,
		fiberHeaderUserAgent:                "prometheus/3.0.0",
		"x-arc-database":                    "d",
	}
}

func remoteWriteTestPost(t *testing.T, r *lpWriteRig, body []byte, headers map[string]string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, prometheusRemoteWritePath, bytes.NewReader(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out bytes.Buffer
	_, err = out.ReadFrom(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out.String()
}

// Keep header names as constants here so the fixture makes the HTTP contract visible.
const (
	fiberHeaderContentEncoding = "Content-Encoding"
	fiberHeaderContentType     = "Content-Type"
	fiberHeaderUserAgent       = "User-Agent"
)

func TestPrometheusRemoteWriteDecodesAndMapsSamples(t *testing.T) {
	cpuHost := remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "host", value: "a"}},
		[]remoteWriteTestSample{{value: 12.5, time: 1_700_000_000_000}, {value: 13.5, time: 1_700_000_001_000}},
		false, false,
	)
	cpuRegion := remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "region", value: "eu"}},
		[]remoteWriteTestSample{{value: 7, time: 1_700_000_002_000}},
		false, false,
	)
	request := remoteWriteTestRequest(cpuHost, cpuRegion)
	// Remote Write metadata is ancillary and is ignored by the v1 receiver.
	request = protowire.AppendTag(request, 3, protowire.BytesType)
	request = protowire.AppendBytes(request, []byte{0x08, 0x01})

	decoded, decodedSize, err := decodePrometheusRemoteWrite(remoteWriteTestBody(request), maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Equal(t, len(request), decodedSize)
	records, _, drops, err := prometheusRemoteWriteRecords(decoded, "d", nil, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Empty(t, drops)
	require.Equal(t, []string{"host", "region"}, records["cpu"].TagColumns)
	require.Equal(t, []interface{}{int64(1_700_000_000_000_000), int64(1_700_000_001_000_000), int64(1_700_000_002_000_000)}, records["cpu"].Columns["time"])
	require.Equal(t, []interface{}{12.5, 13.5, 7.0}, records["cpu"].Columns["value"])
	require.Equal(t, []interface{}{"a", "a", nil}, records["cpu"].Columns["host"])
	require.Equal(t, []interface{}{nil, nil, "eu"}, records["cpu"].Columns["region"])

	trig := newLPWriteRig(t, nil)
	status, out := remoteWriteTestPost(t, trig, remoteWriteTestBody(request), remoteWriteTestHeaders())
	require.Equal(t, http.StatusNoContent, status, out)
	rows, types := trig.stored(t)
	require.Equal(t, map[string]int64{"cpu": 3}, rows)
	require.Equal(t, "float64", types["cpu"]["value"])
	require.Equal(t, "utf8", types["cpu"]["host"])
	require.Equal(t, "utf8", types["cpu"]["region"])

	backend, err := storage.NewLocalBackend(trig.dir, zerolog.Nop())
	require.NoError(t, err)
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: trig.dir,
	}, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })
	queryHandler := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
	queryHandler.RegisterRoutes(trig.app)
	queryReq := httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewBufferString(
		`{"sql":"SELECT count(*), count(host), count(region), sum(value) FROM d.cpu"}`,
	))
	queryReq.Header.Set(fiberHeaderContentType, "application/json")
	queryResp, err := trig.app.Test(queryReq, -1)
	require.NoError(t, err)
	defer queryResp.Body.Close()
	var queryResult struct {
		Success bool            `json:"success"`
		Data    [][]interface{} `json:"data"`
		Error   string          `json:"error"`
	}
	require.NoError(t, json.NewDecoder(queryResp.Body).Decode(&queryResult))
	require.Equal(t, http.StatusOK, queryResp.StatusCode, queryResult.Error)
	require.True(t, queryResult.Success, queryResult.Error)
	require.Equal(t, [][]interface{}{{float64(3), float64(2), float64(1), float64(33)}}, queryResult.Data)
}

func TestPrometheusRemoteWriteAcceptsEmptyNegotiationRequest(t *testing.T) {
	trig := newLPWriteRig(t, nil)
	status, out := remoteWriteTestPost(t, trig, remoteWriteTestBody(nil), remoteWriteTestHeaders())
	require.Equal(t, http.StatusNoContent, status, out)
	require.Zero(t, trig.buf.BufferedBytes())
}

func TestDecodePrometheusRemoteWritePreservesSnappyBlockErrors(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "invalid length prefix", body: []byte{0xff}},
		{name: "truncated literal", body: []byte{4, 0x0c, 'a'}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := decodePrometheusRemoteWrite(tt.body, maxRemoteWritePayloadBytes)
			require.EqualError(t, err, "invalid snappy block: snappy: corrupt input")
		})
	}
}

func TestPrometheusRemoteWriteRejectsInvalidRequestsWithoutWriting(t *testing.T) {
	valid := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: 1_700_000_000_000}},
		false, false,
	)))
	badProto := remoteWriteTestBody([]byte{0x0a, 0x02, 0x0a})
	largeDecodedPrefix := binary.AppendUvarint(nil, uint64(maxRemoteWritePayloadBytes+1))
	largeDecoded := largeDecodedPrefix
	noName := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "host", value: "a"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, false,
	)))
	duplicate := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "host", value: "a"}, {name: "host", value: "b"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, false,
	)))
	timeLabel := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "time", value: "x"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, false,
	)))
	valueLabel := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "value", value: "x"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, false,
	)))
	tooLargeTimestamp := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: math.MaxInt64}},
		false, false,
	)))

	tests := []struct {
		name    string
		body    []byte
		headers map[string]string
		status  int
		message string
	}{
		{name: "wrong content encoding", body: valid, headers: map[string]string{fiberHeaderContentEncoding: "gzip"}, status: http.StatusUnsupportedMediaType, message: "Content-Encoding"},
		{name: "wrong content type", body: valid, headers: map[string]string{fiberHeaderContentType: "application/json"}, status: http.StatusUnsupportedMediaType, message: "Content-Type"},
		{name: "remote write 2.0 message", body: valid, headers: map[string]string{fiberHeaderContentType: "application/x-protobuf;proto=io.prometheus.write.v2.Request"}, status: http.StatusUnsupportedMediaType, message: "Remote Write 1.0"},
		{name: "missing version", body: valid, headers: map[string]string{"X-Prometheus-Remote-Write-Version": ""}, status: http.StatusBadRequest, message: "Version"},
		{name: "missing user agent", body: valid, headers: map[string]string{fiberHeaderUserAgent: ""}, status: http.StatusBadRequest, message: "User-Agent"},
		{name: "malformed snappy", body: []byte{0xff}, headers: nil, status: http.StatusBadRequest, message: "snappy"},
		{name: "malformed protobuf", body: badProto, headers: nil, status: http.StatusBadRequest, message: "protobuf"},
		{name: "decoded body limit", body: largeDecoded, headers: nil, status: http.StatusRequestEntityTooLarge, message: "remote write payload exceeds 100MB limit"},
		{name: "missing metric name", body: noName, headers: nil, status: http.StatusBadRequest, message: "partial write: 1 points dropped"},
		{name: "duplicate label", body: duplicate, headers: nil, status: http.StatusBadRequest, message: "same label twice"},
		{name: "timestamp label", body: timeLabel, headers: nil, status: http.StatusBadRequest, message: "time"},
		{name: "value label", body: valueLabel, headers: nil, status: http.StatusBadRequest, message: "conflicts with the sample column"},
		{name: "timestamp overflow", body: tooLargeTimestamp, headers: nil, status: http.StatusBadRequest, message: "outside Arc"},
	}

	trig := newLPWriteRig(t, nil)
	baseHeaders := remoteWriteTestHeaders()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := make(map[string]string, len(baseHeaders)+len(tt.headers))
			for name, value := range baseHeaders {
				headers[name] = value
			}
			for name, value := range tt.headers {
				headers[name] = value
			}
			status, out := remoteWriteTestPost(t, trig, tt.body, headers)
			require.Equal(t, tt.status, status, out)
			require.Contains(t, out, tt.message)
			require.Zero(t, trig.buf.BufferedBytes())
		})
	}
}

func TestPrometheusRemoteWriteBoundsMaterialisedColumnCells(t *testing.T) {
	request := &remoteWriteRequest{Timeseries: make([]remoteWriteTimeSeries, 0, 3000)}
	for i := 0; i < 3000; i++ {
		request.Timeseries = append(request.Timeseries, remoteWriteTimeSeries{
			Labels: []remoteWriteLabel{
				{Name: "__name__", Value: "cpu"},
				{Name: fmt.Sprintf("tag_%06d", i), Value: "x"},
			},
			Samples: []remoteWriteSample{{Value: 1, Timestamp: int64(i)}},
		})
	}
	_, _, _, err := prometheusRemoteWriteRecords(request, "d", nil, maxRemoteWritePayloadBytes)
	require.ErrorIs(t, err, errRemoteWriteTooManyCells)
	require.True(t, isRemoteWriteTooLarge(err))
}

func TestPrometheusRemoteWriteBoundsProtoElements(t *testing.T) {
	t.Run("time series", func(t *testing.T) {
		var request []byte
		for i := 0; i <= maxRemoteWriteTimeSeries; i++ {
			request = protowire.AppendTag(request, 1, protowire.BytesType)
			request = protowire.AppendBytes(request, nil)
		}
		_, err := unmarshalRemoteWriteRequest(request)
		require.ErrorIs(t, err, errRemoteWriteTooManySeries)
		require.True(t, isRemoteWriteTooLarge(err))
	})

	t.Run("samples", func(t *testing.T) {
		var series []byte
		for i := 0; i < 2; i++ {
			series = protowire.AppendTag(series, 2, protowire.BytesType)
			series = protowire.AppendBytes(series, nil)
		}
		budget := &remoteWriteParseBudget{samples: maxRemoteWriteSamples - 1}
		_, err := unmarshalRemoteWriteTimeSeries(series, budget)
		require.ErrorIs(t, err, errRemoteWriteTooManySamples)
		require.True(t, isRemoteWriteTooLarge(err))
	})

	t.Run("labels", func(t *testing.T) {
		var series []byte
		for i := 0; i < 2; i++ {
			series = protowire.AppendTag(series, 1, protowire.BytesType)
			series = protowire.AppendBytes(series, nil)
		}
		budget := &remoteWriteParseBudget{labels: maxRemoteWriteLabels - 1}
		_, err := unmarshalRemoteWriteTimeSeries(series, budget)
		require.ErrorIs(t, err, errRemoteWriteTooManyLabels)
		require.True(t, isRemoteWriteTooLarge(err))
	})
}

// One batch as Prometheus sends it: scraped series, a recording rule, series
// Arc cannot store, staleness markers and exemplars. Only what cannot be
// stored is left out; Prometheus drops a 4xx batch without retrying, so a
// refusal of the whole request lost every other series in it.
func TestPrometheusRemoteWriteDropsOnlyTheSeriesItCannotStore(t *testing.T) {
	const ts = int64(1_763_037_600_000) // ms
	stale := math.Float64frombits(prometheusStaleNaN)
	series := func(labels []remoteWriteTestLabel, samples []remoteWriteTestSample, exemplars bool) []byte {
		return remoteWriteTestSeries(labels, samples, exemplars, false)
	}
	request := remoteWriteTestRequest(
		series([]remoteWriteTestLabel{{"__name__", "node_load1"}, {"instance", "host-01:9100"}, {"job", "node"}},
			[]remoteWriteTestSample{{0.18, ts}, {stale, ts + 5000}}, false),
		// A recording rule: colons become underscores.
		series([]remoteWriteTestLabel{{"__name__", "job:up:sum"}, {"job", "node"}},
			[]remoteWriteTestSample{{1, ts}}, false),
		// A UTF-8 label name (Prometheus 3) is a valid Arc column.
		series([]remoteWriteTestLabel{{"__name__", "http_server_duration_seconds_count"}, {"service.name", "checkout"}},
			[]remoteWriteTestSample{{42, ts}}, false),
		// Samples out of order are stored as sent.
		series([]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "host-02:9100"}, {"job", "node"}},
			[]remoteWriteTestSample{{1, ts + 1000}, {0, ts}}, false),
		// Only a staleness marker: nothing to store, nothing dropped.
		series([]remoteWriteTestLabel{{"__name__", "node_cpu_seconds_total"}, {"cpu", "0"}, {"mode", "idle"}},
			[]remoteWriteTestSample{{stale, ts}}, false),
		// An empty label value is the same as no label.
		series([]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "host-03:9100"}, {"job", "node"}, {"zone", ""}},
			[]remoteWriteTestSample{{1, ts}}, false),
		// Dropped: a label named value, a label named time, no __name__.
		series([]remoteWriteTestLabel{{"__name__", "kube_pod_labels"}, {"value", "x"}},
			[]remoteWriteTestSample{{1, ts}}, false),
		series([]remoteWriteTestLabel{{"__name__", "kube_pod_labels"}, {"Time", "x"}},
			[]remoteWriteTestSample{{1, ts}, {1, ts + 1}}, false),
		series([]remoteWriteTestLabel{{"instance", "host-01:9100"}},
			[]remoteWriteTestSample{{1, ts}}, false),
		// Dropped: Arc does not store a column whose name starts with _,
		// so this series would merge with host-01's up.
		series([]remoteWriteTestLabel{{"__name__", "up"}, {"_tenant", "b"}, {"instance", "host-01:9100"}},
			[]remoteWriteTestSample{{1, ts}}, false),
		// The exemplar is reported; the sample is stored.
		series([]remoteWriteTestLabel{{"__name__", "http_requests_total"}, {"code", "200"}},
			[]remoteWriteTestSample{{7, ts}}, true),
	)

	r := newLPWriteRig(t, nil)
	status, out := remoteWriteTestPost(t, r, remoteWriteTestBody(request), remoteWriteTestHeaders())
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 6 points dropped")
	require.Contains(t, out, `column \"_tenant\"`)
	require.Contains(t, out, `column \"value\"`)
	require.Contains(t, out, `column \"Time\"`)
	require.Contains(t, out, "no __name__ label")
	require.Contains(t, out, "exemplars are not supported")
	require.Contains(t, out, "the other points were written")

	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{
		"node_load1":                         1,
		"job_up_sum":                         1,
		"http_server_duration_seconds_count": 1,
		"up":                                 3,
		"http_requests_total":                1,
	}, rows)
	require.Equal(t, "utf8", types["http_server_duration_seconds_count"]["service.name"])
	require.NotContains(t, types["up"], "zone")
}

func TestPrometheusMeasurementName(t *testing.T) {
	for in, want := range map[string]string{
		"node_load1":               "node_load1",
		"job:http_requests:rate5m": "job_http_requests_rate5m",
		"http.server.duration":     "http_server_duration",
		"temp_°c":                  "temp__c",
		"a-b":                      "a_b",
	} {
		require.Equal(t, want, prometheusMeasurementName(in), in)
	}
	// Still refused after the mapping: Arc measurements start with a letter.
	for _, name := range []string{":up", "_x", "1x"} {
		require.False(t, isValidMeasurementName(prometheusMeasurementName(name)), name)
	}
}

// #1422 on this route: the ingest gate is checked once for the request, so a
// fence closing after the first measurement does not split a batch that
// Prometheus would then resend whole.
func TestPrometheusRemoteWriteFenceClosingMidRequestDoesNotSplitIt(t *testing.T) {
	r := newLPWriteRig(t, nil)
	var mu sync.Mutex
	calls := 0
	r.buf.SetIngestGate(func() error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 2 {
			return nil
		}
		return fencedErr(t)
	})
	request := remoteWriteTestRequest(
		remoteWriteTestSeries([]remoteWriteTestLabel{{"__name__", "node_load1"}, {"instance", "host-01:9100"}},
			[]remoteWriteTestSample{{0.18, 1_763_037_600_000}}, false, false),
		remoteWriteTestSeries([]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "host-01:9100"}},
			[]remoteWriteTestSample{{1, 1_763_037_600_000}}, false, false),
	)
	status, out := remoteWriteTestPost(t, r, remoteWriteTestBody(request), remoteWriteTestHeaders())
	require.Equal(t, http.StatusNoContent, status, out)
	mu.Lock()
	require.Equal(t, 1, calls, "the ingest gate is checked once per request")
	mu.Unlock()
	rows, _ := r.stored(t)
	require.Equal(t, map[string]int64{"node_load1": 1, "up": 1}, rows)
}

// Remote Write 1.0 senders may name their message in the Content-Type.
func TestPrometheusRemoteWriteAcceptsTheNamedV1Message(t *testing.T) {
	r := newLPWriteRig(t, nil)
	headers := remoteWriteTestHeaders()
	headers[fiberHeaderContentType] = "application/x-protobuf;proto=prometheus.WriteRequest"
	body := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "host-01:9100"}},
		[]remoteWriteTestSample{{1, 1_763_037_600_000}}, false, false)))
	status, out := remoteWriteTestPost(t, r, body, headers)
	require.Equal(t, http.StatusNoContent, status, out)
}

// A request whose only series is dropped (a label differing only in letter
// case from another of the same series) leaves nothing written: the answer
// must not say the rest was.
func TestPrometheusRemoteWriteAllDroppedSaysNothingWasWritten(t *testing.T) {
	r := newLPWriteRig(t, nil)
	body := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{"Instance", "host-01:9100"}, {"__name__", "up"}, {"instance", "host-01:9100"}},
		[]remoteWriteTestSample{{1, 1_763_037_600_000}}, false, false)))
	status, out := remoteWriteTestPost(t, r, body, remoteWriteTestHeaders())
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	require.NotContains(t, out, "the other points were written")
	rows, _ := r.stored(t)
	require.Empty(t, rows)
}

// Labels are sent once per series and repeated on every sample row: a small
// body must not build an unbounded amount of label bytes.
func TestPrometheusRemoteWriteBoundsRepeatedLabelBytes(t *testing.T) {
	big := strings.Repeat("x", 16<<10)
	samples := make([]remoteWriteSample, 7000) // 7000 x 16 KiB > 100 MiB
	for i := range samples {
		samples[i] = remoteWriteSample{Value: 1, Timestamp: int64(1_763_037_600_000 + i)}
	}
	request := &remoteWriteRequest{Timeseries: []remoteWriteTimeSeries{{
		Labels:  []remoteWriteLabel{{Name: "__name__", Value: "up"}, {Name: "pod", Value: big}},
		Samples: samples,
	}}}
	_, _, _, err := prometheusRemoteWriteRecords(request, "d", nil, maxRemoteWritePayloadBytes)
	require.ErrorAs(t, err, &remoteWriteLimitError{})
	require.EqualError(t, err, "remote write request repeats more than 100MB of label names and values across its samples")
	require.True(t, isRemoteWriteTooLarge(err))

	request.Timeseries[0].Samples = samples[:6000] // 6000 x 16 KiB < 100 MiB
	_, _, _, err = prometheusRemoteWriteRecords(request, "d", nil, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
}

// A Snappy block declaring far more than its body can hold is refused before
// the decoder allocates the declared length.
func TestPrometheusRemoteWriteRefusesAnImpossibleSnappyLength(t *testing.T) {
	_, _, err := decodePrometheusRemoteWrite(binary.AppendUvarint(nil, 50<<20), maxRemoteWritePayloadBytes)
	require.ErrorContains(t, err, "invalid snappy block: declares")
	require.False(t, isRemoteWriteTooLarge(err))
}

// The ratio guard refuses only what Snappy cannot produce: a very repetitive
// valid request, compressed by a real encoder, is accepted.
func TestPrometheusRemoteWriteAcceptsAHighlyRepetitiveRequest(t *testing.T) {
	samples := make([]remoteWriteTestSample, 20000)
	for i := range samples {
		samples[i] = remoteWriteTestSample{value: 0, time: 1_763_037_600_000}
	}
	request := remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{"__name__", "up"}, {"instance", "host-01:9100"}}, samples, false, false))
	body := remoteWriteTestBody(request)
	require.Greater(t, len(request)/len(body), 15, "the request must compress close to Snappy's limit")
	_, decodedSize, err := decodePrometheusRemoteWrite(body, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Equal(t, len(request), decodedSize)
}

// remoteWriteGoldenRequest is the protobuf behind remoteWriteGoldenBody: two
// node_exporter series of three samples each.
func remoteWriteGoldenRequest() []byte {
	var series [][]byte
	for _, host := range []string{"host-01:9100", "host-02:9100"} {
		series = append(series, remoteWriteTestSeries(
			[]remoteWriteTestLabel{{"__name__", "node_cpu_seconds_total"}, {"cpu", "0"}, {"instance", host}, {"job", "node"}, {"mode", "idle"}},
			[]remoteWriteTestSample{{1234.5, 1_700_000_000_000}, {1235.5, 1_700_000_015_000}, {1236.5, 1_700_000_030_000}},
			false, false))
	}
	return remoteWriteTestRequest(series...)
}

// remoteWriteGoldenBody is remoteWriteGoldenRequest as github.com/golang/snappy
// v1.0.0 Encode wrote it (literals, copy1 and copy2 tags): the bytes a sender
// using the reference encoder puts on the wire, kept as bytes so the test does
// not depend on whichever encoder the test files import (#1702).
const remoteWriteGoldenBody = "b802f08b0a99010a220a085f5f6e616d655f5f12166e6f64655f6370755f7365636f6e64735f746f74616c" +
	"0a080a036370751201300a180a08696e7374616e6365120c686f73742d30313a393130300a0b0a036a6f62" +
	"12046e6f64650a0c0a046d6f6465120469646c6512100900000000004a93401080d095ffbc3112100900000000" +
	"004e93401098c596ffbc31121009122452934010b0ba97ffbc31fe9c00059c04323afe9c00529c00"

// snappyTestBlock hand-assembles a Snappy block, so a test reaches the tag
// forms an encoder may never emit for a small input: the 60-63 literal-length
// forms, copy4, and copies that overlap their own output.
type snappyTestBlock []byte

func newSnappyTestBlock(decodedLen int) snappyTestBlock {
	return binary.AppendUvarint(nil, uint64(decodedLen))
}

// literal writes lit with the given length form: 0 is the inline form (up to
// 60 bytes), 60-63 carry the length in 1-4 trailing bytes.
func (b snappyTestBlock) literal(form int, lit []byte) snappyTestBlock {
	n := uint32(len(lit) - 1)
	if form == 0 {
		b = append(b, byte(n<<2))
	} else {
		b = append(b, byte(form<<2))
		for i := 0; i < form-59; i++ {
			b = append(b, byte(n>>(8*i)))
		}
	}
	return append(b, lit...)
}

func (b snappyTestBlock) copy1(offset, length int) snappyTestBlock {
	return append(b, byte(1|(length-4)<<2|(offset>>8)<<5), byte(offset))
}

func (b snappyTestBlock) copy2(offset, length int) snappyTestBlock {
	return append(b, byte(2|(length-1)<<2), byte(offset), byte(offset>>8))
}

func (b snappyTestBlock) copy4(offset, length int) snappyTestBlock {
	return append(b, byte(3|(length-1)<<2), byte(offset), byte(offset>>8), byte(offset>>16), byte(offset>>24))
}

// The receiver decodes exactly the Snappy block format: these vectors pin
// which bodies it accepts, what each decodes to, and the exact error text, so
// a decoder change that alters any of it fails here (#1702).
func TestPrometheusRemoteWriteSnappyGoldenVectors(t *testing.T) {
	golden, err := hex.DecodeString(remoteWriteGoldenBody)
	require.NoError(t, err)
	goldenRequest := remoteWriteGoldenRequest()

	// A request whose instance label is 200 'a's, so copies can overlap.
	run := strings.Repeat("a", 200)
	longRequest := remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{"__name__", "up"}, {"instance", run}, {"job", "node"}},
		[]remoteWriteTestSample{{1, 1_700_000_000_000}, {0, 1_700_000_015_000}},
		false, false))
	start := bytes.Index(longRequest, []byte(run))
	rest := longRequest[start+len(run):]
	require.Greater(t, len(rest), 40)
	handBuilt := newSnappyTestBlock(len(longRequest)).
		literal(61, longRequest[:start+1]). // through the first 'a'
		copy4(1, 64).                       // offset 1 < length: overlapping
		copy2(1, 64).
		copy4(1, 64).
		copy1(1, 7). // 1+64+64+64+7 = 200
		literal(0, rest[:10]).
		literal(60, rest[10:20]).
		literal(62, rest[20:30]).
		literal(63, rest[30:])

	// An offset-0 copy after a valid one is S2's "repeat the last offset",
	// which Snappy does not have. The receiver decodes with s2 (#1711), so
	// it accepts it: nine 'a's. Through the handler that is a 204 only
	// because those bytes parse as a skippable unknown protobuf field (12,
	// fixed64); with other bytes the protobuf stage would refuse the body
	// under any decoder and the row would tell nothing. Keep the bytes.
	s2Repeat := []byte{0x09, 0x00, 'a', 0x01, 0x01, 0x01, 0x00}
	// The same acceptance at the decode stage, independent of the handler:
	// a strict Snappy decoder refuses this body as corrupt.
	_, s2Size, err := decodePrometheusRemoteWrite(s2Repeat, maxRemoteWritePayloadBytes)
	require.NoError(t, err)
	require.Equal(t, 9, s2Size)

	const corrupt = "invalid snappy block: snappy: corrupt input"
	declaredAbove := append(binary.AppendUvarint(nil, uint64(len(goldenRequest)+5)), golden[2:]...)
	declaredBelow := append(binary.AppendUvarint(nil, uint64(len(goldenRequest)-5)), golden[2:]...)
	require.Equal(t, binary.AppendUvarint(nil, uint64(len(goldenRequest))), golden[:2])

	tests := []struct {
		name    string
		body    []byte
		decoded []byte // the request it decodes to, when accepted
		status  int
		message string // the exact "error" value, when refused
	}{
		{name: "reference encoder", body: golden, decoded: goldenRequest, status: http.StatusNoContent},
		{name: "hand-built copy4 overlap and long literals", body: handBuilt, decoded: longRequest, status: http.StatusNoContent},
		{name: "S2 repeat offset", body: s2Repeat, decoded: []byte("aaaaaaaaa"), status: http.StatusNoContent},
		// A 6-byte varint header for 0: no encoder writes it (headers are
		// minimal varints). github.com/golang/snappy accepted it; klauspost's
		// DecodedLen refuses a header longer than 5 bytes.
		{name: "overlong length header", body: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x00}, status: http.StatusBadRequest, message: corrupt},
		{name: "truncated", body: golden[:len(golden)/2], status: http.StatusBadRequest, message: corrupt},
		{name: "declares more than it holds", body: declaredAbove, status: http.StatusBadRequest, message: corrupt},
		{name: "declares less than it holds", body: declaredBelow, status: http.StatusBadRequest, message: corrupt},
		{name: "bad header byte", body: []byte{0xff}, status: http.StatusBadRequest, message: corrupt},
		{name: "header above 32 bits", body: []byte{0x80, 0x80, 0x80, 0x80, 0x10}, status: http.StatusBadRequest, message: corrupt},
		{name: "header varint overflow", body: bytes.Repeat([]byte{0xff}, 11), status: http.StatusBadRequest, message: corrupt},
		{name: "offset-0 first copy", body: []byte{0x05, 0x00, 'a', 0x01, 0x00}, status: http.StatusBadRequest, message: corrupt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trig := newLPWriteRig(t, nil)
			status, out := remoteWriteTestPost(t, trig, tt.body, remoteWriteTestHeaders())
			require.Equal(t, tt.status, status, out)
			if tt.status != http.StatusNoContent {
				var response struct {
					Error string `json:"error"`
				}
				require.NoError(t, json.Unmarshal([]byte(out), &response), out)
				require.Equal(t, tt.message, response.Error)
				require.Zero(t, trig.buf.BufferedBytes())
				return
			}
			request, decodedSize, err := decodePrometheusRemoteWrite(tt.body, maxRemoteWritePayloadBytes)
			require.NoError(t, err)
			require.Equal(t, len(tt.decoded), decodedSize)
			want, err := unmarshalRemoteWriteRequest(tt.decoded)
			require.NoError(t, err)
			require.Equal(t, want, request)
			if len(want.Timeseries) > 0 {
				require.NotZero(t, trig.buf.BufferedBytes())
			}
		})
	}
}
