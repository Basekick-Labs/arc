package api

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/golang/snappy"
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

	decoded, decodedSize, err := decodePrometheusRemoteWrite(remoteWriteTestBody(request))
	require.NoError(t, err)
	require.Equal(t, len(request), decodedSize)
	records, sampleCount, err := prometheusRemoteWriteRecords(decoded)
	require.NoError(t, err)
	require.Equal(t, 3, sampleCount)
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
	validThenNoName := remoteWriteTestBody(remoteWriteTestRequest(
		remoteWriteTestSeries(
			[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
			[]remoteWriteTestSample{{value: 1, time: 1}},
			false, false,
		),
		remoteWriteTestSeries(
			[]remoteWriteTestLabel{{name: "host", value: "a"}},
			[]remoteWriteTestSample{{value: 2, time: 2}},
			false, false,
		),
	))
	unsorted := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "host", value: "a"}, {name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, false,
	)))
	duplicate := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "host", value: "a"}, {name: "host", value: "b"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, false,
	)))
	colonMetric := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "process_cpu:seconds"}},
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
	exemplar := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		true, false,
	)))
	histogram := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: 1}},
		false, true,
	)))
	tooLargeTimestamp := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: math.MaxInt64}},
		false, false,
	)))
	outOfOrder := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
		[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}},
		[]remoteWriteTestSample{{value: 1, time: 2}, {value: 2, time: 1}},
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
		{name: "missing version", body: valid, headers: map[string]string{"X-Prometheus-Remote-Write-Version": ""}, status: http.StatusBadRequest, message: "Version"},
		{name: "missing user agent", body: valid, headers: map[string]string{fiberHeaderUserAgent: ""}, status: http.StatusBadRequest, message: "User-Agent"},
		{name: "malformed snappy", body: []byte{0xff}, headers: nil, status: http.StatusBadRequest, message: "snappy"},
		{name: "malformed protobuf", body: badProto, headers: nil, status: http.StatusBadRequest, message: "protobuf"},
		{name: "decoded body limit", body: largeDecoded, headers: nil, status: http.StatusRequestEntityTooLarge, message: "100 MiB"},
		{name: "missing metric name", body: noName, headers: nil, status: http.StatusBadRequest, message: "__name__"},
		{name: "valid series before invalid series", body: validThenNoName, headers: nil, status: http.StatusBadRequest, message: "__name__"},
		{name: "unsorted labels", body: unsorted, headers: nil, status: http.StatusBadRequest, message: "sorted"},
		{name: "duplicate label", body: duplicate, headers: nil, status: http.StatusBadRequest, message: "duplicates"},
		{name: "Arc-incompatible metric name", body: colonMetric, headers: nil, status: http.StatusBadRequest, message: "Arc measurement"},
		{name: "timestamp label", body: timeLabel, headers: nil, status: http.StatusBadRequest, message: "time"},
		{name: "value label", body: valueLabel, headers: nil, status: http.StatusBadRequest, message: "value column"},
		{name: "exemplar", body: exemplar, headers: nil, status: http.StatusBadRequest, message: "exemplars"},
		{name: "native histogram", body: histogram, headers: nil, status: http.StatusBadRequest, message: "histograms"},
		{name: "timestamp overflow", body: tooLargeTimestamp, headers: nil, status: http.StatusBadRequest, message: "outside Arc"},
		{name: "out-of-order samples", body: outOfOrder, headers: nil, status: http.StatusBadRequest, message: "ordered"},
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
	_, _, err := prometheusRemoteWriteRecords(request)
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
