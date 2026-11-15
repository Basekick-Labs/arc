package api

import (
	"errors"
	"fmt"
	"math"
	"mime"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	prometheusRemoteWritePath    = "/api/v1/write/prometheus"
	maxRemoteWritePayloadBytes   = 100 << 20
	maxRemoteWriteColumnCells    = 4_000_000
	maxRemoteWriteTimeSeries     = 100_000
	maxRemoteWriteSamples        = 1_000_000
	maxRemoteWriteLabels         = 1_000_000
	prometheusRemoteWriteVersion = "0.1.0"
	prometheusValueColumn        = "value"
	remoteWriteProtocolName      = "Prometheus remote write"
	// maxRemoteWriteLabelBytes bounds the label bytes a request materialises:
	// a series sends its labels once and every sample row repeats them, so a
	// small body with a long label and many samples would otherwise build
	// gigabytes of strings at flush and in the WAL. A line-protocol body of
	// the same rows carries those bytes itself.
	maxRemoteWriteLabelBytes = maxRemoteWritePayloadBytes
	// maxSnappyRatio is above the largest ratio a Snappy block reaches (a
	// 64-byte copy in a 3-byte tag): a block declaring more is corrupt, and
	// decoding it would allocate the declared length first.
	maxSnappyRatio = 32
)

var (
	errRemoteWriteTooBig            = errors.New("remote write payload exceeds 100 MiB limit")
	errRemoteWriteTooManyCells      = errors.New("remote write request exceeds 4 million column cells")
	errRemoteWriteTooManyLabelBytes = errors.New("remote write request repeats more than 100 MiB of label values across its samples")
	errRemoteWriteTooManySeries     = errors.New("remote write request exceeds 100,000 time series")
	errRemoteWriteTooManySamples    = errors.New("remote write request exceeds 1 million samples")
	errRemoteWriteTooManyLabels     = errors.New("remote write request exceeds 1 million labels")
)

// WritePrometheusRemoteWrite accepts Prometheus Remote Write 1.0 samples.
func (h *LineProtocolHandler) WritePrometheusRemoteWrite(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	h.totalBytes.Add(int64(len(c.Request().Body())))
	defer func() {
		if c.Response().StatusCode() >= fiber.StatusBadRequest {
			h.totalErrors.Add(1)
		}
	}()

	database := c.Query("db", "default")
	if headerDB := c.Get("x-arc-database"); headerDB != "" {
		database = headerDB
	}
	database = strings.Clone(database)
	if !isValidDatabaseName(database) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	if storage.IsWarehouseRootName(database) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": warehouseDatabaseError(database)})
	}

	if status, message := validatePrometheusRemoteWriteHeaders(c); status != 0 {
		return c.Status(status).JSON(fiber.Map{"error": message})
	}

	switch WriteForwardDecision(h.router, c) {
	case ForwardAlreadyForwarded:
		return RespondAlreadyForwarded(c)
	case ForwardToPeer:
		// Built only once the write is forwarded, on the router's lifetime,
		// as handleWrite does (#1629).
		fctx := h.router.Lifetime()
		httpReq, release, err := BuildHTTPRequest(c, fctx)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to build HTTP request for Prometheus remote write forwarding")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to prepare request for forwarding",
			})
		}
		defer release()
		resp, err := h.router.RouteWrite(fctx, httpReq)
		if err == nil {
			return CopyResponse(c, resp)
		}
		if errors.Is(err, cluster.ErrLocalNodeCanHandle) {
			goto localProcessing
		}
		if !errors.Is(err, cluster.ErrWriteForwardUnavailable) && !errors.Is(err, cluster.ErrWriteForwardAmbiguous) {
			h.logger.Error().Err(err).Msg("Failed to route Prometheus remote write request")
		}
		return respondWriteForwardError(c, err)
	}

localProcessing:
	if handled, err := forwardStandbyWrite(h.router, c); handled {
		return err
	}

	request, decodedSize, err := decodePrometheusRemoteWrite(c.Request().Body())
	if err != nil {
		status := fiber.StatusBadRequest
		if isRemoteWriteTooLarge(err) {
			status = fiber.StatusRequestEntityTooLarge
		}
		return c.Status(status).JSON(fiber.Map{"error": err.Error()})
	}

	// A series Arc cannot store is dropped and the others are written, as
	// line protocol drops a point (#1342). Prometheus discards a batch
	// answered 4xx without retrying, so refusing the request for one series
	// lost every other series sent with it.
	records, drops, err := prometheusRemoteWriteRecords(request)
	if err != nil {
		status := fiber.StatusBadRequest
		if isRemoteWriteTooLarge(err) {
			status = fiber.StatusRequestEntityTooLarge
		}
		return c.Status(status).JSON(fiber.Map{"error": err.Error()})
	}

	// In name order: RBAC reads them, and a write that stops part-way says
	// which were written (#1437).
	measurements := make([]string, 0, len(records))
	for measurement := range records {
		measurements = append(measurements, measurement)
	}
	sort.Strings(measurements)
	names := ingest.NewRecordNameFilter(writableKnownColumns(c, h.rbacManager, database, h.buffer.KnownColumns(database)))
	kept := measurements[:0]
	for _, measurement := range measurements {
		if names.Keep(records[measurement]) {
			kept = append(kept, measurement)
		}
	}
	measurements = kept
	drops = append(drops, names.Drops()...)
	dropped := droppedPoints(drops)

	rows := 0
	for _, measurement := range measurements {
		rows += len(records[measurement].Columns["time"])
	}
	if rows == 0 {
		if dropped > 0 {
			logPointDrops(h.logger, "Prometheus remote write", database, drops)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": partialWriteMessage(drops, dropped, false)})
		}
		// An empty request, or one that carries only staleness markers.
		return c.SendStatus(fiber.StatusNoContent)
	}

	if h.rbacManager != nil { // not license-gated; see CheckWritePermissions
		if err := h.checkWritePermissions(c, database, measurements); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
		}
	}

	for _, measurement := range measurements {
		if err := h.buffer.CheckColumnsTextBinary(database, measurement, records[measurement].Columns); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
	}

	// The whole request is admitted once, as handleWrite admits it (#1422),
	// and a failure part-way answers with what was written (#1437).
	writeCtx, release, err := h.buffer.AdmitWholeRequest(c.Context(), int64(decodedSize))
	if err != nil {
		return respondWriteRefused(c, err)
	}
	defer release()

	written, dropped, drops, stop := h.writeLPMeasurements(measurements, rows, dropped, drops, func(m string) (int, error) {
		record := records[m]
		return len(record.Columns["time"]), lpWriteRecord(h.buffer, writeCtx, database, record)
	})
	if stop != nil {
		return h.answerLPStop(c, remoteWriteProtocolName, database, stop, drops)
	}

	m := metrics.Get()
	m.IncIngestRecords(int64(written))
	m.IncIngestBytes(int64(decodedSize))
	m.IncIngestBatches()
	h.totalRecords.Add(int64(written))
	if dropped > 0 {
		logPointDrops(h.logger, "Prometheus remote write", database, drops)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": partialWriteMessage(drops, dropped, true)})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func isRemoteWriteTooLarge(err error) bool {
	return errors.Is(err, errRemoteWriteTooBig) ||
		errors.Is(err, errRemoteWriteTooManyCells) ||
		errors.Is(err, errRemoteWriteTooManyLabelBytes) ||
		errors.Is(err, errRemoteWriteTooManySeries) ||
		errors.Is(err, errRemoteWriteTooManySamples) ||
		errors.Is(err, errRemoteWriteTooManyLabels)
}

func validatePrometheusRemoteWriteHeaders(c *fiber.Ctx) (int, string) {
	encoding, ok := oneRequestHeader(c, fiber.HeaderContentEncoding)
	if !ok || !strings.EqualFold(encoding, "snappy") {
		return fiber.StatusUnsupportedMediaType, "Content-Encoding must be snappy"
	}

	contentType, ok := oneRequestHeader(c, fiber.HeaderContentType)
	if !ok {
		return fiber.StatusUnsupportedMediaType, "Content-Type must be application/x-protobuf"
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "application/x-protobuf") {
		return fiber.StatusUnsupportedMediaType, "Content-Type must be application/x-protobuf"
	}
	// Remote Write 2.0 names its message here; its spec has a receiver
	// answer 415 to a message it does not take.
	if proto, ok := params["proto"]; ok && proto != "prometheus.WriteRequest" {
		return fiber.StatusUnsupportedMediaType, "only Remote Write 1.0 (proto=prometheus.WriteRequest) is supported; set protobuf_message: prometheus.WriteRequest"
	}

	version, ok := oneRequestHeader(c, "X-Prometheus-Remote-Write-Version")
	if !ok || version != prometheusRemoteWriteVersion {
		return fiber.StatusBadRequest, "X-Prometheus-Remote-Write-Version must be 0.1.0"
	}
	userAgent, ok := oneRequestHeader(c, fiber.HeaderUserAgent)
	if !ok || userAgent == "" {
		return fiber.StatusBadRequest, "User-Agent is required"
	}
	return 0, ""
}

func oneRequestHeader(c *fiber.Ctx, name string) (string, bool) {
	var value string
	count := 0
	c.Request().Header.VisitAll(func(key, headerValue []byte) {
		if strings.EqualFold(string(key), name) {
			count++
			value = strings.TrimSpace(string(headerValue))
		}
	})
	return value, count == 1 && value != ""
}

type remoteWriteRequest struct {
	Timeseries []remoteWriteTimeSeries
}

type remoteWriteTimeSeries struct {
	Labels     []remoteWriteLabel
	Samples    []remoteWriteSample
	Exemplars  int
	Histograms int
}

type remoteWriteLabel struct {
	Name  string
	Value string
}

type remoteWriteSample struct {
	Value     float64
	Timestamp int64
}

func decodePrometheusRemoteWrite(rawBody []byte) (*remoteWriteRequest, int, error) {
	if len(rawBody) == 0 {
		return nil, 0, errors.New("empty remote write request body")
	}
	if len(rawBody) > maxRemoteWritePayloadBytes {
		return nil, 0, errRemoteWriteTooBig
	}
	decodedLength, err := snappy.DecodedLen(rawBody)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid snappy block: %w", err)
	}
	if decodedLength > maxRemoteWritePayloadBytes {
		return nil, 0, errRemoteWriteTooBig
	}
	if decodedLength > maxSnappyRatio*len(rawBody)+1024 {
		return nil, 0, fmt.Errorf("invalid snappy block: declares %d bytes from a %d-byte body", decodedLength, len(rawBody))
	}
	decoded, err := snappy.Decode(nil, rawBody)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid snappy block: %w", err)
	}
	request, err := unmarshalRemoteWriteRequest(decoded)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid remote write protobuf: %w", err)
	}
	return request, len(decoded), nil
}

func unmarshalRemoteWriteRequest(data []byte) (*remoteWriteRequest, error) {
	request := &remoteWriteRequest{}
	budget := &remoteWriteParseBudget{}
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if number == 1 && wireType == protowire.BytesType {
			if len(request.Timeseries) >= maxRemoteWriteTimeSeries {
				return nil, errRemoteWriteTooManySeries
			}
			message, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			series, err := unmarshalRemoteWriteTimeSeries(message, budget)
			if err != nil {
				return nil, fmt.Errorf("time series %d: %w", len(request.Timeseries), err)
			}
			request.Timeseries = append(request.Timeseries, *series)
			data = data[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(number, wireType, data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
	}
	return request, nil
}

type remoteWriteParseBudget struct {
	samples int
	labels  int
}

func unmarshalRemoteWriteTimeSeries(data []byte, budget *remoteWriteParseBudget) (*remoteWriteTimeSeries, error) {
	series := &remoteWriteTimeSeries{}
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		switch number {
		case 1:
			if budget.labels >= maxRemoteWriteLabels {
				return nil, errRemoteWriteTooManyLabels
			}
			message, rest, err := consumeRemoteWriteMessage(wireType, data)
			if err != nil {
				return nil, fmt.Errorf("labels: %w", err)
			}
			label, err := unmarshalRemoteWriteLabel(message)
			if err != nil {
				return nil, fmt.Errorf("label %d: %w", len(series.Labels), err)
			}
			series.Labels = append(series.Labels, label)
			budget.labels++
			data = rest
		case 2:
			if budget.samples >= maxRemoteWriteSamples {
				return nil, errRemoteWriteTooManySamples
			}
			message, rest, err := consumeRemoteWriteMessage(wireType, data)
			if err != nil {
				return nil, fmt.Errorf("samples: %w", err)
			}
			sample, err := unmarshalRemoteWriteSample(message)
			if err != nil {
				return nil, fmt.Errorf("sample %d: %w", len(series.Samples), err)
			}
			series.Samples = append(series.Samples, sample)
			budget.samples++
			data = rest
		case 3, 4:
			_, rest, err := consumeRemoteWriteMessage(wireType, data)
			if err != nil {
				return nil, err
			}
			if number == 3 {
				series.Exemplars++
			} else {
				series.Histograms++
			}
			data = rest
		default:
			n = protowire.ConsumeFieldValue(number, wireType, data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return series, nil
}

func unmarshalRemoteWriteLabel(data []byte) (remoteWriteLabel, error) {
	var label remoteWriteLabel
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return label, protowire.ParseError(n)
		}
		data = data[n:]
		switch number {
		case 1, 2:
			value, rest, err := consumeRemoteWriteString(wireType, data)
			if err != nil {
				return label, err
			}
			if number == 1 {
				label.Name = value
			} else {
				label.Value = value
			}
			data = rest
		default:
			n = protowire.ConsumeFieldValue(number, wireType, data)
			if n < 0 {
				return label, protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return label, nil
}

func unmarshalRemoteWriteSample(data []byte) (remoteWriteSample, error) {
	var sample remoteWriteSample
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return sample, protowire.ParseError(n)
		}
		data = data[n:]
		switch number {
		case 1:
			if wireType != protowire.Fixed64Type {
				return sample, errors.New("sample value must use the fixed64 wire type")
			}
			bits, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return sample, protowire.ParseError(n)
			}
			sample.Value = math.Float64frombits(bits)
			data = data[n:]
		case 2:
			if wireType != protowire.VarintType {
				return sample, errors.New("sample timestamp must use the varint wire type")
			}
			timestamp, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return sample, protowire.ParseError(n)
			}
			sample.Timestamp = int64(timestamp)
			data = data[n:]
		default:
			n = protowire.ConsumeFieldValue(number, wireType, data)
			if n < 0 {
				return sample, protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return sample, nil
}

func consumeRemoteWriteMessage(wireType protowire.Type, data []byte) ([]byte, []byte, error) {
	if wireType != protowire.BytesType {
		return nil, nil, errors.New("message field must use the bytes wire type")
	}
	message, n := protowire.ConsumeBytes(data)
	if n < 0 {
		return nil, nil, protowire.ParseError(n)
	}
	return message, data[n:], nil
}

func consumeRemoteWriteString(wireType protowire.Type, data []byte) (string, []byte, error) {
	message, rest, err := consumeRemoteWriteMessage(wireType, data)
	if err != nil {
		return "", nil, err
	}
	return string(message), rest, nil
}

// prometheusStaleNaN is the value Prometheus sends as a staleness marker: the
// series stopped (its target went away, or its rule stopped producing it). It
// is a signal, not a sample. Stored, it is a NaN in value, and avg or sum over
// the series is NaN from then on. Any other NaN is a sample and is kept.
const prometheusStaleNaN uint64 = 0x7ff0000000000002

// Reasons a series is dropped, for the partial-write answer.
const (
	remoteWriteDropNoName     = "series has no __name__ label"
	remoteWriteDropEmptyLabel = "series has a label with an empty name"
	remoteWriteDropDupLabel   = "series has the same label twice"
	remoteWriteDropValueLabel = "label conflicts with the sample column \"value\""
	remoteWriteDropLabelUTF8  = "label value is not valid UTF-8"
	remoteWriteDropUnderscore = "label names starting with _ are not stored by Arc"
	remoteWriteDropTimestamp  = "sample timestamp is outside Arc's supported range"
	remoteWriteDropHistograms = "native histograms are not supported"
	remoteWriteDropExemplars  = "exemplars are not supported"
)

// prometheusRemoteWriteRecords maps a request to one columnar record per
// measurement. A series Arc cannot store is left out and listed in drops;
// its exemplars and native histograms are listed too, and its float samples
// are kept. Staleness markers are skipped. err is set only when the request
// as a whole is over a limit.
func prometheusRemoteWriteRecords(request *remoteWriteRequest) (map[string]*models.ColumnarRecord, []ingest.LPDrop, error) {
	records := make(map[string]*models.ColumnarRecord)
	tagNames := make(map[string]map[string]struct{})
	var drops []ingest.LPDrop
	dropAt := make(map[[3]string]int)
	drop := func(measurement, column, reason string, points int) {
		key := [3]string{measurement, column, reason}
		if i, ok := dropAt[key]; ok {
			drops[i].Points += points
			return
		}
		dropAt[key] = len(drops)
		drops = append(drops, ingest.LPDrop{Measurement: measurement, Column: column, Reason: reason, Points: points})
	}
	columnCells := 0
	labelBytes := 0

	for _, series := range request.Timeseries {
		measurement, tags, column, reason := prometheusSeriesLabels(series.Labels)
		if series.Histograms > 0 {
			drop(measurement, "", remoteWriteDropHistograms, series.Histograms)
		}
		if series.Exemplars > 0 {
			drop(measurement, "", remoteWriteDropExemplars, series.Exemplars)
		}
		samples := 0
		for _, sample := range series.Samples {
			if math.Float64bits(sample.Value) != prometheusStaleNaN {
				samples++
			}
		}
		if samples == 0 {
			continue
		}
		if reason != "" {
			drop(measurement, column, reason, samples)
			continue
		}
		inRange := true
		for _, sample := range series.Samples {
			if sample.Timestamp < math.MinInt64/1000 || sample.Timestamp > math.MaxInt64/1000 {
				inRange = false
				break
			}
		}
		if !inRange {
			drop(measurement, "", remoteWriteDropTimestamp, samples)
			continue
		}

		record := records[measurement]
		if record == nil {
			record = &models.ColumnarRecord{
				Measurement: measurement,
				Columnar:    true,
				Columns: map[string][]interface{}{
					"time":                {},
					prometheusValueColumn: {},
				},
			}
			records[measurement] = record
			tagNames[measurement] = make(map[string]struct{})
		}

		rowCount := len(record.Columns["time"])
		columnCount := len(record.Columns)
		newColumns := 0
		for name := range tags {
			if _, exists := record.Columns[name]; !exists {
				newColumns++
			}
		}
		newCellCount := (rowCount+samples)*(columnCount+newColumns) - rowCount*columnCount
		if newCellCount > maxRemoteWriteColumnCells-columnCells {
			return nil, nil, errRemoteWriteTooManyCells
		}
		columnCells += newCellCount
		seriesLabelBytes := 0
		for _, value := range tags {
			seriesLabelBytes += len(value)
		}
		if seriesLabelBytes > 0 && samples > (maxRemoteWriteLabelBytes-labelBytes)/seriesLabelBytes {
			return nil, nil, errRemoteWriteTooManyLabelBytes
		}
		labelBytes += samples * seriesLabelBytes

		for name := range tags {
			if _, exists := record.Columns[name]; !exists {
				record.Columns[name] = make([]interface{}, rowCount)
			}
			tagNames[measurement][name] = struct{}{}
		}
		for _, sample := range series.Samples {
			if math.Float64bits(sample.Value) == prometheusStaleNaN {
				continue
			}
			record.Columns["time"] = append(record.Columns["time"], sample.Timestamp*1000)
			record.Columns[prometheusValueColumn] = append(record.Columns[prometheusValueColumn], sample.Value)
			for name, values := range record.Columns {
				if name == "time" || name == prometheusValueColumn {
					continue
				}
				if value, exists := tags[name]; exists {
					record.Columns[name] = append(values, value)
				} else {
					record.Columns[name] = append(values, nil)
				}
			}
		}
	}

	for measurement, record := range records {
		for name := range tagNames[measurement] {
			record.TagColumns = append(record.TagColumns, name)
		}
		sort.Strings(record.TagColumns)
	}
	return records, drops, nil
}

// prometheusSeriesLabels is a series' measurement and tags. When the series
// cannot be stored, reason says why (and column names the label at fault);
// measurement is then the best name for the answer.
func prometheusSeriesLabels(labels []remoteWriteLabel) (measurement string, tags map[string]string, column, reason string) {
	metricName := ""
	tags = make(map[string]string, len(labels))
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if label.Name == "__name__" {
			metricName = label.Value
		}
	}
	measurement = prometheusMeasurementName(metricName)
	if metricName == "" {
		return "", nil, "", remoteWriteDropNoName
	}
	if !isValidMeasurementName(measurement) {
		return ingest.QuoteColumnName(metricName), nil, "", "metric name is not a valid Arc measurement name (letters, digits and _, starting with a letter, up to 128 characters)"
	}
	for _, label := range labels {
		if label.Name == "" {
			return measurement, nil, "", remoteWriteDropEmptyLabel
		}
		if _, dup := seen[label.Name]; dup {
			return measurement, nil, label.Name, remoteWriteDropDupLabel
		}
		seen[label.Name] = struct{}{}
		if label.Name == "__name__" || label.Value == "" {
			// An empty label value is the same as no label (Prometheus data model).
			continue
		}
		if problem := ingest.ColumnNameProblem(label.Name); problem != "" {
			return measurement, nil, label.Name, problem
		}
		if label.Name[0] == '_' {
			// The buffer leaves such columns out (unstoredColumn): stored,
			// the label would vanish and its series merge with another.
			return measurement, nil, label.Name, remoteWriteDropUnderscore
		}
		if ingest.FoldColumnName(label.Name) == ingest.FoldColumnName(prometheusValueColumn) {
			return measurement, nil, label.Name, remoteWriteDropValueLabel
		}
		if !utf8.ValidString(label.Value) {
			return measurement, nil, label.Name, remoteWriteDropLabelUTF8
		}
		tags[label.Name] = label.Value
	}
	return measurement, tags, "", ""
}

// prometheusMeasurementName is the measurement for a metric name: every
// character other than an ASCII letter, digit or _ becomes _, the rule
// arc-opentelemetry-exporter applies to OTLP metric names, so a recording
// rule's job:http_requests:rate5m is stored as job_http_requests_rate5m.
func prometheusMeasurementName(metric string) string {
	for i := 0; i < len(metric); i++ {
		if !prometheusMeasurementByte(metric[i]) {
			return strings.Map(func(r rune) rune {
				if r < utf8.RuneSelf && prometheusMeasurementByte(byte(r)) {
					return r
				}
				return '_'
			}, metric)
		}
	}
	return metric
}

func prometheusMeasurementByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
