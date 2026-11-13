package api

import (
	"errors"
	"fmt"
	"math"
	"mime"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
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
)

var (
	prometheusMetricName         = regexp.MustCompile("^[a-zA-Z_:][a-zA-Z0-9_:]*$")
	prometheusLabelName          = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")
	errRemoteWriteTooBig         = errors.New("remote write payload exceeds 100 MiB limit")
	errRemoteWriteTooManyCells   = errors.New("remote write request exceeds 4 million column cells")
	errRemoteWriteTooManySeries  = errors.New("remote write request exceeds 100,000 time series")
	errRemoteWriteTooManySamples = errors.New("remote write request exceeds 1 million samples")
	errRemoteWriteTooManyLabels  = errors.New("remote write request exceeds 1 million labels")
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

	records, sampleCount, err := prometheusRemoteWriteRecords(request)
	if err != nil {
		status := fiber.StatusBadRequest
		if isRemoteWriteTooLarge(err) {
			status = fiber.StatusRequestEntityTooLarge
		}
		return c.Status(status).JSON(fiber.Map{"error": err.Error()})
	}
	if sampleCount == 0 {
		return c.SendStatus(fiber.StatusNoContent)
	}

	measurements := make([]string, 0, len(records))
	for measurement := range records {
		measurements = append(measurements, measurement)
	}
	sort.Strings(measurements)

	names := ingest.NewRecordNameFilter(writableKnownColumns(c, h.rbacManager, database, h.buffer.KnownColumns(database)))
	for _, measurement := range measurements {
		if !names.Keep(records[measurement]) {
			drops := names.Drops()
			if len(drops) == 0 {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid Prometheus label names"})
			}
			drop := drops[len(drops)-1]
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("invalid label name %q for measurement %q: %s", ingest.TruncateColumnName(drop.Column), drop.Measurement, drop.Reason),
			})
		}
	}

	if h.rbacManager != nil {
		if err := h.checkWritePermissions(c, database, measurements); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
		}
	}

	for _, measurement := range measurements {
		if err := h.buffer.CheckColumnsTextBinary(database, measurement, records[measurement].Columns); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
	}

	writeCtx, release, err := h.buffer.AdmitRequest(c.Context(), int64(decodedSize))
	if err != nil {
		return respondWriteRefused(c, err)
	}
	defer release()

	for _, measurement := range measurements {
		record := records[measurement]
		if err := h.buffer.WriteColumnarRecord(writeCtx, database, record); err != nil {
			if ingest.IsWriteRefusal(err) {
				return respondWriteRefused(c, err)
			}
			if errors.Is(err, ingest.ErrColumnTypeChange) {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
			}
			if errors.Is(err, ingest.ErrBufferClosing) || errors.Is(err, ingest.ErrSchemaChurnExceeded) {
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "Write rejected: " + err.Error()})
			}
			metrics.Get().IncIngestErrors()
			h.logger.Error().Err(err).
				Str("database", database).
				Str("measurement", measurement).
				Int("samples", len(record.Columns["time"])).
				Msg("Failed to write Prometheus remote write samples to Arrow buffer")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Write failed: " + err.Error()})
		}
	}

	m := metrics.Get()
	m.IncIngestRecords(int64(sampleCount))
	m.IncIngestBytes(int64(decodedSize))
	m.IncIngestBatches()
	h.totalRecords.Add(int64(sampleCount))
	return c.SendStatus(fiber.StatusNoContent)
}

func isRemoteWriteTooLarge(err error) bool {
	return errors.Is(err, errRemoteWriteTooBig) ||
		errors.Is(err, errRemoteWriteTooManyCells) ||
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
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "application/x-protobuf") {
		return fiber.StatusUnsupportedMediaType, "Content-Type must be application/x-protobuf"
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
	Labels        []remoteWriteLabel
	Samples       []remoteWriteSample
	HasExemplars  bool
	HasHistograms bool
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
				series.HasExemplars = true
			} else {
				series.HasHistograms = true
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

func prometheusRemoteWriteRecords(request *remoteWriteRequest) (map[string]*models.ColumnarRecord, int, error) {
	records := make(map[string]*models.ColumnarRecord)
	tagNames := make(map[string]map[string]struct{})
	sampleCount := 0
	columnCells := 0

	for _, series := range request.Timeseries {
		if series.HasHistograms {
			return nil, 0, errors.New("native histograms are not supported")
		}
		if series.HasExemplars {
			return nil, 0, errors.New("exemplars are not supported")
		}
		if len(series.Samples) == 0 {
			continue
		}

		measurement, tags, err := prometheusSeriesLabels(series.Labels)
		if err != nil {
			return nil, 0, err
		}
		record := records[measurement]
		if record == nil {
			record = &models.ColumnarRecord{
				Measurement: measurement,
				Columnar:    true,
				Columns: map[string][]interface{}{
					"time":  {},
					"value": {},
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
		newCellCount := (rowCount+len(series.Samples))*(columnCount+newColumns) - rowCount*columnCount
		if newCellCount > maxRemoteWriteColumnCells-columnCells {
			return nil, 0, errRemoteWriteTooManyCells
		}
		columnCells += newCellCount

		for i, sample := range series.Samples {
			if i > 0 && sample.Timestamp < series.Samples[i-1].Timestamp {
				return nil, 0, fmt.Errorf("samples for measurement %q are not ordered by timestamp", measurement)
			}
			if sample.Timestamp < math.MinInt64/1000 || sample.Timestamp > math.MaxInt64/1000 {
				return nil, 0, fmt.Errorf("sample timestamp %d ms is outside Arc's supported range", sample.Timestamp)
			}

			row := len(record.Columns["time"])
			for name := range tags {
				if _, exists := record.Columns[name]; !exists {
					record.Columns[name] = make([]interface{}, row)
				}
				tagNames[measurement][name] = struct{}{}
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
			sampleCount++
		}
	}

	for measurement, record := range records {
		for name := range tagNames[measurement] {
			record.TagColumns = append(record.TagColumns, name)
		}
		sort.Strings(record.TagColumns)
	}
	return records, sampleCount, nil
}

func prometheusSeriesLabels(labels []remoteWriteLabel) (string, map[string]string, error) {
	metricName := ""
	tags := make(map[string]string, len(labels))
	previous := ""
	for i, label := range labels {
		if label.Name == "" || label.Value == "" {
			return "", nil, errors.New("Prometheus labels must have non-empty names and values")
		}
		if !prometheusLabelName.MatchString(label.Name) {
			return "", nil, fmt.Errorf("invalid Prometheus label name %q", ingest.TruncateColumnName(label.Name))
		}
		if !utf8.ValidString(label.Value) {
			return "", nil, fmt.Errorf("Prometheus label %q has a value that is not valid UTF-8", ingest.TruncateColumnName(label.Name))
		}
		if i > 0 && label.Name <= previous {
			return "", nil, errors.New("Prometheus labels must be sorted by name with no duplicates")
		}
		previous = label.Name

		if label.Name == "__name__" {
			metricName = label.Value
			continue
		}
		if problem := ingest.ColumnNameProblem(label.Name); problem != "" {
			return "", nil, fmt.Errorf("invalid Prometheus label name %q: %s", ingest.TruncateColumnName(label.Name), problem)
		}
		if ingest.FoldColumnName(label.Name) == ingest.FoldColumnName(prometheusValueColumn) {
			return "", nil, fmt.Errorf("Prometheus label name %q conflicts with Arc's sample value column %q", ingest.TruncateColumnName(label.Name), prometheusValueColumn)
		}
		tags[label.Name] = label.Value
	}

	if metricName == "" {
		return "", nil, errors.New("Prometheus time series must include a non-empty __name__ label")
	}
	if !prometheusMetricName.MatchString(metricName) {
		return "", nil, fmt.Errorf("invalid Prometheus metric name %q", ingest.TruncateColumnName(metricName))
	}
	if !isValidMeasurementName(metricName) {
		return "", nil, fmt.Errorf("Prometheus metric name %q is not a valid Arc measurement name", ingest.TruncateColumnName(metricName))
	}
	return metricName, tags, nil
}
