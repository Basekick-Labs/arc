package api

import (
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type atomicPressureWAL struct {
	reservations int
	payloads     int
	appendCalls  int
}

func (w *atomicPressureWAL) Append([]map[string]interface{}) error  { w.appendCalls++; return nil }
func (w *atomicPressureWAL) AppendRaw([]byte) error                 { w.appendCalls++; return nil }
func (w *atomicPressureWAL) AppendRawWithMeta(string, []byte) error { w.appendCalls++; return nil }
func (*atomicPressureWAL) Stats() map[string]interface{}            { return nil }
func (*atomicPressureWAL) Close() error                             { return nil }
func (w *atomicPressureWAL) AppendTracked([]map[string]interface{}) ([]string, error) {
	w.appendCalls++
	return nil, nil
}
func (w *atomicPressureWAL) AppendRawWithMetaTracked(string, []byte) ([]string, error) {
	w.appendCalls++
	return nil, nil
}
func (w *atomicPressureWAL) AppendRawWithMetaTrackedReserved(string, []byte, *wal.DiskReservation) ([]string, error) {
	w.appendCalls++
	return nil, nil
}
func (w *atomicPressureWAL) ReserveRawWithMetaBatch(_ string, payloads [][]byte) (*wal.DiskReservation, error) {
	w.reservations++
	w.payloads += len(payloads)
	return nil, wal.ErrWALDiskPressure
}
func (*atomicPressureWAL) MarkFlushed([]string) error { return nil }
func (*atomicPressureWAL) ForgetTracked([]string)     {}

func TestLineProtocolDiskPressureRejectsEntireMultiMeasurementRequest(t *testing.T) {
	walWriter := &atomicPressureWAL{}
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}, nil, zerolog.Nop())
	buffer.SetWAL(walWriter)
	defer buffer.Close()

	app := fiber.New()
	app.Post("/write", NewLineProtocolHandler(buffer, zerolog.Nop()).WriteSimple)
	req := httptest.NewRequest("POST", "/write", strings.NewReader("first value=1 1\nsecond value=2 2\n"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-arc-database", "testdb")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("send Line Protocol request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}
	if walWriter.reservations != 1 || walWriter.payloads != 2 {
		t.Fatalf("batch reservations/payloads = %d/%d, want 1/2", walWriter.reservations, walWriter.payloads)
	}
	if walWriter.appendCalls != 0 {
		t.Fatalf("rejected request attempted %d WAL appends", walWriter.appendCalls)
	}
	if got := buffer.GetStats()["total_records_buffered"]; got != int64(0) {
		t.Fatalf("buffered records after 503 = %v, want 0", got)
	}
}

func TestLineProtocolDiskPressureWithRealWALRejectsBeforeAnyEntry(t *testing.T) {
	walWriter, err := wal.NewWriter(&wal.WriterConfig{
		WALDir:                   t.TempDir(),
		SyncMode:                 wal.SyncModeFdatasync,
		BufferSize:               16,
		DiskHighWatermarkPercent: 99,
		DiskMinFreeMB:            1 << 30,
		Logger:                   zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("create WAL writer: %v", err)
	}
	defer walWriter.Close()

	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}, nil, zerolog.Nop())
	buffer.SetWAL(walWriter)
	defer buffer.Close()

	app := fiber.New()
	app.Post("/write", NewLineProtocolHandler(buffer, zerolog.Nop()).WriteSimple)
	req := httptest.NewRequest("POST", "/write", strings.NewReader("first value=1 1\nsecond value=2 2\n"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-arc-database", "testdb")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("send Line Protocol request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}
	if got := buffer.GetStats()["total_records_buffered"]; got != int64(0) {
		t.Fatalf("buffered records after 503 = %v, want 0", got)
	}
	stats := walWriter.Stats()
	if got := stats["total_entries"]; got != int64(0) {
		t.Fatalf("WAL entries after 503 = %v, want 0", got)
	}
	if got := stats["pending_unflushed"]; got != 0 {
		t.Fatalf("pending WAL entries after 503 = %v, want 0", got)
	}
}

func TestLineProtocolImportDiskPressureRejectsEntireMultiMeasurementRequest(t *testing.T) {
	walWriter := &atomicPressureWAL{}
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}, nil, zerolog.Nop())
	buffer.SetWAL(walWriter)
	defer buffer.Close()

	handler := NewImportHandler(zerolog.Nop())
	handler.SetArrowBuffer(buffer)
	app := fiber.New()
	app.Post("/import/lp", handler.handleLineProtocolImport)
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	file, err := form.CreateFormFile("file", "measurements.lp")
	if err != nil {
		t.Fatalf("create import form file: %v", err)
	}
	if _, err := file.Write([]byte("first value=1 1\nsecond value=2 2\n")); err != nil {
		t.Fatalf("write import form file: %v", err)
	}
	if err := form.Close(); err != nil {
		t.Fatalf("close import form: %v", err)
	}
	req := httptest.NewRequest("POST", "/import/lp?db=testdb", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("send Line Protocol import: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}
	if walWriter.reservations != 1 || walWriter.payloads != 2 {
		t.Fatalf("batch reservations/payloads = %d/%d, want 1/2", walWriter.reservations, walWriter.payloads)
	}
	if walWriter.appendCalls != 0 {
		t.Fatalf("rejected import attempted %d WAL appends", walWriter.appendCalls)
	}
	if got := buffer.GetStats()["total_records_buffered"]; got != int64(0) {
		t.Fatalf("buffered records after 503 = %v, want 0", got)
	}
}

func TestMessagePackDiskPressureRejectsEntireMultiMeasurementRequest(t *testing.T) {
	walWriter := &atomicPressureWAL{}
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}, nil, zerolog.Nop())
	buffer.SetWAL(walWriter)
	defer buffer.Close()

	payload, err := msgpack.Marshal(map[string]interface{}{
		"batch": []interface{}{
			map[string]interface{}{
				"m":      "first",
				"t":      int64(1),
				"fields": map[string]interface{}{"value": int64(1)},
			},
			map[string]interface{}{
				"m":      "second",
				"t":      int64(2),
				"fields": map[string]interface{}{"value": int64(2)},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal MessagePack batch: %v", err)
	}

	handler := NewMsgPackHandler(zerolog.Nop(), buffer, int64(len(payload)+1))
	app := fiber.New()
	app.Post("/api/v1/write/msgpack", handler.writeMsgPack)
	req := httptest.NewRequest("POST", "/api/v1/write/msgpack", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("x-arc-database", "testdb")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("send MessagePack request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}
	if walWriter.reservations != 1 || walWriter.payloads != 2 {
		t.Fatalf("batch reservations/payloads = %d/%d, want 1/2", walWriter.reservations, walWriter.payloads)
	}
	if walWriter.appendCalls != 0 {
		t.Fatalf("rejected request attempted %d WAL appends", walWriter.appendCalls)
	}
	if got := buffer.GetStats()["total_records_buffered"]; got != int64(0) {
		t.Fatalf("buffered records after 503 = %v, want 0", got)
	}
}

func TestImportBufferDiskPressureSetsRetryAfter(t *testing.T) {
	app := fiber.New()
	app.Post("/import/error", func(c *fiber.Ctx) error {
		handler := NewImportHandler(zerolog.Nop())
		return handler.importErrorResponse(c, &importError{
			StatusCode: importBufferStatus(wal.ErrWALDiskPressure),
			Message:    "import rejected (WAL disk pressure)",
			Err:        wal.ErrWALDiskPressure,
		})
	})
	req := httptest.NewRequest("POST", "/import/error", nil)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("send import error request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}
}
