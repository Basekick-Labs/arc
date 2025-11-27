package ingest

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// schemaCacheEntry holds a cached schema with LRU tracking
type schemaCacheEntry struct {
	schema     *arrow.Schema
	key        string
	prev, next *schemaCacheEntry
}

// schemaLRUCache is a thread-safe LRU cache for Arrow schemas
type schemaLRUCache struct {
	capacity int
	cache    map[string]*schemaCacheEntry
	head     *schemaCacheEntry // Most recently used
	tail     *schemaCacheEntry // Least recently used
	mu       sync.RWMutex
	hits     int64
	misses   int64
}

// newSchemaLRUCache creates a new LRU cache with given capacity
func newSchemaLRUCache(capacity int) *schemaLRUCache {
	return &schemaLRUCache{
		capacity: capacity,
		cache:    make(map[string]*schemaCacheEntry),
	}
}

// get retrieves a schema from cache, returns nil if not found
func (c *schemaLRUCache) get(key string) *arrow.Schema {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.cache[key]
	if !ok {
		c.misses++
		return nil
	}

	// Move to front (most recently used)
	c.moveToFront(entry)
	c.hits++
	return entry.schema
}

// set adds or updates a schema in cache
func (c *schemaLRUCache) set(key string, schema *arrow.Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if entry, ok := c.cache[key]; ok {
		entry.schema = schema
		c.moveToFront(entry)
		return
	}

	// Create new entry
	entry := &schemaCacheEntry{
		schema: schema,
		key:    key,
	}

	// Add to cache
	c.cache[key] = entry
	c.addToFront(entry)

	// Evict if over capacity
	if len(c.cache) > c.capacity {
		c.evictLRU()
	}
}

// moveToFront moves an entry to the front of the list
func (c *schemaLRUCache) moveToFront(entry *schemaCacheEntry) {
	if entry == c.head {
		return // Already at front
	}

	// Remove from current position
	c.removeEntry(entry)

	// Add to front
	c.addToFront(entry)
}

// addToFront adds an entry to the front of the list
func (c *schemaLRUCache) addToFront(entry *schemaCacheEntry) {
	entry.prev = nil
	entry.next = c.head

	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

// removeEntry removes an entry from the list
func (c *schemaLRUCache) removeEntry(entry *schemaCacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
}

// evictLRU removes the least recently used entry
func (c *schemaLRUCache) evictLRU() {
	if c.tail == nil {
		return
	}

	// Remove from cache map
	delete(c.cache, c.tail.key)

	// Remove from list
	c.removeEntry(c.tail)
}

// stats returns cache statistics
func (c *schemaLRUCache) stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(c.hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"capacity":  c.capacity,
		"size":      len(c.cache),
		"hits":      c.hits,
		"misses":    c.misses,
		"hit_rate":  hitRate,
	}
}

// ArrowWriter handles Arrow schema inference and Parquet writing
type ArrowWriter struct {
	compression     compress.Compression
	useDictionary   bool
	writeStatistics bool
	dataPageVersion string

	// LRU Schema cache (measurement -> schema) with bounded size
	schemaCache *schemaLRUCache

	logger zerolog.Logger
}

// NewArrowWriter creates a new Arrow writer
func NewArrowWriter(cfg *config.IngestConfig, logger zerolog.Logger) *ArrowWriter {
	// Parse compression
	var comp compress.Compression
	switch cfg.Compression {
	case "gzip":
		comp = compress.Codecs.Gzip
	case "zstd":
		comp = compress.Codecs.Zstd
	case "snappy":
		comp = compress.Codecs.Snappy
	default:
		comp = compress.Codecs.Snappy
	}

	// Schema cache capacity - 1000 schemas is ~100-200KB memory
	// Most deployments have <100 unique measurement/schema combinations
	const schemaCacheCapacity = 1000

	return &ArrowWriter{
		compression:     comp,
		useDictionary:   cfg.UseDictionary,
		writeStatistics: cfg.WriteStatistics,
		dataPageVersion: cfg.DataPageVersion,
		schemaCache:     newSchemaLRUCache(schemaCacheCapacity),
		logger:          logger.With().Str("component", "arrow-writer").Logger(),
	}
}

// getSchema gets or infers Arrow schema for columnar data (LRU cached per measurement)
func (w *ArrowWriter) getSchema(measurement string, columns map[string]interface{}) (*arrow.Schema, error) {
	// Create cache key from column names and types
	var colNames []string
	var typeNames []string

	for name := range columns {
		if name[0] == '_' {
			continue // Skip internal columns
		}
		colNames = append(colNames, name)
	}

	// Get type signatures
	for _, name := range colNames {
		col := columns[name]
		switch col.(type) {
		case []int64:
			if name == "time" {
				typeNames = append(typeNames, "timestamp")
			} else {
				typeNames = append(typeNames, "int64")
			}
		case []float64:
			typeNames = append(typeNames, "float64")
		case []string:
			typeNames = append(typeNames, "string")
		case []bool:
			typeNames = append(typeNames, "bool")
		default:
			typeNames = append(typeNames, "unknown")
		}
	}

	// Create cache key
	cacheKey := fmt.Sprintf("%s:%v:%v", measurement, colNames, typeNames)

	// Check LRU cache
	if schema := w.schemaCache.get(cacheKey); schema != nil {
		return schema, nil
	}

	// Cache miss - infer schema
	schema, err := w.inferSchema(columns)
	if err != nil {
		return nil, err
	}

	// Store in LRU cache
	w.schemaCache.set(cacheKey, schema)

	w.logger.Debug().
		Str("measurement", measurement).
		Str("cache_key", cacheKey).
		Msg("Schema cache miss, inferred and cached")

	return schema, nil
}

// inferSchema infers Arrow schema from columnar data
func (w *ArrowWriter) inferSchema(columns map[string]interface{}) (*arrow.Schema, error) {
	var fields []arrow.Field

	for name, col := range columns {
		// Skip internal metadata columns
		if name[0] == '_' {
			continue
		}

		var arrowType arrow.DataType

		switch arr := col.(type) {
		case []int64:
			// Special case: time column uses timestamp type
			if name == "time" {
				arrowType = arrow.FixedWidthTypes.Timestamp_us
			} else {
				arrowType = arrow.PrimitiveTypes.Int64
			}
		case []float64:
			arrowType = arrow.PrimitiveTypes.Float64
		case []string:
			arrowType = arrow.BinaryTypes.String
		case []bool:
			arrowType = arrow.FixedWidthTypes.Boolean
		default:
			return nil, fmt.Errorf("unsupported column type for column %s: %T", name, arr)
		}

		fields = append(fields, arrow.Field{Name: name, Type: arrowType, Nullable: true})
	}

	return arrow.NewSchema(fields, nil), nil
}

// WriteParquetColumnar writes columnar data directly to Parquet (zero-copy path)
func (w *ArrowWriter) WriteParquetColumnar(ctx context.Context, measurement string, columns map[string]interface{}) ([]byte, error) {
	// Get or infer schema (with caching)
	schema, err := w.getSchema(measurement, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Create Arrow arrays from columns
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(schema.Fields()))
	arrays := make([]arrow.Array, len(schema.Fields()))

	// CRITICAL: Release both builders and arrays to prevent memory leak
	defer func() {
		for _, builder := range builders {
			if builder != nil {
				builder.Release()
			}
		}
		for _, arr := range arrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	// Build arrays
	for i, field := range schema.Fields() {
		col, ok := columns[field.Name]
		if !ok {
			return nil, fmt.Errorf("column %s not found in data", field.Name)
		}

		switch field.Type.ID() {
		case arrow.INT64:
			builder := array.NewInt64Builder(mem)
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				builder.AppendValues(intCol, nil)
			} else {
				return nil, fmt.Errorf("column %s: expected []int64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.TIMESTAMP:
			builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				// Convert int64 microseconds to arrow.Timestamp
				tsValues := make([]arrow.Timestamp, len(intCol))
				for j, v := range intCol {
					tsValues[j] = arrow.Timestamp(v)
				}
				builder.AppendValues(tsValues, nil)
			} else {
				return nil, fmt.Errorf("column %s: expected []int64 for timestamp, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(mem)
			builders[i] = builder
			if floatCol, ok := col.([]float64); ok {
				builder.AppendValues(floatCol, nil)
			} else {
				return nil, fmt.Errorf("column %s: expected []float64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.STRING:
			builder := array.NewStringBuilder(mem)
			builders[i] = builder
			if strCol, ok := col.([]string); ok {
				builder.AppendValues(strCol, nil)
			} else {
				return nil, fmt.Errorf("column %s: expected []string, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.BOOL:
			builder := array.NewBooleanBuilder(mem)
			builders[i] = builder
			if boolCol, ok := col.([]bool); ok {
				builder.AppendValues(boolCol, nil)
			} else {
				return nil, fmt.Errorf("column %s: expected []bool, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		default:
			return nil, fmt.Errorf("unsupported Arrow type for column %s: %s", field.Name, field.Type.Name())
		}
	}

	return w.writeRecordToParquet(schema, arrays)
}

// WriteParquetFromInterface writes columnar data directly from []interface{} to Parquet
// SINGLE-PASS OPTIMIZATION: Skips convertColumnsToTyped() - builds Arrow arrays directly
// This reduces iterations from 2 to 1, improving latency by ~30-40%
func (w *ArrowWriter) WriteParquetFromInterface(ctx context.Context, measurement string, columns map[string][]interface{}) ([]byte, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns provided")
	}

	// Get number of records from first column
	var numRows int
	for _, col := range columns {
		numRows = len(col)
		break
	}
	if numRows == 0 {
		return nil, fmt.Errorf("empty columns")
	}

	// Get or infer schema from interface columns
	schema, err := w.getSchemaFromInterface(measurement, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Create Arrow arrays from columns - SINGLE PASS
	mem := memory.NewGoAllocator()
	arrays := make([]arrow.Array, len(schema.Fields()))

	defer func() {
		for _, arr := range arrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	// Build arrays directly from []interface{} - NO pre-conversion!
	for i, field := range schema.Fields() {
		col, ok := columns[field.Name]
		if !ok {
			return nil, fmt.Errorf("column %s not found in data", field.Name)
		}

		arr, err := w.buildArrayFromInterface(mem, field, col)
		if err != nil {
			return nil, fmt.Errorf("failed to build array for column %s: %w", field.Name, err)
		}
		arrays[i] = arr
	}

	return w.writeRecordToParquet(schema, arrays)
}

// buildArrayFromInterface builds an Arrow array directly from []interface{}
// SINGLE-PASS: Iterates once while building, no pre-conversion needed
// IMPORTANT: Caller must call Release() on the returned array when done
func (w *ArrowWriter) buildArrayFromInterface(mem memory.Allocator, field arrow.Field, col []interface{}) (arrow.Array, error) {
	switch field.Type.ID() {
	case arrow.INT64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release() // CRITICAL: Release builder to prevent memory leak
		builder.Reserve(len(col))
		for _, v := range col {
			if v == nil {
				builder.AppendNull()
				continue
			}
			switch val := v.(type) {
			case int64:
				builder.Append(val)
			case int:
				builder.Append(int64(val))
			case int32:
				builder.Append(int64(val))
			case uint64:
				builder.Append(int64(val))
			case uint:
				builder.Append(int64(val))
			case uint32:
				builder.Append(int64(val))
			case float64:
				builder.Append(int64(val))
			default:
				return nil, fmt.Errorf("cannot convert %T to int64", v)
			}
		}
		return builder.NewArray(), nil

	case arrow.TIMESTAMP:
		builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
		defer builder.Release() // CRITICAL: Release builder to prevent memory leak
		builder.Reserve(len(col))
		for _, v := range col {
			if v == nil {
				builder.AppendNull()
				continue
			}
			switch val := v.(type) {
			case int64:
				builder.Append(arrow.Timestamp(val))
			case int:
				builder.Append(arrow.Timestamp(val))
			case int32:
				builder.Append(arrow.Timestamp(val))
			case uint64:
				builder.Append(arrow.Timestamp(val))
			case float64:
				builder.Append(arrow.Timestamp(int64(val)))
			default:
				return nil, fmt.Errorf("cannot convert %T to timestamp", v)
			}
		}
		return builder.NewArray(), nil

	case arrow.FLOAT64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release() // CRITICAL: Release builder to prevent memory leak
		builder.Reserve(len(col))
		for _, v := range col {
			if v == nil {
				builder.AppendNull()
				continue
			}
			switch val := v.(type) {
			case float64:
				builder.Append(val)
			case float32:
				builder.Append(float64(val))
			case int64:
				builder.Append(float64(val))
			case int:
				builder.Append(float64(val))
			default:
				return nil, fmt.Errorf("cannot convert %T to float64", v)
			}
		}
		return builder.NewArray(), nil

	case arrow.STRING:
		builder := array.NewStringBuilder(mem)
		defer builder.Release() // CRITICAL: Release builder to prevent memory leak
		builder.Reserve(len(col))
		for _, v := range col {
			if v == nil {
				builder.AppendNull()
				continue
			}
			if str, ok := v.(string); ok {
				builder.Append(str)
			} else {
				return nil, fmt.Errorf("cannot convert %T to string", v)
			}
		}
		return builder.NewArray(), nil

	case arrow.BOOL:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release() // CRITICAL: Release builder to prevent memory leak
		builder.Reserve(len(col))
		for _, v := range col {
			if v == nil {
				builder.AppendNull()
				continue
			}
			if b, ok := v.(bool); ok {
				builder.Append(b)
			} else {
				return nil, fmt.Errorf("cannot convert %T to bool", v)
			}
		}
		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", field.Type.Name())
	}
}

// getSchemaFromInterface infers schema from []interface{} columns (LRU cached)
func (w *ArrowWriter) getSchemaFromInterface(measurement string, columns map[string][]interface{}) (*arrow.Schema, error) {
	// Check LRU cache first
	if schema := w.schemaCache.get(measurement); schema != nil {
		return schema, nil
	}

	// Infer schema from data
	fields := make([]arrow.Field, 0, len(columns))

	// Process columns in consistent order (time first, then alphabetical)
	colNames := make([]string, 0, len(columns))
	for name := range columns {
		colNames = append(colNames, name)
	}

	// Sort: time first, then alphabetical
	for i := 0; i < len(colNames); i++ {
		for j := i + 1; j < len(colNames); j++ {
			swapI := colNames[i]
			swapJ := colNames[j]
			if swapI == "time" {
				continue
			}
			if swapJ == "time" || swapJ < swapI {
				colNames[i], colNames[j] = colNames[j], colNames[i]
			}
		}
	}

	for _, name := range colNames {
		col := columns[name]
		if len(col) == 0 {
			continue
		}

		// Find first non-nil value for type inference
		var firstVal interface{}
		for _, v := range col {
			if v != nil {
				firstVal = v
				break
			}
		}
		if firstVal == nil {
			continue // Skip all-nil columns
		}

		// Infer Arrow type
		var arrowType arrow.DataType
		if name == "time" {
			arrowType = arrow.FixedWidthTypes.Timestamp_us
		} else {
			switch firstVal.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				arrowType = arrow.PrimitiveTypes.Int64
			case float32, float64:
				arrowType = arrow.PrimitiveTypes.Float64
			case string:
				arrowType = arrow.BinaryTypes.String
			case bool:
				arrowType = arrow.FixedWidthTypes.Boolean
			default:
				return nil, fmt.Errorf("unsupported type for column %s: %T", name, firstVal)
			}
		}

		fields = append(fields, arrow.Field{Name: name, Type: arrowType, Nullable: true})
	}

	schema := arrow.NewSchema(fields, nil)

	// Store in LRU cache
	w.schemaCache.set(measurement, schema)

	return schema, nil
}

// writeRecordToParquet writes Arrow arrays to Parquet bytes
func (w *ArrowWriter) writeRecordToParquet(schema *arrow.Schema, arrays []arrow.Array) ([]byte, error) {
	// Create record batch
	record := array.NewRecord(schema, arrays, -1)
	defer record.Release()

	// Write to Parquet
	var buf bytes.Buffer

	// Configure Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(w.compression),
		parquet.WithDictionaryDefault(w.useDictionary),
		parquet.WithStats(w.writeStatistics),
	)

	// Set data page version
	if w.dataPageVersion == "2.0" {
		writerProps = parquet.NewWriterProperties(
			parquet.WithCompression(w.compression),
			parquet.WithDictionaryDefault(w.useDictionary),
			parquet.WithStats(w.writeStatistics),
			parquet.WithDataPageVersion(parquet.DataPageV2),
		)
	}

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		writerProps,
		arrowProps,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Write record batch
	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write record batch: %w", err)
	}

	// Close writer
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	w.logger.Debug().
		Int("columns", len(schema.Fields())).
		Int("rows", int(record.NumRows())).
		Int("size", buf.Len()).
		Msg("Wrote Parquet file")

	return buf.Bytes(), nil
}

// bufferShard represents a single shard of the buffer map with its own lock
type bufferShard struct {
	buffers            map[string][]interface{}
	bufferStartTimes   map[string]time.Time
	bufferRecordCounts map[string]int
	mu                 sync.RWMutex
}

// flushTask represents a flush operation to be executed by workers
type flushTask struct {
	ctx         context.Context
	bufferKey   string
	database    string
	measurement string
	records     []interface{}
	recordCount int
}

// WALWriter interface for Write-Ahead Log support
type WALWriter interface {
	Append(records []map[string]interface{}) error
	AppendRaw(payload []byte) error // Zero-copy: write raw msgpack bytes directly
	Stats() map[string]interface{}
	Close() error
}

// ArrowBuffer manages buffering and periodic flushing of Arrow data
// Uses lock sharding to reduce contention across concurrent writes
type ArrowBuffer struct {
	config  *config.IngestConfig
	storage storage.Backend
	writer  *ArrowWriter

	// Optional WAL for durability
	wal WALWriter

	// OPTIMIZATION: Shard buffers to reduce lock contention
	// With 32 shards, each shard handles ~1/32 of measurements
	// This allows 32 concurrent writes to different measurements
	shards     [32]*bufferShard
	shardCount uint32

	// Background flush
	ctx        context.Context
	cancel     context.CancelFunc
	flushTimer *time.Ticker
	wg         sync.WaitGroup

	// OPTIMIZATION: Worker pool for bounded flush concurrency
	// Prevents goroutine explosion under sustained load
	flushQueue   chan flushTask
	flushWorkers int

	// Metrics (using atomic operations to avoid lock contention)
	totalRecordsBuffered atomic.Int64
	totalRecordsWritten  atomic.Int64
	totalFlushes         atomic.Int64
	totalErrors          atomic.Int64
	queueDepth           atomic.Int64 // Current flush queue depth

	logger zerolog.Logger
}

// getShard returns the shard for a given buffer key using FNV-1a hash
func (b *ArrowBuffer) getShard(bufferKey string) *bufferShard {
	// FNV-1a hash (fast, good distribution)
	hash := uint32(2166136261)
	for i := 0; i < len(bufferKey); i++ {
		hash ^= uint32(bufferKey[i])
		hash *= 16777619
	}
	return b.shards[hash%b.shardCount]
}

// NewArrowBuffer creates a new Arrow buffer with automatic flushing
func NewArrowBuffer(cfg *config.IngestConfig, storage storage.Backend, logger zerolog.Logger) *ArrowBuffer {
	ctx, cancel := context.WithCancel(context.Background())

	// OPTIMIZATION: Worker pool size (10-20 workers optimal for I/O bound tasks)
	flushWorkers := 16
	queueSize := 100 // Buffered channel for burst handling

	buffer := &ArrowBuffer{
		config:       cfg,
		storage:      storage,
		writer:       NewArrowWriter(cfg, logger),
		shardCount:   32,
		ctx:          ctx,
		cancel:       cancel,
		flushTimer:   time.NewTicker(time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond),
		flushQueue:   make(chan flushTask, queueSize),
		flushWorkers: flushWorkers,
		logger:       logger.With().Str("component", "arrow-buffer").Logger(),
	}

	// Initialize shards
	for i := range buffer.shards {
		buffer.shards[i] = &bufferShard{
			buffers:            make(map[string][]interface{}),
			bufferStartTimes:   make(map[string]time.Time),
			bufferRecordCounts: make(map[string]int),
		}
	}

	// Start flush workers
	for i := 0; i < flushWorkers; i++ {
		buffer.wg.Add(1)
		go buffer.flushWorker(i)
	}

	// Start background flush
	buffer.wg.Add(1)
	go buffer.periodicFlush()

	buffer.logger.Info().
		Int("max_buffer_size", cfg.MaxBufferSize).
		Int("max_buffer_age_ms", cfg.MaxBufferAgeMS).
		Str("compression", cfg.Compression).
		Int("shards", int(buffer.shardCount)).
		Int("flush_workers", flushWorkers).
		Int("queue_size", queueSize).
		Msg("ArrowBuffer initialized with lock sharding and worker pool")

	return buffer
}

// SetWAL sets the WAL writer for durability
// When set, records are written to WAL before being buffered
func (b *ArrowBuffer) SetWAL(wal WALWriter) {
	b.wal = wal
	b.logger.Info().Msg("WAL enabled for ArrowBuffer")
}

// columnarToWALRecords converts columnar data to row-based records for WAL storage
// Each record includes database, measurement, and all column values
func (b *ArrowBuffer) columnarToWALRecords(database string, record *models.ColumnarRecord) []map[string]interface{} {
	if len(record.Columns) == 0 {
		return nil
	}

	// Find the number of rows from the first column
	var numRows int
	for _, col := range record.Columns {
		numRows = len(col)
		break
	}

	if numRows == 0 {
		return nil
	}

	// Convert columnar to row format
	records := make([]map[string]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		row := map[string]interface{}{
			"_database":    database,
			"_measurement": record.Measurement,
		}
		for colName, colData := range record.Columns {
			if i < len(colData) {
				row[colName] = colData[i]
			}
		}
		records[i] = row
	}

	return records
}

// Write adds records to the buffer (for MessagePack handler)
func (b *ArrowBuffer) Write(ctx context.Context, database string, records interface{}) error {
	// Handle batch of records (from MessagePack decoder)
	recordList, ok := records.([]interface{})
	if !ok {
		return fmt.Errorf("expected []interface{}, got %T", records)
	}

	for _, record := range recordList {
		switch r := record.(type) {
		case *models.ColumnarRecord:
			if err := b.writeColumnar(ctx, database, r); err != nil {
				b.logger.Error().Err(err).Str("measurement", r.Measurement).Msg("Failed to write columnar record")
				b.totalErrors.Add(1)
				return err
			}
		case *models.Record:
			// TODO: Implement row format handling (convert to columnar first)
			b.logger.Warn().Msg("Row format not yet implemented, skipping")
		default:
			b.logger.Warn().Interface("type", fmt.Sprintf("%T", record)).Msg("Unknown record type")
		}
	}

	return nil
}

// WriteColumnarDirect writes columnar data directly to the buffer
// This is the preferred method for Line Protocol which already has columnar data
func (b *ArrowBuffer) WriteColumnarDirect(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
	record := &models.ColumnarRecord{
		Measurement: measurement,
		Columns:     columns,
		Columnar:    true,
	}
	return b.writeColumnar(ctx, database, record)
}

// writeColumnar writes a columnar record to the buffer
func (b *ArrowBuffer) writeColumnar(ctx context.Context, database string, record *models.ColumnarRecord) error {
	// Create buffer key: database/measurement
	bufferKey := fmt.Sprintf("%s/%s", database, record.Measurement)

	// WAL: Write to WAL before buffering (if enabled)
	// This ensures data survives crashes even if not yet flushed to Parquet
	if b.wal != nil {
		// ZERO-COPY PATH: Use raw msgpack bytes if available (avoids re-serialization)
		if len(record.RawPayload) > 0 {
			if err := b.wal.AppendRaw(record.RawPayload); err != nil {
				// Log error but don't fail the write - WAL is for durability, not correctness
				b.logger.Error().Err(err).
					Str("database", database).
					Str("measurement", record.Measurement).
					Int("payload_size", len(record.RawPayload)).
					Msg("WAL write failed (zero-copy) - data may be lost on crash")
			}
		} else {
			// FALLBACK: Convert columnar to row format for WAL storage
			// This path is used for LineProtocol or when raw bytes aren't available
			walRecords := b.columnarToWALRecords(database, record)
			if len(walRecords) > 0 {
				if err := b.wal.Append(walRecords); err != nil {
					b.logger.Error().Err(err).
						Str("database", database).
						Str("measurement", record.Measurement).
						Int("records", len(walRecords)).
						Msg("WAL write failed - data may be lost on crash")
				}
			}
		}
	}

	// Convert []interface{} columns to typed arrays (optimized with zero-copy fast paths)
	typedColumns, numRecords, err := b.convertColumnsToTyped(record.Columns)
	if err != nil {
		return fmt.Errorf("failed to convert columns: %w", err)
	}

	// OPTIMIZATION: Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	// OPTIMIZATION: Extract-then-flush pattern
	// Hold lock ONLY to extract records, flush outside lock
	var recordsToFlush []interface{}
	var shouldFlush bool

	shard.mu.Lock()

	// Initialize buffer and record count if needed
	if _, exists := shard.buffers[bufferKey]; !exists {
		shard.bufferStartTimes[bufferKey] = time.Now()
		shard.bufferRecordCounts[bufferKey] = 0
	}

	// Add typed columns to buffer (already converted via zero-copy fast paths)
	shard.buffers[bufferKey] = append(shard.buffers[bufferKey], typedColumns)

	// CRITICAL FIX: Track count incrementally instead of O(n) loop
	shard.bufferRecordCounts[bufferKey] += numRecords
	totalBuffered := shard.bufferRecordCounts[bufferKey]

	// Check if buffer needs flush (size-based)
	if totalBuffered >= b.config.MaxBufferSize {
		// Extract records to flush (hold lock for microseconds only)
		recordsToFlush = make([]interface{}, len(shard.buffers[bufferKey]))
		copy(recordsToFlush, shard.buffers[bufferKey])

		// Clear buffer immediately
		shard.buffers[bufferKey] = nil
		delete(shard.bufferStartTimes, bufferKey)
		delete(shard.bufferRecordCounts, bufferKey)

		shouldFlush = true

		b.logger.Debug().
			Str("buffer_key", bufferKey).
			Int("total_records", totalBuffered).
			Msg("Extracted records for flush (fire-and-forget)")
	}

	// Release lock IMMEDIATELY (lock held for <1ms)
	shard.mu.Unlock()

	// OPTIMIZATION: Update metrics with atomic operations (lock-free!)
	b.totalRecordsBuffered.Add(int64(numRecords))

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added columnar data to buffer")

	// OPTIMIZATION: Queue flush to worker pool (bounded concurrency)
	// This prevents goroutine explosion under sustained load
	if shouldFlush {
		task := flushTask{
			ctx:         context.Background(),
			bufferKey:   bufferKey,
			database:    database,
			measurement: record.Measurement,
			records:     recordsToFlush,
			recordCount: totalBuffered,
		}

		// Non-blocking send to queue
		select {
		case b.flushQueue <- task:
			b.queueDepth.Add(1)
			b.logger.Info().
				Str("buffer_key", bufferKey).
				Int("total_records", totalBuffered).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Buffer size exceeded, queued flush to worker pool")
		default:
			// Queue full - log warning but don't block
			b.logger.Warn().
				Str("buffer_key", bufferKey).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Flush queue full, dropping task (backpressure)")
			b.totalErrors.Add(1)
		}
	}

	// Return immediately (don't wait for flush to complete!)
	return nil
}

// convertColumnsToTyped converts []interface{} columns to typed arrays
// ZERO-COPY OPTIMIZATION: Try bulk type assertion first before element-by-element conversion
func (b *ArrowBuffer) convertColumnsToTyped(columns map[string][]interface{}) (map[string]interface{}, int, error) {
	typed := make(map[string]interface{})
	var numRecords int

	for name, col := range columns {
		if len(col) == 0 {
			continue
		}

		// Set record count from first column
		if numRecords == 0 {
			numRecords = len(col)
		}

		// Infer type from first non-nil value
		var firstVal interface{}
		for _, v := range col {
			if v != nil {
				firstVal = v
				break
			}
		}

		if firstVal == nil {
			continue // Skip all-nil columns
		}

		// FAST PATH: Try zero-copy bulk conversion for homogeneous types
		// MessagePack often provides []interface{} that's actually homogeneous underneath
		switch firstVal.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Try zero-copy int64 first
			if arr, ok := b.tryInt64ZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			// OPTIMIZATION: Bulk conversion with pre-allocation
			// Pre-allocate result array (avoid reallocations)
			arr := make([]int64, len(col))

			// FAST PATH: Check if all values are same type (common case)
			// This allows tighter loop without type switch
			allInt64 := true
			for _, v := range col {
				if v != nil {
					if _, ok := v.(int64); !ok {
						allInt64 = false
						break
					}
				}
			}

			if allInt64 {
				// HOT PATH: All int64 - tight loop without type switch
				for i, v := range col {
					if v != nil {
						arr[i] = v.(int64)
					}
				}
			} else {
				// Fallback: Mixed types - use type switch
				for i, v := range col {
					if v == nil {
						arr[i] = 0
						continue
					}
					switch val := v.(type) {
					case int:
						arr[i] = int64(val)
					case int8:
						arr[i] = int64(val)
					case int16:
						arr[i] = int64(val)
					case int32:
						arr[i] = int64(val)
					case int64:
						arr[i] = val
					case uint:
						arr[i] = int64(val)
					case uint8:
						arr[i] = int64(val)
					case uint16:
						arr[i] = int64(val)
					case uint32:
						arr[i] = int64(val)
					case uint64:
						arr[i] = int64(val)
					default:
						return nil, 0, fmt.Errorf("unexpected type in int column '%s': %T", name, val)
					}
				}
			}
			typed[name] = arr

		case float32, float64:
			// Try zero-copy float64 first
			if arr, ok := b.tryFloat64ZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			// OPTIMIZATION: Bulk conversion with pre-allocation
			arr := make([]float64, len(col))

			// FAST PATH: Check if all values are float64
			allFloat64 := true
			for _, v := range col {
				if v != nil {
					if _, ok := v.(float64); !ok {
						allFloat64 = false
						break
					}
				}
			}

			if allFloat64 {
				// HOT PATH: All float64 - tight loop
				for i, v := range col {
					if v != nil {
						arr[i] = v.(float64)
					}
				}
			} else {
				// Fallback: Mixed types
				for i, v := range col {
					if v == nil {
						arr[i] = 0.0
						continue
					}
					switch val := v.(type) {
					case float32:
						arr[i] = float64(val)
					case float64:
						arr[i] = val
					default:
						return nil, 0, fmt.Errorf("unexpected type in float column '%s': %T", name, val)
					}
				}
			}
			typed[name] = arr

		case string:
			// Try zero-copy string first
			if arr, ok := b.tryStringZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			// Fall back to element-by-element conversion
			arr := make([]string, len(col))
			for i, v := range col {
				if v == nil {
					arr[i] = ""
					continue
				}
				str, ok := v.(string)
				if !ok {
					return nil, 0, fmt.Errorf("unexpected type in string column '%s': %T", name, v)
				}
				arr[i] = str
			}
			typed[name] = arr

		case bool:
			// Try zero-copy bool first
			if arr, ok := b.tryBoolZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			// Fall back to element-by-element conversion
			arr := make([]bool, len(col))
			for i, v := range col {
				if v == nil {
					arr[i] = false
					continue
				}
				b, ok := v.(bool)
				if !ok {
					return nil, 0, fmt.Errorf("unexpected type in bool column '%s': %T", name, v)
				}
				arr[i] = b
			}
			typed[name] = arr

		default:
			return nil, 0, fmt.Errorf("unsupported column type for '%s': %T", name, firstVal)
		}
	}

	return typed, numRecords, nil
}

// ZERO-COPY HELPERS: Try bulk type assertion for homogeneous arrays

// tryInt64ZeroCopy attempts zero-copy conversion for int64 arrays
// OPTIMIZATION: Single-pass check + conversion to reduce CPU cache thrashing
func (b *ArrowBuffer) tryInt64ZeroCopy(col []interface{}) ([]int64, bool) {
	arr := make([]int64, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false // Has nils, need element-by-element
		}
		val, ok := v.(int64)
		if !ok {
			return nil, false // Mixed types, need conversion
		}
		arr[i] = val
	}
	return arr, true
}

// tryFloat64ZeroCopy attempts zero-copy conversion for float64 arrays
// OPTIMIZATION: Single-pass check + conversion to reduce CPU cache thrashing
func (b *ArrowBuffer) tryFloat64ZeroCopy(col []interface{}) ([]float64, bool) {
	arr := make([]float64, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(float64)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// tryStringZeroCopy attempts zero-copy conversion for string arrays
// OPTIMIZATION: Single-pass check + conversion to reduce CPU cache thrashing
func (b *ArrowBuffer) tryStringZeroCopy(col []interface{}) ([]string, bool) {
	arr := make([]string, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(string)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// tryBoolZeroCopy attempts zero-copy conversion for bool arrays
// OPTIMIZATION: Single-pass check + conversion to reduce CPU cache thrashing
func (b *ArrowBuffer) tryBoolZeroCopy(col []interface{}) ([]bool, bool) {
	arr := make([]bool, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(bool)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// periodicFlush runs in the background and flushes old buffers
func (b *ArrowBuffer) periodicFlush() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.flushTimer.C:
			b.flushAgedBuffers()
		}
	}
}

// flushAgedBuffers flushes buffers that have exceeded max age
func (b *ArrowBuffer) flushAgedBuffers() {
	now := time.Now()
	maxAge := time.Duration(b.config.MaxBufferAgeMS) * time.Millisecond

	// Iterate over all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		// Check each buffer in this shard for age
		for key, startTime := range shard.bufferStartTimes {
			age := now.Sub(startTime)
			if age >= maxAge {
				b.logger.Info().
					Str("buffer_key", key).
					Dur("age", age).
					Int("shard", shardIdx).
					Msg("Flushing aged buffer")

				// Parse buffer key to get database and measurement
				parts := splitBufferKey(key)
				if len(parts) != 2 {
					b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format")
					continue
				}

				if err := b.flushBufferLocked(context.Background(), shard, key, parts[0], parts[1]); err != nil {
					b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush aged buffer")
				}
			}
		}

		shard.mu.Unlock()
	}
}

// splitBufferKey splits "database/measurement" into [database, measurement]
func splitBufferKey(key string) []string {
	// Find first slash to split database/measurement
	for i, c := range key {
		if c == '/' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return []string{key}
}

// flushRecordsAsync performs fire-and-forget flush in background goroutine
// OPTIMIZATION: This is launched as a goroutine and doesn't block the write path
// flushWorker processes flush tasks from the queue
// OPTIMIZATION: Bounded worker pool prevents goroutine explosion
func (b *ArrowBuffer) flushWorker(workerID int) {
	defer b.wg.Done()

	b.logger.Info().Int("worker_id", workerID).Msg("Flush worker started")

	for {
		select {
		case <-b.ctx.Done():
			b.logger.Info().Int("worker_id", workerID).Msg("Flush worker stopping")
			return
		case task := <-b.flushQueue:
			b.queueDepth.Add(-1)

			b.logger.Debug().
				Int("worker_id", workerID).
				Str("buffer_key", task.bufferKey).
				Int("records", task.recordCount).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Worker processing flush task")

			// Execute flush
			b.flushRecordsAsync(task.ctx, task.bufferKey, task.database, task.measurement, task.records, task.recordCount)
		}
	}
}

func (b *ArrowBuffer) flushRecordsAsync(ctx context.Context, bufferKey, database, measurement string, records []interface{}, recordCount int) {
	startTime := time.Now()

	// Merge typed column batches
	merged, err := b.mergeBatches(records)
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Msg("Failed to merge batches during async flush")

		b.totalErrors.Add(1)
		return
	}

	// Write merged columns to Parquet (uses optimized typed path)
	parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, merged)
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Msg("Failed to write Parquet during async flush")

		b.totalErrors.Add(1)
		return
	}

	// Generate storage path: database/measurement/YYYYMMDD/HHmmss_uuid.parquet
	storagePath := b.generateStoragePath(database, measurement)

	// Write to storage
	if err := b.storage.Write(ctx, storagePath, parquetData); err != nil {
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Str("storage_path", storagePath).
			Msg("Failed to write to storage during async flush")

		b.totalErrors.Add(1)
		return
	}

	flushDuration := time.Since(startTime)

	// Update metrics (lock-free atomic operations!)
	b.totalRecordsWritten.Add(int64(recordCount))
	b.totalFlushes.Add(1)

	b.logger.Info().
		Str("buffer_key", bufferKey).
		Str("storage_path", storagePath).
		Int("records", recordCount).
		Int("size_bytes", len(parquetData)).
		Dur("flush_duration", flushDuration).
		Msg("Async flush completed successfully")
}

// flushBufferLocked writes buffered data to Parquet and storage (synchronous version for periodic flush)
// Note: Caller must hold shard.mu lock
func (b *ArrowBuffer) flushBufferLocked(ctx context.Context, shard *bufferShard, bufferKey, database, measurement string) error {
	batches, exists := shard.buffers[bufferKey]
	if !exists || len(batches) == 0 {
		return nil
	}

	// Get record count before clearing buffer
	recordCount := shard.bufferRecordCounts[bufferKey]

	// Extract records to flush (hold lock for minimal time)
	recordsToFlush := make([]interface{}, len(batches))
	copy(recordsToFlush, batches)

	// Clear buffer immediately
	delete(shard.buffers, bufferKey)
	delete(shard.bufferStartTimes, bufferKey)
	delete(shard.bufferRecordCounts, bufferKey)

	// Release lock before expensive operations
	shard.mu.Unlock()

	// Merge typed column batches
	merged, err := b.mergeBatches(recordsToFlush)
	if err != nil {
		shard.mu.Lock() // Re-acquire lock for caller
		return fmt.Errorf("failed to merge batches: %w", err)
	}

	// Write merged columns to Parquet (uses optimized typed path)
	parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, merged)
	if err != nil {
		shard.mu.Lock() // Re-acquire lock for caller
		return fmt.Errorf("failed to write Parquet: %w", err)
	}

	// Generate storage path: database/measurement/YYYYMMDD/HHmmss_uuid.parquet
	storagePath := b.generateStoragePath(database, measurement)

	// Write to storage
	if err := b.storage.Write(ctx, storagePath, parquetData); err != nil {
		shard.mu.Lock() // Re-acquire lock for caller
		return fmt.Errorf("failed to write to storage: %w", err)
	}

	// Update metrics (lock-free atomic operations!)
	b.totalRecordsWritten.Add(int64(recordCount))
	b.totalFlushes.Add(1)

	b.logger.Info().
		Str("buffer_key", bufferKey).
		Str("storage_path", storagePath).
		Int("records", recordCount).
		Int("size_bytes", len(parquetData)).
		Msg("Periodic flush completed")

	// Re-acquire lock for caller
	shard.mu.Lock()
	return nil
}

// mergeBatches merges multiple column batches into a single columnar structure
// OPTIMIZATION: Pre-allocate merged arrays to avoid O(n²) append reallocations
func (b *ArrowBuffer) mergeBatches(batches []interface{}) (map[string]interface{}, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}

	// If only one batch, return it directly
	if len(batches) == 1 {
		if cols, ok := batches[0].(map[string]interface{}); ok {
			return cols, nil
		}
		return nil, fmt.Errorf("invalid batch type: %T", batches[0])
	}

	// PHASE 1: Calculate total sizes per column and detect types
	type colInfo struct {
		totalSize int
		colType   string // "int64", "float64", "string", "bool"
	}
	sizes := make(map[string]*colInfo)

	for _, batch := range batches {
		cols, ok := batch.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid batch type: %T", batch)
		}

		for name, col := range cols {
			info := sizes[name]
			if info == nil {
				info = &colInfo{}
				sizes[name] = info
			}

			switch v := col.(type) {
			case []int64:
				info.totalSize += len(v)
				info.colType = "int64"
			case []float64:
				info.totalSize += len(v)
				info.colType = "float64"
			case []string:
				info.totalSize += len(v)
				info.colType = "string"
			case []bool:
				info.totalSize += len(v)
				info.colType = "bool"
			default:
				return nil, fmt.Errorf("unsupported column type: %T", v)
			}
		}
	}

	// PHASE 2: Pre-allocate merged arrays with exact capacity
	merged := make(map[string]interface{}, len(sizes))
	offsets := make(map[string]int, len(sizes)) // Track copy position per column

	for name, info := range sizes {
		switch info.colType {
		case "int64":
			merged[name] = make([]int64, info.totalSize)
		case "float64":
			merged[name] = make([]float64, info.totalSize)
		case "string":
			merged[name] = make([]string, info.totalSize)
		case "bool":
			merged[name] = make([]bool, info.totalSize)
		}
		offsets[name] = 0
	}

	// PHASE 3: Copy data without reallocation
	for _, batch := range batches {
		cols := batch.(map[string]interface{}) // Already validated above

		for name, col := range cols {
			offset := offsets[name]
			switch v := col.(type) {
			case []int64:
				copy(merged[name].([]int64)[offset:], v)
				offsets[name] = offset + len(v)
			case []float64:
				copy(merged[name].([]float64)[offset:], v)
				offsets[name] = offset + len(v)
			case []string:
				copy(merged[name].([]string)[offset:], v)
				offsets[name] = offset + len(v)
			case []bool:
				copy(merged[name].([]bool)[offset:], v)
				offsets[name] = offset + len(v)
			}
		}
	}

	return merged, nil
}

// mergeBatchesRaw merges multiple raw []interface{} column batches
// OPTIMIZATION: Pre-allocate merged arrays to avoid O(n²) append reallocations
func (b *ArrowBuffer) mergeBatchesRaw(batches []interface{}) (map[string][]interface{}, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}

	// If only one batch, return it directly
	if len(batches) == 1 {
		if cols, ok := batches[0].(map[string][]interface{}); ok {
			return cols, nil
		}
		return nil, fmt.Errorf("invalid batch type: %T", batches[0])
	}

	// PHASE 1: Calculate total sizes per column
	sizes := make(map[string]int)

	for _, batch := range batches {
		cols, ok := batch.(map[string][]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid batch type: %T", batch)
		}

		for name, col := range cols {
			sizes[name] += len(col)
		}
	}

	// PHASE 2: Pre-allocate merged arrays with exact capacity
	merged := make(map[string][]interface{}, len(sizes))
	offsets := make(map[string]int, len(sizes))

	for name, size := range sizes {
		merged[name] = make([]interface{}, size)
		offsets[name] = 0
	}

	// PHASE 3: Copy data without reallocation
	for _, batch := range batches {
		cols := batch.(map[string][]interface{}) // Already validated above

		for name, col := range cols {
			offset := offsets[name]
			copy(merged[name][offset:], col)
			offsets[name] = offset + len(col)
		}
	}

	return merged, nil
}

// generateStoragePath creates a hierarchical storage path for partition pruning
// Format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{measurement}_{timestamp}_{nanos}.parquet
//
// This hierarchical structure enables DuckDB to skip entire directories when querying time ranges:
// - Query all of November: read_parquet('s3://bucket/db/cpu/2025/11/*/*/*.parquet')
// - Query specific day: read_parquet('s3://bucket/db/cpu/2025/11/25/*/*.parquet')
// - Query specific hour: read_parquet('s3://bucket/db/cpu/2025/11/25/16/*.parquet')
func (b *ArrowBuffer) generateStoragePath(database, measurement string) string {
	now := time.Now().UTC()

	// Hierarchical partitioning: year/month/day/hour
	year := now.Format("2006")
	month := now.Format("01")
	day := now.Format("02")
	hour := now.Format("15")

	// Filename includes measurement, timestamp, and nanos for uniqueness
	timestamp := now.Format("20060102_150405")
	nanos := now.UnixNano() % 1_000_000_000

	return fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s_%s_%09d.parquet",
		database, measurement, year, month, day, hour, measurement, timestamp, nanos)
}

// FlushAll flushes all buffered data to storage
func (b *ArrowBuffer) FlushAll(ctx context.Context) error {
	b.logger.Info().Msg("Flushing all buffers...")

	var lastErr error

	// Flush all buffers in all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		// Copy keys to avoid modifying map while iterating
		keys := make([]string, 0, len(shard.buffers))
		for key := range shard.buffers {
			keys = append(keys, key)
		}

		for _, key := range keys {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during flush")
				continue
			}

			if err := b.flushBufferLocked(ctx, shard, key, parts[0], parts[1]); err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush buffer")
				lastErr = err
			}

			// Re-acquire lock since flushBufferLocked releases it
			shard.mu.Lock()
		}

		shard.mu.Unlock()
	}

	b.logger.Info().Msg("All buffers flushed")
	return lastErr
}

// Close stops the buffer and flushes remaining data
func (b *ArrowBuffer) Close() error {
	b.logger.Info().Msg("Closing ArrowBuffer...")

	// Stop periodic flush
	b.cancel()
	b.flushTimer.Stop()

	// Close flush queue (workers will exit when queue is drained)
	close(b.flushQueue)

	// Wait for all workers to finish
	b.wg.Wait()

	b.logger.Info().Msg("All flush workers stopped, flushing remaining buffers")

	// Flush all remaining buffers in all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		for key := range shard.buffers {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during close")
				continue
			}

			if err := b.flushBufferLocked(context.Background(), shard, key, parts[0], parts[1]); err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush buffer during close")
			}
		}

		shard.mu.Unlock()
	}

	b.logger.Info().
		Int64("total_records_written", b.totalRecordsWritten.Load()).
		Int64("total_flushes", b.totalFlushes.Load()).
		Msg("ArrowBuffer closed")

	return nil
}

// GetStats returns buffer statistics
func (b *ArrowBuffer) GetStats() map[string]interface{} {
	// Count active buffers across all shards
	activeBuffers := 0
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]
		shard.mu.RLock()
		activeBuffers += len(shard.buffers)
		shard.mu.RUnlock()
	}

	// Read atomic values (lock-free!)
	return map[string]interface{}{
		"total_records_buffered": b.totalRecordsBuffered.Load(),
		"total_records_written":  b.totalRecordsWritten.Load(),
		"total_flushes":          b.totalFlushes.Load(),
		"total_errors":           b.totalErrors.Load(),
		"active_buffers":         activeBuffers,
		"flush_queue_depth":      b.queueDepth.Load(),
		"flush_workers":          b.flushWorkers,
	}
}
