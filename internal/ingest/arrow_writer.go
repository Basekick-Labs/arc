package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

const (
	flushTypeAsync = "async"
	flushTypeSync  = "sync"
)

// sharedArrowAllocator is a package-level shared allocator for Arrow operations.
// memory.GoAllocator is documented as thread-safe for concurrent use.
// Using a shared instance avoids allocator overhead per-write operation.
var sharedArrowAllocator = memory.NewGoAllocator()

// int64SliceToTimestamps reinterprets a []int64 as []arrow.Timestamp without copying.
// Safe because arrow.Timestamp is defined as `type Timestamp int64` (identical layout).
// LIFETIME: the caller must ensure src is not GC'd or reallocated while the returned
// slice or any Arrow array/builder built from it is still alive. Use only within the
// same stack frame as src.
func int64SliceToTimestamps(src []int64) []arrow.Timestamp {
	return *(*[]arrow.Timestamp)(unsafe.Pointer(&src))
}

// getFlushMessageType returns the human-readable flush type message for logging
func getFlushMessageType(flushType string) string {
	switch flushType {
	case flushTypeAsync:
		return "Async flush"
	case flushTypeSync:
		return "Periodic flush"
	default:
		return flushType + " flush"
	}
}

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

// ArrowWriter handles Arrow schema inference and Parquet writing
type ArrowWriter struct {
	compression       compress.Compression
	useDictionary     bool
	numericDictionary bool
	writeStatistics   bool
	dataPageVersion   string

	// Pre-built Parquet writer properties (immutable after construction).
	// Used directly for the all-columns-alike configurations (the
	// use_dictionary=false default, and use_dictionary+numeric_dictionary
	// both true) and for all-string schemas; the strings-only tier
	// (use_dictionary=true alone) builds per-schema properties in
	// writerPropsFor instead.
	writerProps *parquet.WriterProperties
	// baseWriterOpts are the schema-independent options writerPropsFor
	// extends with per-column dictionary overrides.
	baseWriterOpts []parquet.WriterProperty
	arrowProps     pqarrow.ArrowWriterProperties

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

	// Pre-build Parquet writer properties once — they are immutable config objects
	// that do not change after startup. Rebuilding them on every flush wastes CPU.
	writerOpts := []parquet.WriterProperty{
		parquet.WithCompression(comp),
		parquet.WithDictionaryDefault(cfg.UseDictionary),
		parquet.WithStats(cfg.WriteStatistics),
	}
	if cfg.DataPageVersion == "2.0" {
		writerOpts = append(writerOpts, parquet.WithDataPageVersion(parquet.DataPageV2))
	}

	return &ArrowWriter{
		compression:       comp,
		useDictionary:     cfg.UseDictionary,
		numericDictionary: cfg.NumericDictionary,
		writeStatistics:   cfg.WriteStatistics,
		dataPageVersion:   cfg.DataPageVersion,
		writerProps:       parquet.NewWriterProperties(writerOpts...),
		baseWriterOpts:    writerOpts,
		arrowProps:        pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()),
		schemaCache:       newSchemaLRUCache(schemaCacheCapacity),
		logger:            logger.With().Str("component", "arrow-writer").Logger(),
	}
}

// writerPropsFor returns Parquet writer properties for one schema. String and
// binary columns keep the configured dictionary setting — dictionaries
// compress repeated tag values extremely well. All other columns (numeric,
// boolean, timestamp, decimal) get dictionary encoding disabled unless
// ingest.numeric_dictionary is set: metric values are mostly
// high-cardinality, so the dictionary path pays a hash-table insert plus an
// interface boxing per value (~8-10% of write CPU on the sustained-ingest
// benchmark) and then typically falls back to plain encoding anyway.
// (Booleans are listed for completeness; the parquet writer never
// dictionary-encodes the Boolean physical type, so their override is a
// no-op either way.)
//
// Building properties per flush is deliberate: flushes happen tens of times
// per second at most, and the alternative — caching per schema — would need
// eviction tied to the schema LRU for no measurable gain.
func (w *ArrowWriter) writerPropsFor(schema *arrow.Schema) *parquet.WriterProperties {
	if !w.useDictionary || w.numericDictionary {
		return w.writerProps
	}
	var opts []parquet.WriterProperty
	for _, f := range schema.Fields() {
		switch f.Type.ID() {
		case arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
			// keep the dictionary default
		default:
			if opts == nil {
				opts = make([]parquet.WriterProperty, 0, len(w.baseWriterOpts)+len(schema.Fields()))
				opts = append(opts, w.baseWriterOpts...)
			}
			opts = append(opts, parquet.WithDictionaryFor(f.Name, false))
		}
	}
	if opts == nil {
		// No non-string columns — the prebuilt props are already correct.
		return w.writerProps
	}
	return parquet.NewWriterProperties(opts...)
}

// =============================================================================
// Type Conversion Helpers - Consolidated from duplicate implementations
// =============================================================================

// toInt64 converts any numeric type to int64
// Returns (value, ok) where ok is false if conversion failed
func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		// On 64-bit systems, uint can exceed MaxInt64
		if uint64(val) > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		if val > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case float32:
		// Bounds check required before conversion to int64
		if val > float32(math.MaxInt64) || val < float32(math.MinInt64) {
			return 0, false
		}
		return int64(val), true //nolint:gosec // Bounds checked above
	case float64:
		// Bounds check required before conversion to int64
		if val > float64(math.MaxInt64) || val < float64(math.MinInt64) {
			return 0, false
		}
		return int64(val), true //nolint:gosec // Bounds checked above
	default:
		return 0, false
	}
}

// toFloat64 converts any numeric type to float64
// Returns (value, ok) where ok is false if conversion failed
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

// firstNonNil returns the first non-nil value from a slice
// Returns nil if the slice is empty or all values are nil
func firstNonNil(col []interface{}) interface{} {
	for _, v := range col {
		if v != nil {
			return v
		}
	}
	return nil
}

// inferArrowType determines the Arrow data type from a Go value
// Special handling for "time" column which uses Timestamp type
func inferArrowType(colName string, firstVal interface{}) (arrow.DataType, error) {
	if colName == "time" {
		return arrow.FixedWidthTypes.Timestamp_us, nil
	}

	switch firstVal.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return arrow.PrimitiveTypes.Int64, nil
	case float32, float64:
		return arrow.PrimitiveTypes.Float64, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", firstVal)
	}
}

// sortColumnsTimeFirst sorts column names with "time" first, then alphabetical
func sortColumnsTimeFirst(colNames []string) {
	sort.Slice(colNames, func(i, j int) bool {
		if colNames[i] == "time" {
			return true
		}
		if colNames[j] == "time" {
			return false
		}
		return colNames[i] < colNames[j]
	})
}

// =============================================================================
// Schema Inference
// =============================================================================

// getSchema gets or infers Arrow schema for columnar data (LRU cached per measurement)
func (w *ArrowWriter) getSchema(measurement string, columns map[string]interface{}, tagColumns []string, dedupTime bool, decimalCols map[string]config.DecimalSpec) (*arrow.Schema, error) {
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
		case []decimal128.Num:
			typeNames = append(typeNames, "decimal128")
		default:
			typeNames = append(typeNames, "unknown")
		}
	}

	// Create cache key (includes tag columns and the dedup-time marker to ensure
	// metadata correctness — the flag changes the emitted arc:dedup_time key)
	cacheKey := fmt.Sprintf("%s:%v:%v:%v:%t", measurement, colNames, typeNames, tagColumns, dedupTime)

	// Check LRU cache
	if schema := w.schemaCache.get(cacheKey); schema != nil {
		return schema, nil
	}

	// Cache miss - infer schema
	schema, err := w.inferSchema(columns, tagColumns, dedupTime, decimalCols)
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

// inferSchema infers Arrow schema from columnar data.
// tagColumns optionally lists which columns are tags (stored as schema metadata for compaction dedup).
// decimalCols optionally maps column names to DecimalSpec for Decimal128 columns.
func (w *ArrowWriter) inferSchema(columns map[string]interface{}, tagColumns []string, dedupTime bool, decimalCols map[string]config.DecimalSpec) (*arrow.Schema, error) {
	var fields []arrow.Field

	for name, col := range columns {
		// Skip internal metadata columns
		if name[0] == '_' {
			continue
		}

		var arrowType arrow.DataType

		// The time column MUST be int64 microseconds (→ Timestamp_us). This is
		// checked BEFORE the type switch so it applies to every incoming Go
		// type, not just []int64. The previous code only special-cased time
		// inside `case []int64`, so a time column arriving as []string or
		// []float64 fell through to String/Float64 — silently producing a
		// VARCHAR (or DOUBLE) `time` parquet file. When such a file landed in a
		// partition alongside a normally-written Timestamp file, compaction's
		// read_parquet(union_by_name=true) could not reconcile the conflicting
		// types and failed to bind "time" (TIMESTAMP WITH TIME ZONE != VARCHAR),
		// permanently wedging the partition. Reject the write loudly instead —
		// a clear error at ingest beats a corrupt-schema file discovered weeks
		// later at compaction time, and it surfaces which writer sent bad time.
		if name == "time" {
			if _, ok := col.([]int64); !ok {
				return nil, fmt.Errorf("time column must be int64 microseconds, got %T (writer must send an integer epoch, not a string or float)", col)
			}
			fields = append(fields, arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true})
			continue
		}

		switch arr := col.(type) {
		case []int64:
			arrowType = arrow.PrimitiveTypes.Int64
		case []float64:
			arrowType = arrow.PrimitiveTypes.Float64
		case []string:
			arrowType = arrow.BinaryTypes.String
		case []bool:
			arrowType = arrow.FixedWidthTypes.Boolean
		case []decimal128.Num:
			if spec, ok := decimalCols[name]; ok {
				arrowType = &arrow.Decimal128Type{Precision: spec.Precision, Scale: spec.Scale}
			} else {
				// Fallback: use max precision if no config (shouldn't happen in normal flow)
				arrowType = &arrow.Decimal128Type{Precision: 38, Scale: 18}
			}
		default:
			return nil, fmt.Errorf("unsupported column type for column %s: %T", name, arr)
		}

		fields = append(fields, arrow.Field{Name: name, Type: arrowType, Nullable: true})
	}

	// Build schema metadata keys/values
	var metaKeys, metaValues []string

	// Store tag column names for compaction auto-dedup
	if len(tagColumns) > 0 {
		sorted := make([]string, len(tagColumns))
		copy(sorted, tagColumns)
		sort.Strings(sorted)
		metaKeys = append(metaKeys, "arc:tags")
		metaValues = append(metaValues, strings.Join(sorted, ","))
	}

	// Mark data as safe to dedup on time even without tag columns. Written only
	// by producers whose data model is one-row-per-(tags,time) — continuous
	// queries (#521). Compaction reads this to dedup a no-group-by CQ's duplicate
	// window emissions (PARTITION BY "time" alone). Raw ingest never sets it.
	if dedupTime {
		metaKeys = append(metaKeys, "arc:dedup_time")
		metaValues = append(metaValues, "true")
	}

	// Store decimal column specs for self-describing Parquet files
	if len(decimalCols) > 0 {
		var parts []string
		for col, spec := range decimalCols {
			parts = append(parts, fmt.Sprintf("%s:%d,%d", col, spec.Precision, spec.Scale))
		}
		sort.Strings(parts)
		metaKeys = append(metaKeys, "arc:decimals")
		metaValues = append(metaValues, strings.Join(parts, ";"))
	}

	var metadata *arrow.Metadata
	if len(metaKeys) > 0 {
		md := arrow.NewMetadata(metaKeys, metaValues)
		metadata = &md
	}

	return arrow.NewSchema(fields, metadata), nil
}

// WriteParquetColumnar writes columnar data directly to Parquet (zero-copy path).
// validity is an optional map of column name → []bool where false means null.
// Columns without a validity entry (or when validity is nil) are treated as fully valid.
// tagColumns optionally lists which columns are tags (stored as Parquet metadata for compaction dedup).
// dedupTime, when true, marks the file with arc:dedup_time so compaction dedups on time even with no tags.
// decimalCols optionally maps column names to DecimalSpec for Decimal128 type inference.
func (w *ArrowWriter) WriteParquetColumnar(ctx context.Context, measurement string, columns map[string]interface{}, validity map[string][]bool, tagColumns []string, dedupTime bool, decimalCols map[string]config.DecimalSpec) ([]byte, error) {
	data, _, err := w.writeParquetColumnarWithSchema(ctx, measurement, columns, validity, tagColumns, dedupTime, decimalCols)
	return data, err
}

// writeParquetColumnarWithSchema is WriteParquetColumnar returning the Arrow
// schema the file was written with, for field schema registration (#914).
func (w *ArrowWriter) writeParquetColumnarWithSchema(ctx context.Context, measurement string, columns map[string]interface{}, validity map[string][]bool, tagColumns []string, dedupTime bool, decimalCols map[string]config.DecimalSpec) ([]byte, *arrow.Schema, error) {
	// Get or infer schema (with caching)
	schema, err := w.getSchema(measurement, columns, tagColumns, dedupTime, decimalCols)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Create Arrow arrays from columns
	// MEMORY FIX: Use shared allocator instead of creating new one per write
	mem := sharedArrowAllocator
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
			return nil, nil, fmt.Errorf("column %s not found in data", field.Name)
		}

		// Get validity bitmap for this column (nil means all valid)
		var colValidity []bool
		if validity != nil {
			colValidity = validity[field.Name]
		}

		switch field.Type.ID() {
		case arrow.INT64:
			builder := array.NewInt64Builder(mem)
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				builder.AppendValues(intCol, colValidity)
			} else {
				return nil, nil, fmt.Errorf("column %s: expected []int64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.TIMESTAMP:
			builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				// MEMORY FIX: Zero-copy conversion from []int64 to []arrow.Timestamp
				// This avoids allocating a temporary slice on every write
				tsValues := int64SliceToTimestamps(intCol)
				builder.AppendValues(tsValues, colValidity)
			} else {
				return nil, nil, fmt.Errorf("column %s: expected []int64 for timestamp, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(mem)
			builders[i] = builder
			if floatCol, ok := col.([]float64); ok {
				builder.AppendValues(floatCol, colValidity)
			} else {
				return nil, nil, fmt.Errorf("column %s: expected []float64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.STRING:
			builder := array.NewStringBuilder(mem)
			builders[i] = builder
			if strCol, ok := col.([]string); ok {
				builder.AppendValues(strCol, colValidity)
			} else {
				return nil, nil, fmt.Errorf("column %s: expected []string, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.BOOL:
			builder := array.NewBooleanBuilder(mem)
			builders[i] = builder
			if boolCol, ok := col.([]bool); ok {
				builder.AppendValues(boolCol, colValidity)
			} else {
				return nil, nil, fmt.Errorf("column %s: expected []bool, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.DECIMAL128:
			dt := field.Type.(*arrow.Decimal128Type)
			builder := array.NewDecimal128Builder(mem, dt)
			builders[i] = builder
			if decCol, ok := col.([]decimal128.Num); ok {
				builder.AppendValues(decCol, colValidity)
			} else {
				return nil, nil, fmt.Errorf("column %s: expected []decimal128.Num, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		default:
			return nil, nil, fmt.Errorf("unsupported Arrow type for column %s: %s", field.Name, field.Type.Name())
		}
	}

	data, err := w.writeRecordToParquet(schema, arrays)
	return data, schema, err
}

// writeRecordToParquet writes Arrow arrays to Parquet bytes
func (w *ArrowWriter) writeRecordToParquet(schema *arrow.Schema, arrays []arrow.Array) ([]byte, error) {
	// Create record batch
	record := array.NewRecord(schema, arrays, -1)
	defer record.Release()

	// Write to Parquet
	var buf bytes.Buffer

	// Schema-aware writer properties (see writerPropsFor for the
	// per-configuration dictionary tiers).
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		w.writerPropsFor(schema),
		w.arrowProps,
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
// TypedColumnBatch holds typed column arrays with optional validity bitmaps.
// Validity tracks which values are null (false=null, true=valid).
// Columns without a validity entry are fully valid (no nulls).
type TypedColumnBatch struct {
	Data       map[string]interface{} // typed arrays ([]int64, []float64, []string, []bool)
	Validity   map[string][]bool      // per-column null bitmap; nil entry = all valid
	TagColumns []string               // tag column names (for Parquet metadata, enables auto-dedup)
	// DedupTime propagates ColumnarRecord.DedupTime: when true, the Parquet
	// footer gets an arc:dedup_time marker so compaction dedups on time even with
	// no tag columns. See ColumnarRecord.DedupTime — CQ output only (#521).
	DedupTime bool
	Signature string   // sorted column-name string; cached to avoid per-write recomputation
	WALHashes []string // identities of WAL entries represented by this batch
}

type bufferShard struct {
	buffers            map[string][]interface{}
	bufferStartTimes   map[string]time.Time
	bufferRecordCounts map[string]int
	bufferSchemas      map[string]string // Column signature for schema evolution detection
	mu                 sync.RWMutex

	// deferredKeys are buffers in this shard that crossed MaxBufferSize but
	// could not be handed to a worker because the flush queue was full, so
	// their records are still here. Maintained under mu.
	//
	// Without this set nothing knows a deferred buffer exists: it is re-enqueued
	// only by a later write to the same key, by the age sweep, or by Close. When
	// ingest moves on to other measurements, that buffer waits while flush
	// workers sit idle. flushDrainer consumes this set.
	deferredKeys map[string]struct{}

	// swept is set by Close()'s shard-flush loop, under the same mu
	// acquisition that flushes the shard, and is the signal that no further
	// append may happen here: anything added after the sweep would never be
	// flushed and would not be counted by CloseFlushedCleanly (#1007).
	//
	// It is deliberately PER SHARD rather than the global b.closing flag.
	// b.closing is set as Close()'s first statement, but the shard loop does
	// not start until after b.cancel() and b.wg.Wait() — a full flushTimeout
	// later in the worst case. Refusing on the global flag would reject every
	// write in that window, including replicated entries that have no WAL
	// copy of their own (WriteColumnarDirectNoWAL passes skipWAL), where today
	// Close() flushes them successfully.
	swept bool
}

// flushTask represents a flush operation to be executed by workers.
//
// It deliberately carries NO context: the flush timeout is created by the
// worker that receives the task, not by the writer that enqueues it (#1006).
// A timeout started at enqueue is consumed while the task waits in the queue,
// so under a backlog of N tasks at T per flush the task at the back arrives
// with flushTimeout-N*T left and can be already expired — the storage write
// then fails for a queueing reason and the batch is dropped as if storage had
// failed.
type flushTask struct {
	bufferKey   string
	database    string
	measurement string
	records     []interface{}
	recordCount int
	walHashes   []string
}

// WALWriter interface for Write-Ahead Log support
type WALWriter interface {
	Append(records []map[string]interface{}) error
	AppendRaw(payload []byte) error                          // Zero-copy: write raw msgpack bytes directly
	AppendRawWithMeta(database string, payload []byte) error // Zero-copy with database metadata envelope
	Stats() map[string]interface{}
	Close() error
}

type trackedWALWriter interface {
	AppendTracked(records []map[string]interface{}) ([]string, error)
	AppendRawWithMetaTracked(database string, payload []byte) ([]string, error)
	MarkFlushed(hashes []string) error
	// ForgetTracked releases identities whose write was abandoned after its
	// WAL append, so the purge floor stops waiting for data no buffer holds.
	ForgetTracked(hashes []string)
}

// walTrackedIdentityHexLen is the length of a tracked WAL identity:
// fmt.Sprintf("%016x%016x", instance, sequence). An untracked entry's identity
// is a 64-hex SHA-256 of its payload, which must never be treated as one.
const walTrackedIdentityHexLen = 32

func collectWALHashes(records []interface{}) []string {
	var hashes []string
	for _, record := range records {
		batch, ok := record.(*TypedColumnBatch)
		if !ok || len(batch.WALHashes) == 0 {
			continue
		}
		hashes = append(hashes, batch.WALHashes...)
	}
	return hashes
}

// FileRegistrar announces a newly written Parquet file to the cluster-wide
// manifest. Implementations should be non-blocking — file registration is
// fire-and-forget from the flush path's perspective. Wired by the cluster
// coordinator when peer replication is enabled (Enterprise feature).
//
// sha256 is a hex-encoded content checksum of the Parquet file bytes.
// Peers use this to verify the integrity of data pulled from the origin.
type FileRegistrar interface {
	RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string)
}

// ArrowBuffer manages buffering and periodic flushing of Arrow data
// Uses lock sharding to reduce contention across concurrent writes
type ArrowBuffer struct {
	config  *config.IngestConfig
	storage storage.Backend
	writer  *ArrowWriter

	// Optional WAL for durability
	wal WALWriter

	// Optional tiering manager for registering files in tier metadata
	tieringManager *tiering.Manager

	// Optional measurement field schema registrar (#914). Every flushed
	// file's schema is folded into the measurement's stored anchor so the
	// query path binds fields independently of the selected time range.
	fieldSchema FieldSchemaRegistrar

	// Optional file registrar for cluster-wide file manifest (Enterprise peer replication)
	// Set by cmd/arc/main.go when clustering + peer replication is enabled.
	// Called asynchronously after each flush — never blocks the flush path.
	fileRegistrar FileRegistrar

	// OPTIMIZATION: Shard buffers to reduce lock contention
	// Configurable via ingest.shard_count (default 32)
	// Each shard handles ~1/N of measurements where N = shard count
	// This allows N concurrent writes to different measurements
	shards     []*bufferShard
	shardCount uint32

	// Background flush
	ctx    context.Context
	cancel context.CancelFunc

	// flushParent is the parent of every flush I/O context. It is derived
	// with context.WithoutCancel(b.ctx), so b.cancel() — which exists to stop
	// the metrics sampler, the periodic flush and the workers' receive loops —
	// cannot abort a storage write that is already in progress (#1007).
	//
	// flushParentCancel is NOT part of normal shutdown. Close() calls it only
	// when its budget expires, as a last resort so that Close() always returns
	// and the remaining shutdown components still run. That matters because the
	// coordinator invokes Close() with no context and checks its own deadline
	// only BETWEEN components: a Close that never returns would skip the
	// wal-purge and wal components, and wal.Writer.Close is what performs the
	// WAL's final drain-and-sync. A shutdown that hangs here would retain a
	// WAL missing its newest entries.
	flushParent       context.Context
	flushParentCancel context.CancelFunc
	flushTimer        *time.Timer   // self-adjusting: fires when the oldest buffer is due to expire
	flushDeadline     time.Time     // absolute time when flushTimer will fire; updated whenever the timer is (re)set
	newBufferCh       chan struct{} // signals periodicFlush that a new buffer was created (used for idle→active wake-up)
	wg                sync.WaitGroup

	// OPTIMIZATION: Worker pool for bounded flush concurrency
	// Prevents goroutine explosion under sustained load
	flushQueue   chan flushTask
	flushWorkers int

	// closing is the shutdown short-circuit checked by tryEnqueueFlush.
	// See Close() for the full ordering rationale; senders see this
	// flag set before the channel could be closed (the channel is
	// never closed; workers exit on b.ctx.Done()).
	closing atomic.Bool

	// drainCh wakes flushDrainer when a flush-queue slot may have freed. Capacity
	// 1 with a non-blocking send: the signal is an edge, not a count, and the
	// drainer re-reads the real state when it wakes, so a coalesced signal costs
	// nothing.
	drainCh chan struct{}

	// totalFlushDeferred counts deferrals: a buffer that was ready to flush could
	// not be handed to a worker because the bounded queue had no room, so its
	// records stay in the shard. Both the write path and flushDrainer increment
	// it, so it is a saturation signal rather than a count of distinct buffers.
	//
	// A deferred buffer is picked up by flushDrainer as soon as a worker frees a
	// slot, by a later write to the same key, by the age-based sweep, or by
	// Close. Before #966 route 1 this condition extracted the batch and dropped
	// it, so this counter is the operator-visible replacement for what used to be
	// a silent data loss. Exposed as arc_ingest_flush_deferred_total and as
	// total_flush_deferred in GetStats.
	totalFlushDeferred atomic.Int64

	// flushDeferredLastLogNano is the sampler timestamp for the deferral Warn,
	// following recordWALError's pattern: sustained backpressure can defer on
	// every write to a hot measurement, and an unsampled line per deferral
	// floods the log exactly when an operator needs to read it.
	flushDeferredLastLogNano atomic.Int64

	// closeFlushClean records whether Close()'s final flush persisted every
	// buffered record. False until Close() completes successfully, so callers
	// that read it early (or after a shutdown that never reached Close) treat
	// the data as unflushed. Read via CloseFlushedCleanly; the shutdown WAL
	// purge uses it to avoid deleting the only copy of unflushed data (#803).
	closeFlushClean atomic.Bool

	// walOnlyRecords counts records that left the in-memory buffers without
	// reaching storage and therefore exist only in the WAL: enqueue rejections
	// (tryEnqueueFlush's three fallback paths) and tasks still queued when
	// Close() cancels the flush workers. It is the async counterpart to the
	// synchronous flush errors Close() collects, and CloseFlushedCleanly
	// requires it to be zero — purging the WAL while it is non-zero destroys
	// the only copy of those records (#803).
	walOnlyRecords atomic.Int64

	// closeFailed latches true the first time a Close observes data that did
	// not reach storage, so a subsequent Close cannot report a clean flush and
	// re-enable the shutdown WAL purge (#803).
	closeFailed atomic.Bool

	// closeBudget bounds Close() as a whole, including the wait for in-flight
	// flushes. Set by SetCloseBudget from server.shutdown_timeout; falls back
	// to flushTimeout when unset or non-positive. It bounds Close() only, not
	// the coordinator's remaining time, which Close() cannot observe.
	closeBudget time.Duration

	// Sort key configuration (for multi-column sorting)
	sortKeysConfig  map[string][]string // measurement -> sort keys
	defaultSortKeys []string            // default sort keys

	// Decimal column configuration (for Decimal128 precision support)
	decimalConfig        map[string]map[string]config.DecimalSpec // measurement -> column -> spec
	defaultDecimalConfig map[string]config.DecimalSpec            // default decimal columns

	// Flush timeout for storage writes (prevents workers from blocking forever on S3 hangs)
	flushTimeout time.Duration
	maxBufferAge time.Duration // pre-calculated from cfg.MaxBufferAgeMS

	// Metrics (using atomic operations to avoid lock contention)
	totalRecordsBuffered atomic.Int64
	totalRecordsWritten  atomic.Int64
	totalFlushes         atomic.Int64
	totalErrors          atomic.Int64
	totalWALErrors       atomic.Int64 // WAL write failures (real I/O / serialization errors)
	totalWALDropped      atomic.Int64 // WAL backpressure drops (entry queued but channel full)
	// totalSchemaChurnExceeded counts requests rejected because the
	// schema-evolution flush loop hit schemaEvolutionMaxIters under
	// sustained concurrent schema rotation against the same
	// (database, measurement) buffer. Pathological signal — operators
	// alert on a non-zero rate.
	totalSchemaChurnExceeded atomic.Int64
	queueDepth               atomic.Int64 // Current flush queue depth

	// walDropLogSampler debounces the WAL-dropped Warn so a sustained
	// burst of backpressure produces ~one log line per second instead
	// of one per dropped record. Operators get the rate via the
	// totalWALDropped counter and the underlying metrics.IncWALDroppedEntries
	// counter; the log line is for human-readable signal that the
	// degraded state is in effect.
	walDropLastLogNano atomic.Int64

	// Flush failure tracking for WAL maintenance.
	// Set when a storage write fails (S3 outage etc.), cleared after successful recovery.
	// The periodic WAL goroutine checks this to decide whether WAL replay is needed.
	hasFlushFailure atomic.Bool

	logger zerolog.Logger
}

// getColumnSignature returns a sorted string of "name:type" pairs for schema comparison.
// Encodes both column names and their Go slice types so that a type change (e.g.
// int64→float64 on the same column) is detected as schema evolution and triggers a
// flush before the new-schema data is appended.
func getColumnSignature(columns map[string]interface{}) string {
	// Signature encodes column NAME:TYPE. The type component is load-bearing:
	// if two batches for the same measurement differ in a column's Go type
	// (e.g. a field "cpu" sent as int64 then float64), they MUST get different
	// signatures so they land in separate buffers. Otherwise mergeBatches
	// allocates the merged column by the first batch's type and then
	// type-asserts the second batch against it (copy(merged[name].([]float64),
	// v)) — which panics and crashes the server when the types disagree.
	//
	// This does NOT reintroduce the time-column fan-out that wedged compaction
	// (#411 regression): the "time" column is forced to int64 at the typing
	// chokepoint (see convertColumnsToTyped), so its signature component is
	// always "time:i64" and time can never fan out into mixed-type files. The
	// type-awareness only protects the OTHER columns from the merge panic.
	type colEntry struct{ name, typ string }
	entries := make([]colEntry, 0, len(columns))
	size := -1 // first entry adds 0 commas; each subsequent adds 1
	for name, val := range columns {
		if len(name) == 0 || name[0] == '_' {
			continue // skip empty and internal columns
		}
		var typ string
		switch val.(type) {
		case []int64:
			typ = "i64"
		case []float64:
			typ = "f64"
		case []string:
			typ = "str"
		case []bool:
			typ = "bool"
		case []decimal128.Num:
			typ = "dec"
		default:
			typ = "unk"
		}
		entries = append(entries, colEntry{name, typ})
		size += 1 + len(name) + 1 + len(typ) // comma + name + colon + typ
	}
	if len(entries) == 0 {
		return ""
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	var sb strings.Builder
	sb.Grow(size)
	for i, e := range entries {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(e.name)
		sb.WriteByte(':')
		sb.WriteString(e.typ)
	}
	return sb.String()
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

// HasFlushFailure returns true if any flush has failed since the last reset.
// Used by the periodic WAL maintenance goroutine to decide whether WAL replay
// is needed (e.g., after an S3 outage where data was cleared from buffers).
func (b *ArrowBuffer) HasFlushFailure() bool {
	return b.hasFlushFailure.Load()
}

// ResetFlushFailure clears the flush failure flag.
// Called after successful WAL recovery replay.
func (b *ArrowBuffer) ResetFlushFailure() {
	b.hasFlushFailure.Store(false)
}

// markFlushFailure records that buffered data could not be persisted and must
// be recovered from WAL.
// publishBufferMetrics mirrors the buffer's internal counters into the
// exported metrics. The buffer has always tracked these; they were simply
// never published, leaving arc_buffer_flushes_total, records_written,
// records_buffered and queue_depth permanently zero (#802).
//
// Called after a flush completes and whenever the flush queue depth changes.
// All reads are atomic loads and the sets are atomic stores, so this adds no
// locking to the flush path.
func (b *ArrowBuffer) publishBufferMetrics() {
	m := metrics.Get()
	m.SetBufferFlushes(b.totalFlushes.Load())
	m.SetBufferRecordsWritten(b.totalRecordsWritten.Load())
	m.SetBufferQueueDepth(b.queueDepth.Load())
	m.SetBufferErrors(b.totalErrors.Load())
	m.SetBufferRecordsBuffered(b.currentBufferedRecords())
}

// countDeferredBuffers counts buffers whose records no worker could take. Also
// published as the arc_buffer_deferred_buffers gauge.
func (b *ArrowBuffer) countDeferredBuffers() int64 {
	var total int64
	for i := range b.shards {
		shard := b.shards[i]
		shard.mu.RLock()
		total += int64(len(shard.deferredKeys))
		shard.mu.RUnlock()
	}
	return total
}

// currentBufferedRecords sums the records sitting in every shard's buffers.
// This is the backpressure signal operators actually want: records accepted
// but not yet written to storage.
func (b *ArrowBuffer) currentBufferedRecords() int64 {
	var total int64
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]
		shard.mu.RLock()
		for _, n := range shard.bufferRecordCounts {
			total += int64(n)
		}
		shard.mu.RUnlock()
	}
	return total
}

// newFlushContext returns the context every flush I/O must use: derived from
// flushParent, so neither Close()'s b.cancel() nor a client disconnect can
// abort a write whose records are already out of the buffer, and bounded by
// flushTimeout.
//
// The nil guard covers tests that hand-build &ArrowBuffer{} without calling
// NewArrowBuffer: context.WithTimeout(nil, d) panics, and none of those tests
// should have to know that a flush needs a parent.
//
// NOTE on what the timeout actually bounds: it is only as effective as the
// backend. LocalBackend.Write ignores its context entirely, so on the OSS
// default backend a write in progress runs to completion regardless. The
// timeout is a real bound for S3 and Azure, whose SDKs are context-aware.
func (b *ArrowBuffer) newFlushContext() (context.Context, context.CancelFunc) {
	parent := b.flushParent
	if parent == nil {
		parent = context.Background()
	}
	timeout := b.flushTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

// SetCloseBudget bounds how long Close() may take in total, including the wait
// for in-flight flushes. main.go passes the validated server.shutdown_timeout
// duration right after construction.
//
// This bounds Close() alone. The shutdown coordinator runs every hook before
// any component and invokes a component's Close with no context, checking its
// own deadline only between components, so Close cannot see how much of the
// overall budget the hooks already spent. If they overran it and the
// coordinator skips the buffer entirely, Close never runs, closeFlushClean is
// never set, and CloseFlushedCleanly reports false — the WAL is retained,
// which is the safe direction.
func (b *ArrowBuffer) SetCloseBudget(d time.Duration) {
	if d <= 0 {
		return
	}
	b.closeBudget = d
}

// closeDeadlineBudget is the effective budget for a Close(): the configured one
// when set, otherwise a single flushTimeout, which is what one storage write is
// already allowed to take.
func (b *ArrowBuffer) closeDeadlineBudget() time.Duration {
	if b.closeBudget > 0 {
		return b.closeBudget
	}
	if b.flushTimeout > 0 {
		return b.flushTimeout
	}
	return 30 * time.Second
}

func (b *ArrowBuffer) markFlushFailure() {
	b.totalErrors.Add(1)
	b.hasFlushFailure.Store(true)
	metrics.Get().IncBufferFlushFailures()
}

// getSortKeys returns sort keys for a measurement.
// Users configure ADDITIONAL sort columns - "time" is always appended automatically.
// This ensures data is always sorted by time within each partition.
func (b *ArrowBuffer) getSortKeys(measurement string) []string {
	var keys []string

	// Check measurement-specific config
	if measurementKeys, exists := b.sortKeysConfig[measurement]; exists {
		keys = measurementKeys
	} else {
		// Use default
		keys = b.defaultSortKeys
	}

	// Always ensure "time" is the last sort key
	// Skip adding if already present (backwards compatibility with legacy configs)
	for _, k := range keys {
		if k == "time" {
			return keys
		}
	}

	// Append "time" - users configure ADDITIONAL sort keys only
	return append(keys, "time")
}

// getDecimalColumns returns the decimal column config for a measurement.
// Falls back to default config if no measurement-specific config exists.
func (b *ArrowBuffer) getDecimalColumns(measurement string) map[string]config.DecimalSpec {
	if specs, exists := b.decimalConfig[measurement]; exists {
		return specs
	}
	return b.defaultDecimalConfig
}

// HasDecimalColumns reports whether ANY measurement has decimal columns
// configured. The typed msgpack decode path is disabled entirely on such
// deployments: decimal conversion is config-driven and happens inside
// convertColumnsToTyped, and diverting per-measurement at decode time would
// need the measurement name before the columns are decoded (msgpack map key
// order is not guaranteed). Decimal deployments keep the generic path's
// exact semantics; everyone else gets the fast path.
//
// INVARIANT: decimalConfig and defaultDecimalConfig are parsed once in
// NewArrowBuffer and never mutated afterwards. The msgpack handler snapshots
// this value at construction (SetTypedDecodeEnabled); if decimal config ever
// becomes runtime-mutable, that snapshot must be revisited or decimal
// columns would silently take the typed path (signature without "dec" →
// wrong buffer keying).
func (b *ArrowBuffer) HasDecimalColumns() bool {
	return len(b.decimalConfig) > 0 || len(b.defaultDecimalConfig) > 0
}

// NewArrowBuffer creates a new Arrow buffer with automatic flushing
func NewArrowBuffer(cfg *config.IngestConfig, storage storage.Backend, logger zerolog.Logger) *ArrowBuffer {
	ctx, cancel := context.WithCancel(context.Background())

	// Flush I/O runs on a context that b.cancel() cannot reach, so a graceful
	// shutdown stops the background loops without aborting a storage write
	// whose records have already been removed from the buffer (#1007).
	// Close() cancels flushParent only if its own budget expires.
	flushParent, flushParentCancel := context.WithCancel(context.WithoutCancel(ctx))

	// Use configured values with sensible fallbacks
	flushWorkers := cfg.FlushWorkers
	if flushWorkers <= 0 {
		flushWorkers = 16 // Fallback if not configured
	}

	queueSize := cfg.FlushQueueSize
	if queueSize <= 0 {
		queueSize = 100 // Fallback if not configured
	}

	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 32 // Fallback if not configured
	}

	// Parse sort keys config using shared function
	sortKeysConfig, defaultSortKeys, err := config.ParseSortKeys(*cfg)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid sort keys config, using defaults")
		sortKeysConfig = make(map[string][]string)
		defaultSortKeys = []string{"time"}
	}

	// Parse decimal column config
	decimalConfig, defaultDecimalConfig, err := config.ParseDecimalColumns(*cfg)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid decimal columns config, decimal support disabled")
		decimalConfig = make(map[string]map[string]config.DecimalSpec)
		defaultDecimalConfig = nil
	}

	// Parse flush timeout (default 30s)
	flushTimeout := time.Duration(cfg.FlushTimeoutSeconds) * time.Second
	if cfg.FlushTimeoutSeconds <= 0 {
		flushTimeout = 30 * time.Second
	}

	buffer := &ArrowBuffer{
		config:               cfg,
		storage:              storage,
		writer:               NewArrowWriter(cfg, logger),
		shards:               make([]*bufferShard, shardCount),
		shardCount:           uint32(shardCount),
		ctx:                  ctx,
		cancel:               cancel,
		drainCh:              make(chan struct{}, 1),
		flushParent:          flushParent,
		flushParentCancel:    flushParentCancel,
		flushTimer:           time.NewTimer(time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond),
		flushDeadline:        time.Now().UTC().Add(time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond),
		newBufferCh:          make(chan struct{}, 1),
		flushQueue:           make(chan flushTask, queueSize),
		flushWorkers:         flushWorkers,
		flushTimeout:         flushTimeout,
		maxBufferAge:         time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond,
		sortKeysConfig:       sortKeysConfig,
		defaultSortKeys:      defaultSortKeys,
		decimalConfig:        decimalConfig,
		defaultDecimalConfig: defaultDecimalConfig,
		logger:               logger.With().Str("component", "arrow-buffer").Logger(),
	}

	// Initialize shards
	for i := 0; i < shardCount; i++ {
		buffer.shards[i] = &bufferShard{
			buffers:            make(map[string][]interface{}),
			bufferStartTimes:   make(map[string]time.Time),
			bufferRecordCounts: make(map[string]int),
			bufferSchemas:      make(map[string]string),
			deferredKeys:       make(map[string]struct{}),
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

	// One drainer, regardless of worker count: its work is a bounded scan plus a
	// non-blocking enqueue, and serialising it keeps the oldest-first ordering
	// meaningful.
	buffer.wg.Add(1)
	go buffer.flushDrainer()

	// Publish buffer gauges on a fixed cadence (#802)
	buffer.wg.Add(1)
	go buffer.metricsSampler()

	buffer.logger.Info().
		Int("max_buffer_size", cfg.MaxBufferSize).
		Int("max_buffer_age_ms", cfg.MaxBufferAgeMS).
		Str("compression", cfg.Compression).
		Int("shards", shardCount).
		Int("flush_workers", flushWorkers).
		Int("queue_size", queueSize).
		Dur("flush_timeout", flushTimeout).
		Msg("ArrowBuffer initialized with lock sharding and worker pool")

	return buffer
}

// SetWAL sets the WAL writer for durability
// When set, records are written to WAL before being buffered
func (b *ArrowBuffer) SetWAL(wal WALWriter) {
	b.wal = wal
	b.logger.Info().Msg("WAL enabled for ArrowBuffer")
}

// SetTieringManager sets the tiering manager for automatic file registration.
// When set, newly written parquet files are automatically registered in tiering metadata.
// FieldSchemaRegistrar receives the Arrow schema of every Parquet file the
// buffer writes. Implemented by fieldschema.Registry; an interface so ingest
// tests can observe registrations without a storage-backed registry.
type FieldSchemaRegistrar interface {
	EnsureFile(ctx context.Context, database, measurement string, schema *arrow.Schema, tagColumns []string, storageKey string) error
}

// SetFieldSchema installs the field schema registrar (#914).
func (b *ArrowBuffer) SetFieldSchema(r FieldSchemaRegistrar) {
	b.fieldSchema = r
}

// registerFieldSchema folds a just-written file's schema into the
// measurement's anchor. The file is already durable; a registry failure is
// logged and never fails the flush.
func (b *ArrowBuffer) registerFieldSchema(ctx context.Context, database, measurement string, schema *arrow.Schema, tagColumns []string, storageKey string) {
	if b.fieldSchema == nil || schema == nil {
		return
	}
	if err := b.fieldSchema.EnsureFile(ctx, database, measurement, schema, tagColumns, storageKey); err != nil {
		b.logger.Warn().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Msg("Field schema registration failed; the file is written and registration retries on the next flush")
	}
}

func (b *ArrowBuffer) SetTieringManager(tm *tiering.Manager) {
	b.tieringManager = tm
	b.logger.Info().Msg("Tiering manager enabled for ArrowBuffer - files will be auto-registered")
}

// SetFileRegistrar sets the cluster-wide file manifest registrar.
// When set, newly written Parquet files are announced to the cluster manifest
// asynchronously (non-blocking) — used by peer replication to discover files
// that need to be pulled from other nodes.
func (b *ArrowBuffer) SetFileRegistrar(fr FileRegistrar) {
	b.fileRegistrar = fr
	b.logger.Info().Msg("File registrar enabled for ArrowBuffer - files will be announced to cluster manifest")
}

// registerFileInTiering registers a newly written parquet file in the tiering metadata
// and (if enabled) announces it to the cluster-wide file manifest for peer replication.
// This allows the tiering system to track the file for future migration and query routing,
// and enables Enterprise peer replication to replicate the file to other cluster nodes.
//
// sha256Hex is a hex-encoded SHA-256 of the Parquet bytes, computed by the caller on the
// in-memory buffer immediately before the backend write. Peers validate downloaded bytes
// against this checksum.
func (b *ArrowBuffer) registerFileInTiering(ctx context.Context, database, measurement, storagePath string, partitionTime time.Time, sizeBytes int64, sha256Hex string) {
	// Register in local tiering metadata (hot/cold tracking)
	if b.tieringManager != nil {
		metadata := b.tieringManager.GetMetadata()
		if metadata != nil {
			file := &tiering.FileMetadata{
				Path:          storagePath,
				Database:      database,
				Measurement:   measurement,
				PartitionTime: partitionTime,
				Tier:          tiering.TierHot,
				SizeBytes:     sizeBytes,
				CreatedAt:     time.Now().UTC(),
			}
			if err := metadata.RecordFile(ctx, file); err != nil {
				b.logger.Warn().Err(err).
					Str("path", storagePath).
					Str("database", database).
					Str("measurement", measurement).
					Msg("Failed to register file in tiering metadata")
			}
		}
	}

	// Announce to cluster-wide file manifest (peer replication).
	// The registrar implementation MUST be non-blocking — it's called on
	// the hot flush path.
	if b.fileRegistrar != nil {
		b.fileRegistrar.RegisterFile(database, measurement, storagePath, partitionTime, sizeBytes, sha256Hex)
	}
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

// rowsToColumnar converts a slice of row-format Records into a ColumnarRecord.
// This enables the MessagePack handler to accept row-format data and convert it
// to the columnar format expected by the Arrow writer.
//
// The conversion:
// - time column: populated from Record.Timestamp (microseconds) or Record.Time
// - Tag columns: stored directly by tag name (matches Line Protocol behavior)
// - Field columns: stored directly by field name (conflicts get "_value" suffix)
func (b *ArrowBuffer) rowsToColumnar(measurement string, rows []*models.Record) *models.ColumnarRecord {
	if len(rows) == 0 {
		return &models.ColumnarRecord{
			Measurement: measurement,
			Columnar:    true,
			Columns:     make(map[string][]interface{}),
		}
	}

	// Pre-allocate columns map - estimate based on first record
	firstRow := rows[0]
	estimatedCols := 1 + len(firstRow.Tags) + len(firstRow.Fields) // time + tags + fields
	columns := make(map[string][]interface{}, estimatedCols)

	// Initialize time column
	columns["time"] = make([]interface{}, 0, len(rows))

	// First pass: collect all unique column names across all rows
	// This handles schema variations where different rows may have different fields/tags
	allTags := make(map[string]struct{})
	allFields := make(map[string]struct{})
	for _, row := range rows {
		for tag := range row.Tags {
			allTags[tag] = struct{}{}
		}
		for field := range row.Fields {
			allFields[field] = struct{}{}
		}
	}

	// Initialize columns for all tags and fields
	// Tags are stored directly by name (matching Line Protocol behavior)
	// Fields that conflict with tags get "_value" suffix
	for tag := range allTags {
		columns[tag] = make([]interface{}, 0, len(rows))
	}
	for field := range allFields {
		if _, hasTag := allTags[field]; hasTag {
			columns[field+"_value"] = make([]interface{}, 0, len(rows))
		} else {
			columns[field] = make([]interface{}, 0, len(rows))
		}
	}

	// Second pass: populate columns with values
	for _, row := range rows {
		// Handle timestamp: prefer Timestamp (microseconds) if set, otherwise convert Time
		var timestamp int64
		if row.Timestamp != 0 {
			timestamp = row.Timestamp
		} else if !row.Time.IsZero() {
			timestamp = row.Time.UnixMicro()
		} else {
			// Use current time if no timestamp provided
			timestamp = time.Now().UnixMicro()
		}
		columns["time"] = append(columns["time"], timestamp)

		// Add tag values (nil for missing tags to maintain column alignment)
		for tag := range allTags {
			if val, ok := row.Tags[tag]; ok {
				columns[tag] = append(columns[tag], val)
			} else {
				columns[tag] = append(columns[tag], nil)
			}
		}

		// Add field values (nil for missing fields to maintain column alignment)
		// Fields that conflict with tags get "_value" suffix
		for field := range allFields {
			colName := field
			if _, hasTag := allTags[field]; hasTag {
				colName = field + "_value"
			}
			if val, ok := row.Fields[field]; ok {
				columns[colName] = append(columns[colName], val)
			} else {
				columns[colName] = append(columns[colName], nil)
			}
		}
	}

	// Collect tag column names for Parquet metadata (enables auto-dedup in compaction)
	tagColumns := make([]string, 0, len(allTags))
	for tag := range allTags {
		tagColumns = append(tagColumns, tag)
	}

	return &models.ColumnarRecord{
		Measurement: measurement,
		Columnar:    true,
		Columns:     columns,
		TagColumns:  tagColumns,
	}
}

// Write adds records to the buffer (for MessagePack handler)
func (b *ArrowBuffer) Write(ctx context.Context, database string, records interface{}) error {
	// Handle batch of records (from MessagePack decoder)
	recordList, ok := records.([]interface{})
	if !ok {
		return fmt.Errorf("expected []interface{}, got %T", records)
	}

	// OPTIMIZATION: Lazy initialization - avoid map allocation for pure columnar writes (common path)
	var rowRecordsByMeasurement map[string][]*models.Record

	for _, record := range recordList {
		switch r := record.(type) {
		case *models.ColumnarRecord:
			if err := b.writeColumnar(ctx, database, r); err != nil {
				b.logger.Error().Err(err).Str("measurement", r.Measurement).Msg("Failed to write columnar record")
				b.totalErrors.Add(1)
				return err
			}
		case *TypedColumnarRecord:
			// Typed msgpack decode fast path: already converted, carries the
			// raw client bytes for the zero-copy WAL branch.
			if err := b.writeTypedColumnarRaw(ctx, database, r.Measurement, r.Batch, r.NumRecords, r.RawPayload, false); err != nil {
				b.logger.Error().Err(err).Str("measurement", r.Measurement).Msg("Failed to write typed columnar record")
				b.totalErrors.Add(1)
				return err
			}
		case *models.Record:
			// Lazy init: only allocate map when we actually have row records
			if rowRecordsByMeasurement == nil {
				rowRecordsByMeasurement = make(map[string][]*models.Record)
			}
			// Group row records by measurement for batch conversion
			rowRecordsByMeasurement[r.Measurement] = append(rowRecordsByMeasurement[r.Measurement], r)
		default:
			// FAIL LOUDLY: an unwired record type here means a decoder
			// produced a type this dispatch doesn't know — silently dropping
			// it would return 204 to the client while writing nothing, and
			// would also mean api/msgpack.go extractMeasurements skipped
			// measurement validation and RBAC for it. Refuse the write.
			b.totalErrors.Add(1)
			return fmt.Errorf("unknown record type %T in write dispatch (unwired decoder output)", record)
		}
	}

	// Convert grouped row records to columnar format and write
	for measurement, rowRecords := range rowRecordsByMeasurement {
		columnar := b.rowsToColumnar(measurement, rowRecords)
		if err := b.writeColumnar(ctx, database, columnar); err != nil {
			b.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to write converted row records")
			b.totalErrors.Add(1)
			return err
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

// WriteColumnarRecord writes a pre-built ColumnarRecord to the buffer.
// Preserves TagColumns metadata for Parquet schema (enables auto-dedup in compaction).
func (b *ArrowBuffer) WriteColumnarRecord(ctx context.Context, database string, record *models.ColumnarRecord) error {
	return b.writeColumnar(ctx, database, record)
}

// WriteColumnarDirectNoWAL writes columnar data without writing to WAL.
// Used during WAL recovery to avoid re-writing recovered data back to WAL.
//
// Note (#521): the WAL stores flattened rows and does NOT carry TagColumns or
// the DedupTime marker (both are json:"-"), so a Parquet file produced purely
// from WAL replay has neither arc:tags nor arc:dedup_time. In practice this
// self-heals: compaction unions metadata across all files in a partition, so
// a replayed file dedups against any marked sibling written normally for the
// same window. It only fails to dedup if the replayed copy and a marked copy
// land in different compaction batches — a transient duplicate, never data
// loss. This matches the pre-existing arc:tags WAL behavior; propagating the
// markers through the WAL is a separate, larger change (WAL schema).
// WriteColumnarDirectNoWAL writes a replayed or replicated columnar payload into
// the buffer without appending it to the WAL. It inherits no WAL identity; use
// WriteColumnarDirectReplay when replaying an entry whose identity should be
// checkpointed once the batch reaches storage.
func (b *ArrowBuffer) WriteColumnarDirectNoWAL(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
	return b.writeColumnarDirect(ctx, database, measurement, columns, "")
}

func (b *ArrowBuffer) writeColumnarDirect(ctx context.Context, database, measurement string, columns map[string][]interface{}, walIdentity string) error {
	// #590: both callers (WAL crash replay, cluster WAL replication) feed
	// RAW client payloads that never went through the live decode path's
	// post-processing. Apply it here so replayed data behaves exactly like
	// live-ingested data:
	//   - missing/empty time column → generate now-µs (the live path did
	//     this at original ingest; the WAL stores the client's original
	//     bytes WITHOUT the generated column, and without it the flush
	//     wrote nothing — silent data loss)
	//   - normalize timestamp units to µs (a seconds-precision client's
	//     entries otherwise land in 1970-era partitions via groupByHour)
	//   - sanitize strings to valid UTF-8 (invalid UTF-8 breaks DuckDB)
	// All three are idempotent for already-processed data.
	//
	// Structural validation mirrors decodeColumnar: today's inputs are
	// leader-validated (the WAL stores only payloads that passed the live
	// decode; replication is HMAC-authenticated), but a checksum-passing
	// corrupt entry with mismatched column lengths must not flow into the
	// typed conversion unchecked.
	numRecords := -1
	for colName, col := range columns {
		if numRecords == -1 {
			numRecords = len(col)
		} else if len(col) != numRecords {
			return fmt.Errorf("replayed columnar entry for %s: column length mismatch (expected %d, got %d for '%s')", measurement, numRecords, len(col), colName)
		}
	}
	if numRecords <= 0 {
		return fmt.Errorf("replayed columnar entry for %s: empty columns", measurement)
	}
	if _, ok := columns["time"]; !ok {
		b.logger.Warn().
			Str("measurement", measurement).
			Int("row_count", numRecords).
			Msg("Replayed data missing 'time' column - generating UTC timestamps")
		nowMicros := time.Now().UTC().UnixMicro()
		generated := make([]interface{}, numRecords)
		for i := range generated {
			generated[i] = nowMicros
		}
		columns["time"] = generated
	}
	if err := normalizeTimestampColumns(columns); err != nil {
		return fmt.Errorf("normalize replayed timestamps for %s: %w", measurement, err)
	}
	sanitizeColumnarStrings(columns)

	record := &models.ColumnarRecord{
		Measurement: measurement,
		Columns:     columns,
		Columnar:    true,
	}
	return b.writeColumnarInternal(ctx, database, record, true, walIdentity)
}

// WriteColumnarDirectReplay is WriteColumnarDirectNoWAL for WAL recovery, with
// one addition: it carries the identity of the WAL entry being replayed, so the
// re-buffered batch inherits it.
//
// That is what makes replay idempotent. A replayed batch writes nothing to the
// WAL (there is already a copy on disk — that is what is being replayed), so
// before this it also produced no flush checkpoint, and the entry was replayed
// again by every subsequent recovery pass. Any file recovery keeps — one poisoned
// entry is enough (#590) — therefore re-applied all of its healthy entries on
// every pass, and for tagless measurements compaction can never remove the
// duplicates. Inheriting the identity means the eventual flush checkpoints the
// ORIGINAL entry, and the next pass skips it.
//
// Pass the empty string to inherit nothing. Callers MUST do that unless the
// identity covers exactly the records in this call: the row-format recovery
// callback explodes one WAL entry into one call per record, so checkpointing
// the entry when only some of those calls flush would mark data durable that
// was discarded. See cmd/arc's recovery callbacks.
func (b *ArrowBuffer) WriteColumnarDirectReplay(ctx context.Context, database, measurement string, columns map[string][]interface{}, walIdentity string) error {
	return b.writeColumnarDirect(ctx, database, measurement, columns, walIdentity)
}

// WriteTypedColumnarDirect writes a pre-typed column batch to the buffer,
// bypassing the []interface{} → typed conversion in convertColumnsToTyped.
// Used by format-specific parsers (e.g., TLE) that know column types at compile time.
func (b *ArrowBuffer) WriteTypedColumnarDirect(ctx context.Context, database, measurement string, batch *TypedColumnBatch, numRecords int) error {
	return b.writeTypedColumnarInternal(ctx, database, measurement, batch, numRecords, false)
}

// walDropLogIntervalNano is the minimum interval between successive
// WAL-dropped Warn log emissions. Backpressure on a busy node can
// produce hundreds of dropped entries per second; one Warn per drop
// is log-spam. Operators get the rate from the totalWALDropped
// counter; the log line just signals "the degraded state is in
// effect, look at the counter."
const walDropLogIntervalNano = int64(time.Second)

// recordWALError classifies the error from a WAL append: a backpressure
// drop (wal.ErrWALDropped) is operationally distinct from a real I/O
// failure. Backpressure increments the dedicated totalWALDropped
// counter and emits a sampled Warn; other errors hit totalWALErrors
// and an unsampled Error log. The caller must NOT propagate the
// error — both paths leave the data buffered and the caller continues.
//
// fields is a small closure that adds context (db/measurement/size)
// to whichever logger ends up firing — keeps the call sites clean.
func (b *ArrowBuffer) recordWALError(err error, fields func(*zerolog.Event)) {
	if errors.Is(err, wal.ErrWALDropped) {
		b.totalWALDropped.Add(1)
		// Sampled Warn — at most one log line per walDropLogIntervalNano.
		now := time.Now().UnixNano()
		last := b.walDropLastLogNano.Load()
		if now-last >= walDropLogIntervalNano && b.walDropLastLogNano.CompareAndSwap(last, now) {
			ev := b.logger.Warn().Err(err).
				Int64("total_dropped", b.totalWALDropped.Load())
			if fields != nil {
				fields(ev)
			}
			ev.Msg("WAL backpressure: entries dropped on full async channel; data buffered in memory and will flush, but follower-side durability is degraded until backpressure clears")
		}
		return
	}
	b.totalWALErrors.Add(1)
	ev := b.logger.Error().Err(err)
	if fields != nil {
		fields(ev)
	}
	ev.Msg("WAL write failed - data may be lost on crash")
}

// enqueueOrDeferLocked handles a size-triggered flush. It MUST be called with
// shard.mu held, and it never blocks.
//
// Either the whole buffer is handed to a worker and its map entries are removed,
// or nothing is touched and the records stay where they are. That choice is the
// fix for #966 route 1: the old code extracted and deleted the buffer FIRST and
// only then attempted the send, so a full queue destroyed a batch built from
// many already-acknowledged writes. Nothing is extracted unless it has somewhere
// to go.
//
// Returns (queued, deferred). Both false means shutdown is in progress, where
// Close's own shard loop flushes the buffer and a deferral is not worth
// reporting.
//
// Why the send belongs under shard.mu: it is a non-blocking select, so it cannot
// deadlock, and holding the lock across it is what makes the decision atomic
// with respect to the buffer. It also removes the window that used to exist
// between extracting and enqueueing, in which a writer held records that were in
// no buffer, no queue and no flush.
//
// The pre-check on queue length matters for its own sake: without it, a buffer
// that is over the threshold while the queue is saturated would build a task and
// a context on every single write just to discard them.
func (b *ArrowBuffer) enqueueOrDeferLocked(
	shard *bufferShard,
	bufferKey, database, measurement string,
	totalBuffered int,
) (queued bool, deferred bool) {
	// The buffer may be gone. flushDrainer picks a key from a snapshot taken
	// under a DIFFERENT lock acquisition, so between its scan and this call the
	// key can be flushed by the age sweep, by a schema-change flush, by FlushAll,
	// or by an ordinary write. Without this check the task below would carry
	// records == nil with a stale recordCount, mergeBatches would fail with "no
	// batches to merge", and markFlushFailure would then trigger a full periodic
	// WAL recovery pass and leave CloseFlushedCleanly false for the rest of the
	// process lifetime — all on a healthy system.
	if _, exists := shard.buffers[bufferKey]; !exists {
		// Clear the whole entry, not just the marker: leaving bufferStartTimes,
		// bufferRecordCounts or bufferSchemas behind is the same phantom class the
		// swept-guard move in this change exists to eliminate, and the aged sweep
		// and next-deadline computation both walk bufferStartTimes.
		delete(shard.deferredKeys, bufferKey)
		delete(shard.bufferStartTimes, bufferKey)
		delete(shard.bufferRecordCounts, bufferKey)
		delete(shard.bufferSchemas, bufferKey)
		return false, false
	}

	closing := b.closing.Load()
	if closing || len(b.flushQueue) >= cap(b.flushQueue) {
		// Deferring during shutdown is normal and uninteresting: Close flushes
		// this buffer. Only a genuine queue-full deferral is worth counting.
		if !closing {
			b.markDeferredLocked(shard, bufferKey)
			return false, true
		}
		return false, false
	}

	// Hand the buffer's slice straight to the task rather than copying it. Safe
	// because the map entries are deleted below on success, so the next write to
	// this key starts a fresh slice and nothing appends to the one in flight.
	//
	// No context on the task: the worker builds the timeout when it receives it,
	// so the timeout measures the storage write and not the queue wait (#1006).
	task := flushTask{
		bufferKey:   bufferKey,
		database:    database,
		measurement: measurement,
		records:     shard.buffers[bufferKey],
		recordCount: totalBuffered,
		// Collect the WAL identities HERE, while the batches are still reachable
		// and the shard lock is held. The worker cannot do it later: the first
		// thing flushRecordsAsync does after merging is nil out every entry of
		// task.records, so the *TypedColumnBatch values carrying WALHashes are
		// gone by the time the flush succeeds.
		//
		// Omitting this left task.walHashes nil on every asynchronous flush, so
		// markWALFlushed returned immediately and no checkpoint was ever written
		// for the size-triggered path — recovery then replayed those entries
		// after a crash, which is #948 for the one path that carries production
		// ingest. Only flushBufferLocked (the age, schema-change, FlushAll and
		// Close path) was checkpointing.
		walHashes: collectWALHashes(shard.buffers[bufferKey]),
	}

	select {
	case b.flushQueue <- task:
		b.queueDepth.Add(1)
		// Only now does the buffer stop owning these records.
		delete(shard.deferredKeys, bufferKey)
		delete(shard.buffers, bufferKey)
		delete(shard.bufferStartTimes, bufferKey)
		delete(shard.bufferRecordCounts, bufferKey)
		delete(shard.bufferSchemas, bufferKey)
		return true, false
	case <-b.ctx.Done():
		// Close cancelled b.ctx between the checks above and this select. The
		// records stay in the buffer for Close's shard loop.
		return false, false
	default:
		// Lost the race against another writer for the last slot.
		b.markDeferredLocked(shard, bufferKey)
		return false, true
	}
}

// signalDrain nudges flushDrainer. Non-blocking: the channel is an edge, not a
// queue, and the drainer re-reads real state when it wakes.
func (b *ArrowBuffer) signalDrain() {
	select {
	case b.drainCh <- struct{}{}:
	default:
	}
}

// flushDrainer re-enqueues buffers that were deferred because the flush queue
// was full, as soon as a slot frees.
//
// Without it a deferred buffer is picked up only by a later write to the same
// measurement, by the age-based sweep, or by Close. When ingest moves on to
// other measurements, that buffer sits while flush workers go idle — and the
// age sweep is a single goroutine that holds each shard's lock across its whole
// pass, so with many measurements it is the slowest way to drain anything.
//
// Two properties this loop must have, both learned the hard way:
//
//  1. **It must always reach its outer select.** Close sets b.closing, cancels
//     b.ctx, and only then waits on b.wg — and that wait has no timeout of its
//     own beyond the close budget. A drainer that spun in its inner loop would
//     keep Close from returning, the coordinator would skip the wal-purge and
//     wal components, and the WAL writer's final drain-and-sync would never run,
//     leaving a retained WAL missing its newest entries.
//  2. **It must never retry a buffer that refused to queue, but must skip past
//     one that vanished.** enqueueOrDeferLocked returns (false, _) for a full
//     queue, for shutdown, for losing the last slot, and for a buffer that no
//     longer exists. Retrying the first three is how this loop fails to
//     terminate; ENDING the pass on the fourth strands every other deferred
//     buffer, because a buffer removed by the age sweep or a schema-change flush
//     never touches flushQueue and so generates no signal to wake us again. The
//     two are told apart by whether the key is still in deferredKeys afterwards.
func (b *ArrowBuffer) flushDrainer() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.drainCh:
			b.drainDeferredOnce()
		}
	}
}

// drainDeferredOnce enqueues deferred buffers, oldest first, and returns.
//
// A pass ends on whichever comes first: the attempt budget is spent, the queue is
// full, nothing is deferred, shutdown has begun, or a real buffer refused to
// queue. All of them are reachable — the budget in particular is the ordinary
// exit when starting from an empty queue, since cap(flushQueue) successful
// enqueues spend it before the queue-full check sees a full queue.
func (b *ArrowBuffer) drainDeferredOnce() {
	budget := cap(b.flushQueue)
	if budget < 1 {
		budget = 1
	}

	for attempt := 0; attempt < budget; attempt++ {
		// Re-checked every iteration, not just at the outer select: Close can
		// begin at any point during a drain pass.
		if b.closing.Load() || b.ctx.Err() != nil {
			return
		}
		if len(b.flushQueue) >= cap(b.flushQueue) {
			return
		}

		shard, key, ok := b.oldestDeferred()
		if !ok {
			return
		}

		database, measurement := splitDatabaseAndMeasurement(key)

		shard.mu.Lock()
		// recordCount is read under the lock rather than carried from the
		// snapshot: the snapshot came from a different acquisition and a write
		// may have grown the buffer since, so trusting it would under-report the
		// task's size.
		queued, _ := b.enqueueOrDeferLocked(shard, key, database, measurement, shard.bufferRecordCounts[key])
		// Still deferred means the buffer is real and simply could not be queued.
		// Gone means enqueueOrDeferLocked found no buffer and cleaned the marker,
		// i.e. something else flushed it between the scan and now.
		_, stillDeferred := shard.deferredKeys[key]
		shard.mu.Unlock()

		if queued {
			continue
		}
		if !stillDeferred {
			// The pick vanished. Move on to the next oldest rather than ending
			// the pass: this costs nothing and NOT doing it strands every other
			// deferred buffer, because a buffer removed by the age sweep or a
			// schema-change flush never touches flushQueue and so produces no
			// dequeue signal to wake us again.
			continue
		}
		// A real buffer that would not queue: the queue is full, we lost the last
		// slot, or shutdown began. Retrying this key is how the loop would fail
		// to terminate, so end the pass.
		return
	}
}

// oldestDeferred finds the deferred buffer with the earliest start time across
// all shards. Read-locks one shard at a time; no two shard locks are ever held
// at once anywhere in this file, and that stays true here.
//
// The result is a snapshot: by the time the caller takes the write lock the key
// may be gone. enqueueOrDeferLocked checks for that, which is why this can
// return a stale key safely.
func (b *ArrowBuffer) oldestDeferred() (*bufferShard, string, bool) {
	var (
		oldestShard *bufferShard
		oldestKey   string
		oldest      time.Time
		found       bool
	)
	for i := range b.shards {
		shard := b.shards[i]
		shard.mu.RLock()
		for key := range shard.deferredKeys {
			start, ok := shard.bufferStartTimes[key]
			if !ok {
				// Defensive, and deliberately not load-bearing. No path should
				// leave a deferred key without a start time now — the swept guard
				// moved ahead of the buffer initialisation in this change, and
				// every removal clears deferredKeys. If one ever does appear it is
				// unreachable from here, which is why the removals clear the
				// marker rather than relying on this skip.
				continue
			}
			if !found || start.Before(oldest) {
				oldestShard, oldestKey, oldest, found = shard, key, start, true
			}
		}
		shard.mu.RUnlock()
	}
	return oldestShard, oldestKey, found
}

// splitDatabaseAndMeasurement unpacks a buffer key for the drainer, which only
// has the key. Returns ("", "") for a malformed key, which enqueueOrDeferLocked
// then treats as a normal flush of whatever the buffer holds — the key itself is
// what identifies the buffer, and the pair is only used to build the task.
func splitDatabaseAndMeasurement(bufferKey string) (string, string) {
	parts := splitBufferKey(bufferKey)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// markDeferredLocked records that bufferKey's records are still in this shard
// because no worker could take them, so flushDrainer can come back for it.
// Caller holds shard.mu.
func (b *ArrowBuffer) markDeferredLocked(shard *bufferShard, bufferKey string) {
	shard.deferredKeys[bufferKey] = struct{}{}
	b.recordFlushDeferred()

	// Wake the drainer for THIS deferral. Relying only on the worker's
	// dequeue signal loses a race: a writer can read a full queue, be
	// descheduled, have its token consumed by a drainer pass that finds nothing
	// deferred yet, and only then record the deferral — leaving an idle worker,
	// an empty queue, a deferred buffer and no pending signal. That is the
	// pre-#1008 behaviour, reached through the new code.
	//
	// Cheap even under sustained saturation: a woken drainer checks the queue
	// before it scans anything, so a wake with no room costs one channel receive.
	// Non-blocking, so holding shard.mu here is safe.
	b.signalDrain()
}

// recordFlushDeferred counts a deferral. Before #966 route 1 this condition
// silently dropped the batch, so this counter is the operator-visible
// replacement for a data loss.
func (b *ArrowBuffer) recordFlushDeferred() {
	b.totalFlushDeferred.Add(1)
	metrics.Get().IncIngestFlushDeferred()
}

// logFlushDeferred emits a sampled Warn, on the same sampler pattern as
// recordWALError: sustained backpressure defers on nearly every write to a hot
// measurement, and one line per deferral would bury the signal.
//
// Called OUTSIDE shard.mu.
func (b *ArrowBuffer) logFlushDeferred(bufferKey string, totalBuffered int) {
	now := time.Now().UnixNano()
	last := b.flushDeferredLastLogNano.Load()
	if now-last >= walDropLogIntervalNano && b.flushDeferredLastLogNano.CompareAndSwap(last, now) {
		b.logger.Warn().
			Str("buffer_key", bufferKey).
			Int("records", totalBuffered).
			Int64("deferred_total", b.totalFlushDeferred.Load()).
			Int64("queue_depth", b.queueDepth.Load()).
			Msg("Flush queue full; records retained in memory for a later flush")
	}
}

// schemaEvolutionMaxIters bounds the schema-evolution flush loop.
// Steady state: 1 iteration (no schema change). Adversarial state:
// rotating-schema writers could in principle trigger an unbounded
// loop because flushBufferLocked releases shard.mu for I/O and a
// concurrent writer can install a fresh schema in that window.
//
// Hitting the cap means at least 8 distinct schemas raced through
// the same (database, measurement) buffer in the time window of one
// flush — a sustained-churn signal, not transient. Surfacing this as
// ErrSchemaChurnExceeded lets the caller fail the request with a 503
// rather than committing a wide schema-mixed buffer to disk that
// query-side schema-on-read would then have to reconcile.
const schemaEvolutionMaxIters = 8

// ErrSchemaChurnExceeded is returned by flushOnSchemaChangeLocked when
// schemaEvolutionMaxIters is reached. Treat as a transient backpressure
// signal: the caller should reject the write with a retryable status
// (503) so upstream senders back off; the in-buffer rows for the older
// schemas have already been flushed to durable Parquet by the loop's
// per-iteration flushes, so there is no data loss — only a per-request
// failure under sustained schema-rotation churn.
var ErrSchemaChurnExceeded = errors.New("schema-evolution loop exceeded max iterations: sustained concurrent schema churn against the same (database, measurement)")

// ErrBufferClosing is returned by a write whose shard has already been flushed
// by Close(). Appending after the sweep would leave records that no flush ever
// reaches and that CloseFlushedCleanly does not count, so the write is refused
// instead. Handlers map it to 503: the node is shutting down, and the client
// should retry elsewhere rather than treat it as a permanent server error.
var ErrBufferClosing = errors.New("arrow buffer is closing: write refused")

// flushOnSchemaChangeLocked is the shared helper used by both columnar
// write paths to handle schema evolution under the shard lock.
//
// Caller MUST hold shard.mu. The loop terminates when either:
//  1. The bufferSchemas entry is absent or matches newSignature (steady
//     state — single iteration).
//  2. ctx is cancelled — return ctx.Err() so the caller can abort the
//     write entirely.
//  3. schemaEvolutionMaxIters is reached — return ErrSchemaChurnExceeded
//     so the caller can reject the write with a retryable status (HTTP
//     503). The per-iteration flushes inside the loop already wrote
//     older schemas' rows to durable Parquet, so there is no data loss —
//     only the current request fails under sustained schema-rotation
//     churn.
//
// flushBufferLocked is called inside the loop; it releases-and-
// reacquires shard.mu around its I/O. On flush error the buffer
// entry is still deleted, so the next iteration sees !exists and
// terminates cleanly.
func (b *ArrowBuffer) flushOnSchemaChangeLocked(
	ctx context.Context,
	shard *bufferShard,
	bufferKey, database, measurement, newSignature string,
) error {
	for i := 0; i < schemaEvolutionMaxIters; i++ {
		// ctx-aware: a cancelled request shouldn't be starved by
		// rotating-schema racers. Caller releases lock before
		// surfacing the error.
		if err := ctx.Err(); err != nil {
			return err
		}
		existingSignature, exists := shard.bufferSchemas[bufferKey]
		if !exists || existingSignature == newSignature {
			return nil
		}
		b.logger.Debug().
			Str("buffer_key", bufferKey).
			Str("old_schema", existingSignature).
			Str("new_schema", newSignature).
			Msg("Schema evolution detected, flushing buffer")

		// The flush I/O runs on flushParent, NOT on the request context: these
		// rows belong to whoever wrote them earlier, and a client disconnecting
		// mid-flush must not abort a flush of other clients' acknowledged rows
		// (#1007). The request context still governs the loop itself via the
		// ctx.Err() check above, so a disconnect is observed between
		// iterations rather than in the middle of a write.
		flushCtx, flushCancel := b.newFlushContext()
		err := b.flushBufferLocked(flushCtx, shard, bufferKey, database, measurement)
		flushCancel()
		if err != nil {
			b.logger.Error().Err(err).
				Str("buffer_key", bufferKey).
				Msg("Failed to flush buffer on schema change")
			// flushBufferLocked deletes the buffer entry even on
			// error — the next iteration sees !exists and exits.
		}
	}
	// Reached the iteration cap. Surface as a typed error so callers can
	// reject the request with a retryable 503 rather than silently
	// committing a wide schema-mixed buffer to disk. The per-iteration
	// flushes inside the loop already wrote the older schemas' rows to
	// durable Parquet, so there is no data loss — only this single
	// request fails under sustained schema-rotation churn.
	b.totalSchemaChurnExceeded.Add(1)
	b.logger.Warn().
		Str("buffer_key", bufferKey).
		Int("max_iters", schemaEvolutionMaxIters).
		Msg("Schema-evolution loop hit max iterations; rejecting write with ErrSchemaChurnExceeded")
	return ErrSchemaChurnExceeded
}

// writeColumnar writes a columnar record to the buffer
func (b *ArrowBuffer) writeColumnar(ctx context.Context, database string, record *models.ColumnarRecord) error {
	return b.writeColumnarInternal(ctx, database, record, false, "")
}

func (b *ArrowBuffer) writeColumnarInternal(ctx context.Context, database string, record *models.ColumnarRecord, skipWAL bool, inheritedWALIdentity string) error {
	// Create buffer key: database/measurement
	// OPTIMIZATION: String concatenation is faster than fmt.Sprintf (no reflection)
	bufferKey := database + "/" + record.Measurement

	// WAL: Write to WAL before buffering (if enabled)
	// Skip WAL during recovery to avoid re-writing recovered data
	var walHashes []string
	// A replayed entry writes nothing to the WAL — the copy being replayed is
	// already on disk — but it DOES carry that copy's identity, so the flush
	// that eventually persists it checkpoints the original entry and no later
	// recovery pass replays it again. Only a 32-hex tracked identity is
	// inherited: an untracked entry's identity is a 64-hex CONTENT hash, and
	// two legitimately identical payloads share it, so checkpointing one would
	// make recovery skip the other (#998 moved off content hashes for exactly
	// this reason). Untracked entries keep replaying as before.
	if skipWAL && len(inheritedWALIdentity) == walTrackedIdentityHexLen {
		walHashes = []string{inheritedWALIdentity}
	}
	if b.wal != nil && !skipWAL {
		tracked, canTrack := b.wal.(trackedWALWriter)
		// ZERO-COPY PATH: Use raw msgpack bytes if available (avoids re-serialization)
		if len(record.RawPayload) > 0 {
			var err error
			if canTrack {
				walHashes, err = tracked.AppendRawWithMetaTracked(database, record.RawPayload)
			} else {
				err = b.wal.AppendRawWithMeta(database, record.RawPayload)
			}
			if err != nil {
				// Don't fail the write — WAL is for durability, not
				// correctness. recordWALError differentiates backpressure
				// drops (sampled Warn) from real I/O failures (unsampled
				// Error) so operators can alert on the right signal.
				b.recordWALError(err, func(ev *zerolog.Event) {
					ev.Str("database", database).
						Str("measurement", record.Measurement).
						Int("payload_size", len(record.RawPayload))
				})
			}
		} else {
			// FALLBACK: Convert columnar to row format for WAL storage
			// This path is used for LineProtocol or when raw bytes aren't available.
			walRecords := b.columnarToWALRecords(database, record)
			if len(walRecords) > 0 {
				// Row-format entries need the database envelope too. AppendTracked
				// historically wrote the raw row payload, so replication parsed
				// those entries with the receiver's "default" database (#889).
				rowPayload, marshalErr := msgpack.Marshal(walRecords)
				var err error
				if marshalErr != nil {
					err = marshalErr
				} else if canTrack {
					walHashes, err = tracked.AppendRawWithMetaTracked(database, rowPayload)
				} else {
					err = b.wal.AppendRawWithMeta(database, rowPayload)
				}
				if err != nil {
					b.recordWALError(err, func(ev *zerolog.Event) {
						ev.Str("database", database).
							Str("measurement", record.Measurement).
							Int("records", len(walRecords))
					})
				}
			}
		}
	}

	// Convert []interface{} columns to typed arrays (optimized with zero-copy fast paths)
	// From here to the line that puts this batch into shard.buffers, every
	// return abandons a write whose WAL append has already happened. One
	// deferred release covers all of them — including returns added later,
	// which is the point: releasing at each error site is a contract the next
	// edit silently breaks, and a single leaked identity pins the WAL purge
	// floor for the life of the process. PurgeFlushed stops at the first
	// retained file rather than skipping it, so nothing after that file is
	// reclaimed either and the WAL grows until the disk fills (#676).
	//
	// Deliberately not MarkFlushed: nothing reached storage, so there is no
	// checkpoint to write. It only stops the floor waiting for data that no
	// buffer holds.
	//
	// Only for identities this write MINTED. On the replay path walHashes is
	// the identity inherited from the entry being replayed (#1048), and that
	// identity belongs to a WAL file whose keep-or-delete decision is
	// recovery's — releasing it would drop the floor below a sequence that is
	// still legitimately unflushed, and the periodic flush-failure recovery
	// replays THIS process's own files, so the instance prefix matches and the
	// release really lands.
	bufferCommitted := skipWAL
	defer func() {
		if !bufferCommitted && len(walHashes) > 0 {
			if tracked, ok := b.wal.(trackedWALWriter); ok {
				tracked.ForgetTracked(walHashes)
			}
		}
	}()
	typedColumns, numRecords, err := b.convertColumnsToTyped(record.Measurement, record.Columns)
	if err != nil {
		return fmt.Errorf("failed to convert columns: %w", err)
	}

	// Propagate tag column names for Parquet metadata (enables auto-dedup in compaction)
	typedColumns.TagColumns = record.TagColumns
	// Propagate the dedup-on-time marker (CQ output only — see ColumnarRecord.DedupTime)
	typedColumns.DedupTime = record.DedupTime
	typedColumns.WALHashes = walHashes

	// Column signature for schema evolution detection (pre-computed in convertColumnsToTyped)
	newSignature := typedColumns.Signature
	if newSignature == "" && len(typedColumns.Data) > 0 {
		newSignature = getColumnSignature(typedColumns.Data)
	}

	// OPTIMIZATION: Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	// OPTIMIZATION: Extract-then-flush pattern
	// Hold lock ONLY to extract records, flush outside lock
	var shouldFlush, flushDeferred bool

	shard.mu.Lock()

	// Schema evolution detection: flush buffer if columns changed.
	// The loop guards against the I/O window inside flushBufferLocked
	// where a concurrent writer can install a third schema; see
	// flushOnSchemaChangeLocked for the full rationale.
	if err := b.flushOnSchemaChangeLocked(ctx, shard, bufferKey, database, record.Measurement, newSignature); err != nil {
		shard.mu.Unlock()
		return err
	}

	// Initialize buffer and record count if needed
	// Checked BEFORE the initialisation below, not after it. The old position
	// left bufferStartTimes, bufferRecordCounts and bufferSchemas entries behind
	// for a refused write, with no matching shard.buffers entry — a phantom the
	// age sweep and the next-deadline computation both walk over.
	// Refuse once Close() has swept THIS shard: anything appended after the
	// sweep is never flushed and is not counted by CloseFlushedCleanly, so the
	// shutdown WAL purge would delete its only other copy (#1007).
	//
	// The check belongs here, after flushOnSchemaChangeLocked and immediately
	// before the append with no unlock in between. A check right after
	// shard.mu.Lock() would be bypassed: flushBufferLocked releases and
	// re-acquires the lock around its I/O, so a writer could pass the check,
	// the schema flush could unlock, Close() could sweep this shard, and the
	// writer would then append into it.
	//
	// It is per-shard rather than the global b.closing flag on purpose; see
	// bufferShard.swept for why refusing on the global flag loses replicated
	// rows that have no WAL copy of their own.
	//
	// The record is already in the WAL when the WAL is enabled (that append
	// precedes this lock), so count it as WAL-only: CloseFlushedCleanly then
	// stays false and the shutdown purge is skipped.
	if shard.swept {
		shard.mu.Unlock()
		b.walOnlyRecords.Add(int64(numRecords))
		metrics.Get().IncWALRecordsPreserved(int64(numRecords))
		return ErrBufferClosing
	}

	if _, exists := shard.buffers[bufferKey]; !exists {
		shard.bufferStartTimes[bufferKey] = time.Now().UTC()
		shard.bufferRecordCounts[bufferKey] = 0
		shard.bufferSchemas[bufferKey] = newSignature // Store schema for evolution detection
		// Tell periodicFlush to recompute its wakeup time for this new buffer.
		select {
		case b.newBufferCh <- struct{}{}:
		default:
		}
	}

	// Add typed columns to buffer (already converted via zero-copy fast paths)
	shard.buffers[bufferKey] = append(shard.buffers[bufferKey], typedColumns)
	// The buffer owns these records now, so the deferred release must not fire.
	bufferCommitted = true

	// CRITICAL FIX: Track count incrementally instead of O(n) loop
	shard.bufferRecordCounts[bufferKey] += numRecords
	totalBuffered := shard.bufferRecordCounts[bufferKey]

	// Check if buffer needs flush (size-based)
	if totalBuffered >= b.config.MaxBufferSize {
		shouldFlush, flushDeferred = b.enqueueOrDeferLocked(shard, bufferKey, database, record.Measurement, totalBuffered)
	}

	// Release lock IMMEDIATELY (lock held for <1ms)
	shard.mu.Unlock()

	// Logging and metrics outside the lock: the send itself is non-blocking, but
	// there is no reason to hold a shard lock across zerolog formatting.
	if shouldFlush {
		metrics.Get().SetBufferQueueDepth(b.queueDepth.Load())
		b.logger.Info().
			Str("buffer_key", bufferKey).
			Int("total_records", totalBuffered).
			Int64("queue_depth", b.queueDepth.Load()).
			Msg("Buffer size exceeded, queued flush to worker pool")
	}
	if flushDeferred {
		b.logFlushDeferred(bufferKey, totalBuffered)
	}

	// OPTIMIZATION: Update metrics with atomic operations (lock-free!)
	b.totalRecordsBuffered.Add(int64(numRecords))

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added columnar data to buffer")

	// Return immediately (don't wait for flush to complete!)
	return nil
}

// writeTypedColumnarInternal writes a pre-typed column batch to the buffer.
// Mirrors writeColumnarInternal but skips convertColumnsToTyped since the batch
// is already typed ([]int64, []float64, []string). Used by format-specific parsers
// that know column types at compile time.
func (b *ArrowBuffer) writeTypedColumnarInternal(ctx context.Context, database, measurement string, typedColumns *TypedColumnBatch, numRecords int, skipWAL bool) error {
	return b.writeTypedColumnarRaw(ctx, database, measurement, typedColumns, numRecords, nil, skipWAL)
}

// writeTypedColumnarRaw is writeTypedColumnarInternal with an optional raw
// msgpack payload for the zero-copy WAL path. When rawPayload is non-empty
// the WAL stores the original client bytes (AppendRawWithMeta, same as
// writeColumnarInternal's fast path); otherwise the typed batch is
// transposed to row records — the pre-existing typed fallback, which is
// lossy for NULLs (typedBatchToWALRecords ignores Validity; see plan doc).
// The typed msgpack decode path always supplies rawPayload, so it never
// takes the lossy fallback.
func (b *ArrowBuffer) writeTypedColumnarRaw(ctx context.Context, database, measurement string, typedColumns *TypedColumnBatch, numRecords int, rawPayload []byte, skipWAL bool) error {
	bufferKey := database + "/" + measurement
	var walHashes []string

	// WAL: raw client bytes when available (zero-copy), row transpose otherwise
	if b.wal != nil && !skipWAL {
		tracked, canTrack := b.wal.(trackedWALWriter)
		if len(rawPayload) > 0 {
			var err error
			if canTrack {
				walHashes, err = tracked.AppendRawWithMetaTracked(database, rawPayload)
			} else {
				err = b.wal.AppendRawWithMeta(database, rawPayload)
			}
			if err != nil {
				b.recordWALError(err, func(ev *zerolog.Event) {
					ev.Str("database", database).
						Str("measurement", measurement).
						Int("payload_size", len(rawPayload))
				})
			}
		} else {
			walRecords := typedBatchToWALRecords(database, measurement, typedColumns, numRecords, b.getDecimalColumns(measurement))
			if len(walRecords) > 0 {
				var err error
				if canTrack {
					walHashes, err = tracked.AppendTracked(walRecords)
				} else {
					err = b.wal.Append(walRecords)
				}
				if err != nil {
					b.recordWALError(err, func(ev *zerolog.Event) {
						ev.Str("database", database).
							Str("measurement", measurement).
							Int("records", len(walRecords))
					})
				}
			}
		}
	}
	typedColumns.WALHashes = walHashes
	// From here to the line that puts this batch into shard.buffers, every
	// return abandons a write whose WAL append has already happened. One
	// deferred release covers all of them — including returns added later,
	// which is the point: releasing at each error site is a contract the next
	// edit silently breaks, and a single leaked identity pins the WAL purge
	// floor for the life of the process. PurgeFlushed stops at the first
	// retained file rather than skipping it, so nothing after that file is
	// reclaimed either and the WAL grows until the disk fills (#676).
	//
	// Deliberately not MarkFlushed: nothing reached storage, so there is no
	// checkpoint to write. It only stops the floor waiting for data that no
	// buffer holds.
	//
	// Only for identities this write MINTED. On the replay path walHashes is
	// the identity inherited from the entry being replayed (#1048), and that
	// identity belongs to a WAL file whose keep-or-delete decision is
	// recovery's — releasing it would drop the floor below a sequence that is
	// still legitimately unflushed, and the periodic flush-failure recovery
	// replays THIS process's own files, so the instance prefix matches and the
	// release really lands.
	bufferCommitted := skipWAL
	defer func() {
		if !bufferCommitted && len(walHashes) > 0 {
			if tracked, ok := b.wal.(trackedWALWriter); ok {
				tracked.ForgetTracked(walHashes)
			}
		}
	}()

	// Column signature for schema evolution detection (pre-computed in convertColumnsToTyped)
	newSignature := typedColumns.Signature
	if newSignature == "" && len(typedColumns.Data) > 0 {
		newSignature = getColumnSignature(typedColumns.Data)
	}

	// Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	var shouldFlush, flushDeferred bool

	shard.mu.Lock()

	// Schema evolution detection: see flushOnSchemaChangeLocked.
	if err := b.flushOnSchemaChangeLocked(ctx, shard, bufferKey, database, measurement, newSignature); err != nil {
		shard.mu.Unlock()
		return err
	}

	// Initialize buffer and record count if needed
	// Checked BEFORE the initialisation below, not after it. The old position
	// left bufferStartTimes, bufferRecordCounts and bufferSchemas entries behind
	// for a refused write, with no matching shard.buffers entry — a phantom the
	// age sweep and the next-deadline computation both walk over.
	// Refuse once Close() has swept THIS shard: anything appended after the
	// sweep is never flushed and is not counted by CloseFlushedCleanly, so the
	// shutdown WAL purge would delete its only other copy (#1007).
	//
	// The check belongs here, after flushOnSchemaChangeLocked and immediately
	// before the append with no unlock in between. A check right after
	// shard.mu.Lock() would be bypassed: flushBufferLocked releases and
	// re-acquires the lock around its I/O, so a writer could pass the check,
	// the schema flush could unlock, Close() could sweep this shard, and the
	// writer would then append into it.
	//
	// It is per-shard rather than the global b.closing flag on purpose; see
	// bufferShard.swept for why refusing on the global flag loses replicated
	// rows that have no WAL copy of their own.
	//
	// The record is already in the WAL when the WAL is enabled (that append
	// precedes this lock), so count it as WAL-only: CloseFlushedCleanly then
	// stays false and the shutdown purge is skipped.
	if shard.swept {
		shard.mu.Unlock()
		b.walOnlyRecords.Add(int64(numRecords))
		metrics.Get().IncWALRecordsPreserved(int64(numRecords))
		return ErrBufferClosing
	}

	if _, exists := shard.buffers[bufferKey]; !exists {
		shard.bufferStartTimes[bufferKey] = time.Now().UTC()
		shard.bufferRecordCounts[bufferKey] = 0
		shard.bufferSchemas[bufferKey] = newSignature
		// Tell periodicFlush to recompute its wakeup time for this new buffer.
		select {
		case b.newBufferCh <- struct{}{}:
		default:
		}
	}

	// Add typed columns to buffer directly (no conversion needed)
	shard.buffers[bufferKey] = append(shard.buffers[bufferKey], typedColumns)
	// The buffer owns these records now, so the deferred release must not fire.
	bufferCommitted = true

	shard.bufferRecordCounts[bufferKey] += numRecords
	totalBuffered := shard.bufferRecordCounts[bufferKey]

	// Check if buffer needs flush (size-based)
	if totalBuffered >= b.config.MaxBufferSize {
		shouldFlush, flushDeferred = b.enqueueOrDeferLocked(shard, bufferKey, database, measurement, totalBuffered)
	}

	shard.mu.Unlock()

	if shouldFlush {
		metrics.Get().SetBufferQueueDepth(b.queueDepth.Load())
		b.logger.Info().
			Str("buffer_key", bufferKey).
			Int("total_records", totalBuffered).
			Int64("queue_depth", b.queueDepth.Load()).
			Msg("Buffer size exceeded, queued flush to worker pool")
	}
	if flushDeferred {
		b.logFlushDeferred(bufferKey, totalBuffered)
	}

	b.totalRecordsBuffered.Add(int64(numRecords))

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added typed columnar data to buffer")

	return nil
}

// typedBatchToWALRecords converts a TypedColumnBatch to row-format records for WAL storage.
// This is the WAL fallback path for typed batches (e.g., TLE) that don't have raw msgpack bytes.
func typedBatchToWALRecords(database, measurement string, batch *TypedColumnBatch, numRecords int, decimalCols map[string]config.DecimalSpec) []map[string]interface{} {
	if numRecords == 0 {
		return nil
	}

	records := make([]map[string]interface{}, numRecords)
	for i := 0; i < numRecords; i++ {
		row := map[string]interface{}{
			"_database":    database,
			"_measurement": measurement,
		}
		for colName, colData := range batch.Data {
			switch arr := colData.(type) {
			case []int64:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []float64:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []string:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []bool:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []decimal128.Num:
				// WAL stores decimals as float64 (lossy but WAL is recovery-only)
				if i < len(arr) {
					s := int32(0)
					if decimalCols != nil {
						if spec, ok := decimalCols[colName]; ok {
							s = spec.Scale
						}
					}
					f := arr[i].ToBigFloat(s)
					row[colName], _ = f.Float64()
				}
			}
		}
		records[i] = row
	}

	return records
}

// convertColumnsToTyped converts []interface{} columns to typed arrays with null tracking.
// Returns a TypedColumnBatch where Validity maps track which values are null (false=null).
// Columns with no nil values have no entry in Validity (all valid).
// ZERO-COPY OPTIMIZATION: Try bulk type assertion first before element-by-element conversion
func (b *ArrowBuffer) convertColumnsToTyped(measurement string, columns map[string][]interface{}) (*TypedColumnBatch, int, error) {
	typed := make(map[string]interface{})
	validity := make(map[string][]bool)
	var numRecords int

	// Look up decimal column config for this measurement (nil if none configured)
	decimalCols := b.getDecimalColumns(measurement)

	for name, col := range columns {
		if len(col) == 0 {
			continue
		}

		// Set record count from first column
		if numRecords == 0 {
			numRecords = len(col)
		}

		// Check if this column is declared as decimal — override normal type inference
		if decimalCols != nil {
			if spec, isDecimal := decimalCols[name]; isDecimal {
				arr, valid, err := convertToDecimal128Slice(col, spec.Precision, spec.Scale)
				if err != nil {
					return nil, 0, fmt.Errorf("decimal conversion error in column '%s': %w", name, err)
				}
				typed[name] = arr
				if valid != nil {
					validity[name] = valid
				}
				continue
			}
		}

		// Infer type from first non-nil value
		firstVal := firstNonNil(col)
		if firstVal == nil {
			// Every value is nil, so there is nothing to infer a type from.
			// Dropping the column would make it vanish from this batch's
			// parquet file; a column that is all-nil in every batch would then
			// never exist at all, and querying it fails to bind instead of
			// returning NULLs (#337).
			//
			// Emit it as an all-null string column instead. The value is
			// correct either way — every entry is NULL — and string is the
			// safe placeholder: a later batch carrying real values writes its
			// own inferred type, and readers union across files by name
			// (read_parquet union_by_name=true), so the type only has to be
			// consistent within a file, not across them.
			//
			// Time is exempt: a partition whose time column is VARCHAR cannot be
			// compacted (TIMESTAMP != VARCHAR bind failure), so an all-nil time
			// is rejected here rather than written as a string. The msgpack path
			// already rejects it upstream in normalizeTimestamps, but this
			// function is the chokepoint every typed write passes through, so
			// the guard belongs here too.
			if name == "time" {
				return nil, 0, fmt.Errorf("time column contains only null values in measurement '%s' (writer must send an integer/float timestamp)", measurement)
			}

			arr := make([]string, len(col))
			valid := make([]bool, len(col)) // all false — every entry is NULL
			typed[name] = arr
			validity[name] = valid
			continue
		}

		// The "time" column is always int64 microseconds → Arrow Timestamp.
		// Force it here so a client that sends time as a string (or float) can
		// never produce a VARCHAR/DOUBLE time parquet file, which would make a
		// partition un-compactable (TIMESTAMP != VARCHAR bind failure). The
		// msgpack columnar path already normalizes time via normalizeTimestamps,
		// but this is the single chokepoint every typed write passes through, so
		// enforce it here regardless of source. No extra pass: it replaces the
		// generic type switch for this one column. Reject (not coerce) a
		// non-numeric time so the bad writer is surfaced loudly at ingest.
		if name == "time" {
			if _, ok := firstVal.(string); ok {
				return nil, 0, fmt.Errorf("time column must be numeric epoch, got string in measurement '%s' (writer must send an integer/float timestamp, not a string)", measurement)
			}
			arr := make([]int64, len(col))
			for i, v := range col {
				// Fast path: time is overwhelmingly int64 in production, so a
				// direct assertion avoids toInt64's call + multi-case type
				// switch on the hot path. Fall back to toInt64 for other
				// numeric kinds (float64, uints from some msgpack decoders).
				if ts, ok := v.(int64); ok {
					arr[i] = ts
					continue
				}
				// Reject null time: groupByHour reads the time slice directly
				// (no validity check), so a nil would become 0 and silently
				// route the record to the 1970-01-01 partition.
				if v == nil {
					return nil, 0, fmt.Errorf("time column cannot contain null values in measurement '%s'", measurement)
				}
				ts, ok := toInt64(v)
				if !ok {
					return nil, 0, fmt.Errorf("time column value %T not convertible to int64 in measurement '%s'", v, measurement)
				}
				arr[i] = ts
			}
			typed[name] = arr
			continue
		}

		// FAST PATH: Try zero-copy bulk conversion first (fails fast on nils or mixed types).
		// If zero-copy succeeds, no nils exist and no validity bitmap is needed.
		switch firstVal.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			if arr, ok := b.tryInt64ZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			// Zero-copy failed (has nils or mixed types) — single-pass conversion with validity
			arr := make([]int64, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				val, ok := toInt64(v)
				if !ok {
					return nil, 0, fmt.Errorf("cannot convert %T to int64 in column '%s'", v, name)
				}
				arr[i] = val
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		case float32, float64:
			if arr, ok := b.tryFloat64ZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			arr := make([]float64, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				val, ok := toFloat64(v)
				if !ok {
					return nil, 0, fmt.Errorf("cannot convert %T to float64 in column '%s'", v, name)
				}
				arr[i] = val
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		case string:
			if arr, ok := b.tryStringZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			arr := make([]string, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				str, ok := v.(string)
				if !ok {
					return nil, 0, fmt.Errorf("unexpected type in string column '%s': %T", name, v)
				}
				arr[i] = str
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		case bool:
			if arr, ok := b.tryBoolZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			arr := make([]bool, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				bval, ok := v.(bool)
				if !ok {
					return nil, 0, fmt.Errorf("unexpected type in bool column '%s': %T", name, v)
				}
				arr[i] = bval
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		default:
			return nil, 0, fmt.Errorf("unsupported column type for '%s': %T", name, firstVal)
		}
	}

	batch := &TypedColumnBatch{Data: typed, Validity: validity, Signature: getColumnSignature(typed)}
	return batch, numRecords, nil
}

// convertToDecimal128Slice converts a []interface{} column to []decimal128.Num.
// Accepts float64, float32, int64, int*, uint*, and string values.
// Returns the typed array, optional validity bitmap (nil if no nulls), and error.
func convertToDecimal128Slice(col []interface{}, precision, scale int32) ([]decimal128.Num, []bool, error) {
	arr := make([]decimal128.Num, len(col))
	var valid []bool

	for i, v := range col {
		if v == nil {
			if valid == nil {
				valid = make([]bool, len(col))
				for j := 0; j < i; j++ {
					valid[j] = true
				}
			}
			continue
		}
		if valid != nil {
			valid[i] = true
		}

		var num decimal128.Num
		var err error

		switch val := v.(type) {
		case float64:
			num, err = decimal128.FromFloat64(val, precision, scale)
		case float32:
			num, err = decimal128.FromFloat64(float64(val), precision, scale)
		case int64:
			num, err = decimal128.FromString(strconv.FormatInt(val, 10), precision, scale)
		case int:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int32:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int16:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int8:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case uint64:
			num, err = decimal128.FromString(strconv.FormatUint(val, 10), precision, scale)
		case uint:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint32:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint16:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint8:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case string:
			num, err = decimal128.FromString(val, precision, scale)
		default:
			return nil, nil, fmt.Errorf("cannot convert %T to decimal128 at row %d", v, i)
		}

		if err != nil {
			return nil, nil, fmt.Errorf("row %d: %w", i, err)
		}
		arr[i] = num
	}

	return arr, valid, nil
}

// ZERO-COPY HELPERS: Try bulk type assertion for homogeneous arrays

// tryInt64ZeroCopy attempts zero-copy conversion for homogeneous int64 arrays.
// Single-pass: allocates and fills in one scan. Returns nil on first nil/type-mismatch,
// paying only the GC cost of discarding the partial allocation — which is rare in practice.
func (b *ArrowBuffer) tryInt64ZeroCopy(col []interface{}) ([]int64, bool) {
	arr := make([]int64, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(int64)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// tryFloat64ZeroCopy attempts zero-copy conversion for homogeneous float64 arrays.
// Single-pass: allocates and fills in one scan.
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

// tryStringZeroCopy attempts zero-copy conversion for homogeneous string arrays.
// Single-pass: allocates and fills in one scan.
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

// tryBoolZeroCopy attempts zero-copy conversion for homogeneous bool arrays.
// Single-pass: allocates and fills in one scan.
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

// periodicFlush runs in the background and flushes old buffers.
// It uses a self-adjusting timer that fires exactly when the oldest buffer is due
// to expire, eliminating the phase-misalignment lag of a fixed-period ticker.
// metricsSampler refreshes the buffer gauges on a fixed cadence.
//
// The counters (flushes, records written) can be published when a flush
// completes, but arc_buffer_records_buffered is a live gauge: it must reflect
// records sitting in the buffer *right now*. Publishing it only after a flush
// would always report 0, because the flush is what empties the buffer — which
// is exactly the backpressure case an operator needs to see (#802).
//
// One second is fine-grained enough to catch a growing backlog and coarse
// enough that the shard scan (a read-lock per shard over a small map) is
// irrelevant next to ingest work.
func (b *ArrowBuffer) metricsSampler() {
	defer b.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.publishBufferMetrics()
			// Only on the sampler, not inside publishBufferMetrics:
			// publishBufferMetrics also runs once per flushed file, and this
			// walks every shard under RLock — 32 lock round-trips per flush on
			// locks writers contend for, for a gauge that needs 1 Hz.
			metrics.Get().SetBufferDeferredBuffers(b.countDeferredBuffers())
		}
	}
}

func (b *ArrowBuffer) periodicFlush() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return

		case <-b.newBufferCh:
			// A new buffer was created. Only reset the timer if the oldest
			// buffer's expiry is earlier than the currently scheduled deadline.
			// In practice this only triggers the idle→active transition: once
			// the timer is armed, a new buffer (expiry = now+maxAge) is always
			// later than any existing buffer's expiry, so the condition is false
			// and we skip the expensive shard scan entirely.
			nextDeadline := b.computeNextFlushDeadline()
			if nextDeadline.Before(b.flushDeadline) {
				if !b.flushTimer.Stop() {
					select {
					case <-b.flushTimer.C:
					default:
					}
				}
				b.flushDeadline = nextDeadline
				b.flushTimer.Reset(time.Until(nextDeadline))
			}

		case <-b.flushTimer.C:
			b.flushAgedBuffers()
			// Rearm the timer for the next oldest buffer expiry.
			b.flushDeadline = b.computeNextFlushDeadline()
			b.flushTimer.Reset(time.Until(b.flushDeadline))
		}
	}
}

// computeNextFlushDeadline returns the absolute time when the oldest buffered
// key is due to be flushed. If no buffers exist it returns now+maxAge so the
// goroutine sleeps until a new buffer signals it via newBufferCh.
// Returning an absolute time avoids drift from a second time.Now() call at
// the call site.
func (b *ArrowBuffer) computeNextFlushDeadline() time.Time {
	now := time.Now().UTC()
	maxAge := b.maxBufferAge
	earliest := now.Add(maxAge) // default when no buffers exist

	for _, shard := range b.shards {
		shard.mu.RLock()
		for _, startTime := range shard.bufferStartTimes {
			if expiry := startTime.Add(maxAge); expiry.Before(earliest) {
				earliest = expiry
			}
		}
		shard.mu.RUnlock()
	}

	// Clamp to at least 1ms in the future so Reset never gets zero/negative.
	if earliest.Before(now.Add(time.Millisecond)) {
		return now.Add(time.Millisecond)
	}
	return earliest
}

// flushAgedBuffers flushes buffers that have exceeded max age
func (b *ArrowBuffer) flushAgedBuffers() {
	now := time.Now().UTC()
	maxAge := b.maxBufferAge

	threshold := maxAge

	// Iterate over all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		// Check each buffer in this shard for age
		for key, startTime := range shard.bufferStartTimes {
			// Stop the sweep once Close() has begun and leave the rest to
			// Close()'s own shard loop. This sweep is a single serial goroutine
			// over every shard, and Close() waits for it via b.wg.Wait(); with
			// many aged buffers on slow storage, finishing the whole sweep can
			// outlast the shutdown budget.
			//
			// The unlock is REQUIRED: shard.mu is held across this entire loop
			// (released only inside flushBufferLocked, around its I/O). A bare
			// return would leak the lock, and Close()'s shard loop — plus every
			// later write to this shard — would block on it forever.
			if b.closing.Load() {
				shard.mu.Unlock()
				b.logger.Info().
					Int("shard", shardIdx).
					Msg("Aged-buffer sweep stopping: buffer is closing, remaining buffers left to Close()")
				return
			}
			age := now.Sub(startTime)
			if age >= threshold {
				b.logger.Info().
					Str("buffer_key", key).
					Dur("age", age).
					Dur("threshold", threshold).
					Int("shard", shardIdx).
					Msg("Flushing aged buffer")

				// Parse buffer key to get database and measurement
				parts := splitBufferKey(key)
				if len(parts) != 2 {
					b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format")
					continue
				}

				// flushParent-derived: Close() cancelling b.ctx must not abort
				// an aged flush mid-write, because flushBufferLocked has
				// already deleted the buffer entry (#1007).
				flushCtx, flushCancel := b.newFlushContext()
				if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1]); err != nil {
					b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush aged buffer")
				}
				flushCancel()
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
		case task, ok := <-b.flushQueue:
			if !ok {
				// Channel closed during shutdown
				return
			}
			metrics.Get().SetBufferQueueDepth(b.queueDepth.Add(-1))
			// Signal here, not only after the flush: the queue slot frees at
			// RECEIVE, so waiting for completion delays the drain by a whole
			// flush time and lets the write path win the slot instead.
			b.signalDrain()

			b.logger.Debug().
				Int("worker_id", workerID).
				Str("buffer_key", task.bufferKey).
				Int("records", task.recordCount).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Worker processing flush task")

			// Build the timeout HERE, not at enqueue: it must measure the
			// storage write, not the time this task waited in the queue
			// (#1006). It derives from flushParent, so Close() cancelling
			// b.ctx does not abort a write already in progress (#1007).
			flushCtx, flushCancel := b.newFlushContext()
			// Error is already logged and recorded by flushRecordsAsync via
			// markFlushFailure; the worker has nowhere to return it to.
			_ = b.flushRecordsAsync(flushCtx, task.bufferKey, task.database, task.measurement, task.records, task.recordCount, task.walHashes)
			flushCancel()
		}
	}
}

// flushRecordsAsync merges and writes one extracted batch.
//
// It returns an error as well as recording the failure through
// markFlushFailure: Close() drains the queue and flushes each remaining task
// itself, and it has to know WHICH tasks failed in order to count only those as
// WAL-only. markFlushFailure alone cannot say — hasFlushFailure is a
// process-wide latch with no per-task attribution.
func (b *ArrowBuffer) flushRecordsAsync(ctx context.Context, bufferKey, database, measurement string, records []interface{}, recordCount int, walHashes []string) error {
	startTime := time.Now()

	// Merge typed column batches
	merged, err := b.mergeBatches(records)

	// MEMORY FIX: Clear batch references immediately after merge to allow GC
	// The merged map now owns all the data, original batches can be collected
	for i := range records {
		records[i] = nil
	}

	if err != nil {
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Msg("Failed to merge batches during async flush")

		b.markFlushFailure()
		// Data is already in WAL (written at ingest time) - no need to restore to buffer
		// WAL will be replayed on restart or via periodic recovery
		return err
	}

	// Flush with data timestamp partitioning
	if err := b.flushWithDataTimePartitioning(ctx, bufferKey, database, measurement, merged, recordCount, startTime); err != nil {
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Int("records", recordCount).
			Msg("Failed to flush - data preserved in WAL for recovery")
		b.markFlushFailure()
		// Data is already in WAL (written at ingest time) - no memory growth
		// WAL will be replayed on restart or via periodic recovery
		return err
	}
	b.markWALFlushed(walHashes)
	return nil
}

func (b *ArrowBuffer) markWALFlushed(hashes []string) {
	if len(hashes) == 0 || b.wal == nil {
		return
	}
	tracked, ok := b.wal.(trackedWALWriter)
	if !ok {
		return
	}
	if err := tracked.MarkFlushed(hashes); err != nil {
		b.logger.Error().Err(err).Int("entries", len(hashes)).Msg("Failed to write WAL flush checkpoint")
		// The data IS in storage; only its checkpoint is missing. Holding the
		// purge floor here would be waiting for a flush that already
		// happened, and MarkFlushed writes its batches in order, so a failure
		// part-way leaves every later identity pinned forever. Release them:
		// the cost of a missing checkpoint is that recovery may replay the
		// entry, which #1048 made idempotent — a duplicate risk, not a loss
		// risk — whereas a pinned floor stops the WAL reclaiming anything.
		tracked.ForgetTracked(hashes)
	}
}

// flushWithDataTimePartitioning partitions data by data timestamps (async path)
func (b *ArrowBuffer) flushWithDataTimePartitioning(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeAsync, startTime)
}

// flushPartitionedData is the shared core logic for partitioning and writing data by hour boundaries
// Called by both async (flushWithDataTimePartitioning) and sync (flushBufferLockedDataTime) paths
// Uses hash-based grouping to partition by hour, then sorts each hour independently
func (b *ArrowBuffer) flushPartitionedData(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, flushType string, startTime time.Time) error {
	// Get sort keys for this measurement (guaranteed to include "time")
	sortKeys := b.getSortKeys(measurement)

	// Get decimal column config for this measurement (nil if none configured)
	decimalCols := b.getDecimalColumns(measurement)

	// Extract time column (doesn't need to be sorted yet)
	times, ok := merged.Data["time"].([]int64)
	if !ok || len(times) == 0 {
		return fmt.Errorf("no time data in batch")
	}

	// Cheap O(n) min/max scan to decide single- vs multi-hour. The full per-hour
	// bucketing (groupByHour) allocates an index slice covering every row and is only
	// needed when the flush actually spans multiple hours — which the common live-ingest
	// case (all rows in the current hour) never does. Deferring it past the single-hour
	// check saved ~15GB of bucket-index allocation per the 2026-06-22 profile.
	// Two unconditional comparisons (not else-if) on purpose: live ingest arrives
	// monotonically ascending, so `t < globalMin` is reliably false and `t > globalMax`
	// reliably true — both branches are near-perfectly predicted and run in parallel.
	// An else-if makes the second comparison data-dependent on the first and measured
	// ~24% slower on ascending input (benchmarked 5M rows: 1.25ms vs 1.55ms).
	globalMin, globalMax := times[0], times[0]
	for _, t := range times {
		if t < globalMin {
			globalMin = t
		}
		if t > globalMax {
			globalMax = t
		}
	}

	minTime := time.UnixMicro(globalMin).UTC()
	maxTime := time.UnixMicro(globalMax).UTC()

	// Log warning if data is significantly old or in the future
	now := time.Now().UTC()
	if minTime.Before(now.AddDate(0, 0, -7)) {
		b.logger.Warn().
			Time("data_time", minTime).
			Str("buffer_key", bufferKey).
			Msg("Data timestamp is >7 days old - possible backfill or clock skew")
	} else if minTime.After(now.Add(time.Hour)) {
		b.logger.Warn().
			Time("data_time", minTime).
			Str("buffer_key", bufferKey).
			Msg("Data timestamp is >1 hour in future - possible clock skew")
	}

	// OPTIMIZATION: If batch fits within single hour, skip splitting
	// Check if min and max fall within the same hour
	minHour := minTime.Truncate(time.Hour)
	maxHour := maxTime.Truncate(time.Hour)
	if minHour.Equal(maxHour) {
		// Single hour - sort once and write one file
		sorted := sortTypedColumnBatchByKeys(merged, sortKeys)

		parquetData, fileSchema, err := b.writer.writeParquetColumnarWithSchema(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, sorted.DedupTime, decimalCols)
		if err != nil {
			return fmt.Errorf("failed to write Parquet: %w", err)
		}

		storagePath := b.generateStoragePath(database, measurement, minTime)

		// Compute SHA-256 of the Parquet buffer before the backend write so the
		// hash lands in the cluster manifest with the same commit that announces
		// the file. Peers use this to verify bytes pulled from the origin.
		// The buffer is already in memory; this is an O(n) scan of ~MB of data.
		parquetSum := sha256.Sum256(parquetData)
		parquetSumHex := hex.EncodeToString(parquetSum[:])

		if err := b.storage.Write(ctx, storagePath, parquetData); err != nil {
			return fmt.Errorf("failed to write to storage: %w", err)
		}
		b.registerFieldSchema(ctx, database, measurement, fileSchema, sorted.TagColumns, storagePath)

		// Register file in tiering metadata for query routing
		b.registerFileInTiering(ctx, database, measurement, storagePath, minTime, int64(len(parquetData)), parquetSumHex)

		b.totalRecordsWritten.Add(int64(recordCount))
		b.totalFlushes.Add(1)
		b.publishBufferMetrics()

		flushDuration := time.Since(startTime)
		msgType := getFlushMessageType(flushType)

		b.logger.Info().
			Str("buffer_key", bufferKey).
			Str("storage_path", storagePath).
			Int("records", recordCount).
			Int("size_bytes", len(parquetData)).
			Dur("flush_duration", flushDuration).
			Strs("sort_keys", sortKeys).
			Msgf("%s completed (single hour, data_time)", msgType)

		return nil
	}

	// Multiple hours - now (and only now) build the per-hour index buckets. This is the
	// allocation-heavy path, but a multi-hour flush is the uncommon backfill/clock-skew
	// case, so paying for the full bucketing here keeps the common single-hour path cheap.
	hourBuckets, _, _, err := groupByHour(times)
	if err != nil {
		return fmt.Errorf("failed to group by hour: %w", err)
	}

	// Process each hour bucket independently
	b.logger.Info().
		Str("buffer_key", bufferKey).
		Int("num_hours", len(hourBuckets)).
		Int("total_records", recordCount).
		Msg("Splitting batch across multiple hour partitions")

	// Collect registration entries after all storage writes succeed.
	// registerFileInTiering is called only after every hour's storage.Write succeeds
	// to avoid leaving stale manifest entries when a later hour fails — WAL replay
	// would otherwise create a duplicate file for the already-registered hour.
	type tieringEntry struct {
		storagePath string
		bucketTime  time.Time
		sizeBytes   int64
		sha256Hex   string
		records     int
	}
	written := make([]tieringEntry, 0, len(hourBuckets))

	for hourID, bucket := range hourBuckets {
		// Save count before clearing indices
		splitRecordCount := len(bucket.indices)

		// Extract rows for this hour using the index list
		hourBatch := sliceTypedColumnBatchByIndices(merged, bucket.indices)

		// Sort this hour's data by configured sort keys
		sorted := sortTypedColumnBatchByKeys(hourBatch, sortKeys)

		// Write Parquet file for this hour
		parquetData, fileSchema, err := b.writer.writeParquetColumnarWithSchema(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, sorted.DedupTime, decimalCols)
		if err != nil {
			return fmt.Errorf("failed to write Parquet for hour %d: %w", hourID, err)
		}

		// Use bucket's minTime for path generation (convert hourID to time only here)
		bucketTime := hourIDToTime(hourID)
		storagePath := b.generateStoragePath(database, measurement, bucketTime)

		// Compute SHA-256 of the Parquet buffer for peer replication checksum.
		// See the single-hour branch above for rationale.
		parquetSum := sha256.Sum256(parquetData)
		parquetSumHex := hex.EncodeToString(parquetSum[:])

		if err := b.storage.Write(ctx, storagePath, parquetData); err != nil {
			return fmt.Errorf("failed to write to storage for hour %d: %w", hourID, err)
		}
		b.registerFieldSchema(ctx, database, measurement, fileSchema, sorted.TagColumns, storagePath)

		written = append(written, tieringEntry{
			storagePath: storagePath,
			bucketTime:  bucketTime,
			sizeBytes:   int64(len(parquetData)),
			sha256Hex:   parquetSumHex,
			records:     splitRecordCount,
		})

		b.logger.Info().
			Str("buffer_key", bufferKey).
			Str("storage_path", storagePath).
			Int64("hour_id", hourID).
			Int("records", splitRecordCount).
			Int("size_bytes", len(parquetData)).
			Msg("Wrote hour partition")
	}

	// All hours written successfully — now register in tiering and cluster manifest.
	totalWritten := 0
	for _, e := range written {
		b.registerFileInTiering(ctx, database, measurement, e.storagePath, e.bucketTime, e.sizeBytes, e.sha256Hex)
		totalWritten += e.records
	}

	b.totalRecordsWritten.Add(int64(totalWritten))
	b.totalFlushes.Add(int64(len(hourBuckets)))
	b.publishBufferMetrics()

	flushDuration := time.Since(startTime)
	msgType := getFlushMessageType(flushType)

	b.logger.Info().
		Str("buffer_key", bufferKey).
		Int("num_files", len(hourBuckets)).
		Int("total_records", totalWritten).
		Dur("flush_duration", flushDuration).
		Msgf("%s completed (multi-hour split, data_time)", msgType)

	return nil
}

// flushBufferLocked writes buffered data to Parquet and storage (synchronous version for periodic flush)
// Note: Caller must hold shard.mu lock
func (b *ArrowBuffer) flushBufferLocked(ctx context.Context, shard *bufferShard, bufferKey, database, measurement string) error {
	batches, exists := shard.buffers[bufferKey]
	if !exists || len(batches) == 0 {
		// Clean up stale tracking entries even if buffer is empty.
		//
		// deferredKeys has to go too. A key left here with no bufferStartTimes
		// entry is UNCOLLECTABLE: oldestDeferred skips any key whose start time is
		// missing, so it is never selected, and only a selected key reaches the
		// cleanup in enqueueOrDeferLocked. It would then sit in
		// arc_buffer_deferred_buffers and GetStats forever.
		delete(shard.deferredKeys, bufferKey)
		delete(shard.buffers, bufferKey)
		delete(shard.bufferStartTimes, bufferKey)
		delete(shard.bufferRecordCounts, bufferKey)
		delete(shard.bufferSchemas, bufferKey)
		return nil
	}

	// Get record count before clearing buffer
	recordCount := shard.bufferRecordCounts[bufferKey]
	walHashes := collectWALHashes(batches)

	// Extract records to flush (hold lock for minimal time)
	recordsToFlush := make([]interface{}, len(batches))
	copy(recordsToFlush, batches)

	// Clear buffer immediately
	delete(shard.deferredKeys, bufferKey)
	delete(shard.buffers, bufferKey)
	delete(shard.bufferStartTimes, bufferKey)
	delete(shard.bufferRecordCounts, bufferKey)
	delete(shard.bufferSchemas, bufferKey)

	// Release lock before expensive operations
	shard.mu.Unlock()

	// Merge typed column batches
	merged, err := b.mergeBatches(recordsToFlush)
	if err != nil {
		shard.mu.Lock() // Re-acquire lock for caller
		b.markFlushFailure()
		return fmt.Errorf("failed to merge batches: %w", err)
	}

	// Flush with data timestamp partitioning
	startTime := time.Now().UTC()
	if err := b.flushBufferLockedDataTime(ctx, bufferKey, database, measurement, merged, recordCount, startTime); err != nil {
		shard.mu.Lock() // Re-acquire lock for caller
		b.markFlushFailure()
		b.logger.Warn().
			Err(err).
			Str("buffer_key", bufferKey).
			Int("records", recordCount).
			Msg("Flush failed - data preserved in WAL for recovery")
		// Data is already in WAL (written at ingest time) - no need to restore to buffer
		// This prevents memory growth during prolonged S3 outages
		// WAL will be replayed on restart or via periodic recovery
		return err
	}
	b.markWALFlushed(walHashes)

	// Re-acquire lock for caller
	shard.mu.Lock()
	return nil
}

// flushBufferLockedDataTime flushes with data_time partitioning (sync path)
func (b *ArrowBuffer) flushBufferLockedDataTime(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeSync, startTime)
}

// mergeBatches merges multiple column batches into a single TypedColumnBatch.
// OPTIMIZATION: Pre-allocate merged arrays to avoid O(n²) append reallocations
// Handles sparse columns (schema evolution) by marking missing positions as null via validity bitmaps.
func (b *ArrowBuffer) mergeBatches(batches []interface{}) (*TypedColumnBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}

	// If only one batch, return it directly
	if len(batches) == 1 {
		if tcb, ok := batches[0].(*TypedColumnBatch); ok {
			return tcb, nil
		}
		// Legacy: bare map without validity (e.g. from WAL replay)
		if cols, ok := batches[0].(map[string]interface{}); ok {
			return &TypedColumnBatch{Data: cols, Validity: nil}, nil
		}
		return nil, fmt.Errorf("invalid batch type: %T", batches[0])
	}

	// PHASE 1: Calculate total rows from time column and collect column type info
	type colInfo struct {
		colType string // "int64", "float64", "string", "bool", "decimal128"
	}
	colTypes := make(map[string]colInfo)
	totalRows := 0

	// Track which columns have validity bitmaps and which batches have which columns
	hasAnyValidity := false

	// Union of tag columns across all batches (for Parquet metadata)
	tagColumnSet := make(map[string]struct{})
	// Dedup-time marker is sticky: if ANY merged batch carries it, the output
	// does (all writes to one measurement share the same producer, so this only
	// ORs identical values in practice — the OR is defensive).
	mergedDedupTime := false

	// First pass: count total rows using time column
	for _, batch := range batches {
		var cols map[string]interface{}
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
			if len(b.Validity) > 0 {
				hasAnyValidity = true
			}
			for _, tag := range b.TagColumns {
				tagColumnSet[tag] = struct{}{}
			}
			if b.DedupTime {
				mergedDedupTime = true
			}
		case map[string]interface{}:
			cols = b
		default:
			return nil, fmt.Errorf("invalid batch type: %T", batch)
		}

		// Count rows from time column (always present)
		if timeCol, ok := cols["time"].([]int64); ok {
			totalRows += len(timeCol)
		}

		// Collect column types
		for name, col := range cols {
			if _, seen := colTypes[name]; !seen {
				var ct string
				switch col.(type) {
				case []int64:
					ct = "int64"
				case []float64:
					ct = "float64"
				case []string:
					ct = "string"
				case []bool:
					ct = "bool"
				case []decimal128.Num:
					ct = "decimal128"
				default:
					return nil, fmt.Errorf("unsupported column type: %T", col)
				}
				colTypes[name] = colInfo{colType: ct}
			}
		}
	}

	// Determine if we need validity tracking.
	// Needed if: any batch already has validity, OR columns are sparse across batches.
	// Check sparsity: if any batch doesn't have all columns, those positions are null.
	hasSparseColumns := false
	for _, batch := range batches {
		var cols map[string]interface{}
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
		case map[string]interface{}:
			cols = b
		}
		if len(cols) < len(colTypes) {
			hasSparseColumns = true
			break
		}
	}
	needsValidity := hasAnyValidity || hasSparseColumns

	// PHASE 2: Pre-allocate ALL columns to totalRows (handles sparse columns)
	merged := make(map[string]interface{}, len(colTypes))
	var mergedValidity map[string][]bool
	if needsValidity {
		mergedValidity = make(map[string][]bool, len(colTypes))
	}

	for name, info := range colTypes {
		switch info.colType {
		case "int64":
			merged[name] = make([]int64, totalRows)
		case "float64":
			merged[name] = make([]float64, totalRows)
		case "string":
			merged[name] = make([]string, totalRows)
		case "bool":
			merged[name] = make([]bool, totalRows)
		case "decimal128":
			merged[name] = make([]decimal128.Num, totalRows)
		}
		if needsValidity {
			// Initialize all positions as invalid (null). Positions with data get set to true below.
			mergedValidity[name] = make([]bool, totalRows)
		}
	}

	// PHASE 3: Copy data at correct row offsets (not per-column offsets)
	rowOffset := 0
	for _, batch := range batches {
		var cols map[string]interface{}
		var batchValidity map[string][]bool
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
			batchValidity = b.Validity
		case map[string]interface{}:
			cols = b
		}

		// Determine batch size from time column
		batchRows := 0
		if timeCol, ok := cols["time"].([]int64); ok {
			batchRows = len(timeCol)
		}

		// Copy each column's data at the current row offset
		for name, col := range cols {
			switch v := col.(type) {
			case []int64:
				copy(merged[name].([]int64)[rowOffset:], v)
			case []float64:
				copy(merged[name].([]float64)[rowOffset:], v)
			case []string:
				copy(merged[name].([]string)[rowOffset:], v)
			case []bool:
				copy(merged[name].([]bool)[rowOffset:], v)
			case []decimal128.Num:
				copy(merged[name].([]decimal128.Num)[rowOffset:], v)
			}

			// Copy validity bitmap for this column
			if needsValidity {
				dest := mergedValidity[name][rowOffset : rowOffset+batchRows]
				if batchValidity != nil {
					srcValid, ok := batchValidity[name]
					if ok && srcValid != nil {
						// Batch has explicit validity for this column — copy it
						copy(dest, srcValid)
					} else {
						// Either the column has no validity entry, or entry is nil
						// (contract: nil entry = all valid). Either way: all valid.
						for i := range dest {
							dest[i] = true
						}
					}
				} else {
					// Batch has no validity tracking at all → all valid
					for i := range dest {
						dest[i] = true
					}
				}
			}
		}
		// Sparse columns that don't exist in this batch keep validity=false (null)
		// at positions [rowOffset : rowOffset+batchRows] — no action needed.

		rowOffset += batchRows
	}

	// Optimization: strip validity entries that are all-true (no nulls)
	if mergedValidity != nil {
		for name, valid := range mergedValidity {
			allValid := true
			for _, v := range valid {
				if !v {
					allValid = false
					break
				}
			}
			if allValid {
				delete(mergedValidity, name)
			}
		}
		if len(mergedValidity) == 0 {
			mergedValidity = nil
		}
	}

	// Collect merged tag columns
	var mergedTagColumns []string
	if len(tagColumnSet) > 0 {
		mergedTagColumns = make([]string, 0, len(tagColumnSet))
		for tag := range tagColumnSet {
			mergedTagColumns = append(mergedTagColumns, tag)
		}
	}

	return &TypedColumnBatch{Data: merged, Validity: mergedValidity, TagColumns: mergedTagColumns, DedupTime: mergedDedupTime}, nil
}

// sortColumnsByTime sorts all columns by the time column in-place
// Returns the sorted columns and any error encountered
func sortColumnsByTime(columns map[string]interface{}) (map[string]interface{}, error) {
	// Delegate to multi-key sort with just "time" key
	return sortColumnsByKeys(columns, []string{"time"})
}

// sortColumnsByKeys sorts columns by multiple keys (e.g., sensor_id, then time)
// Returns the sorted columns and any error encountered
func sortColumnsByKeys(columns map[string]interface{}, sortKeys []string) (map[string]interface{}, error) {
	sorted, _, err := sortColumnsByKeysWithPermutation(columns, sortKeys)
	return sorted, err
}

// sortColumnsByKeysWithPermutation sorts columns and returns the permutation indices used.
// The permutation can be applied to validity bitmaps or other parallel arrays by the caller,
// avoiding a second sort pass.
func sortColumnsByKeysWithPermutation(columns map[string]interface{}, sortKeys []string) (map[string]interface{}, []int, error) {
	if len(sortKeys) == 0 {
		return nil, nil, fmt.Errorf("no sort keys provided")
	}

	// FAST PATH: Time-only sort (most common case) - avoid multi-key overhead
	if len(sortKeys) == 1 && sortKeys[0] == "time" {
		sorted, indices, err := sortColumnsByTimeOnlyWithPermutation(columns)
		return sorted, indices, err
	}

	// Validate all sort keys exist and cache column pointers
	cachedCols := make([]interface{}, len(sortKeys))
	for i, key := range sortKeys {
		col, exists := columns[key]
		if !exists {
			return nil, nil, fmt.Errorf("sort key column not found: %s", key)
		}
		cachedCols[i] = col
	}

	// Get first column to determine row count
	var n int
	for _, col := range columns {
		switch c := col.(type) {
		case []int64:
			n = len(c)
		case []float64:
			n = len(c)
		case []string:
			n = len(c)
		case []bool:
			n = len(c)
		case []decimal128.Num:
			n = len(c)
		}
		if n > 0 {
			break
		}
	}

	if n == 0 {
		return columns, nil, nil
	}

	// Create permutation indices [0, 1, 2, ..., n-1]
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Multi-key sort with cached columns (no map lookups in comparator)
	sort.Slice(indices, func(i, j int) bool {
		return compareMultiKeyCached(cachedCols, indices[i], indices[j])
	})

	// Apply permutation to all columns
	result := make(map[string]interface{}, len(columns))
	for colName, colData := range columns {
		result[colName] = applyPermutation(colData, indices)
	}

	return result, indices, nil
}

// sortColumnsByTimeOnly is an optimized path for time-only sorting.
// Avoids the multi-key comparator overhead for the common case.
func sortColumnsByTimeOnly(columns map[string]interface{}) (map[string]interface{}, error) {
	sorted, _, err := sortColumnsByTimeOnlyWithPermutation(columns)
	return sorted, err
}

// sortColumnsByTimeOnlyWithPermutation sorts by time and returns the permutation used.
// Returns nil indices when data is already sorted (no permutation needed).
func sortColumnsByTimeOnlyWithPermutation(columns map[string]interface{}) (map[string]interface{}, []int, error) {
	timeCol, exists := columns["time"]
	if !exists {
		return nil, nil, fmt.Errorf("time column not found")
	}

	times, ok := timeCol.([]int64)
	if !ok {
		return nil, nil, fmt.Errorf("time column is not []int64")
	}

	n := len(times)
	if n == 0 {
		return columns, nil, nil
	}

	// Compute the time-ordering permutation. permuteByTime returns nil when the
	// data is already sorted (identity), letting callers skip rematerialization.
	indices := permuteByTime(times)
	if indices == nil {
		return columns, nil, nil // already sorted — nil indices signals identity permutation
	}

	// Apply permutation to all columns
	result := make(map[string]interface{}, len(columns))
	for colName, colData := range columns {
		result[colName] = applyPermutation(colData, indices)
	}

	return result, indices, nil
}

// radixSkipThreshold: below this row count the comparison sort wins (radix's fixed
// per-pass overhead isn't amortized), so permuteByTime uses sort.Slice for small buffers.
const radixSkipThreshold = 4096

// permuteByTime returns the permutation that sorts times ascending, or nil when the
// data is already sorted (identity permutation — callers skip rematerialization).
//
// The merged 5M-row flush buffer is, under concurrent producers, effectively unordered:
// rows from many in-flight batches interleave at the row level as they append to the
// shared buffer (the 2026-06-22 profile measured ~2.5M sorted runs in a 5M-row buffer,
// i.e. no exploitable run structure). The previous closure-based sort.Slice cost ~6.9%
// of total CPU on this path. The keys are int64 microsecond timestamps, so an LSD radix
// sort wins decisively (no comparisons; the near-constant high bytes are skipped per
// pass): measured ~5x faster than sort.Slice (796ms -> 157ms on 5M rows). Negative
// (pre-epoch) timestamps are handled via radixSortBias.
//
// The already-sorted check is kept first: in-order single-producer ingest stays an O(n)
// scan with zero permutation work.
func permuteByTime(times []int64) []int {
	n := len(times)
	if n == 0 {
		return nil
	}

	// FAST PATH: already sorted (single in-order producer) — identity permutation.
	alreadySorted := true
	for i := 1; i < n; i++ {
		if times[i] < times[i-1] {
			alreadySorted = false
			break
		}
	}
	if alreadySorted {
		return nil
	}

	// Small buffers: comparison sort beats radix's fixed per-pass cost.
	if n < radixSkipThreshold {
		return permuteByTimeSort(times)
	}

	return radixPermuteByTime(times)
}

// permuteByTimeSort is the comparison-sort path for small buffers.
func permuteByTimeSort(times []int64) []int {
	n := len(times)
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return times[indices[i]] < times[indices[j]]
	})
	return indices
}

// radixSortBias maps a signed int64 to its order-preserving unsigned key by flipping the
// sign bit: int64 min -> 0, int64 max -> max. This lets an unsigned LSD radix sort produce
// correct ascending order even when timestamps are negative (pre-1970, which Line Protocol
// and MessagePack both accept — see #312). Without it, negatives' set sign bit would sort
// them after positives.
func radixSortBias(t int64) uint64 {
	return uint64(t) ^ 0x8000000000000000 // flip the sign bit
}

// radixPermuteByTime returns the ascending-time permutation via an LSD radix sort over
// the int64 timestamps (8 passes of 8 bits), using radixSortBias so negative (pre-epoch)
// timestamps order correctly. Passes where every key shares one byte value are skipped,
// which makes the near-constant high-order bytes (all timestamps near "now") almost free.
// Stable, which keeps equal-timestamp rows in arrival order.
func radixPermuteByTime(times []int64) []int {
	n := len(times)
	if n == 0 {
		// Defensive: permuteByTime already short-circuits empty input, but guard here
		// too so a direct caller can't trip the times[src[0]] index below.
		return nil
	}
	src := make([]int, n)
	for i := range src {
		src[i] = i
	}
	dst := make([]int, n)

	var count [256]int
	for shift := uint(0); shift < 64; shift += 8 {
		count = [256]int{}
		for _, ix := range src {
			count[(radixSortBias(times[ix])>>shift)&0xff]++
		}
		// Skip this pass if all keys fall in a single bucket (e.g. constant high bytes).
		if count[(radixSortBias(times[src[0]])>>shift)&0xff] == n {
			continue
		}
		sum := 0
		for i := 0; i < 256; i++ {
			c := count[i]
			count[i] = sum
			sum += c
		}
		for _, ix := range src {
			b := (radixSortBias(times[ix]) >> shift) & 0xff
			dst[count[b]] = ix
			count[b]++
		}
		src, dst = dst, src
	}
	return src
}

// compareMultiKeyCached compares two rows by multiple sort keys using cached column pointers
// This avoids map lookups on every comparison (called O(n log n) times)
func compareMultiKeyCached(cachedCols []interface{}, i, j int) bool {
	for _, col := range cachedCols {
		switch c := col.(type) {
		case []int64:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}
			// Equal, continue to next key

		case []float64:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}

		case []string:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}

		case []bool:
			if !c[i] && c[j] { // false < true
				return true
			}
			if c[i] && !c[j] {
				return false
			}

		case []decimal128.Num:
			if c[i].Less(c[j]) {
				return true
			}
			if c[i].Greater(c[j]) {
				return false
			}
		}
	}

	// All keys equal
	return false
}

// applyPermutation reorders a column according to permutation indices
func applyPermutation(colData interface{}, indices []int) interface{} {
	switch col := colData.(type) {
	case []int64:
		result := make([]int64, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []float64:
		result := make([]float64, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []string:
		result := make([]string, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []bool:
		result := make([]bool, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []decimal128.Num:
		result := make([]decimal128.Num, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	default:
		return colData // Unknown type, return as-is
	}
}

// sortTypedColumnBatchByKeys sorts a TypedColumnBatch by the given keys,
// keeping validity bitmaps aligned with the reordered data.
// Uses the permutation returned by sortColumnsByKeysWithPermutation to avoid
// a second sort pass when validity bitmaps need reordering.
func sortTypedColumnBatchByKeys(batch *TypedColumnBatch, sortKeys []string) *TypedColumnBatch {
	sorted, indices, err := sortColumnsByKeysWithPermutation(batch.Data, sortKeys)
	if err != nil {
		return batch
	}

	// nil indices means data was already sorted — no permutation needed
	if indices == nil || batch.Validity == nil {
		return &TypedColumnBatch{Data: sorted, Validity: batch.Validity, TagColumns: batch.TagColumns, DedupTime: batch.DedupTime, Signature: batch.Signature}
	}

	// Apply the same permutation to validity bitmaps (no second sort)
	sortedValidity := make(map[string][]bool, len(batch.Validity))
	for name, valid := range batch.Validity {
		// Per TypedColumnBatch contract, a nil entry means "all valid" — preserve as nil.
		if valid == nil {
			sortedValidity[name] = nil
			continue
		}
		newValid := make([]bool, len(indices))
		for i, idx := range indices {
			newValid[i] = valid[idx]
		}
		sortedValidity[name] = newValid
	}

	return &TypedColumnBatch{Data: sorted, Validity: sortedValidity, TagColumns: batch.TagColumns, DedupTime: batch.DedupTime, Signature: batch.Signature}
}

// sliceTypedColumnBatchByIndices extracts rows from a TypedColumnBatch by index list,
// keeping validity bitmaps aligned.
func sliceTypedColumnBatchByIndices(batch *TypedColumnBatch, indices []int) *TypedColumnBatch {
	slicedData := sliceColumnsByIndices(batch.Data, indices)

	if batch.Validity == nil {
		return &TypedColumnBatch{Data: slicedData, Validity: nil, TagColumns: batch.TagColumns, DedupTime: batch.DedupTime, Signature: batch.Signature}
	}

	slicedValidity := make(map[string][]bool, len(batch.Validity))
	for name, valid := range batch.Validity {
		// Per TypedColumnBatch contract, a nil entry means "all valid" — preserve as nil.
		if valid == nil {
			slicedValidity[name] = nil
			continue
		}
		newValid := make([]bool, len(indices))
		validLen := len(valid)
		for i, idx := range indices {
			if idx < validLen {
				newValid[i] = valid[idx]
			}
			// else: out-of-bounds stays false (null)
		}
		slicedValidity[name] = newValid
	}

	return &TypedColumnBatch{Data: slicedData, Validity: slicedValidity, TagColumns: batch.TagColumns, DedupTime: batch.DedupTime, Signature: batch.Signature}
}

// microPerHour is the number of microseconds in one hour (3600 * 1,000,000)
const microPerHour = int64(3600_000_000)

// hourBucket represents a collection of row indices belonging to a specific hour
// Used for hash-based grouping that doesn't require globally sorted data
type hourBucket struct {
	hourID  int64 // Hour identifier (microseconds / microPerHour)
	indices []int // Row indices belonging to this hour
	minTime int64 // Minimum timestamp in this hour (microseconds)
	maxTime int64 // Maximum timestamp in this hour (microseconds)
}

// hourIDToTime converts an hourID back to a time.Time for path generation
func hourIDToTime(hourID int64) time.Time {
	return time.UnixMicro(hourID * microPerHour).UTC()
}

// HourBucketID returns the hour bucket for a microsecond timestamp, using
// floor division so the bucket is the hour that actually contains the
// timestamp. Plain integer division truncates toward zero, which puts
// pre-epoch (negative) timestamps in the wrong hour — e.g. a timestamp 1µs
// before the epoch would map to hour 0 (1970-01-01 00:00) instead of hour -1
// (1969-12-31 23:00), misfiling the row's partition (#312). For t >= 0 this
// is identical to t / microPerHour.
func HourBucketID(microTime int64) int64 {
	h := microTime / microPerHour
	// Sign check first so the modulo is short-circuited away for the common
	// non-negative case on the per-row ingest hot path.
	if microTime < 0 && microTime%microPerHour != 0 {
		h--
	}
	return h
}

// groupByHour groups row indices by hour and tracks min/max times
// Works correctly regardless of whether data is globally sorted by time
// Returns: map of hourID -> bucket, global min time, global max time
// Uses HourBucketID for fast, allocation-free hour extraction (no time.Time)
func groupByHour(times []int64) (map[int64]*hourBucket, int64, int64, error) {
	if len(times) == 0 {
		return nil, 0, 0, fmt.Errorf("empty time column")
	}

	buckets := make(map[int64]*hourBucket)
	globalMin := times[0]
	globalMax := times[0]

	// Cache the last hour's bucket to skip the map lookup for consecutive
	// same-hour timestamps. Data enters the buffer in arrival order, which for
	// a typical time-series writer is near-chronological, so the cache hits on
	// the vast majority of rows. Benchmark (1M rows): ordered ~8.0ms → ~3.6ms
	// (2.2x); the only loss is ~5% on heavily hour-interleaved input (e.g.
	// multi-tag secondary-sort spread across many hours in one flush).
	var lastID int64
	var lastBucket *hourBucket

	// Single pass: group by hour and track min/max
	for i, t := range times {
		// Update global min/max
		if t < globalMin {
			globalMin = t
		}
		if t > globalMax {
			globalMax = t
		}

		// Fast hour extraction (no time.Time allocation). Floor division so
		// pre-epoch (negative) timestamps bucket into the hour that contains
		// them rather than truncating toward the epoch (#312).
		hourID := HourBucketID(t)

		// Get or create bucket, reusing the cached one on a same-hour run.
		var bucket *hourBucket
		if i > 0 && hourID == lastID {
			bucket = lastBucket
		} else {
			var exists bool
			bucket, exists = buckets[hourID]
			if !exists {
				bucket = &hourBucket{
					hourID:  hourID,
					indices: make([]int, 0, 100), // Pre-allocate some capacity
					minTime: t,
					maxTime: t,
				}
				buckets[hourID] = bucket
			}
			lastID = hourID
			lastBucket = bucket
		}

		// Update bucket min/max (the newly-created bucket already has t as
		// both, so these comparisons are no-ops on first insert).
		if t < bucket.minTime {
			bucket.minTime = t
		}
		if t > bucket.maxTime {
			bucket.maxTime = t
		}

		// Add row index to bucket
		bucket.indices = append(bucket.indices, i)
	}

	return buckets, globalMin, globalMax, nil
}

// sliceColumnsByIndices extracts rows from all columns based on a list of indices
// Returns a new column map with only the selected rows
// Handles sparse columns (columns shorter than indices) by using zero values for out-of-bounds access
func sliceColumnsByIndices(columns map[string]interface{}, indices []int) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))

	for colName, colData := range columns {
		switch col := colData.(type) {
		case []int64:
			newCol := make([]int64, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as zero value (sparse column handling)
			}
			result[colName] = newCol

		case []float64:
			newCol := make([]float64, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as zero value (sparse column handling)
			}
			result[colName] = newCol

		case []string:
			newCol := make([]string, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as empty string (sparse column handling)
			}
			result[colName] = newCol

		case []bool:
			newCol := make([]bool, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as false (sparse column handling)
			}
			result[colName] = newCol

		case []decimal128.Num:
			newCol := make([]decimal128.Num, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
			}
			result[colName] = newCol

		default:
			// Unknown type, copy as-is
			result[colName] = colData
		}
	}

	return result
}

// generateStoragePath creates a hierarchical storage path for partition pruning
// Format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{measurement}_{timestamp}_{nanos}.parquet
//
// This hierarchical structure enables DuckDB to skip entire directories when querying time ranges:
// - Query all of November: read_parquet('s3://bucket/db/cpu/2025/11/*/*/*.parquet')
// - Query specific day: read_parquet('s3://bucket/db/cpu/2025/11/25/*/*.parquet')
// - Query specific hour: read_parquet('s3://bucket/db/cpu/2025/11/25/16/*.parquet')
func (b *ArrowBuffer) generateStoragePath(database, measurement string, partitionTime time.Time) string {
	// Hierarchical partitioning: year/month/day/hour
	year := partitionTime.Format("2006")
	month := partitionTime.Format("01")
	day := partitionTime.Format("02")
	hour := partitionTime.Format("15")

	// Filename includes measurement, timestamp, and nanos for uniqueness
	// Use current time for filename to avoid collisions
	now := time.Now().UTC()
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
			// The caller's context is honoured BETWEEN buffers only. A buffer is
			// extracted before its I/O starts, so abandoning a flush in progress
			// would lose it — the caller's cancellation must not reach the write
			// itself (#1007).
			if err := ctx.Err(); err != nil {
				shard.mu.Unlock()
				b.logger.Warn().Err(err).Msg("FlushAll stopping between buffers: caller context done")
				return err
			}

			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during flush")
				continue
			}

			flushCtx, flushCancel := b.newFlushContext()
			err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1])
			flushCancel()
			if err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush buffer")
				lastErr = err
			}
			// flushBufferLocked returns with the lock held (re-acquires after I/O)
		}

		shard.mu.Unlock()
	}

	b.logger.Info().Msg("All buffers flushed")
	return lastErr
}

// Close stops the buffer and flushes everything it still holds.
//
// Ordering, and why each step is where it is:
//
//  1. Set b.closing. tryEnqueueFlush reads it to decide that no worker will take
//     a task, and the aged sweep reads it to stop between buffers.
//  2. b.cancel() stops the background loops — the metrics sampler, the periodic
//     flush, and the workers' receive select. It does NOT reach flush I/O: every
//     flush context derives from flushParent (context.WithoutCancel), so a write
//     already in progress is waited for rather than aborted. Its records were
//     removed from the buffer when the task was built, so cancelling it would
//     lose them (#1007). The wait is bounded by the close budget; on expiry
//     flushParent is cancelled so ctx-aware backends give up and Close returns.
//  3. Mark each shard swept and flush its buffers, under the shard lock. Swept
//     is set BEFORE the flush, because flushBufferLocked releases the lock for
//     its I/O and a writer appending in that window would otherwise be lost.
//  4. Drain the flush queue and FLUSH what is in it. Previously those tasks were
//     discarded "in favour of WAL replay", which with the shipped
//     wal.enabled=false is loss on every graceful stop under load. No writer can
//     be mid-handoff here: the enqueue happens under shard.mu, and step 3 took
//     every shard lock, so a writer either completed its enqueue before its
//     shard was swept or was refused with ErrBufferClosing.
//  5. Decide whether the close was clean. Anything not confirmed written is
//     counted into walOnlyRecords, which makes CloseFlushedCleanly false and
//     keeps the shutdown WAL purge from deleting the only remaining copy (#803).
//
// We deliberately do NOT close(b.flushQueue): workers exit on b.ctx.Done(), and
// closing the channel would re-introduce the send-on-closed-channel race the
// closing flag was added to fix.
func (b *ArrowBuffer) Close() error {
	b.logger.Info().Msg("Closing ArrowBuffer...")

	// One deadline for the whole of Close, including the wait for in-flight
	// flushes. The coordinator calls this with no context and only checks its
	// own budget BETWEEN components, so a Close that does not return skips the
	// wal-purge and wal components — and wal.Writer.Close is what performs the
	// WAL's final drain-and-sync. Hanging here would retain a WAL missing its
	// newest entries, which is worse than the loss this change fixes.
	budget := b.closeDeadlineBudget()
	start := time.Now()
	// The budget is SPLIT, not spent wholly on whichever phase runs first.
	//
	// Waiting for in-flight flushes and flushing what is left are both bounded
	// by it, and either one can consume all of it. If the wait took the lot,
	// Close would reach the flush steps with nothing left and write nothing —
	// the same outcome as cancelling the flushes, which is what this change
	// exists to avoid. Half each: the wait gets a bounded chance to let
	// in-progress writes finish, and the flush steps keep a guaranteed slice.
	waitDeadline := start.Add(budget / 2)
	deadline := start.Add(budget)

	// Mark closing BEFORE cancelling so any writer past the shard
	// unlock observes either the flag (flushes inline) or the cancelled
	// ctx (Done arm fires, also flushes inline). Either path avoids the panic.
	b.closing.Store(true)

	// Stop periodic flush
	b.cancel()
	b.flushTimer.Stop()

	// Wait for the workers and the aged sweep to finish, bounded by the budget.
	//
	// Flush I/O no longer derives from b.ctx, so b.cancel() above does NOT
	// abort a storage write in progress — that is the point of the change. The
	// consequence is that this wait is bounded by flushTimeout rather than
	// being instant, which is why it has to respect the budget: on expiry,
	// cancel flushParent so context-aware backends abandon their writes and the
	// workers return.
	//
	// Caveat worth knowing when reading a shutdown log: LocalBackend.Write does
	// not observe its context, so on the local backend this cancel has no
	// effect and the wait ends when the filesystem finishes.
	workersDone := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(workersDone)
	}()
	budgetTimer := time.NewTimer(time.Until(waitDeadline))
	defer budgetTimer.Stop()
	select {
	case <-workersDone:
	case <-budgetTimer.C:
		b.logger.Error().
			Dur("budget", budget).
			Dur("wait_slice", budget/2).
			Msg("Close budget's wait slice expired; cancelling flushes in progress so the remaining buffers still get their share of the budget")
		if b.flushParentCancel != nil {
			b.flushParentCancel()
		}
		<-workersDone
	}

	b.logger.Info().Msg("All flush workers stopped, flushing remaining buffers")

	// Collect per-buffer flush failures rather than only logging them: the
	// shutdown WAL purge must not run when any buffer failed to reach storage
	// (#803).
	var flushErrs []error
	totalBuffers := 0
	// Records left unflushed because the budget ran out mid-close. Counted as
	// WAL-only so the close reports unclean and the shutdown purge is skipped.
	budgetExpiredRecords := 0

	// STEP 1: mark each shard swept, then flush every buffer in it.
	//
	// This runs BEFORE the queue drain below, so the drain sees a queue that no
	// shard can add to any more.
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		// Mark the shard swept BEFORE flushing it, not after.
		//
		// flushBufferLocked releases shard.mu for the whole merge-encode-write
		// and re-acquires it afterwards. If swept were set only at the end of
		// this loop, a writer could take the lock during that I/O window, pass
		// the swept check, and append either to a key that is not in the
		// snapshot below or to a key this loop has already flushed. Those rows
		// would then sit in shard.buffers forever: never written, never counted,
		// and CloseFlushedCleanly would still report clean, so the shutdown
		// purge would delete the WAL that held them. HTTP is already drained by
		// then, but MQTT, continuous queries and replication apply are not, so
		// that window is reached routinely rather than rarely.
		//
		// Setting it first closes the window completely: no append can land in
		// this shard from here on. A write that arrives is refused with
		// ErrBufferClosing and counted as WAL-only, which is recoverable. This
		// is still PER SHARD — shards this loop has not reached yet keep
		// accepting writes normally, which is what keeps replicated entries
		// (skipWAL, no second copy) from being refused for the whole of Close.
		shard.swept = true

		// Copy keys to avoid modifying map while iterating
		// (flushBufferLocked releases and re-acquires the lock during I/O)
		keys := make([]string, 0, len(shard.buffers))
		for key := range shard.buffers {
			keys = append(keys, key)
		}
		totalBuffers += len(keys)

		for _, key := range keys {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during close")
				flushErrs = append(flushErrs, fmt.Errorf("invalid buffer key format: %q", key))
				continue
			}

			flushCtx, flushCancel, ok := b.closeFlushContext(deadline)
			if !ok {
				// Out of budget. Leave the rest in the buffer and count it, so
				// the close reports unclean and the WAL is retained for it.
				// Attempting it anyway with a fresh floor is what made Close
				// overrun and get the WAL writer's final sync skipped.
				unflushed := shard.bufferRecordCounts[key]
				budgetExpiredRecords += unflushed
				continue
			}
			if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1]); err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush buffer during close")
				flushErrs = append(flushErrs, fmt.Errorf("buffer %q: %w", key, err))
			}
			flushCancel()
			// flushBufferLocked returns with the lock held (re-acquires after I/O)
		}

		shard.mu.Unlock()
	}

	// STEP 2: drain the queue and FLUSH what is in it, instead of discarding it.
	//
	// These records were removed from shard.buffers at enqueue time, so the loop
	// above never saw them. Previously they were counted into walOnlyRecords and
	// thrown away "in favour of WAL replay", which with the shipped
	// wal.enabled=false default is simply loss on every graceful stop under
	// load (#1007).
	if budgetExpiredRecords > 0 {
		b.logger.Error().
			Int("records", budgetExpiredRecords).
			Msg("Close budget expired with buffers still unflushed; records remain in the WAL and will be replayed on next startup")
		metrics.Get().IncWALRecordsPreserved(int64(budgetExpiredRecords))
		b.walOnlyRecords.Add(int64(budgetExpiredRecords))
	}

	abandoned := b.drainAndFlushQueue(deadline)
	if abandoned > 0 {
		b.logger.Error().
			Int("records", abandoned).
			Msg("Flush tasks could not be written during close; records remain in the WAL and will be replayed on next startup")
		metrics.Get().IncWALRecordsPreserved(int64(abandoned))
		b.walOnlyRecords.Add(int64(abandoned))
	}

	// Record whether every record reached durable storage. The WAL purge on
	// shutdown consults this via CloseFlushedCleanly: purging the WAL after a
	// failed flush destroys the only remaining copy of that data (#803).
	//
	// "Clean" requires three things, not just the synchronous flush:
	//   - no synchronous flush returned an error (flushErrs)
	//   - no records ended up WAL-only: rejected at enqueue, or still unwritten
	//     after the drain above (walOnlyRecords)
	//   - no earlier async flush failed (hasFlushFailure) — the same signal the
	//     WAL maintenance loop already trusts
	//
	// The flag is latching: once a Close observes lost data it stays false for
	// the lifetime of the buffer. closeFailed guards that, so a second Close
	// (which finds no buffers left and no new errors) cannot report clean and
	// re-enable the purge — a repeat call cannot un-lose already-lost data.
	if len(flushErrs) > 0 || b.walOnlyRecords.Load() > 0 || b.hasFlushFailure.Load() {
		b.closeFailed.Store(true)
	}
	b.closeFlushClean.Store(!b.closeFailed.Load())

	b.logger.Info().
		Int64("total_records_written", b.totalRecordsWritten.Load()).
		Int64("total_flushes", b.totalFlushes.Load()).
		Int("failed_buffers", len(flushErrs)).
		Bool("flushed_cleanly", b.closeFlushClean.Load()).
		Msg("ArrowBuffer closed")

	if len(flushErrs) > 0 {
		return fmt.Errorf("ArrowBuffer close flushed %d of %d buffers: %w",
			totalBuffers-len(flushErrs), totalBuffers, errors.Join(flushErrs...))
	}

	return nil
}

// closeDrainParallelism caps how many drained tasks Close flushes at once.
//
// It is capped independently of flushWorkers because each concurrent flush
// merges a whole extracted batch (mergeBatches allocates a full copy), and
// flush_queue_size x max_buffer_size records can be a large transient at
// exactly the moment the process is shutting down under memory pressure.
const closeDrainParallelism = 4

// drainAndFlushQueue empties b.flushQueue and writes each task, returning the
// record count it could NOT write. The workers have returned by the time this
// runs, so nothing else receives from the queue and a non-blocking drain is
// race-free.
func (b *ArrowBuffer) drainAndFlushQueue(deadline time.Time) int {
	var tasks []flushTask
drain:
	for {
		select {
		case task, ok := <-b.flushQueue:
			if !ok {
				// Not reachable today (the queue is never closed), but a
				// labelled break keeps this loop terminating if that changes —
				// a bare break would only exit the select.
				break drain
			}
			b.queueDepth.Add(-1)
			tasks = append(tasks, task)
		default:
			break drain
		}
	}
	if len(tasks) == 0 {
		return 0
	}

	metrics.Get().SetBufferQueueDepth(b.queueDepth.Load())
	b.logger.Info().
		Int("tasks", len(tasks)).
		Msg("Flushing flush tasks that were still queued at close")

	parallel := b.flushWorkers
	if parallel > closeDrainParallelism {
		parallel = closeDrainParallelism
	}
	if parallel < 1 {
		parallel = 1
	}

	var (
		mu     sync.Mutex
		unwrit int
		wg     sync.WaitGroup // local: b.wg has already been waited on, and Close can be called twice
		slots  = make(chan struct{}, parallel)
	)

	for i := range tasks {
		task := tasks[i]
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()

			// Built HERE, on slot acquisition, not when the task was drained:
			// a context created up front would burn its budget waiting for a
			// slot, which is #1006 all over again inside Close.
			flushCtx, flushCancel, ok := b.closeFlushContext(deadline)
			if !ok {
				mu.Lock()
				unwrit += task.recordCount
				mu.Unlock()
				return
			}
			defer flushCancel()

			if err := b.flushRecordsAsync(flushCtx, task.bufferKey, task.database, task.measurement, task.records, task.recordCount, task.walHashes); err != nil {
				mu.Lock()
				unwrit += task.recordCount
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return unwrit
}

// closeFlushContext builds the context for one flush performed by Close, and
// reports whether there is any budget left to perform it at all.
//
// Two things here are load-bearing, and getting either wrong reintroduces a bug
// this change exists to fix:
//
//  1. The parent is context.WithoutCancel(b.flushParent). When the budget
//     expires, Close cancels flushParent to unstick workers blocked in a
//     storage write — but Close's OWN remaining flushes must still be able to
//     run. Deriving them from the cancelled parent would make every one of them
//     start already Done, so Close would write nothing at all: worse than the
//     behaviour before this change, where the close loop used
//     context.Background() and b.cancel() could not reach it.
//  2. The timeout is the real remaining budget, with no floor. A floor would
//     let Close exceed its budget by (units of work x floor) — with a default
//     flush_queue_size of 100 and parallelism 4, a 2s floor alone is ~50s past
//     the deadline. Overrunning makes the coordinator skip the components after
//     the buffer, including the WAL writer's final drain-and-sync.
//
// ok is false once the deadline has passed; the caller must then count the work
// as unwritten rather than attempting it, so the WAL is retained for it.
func (b *ArrowBuffer) closeFlushContext(deadline time.Time) (ctx context.Context, cancel context.CancelFunc, ok bool) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, nil, false
	}
	parent := b.flushParent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel = context.WithTimeout(context.WithoutCancel(parent), remaining)
	return ctx, cancel, true
}

// CloseFlushedCleanly reports whether every record this buffer accepted
// reached durable storage by the time Close() finished.
//
// It accounts for all three ways a record can fail to land:
//   - a synchronous flush error during Close()
//   - a record dropped to WAL-replay, either rejected at enqueue
//     (tryEnqueueFlush) or abandoned in the flush queue when Close() cancelled
//     the workers
//   - an earlier asynchronous flush failure (HasFlushFailure)
//
// It returns false until Close() has run to completion, so a caller that
// consults it before Close() (or when Close() never ran, e.g. a shutdown that
// timed out before reaching the buffer component) conservatively treats the
// data as unflushed. It is latching: once false due to lost data it never
// returns true again for this buffer.
//
// That bias is deliberate. The only consumer is the shutdown WAL purge, and
// retaining a WAL that turns out to be redundant costs one idempotent replay,
// whereas purging a WAL that was still needed is unrecoverable data loss
// (#803).
func (b *ArrowBuffer) CloseFlushedCleanly() bool {
	// Re-read the live signals rather than trusting only the snapshot Close
	// took. HTTP is drained before this component closes, but MQTT, continuous
	// queries and replication apply can still be writing, so an inline flush
	// can fail after Close computed the flag. A stale "clean" here would purge
	// a WAL that is still needed.
	if b.closeFailed.Load() || b.hasFlushFailure.Load() || b.walOnlyRecords.Load() > 0 {
		return false
	}
	return b.closeFlushClean.Load()
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
		"total_records_buffered":      b.totalRecordsBuffered.Load(),
		"total_records_written":       b.totalRecordsWritten.Load(),
		"total_flushes":               b.totalFlushes.Load(),
		"total_errors":                b.totalErrors.Load(),
		"total_wal_errors":            b.totalWALErrors.Load(),
		"total_wal_dropped":           b.totalWALDropped.Load(),
		"total_schema_churn_exceeded": b.totalSchemaChurnExceeded.Load(),
		// The field comment on totalFlushDeferred has claimed these were here
		// since #997; they were not.
		"total_flush_deferred": b.totalFlushDeferred.Load(),
		"deferred_buffers":     b.countDeferredBuffers(),
		"active_buffers":       activeBuffers,
		"flush_queue_depth":    b.queueDepth.Load(),
		"flush_workers":        b.flushWorkers,
	}
}
