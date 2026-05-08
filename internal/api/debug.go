package api

import (
	"context"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// freeOSMemoryDebounce debounces /debug/free-os-memory so polled callers
// can't pin the runtime in stop-the-world GC.
const freeOSMemoryDebounce = 30 * time.Second

// debugProcessStart anchors the debounce to the monotonic clock so wall-clock
// adjustments (NTP steps, manual `date` changes) cannot misbehave.
var debugProcessStart = time.Now()

// debugFreeOSMemoryLastNanos stores nanoseconds since debugProcessStart
// (monotonic) of the last /debug/free-os-memory call that wasn't throttled.
var debugFreeOSMemoryLastNanos atomic.Int64

// DebugHandler exposes process- and DuckDB-level memory diagnostics. Used to
// attribute RSS to Go heap, DuckDB native heap, or glibc arenas during
// support cases where the customer reports unexplained memory growth.
type DebugHandler struct {
	db          *database.DuckDB
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

func NewDebugHandler(db *database.DuckDB, authManager *auth.AuthManager, logger zerolog.Logger) *DebugHandler {
	return &DebugHandler{
		db:          db,
		authManager: authManager,
		logger:      logger.With().Str("component", "debug").Logger(),
	}
}

func (h *DebugHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/debug")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	} else {
		// Match retention/delete behaviour: when auth is disabled the routes
		// are open. Surface a warning so operators running auth-off in prod
		// know they've exposed a diagnostic surface.
		h.logger.Warn().Msg("debug endpoints registered WITHOUT auth (auth.enabled=false)")
	}
	group.Get("/memstats", h.handleMemstats)
	group.Get("/duckdb-memory", h.handleDuckDBMemory)
	group.Post("/free-os-memory", h.handleFreeOSMemory)
}

type memstatsResponse struct {
	Timestamp     string            `json:"timestamp"`
	Go            goMemStats        `json:"go"`
	Process       *processMemStats  `json:"process,omitempty"`
	GlibcHeap     *glibcHeapStats   `json:"glibc_heap,omitempty"`
	DuckDBSummary *duckdbMemSummary `json:"duckdb_summary,omitempty"`
}

type goMemStats struct {
	HeapAllocBytes    uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes    uint64 `json:"heap_inuse_bytes"`
	HeapIdleBytes     uint64 `json:"heap_idle_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	HeapSysBytes      uint64 `json:"heap_sys_bytes"`
	StackInuseBytes   uint64 `json:"stack_inuse_bytes"`
	SysBytes          uint64 `json:"sys_bytes"`
	NumGC             uint32 `json:"num_gc"`
	NextGCBytes       uint64 `json:"next_gc_bytes"`
	NumGoroutine      int    `json:"num_goroutine"`
}

type processMemStats struct {
	VmRSSBytes  uint64 `json:"vm_rss_bytes"`
	VmSizeBytes uint64 `json:"vm_size_bytes"`
	VmHWMBytes  uint64 `json:"vm_hwm_bytes"`
	VmDataBytes uint64 `json:"vm_data_bytes"`
	RssAnon     uint64 `json:"rss_anon_bytes"`
	RssFile     uint64 `json:"rss_file_bytes"`
}

type glibcHeapStats struct {
	HeapStartHex string `json:"heap_start_hex"`
	HeapEndHex   string `json:"heap_end_hex"`
	HeapSizeKB   uint64 `json:"heap_size_kb"`
}

type duckdbMemSummary struct {
	TotalBytes uint64              `json:"total_bytes"`
	ByTag      []duckdbMemTagEntry `json:"by_tag"`
	Error      string              `json:"error,omitempty"`
}

type duckdbMemTagEntry struct {
	Tag         string `json:"tag"`
	MemoryBytes uint64 `json:"memory_bytes"`
	TempBytes   uint64 `json:"temp_bytes"`
}

func (h *DebugHandler) handleMemstats(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	resp := memstatsResponse{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Go: goMemStats{
			HeapAllocBytes:    ms.HeapAlloc,
			HeapInuseBytes:    ms.HeapInuse,
			HeapIdleBytes:     ms.HeapIdle,
			HeapReleasedBytes: ms.HeapReleased,
			HeapSysBytes:      ms.HeapSys,
			StackInuseBytes:   ms.StackInuse,
			SysBytes:          ms.Sys,
			NumGC:             ms.NumGC,
			NextGCBytes:       ms.NextGC,
			NumGoroutine:      runtime.NumGoroutine(),
		},
		Process:       readProcStatus(h.logger),
		GlibcHeap:     readGlibcHeap(h.logger),
		DuckDBSummary: h.duckdbMemSummary(ctx),
	}

	return c.JSON(resp)
}

func (h *DebugHandler) handleDuckDBMemory(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	summary := h.duckdbMemSummary(ctx)
	if summary == nil {
		// duckdbMemSummary returns nil only when the parent context is
		// already cancelled or expired before the query could run. Surface
		// it as 504 instead of pretending the heap is empty.
		return c.Status(fiber.StatusGatewayTimeout).JSON(fiber.Map{
			"error": "Context cancelled before DuckDB memory query",
		})
	}
	if summary.Error != "" {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to query DuckDB memory",
		})
	}

	return c.JSON(fiber.Map{
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"total_bytes": summary.TotalBytes,
		"by_tag":      summary.ByTag,
	})
}

// handleFreeOSMemory triggers debug.FreeOSMemory and returns the delta in
// HeapReleased so callers can see how many bytes the runtime returned to the
// OS. Throttled to one call per freeOSMemoryDebounce; debug.FreeOSMemory is
// stop-the-world and a tight polling loop would tank ingest throughput.
func (h *DebugHandler) handleFreeOSMemory(c *fiber.Ctx) error {
	now := time.Since(debugProcessStart).Nanoseconds()
	last := debugFreeOSMemoryLastNanos.Load()
	// last==0 means "never fired" — first call always proceeds.
	if last != 0 && now-last < int64(freeOSMemoryDebounce) {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error":              "throttled",
			"retry_after_seconds": int64(freeOSMemoryDebounce/time.Second) - (now-last)/int64(time.Second),
		})
	}
	if !debugFreeOSMemoryLastNanos.CompareAndSwap(last, now) {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error": "throttled",
		})
	}

	if info := auth.GetTokenInfo(c); info != nil {
		h.logger.Info().Str("token_name", info.Name).Msg("FreeOSMemory invoked via debug endpoint")
	} else {
		h.logger.Info().Msg("FreeOSMemory invoked via debug endpoint")
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	debug.FreeOSMemory()
	dur := time.Since(start)
	runtime.ReadMemStats(&after)

	// Subtract on the uint64s first then reinterpret as int64. For realistic
	// HeapReleased values (well under 2^63) this produces the correct signed
	// delta, including a negative value if the counter went backwards.
	delta := int64(after.HeapReleased - before.HeapReleased)

	return c.JSON(fiber.Map{
		"timestamp":            time.Now().UTC().Format(time.RFC3339Nano),
		"duration_ms":          float64(dur.Microseconds()) / 1000.0,
		"heap_released_before": before.HeapReleased,
		"heap_released_after":  after.HeapReleased,
		"heap_released_delta":  delta,
		"heap_idle_before":     before.HeapIdle,
		"heap_idle_after":      after.HeapIdle,
		"sys_before":           before.Sys,
		"sys_after":            after.Sys,
	})
}

// duckdbMemSummary queries duckdb_memory() and returns the aggregated view.
// Returns nil if the parent context is already cancelled, otherwise an error
// is reported via the Error field on the returned struct (so the JSON shape
// stays consistent for callers).
func (h *DebugHandler) duckdbMemSummary(parent context.Context) *duckdbMemSummary {
	if err := parent.Err(); err != nil {
		return nil
	}

	rows, err := h.db.QueryContext(parent, "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()")
	if err != nil {
		h.logger.Error().Err(err).Msg("duckdb_memory query failed")
		return &duckdbMemSummary{Error: "query failed"}
	}
	defer rows.Close()

	out := &duckdbMemSummary{ByTag: make([]duckdbMemTagEntry, 0, 32)}
	for rows.Next() {
		var e duckdbMemTagEntry
		if err := rows.Scan(&e.Tag, &e.MemoryBytes, &e.TempBytes); err != nil {
			h.logger.Error().Err(err).Msg("duckdb_memory scan failed")
			out.Error = "scan failed"
			return out
		}
		out.TotalBytes += e.MemoryBytes
		out.ByTag = append(out.ByTag, e)
	}
	if err := rows.Err(); err != nil {
		h.logger.Error().Err(err).Msg("duckdb_memory iteration failed")
		out.Error = "iteration failed"
	}
	return out
}

// parseKBLine parses "1234 kB" → 1234*1024. Returns 0 unless the line is in
// the exact "<int> kB" shape that /proc/self/status emits — silently
// rescaling a different-unit line (e.g. a future "1234 mB") would mis-report
// memory by orders of magnitude on dashboards. Used by the linux-only
// readProcStatus implementation in debug_proc_linux.go.
func parseKBLine(v string) uint64 {
	fields := strings.Fields(v)
	if len(fields) != 2 || fields[1] != "kB" {
		return 0
	}
	n, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0
	}
	return n * 1024
}
