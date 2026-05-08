package api

import (
	"bufio"
	"context"
	"fmt"
	"os"
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

// freeOSMemoryDebounceNano debounces /debug/free-os-memory so polled callers
// can't pin the runtime in stop-the-world GC.
const freeOSMemoryDebounce = 30 * time.Second

var debugFreeOSMemoryLastNano atomic.Int64

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
		Process:       readProcStatus(),
		GlibcHeap:     readGlibcHeap(),
		DuckDBSummary: h.duckdbMemSummary(ctx),
	}

	return c.JSON(resp)
}

func (h *DebugHandler) handleDuckDBMemory(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	summary := h.duckdbMemSummary(ctx)
	if summary != nil && summary.Error != "" {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to query DuckDB memory",
		})
	}

	if summary == nil {
		summary = &duckdbMemSummary{}
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
	now := time.Now().UnixNano()
	last := debugFreeOSMemoryLastNano.Load()
	if now-last < int64(freeOSMemoryDebounce) {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error":              "throttled",
			"retry_after_seconds": int64(freeOSMemoryDebounce/time.Second) - (now-last)/int64(time.Second),
		})
	}
	if !debugFreeOSMemoryLastNano.CompareAndSwap(last, now) {
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

// readProcStatus parses /proc/self/status. Linux-only; returns nil elsewhere
// or if the read failed (errors are logged at trace via the file handle and
// surfaced as a missing Process field in the response).
func readProcStatus() *processMemStats {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return nil
	}
	defer f.Close()

	out := &processMemStats{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		key := line[:colon]
		val := strings.TrimSpace(line[colon+1:])
		switch key {
		case "VmRSS":
			out.VmRSSBytes = parseKBLine(val)
		case "VmSize":
			out.VmSizeBytes = parseKBLine(val)
		case "VmHWM":
			out.VmHWMBytes = parseKBLine(val)
		case "VmData":
			out.VmDataBytes = parseKBLine(val)
		case "RssAnon":
			out.RssAnon = parseKBLine(val)
		case "RssFile":
			out.RssFile = parseKBLine(val)
		}
	}
	// Scanner errors (truncated read, EIO, LSM denial mid-stream) are
	// swallowed silently — this is a diagnostic-only path and partial data
	// is still useful. Caller sees a partially-populated struct.
	_ = scanner.Err()
	return out
}

// parseKBLine parses "1234 kB" → 1234*1024. Returns 0 unless the line is in
// the exact "<int> kB" shape that /proc/self/status emits — silently
// rescaling a different-unit line (e.g. a future "1234 mB") would mis-report
// memory by orders of magnitude on dashboards.
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

// readGlibcHeap walks /proc/self/maps for the [heap] segment so we can see
// the size of the program break — distinct from the mmap'd glibc arenas.
// Together with VmRSS, this lets us tell whether RSS bloat is in [heap]
// (single arena) or in mmap'd anonymous regions (multiple arenas).
// Linux-only; returns nil elsewhere.
func readGlibcHeap() *glibcHeapStats {
	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return nil
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// /proc/self/maps lines for some kernels exceed bufio's default 64 KiB
	// when paths are deep. Give the scanner a 1 MiB ceiling.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		// /proc/<pid>/maps emits "[heap]" as the trailing pseudo-pathname for the
		// program-break segment (kernel fs/proc/task_mmu.c). Hardened kernels can
		// strip pseudo-pathnames altogether — the function then silently returns
		// nil and the response omits the glibc_heap field, which is fine.
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[len(fields)-1] != "[heap]" {
			continue
		}
		// fields[0] is "<startHex>-<endHex>"
		dash := strings.IndexByte(fields[0], '-')
		if dash < 0 {
			continue
		}
		startHex := fields[0][:dash]
		endHex := fields[0][dash+1:]
		start, err1 := strconv.ParseUint(startHex, 16, 64)
		end, err2 := strconv.ParseUint(endHex, 16, 64)
		if err1 != nil || err2 != nil || end < start {
			continue
		}
		return &glibcHeapStats{
			HeapStartHex: fmt.Sprintf("0x%s", startHex),
			HeapEndHex:   fmt.Sprintf("0x%s", endHex),
			HeapSizeKB:   (end - start) / 1024,
		}
	}
	// No [heap] segment found, or scanner errored mid-read. Both cases
	// produce nil — diagnostic-only path, JSON omits glibc_heap.
	_ = scanner.Err()
	return nil
}
