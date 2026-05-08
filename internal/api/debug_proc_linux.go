//go:build linux

package api

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// readProcStatus parses /proc/self/status into a processMemStats summary.
// Linux-only build; the !linux stub returns nil. A scanner error mid-read
// produces a partially-populated struct plus a Debug log so an operator
// debugging a truncated response can find the cause.
func readProcStatus(logger zerolog.Logger) *processMemStats {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return nil
	}
	defer f.Close()

	out := &processMemStats{}
	scanner := bufio.NewScanner(f)
	// Match the buffer size used by readGlibcHeap so a future kernel that
	// adds a long line to /proc/self/status doesn't silently truncate.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
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
	if err := scanner.Err(); err != nil {
		// Diagnostic-only path: still return what we parsed, but tell an
		// operator the read was truncated. Fires at most once per /memstats
		// request so it cannot spam the log.
		logger.Debug().Err(err).Msg("partial read of /proc/self/status")
	}
	return out
}

// readGlibcHeap walks /proc/self/maps for the [heap] segment so we can tell
// whether RSS bloat lives in the program-break heap (single arena) or in
// mmap'd anonymous regions (multiple per-thread arenas). Returns nil when
// the [heap] line is absent — hardened kernels and some LSMs strip the
// pseudo-pathname, in which case the response simply omits glibc_heap.
func readGlibcHeap(logger zerolog.Logger) *glibcHeapStats {
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
	if err := scanner.Err(); err != nil {
		logger.Debug().Err(err).Msg("partial read of /proc/self/maps")
	}
	return nil
}
