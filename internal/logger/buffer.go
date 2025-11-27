package logger

import (
	"io"
	"strings"
	"sync"
	"time"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Component string    `json:"component,omitempty"`
	Message   string    `json:"message"`
	Caller    string    `json:"caller,omitempty"`
}

// LogBuffer is a circular buffer that stores recent log entries
type LogBuffer struct {
	mu       sync.RWMutex
	entries  []LogEntry
	size     int
	writePos int
	count    int
}

var (
	globalBuffer *LogBuffer
	bufferOnce   sync.Once
)

// GetBuffer returns the global log buffer instance
func GetBuffer() *LogBuffer {
	bufferOnce.Do(func() {
		globalBuffer = NewLogBuffer(10000) // Store last 10k entries
	})
	return globalBuffer
}

// NewLogBuffer creates a new log buffer with specified capacity
func NewLogBuffer(size int) *LogBuffer {
	return &LogBuffer{
		entries: make([]LogEntry, size),
		size:    size,
	}
}

// Add adds a log entry to the buffer
func (b *LogBuffer) Add(entry LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries[b.writePos] = entry
	b.writePos = (b.writePos + 1) % b.size
	if b.count < b.size {
		b.count++
	}
}

// GetRecent returns the most recent log entries
func (b *LogBuffer) GetRecent(limit int, level string, sinceMinutes int) []LogEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if limit <= 0 || limit > b.count {
		limit = b.count
	}

	cutoffTime := time.Now().Add(-time.Duration(sinceMinutes) * time.Minute)
	levelUpper := strings.ToUpper(level)

	// Collect matching entries from most recent to oldest
	var result []LogEntry
	for i := 0; i < b.count && len(result) < limit; i++ {
		idx := (b.writePos - 1 - i + b.size) % b.size
		entry := b.entries[idx]

		// Skip if before cutoff time
		if entry.Timestamp.Before(cutoffTime) {
			continue
		}

		// Filter by level if specified
		if levelUpper != "" && !matchesLevel(entry.Level, levelUpper) {
			continue
		}

		result = append(result, entry)
	}

	return result
}

// matchesLevel checks if the entry level matches or exceeds the filter level
func matchesLevel(entryLevel, filterLevel string) bool {
	levels := map[string]int{
		"DEBUG": 0,
		"INFO":  1,
		"WARN":  2,
		"ERROR": 3,
		"FATAL": 4,
	}

	entryPriority, ok1 := levels[strings.ToUpper(entryLevel)]
	filterPriority, ok2 := levels[filterLevel]

	if !ok1 || !ok2 {
		return strings.EqualFold(entryLevel, filterLevel)
	}

	return entryPriority >= filterPriority
}

// Count returns the current number of entries in the buffer
func (b *LogBuffer) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.count
}

// LogBufferWriter is an io.Writer that captures log output and stores in buffer
type LogBufferWriter struct {
	buffer   *LogBuffer
	original io.Writer
}

// NewLogBufferWriter creates a writer that captures logs to buffer
func NewLogBufferWriter(original io.Writer) *LogBufferWriter {
	return &LogBufferWriter{
		buffer:   GetBuffer(),
		original: original,
	}
}

// Write implements io.Writer, parsing zerolog JSON and storing entries
func (w *LogBufferWriter) Write(p []byte) (n int, err error) {
	// Write to original output first
	if w.original != nil {
		n, err = w.original.Write(p)
	} else {
		n = len(p)
	}

	// Parse and store the log entry
	entry := parseLogLine(string(p))
	if entry.Message != "" || entry.Level != "" {
		w.buffer.Add(entry)
	}

	return n, err
}

// parseLogLine extracts log entry from zerolog JSON output
func parseLogLine(line string) LogEntry {
	entry := LogEntry{
		Timestamp: time.Now(),
	}

	// Simple parsing of zerolog JSON format
	// Format: {"level":"info","component":"api-server","time":"...","message":"..."}

	if idx := strings.Index(line, `"level":"`); idx >= 0 {
		start := idx + 9
		end := strings.Index(line[start:], `"`)
		if end > 0 {
			entry.Level = strings.ToUpper(line[start : start+end])
		}
	}

	if idx := strings.Index(line, `"component":"`); idx >= 0 {
		start := idx + 13
		end := strings.Index(line[start:], `"`)
		if end > 0 {
			entry.Component = line[start : start+end]
		}
	}

	if idx := strings.Index(line, `"message":"`); idx >= 0 {
		start := idx + 11
		end := strings.Index(line[start:], `"`)
		if end > 0 {
			entry.Message = line[start : start+end]
		}
	}

	// Also try "msg" field (zerolog uses both)
	if entry.Message == "" {
		if idx := strings.Index(line, `"msg":"`); idx >= 0 {
			start := idx + 7
			end := strings.Index(line[start:], `"`)
			if end > 0 {
				entry.Message = line[start : start+end]
			}
		}
	}

	if idx := strings.Index(line, `"caller":"`); idx >= 0 {
		start := idx + 10
		end := strings.Index(line[start:], `"`)
		if end > 0 {
			entry.Caller = line[start : start+end]
		}
	}

	if idx := strings.Index(line, `"time":"`); idx >= 0 {
		start := idx + 8
		end := strings.Index(line[start:], `"`)
		if end > 0 {
			if t, err := time.Parse(time.RFC3339, line[start:start+end]); err == nil {
				entry.Timestamp = t
			}
		}
	}

	return entry
}
