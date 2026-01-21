package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestBufferFlushTimingUnderLoad tests age-based flushing under high write load
// This simulates production conditions with:
// - 4 high-volume measurements (frequently hit size limit)
// - 4 low-volume measurements (rely on age-based flushing)
func TestBufferFlushTimingUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	// Create temporary directory for test data
	tmpDir, err := os.MkdirTemp("", "arc-load-timing-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Configure similar to production
	maxBufferAgeMS := 5000
	cfg := &config.IngestConfig{
		MaxBufferSize:  100000,        // Lower to trigger size flushes faster
		MaxBufferAgeMS: maxBufferAgeMS, // 5 seconds
		FlushWorkers:   8,              // More workers like production
		FlushQueueSize: 20,
		ShardCount:     32, // Full shard count
		Compression:    "none",
	}

	// Use info level to see aged buffer flushes
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()

	localStorage, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer localStorage.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buffer := NewArrowBuffer(cfg, localStorage, logger)
	defer buffer.Close()

	t.Log("Starting high-load test with mixed workload")

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// High-volume measurements (will trigger size-based flushes)
	highVolumeMeasurements := []string{"cpu", "mem", "disk", "net"}
	for _, measurement := range highVolumeMeasurements {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			count := 0
			for {
				select {
				case <-stopCh:
					t.Logf("High-volume writer %s wrote %d batches", m, count)
					return
				default:
					// Write batches of 1000 records rapidly
					timeCol := make([]interface{}, 1000)
					valueCol := make([]interface{}, 1000)
					now := time.Now().UnixMicro()
					for i := 0; i < 1000; i++ {
						timeCol[i] = now + int64(i)
						valueCol[i] = float64(i)
					}
					columns := map[string][]interface{}{
						"time":  timeCol,
						"value": valueCol,
					}

					if err := buffer.WriteColumnarDirect(ctx, "testdb", m, columns); err != nil {
						t.Errorf("Write failed for %s: %v", m, err)
						return
					}
					count++
					time.Sleep(50 * time.Millisecond) // Sustain high load
				}
			}
		}(measurement)
	}

	// Low-volume measurements (will rely on age-based flushes)
	lowVolumeMeasurements := []string{"alerts", "events", "status", "heartbeat"}
	startTimes := make(map[string]time.Time)
	actualFlushTimes := make(map[string]time.Time)
	var timeMu sync.Mutex

	for _, measurement := range lowVolumeMeasurements {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()

			// Write a single small batch and record start time
			columns := map[string][]interface{}{
				"time":  {time.Now().UnixMicro()},
				"value": {"test"},
			}

			writeTime := time.Now()
			timeMu.Lock()
			startTimes[m] = writeTime
			timeMu.Unlock()

			if err := buffer.WriteColumnarDirect(ctx, "testdb", m, columns); err != nil {
				t.Errorf("Write failed for %s: %v", m, err)
				return
			}

			t.Logf("Low-volume measurement %s written at %v", m, writeTime)
		}(measurement)
	}

	// Monitor for parquet files appearing (indicates flush)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, m := range lowVolumeMeasurements {
					timeMu.Lock()
					_, alreadyFlushed := actualFlushTimes[m]
					timeMu.Unlock()

					if alreadyFlushed {
						continue
					}

					// Check if parquet file exists
					pattern := fmt.Sprintf("%s/testdb/%s/*/*/*/*.parquet", tmpDir, m)
					matches, _ := filepath.Glob(pattern)
					if len(matches) > 0 {
						flushTime := time.Now()
						timeMu.Lock()
						actualFlushTimes[m] = flushTime
						startTime := startTimes[m]
						timeMu.Unlock()

						duration := flushTime.Sub(startTime)
						t.Logf("✓ Low-volume measurement %s flushed after %v", m, duration)
					}
				}

				// Check if all low-volume measurements have flushed
				timeMu.Lock()
				allFlushed := len(actualFlushTimes) == len(lowVolumeMeasurements)
				timeMu.Unlock()

				if allFlushed {
					return
				}
			}
		}
	}()

	// Run for 10 seconds to allow age-based flushes to trigger
	time.Sleep(10 * time.Second)

	// Stop high-volume writers
	close(stopCh)
	wg.Wait()

	// Analyze results
	t.Log("\n=== Age-Based Flush Timing Results ===")
	var totalDuration time.Duration
	var maxDuration time.Duration
	count := 0

	timeMu.Lock()
	for _, m := range lowVolumeMeasurements {
		start, hasStart := startTimes[m]
		flush, hasFlushed := actualFlushTimes[m]

		if !hasStart {
			t.Errorf("❌ Measurement %s was never written", m)
			continue
		}

		if !hasFlushed {
			t.Errorf("❌ Measurement %s never flushed (written at %v)", m, start)
			continue
		}

		duration := flush.Sub(start)
		totalDuration += duration
		if duration > maxDuration {
			maxDuration = duration
		}
		count++

		expectedMax := time.Duration(maxBufferAgeMS)*time.Millisecond + 1*time.Second // 5s + 1s tolerance
		status := "✓"
		if duration > expectedMax {
			status = "⚠️"
		}

		t.Logf("%s %s: %v (expected ≤ %v)", status, m, duration, expectedMax)
	}
	timeMu.Unlock()

	if count > 0 {
		avgDuration := totalDuration / time.Duration(count)
		t.Logf("\nAverage flush time: %v", avgDuration)
		t.Logf("Max flush time:     %v", maxDuration)
		t.Logf("Expected:           ≤ %dms + tolerance", maxBufferAgeMS)

		// Under load, we expect some delay, but should not be excessive
		maxAllowed := time.Duration(maxBufferAgeMS)*time.Millisecond + 2*time.Second
		if maxDuration > maxAllowed {
			t.Errorf("Max flush time %v exceeded allowed threshold %v", maxDuration, maxAllowed)
		}
	} else {
		t.Error("No low-volume measurements were successfully flushed")
	}
}
