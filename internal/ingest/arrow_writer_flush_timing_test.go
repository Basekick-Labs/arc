package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestBufferFlushTiming verifies that buffers are flushed within the configured max_buffer_age_ms
// This test is designed to catch the ticker phase misalignment bug where buffers can take
// up to 2x the configured time to flush.
func TestBufferFlushTiming(t *testing.T) {
	// Create temporary directory for test data
	tmpDir, err := os.MkdirTemp("", "arc-flush-timing-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Configure with 5 second max buffer age
	maxBufferAgeMS := 5000
	cfg := &config.IngestConfig{
		MaxBufferSize:  1000000,       // 1MB - high enough to not trigger size-based flush
		MaxBufferAgeMS: maxBufferAgeMS, // 5 seconds
		FlushWorkers:   2,
		FlushQueueSize: 10,
		ShardCount:     4,
		Compression:    "none",
	}

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create storage backend
	localStorage, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer localStorage.Close()

	// Create the buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buffer := NewArrowBuffer(cfg, localStorage, logger)
	defer buffer.Close()

	// Record when we start the test
	testStartTime := time.Now()
	t.Logf("Test started at: %v", testStartTime)

	// Write a single record to create a buffer
	// This will set the buffer start time
	columns := map[string][]interface{}{
		"time":  {time.Now().UnixMicro()},
		"host":  {"test-server"},
		"value": {42.0},
	}

	writeStartTime := time.Now()
	err = buffer.WriteColumnarDirect(ctx, "testdb", "cpu", columns)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	t.Logf("Write completed at: %v (elapsed: %v)", time.Now(), time.Since(writeStartTime))

	// Now we wait and monitor when the buffer gets flushed
	// The buffer should be flushed within max_buffer_age_ms + some tolerance
	expectedMaxFlushTime := time.Duration(maxBufferAgeMS) * time.Millisecond
	tolerance := 1000 * time.Millisecond // 1 second tolerance for test timing

	// Create a channel to detect when flush happens
	// We'll poll the data directory to see when the parquet file appears
	flushDetected := make(chan time.Time, 1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Check if parquet file exists
				pattern := filepath.Join(tmpDir, "testdb", "cpu", "*", "*", "*", "*", "*.parquet")
				matches, _ := filepath.Glob(pattern)
				if len(matches) > 0 {
					flushDetected <- time.Now()
					return
				}
			}
		}
	}()

	// Wait for flush or timeout
	var flushTime time.Time
	select {
	case flushTime = <-flushDetected:
		actualFlushDuration := flushTime.Sub(writeStartTime)
		t.Logf("Flush detected at: %v (elapsed since write: %v)", flushTime, actualFlushDuration)

		// Check if flush happened within expected time
		maxAllowedTime := expectedMaxFlushTime + tolerance
		if actualFlushDuration > maxAllowedTime {
			t.Errorf("Buffer flush took too long: %v (expected: ≤ %v, configured: %v)",
				actualFlushDuration, maxAllowedTime, expectedMaxFlushTime)
			t.Errorf("This indicates the ticker phase misalignment bug is present")
		} else {
			t.Logf("✓ Buffer flushed within expected time: %v ≤ %v", actualFlushDuration, maxAllowedTime)
		}

		// Additional check: it should NOT take more than 1.5x the configured time
		// (anything more indicates a serious timing issue)
		if actualFlushDuration > expectedMaxFlushTime*3/2 {
			t.Errorf("Buffer flush took significantly longer than configured: %v > 1.5 * %v",
				actualFlushDuration, expectedMaxFlushTime)
		}

	case <-ctx.Done():
		t.Fatal("Timeout waiting for buffer flush")
	}
}

// TestBufferFlushTimingMultiple runs the timing test multiple times to account for variance
func TestBufferFlushTimingMultiple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timing test in short mode")
	}

	const iterations = 5
	var totalDuration time.Duration
	var maxDuration time.Duration
	var minDuration time.Duration = 1000 * time.Second

	maxBufferAgeMS := 5000
	expectedMaxFlushTime := time.Duration(maxBufferAgeMS) * time.Millisecond
	tolerance := 1000 * time.Millisecond

	for i := 0; i < iterations; i++ {
		t.Run("", func(t *testing.T) {
			// Create temporary directory for test data
			tmpDir, err := os.MkdirTemp("", "arc-flush-timing-test-*")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			cfg := &config.IngestConfig{
				MaxBufferSize:  1000000,
				MaxBufferAgeMS: maxBufferAgeMS,
				FlushWorkers:   2,
				FlushQueueSize: 10,
				ShardCount:     4,
				Compression:    "none",
			}

			logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

			localStorage, err := storage.NewLocalBackend(tmpDir, logger)
			if err != nil {
				t.Fatalf("Failed to create storage: %v", err)
			}
			defer localStorage.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			buffer := NewArrowBuffer(cfg, localStorage, logger)
			defer buffer.Close()

			columns := map[string][]interface{}{
				"time":  {time.Now().UnixMicro()},
				"host":  {"test-server"},
				"value": {42.0},
			}

			writeStartTime := time.Now()
			err = buffer.WriteColumnarDirect(ctx, "testdb", "cpu", columns)
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}

			// Poll for flush
			flushDetected := make(chan time.Time, 1)
			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						pattern := filepath.Join(tmpDir, "testdb", "cpu", "*", "*", "*", "*", "*.parquet")
						matches, _ := filepath.Glob(pattern)
						if len(matches) > 0 {
							flushDetected <- time.Now()
							return
						}
					}
				}
			}()

			select {
			case flushTime := <-flushDetected:
				duration := flushTime.Sub(writeStartTime)
				totalDuration += duration
				if duration > maxDuration {
					maxDuration = duration
				}
				if duration < minDuration {
					minDuration = duration
				}
				t.Logf("Iteration %d: flush took %v", i+1, duration)

			case <-ctx.Done():
				t.Fatal("Timeout waiting for buffer flush")
			}
		})
	}

	// Summary statistics
	avgDuration := totalDuration / time.Duration(iterations)
	t.Logf("\nSummary over %d iterations:", iterations)
	t.Logf("  Average flush time: %v", avgDuration)
	t.Logf("  Min flush time:     %v", minDuration)
	t.Logf("  Max flush time:     %v", maxDuration)
	t.Logf("  Expected:           %v ± %v", expectedMaxFlushTime, tolerance)

	maxAllowedTime := expectedMaxFlushTime + tolerance
	if maxDuration > maxAllowedTime {
		t.Errorf("Maximum flush time exceeded expected: %v > %v", maxDuration, maxAllowedTime)
		t.Errorf("This confirms the ticker phase misalignment bug")
	}

	if avgDuration > expectedMaxFlushTime+tolerance/2 {
		t.Errorf("Average flush time higher than expected: %v > %v", avgDuration, expectedMaxFlushTime+tolerance/2)
	}
}
