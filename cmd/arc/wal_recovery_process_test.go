package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Block at the storage boundary after the real recovery barrier submits a
// Parquet write. The parent kills this process without running Close/defers.
type processRecoveryStorage struct {
	storage.Backend
	once sync.Once
}

func (b *processRecoveryStorage) gate(ctx context.Context, key string) error {
	if strings.HasSuffix(key, ".parquet") {
		b.once.Do(func() { fmt.Println("ARC_PROCESS_READY:blocked") })
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (b *processRecoveryStorage) Write(ctx context.Context, key string, data []byte) error {
	if err := b.gate(ctx, key); err != nil {
		return err
	}
	return b.Backend.Write(ctx, key, data)
}

func (b *processRecoveryStorage) WriteReader(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := b.gate(ctx, key); err != nil {
		return err
	}
	return b.Backend.WriteReader(ctx, key, r, size)
}

func recoveryProcess(t *testing.T, root, phase string, kill bool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWALRecoveryProcessHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "ARC_RECOVERY_PROCESS_ROOT="+root, "ARC_RECOVERY_PROCESS_PHASE="+phase)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	ready := make(chan string, 1)
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "ARC_PROCESS_READY:") {
				select {
				case ready <- strings.TrimPrefix(line, "ARC_PROCESS_READY:"):
				default:
				}
			} else {
				t.Logf("%s child: %s", phase, line)
			}
		}
	}()
	// Drain stdout before Wait closes the pipe. A crashed helper must never
	// leave either a live child or a goroutine logging after the parent test.
	done := make(chan error, 1)
	go func() { <-scanned; done <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	if !kill {
		err := <-done
		waited = true
		require.NoError(t, err, "%s: %s", phase, stderr.String())
		return ""
	}
	var stage string
	select {
	case stage = <-ready:
	case err := <-done:
		waited = true
		t.Fatalf("%s exited before its crash point: %v; %s", phase, err, stderr.String())
	case <-ctx.Done():
		t.Fatalf("%s did not reach its crash point: %v", phase, ctx.Err())
	}
	require.NoError(t, cmd.Process.Kill())
	err = <-done
	waited = true
	require.Error(t, err, "crash must not be a graceful exit")
	return stage
}

func testWALRecoveryAcrossTwoProcessKills(t *testing.T, count int, source []string, dataRoot string) {
	t.Helper()
	root := t.TempDir()
	if dataRoot == "" {
		dataRoot = filepath.Join(root, "data")
	}
	rows := make([]map[string]interface{}, count)
	for i := range rows {
		rows[i] = map[string]interface{}{"_database": "process_test", "_measurement": "two_crashes", "time": int64(1700000000000000 + i), "index": i}
		if len(source) > 0 {
			rows[i]["source_line"] = source[i]
		}
	}
	input, err := json.Marshal(rows)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "input.json"), input, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "data-path"), []byte(dataRoot), 0600))
	require.Equal(t, "seeded", recoveryProcess(t, root, "seed", true))
	sourcePath, err := os.ReadFile(filepath.Join(root, "source-wal"))
	require.NoError(t, err)
	dataFile := string(sourcePath)
	before, err := os.ReadFile(dataFile)
	require.NoError(t, err)
	entries, err := wal.NewReader(dataFile, zerolog.Nop()).ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Records, count)

	stage := recoveryProcess(t, root, "interrupted-replay", true)
	require.FileExists(t, dataFile, "recovery deleted its WAL before storage completed")
	after, err := os.ReadFile(dataFile)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(before), sha256.Sum256(after))
	require.Equal(t, "blocked", stage, "kill must occur inside the outstanding Parquet write")
	backend, err := storage.NewLocalBackend(dataRoot, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	keys, err := backend.List(context.Background(), "process_test/two_crashes/")
	require.NoError(t, err)
	for _, key := range keys {
		require.False(t, strings.HasSuffix(key, ".parquet"), "interrupted child published Parquet")
	}

	recoveryProcess(t, root, "healthy-replay", false)
	require.NoFileExists(t, dataFile)
	keys, err = backend.List(context.Background(), "process_test/two_crashes/")
	require.NoError(t, err)
	seen := make(map[int64]int)
	for _, key := range keys {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		data, err := backend.Read(context.Background(), key)
		require.NoError(t, err)
		table, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		func() {
			defer table.Release()
			indices := table.Schema().FieldIndices("index")
			require.Len(t, indices, 1)
			for _, chunk := range table.Column(indices[0]).Data().Chunks() {
				require.Zero(t, chunk.NullN())
				switch values := chunk.(type) {
				case *array.Int64:
					for _, v := range values.Int64Values() {
						seen[v]++
					}
				case *array.Float64:
					for _, v := range values.Float64Values() {
						require.Equal(t, float64(int64(v)), v)
						seen[int64(v)]++
					}
				default:
					t.Fatalf("unexpected index type %T", chunk)
				}
			}
		}()
	}
	require.Len(t, seen, count)
	for i := 0; i < count; i++ {
		require.Equal(t, 1, seen[int64(i)], "Parquet index %d", i)
	}
}

func TestWALRecoveryAcrossTwoProcessKills(t *testing.T) {
	testWALRecoveryAcrossTwoProcessKills(t, 7, nil, "")
}

func TestWALRecoveryProcessHelper(t *testing.T) {
	root := os.Getenv("ARC_RECOVERY_PROCESS_ROOT")
	if root == "" {
		return
	}
	phase := os.Getenv("ARC_RECOVERY_PROCESS_PHASE")
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(root, "wal"), SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	if phase == "seed" {
		input, err := os.ReadFile(filepath.Join(root, "input.json"))
		require.NoError(t, err)
		var rows []map[string]interface{}
		require.NoError(t, json.Unmarshal(input, &rows))
		_, err = w.AppendTracked(rows)
		require.NoError(t, err)
		// AppendTracked admits asynchronous work. Rotate queues behind it and
		// closes/syncs that file before announcing a durable seed to the parent.
		dataFile := w.CurrentFile()
		require.NoError(t, w.Rotate())
		require.NoError(t, os.WriteFile(filepath.Join(root, "source-wal"), []byte(dataFile), 0600))
		fmt.Println("ARC_PROCESS_READY:seeded")
		select {}
	}
	require.Contains(t, []string{"interrupted-replay", "healthy-replay"}, phase)
	path, err := os.ReadFile(filepath.Join(root, "data-path"))
	require.NoError(t, err)
	local, err := storage.NewLocalBackend(string(path), zerolog.Nop())
	require.NoError(t, err)
	var backend storage.Backend = local
	if phase == "interrupted-replay" {
		backend = &processRecoveryStorage{Backend: local}
	}
	b := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 8, FlushTimeoutSeconds: 30}, backend, zerolog.Nop())
	b.SetWAL(w)
	stats, err := wal.NewRecovery(filepath.Join(root, "wal"), zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &wal.RecoveryOptions{
		SkipActiveFile: w.CurrentFile(), TrackedRowCallback: createTrackedWALRecoveryCallback(b, zerolog.Nop()),
		BeforeDelete: b.NewRecoveryFlushBarrier(),
	})
	require.NoError(t, err)
	require.Zero(t, stats.KeptFiles)
	if phase == "interrupted-replay" {
		fmt.Println("ARC_PROCESS_READY:recovered")
		select {}
	}
	require.NoError(t, b.Close())
	require.NoError(t, w.Close())
}
