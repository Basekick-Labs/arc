package backup

// Regression test for the Progress publication race (found in the #626
// review): the manager published its live *Progress and kept writing to it —
// Status, Error, totals, and counters — while GetProgress readers (the
// /status handler, the API admission check) read the same object with no
// synchronization. Run under -race, a poller during a live backup trips the
// detector on the pre-fix code. The fix publishes immutable snapshots.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestGetProgress_DoesNotRaceALiveBackup(t *testing.T) {
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("data backend: %v", err)
	}
	t.Cleanup(func() { data.Close() })

	// Enough files that the copy loop and the poller genuinely overlap.
	ctx := context.Background()
	for i := 0; i < 400; i++ {
		path := fmt.Sprintf("db/m/2026/08/20/10/f%03d.parquet", i)
		if err := data.Write(ctx, path, []byte("parquet-bytes")); err != nil {
			t.Fatalf("seed file: %v", err)
		}
	}

	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		BackupPath:  t.TempDir(),
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		opCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := m.CreateBackup(opCtx, BackupOptions{}); err != nil {
			t.Errorf("backup failed: %v", err)
		}
	}()

	// Poll exactly the way production readers do: field reads plus a full JSON
	// marshal (GetStatus), for the whole life of the operation.
	for {
		select {
		case <-done:
			p := m.GetProgress()
			if p == nil || p.Status != "completed" {
				t.Fatalf("final progress = %+v, want completed", p)
			}
			if p.ProcessedFiles != 400 {
				t.Fatalf("processed = %d, want 400", p.ProcessedFiles)
			}
			return
		default:
		}
		if p := m.GetProgress(); p != nil {
			_ = p.Status
			_ = p.ProcessedFiles
			_ = p.ProcessedBytes
			if _, err := json.Marshal(p); err != nil {
				t.Fatalf("marshal progress: %v", err)
			}
		}
	}
}
