package compaction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Simulate a child paused at the post-upload/pre-input-deletion boundary.
// Use the real parent subprocess runner and real local manifest storage.
func init() {
	marker := os.Getenv("ARC_COMPACTION_TEST_POST_UPLOAD_MARKER")
	if marker == "" || len(os.Args) < 2 || os.Args[1] != "compact" {
		return
	}
	var cfg SubprocessJobConfig
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		os.Exit(90)
	}
	b, err := createStorageBackendFromConfig(&cfg, zerolog.Nop())
	if err != nil {
		os.Exit(91)
	}
	ctx := context.Background()
	output := cfg.PartitionPath + "/test_compacted.parquet"
	// Fixture contents model two original rows, not a DuckDB-produced file.
	if err = b.Write(ctx, output, []byte("row1\nrow2\n")); err != nil {
		os.Exit(92)
	}
	mm := NewManifestManager(b, zerolog.Nop())
	_, err = mm.WriteManifest(ctx, &Manifest{OutputPath: output, OutputSize: 10, InputFiles: cfg.Files, Database: cfg.Database, Measurement: cfg.Measurement, PartitionPath: cfg.PartitionPath, Tier: cfg.Tier, Status: ManifestStatusPending, CreatedAt: time.Now(), JobID: cfg.JobID})
	if err != nil {
		os.Exit(93)
	}
	if err = os.WriteFile(marker, []byte("ready"), 0600); err != nil {
		os.Exit(94)
	}
	time.Sleep(time.Hour)
	os.Exit(95)
}

func TestCycleSubprocessCancellationAndRecovery(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	var logs bytes.Buffer
	m.logger = zerolog.New(&logs)
	marker := filepath.Join(t.TempDir(), "ready")
	t.Setenv("ARC_COMPACTION_TEST_POST_UPLOAD_MARKER", marker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	part := "db/cpu/2026/01/01/00"
	files := []string{part + "/a.parquet", part + "/b.parquet"}
	for i, p := range files {
		if err := b.Write(ctx, p, []byte([]string{"row1\n", "row2\n"}[i])); err != nil {
			t.Fatal(err)
		}
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		return []Candidate{{Database: "db", Measurement: "cpu", PartitionPath: part, Tier: "hourly", Files: files, FileCount: 2}}, nil
	}}}
	done := make(chan error, 1)
	go func() {
		_, err := m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
		done <- err
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
ready:
	for {
		select {
		case err := <-done:
			t.Fatalf("child ended before upload boundary: %v; logs=%s", err, logs.String())
		case <-timer.C:
			t.Fatal("child did not reach upload boundary")
		case <-ticker.C:
			if _, err := os.Stat(marker); err == nil {
				break ready
			}
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parent failed to join child")
	}
	outcome := cycleOutcomeIssue915(t, m)
	if outcome["interrupted_batches"] != int64(1) || outcome["failed_batches"] != int64(0) {
		t.Fatalf("wrong cancellation accounting: %v", outcome)
	}
	if m.Stats()["total_jobs_interrupted"] != 1 {
		t.Fatalf("wrong job stats: %v", m.Stats())
	}
	for _, message := range []string{"Compaction failed at minimum batch size", "Splitting batch after recoverable failure"} {
		if strings.Contains(logs.String(), message) {
			t.Errorf("cancellation emitted failure/retry log: %s", message)
		}
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, nil }}}
	if _, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	for _, p := range files {
		exists, err := b.Exists(context.Background(), p)
		if err != nil || exists {
			t.Fatalf("input not recovered: %s exists=%v err=%v", p, exists, err)
		}
	}
	out, err := b.Read(context.Background(), part+"/test_compacted.parquet")
	if err != nil || string(out) != "row1\nrow2\n" {
		t.Fatalf("output lost during recovery: %q %v", out, err)
	}
	pending, err := m.ManifestManager.ListManifests(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("manifest recovery incomplete: %v %v", pending, err)
	}
}
