package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Tests for #977: a backup names the files it skipped (manifest and status
// skipped_sample), attributes them to a cause (skipped_overlong_keys, the
// skip-ratio message), carries the counts into the listing, and reports the
// total on the arc_backup_skipped_files gauge.

// skipInventory lists extra keys ahead of the real backend's objects. A key in
// readable is served from memory (an overlong key the filesystem could not
// hold); any other extra key fails to read, like a file compaction removed
// between the listing and the copy.
type skipInventory struct {
	storage.Backend
	extra    []string
	readable map[string]string
}

func (s skipInventory) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := s.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var out []storage.ObjectInfo
	for _, k := range s.extra {
		out = append(out, storage.ObjectInfo{Path: k, Size: 4})
	}
	return append(out, objects...), nil
}

func (s skipInventory) ReadTo(ctx context.Context, path string, dst io.Writer) error {
	if content, ok := s.readable[path]; ok {
		_, err := io.WriteString(dst, content)
		return err
	}
	for _, k := range s.extra {
		if k == path {
			return errors.New("object vanished")
		}
	}
	return s.Backend.ReadTo(ctx, path, dst)
}

// overlongSourceKey builds a legal source key whose backup destination exceeds
// the storage key limit by one byte, same construction as key_length_test.
func overlongSourceKey(t *testing.T) string {
	t.Helper()
	sourceLimit := storage.MaxUsableKeyLen - backupDataKeyHeadroom
	prefix := "db/cpu/" + strings.Repeat(strings.Repeat("a", 200)+"/", 4)
	tailLen := sourceLimit + 1 - len(prefix) - len(".parquet")
	key := prefix + strings.Repeat("z", tailLen) + ".parquet"
	if err := storage.ValidateKey(key); err != nil {
		t.Fatalf("source key must be legal: %v", err)
	}
	return key
}

func writeGoodFiles(t *testing.T, ctx context.Context, b storage.Backend, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := b.Write(ctx, fmt.Sprintf("db/cpu/2026/09/17/00/good-%02d.parquet", i), []byte("PAR1-good")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBackupSkippedSampleNamesBothCauses(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// 20 ordinary files keep two skips under maxSkipRatio (2 > 2.2 is false).
	writeGoodFiles(t, ctx, dataStorage, 20)
	longKey := overlongSourceKey(t)
	const ghost = "db/cpu/2026/09/17/00/compacted-away.parquet"
	backupPath := t.TempDir()

	manager, err := NewManager(&ManagerConfig{
		DataStorage: skipInventory{Backend: dataStorage, extra: []string{longKey, ghost}, readable: map[string]string{longKey: "PAR1"}},
		BackupPath:  backupPath,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("two skips in 22 files must not fail the backup: %v", err)
	}
	m := result.Manifest
	if m.TotalFiles != 22 || m.SkippedFiles != 2 || m.SkippedOverlongKeys != 1 {
		t.Errorf("manifest = total %d, skipped %d, overlong %d; want 22, 2, 1", m.TotalFiles, m.SkippedFiles, m.SkippedOverlongKeys)
	}
	if want := []string{longKey, ghost}; strings.Join(m.SkippedSample, ",") != strings.Join(want, ",") {
		t.Errorf("manifest skipped_sample = %v, want %v (copy order)", m.SkippedSample, want)
	}
	if p := manager.GetProgress(); p == nil || strings.Join(p.SkippedSample, ",") != longKey+","+ghost {
		t.Errorf("status skipped_sample = %v, want both keys", p)
	}
	summaries, err := manager.ListBackups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].SkippedFiles != 2 || summaries[0].TotalFiles != 22 {
		t.Errorf("listing = %+v, want total 22, skipped 2", summaries)
	}
	if got, _ := metrics.Get().Snapshot()["backup_skipped_files"].(int64); got != 2 {
		t.Errorf("gauge after the incomplete backup = %d, want 2", got)
	}

	// The next clean run over the same store clears everything.
	clean, err := NewManager(&ManagerConfig{DataStorage: dataStorage, BackupPath: backupPath, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	result, err = clean.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.SkippedSample != nil || result.Manifest.SkippedFiles != 0 || result.Manifest.SkippedOverlongKeys != 0 {
		t.Errorf("clean manifest carries skips: %+v", result.Manifest)
	}
	if got, _ := metrics.Get().Snapshot()["backup_skipped_files"].(int64); got != 0 {
		t.Errorf("gauge after a clean backup = %d, want 0", got)
	}
	data, err := json.Marshal(SummaryFromManifest(result.Manifest))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"skipped_files", "skipped_metadata_files", "unaddressable_files"} {
		if strings.Contains(string(data), key) {
			t.Errorf("clean listing entry carries %q: %s", key, data)
		}
	}
}

// A run the skip ratio fails writes no manifest, so the status endpoint and
// the gauge are where the operator learns which files and how many.
func TestBackupRatioFailureNamesCauseAndKeepsStatusSample(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeGoodFiles(t, ctx, dataStorage, 20)
	var ghosts []string
	for i := 0; i < 5; i++ {
		ghosts = append(ghosts, fmt.Sprintf("db/cpu/2026/09/17/00/gone-%d.parquet", i))
	}
	manager, err := NewManager(&ManagerConfig{
		DataStorage: skipInventory{Backend: dataStorage, extra: ghosts},
		BackupPath:  t.TempDir(),
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("5 of 25 files skipped must fail the backup")
	}
	if !strings.Contains(err.Error(), "5 could not be read at copy time") || strings.Contains(err.Error(), "source keys longer") {
		t.Errorf("message does not name the one cause with its count: %v", err)
	}
	p := manager.GetProgress()
	if p == nil || p.Status != "failed" || strings.Join(p.SkippedSample, ",") != strings.Join(ghosts, ",") {
		t.Errorf("status after the failed run = %+v, want failed with the five ghosts in skipped_sample", p)
	}
	if got, _ := metrics.Get().Snapshot()["backup_skipped_files"].(int64); got != 5 {
		t.Errorf("gauge after the failed run = %d, want 5 (set before the ratio check)", got)
	}
}

func TestSkipTally_SampleIsBounded(t *testing.T) {
	var tally skipTally
	for i := 0; i < 40; i++ {
		tally.record(fmt.Sprintf("k%d", i), i%2 == 0)
	}
	if len(tally.sample) != unaddressableSampleCap {
		t.Errorf("sample length = %d, want %d", len(tally.sample), unaddressableSampleCap)
	}
	if tally.overlong != 20 {
		t.Errorf("overlong = %d, want 20", tally.overlong)
	}
	if tally.sample[0] != "k0" || tally.sample[31] != "k31" {
		t.Errorf("sample keeps the first %d in order: %v", unaddressableSampleCap, tally.sample)
	}
	var none *skipTally
	none.record("x", true) // must not panic
}

func TestCheckSkipRatio_MessageNamesCauses(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	threshold := fmt.Sprintf("longer than %d bytes", storage.MaxUsableKeyLen-backupDataKeyHeadroom)

	err := m.checkSkipRatio(&Progress{SkippedFiles: 5}, 10, &skipTally{overlong: 3})
	if err == nil || !strings.Contains(err.Error(), "2 could not be read at copy time") || !strings.Contains(err.Error(), "3 have source keys "+threshold) {
		t.Errorf("mixed causes: %v", err)
	}
	err = m.checkSkipRatio(&Progress{SkippedFiles: 5}, 10, &skipTally{overlong: 5})
	if err == nil || strings.Contains(err.Error(), "could not be read") || !strings.Contains(err.Error(), "5 have source keys "+threshold) {
		t.Errorf("overlong only: %v", err)
	}
	err = m.checkSkipRatio(&Progress{SkippedFiles: 5}, 10, nil)
	if err == nil || !strings.Contains(err.Error(), "5 could not be read at copy time") || strings.Contains(err.Error(), "source keys") {
		t.Errorf("unreadable only (nil tally): %v", err)
	}
	if err := m.checkSkipRatio(&Progress{SkippedFiles: 1}, 20, &skipTally{overlong: 1}); err != nil {
		t.Errorf("one skip in twenty is tolerated: %v", err)
	}
}

// Uses only symbols that exist before #977, so it compiles against the old
// summary and fails there: the listing must carry the manifest's counts.
func TestSummaryFromManifest_CarriesIncompletenessCounts(t *testing.T) {
	data, err := json.Marshal(SummaryFromManifest(&Manifest{BackupID: "b", TotalFiles: 21, SkippedFiles: 3, SkippedMetadataFiles: 1, UnaddressableFiles: 2}))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]float64{"total_files": 21, "skipped_files": 3, "skipped_metadata_files": 1, "unaddressable_files": 2} {
		if got[key] != want {
			t.Errorf("listing %s = %v, want %v (json: %s)", key, got[key], want, data)
		}
	}
}
