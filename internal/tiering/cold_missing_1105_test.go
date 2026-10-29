package tiering

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1105: ColdTierMissing is exactly "cold on in config, no backend built".
func TestColdTierMissing(t *testing.T) {
	on := &config.TieredStorageConfig{}
	on.Cold.Enabled = true
	off := &config.TieredStorageConfig{}
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	var nilM *Manager
	for _, c := range []struct {
		name string
		m    *Manager
		want bool
	}{
		{"nil manager", nilM, false},
		{"no config", &Manager{}, false},
		{"cold off", &Manager{config: off}, false},
		{"cold on, backend built", &Manager{config: on, coldBackend: local}, false},
		{"cold on, no backend", &Manager{config: on}, true},
	} {
		if got := c.m.ColdTierMissing(); got != c.want {
			t.Errorf("%s: ColdTierMissing = %v, want %v", c.name, got, c.want)
		}
	}
}

// HasColdFilesInRange answers per measurement, ignores hot and quarantined
// rows, keeps nil bounds open, and widens the start by a day so a range that
// starts mid-day still sees that day's cold file (partition_time = midnight).
func TestHasColdFilesInRange(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	recordAt(t, store, "db1/cpu/2025/01/01/a_daily.parquet", "db1", TierCold) // 2025-01-01 00:00
	recordAt(t, store, "db1/cpu/2025/01/01/00/h.parquet", "db2", TierHot)
	recordAt(t, store, "db3/cpu/2025/01/01/q_daily.parquet", "db3", TierCold)
	if err := store.QuarantineFile(ctx, "db3/cpu/2025/01/01/q_daily.parquet", "x"); err != nil {
		t.Fatal(err)
	}
	m := &Manager{metadata: store}
	at := func(s string) *time.Time { v, _ := time.Parse(time.RFC3339, s); return &v }
	for _, c := range []struct {
		name       string
		db         string
		start, end *time.Time
		want       bool
	}{
		{"open range", "db1", nil, nil, true},
		{"range starting mid-day", "db1", at("2025-01-01T15:00:00Z"), at("2025-01-01T16:00:00Z"), true},
		{"range the next day", "db1", at("2025-01-02T01:00:00Z"), nil, false},
		{"range before", "db1", nil, at("2024-12-31T00:00:00Z"), false},
		{"hot rows only", "db2", nil, nil, false},
		{"quarantined cold row", "db3", nil, nil, false},
	} {
		got, err := m.HasColdFilesInRange(ctx, c.db, "cpu", c.start, c.end)
		if err != nil || got != c.want {
			t.Errorf("%s: %v, %v; want %v", c.name, got, err, c.want)
		}
	}
}
