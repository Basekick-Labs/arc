package tiering

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// MigrationBudget is what the scheduler and POST /api/v1/tiering/migrate bound
// a cycle with (#1112): the configured value, or the 2h default for a Manager
// with no config or a zero value, never an already-expired zero.
func TestMigrationBudget(t *testing.T) {
	var nilManager *Manager
	for _, tc := range []struct {
		name string
		m    *Manager
		want time.Duration
	}{
		{"nil manager", nilManager, 2 * time.Hour},
		{"nil config", &Manager{}, 2 * time.Hour},
		{"zero", &Manager{config: &config.TieredStorageConfig{}}, 2 * time.Hour},
		{"configured", &Manager{config: &config.TieredStorageConfig{MigrationTimeout: 45 * time.Minute}}, 45 * time.Minute},
	} {
		if got := tc.m.MigrationBudget(); got != tc.want {
			t.Errorf("%s: MigrationBudget() = %s, want %s", tc.name, got, tc.want)
		}
	}
}
