package scheduler

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #1628: a scheduled run with no new window (the watermark at most a minute
// ahead) is logged at Debug, not as a failure; a watermark further ahead is
// still an Error.
func TestCQSchedulerEmptyWindow(t *testing.T) {
	for _, c := range []struct {
		name  string
		ahead time.Duration
		quiet bool
	}{
		{"within the skew", 30 * time.Second, true},
		{"beyond the skew", 5 * time.Minute, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "arc.db")
			am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { am.Close() })
			h, err := api.NewContinuousQueryHandler(nil, nil, nil, &config.ContinuousQueryConfig{Enabled: true, DBPath: dbPath}, am, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			w := time.Now().UTC().Truncate(time.Second).Add(c.ahead)
			if _, err := am.GetDB().Exec(`INSERT INTO continuous_queries
				(name, database, source_measurement, destination_measurement, query, interval, is_active, last_processed_time)
				VALUES ('q', 'db', 'cpu', 'cpu_1h', 'SELECT 1 WHERE {start_time} < {end_time}', '1h', 1, ?)`, w.Format(time.RFC3339)); err != nil {
				t.Fatal(err)
			}

			var buf bytes.Buffer
			s, _ := NewCQScheduler(&CQSchedulerConfig{CQHandler: h, Logger: zerolog.New(&buf).Level(zerolog.DebugLevel)})
			s.executeJob(&cqJob{cqID: 1, cqName: "q", stopCh: make(chan struct{})})

			logs := buf.String()
			failed := strings.Contains(logs, `"level":"error"`) && strings.Contains(logs, "Scheduled CQ execution failed")
			if c.quiet {
				if failed || strings.Contains(logs, "Scheduled CQ execution completed") ||
					!strings.Contains(logs, `"level":"debug","component":"cq-scheduler","cq_id":1,"cq_name":"q","message":"Scheduled CQ: no new window"`) {
					t.Fatalf("logs:\n%s", logs)
				}
			} else if !failed || !strings.Contains(logs, "start_time must be before end_time") {
				t.Fatalf("want today's Error, logs:\n%s", logs)
			}
		})
	}
}
