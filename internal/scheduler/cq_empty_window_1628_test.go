package scheduler

import (
	"bytes"
	"encoding/json"
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

			// Each line is one JSON event; match on fields, not key order.
			type event struct {
				Level, Message, Error string
				CQID                  int64 `json:"cq_id"`
			}
			var events []event
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				var e event
				if err := json.Unmarshal([]byte(line), &e); err != nil {
					t.Fatalf("log line %q: %v", line, err)
				}
				events = append(events, e)
			}
			has := func(level, msg string) bool {
				for _, e := range events {
					if e.Level == level && e.Message == msg && e.CQID == 1 {
						return true
					}
				}
				return false
			}
			failed := has("error", "Scheduled CQ execution failed")
			if c.quiet {
				if failed || has("info", "Scheduled CQ execution completed") || !has("debug", "Scheduled CQ: no new window") {
					t.Fatalf("logs:\n%s", buf.String())
				}
			} else {
				if !failed || has("debug", "Scheduled CQ: no new window") {
					t.Fatalf("want today's Error, logs:\n%s", buf.String())
				}
				for _, e := range events {
					if e.Level == "error" && e.Error != "start_time must be before end_time" {
						t.Fatalf("error %q, want today's text", e.Error)
					}
				}
			}
		})
	}
}
