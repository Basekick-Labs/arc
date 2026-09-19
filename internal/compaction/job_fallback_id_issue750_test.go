package compaction

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestNewJobFallbackDatabasePathSafeIssue750(t *testing.T) {
	cases := []struct {
		name       string
		database   string
		wantPrefix string
	}{
		{
			name:       "spoke namespace",
			database:   "rocket-01/telemetry",
			wantPrefix: "rocket-01.telemetry_",
		},
		{
			name:       "spoke identifier with dot",
			database:   "rocket.01/telemetry",
			wantPrefix: "rocket.01.telemetry_",
		},
		{
			name:       "ordinary database",
			database:   "analytics",
			wantPrefix: "analytics_",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()

			job := NewJob(&JobConfig{
				Database:      tc.database,
				Measurement:   "cpu",
				PartitionPath: "cpu/2026/09/19/03",
				TempDirectory: tempDir,
				Logger:        zerolog.Nop(),
			})

			if err := validateJobID(job.JobID); err != nil {
				t.Fatalf(
					"generated JobID invalid for %q: %v",
					tc.database, err,
				)
			}

			if !strings.HasPrefix(job.JobID, tc.wantPrefix) {
				t.Errorf(
					"JobID = %q, want prefix %q",
					job.JobID, tc.wantPrefix,
				)
			}

			if job.Database != tc.database {
				t.Errorf(
					"database changed: got %q, want %q",
					job.Database, tc.database,
				)
			}

			if got := filepath.Dir(jobTempDir(tempDir, job.JobID)); got != tempDir {
				t.Errorf(
					"job escaped its immediate temp directory: %q",
					got,
				)
			}
		})
	}
}

func TestNewJobExplicitIDUnchangedIssue750(t *testing.T) {
	const supplied = "caller-provided-job-123"

	job := NewJob(&JobConfig{
		Database:      "rocket-01/telemetry",
		PartitionPath: "cpu/2026/09/19/03",
		JobID:         supplied,
		Logger:        zerolog.Nop(),
	})

	if job.JobID != supplied {
		t.Fatalf("explicit JobID changed: %q", job.JobID)
	}

	if job.Database != "rocket-01/telemetry" {
		t.Fatalf("database changed: %q", job.Database)
	}
}
