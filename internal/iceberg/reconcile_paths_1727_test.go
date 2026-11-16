package iceberg

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1727: other Arc paths report the storage keys they changed; the scheduler
// maps them onto the measurements it knows.

func names(ms []Measurement) string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Database+"|"+m.Measurement)
	}
	return strings.Join(out, ",")
}

func TestMeasurementsForMapsKeysAtSegmentBoundaries(t *testing.T) {
	s := NewScheduler(SchedulerConfig{Logger: zerolog.Nop()})
	s.known = []Measurement{
		{"db", "cpu"}, {"db", "cpu2"}, {"wh", "m"}, {"wh-other", "m"},
		{"spokeA/db", "cpu"}, {"spokeA/db", "mem"},
	}
	for _, tc := range []struct {
		keys []string
		want string
	}{
		{[]string{"db/cpu/2026/11/16/14/f.parquet"}, "db|cpu"},
		{[]string{"db/cpu2/2026/11/16/14/f.parquet"}, "db|cpu2"},
		{[]string{"db/cpu/"}, "db|cpu"},
		{[]string{"db/cpu"}, "db|cpu"},
		{[]string{"db/"}, "db|cpu,db|cpu2"},
		{[]string{"wh/"}, "wh|m"},
		// A spoke's data, named the way DELETE and retention name it: the
		// whole "{spoke}/{db}" directory, which the export holds as one
		// database per spoke database. Never a ("spokeA", "db") table.
		{[]string{"spokeA/db/"}, "spokeA/db|cpu,spokeA/db|mem"},
		{[]string{"spokeA/db/cpu/2026/11/16/14/f.parquet"}, "spokeA/db|cpu"},
		{[]string{"other/x/f.parquet", "db/nope/f.parquet"}, ""},
		{[]string{"db/cpu/a.parquet", "db/cpu/b.parquet"}, "db|cpu"},
	} {
		if got := names(s.measurementsFor(tc.keys)); got != tc.want {
			t.Errorf("measurementsFor(%v) = %q, want %q", tc.keys, got, tc.want)
		}
	}
	if got := s.measurementsFor([]string{"db/cpu/"}); len(got) != 1 || got[0].Database != "db" {
		t.Fatalf("got %v", got)
	}
	// With only cpu known, a file of cpu2 is not cpu's (segment boundary).
	only := NewScheduler(SchedulerConfig{Logger: zerolog.Nop()})
	only.known = []Measurement{{"db", "cpu"}}
	if got := only.measurementsFor([]string{"db/cpu2/2026/11/16/14/f.parquet"}); len(got) != 0 {
		t.Fatalf("db/cpu2's file mapped to %v, want nothing", got)
	}
	empty := NewScheduler(SchedulerConfig{Logger: zerolog.Nop()})
	if got := empty.measurementsFor([]string{"db/cpu/"}); len(got) != 0 {
		t.Fatalf("before any pass: %v, want nothing (the timer covers it)", got)
	}
}

// coldRecorder notes the cold invalidations a request makes, in order with
// the listings.
type coldRecorder struct {
	*StorageWalkSource
	events []string
}

func (c *coldRecorder) InvalidateCold(database string) {
	c.events = append(c.events, "invalidate "+database)
}

func (c *coldRecorder) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	c.events = append(c.events, "list "+m.Database+"/"+m.Measurement)
	return c.StorageWalkSource.FilesAndLocal(ctx, m)
}

// A path request reconciles the measurement holding the path, with the cold
// listing dropped first so a cold removal is seen (H2), and nothing for a
// key under no known measurement.
func TestRequestReconcilePathsServesTheMeasurementAfterDroppingColdListing(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.write(t, winIn2, 30)
	r.sched.runPass(ctx)
	src := &coldRecorder{StorageWalkSource: r.src}
	r.sched.source = src
	if err := r.be.Delete(ctx, winIn1); err != nil {
		t.Fatal(err)
	}
	r.sched.RequestReconcilePaths([]string{winIn1, "elsewhere/x/f.parquet"})
	r.sched.serviceRequests(ctx)
	if got := strings.Join(src.events, ","); got != "invalidate mydb,list mydb/cpu" {
		t.Fatalf("events %s, want the cold listing dropped before the reconcile", got)
	}
	if got := r.live(t); strings.Join(got, ",") != filepath.Base(winIn2) {
		t.Fatalf("live = %v, want the remaining file only", got)
	}
}
