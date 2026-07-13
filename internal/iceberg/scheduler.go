package iceberg

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// WriterGate gates the reconciler to a single node in cluster mode. nil means "no gate,
// allow" (OSS/standalone — single node, always runs). Mirrors compaction's ClusterGate:
// CanRun is checked every tick (not once at Start) so failover takes effect without restart.
type WriterGate interface {
	// CanRun reports whether the local node may run the Iceberg reconcile pass.
	CanRun() bool
}

// Scheduler runs the Iceberg reconcile pass on a fixed interval. Each pass walks the current
// file set (via the FileSetSource) and reconciles every measurement's Iceberg table. It is
// safe to miss or fail a pass — the next pass re-derives the diff from durable state and
// converges (the reconciler is idempotent).
type Scheduler struct {
	exporter *Exporter
	source   FileSetSourceWithSchema
	interval time.Duration
	gate     WriterGate
	logger   zerolog.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

// FileSetSourceWithSchema is a FileSetSource that can also list the local files of a
// measurement for schema derivation. StorageWalkSource satisfies it.
type FileSetSourceWithSchema interface {
	FileSetSource
	LocalFiles(ctx context.Context, m Measurement) ([]string, error)
}

// SchedulerConfig configures the reconcile scheduler.
type SchedulerConfig struct {
	Exporter *Exporter
	Source   FileSetSourceWithSchema
	Interval time.Duration
	Gate     WriterGate // nil = always run (OSS)
	Logger   zerolog.Logger
}

// NewScheduler builds a reconcile scheduler.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	return &Scheduler{
		exporter: cfg.Exporter,
		source:   cfg.Source,
		interval: cfg.Interval,
		gate:     cfg.Gate,
		logger:   cfg.Logger.With().Str("component", "iceberg-scheduler").Logger(),
	}
}

// Start launches the reconcile loop in the background. Stop cancels it.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go s.loop(ctx)
	s.logger.Info().Dur("interval", s.interval).Msg("Iceberg reconcile scheduler started")
}

// Stop halts the loop and waits for the in-flight pass to finish.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	// Run one pass shortly after start so a fresh deployment publishes promptly, then on tick.
	s.runPass(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runPass(ctx)
		}
	}
}

// runPass reconciles every measurement once. Gated per-tick so cluster failover/demotion
// takes effect without a restart (nil gate = always run).
func (s *Scheduler) runPass(ctx context.Context) {
	if s.gate != nil && !s.gate.CanRun() {
		s.logger.Debug().Msg("Iceberg reconcile skipped: node is not the active writer")
		return
	}
	measurements, err := s.source.Measurements(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Iceberg reconcile: failed to enumerate measurements")
		return
	}
	var ok, failed int
	for _, m := range measurements {
		if err := s.reconcileOne(ctx, m); err != nil {
			// Log and continue — one bad measurement must not block the rest; next pass retries.
			s.logger.Error().Err(err).
				Str("database", m.Database).Str("measurement", m.Measurement).
				Msg("Iceberg reconcile: measurement failed")
			failed++
			continue
		}
		ok++
	}
	s.logger.Info().Int("ok", ok).Int("failed", failed).Msg("Iceberg reconcile pass complete")
}

func (s *Scheduler) reconcileOne(ctx context.Context, m Measurement) error {
	localFiles, err := s.source.LocalFiles(ctx, m)
	if err != nil {
		return err
	}
	if len(localFiles) == 0 {
		// No local files to derive schema from (cold-only measurement). Skip in v1 — documented.
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).
			Msg("Iceberg reconcile: no local file for schema derivation, skipping")
		return nil
	}
	// Union the schemas across all files — Arc's per-measurement schema can evolve (a metric
	// added later), so the Iceberg table must carry the superset or a wider file fails AddFiles.
	sc, err := UnionSchema(localFiles)
	if err != nil {
		return err
	}
	files, err := s.source.Files(ctx, m)
	if err != nil {
		return err
	}
	return s.exporter.ReconcileMeasurement(ctx, m.Database, m.Measurement, sc, files)
}
