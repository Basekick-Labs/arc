package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/rehome"
)

// With the Iceberg export on, a table may name the file a job is moving.
// The job keeps that file in storage, left out of Arc's reads, until the
// export has published a version of the table that does not name it (#1825):
//
//   - before the job changes anything for readers, it asks the export for a
//     pass of the table. An export that cannot publish the table now refuses
//     the job, while undoing it is free;
//   - once reads use the new files, it asks for a pass again and removes the
//     old file only when the published table does not name it. The export
//     commits the old file's removal and the new files together
//     (iceberg.Exporter.SetReplaced).
//
// The second wait has no time limit: the old file stays, and so does the
// job. An operator who would rather have the job end than the table right
// waives it (POST /api/v1/rehome/<id>/resume {"skip_export_wait": true}).

// RehomeExportAnswer is what the export said about a table.
type RehomeExportAnswer struct {
	// Clear: the published table does not name the file asked about (with
	// no file asked about: the export published the table just now), or the
	// measurement has no table and gets none.
	Clear bool
	// Why says what is in the way when Clear is false.
	Why string
}

// RehomeExportFunc runs one pass of the measurement's table, started after
// the call, and reports whether the published table names the file at
// fileKey ("" asks only that the pass publishes). An error means no answer
// came: the export did not get to the question before ctx ended.
type RehomeExportFunc func(ctx context.Context, database, measurement, fileKey string) (RehomeExportAnswer, error)

const (
	// rehomeExportAsk bounds one question to the export: longer than one
	// pass of a table may take.
	rehomeExportAsk = 5 * time.Minute
	// rehomeExportWarnEvery is how often a job waiting for the export says
	// so in the log.
	rehomeExportWarnEvery = 5 * time.Minute
)

// rehomeExportRetry is the first pause between questions; it doubles up to
// rehomeExportRetryMax. Variables so tests can shorten them.
var (
	rehomeExportRetry    = time.Second
	rehomeExportRetryMax = 30 * time.Second
	// rehomeExportReadyRetry is the pause before the question a job puts
	// before it starts is asked a second time.
	rehomeExportReadyRetry = 2 * time.Second
)

// SetExport binds the Iceberg export. Call it once the export's scheduler
// exists, and before Wired, which is what jobs found at start wait for.
func (h *RehomeHandler) SetExport(f RehomeExportFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.export != nil || f == nil {
		return
	}
	h.export = f
	close(h.exportBound)
}

func (h *RehomeHandler) exportExpected() bool {
	return h.swap != nil && h.swap.ExportExpected
}

func (h *RehomeHandler) exportFunc() RehomeExportFunc {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.export
}

// errExportRefused: the export cannot publish the table now, and the job was
// undone before it changed anything for readers.
type errExportRefused struct{ why string }

func (e errExportRefused) Error() string { return e.why }

// exportReady asks the export for a pass of the job's table before the job
// changes anything for readers. "" when the export published it.
func (h *RehomeHandler) exportReady(ctx context.Context, job *rehome.Job) string {
	export := h.exportFunc()
	if export == nil {
		return "the Iceberg export is enabled and is not running on this node"
	}
	if why := h.exportNotStarted(); why != "" {
		return why
	}
	// Asked twice before refusing: a pass can fail once for a reason that is
	// gone by the next one (a file another operation removed while the pass
	// read it, as a job that has just ended does with its old file).
	why := ""
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(rehomeExportReadyRetry):
			case <-ctx.Done():
				return "the server is stopping"
			}
		}
		actx, cancel := context.WithTimeout(ctx, rehomeExportAsk)
		ans, err := export(actx, job.Database, job.Measurement, "")
		cancel()
		switch {
		case ctx.Err() != nil:
			return "the server is stopping"
		case err != nil:
			why = fmt.Sprintf("the Iceberg export did not answer within %s", rehomeExportAsk)
		case !ans.Clear:
			why = ans.Why
		default:
			return ""
		}
	}
	return why
}

// waitExport returns once the published table does not name the job's old
// file, or the wait was waived after the export answered at least once. It
// ends early only with ctx.
func (h *RehomeHandler) waitExport(ctx context.Context, job *rehome.Job) error {
	// A waiver's wake-up left from an earlier job is not this one's.
	select {
	case <-h.skipCh:
	default:
	}
	h.mu.Lock()
	h.waitingExport, h.skipExport = job.ID, false
	if j, ok := h.jobs[job.ID]; ok {
		j.WaitingFor = "the Iceberg export, to publish the table without " + job.Source.Key
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.waitingExport, h.skipExport = "", false
		if j, ok := h.jobs[job.ID]; ok {
			j.WaitingFor = ""
		}
		h.mu.Unlock()
	}()
	log := h.logger.With().Str("job", job.ID).Str("path", job.Source.Key).
		Str("table", job.Database+"."+job.Measurement).Logger()

	// The export is bound later than recovery runs.
	select {
	case <-h.exportBound:
	case <-ctx.Done():
		return ctx.Err()
	}
	export := h.exportFunc()
	var (
		answered bool
		warned   time.Time
		pause    = rehomeExportRetry
	)
	for {
		var ans RehomeExportAnswer
		var err error
		if why := h.exportNotStarted(); why != "" {
			// Nothing runs passes: that is an answer, and no listing of
			// the measurement is in flight.
			ans = RehomeExportAnswer{Why: why}
		} else {
			actx, cancel := context.WithTimeout(ctx, rehomeExportAsk)
			ans, err = export(actx, job.Database, job.Measurement, job.Source.Key)
			cancel()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		why := ans.Why
		if err != nil {
			why = fmt.Sprintf("the export did not answer within %s", rehomeExportAsk)
		} else {
			answered = true
			if ans.Clear {
				return nil
			}
		}
		h.mu.Lock()
		skip := h.skipExport
		h.mu.Unlock()
		if skip && answered {
			// answered: a pass that began before reads were switched has
			// ended, or none runs, so no listing of the measurement is in
			// flight that expects the old file.
			log.Warn().Str("export", why).Msg("Re-home: removing the old file without waiting for the Iceberg export, as asked. Until the export next commits this table, it names a file that is gone, and readers of the table fail on it. That commit is no longer tied to this job: if it stops part way, the table can be published for a while without some of the file's rows")
			return nil
		}
		if time.Since(warned) >= rehomeExportWarnEvery {
			warned = time.Now()
			log.Warn().Str("export", why).Msg("Re-home: waiting for the Iceberg export to publish the table without the old file. The old file is kept and left out of Arc's queries; the job, and with it backup, restore, a delete of this database and other re-home jobs, waits. POST /api/v1/rehome/<id>/resume {\"skip_export_wait\": true} ends the wait")
		}
		select {
		case <-time.After(pause):
		case <-h.skipCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		if pause *= 2; pause > rehomeExportRetryMax {
			pause = rehomeExportRetryMax
		}
	}
}

// waiveExportWait ends the export wait of job id, if that job is in it.
func (h *RehomeHandler) waiveExportWait(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.waitingExport != id || id == "" {
		return false
	}
	h.skipExport = true
	select {
	case h.skipCh <- struct{}{}:
	default:
	}
	return true
}

// recoverLater takes up, once startup has bound the Iceberg export and
// tiering or found them absent (Wired), the jobs found at start that need
// them: recovery runs before either exists, and a job past its commit must
// not remove its old file, or send reads to new ones, without asking.
func (h *RehomeHandler) recoverLater(jobs []*rehome.Job) {
	h.mu.Lock()
	h.running = true
	for _, rec := range jobs {
		if _, known := h.jobs[rec.ID]; !known {
			h.jobs[rec.ID] = &rehomeJob{ID: rec.ID, Path: rec.Source.Key, Tier: rec.Source.Tier, State: "running", StartedAt: rec.CreatedAt}
			h.order = append(h.order, rec.ID)
		}
	}
	h.mu.Unlock()
	h.mu.Lock()
	for _, rec := range jobs {
		if j := h.jobs[rec.ID]; j != nil {
			j.WaitingFor = "the server, to finish starting"
		}
	}
	h.mu.Unlock()
	h.done.Add(1)
	go func() {
		defer h.done.Done()
		defer func() {
			h.mu.Lock()
			h.running = false
			h.mu.Unlock()
		}()
		// Nothing is done to a job before the export and tiering exist or
		// are known absent: not its moves, not its latch. The rest of the
		// process is still being wired.
		select {
		case <-h.wired:
		case <-h.ctx.Done():
			return
		}
		for _, rec := range jobs {
			h.mu.Lock()
			if j := h.jobs[rec.ID]; j != nil {
				j.WaitingFor = ""
			}
			h.mu.Unlock()
			h.mu.Lock()
			job := h.jobs[rec.ID]
			h.mu.Unlock()
			log := h.logger.With().Str("job", rec.ID).Str("path", rec.Source.Key).Str("state", string(rec.State)).Logger()
			state, reason, _ := h.runGuarded(h.ctx, job, func(ctx context.Context, _ *rehomeJob) (string, string, *rehomeReport) {
				// A latched job is finished whatever tiering holds of its
				// old file: reads already use the new files. Say what that
				// leaves. (A committed job is asked about in recoverJob.)
				if rec.State == rehome.StateLatched && !rec.Source.Cold() {
					if cerr, known := h.tieringSource(ctx, rec.Source.Key); cerr != nil && known {
						log.Error().Str("why", cerr.Error()).Msg("Re-home: a job found at start had switched reads to its new files, and tiered storage now holds its old file or a copy of it. The job is finished; the copy in cold storage holds the same rows as the new files and must be removed by hand, or they are read twice")
					} else if cerr != nil {
						log.Warn().Str("why", cerr.Error()).Msg("Re-home: a job found at start had switched reads to its new files, and tiered storage could not be asked about its old file. The job is finished; if cold storage holds a copy of that file, its rows are read twice until the copy is removed")
					}
				}
				err := h.resume(ctx, rec.ID)
				switch {
				case err == nil:
					log.Info().Msg("Re-home: a job found at start was taken up")
					return "done", "", nil
				case errors.Is(err, context.Canceled):
					return "stopped", "the server stopped while the job waited; its record is kept and the next start takes it up", nil
				}
				var kept errJobKept
				if errors.As(err, &kept) {
					log.Warn().Str("why", kept.why).Msg("Re-home: a job found at start is kept as recorded, because tiered storage could not be asked about its file. POST /api/v1/rehome/<id>/resume takes it up once it can")
					return "stopped", kept.Error(), nil
				}
				if why, refused := rehome.IsRefused(err); refused {
					log.Warn().Str("why", why).Msg("Re-home: a job found at start was undone, because of what tiered storage holds of its file now; the file is as it was")
					return "refused", why, nil
				}
				log.Error().Err(err).Msg("Re-home: a job found at start could not be taken up. It is kept as recorded: its files stay out of compaction, DELETE and retention. The error says which file is not as the job recorded it; until that is put right by hand, reads may be missing that file's rows")
				return "stopped", "the job could not be taken up and its record is kept; the server log has the error under this job's id", nil
			})
			now := time.Now().UTC()
			h.mu.Lock()
			job.State, job.Reason, job.FinishedAt = state, reason, &now
			h.mu.Unlock()
		}
	}()
}
