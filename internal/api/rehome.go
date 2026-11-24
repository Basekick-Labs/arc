package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
)

// Re-homing a misfiled file (#1825): the dry run.
//
// A file is misfiled when it holds rows whose time is outside the hour or day
// of the directory it sits in (internal/rehome). This handler reads such a
// file, splits its rows into one file per hour or day in a scratch directory,
// checks the result and reports where the rows belong. It changes nothing in
// storage: the split's files are removed when the report is written.
//
// Moving the rows for real is not in this version. A request without
// dry_run is refused with that reason.

const (
	// rehomeJobTimeout bounds one job: the copy of the source to local disk,
	// the read of its times and the split.
	rehomeJobTimeout = 30 * time.Minute
	// rehomeMemoryLimit is the private DuckDB instance's memory_limit. The
	// instance is single-threaded and reads one file.
	rehomeMemoryLimit = 1 << 30
	// rehomeJobsKept bounds the finished reports kept in memory.
	rehomeJobsKept = 50
	// rehomeDestinationsListed bounds the destinations one report lists; the
	// report says how many there are.
	rehomeDestinationsListed = 5000
	// rehomeScratchPrefix names a job's scratch directory under the handler's
	// scratch root; leftovers of a stopped process are removed at start.
	rehomeScratchPrefix = "rehome-"
)

// RehomeHandler serves /api/v1/rehome.
type RehomeHandler struct {
	storage     storage.Backend
	authManager *auth.AuthManager
	logger      zerolog.Logger
	// scratchRoot is where a job copies and splits a file: the DELETE
	// handler's staging directory, on the data volume, not the system's
	// temporary directory (a small tmpfs in many containers).
	scratchRoot string
	// stop ends the running job when the server shuts down.
	ctx  context.Context
	stop context.CancelFunc
	done sync.WaitGroup

	mu      sync.Mutex
	jobs    map[string]*rehomeJob
	order   []string // job ids, oldest first
	running bool
	// run does a job's work and returns how it ended; a test seam.
	run func(ctx context.Context, job *rehomeJob) (state, reason string, report *rehomeReport)
	// swap is what a real swap needs (SetSwap); nil: dry runs only.
	swap *RehomeSwap
	// afterStep is a test seam: called after each step of a swap.
	afterStep func(step string)

	// export asks the Iceberg export about a table (SetExport); exportBound
	// is closed once it is set. waitingExport is the id of the job waiting
	// for the export, skipExport that its wait was waived, skipCh wakes it.
	export        RehomeExportFunc
	exportBound   chan struct{}
	waitingExport string
	skipExport    bool
	skipCh        chan struct{}
	// exportDown is why the export is bound and will not run ("" when it
	// runs): SetExportNotStarted.
	exportDown string

	// tiering is the tiering manager (SetTiering), nil when none runs.
	// wired is closed by Wired, once everything a job found at start may
	// need has been bound or found absent.
	tiering   RehomeTiering
	wired     chan struct{}
	wiredOnce sync.Once

	// cluster is the cluster driver's memory (rehome_cluster.go).
	cluster rehomeClusterState
	// localLocks orders a job's steps on this node on local storage (#1860).
	localLocks rehomeLocalLocks
}

// rehomeJob is one request and its report.
type rehomeJob struct {
	ID         string     `json:"id"`
	Path       string     `json:"path"`
	DryRun     bool       `json:"dry_run"`
	State      string     `json:"state"` // running, done, refused, failed
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Tier is "cold" for a job whose file is in cold storage.
	Tier string `json:"tier,omitempty"`
	// Reason: why a job was refused or failed.
	Reason string `json:"reason,omitempty"`
	// WaitingFor: what a running job is waiting on, when it is not Arc's
	// own work: the Iceberg export.
	WaitingFor string        `json:"waiting_for,omitempty"`
	Report     *rehomeReport `json:"report,omitempty"`
}

// rehomeReport is what a dry run found.
type rehomeReport struct {
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	// Shape: "hour" for a file in an hour directory, split per UTC hour;
	// "day" for a file in a day directory, split per UTC day.
	Shape     string `json:"shape"`
	Directory string `json:"directory"`
	TimeType  string `json:"time_type"`
	Rows      int64  `json:"rows"`
	Bytes     int64  `json:"bytes"`
	// Misfiled: the file holds rows that belong in another directory.
	Misfiled     bool  `json:"misfiled"`
	MisfiledRows int64 `json:"misfiled_rows"`
	// RemainderRows have a NULL or infinite time, and so no directory to
	// belong to; a split keeps them in the file's own directory.
	RemainderRows int64 `json:"remainder_rows"`
	// Destinations are the hours or days that hold rows of the file, up to
	// rehomeDestinationsListed of them; DestinationsTotal is how many there
	// are.
	Destinations      []rehome.Destination `json:"destinations"`
	DestinationsTotal int                  `json:"destinations_total"`
	// SplitChecked: the file was split in a scratch directory and the split
	// passed its checks (every row once, every output inside its
	// directory). False with SplitSkipped saying why when it was not tried.
	SplitChecked bool   `json:"split_checked"`
	SplitSkipped string `json:"split_skipped,omitempty"`
	// Outputs are the files the split wrote, with their rows, sizes and time
	// ranges. Left out of the list of jobs.
	Outputs []rehome.Output `json:"outputs,omitempty"`
}

// NewRehomeHandler creates the handler. authManager may be nil (auth off).
// scratchRoot is a directory on local disk the handler may write to; empty
// uses the system's temporary directory.
func NewRehomeHandler(backend storage.Backend, authManager *auth.AuthManager, scratchRoot string, logger zerolog.Logger) *RehomeHandler {
	if scratchRoot == "" {
		scratchRoot = os.TempDir()
	}
	ctx, stop := context.WithCancel(context.Background())
	h := &RehomeHandler{
		storage:     backend,
		authManager: authManager,
		logger:      logger.With().Str("component", "rehome").Logger(),
		scratchRoot: scratchRoot,
		ctx:         ctx,
		stop:        stop,
		jobs:        map[string]*rehomeJob{},
		exportBound: make(chan struct{}),
		wired:       make(chan struct{}),
		skipCh:      make(chan struct{}, 1),
	}
	h.run = h.dryRun
	h.removeScratch()
	return h
}

// removeScratch removes the scratch directories a stopped process left: a
// job killed mid-way leaves a full copy of the file it was reading.
func (h *RehomeHandler) removeScratch() {
	entries, err := os.ReadDir(h.scratchRoot)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), rehomeScratchPrefix) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(h.scratchRoot, e.Name())); err != nil {
			h.logger.Warn().Err(err).Str("dir", e.Name()).Msg("Could not remove a re-home scratch directory left by a stopped process")
		}
	}
}

// Shutdown ends the running job and waits for it to remove its scratch
// directory, or for ctx.
func (h *RehomeHandler) Shutdown(ctx context.Context) error {
	h.stop()
	ended := make(chan struct{})
	go func() { h.done.Wait(); close(ended) }()
	select {
	case <-ended:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterRoutes mounts the routes. Admin only: a job reads a whole file and
// writes as much again to local disk.
func (h *RehomeHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/rehome")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}
	group.Post("/", h.start)
	group.Get("/", h.list)
	group.Get("/:id", h.get)
	group.Post("/:id/resume", h.resumeJob)
}

type rehomeRequest struct {
	Path   string `json:"path"`
	DryRun bool   `json:"dry_run"`
}

// start handles POST /api/v1/rehome.
func (h *RehomeHandler) start(c *fiber.Ctx) error {
	var req rehomeRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "the body must be JSON: {\"path\": \"...\", \"dry_run\": true}"})
	}
	if !req.DryRun {
		if why := h.swapRefusal(); why != "" {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": why})
		}
	}
	src, err := rehome.ParseKey(req.Path)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()
	size, err := h.storage.StatFile(ctx, src.Key)
	if err != nil && !storage.IsNotFound(err) {
		h.logger.Error().Err(err).Str("path", src.Key).Msg("Re-home: could not look the file up")
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "could not look the file up in storage"})
	}
	tier := ""
	if err != nil || size < 0 {
		// Not on the hot tier: in cold storage, if this node has any.
		inCold, cerr := h.inColdStorage(ctx, src.Key)
		if cerr != nil {
			h.logger.Error().Err(cerr).Str("path", src.Key).Msg("Re-home: could not look the file up in cold storage")
			return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "the file is not on the hot tier, and cold storage could not be asked whether it holds it"})
		}
		if !inCold {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": fmt.Sprintf("%s is not in this node's storage, hot or cold", src.Key)})
		}
		tier = rehome.TierCold
	}

	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "another re-home job is running; one runs at a time"})
	}
	id, err := newRehomeID()
	if err != nil {
		h.mu.Unlock()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not create a job id"})
	}
	job := &rehomeJob{ID: id, Path: src.Key, DryRun: req.DryRun, Tier: tier, State: "running", StartedAt: time.Now().UTC()}
	h.jobs[id] = job
	h.order = append(h.order, id)
	for len(h.order) > rehomeJobsKept {
		delete(h.jobs, h.order[0])
		h.order = h.order[1:]
	}
	h.running = true
	h.mu.Unlock()

	h.done.Add(1)
	go func() {
		defer h.done.Done()
		// Not the request's context: the job outlives the request. The
		// handler's: a shutdown ends it.
		// A dry run is bounded. A swap is not: once its files are moving
		// it must be able to wait for the reads in flight, however long.
		jctx, jcancel := context.WithCancel(h.ctx)
		if job.DryRun {
			jctx, jcancel = context.WithTimeout(h.ctx, rehomeJobTimeout)
		}
		defer jcancel()
		run := h.run
		if !job.DryRun {
			run = h.runSwap
		}
		state, reason, report := h.runGuarded(jctx, job, run)
		// The job is finished and the slot freed together, after the job's
		// own cleanup: a client that sees it done can start the next.
		now := time.Now().UTC()
		h.mu.Lock()
		job.State, job.Reason, job.Report, job.FinishedAt = state, reason, report, &now
		h.running = false
		h.mu.Unlock()
	}()
	return c.Status(fiber.StatusAccepted).JSON(h.view(job))
}

func newRehomeID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// view is a copy of the job, safe to serialise while the job runs.
func (h *RehomeHandler) view(job *rehomeJob) rehomeJob {
	h.mu.Lock()
	defer h.mu.Unlock()
	return *job
}

// runGuarded is run with a panic turned into a failed job.
func (h *RehomeHandler) runGuarded(ctx context.Context, job *rehomeJob, run func(context.Context, *rehomeJob) (string, string, *rehomeReport)) (state, reason string, report *rehomeReport) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error().Interface("panic", r).Str("job", job.ID).Msg("Re-home job panicked")
			state, reason, report = "failed", "the job stopped on an internal error; the server log has it under this job's id", nil
		}
	}()
	return run(ctx, job)
}

// list handles GET /api/v1/rehome: the jobs this process remembers, newest
// first. They are kept in memory: a dry run changes nothing, so nothing is
// lost with them at a restart.
func (h *RehomeHandler) list(c *fiber.Ctx) error {
	h.mu.Lock()
	out := make([]rehomeJob, 0, len(h.order))
	for i := len(h.order) - 1; i >= 0; i-- {
		j := *h.jobs[h.order[i]]
		if j.Report != nil {
			// Without the per-file detail: a report can list thousands.
			r := *j.Report
			r.Outputs, r.Destinations = nil, nil
			j.Report = &r
		}
		out = append(out, j)
	}
	known := make(map[string]bool, len(h.jobs))
	for id := range h.jobs {
		known[id] = true
	}
	h.mu.Unlock()
	// Jobs recorded on disk that this process did not start: found at start
	// and not finished.
	if h.swap != nil && h.swap.Registry != nil {
		for _, j := range h.swap.Registry.Jobs() {
			if !known[j.ID] && !j.Cluster {
				out = append(out, rehomeJob{ID: j.ID, Path: j.Source.Key, State: "stuck", StartedAt: j.CreatedAt,
					Reason: "found at start in state " + string(j.State) + " and could not be taken up; the server log has the reason"})
			}
		}
	}
	return c.JSON(fiber.Map{"jobs": out, "count": len(out), "scope": "this node"})
}

// get handles GET /api/v1/rehome/:id.
func (h *RehomeHandler) get(c *fiber.Ctx) error {
	h.mu.Lock()
	job, ok := h.jobs[c.Params("id")]
	var out rehomeJob
	if ok {
		out = *job
	}
	h.mu.Unlock()
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "no such job on this node"})
	}
	return c.JSON(out)
}

// resumeJob handles POST /api/v1/rehome/:id/resume: a job that stopped part
// way in this process, or that was found at start and could not be taken up,
// is taken up again. It runs in the job slot, like a new job.
func (h *RehomeHandler) resumeJob(c *fiber.Ctx) error {
	if h.swap == nil || h.swap.Registry == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "moving a file's rows is not available on this node"})
	}
	if why := h.swapRefusal(); why != "" {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": why})
	}
	id := strings.Clone(c.Params("id"))
	rec, ok := h.swap.Registry.Get(id)
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "no job with that id is recorded on this node; a job that ended has no record"})
	}
	var body struct {
		SkipExportWait bool `json:"skip_export_wait"`
	}
	if len(c.Body()) > 0 {
		// Parsed as JSON whatever the Content-Type: a form-encoded reading
		// of the same bytes would leave the flag unset and take the request
		// for a plain resume.
		if err := json.Unmarshal(c.Body(), &body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "the body is not JSON"})
		}
	}
	if body.SkipExportWait {
		// A word to the running job, not a second run of it.
		if !h.waiveExportWait(id) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "that job is not waiting for the Iceberg export"})
		}
		h.logger.Warn().Str("job", id).Msg("Re-home: asked to end a job's wait for the Iceberg export")
		// The waiting job is the running one, which the job list does not
		// drop while it runs.
		h.mu.Lock()
		job := h.jobs[id]
		h.mu.Unlock()
		if job == nil {
			return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"id": id, "state": "running"})
		}
		return c.Status(fiber.StatusAccepted).JSON(h.view(job))
	}
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "another re-home job is running; one runs at a time"})
	}
	job, known := h.jobs[id]
	if !known {
		job = &rehomeJob{ID: id, Path: rec.Source.Key, StartedAt: rec.CreatedAt}
		h.jobs[id] = job
		h.order = append(h.order, id)
	}
	job.State, job.Reason, job.FinishedAt = "running", "", nil
	h.running = true
	h.mu.Unlock()

	h.done.Add(1)
	go func() {
		defer h.done.Done()
		jctx, jcancel := context.WithCancel(h.ctx)
		defer jcancel()
		state, reason, _ := h.runGuarded(jctx, job, func(ctx context.Context, job *rehomeJob) (string, string, *rehomeReport) {
			if err := h.resume(ctx, job.ID); err != nil {
				if why, refused := rehome.IsRefused(err); refused {
					return "refused", why, nil
				}
				var kept errJobKept
				if errors.As(err, &kept) {
					return "stopped", kept.Error(), nil
				}
				h.logger.Error().Err(err).Str("job", job.ID).Msg("Re-home: a job could not be taken up again; its record is kept")
				return "stopped", "the job could not be taken up again and its record is kept; the server log has the error under this job's id", nil
			}
			return "done", "", nil
		})
		now := time.Now().UTC()
		h.mu.Lock()
		job.State, job.Reason, job.FinishedAt = state, reason, &now
		h.running = false
		h.mu.Unlock()
	}()
	return c.Status(fiber.StatusAccepted).JSON(h.view(job))
}

// dryRun copies the file to a scratch directory, splits it there, checks the
// split and reports. Everything it writes is in the scratch directory, which
// it removes before it returns.
//
// A failure's reason says which step failed and nothing more: the error
// itself can name the scratch path, the statement or the storage endpoint,
// and goes to the log under the job's id.
func (h *RehomeHandler) dryRun(ctx context.Context, job *rehomeJob) (state, reason string, report *rehomeReport) {
	fail := func(step string, err error) (string, string, *rehomeReport) {
		if why, refused := rehome.IsRefused(err); refused {
			return "refused", why, nil
		}
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", job.Path).Str("step", step).Msg("Re-home dry run failed")
		if ctx.Err() != nil {
			return "failed", fmt.Sprintf("the job was stopped while it was %s (it ran out of its %s, or the server is shutting down)", step, rehomeJobTimeout), nil
		}
		return "failed", fmt.Sprintf("the job failed while it was %s; the server log has the error under this job's id", step), nil
	}
	read := h.storage.ReadTo
	if job.Tier == rehome.TierCold {
		// What the real run would refuse, said by the dry run too. Nothing
		// is owned here, so the answer is as of now.
		_, _, cold, cerr := h.coldSource(ctx, job.Path)
		if cerr != nil {
			return fail("checking the file in cold storage", cerr)
		}
		read = cold.ReadTo
	}
	prep, step, err := h.prepare(ctx, job.Path, read)
	if err != nil {
		return fail(step, err)
	}
	defer prep.close()
	src, plan, local, outDir, db := prep.src, prep.plan, prep.local, prep.outDir, prep.db
	report = &rehomeReport{
		Database: src.Database, Measurement: src.Measurement, Shape: string(src.Shape), Directory: src.Dir,
		TimeType: plan.TimeType, Rows: plan.Rows, Bytes: prep.size,
		Misfiled: plan.Misfiled(), MisfiledRows: plan.MisfiledRows(), RemainderRows: plan.RemainderRows,
		Destinations:      plan.Destinations[:min(len(plan.Destinations), rehomeDestinationsListed)],
		DestinationsTotal: len(plan.Destinations),
	}
	if len(plan.Destinations) > rehome.MaxSplitDestinations {
		// The plan is the answer the caller asked for; the split is the
		// check on it, and its cost is one pass over the file per output.
		report.SplitSkipped = fmt.Sprintf("the file holds rows of %d hours or days; a split writes at most %d files", len(plan.Destinations), rehome.MaxSplitDestinations)
	} else {
		outs, err := rehome.Split(ctx, db, plan, local, outDir)
		if err != nil {
			return fail("splitting the file and checking the split", err)
		}
		report.SplitChecked, report.Outputs = true, outs
	}
	h.logger.Info().Str("job", job.ID).Str("path", src.Key).Int64("rows", plan.Rows).
		Int64("misfiled_rows", plan.MisfiledRows()).Int("destinations", len(plan.Destinations)).
		Bool("split_checked", report.SplitChecked).
		Msg("Re-home dry run done; nothing was changed")
	return "done", "", report
}

// rehomePrep is a source file copied to a scratch directory, with the private
// DuckDB instance that reads it and the plan of where its rows belong.
type rehomePrep struct {
	src     rehome.Source
	scratch string
	local   string // the copy of the source
	outDir  string // where a split writes
	size    int64
	sha256  string
	db      *sql.DB
	plan    rehome.Plan
}

// close removes the scratch directory and closes the instance.
func (p *rehomePrep) close() {
	if p.db != nil {
		p.db.Close()
	}
	if p.scratch != "" {
		os.RemoveAll(p.scratch)
	}
}

// prepare copies the file at key to a scratch directory, hashing it on the
// way, opens the private DuckDB instance and plans the split. On an error it
// returns the step that failed, for the job's reason, and has cleaned up.
func (h *RehomeHandler) prepare(ctx context.Context, key string, read func(ctx context.Context, key string, w io.Writer) error) (prep *rehomePrep, step string, err error) {
	p := &rehomePrep{}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	if p.src, err = rehome.ParseKey(key); err != nil {
		return nil, "reading the path", err
	}
	if p.scratch, err = os.MkdirTemp(h.scratchRoot, rehomeScratchPrefix+"*"); err != nil {
		return nil, "creating a scratch directory", err
	}
	spill := filepath.Join(p.scratch, "spill")
	p.outDir = filepath.Join(p.scratch, "out")
	for _, d := range []string{spill, p.outDir} {
		if err = os.Mkdir(d, 0o700); err != nil {
			return nil, "creating a scratch directory", err
		}
	}
	p.local = filepath.Join(p.scratch, "source.parquet")
	f, err := os.OpenFile(p.local, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, "creating the local copy", err
	}
	sum := sha256.New()
	err = read(ctx, p.src.Key, io.MultiWriter(f, sum))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, "copying the file to local disk", err
	}
	st, err := os.Stat(p.local)
	if err != nil {
		return nil, "copying the file to local disk", err
	}
	p.size, p.sha256 = st.Size(), hex.EncodeToString(sum.Sum(nil))

	if p.db, err = database.OpenRewriteDB(ctx, database.RewriteDBOptions{
		MemoryLimitBytes: rehomeMemoryLimit,
		SpillDir:         spill,
		AllowedDirs:      []string{p.scratch, spill, p.outDir},
	}, h.logger); err != nil {
		return nil, "opening the private DuckDB instance", err
	}
	if p.plan, err = rehome.MakePlan(ctx, p.db, p.src, p.local); err != nil {
		return nil, "reading the file's schema and times (it may not be a readable Parquet file)", err
	}
	return p, "", nil
}
