package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
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
)

// RehomeHandler serves /api/v1/rehome.
type RehomeHandler struct {
	storage     storage.Backend
	authManager *auth.AuthManager
	logger      zerolog.Logger

	mu      sync.Mutex
	jobs    map[string]*rehomeJob
	order   []string // job ids, oldest first
	running bool
	// run does a job's work; a test seam.
	run func(ctx context.Context, job *rehomeJob)
}

// rehomeJob is one request and its report.
type rehomeJob struct {
	ID         string     `json:"id"`
	Path       string     `json:"path"`
	DryRun     bool       `json:"dry_run"`
	State      string     `json:"state"` // running, done, refused, failed
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Reason: why a job was refused or failed.
	Reason string        `json:"reason,omitempty"`
	Report *rehomeReport `json:"report,omitempty"`
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
	// Outputs are the files a split writes, checked: each holds the rows
	// counted for it, and only times inside its directory.
	Outputs []rehome.Output `json:"outputs"`
}

// NewRehomeHandler creates the handler. authManager may be nil (auth off).
func NewRehomeHandler(backend storage.Backend, authManager *auth.AuthManager, logger zerolog.Logger) *RehomeHandler {
	h := &RehomeHandler{
		storage:     backend,
		authManager: authManager,
		logger:      logger.With().Str("component", "rehome").Logger(),
		jobs:        map[string]*rehomeJob{},
	}
	h.run = h.dryRun
	return h
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
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": "moving a file's rows is not available in this version; send \"dry_run\": true to see where they belong",
		})
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
	if err != nil || size < 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": fmt.Sprintf("%s is not in this node's storage (a file on the cold tier is not supported in this version)", src.Key)})
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
	job := &rehomeJob{ID: id, Path: src.Key, DryRun: true, State: "running", StartedAt: time.Now().UTC()}
	h.jobs[id] = job
	h.order = append(h.order, id)
	for len(h.order) > rehomeJobsKept {
		delete(h.jobs, h.order[0])
		h.order = h.order[1:]
	}
	h.running = true
	h.mu.Unlock()

	go func() {
		// Not the request's context: the job outlives the request.
		jctx, jcancel := context.WithTimeout(context.Background(), rehomeJobTimeout)
		defer jcancel()
		defer func() {
			if r := recover(); r != nil {
				h.finish(job, "failed", fmt.Sprintf("the job panicked: %v", r), nil)
			}
			h.mu.Lock()
			h.running = false
			h.mu.Unlock()
		}()
		h.run(jctx, job)
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

func (h *RehomeHandler) finish(job *rehomeJob, state, reason string, report *rehomeReport) {
	now := time.Now().UTC()
	h.mu.Lock()
	job.State, job.Reason, job.Report, job.FinishedAt = state, reason, report, &now
	h.mu.Unlock()
}

// list handles GET /api/v1/rehome: the jobs this process remembers, newest
// first. They are kept in memory: a dry run changes nothing, so nothing is
// lost with them at a restart.
func (h *RehomeHandler) list(c *fiber.Ctx) error {
	h.mu.Lock()
	out := make([]rehomeJob, 0, len(h.order))
	for i := len(h.order) - 1; i >= 0; i-- {
		out = append(out, *h.jobs[h.order[i]])
	}
	h.mu.Unlock()
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

// dryRun copies the file to a scratch directory, splits it there, checks the
// split and reports. Everything it writes is in the scratch directory, which
// it removes.
func (h *RehomeHandler) dryRun(ctx context.Context, job *rehomeJob) {
	fail := func(err error) {
		if reason, refused := rehome.IsRefused(err); refused {
			h.finish(job, "refused", reason, nil)
			return
		}
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", job.Path).Msg("Re-home dry run failed")
		h.finish(job, "failed", err.Error(), nil)
	}
	src, err := rehome.ParseKey(job.Path)
	if err != nil {
		fail(err)
		return
	}
	scratch, err := os.MkdirTemp("", "arc-rehome-")
	if err != nil {
		fail(fmt.Errorf("create a scratch directory: %w", err))
		return
	}
	defer os.RemoveAll(scratch)
	spill, outDir := filepath.Join(scratch, "spill"), filepath.Join(scratch, "out")
	for _, d := range []string{spill, outDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			fail(fmt.Errorf("create a scratch directory: %w", err))
			return
		}
	}

	local := filepath.Join(scratch, "source.parquet")
	f, err := os.OpenFile(local, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		fail(fmt.Errorf("create the local copy: %w", err))
		return
	}
	err = h.storage.ReadTo(ctx, src.Key, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		fail(fmt.Errorf("copy %s to local disk: %w", src.Key, err))
		return
	}
	st, err := os.Stat(local)
	if err != nil {
		fail(err)
		return
	}

	db, err := database.OpenRewriteDB(ctx, database.RewriteDBOptions{
		MemoryLimitBytes: rehomeMemoryLimit,
		SpillDir:         spill,
		AllowedDirs:      []string{scratch, spill, outDir},
	}, h.logger)
	if err != nil {
		fail(fmt.Errorf("open the private DuckDB instance: %w", err))
		return
	}
	defer db.Close()

	plan, err := rehome.MakePlan(ctx, db, src, local)
	if err != nil {
		fail(err)
		return
	}
	outs, err := rehome.Split(ctx, db, plan, local, outDir)
	if err != nil {
		fail(err)
		return
	}
	h.finish(job, "done", "", &rehomeReport{
		Database: src.Database, Measurement: src.Measurement, Shape: string(src.Shape), Directory: src.Dir,
		TimeType: plan.TimeType, Rows: plan.Rows, Bytes: st.Size(),
		Misfiled: plan.Misfiled(), MisfiledRows: plan.MisfiledRows(), RemainderRows: plan.RemainderRows,
		Outputs: outs,
	})
	h.logger.Info().Str("job", job.ID).Str("path", src.Key).Int64("rows", plan.Rows).
		Int64("misfiled_rows", plan.MisfiledRows()).Int("destinations", len(plan.Destinations)).
		Msg("Re-home dry run done; nothing was changed")
}
