package api

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// CompactionGate is the minimal cluster interface the compaction handler needs
// (#1152): whether this node may run a compaction cycle. Same shape as
// compaction.ClusterGate, which the scheduler already consults, so the value
// main builds for the scheduler satisfies this with no adapter — and the two
// paths cannot drift apart. nil = OSS or standalone, no gate.
type CompactionGate interface {
	// CanCompact reports whether the local node may run compaction. With
	// compactor failover configured this is the FSM's active-compactor
	// lease; otherwise it is the static role capability.
	CanCompact() bool
	// Role returns a human-readable role string for the rejection message.
	Role() string
	// LeaseHolder returns the node ID holding the compactor lease, or ""
	// when this cluster manages no lease at all.
	//
	// The rejection message needs this because CanCompact() is false for two
	// unrelated reasons and the remedy differs. With a lease, the operator
	// sends the trigger to its holder. Without one — the default, since
	// cluster.failover_enabled is false — the refusal came from the static
	// role, there is no lease to route to, and POST
	// /api/v1/cluster/compactor/assign cannot create the first one: it
	// answers "this cluster does not manage a compactor lease". Telling that
	// operator to assign a lease sends them to an endpoint that refuses in
	// exactly the state the message describes.
	LeaseHolder() string
}

// CompactionHandler handles compaction API endpoints
type CompactionHandler struct {
	manager         *compaction.Manager
	hourlyScheduler *compaction.Scheduler
	dailyScheduler  *compaction.Scheduler
	authManager     *auth.AuthManager
	gate            CompactionGate // nil in OSS and standalone mode
	logger          zerolog.Logger
}

// NewCompactionHandler creates a new compaction handler.
//
// gate is a constructor parameter rather than a setter (which is how the
// backup handler takes its coordinator) because main already holds the gate
// before it builds this handler: the value exists at the compaction-gate
// block and the handler is constructed much later. A setter would leave the
// wiring optional, which is the failure this gate exists to close — the
// trigger endpoint ran a full cycle on any node precisely because nothing
// required the role check to be wired.
func NewCompactionHandler(manager *compaction.Manager, hourlyScheduler, dailyScheduler *compaction.Scheduler, authManager *auth.AuthManager, gate CompactionGate, logger zerolog.Logger) *CompactionHandler {
	return &CompactionHandler{
		manager:         manager,
		hourlyScheduler: hourlyScheduler,
		dailyScheduler:  dailyScheduler,
		authManager:     authManager,
		gate:            gate,
		logger:          logger.With().Str("component", "compaction-handler").Logger(),
	}
}

// rejectUnlessCompactionLeaseHolder answers 503 when this node is a cluster
// member that may not compact, and reports whether it did. Evaluated per
// request, as the scheduler evaluates its gate at every tick, so a lease
// hand-over or a demotion takes effect without a restart.
//
// 503 rather than 409: this endpoint already answers 409 for two states —
// paused cluster-wide and a cycle already running — and arcli maps any 409 on
// this route to "a compaction cycle is already running (cycle N)", reading
// only cycle_id from the body. A role rejection carries no cycle id, so a
// third 409 would be reported to operators as cycle 0. The backup and delete
// APIs chose 503 over 409 for the same overload.
//
// The wording branches on LeaseHolder because the two refusals need different
// remedies, and it never blames the role alone: a standby RoleCompactor is
// refused while its own role string still reads "compactor".
//
// The action goes in "error", not only in "message", because arcli builds its
// HTTPError from the error field and discards message — so an operator using
// the CLI would otherwise see the refusal without the way out.
//
// can_compact is always false here; it is a hint for a client, not a reliable
// discriminator against the other 503 on this route (manager not initialized),
// which omits the key entirely and so unmarshals to false as well. That 503 is
// unreachable in a running server anyway: the handler is only constructed when
// the manager is non-nil.
func (h *CompactionHandler) rejectUnlessCompactionLeaseHolder(c *fiber.Ctx) bool {
	if h.gate == nil || h.gate.CanCompact() {
		return false
	}
	role := h.gate.Role()
	holder := h.gate.LeaseHolder()

	body := fiber.Map{"role": role, "can_compact": false, "lease_holder": holder}
	if holder != "" {
		body["error"] = "compaction rejected: the compaction lease is held by " + holder + "; send the trigger there, or move the lease with POST /api/v1/cluster/compactor/assign"
		body["message"] = "this node does not hold the compaction lease"
	} else {
		// No lease exists. Saying "does not hold the lease" here would send
		// the operator looking for a holder that does not exist, and the
		// assign endpoint refuses while no lease is managed.
		body["error"] = "compaction rejected: this cluster manages no compaction lease and role " + role + " may not compact; send the trigger to a node whose cluster.role is compactor, or set cluster.failover_enabled so the lease can be assigned"
		body["message"] = "refused by the static role capability, not by a lease"
	}

	h.logger.Warn().
		Str("role", role).
		Str("lease_holder", holder).
		Msg("Manual compaction trigger refused: this node may not run compaction")
	_ = c.Status(fiber.StatusServiceUnavailable).JSON(body)
	return true
}

// RegisterRoutes registers compaction endpoints
func (h *CompactionHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/compaction")

	// Admin-only, including the read-only routes. Compaction is cluster-wide
	// operator work — it cannot be configured per team or per database, so no
	// tenant has a reason to read it — and /candidates, /jobs and /history
	// return {database, measurement, partition_path} for every tenant in the
	// deployment. They previously took any authenticated token, which made
	// them a database- and measurement-name enumeration surface for a token
	// with no grant on either.
	adminOnly := withAdminAuth(h.authManager)
	group.Get("/status", adminOnly, h.getStatus)
	group.Get("/stats", adminOnly, h.getStats)
	group.Get("/candidates", adminOnly, h.getCandidates)
	group.Get("/jobs", adminOnly, h.getActiveJobs)
	group.Get("/history", adminOnly, h.getHistory)
	// Per-cycle lookup (#1162). /compaction/stats carries a single global
	// last_cycle, so the cycle_id the trigger hands back had nowhere to be
	// resolved. Static /cycles is registered before the /cycles/:id param
	// route; Fiber v2 matches in registration order and the two differ in
	// segment count anyway.
	group.Get("/cycles", adminOnly, h.listCycles)
	group.Get("/cycles/:id", adminOnly, h.getCycle)

	// Admin route — trigger compaction requires admin permission
	if h.authManager != nil {
		group.Post("/trigger", auth.RequireAdmin(h.authManager), h.triggerCompaction)
	} else {
		group.Post("/trigger", h.triggerCompaction)
	}

	h.logger.Info().Msg("Compaction routes registered")
}

// getStatus handles GET /api/v1/compaction/status
func (h *CompactionHandler) getStatus(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	stats := h.manager.Stats()
	response := fiber.Map{
		"manager": fiber.Map{
			"active_jobs":     stats["active_jobs"],
			"total_completed": stats["total_jobs_completed"],
			"total_failed":    stats["total_jobs_failed"],
		},
	}

	// Add scheduler status for each tier
	schedulers := fiber.Map{}
	if h.hourlyScheduler != nil {
		schedulers["hourly"] = h.hourlyScheduler.Status()
	}
	if h.dailyScheduler != nil {
		schedulers["daily"] = h.dailyScheduler.Status()
	}
	response["schedulers"] = schedulers

	return c.JSON(response)
}

// getStats handles GET /api/v1/compaction/stats
func (h *CompactionHandler) getStats(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	return c.JSON(h.manager.Stats())
}

// getCandidates handles GET /api/v1/compaction/candidates
func (h *CompactionHandler) getCandidates(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	candidates, err := h.manager.FindCandidates(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to find compaction candidates")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to find candidates: " + err.Error(),
		})
	}

	// Convert candidates to JSON-friendly format
	candidateList := make([]fiber.Map, len(candidates))
	for i, cand := range candidates {
		candidateList[i] = fiber.Map{
			"database":       cand.Database,
			"measurement":    cand.Measurement,
			"partition_path": cand.PartitionPath,
			"file_count":     cand.FileCount,
			"tier":           cand.Tier,
		}
	}

	return c.JSON(fiber.Map{
		"count":      len(candidates),
		"candidates": candidateList,
	})
}

// triggerCompaction handles POST /api/v1/compaction/trigger
// Query parameters:
//   - tier=hourly,daily (optional, defaults to all enabled tiers)
//   - database=mydb (optional, defaults to all databases)
//   - measurement=cpu (optional; requires database)
//
// The manual cycle uses the configured compaction.cycle_timeout budget.
func (h *CompactionHandler) triggerCompaction(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	// Parse database parameter (reuses same validation as database creation API)
	dbParam := c.Query("database", "")
	if dbParam != "" && !isValidDatabaseName(dbParam) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	// Fiber query strings may alias pooled request memory. Clone values
	// retained by the asynchronous worker before returning from this handler.
	dbParam = strings.Clone(dbParam)
	measurementParam := strings.Clone(c.Query("measurement", ""))
	if measurementParam != "" && dbParam == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "measurement requires database",
		})
	}
	if measurementParam != "" && !isValidMeasurementName(measurementParam) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid measurement name",
		})
	}

	// Parse tier parameter (comma-separated list)
	tierParam := c.Query("tier", "")
	var tierNames []string

	if tierParam != "" {
		// Split comma-separated tiers
		parts := strings.Split(tierParam, ",")
		for _, part := range parts {
			tier := strings.TrimSpace(part)
			if tier != "" {
				tierNames = append(tierNames, strings.Clone(tier))
			}
		}
	}

	// If no tiers specified, use all enabled tiers
	if len(tierNames) == 0 {
		for _, tier := range h.manager.Tiers {
			if tier.IsEnabled() {
				tierNames = append(tierNames, tier.GetTierName())
			}
		}
	}

	// Role gate (#1152). After parameter validation — a malformed database
	// or tier name is a client error on every node, and reporting it as a
	// role problem would send an operator to a different node to get the
	// same 400 — but ahead of both state-dependent refusals and ahead of
	// the accepted-trigger log line below: a node that may not compact must
	// not be told its trigger was accepted, and its answer must not depend
	// on whether a cycle happens to be running or paused here. Before this
	// check the endpoint ran a full cycle on whatever node received it —
	// only the scheduler consulted the lease — and the partition lock is
	// per-process, so a trigger on a non-lease node could delete the same
	// inputs as the lease holder. With peer replication it is worse and
	// needs no second node: the completion-manifest watcher starts only on a
	// node that may compact, so the job deletes the sources from storage
	// while nothing registers the output or the deletes in Raft.
	if h.rejectUnlessCompactionLeaseHolder(c) {
		return nil
	}

	// The cluster-wide compaction pause a cluster restore holds (#1087). A
	// cycle started now would stop at its first batch boundary anyway, so
	// refuse up front, before the trigger is logged as accepted. Checked
	// before the running-cycle check: a cycle that was already running when
	// the pause landed is ending at its boundary.
	if h.manager.Paused() {
		h.logger.Info().Strs("tiers", tierNames).Msg("Manual compaction trigger refused: compaction is paused cluster-wide")
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":   "compaction is paused cluster-wide",
			"message": "a restore is running; retry when it ends",
			"paused":  true,
		})
	}

	logEvent := h.logger.Info().
		Strs("tiers", tierNames).
		Dur("cycle_timeout", h.manager.CycleTimeout)
	if measurementParam != "" {
		logEvent = logEvent.Str("measurement", measurementParam)
	}
	if dbParam != "" {
		logEvent = logEvent.Str("database", dbParam)
	}
	// Claim the cycle synchronously (#1153). Before this the handler read
	// IsCycleRunning() here and left the real claim -- the CompareAndSwap --
	// to the goroutine below, so two triggers could both pass the read and
	// both be answered 200 while only one cycle ran. That is not a narrow
	// race: the goroutine is scheduled lazily, so measured against 32
	// concurrent triggers, a substantial fraction were falsely accepted (3 to
	// 16 across runs and machines). The loser's caller had already been handed
	// an id belonging to the winner.
	//
	// Claiming here also makes the reported id real rather than a prediction,
	// which is what an operator needs: /compaction/stats carries a single
	// global last_cycle, so matching on cycle_id is the only way to tell
	// whose result is whose.
	claim, err := h.manager.ClaimCycle()
	if err != nil {
		// RunningCycleID rather than GetCurrentCycleID: it takes the same
		// mutex as the claim, so it cannot report the previous, finished
		// cycle's id in the instant between a claim and its id assignment.
		// arcli prints this number verbatim as "cycle N", so a wrong or
		// zero id is what the operator sees.
		running := h.manager.RunningCycleID()
		h.logger.Info().
			Int64("cycle_id", running).
			Strs("tiers", tierNames).
			Msg("Manual compaction trigger refused: a cycle is already running")
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":      "Compaction cycle already running",
			"message":    "A compaction cycle is already in progress. Please wait for it to complete.",
			"cycle_id":   running,
			"is_running": true,
		})
	}

	// Logged only once the claim is held, so a refused trigger is never
	// recorded as accepted -- the same ordering the lease gate and the pause
	// check already follow. Nothing between the claim and the goroutine can
	// return: an unreleased claim would leave cycleRunning true and stop
	// compaction on this node until a restart.
	logEvent.Int64("cycle_id", claim.ID).Msg("Manual compaction triggered via API")

	// Trigger compaction asynchronously
	go func() {
		// First statement: the claim must be released on every exit from
		// this goroutine, including a panic unwind.
		defer claim.Release()

		ctx, cancel := context.WithTimeout(context.Background(), h.manager.CycleTimeout)
		defer cancel()

		start := time.Now()
		cycleID := claim.ID
		var err error
		if measurementParam != "" {
			err = h.manager.RunClaimedCycleForMeasurement(ctx, claim, dbParam, measurementParam, tierNames)
		} else if dbParam != "" {
			err = h.manager.RunClaimedCycleForDatabase(ctx, claim, dbParam, tierNames)
		} else {
			err = h.manager.RunClaimedCycleForTiers(ctx, claim, tierNames)
		}
		duration := time.Since(start)

		logCtx := h.logger.With().
			Int64("cycle_id", cycleID).
			Dur("duration", duration).
			Strs("tiers", tierNames)
		if dbParam != "" {
			logCtx = logCtx.Str("database", dbParam)
		}
		logger := logCtx.Logger()

		if ctx.Err() != nil {
			logger.Info().Err(ctx.Err()).Msg("Manual compaction interrupted")
		} else if errors.Is(err, compaction.ErrCompactionPaused) {
			logger.Info().Msg("Manual compaction stopped at a batch boundary: compaction is paused cluster-wide")
		} else if err != nil {
			logger.Error().Err(err).Msg("Manual compaction failed")
		} else {
			logger.Info().Msg("Manual compaction completed")
		}
	}()

	resp := fiber.Map{
		"message":  "Compaction triggered",
		"status":   "running",
		"tiers":    tierNames,
		"cycle_id": claim.ID, // the id this cycle claimed, not a prediction
	}
	if dbParam != "" {
		resp["database"] = dbParam
	}
	if measurementParam != "" {
		resp["measurement"] = measurementParam
	}
	// An unscoped trigger honors compaction.exclude_databases exactly like a
	// scheduled cycle. Say so in the response, so an operator wondering why
	// a database was skipped doesn't need the debug log.
	if dbParam == "" {
		if excluded := h.manager.ExcludedDatabases(); len(excluded) > 0 {
			resp["exclude_databases"] = excluded
		}
	}
	return c.JSON(resp)
}

// getActiveJobs handles GET /api/v1/compaction/jobs
func (h *CompactionHandler) getActiveJobs(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	stats := h.manager.Stats()
	activeJobCount := 0
	if count, ok := stats["active_jobs"].(int); ok {
		activeJobCount = count
	}

	return c.JSON(fiber.Map{
		"active_jobs": activeJobCount,
		"jobs":        []fiber.Map{}, // TODO: Return actual job details when available
	})
}

// getHistory handles GET /api/v1/compaction/history
func (h *CompactionHandler) getHistory(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	// Get limit from query param
	limitStr := c.Query("limit", "10")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 10
	}
	if limit > compaction.JobHistoryLimit {
		limit = compaction.JobHistoryLimit
	}

	// Optional filter to one compaction cycle, so the cycle_id a trigger
	// returned can be resolved down to the individual partitions it touched.
	// Rejected rather than treated as "no filter" when invalid: ids start at 1,
	// so a 0 is a bad request, and silently returning every job for a
	// malformed id would read as "this cycle touched everything".
	var cycleID int64
	// An ABSENT or empty cycle_id means no filter; a present-but-invalid one is
	// rejected. The empty string is treated as absent deliberately, because
	// that is what a client building a query string from an unset variable
	// sends, and refusing it would turn a missing filter into an error.
	if raw := c.Query("cycle_id"); raw != "" {
		cycleID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || cycleID < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "cycle_id must be a positive integer",
			})
		}
	}

	// One acquisition: the page, the match count and the cycle cross-check all
	// come from the same snapshot, so no two fields in this response can
	// disagree. Also avoids building the whole Stats map to read one counter.
	page := h.manager.JobHistory(limit, cycleID)
	recentJobs := make([]interface{}, 0, len(page.Jobs))
	for _, job := range page.Jobs {
		recentJobs = append(recentJobs, job)
	}

	// Job records count ATTEMPTS, not batches: the adaptive splitter retries a
	// failed batch at half size, and every attempt writes its own record with
	// the same partition_path, distinguished only by attempt_depth.
	//
	// Depth-0 failures are NOT the batch failures: a batch that fails at depth
	// 0 and is rescued by splitting counts as succeeded and contributes no
	// failed partition, yet still leaves a depth-0 success=false record here.
	// To reconcile, use the cycle record: sum(failed_partitions) ==
	// failed_batches when not truncated.
	resp := fiber.Map{
		"total_jobs":  page.TotalJobsCompleted,
		"recent_jobs": recentJobs,
		"matched":     page.Matched,
		"page_size":   len(recentJobs),
		"retained":    page.Retained,
		"capacity":    compaction.JobHistoryLimit,
		"unit":        "attempts",
	}

	// Decided by the manager, which is the only layer a test can drive against
	// a full ring. See JobHistoryPage.Truncated for why it is not simply
	// matched > page_size.
	resp["truncated"] = page.Truncated
	if page.MayHaveEvicted {
		resp["may_have_evicted_earlier_attempts"] = true
	}

	if cycleID > 0 {
		resp["cycle_id"] = cycleID
		if page.CycleFound {
			// Ground truth. The cycle record is retained far longer than the
			// job ring and counts outer-level batches, so these are the
			// numbers to trust and to compare the page against.
			resp["cycle_status"] = page.CycleStatus
			resp["cycle_failed_batches"] = page.CycleFailedBatches
			resp["cycle_started_batches"] = page.CycleStartedBatches
			if len(page.CycleFailedPartitions) > 0 {
				resp["cycle_failed_partitions"] = page.CycleFailedPartitions
				resp["cycle_failed_partition_count"] = len(page.CycleFailedPartitions)
			}
			if page.CycleFailedPartitionsCapped {
				resp["cycle_failed_partitions_truncated"] = true
			}
		} else {
			// Without this, zero rows for an aged-out cycle is identical to
			// zero rows for a cycle that failed nothing -- the most expensive
			// wrong answer this endpoint can give.
			resp["cycle_retained"] = false
			if page.HasCycleRange {
				resp["oldest_retained_cycle"] = page.OldestRetainedCycle
				resp["newest_retained_cycle"] = page.NewestRetainedCycle
			}
			resp["message"] = "this node retains no record of that compaction cycle, so an empty result here does not mean the cycle failed nothing. Cycle ids restart at 1 when the process restarts, and cycles run on the node holding the compactor lease."
		}
	}
	return c.JSON(resp)
}

// cycleResponse renders a retained cycle. Built as a fiber.Map rather than a
// tagged struct because several keys are omitted conditionally and
// `json:",omitempty"` does nothing for a time.Time: encoding/json (which Fiber
// uses, no custom JSONEncoder is set) has no notion of an empty struct, so a
// zero time would serialise as "0001-01-01T00:00:00Z".
//
// databases is null when the cycle covered every database, and measurement is
// "" when it covered every measurement. This is deliberately the opposite of
// Stats(), which coerces a nil slice to []: here [] would be ambiguous with
// "no databases", so null carries the meaning.
func cycleResponse(rec compaction.CycleRecord) fiber.Map {
	return cycleResponseWithPartitions(rec, true)
}

// cycleResponseWithPartitions renders a cycle, optionally omitting the full
// failed-partition map.
//
// The list route omits it: 500 retained cycles x 200 partitions is ~100,000
// entries in one Fiber-buffered response, several megabytes, where the old
// 10-path sample was a few hundred kilobytes. The count and the truncation
// flag still ride along, and the full map is one request away on
// /cycles/{id} or /history?cycle_id=.
func cycleResponseWithPartitions(rec compaction.CycleRecord, includePartitions bool) fiber.Map {
	startedAt := rec.StartedAt.UTC().Truncate(time.Second)
	out := fiber.Map{
		"cycle_id":            rec.CycleID,
		"status":              rec.Status,
		"source":              rec.Source,
		"databases":           rec.Databases,
		"measurement":         rec.Measurement,
		"tiers":               rec.Tiers,
		"started_at":          startedAt,
		"discovered_batches":  rec.Discovered,
		"started_batches":     rec.Started,
		"succeeded_batches":   rec.Succeeded,
		"failed_batches":      rec.Failed,
		"interrupted_batches": rec.Interrupted,
		"unstarted_batches":   rec.Unstarted,
		"discovery_errors":    rec.DiscoveryErrors,
	}

	if rec.FinishedAt.IsZero() {
		// Still running: the counters above are a live snapshot, so report the
		// elapsed time rather than a duration that does not exist yet.
		out["duration_seconds"] = int64(time.Since(startedAt).Seconds())
	} else {
		// Both derived from the TRUNCATED values that are rendered, so the
		// duration always equals finished_at - started_at as the client reads
		// them. Computing from the untruncated times let a cycle that started
		// at :00.9 and finished at :01.1 render a 1 s gap with duration 0.
		finishedAt := rec.FinishedAt.UTC().Truncate(time.Second)
		out["finished_at"] = finishedAt
		out["duration_seconds"] = int64(finishedAt.Sub(startedAt).Seconds())
	}
	if rec.Err != "" {
		out["error"] = rec.Err
	}
	if len(rec.FailedPartitions) > 0 && includePartitions {
		// WHICH partitions failed, deduplicated, so this is the retry list
		// rather than a sample. Keyed by partition path with the number of its
		// batches that failed, so one entry covers a partition however many
		// batches it was split into.
		out["failed_partitions"] = rec.FailedPartitions
	}
	if n := len(rec.FailedPartitions); n > 0 {
		out["failed_partition_count"] = n
	}
	if rec.FailedPartitionsTruncated {
		// Never let a capped list read as complete.
		out["failed_partitions_truncated"] = true
	}
	return out
}

// getCycle resolves one compaction cycle id (#1162).
func (h *CompactionHandler) getCycle(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	// ParseInt, not Atoi: cycle ids are int64. Ids start at 1, so a zero or
	// negative id is a bad request rather than a cycle that was trimmed --
	// reporting it as "older than the retained window" would be a lie.
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id < 1 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "cycle id must be a positive integer",
		})
	}

	// History first, and only then the claim. The claim is taken before the
	// cycle records anything and released after the terminal record is
	// written, so it is held during two windows where it disagrees with the
	// history; checking the record first means the recorded answer always
	// wins and the synthesized one below is reachable only when there is
	// genuinely nothing recorded.
	//
	// The record and the retained range come from ONE lock acquisition, so the
	// range a 404 reports is the same snapshot that produced the miss.
	rec, found, oldest, newest, haveRange := h.manager.CycleLookup(id)
	if found {
		return c.JSON(cycleResponse(rec))
	}

	// Claimed, not yet started. The trigger answers with the cycle id as soon
	// as it holds the claim, and the cycle body runs on a goroutine that is
	// scheduled lazily, so an operator who triggers and immediately looks up
	// the id they were handed lands here. IsCycleRunning is required:
	// RunningCycleID reports the most recent id when no claim is held, which
	// would synthesize this answer for a long-finished cycle.
	if h.manager.IsCycleRunning() && h.manager.RunningCycleID() == id {
		return c.JSON(fiber.Map{
			"cycle_id": id,
			"status":   "claimed",
			"message":  "this cycle is claimed and starting; its record appears within milliseconds",
		})
	}

	resp := fiber.Map{
		"error":    "no retained record of this compaction cycle on this node",
		"cycle_id": id,
	}
	// On a cluster the holder is the only actionable datum in this response:
	// it names where the cycle actually ran. The trigger 503 already reports
	// it (#1152), so the lookup that follows a trigger should too. nil gate is
	// OSS or standalone, and an empty holder means this cluster manages no
	// lease at all -- in neither case is there a holder to name.
	if h.gate != nil {
		if holder := h.gate.LeaseHolder(); holder != "" {
			resp["lease_holder"] = holder
		}
	}
	switch {
	case !haveRange:
		// No range keys at all. Emitting 0/0 would make every id look newer
		// than the newest retained cycle.
		resp["message"] = "this node has recorded no compaction cycle. History is held in memory only and is lost on restart, and cycles run on the node holding the compactor lease."
	case id < oldest:
		resp["oldest_retained"] = oldest
		resp["newest_retained"] = newest
		resp["message"] = "this cycle is older than the window this node retains. History is held in memory only, so it is also lost on restart."
	default:
		resp["oldest_retained"] = oldest
		resp["newest_retained"] = newest
		// Deliberately not "this cycle never ran": cycle ids restart at 1 on
		// every restart, so an id recorded before one is genuinely absent here
		// and did run.
		resp["message"] = "this node has recorded no cycle with that id. Cycle ids restart at 1 when the process restarts, and cycles run on the node holding the compactor lease, so a cycle triggered elsewhere is not recorded here."
	}
	return c.Status(fiber.StatusNotFound).JSON(resp)
}

// listCycles returns the retained compaction cycles, newest first (#1162).
func (h *CompactionHandler) listCycles(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	// Same fallback as getHistory: a garbage or non-positive limit falls back
	// to the default rather than erroring.
	limit, err := strconv.Atoi(c.Query("limit", "10"))
	if err != nil || limit < 1 {
		limit = 10
	}
	if limit > compaction.CycleHistoryLimit {
		limit = compaction.CycleHistoryLimit
	}

	// One snapshot, so the page cannot disagree with the window or the
	// running-cycle id reported beside it.
	page := h.manager.CyclePage(limit)
	cycles := make([]fiber.Map, 0, len(page.Cycles))
	for _, rec := range page.Cycles {
		cycles = append(cycles, cycleResponseWithPartitions(rec, false))
	}

	resp := fiber.Map{
		"cycles":   cycles,
		"retained": page.Retained,
	}
	if page.HasRange {
		resp["oldest_retained"] = page.Oldest
		resp["newest_retained"] = page.Newest
	}
	// Derived from the history rather than from the claim, so it can never
	// contradict the cycles listed beside it.
	if page.HasRunning {
		resp["running_cycle_id"] = page.RunningCycleID
	}
	return c.JSON(resp)
}
