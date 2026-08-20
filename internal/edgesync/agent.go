package edgesync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// DefaultMaxAttempts is how many times a file is retried before the ledger
// gives up on it.
//
// Deliberately generous: on an intermittent link most failures are the link,
// not the file, and a spoke that abandons data after two bad contact windows
// is worse than one that keeps trying. A genuinely broken file (a checksum
// mismatch that reproduces) still stops after this many.
const DefaultMaxAttempts = 5

// MaxAllowedConcurrent caps simultaneous transfers regardless of configuration.
//
// Each transfer holds an open file handle and an io.Pipe, and a spoke is by
// definition a small machine. An operator typo in max_concurrent should not be
// able to exhaust its file descriptors.
const MaxAllowedConcurrent = 64

// Agent runs one sync pass: discover local files, ask the hub what it is
// missing, and stream those files to it.
//
// This is the manual half of the design. §8.2 describes a connectivity-adaptive
// background loop, but that is the Enterprise feature (§9) — here the pass is
// triggered by an operator and runs once. The internals are the same either
// way, so phase 2 adds a ticker and a license gate rather than a rewrite.
type Agent struct {
	ledger    *Ledger
	transport SyncTransport
	backend   storage.Backend
	hubID     string
	spokeID   string
	logger    zerolog.Logger

	maxAttempts   int
	maxConcurrent int
	batchSize     int

	// compactionDeferEpoch is forwarded to the per-pass Discoverer so it can
	// tell crash-orphaned compacted outputs from legacy ones (issue #610).
	compactionDeferEpoch time.Time

	// namespaceExcluder is forwarded to the per-pass Discoverer on dual-role
	// nodes (see Discoverer.SetNamespaceExcluder).
	namespaceExcluder func(ctx context.Context) (map[string]struct{}, error)
}

// SetNamespaceExcluder forwards the dual-role received-namespace exclusion
// to this agent's discovery passes. nil deactivates it.
func (a *Agent) SetNamespaceExcluder(fn func(ctx context.Context) (map[string]struct{}, error)) {
	a.namespaceExcluder = fn
}

// SetCompactionDeferEpoch forwards the issue-#610 enablement epoch to this
// agent's discovery passes. Zero deactivates the discrimination.
func (a *Agent) SetCompactionDeferEpoch(epoch time.Time) {
	a.compactionDeferEpoch = epoch
}

// AgentConfig configures a sync Agent.
type AgentConfig struct {
	Ledger    *Ledger
	Transport SyncTransport
	Backend   storage.Backend

	// HubID names the hub this agent syncs to. Keyed into every ledger row, so
	// changing it starts a fresh sync history rather than resuming another
	// hub's.
	HubID string

	// SpokeID is this edge instance's identity, as registered on the hub.
	SpokeID string

	// MaxAttempts before a file is marked failed. Zero uses DefaultMaxAttempts.
	MaxAttempts int

	// MaxConcurrent bounds simultaneous transfers. Zero means 2 — edge boxes
	// are small, and §8.2 caps this low deliberately.
	MaxConcurrent int

	// BatchSize caps how many files one reconcile asks about; a larger
	// backlog pages. Zero offers the whole backlog in one reconcile — a page
	// the hub refuses as too large is split and retried either way.
	BatchSize int

	Logger zerolog.Logger
}

// RunResult summarizes one sync pass.
type RunResult struct {
	// Discovered is how many local files were newly added to the ledger.
	Discovered int

	// Recovered is how many interrupted transfers were reset to pending.
	//
	// int like its sibling counters, though the ledger reports RowsAffected as
	// int64 — a RunResult is a per-pass summary, and a mixed-width struct is
	// awkward for every caller that formats it.
	Recovered int

	// AlreadyPresent is how many files the hub reported it already had —
	// the lost-acknowledgment path, resolved without sending bytes.
	AlreadyPresent int

	// Sent is how many files were transferred and acknowledged this pass.
	Sent int

	// BytesSent counts only bytes actually put on the wire, so a resumed
	// transfer contributes its tail rather than the whole file.
	BytesSent int64

	// Partial is how many transfers ended mid-file. Not failures: each left a
	// resume checkpoint and continues on the next pass.
	Partial int

	// Failed is how many transfers errored.
	Failed int

	// Skipped is how many entries were dropped because their source file
	// vanished (compaction or retention) before delivery.
	Skipped int

	// Conflicts are same-path-different-content disagreements. These need an
	// operator, not a retry, so they are surfaced rather than counted away.
	Conflicts []Conflict

	Duration time.Duration
}

// NewAgent validates configuration and returns a ready Agent.
func NewAgent(cfg AgentConfig) (*Agent, error) {
	if cfg.Ledger == nil {
		return nil, errors.New("edgesync: agent requires a ledger")
	}
	if cfg.Transport == nil {
		return nil, errors.New("edgesync: agent requires a transport")
	}
	if cfg.Backend == nil {
		return nil, errors.New("edgesync: agent requires a storage backend")
	}
	if err := validateSpokeID(cfg.SpokeID); err != nil {
		return nil, fmt.Errorf("edgesync: agent spoke ID: %w", err)
	}

	hubID := cfg.HubID
	if hubID == "" {
		hubID = DefaultHubID
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 2
	}
	if maxConcurrent > MaxAllowedConcurrent {
		// An edge box is the machine least able to absorb a goroutine and an
		// open file handle per pending file. Clamp rather than fail: a typo in
		// a field like this should not stop a spoke from syncing at all.
		maxConcurrent = MaxAllowedConcurrent
	}
	// Clamped like its siblings above. A negative value reaches make()'s
	// capacity argument in the paging loop and panics the process on the first
	// pass — a crash on the edge box, for a value an operator can set in
	// arc.toml.
	batchSize := cfg.BatchSize
	if batchSize < 0 {
		batchSize = 0
	}

	return &Agent{
		ledger:        cfg.Ledger,
		transport:     cfg.Transport,
		backend:       cfg.Backend,
		hubID:         hubID,
		spokeID:       cfg.SpokeID,
		logger:        cfg.Logger,
		maxAttempts:   maxAttempts,
		maxConcurrent: maxConcurrent,
		batchSize:     batchSize,
	}, nil
}

// Run performs one sync pass.
//
// The order matters and follows §8.2: recover interrupted transfers, discover
// new files, reconcile the whole backlog in one round-trip, then stream what
// the hub is missing — newest first, so that if a contact window closes
// mid-backlog the freshest telemetry has already landed.
func (a *Agent) Run(ctx context.Context) (*RunResult, error) {
	start := time.Now()
	res := &RunResult{}

	// A transfer that was in flight when the process died cannot still be
	// running, so reset it before discovery. Re-sending a file the hub already
	// committed is harmless — it answers AlreadyPresent.
	recovered, err := a.ledger.RecoverInFlight(ctx)
	if err != nil {
		return nil, fmt.Errorf("edgesync: recover interrupted transfers: %w", err)
	}
	res.Recovered = int(recovered)

	discovered, err := a.Discover(ctx)
	if err != nil {
		return nil, err
	}
	res.Discovered = discovered

	// Page until the backlog drains. A pass that moved only the first batch
	// and reported success would silently strand the rest — and the case that
	// matters most, a spoke returning from a long outage, is exactly the one
	// with more pending files than a batch holds.
	//
	// Keyset pagination on Pending's stable order (partition_time DESC, id
	// ASC): each page starts strictly after the previous page's last row, so
	// rows the pass leaves unresolved — a conflict, an exhausted retry, a
	// partial transfer — sit behind the cursor and cannot hide fresh rows or
	// be re-offered within the pass. The old offered-map approach bought the
	// same property by re-fetching every previously-offered row on every
	// page: O(pages²) row scans across a large backlog.
	var cursor *LedgerEntry
	for {
		pending, err := a.ledger.PendingPage(ctx, a.hubID, a.batchSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("edgesync: read pending: %w", err)
		}
		if len(pending) == 0 {
			break
		}
		cursor = pending[len(pending)-1]

		if err := a.runBatch(ctx, pending, res); err != nil {
			return nil, err
		}
		if a.batchSize <= 0 {
			// No page size means the whole backlog came in one page.
			break
		}
	}

	res.Duration = time.Since(start)
	return res, nil
}

// runBatch reconciles one page of pending files and sends what the hub lacks.
//
// A page the hub refuses as too large (413) is split and retried rather than
// failing the pass: the spoke has no reliable way to learn the hub's caps up
// front (batch_size is operator-set, and the byte limit depends on path
// lengths), so the refusal itself is the negotiation. Every split strictly
// shrinks the page, and a single-entry page that still gets refused is a real
// error — so this terminates.
func (a *Agent) runBatch(ctx context.Context, pending []*LedgerEntry, res *RunResult) error {
	queue := [][]*LedgerEntry{pending}
	for len(queue) > 0 {
		page := queue[0]
		queue = queue[1:]

		err := a.reconcileAndSend(ctx, page, res)
		if err == nil {
			continue
		}
		var tooLarge *ReconcileTooLargeError
		if !errors.As(err, &tooLarge) || len(page) <= 1 {
			return err
		}
		size := tooLarge.MaxEntries
		if size <= 0 || size >= len(page) {
			// Byte-limit refusal (no advertised cap), or an entry cap the
			// page already satisfies: halving is the only signal available.
			size = len(page) / 2
		}
		a.logger.Warn().
			Int("page_entries", len(page)).
			Int("retrying_at", size).
			Msg("Hub refused reconcile page as too large; splitting and retrying")
		for start := 0; start < len(page); start += size {
			end := start + size
			if end > len(page) {
				end = len(page)
			}
			queue = append(queue, page[start:end])
		}
	}
	return nil
}

// reconcileAndSend performs one reconcile round-trip and the transfers it
// prescribes.
func (a *Agent) reconcileAndSend(ctx context.Context, pending []*LedgerEntry, res *RunResult) error {
	// One round-trip for the whole backlog. This is the property that makes a
	// long disconnection survivable: 5,000 pending files cost one request.
	reconciled, err := a.transport.Reconcile(ctx, a.hubID, pending)
	if err != nil {
		var tooLarge *ReconcileTooLargeError
		if errors.As(err, &tooLarge) {
			// Preserved un-wrapped for runBatch's split-and-retry.
			return err
		}
		return fmt.Errorf("edgesync: reconcile: %w", err)
	}
	if err := reconciled.Validate(); err != nil {
		return fmt.Errorf("edgesync: hub returned an invalid reconcile result: %w", err)
	}

	// Files the hub already holds are advanced without sending a byte. This is
	// the lost-ack path: a transfer that completed but whose acknowledgment
	// never arrived is resolved here in bulk.
	for _, p := range reconciled.Present {
		if err := a.ledger.MarkSynced(ctx, a.hubID, p); err != nil {
			// Not fatal: the file is on the hub either way, and the next pass
			// re-discovers the discrepancy. Failing the run would strand the
			// files still queued behind it.
			a.logger.Warn().Err(err).Str("path", p).Msg("Could not mark a hub-confirmed file as synced")
			continue
		}
		res.AlreadyPresent++
	}

	// Conflicts need a human. Surfacing them beats retrying: the same path
	// holding different content means a spoke-ID collision or corruption, and
	// re-sending would either be refused or destroy evidence.
	// Appended, not assigned: a pass spans several pages, and overwriting
	// would report only the last page's conflicts.
	res.Conflicts = append(res.Conflicts, reconciled.Conflicts...)
	for _, c := range reconciled.Conflicts {
		a.logger.Error().
			Str("path", c.Path).
			Str("hub_sha256", c.TheirSHA256).
			Msg("Sync conflict: the hub holds different content at this path; not retrying")

		// Marked failed for the same reason the transfer path does it
		// (see sendOne): a content disagreement needs a human, and retrying
		// cannot resolve it. Left pending, a conflict would ride along in
		// every future reconcile payload forever — unbounded growth on a
		// spoke with a permanent conflict, and the two conflict paths would
		// disagree about whether the same condition is terminal.
		if err := a.ledger.MarkConflicted(ctx, a.hubID, c.Path, "conflict: hub holds different content"); err != nil {
			a.logger.Warn().Err(err).Str("path", c.Path).Msg("Could not mark a conflicted file failed")
		}
	}

	// Newest-first. If the link drops mid-backlog, the most recent telemetry
	// has already reached the hub and backfill catches up next pass — Pending
	// already returns this order, so preserve it rather than re-sorting.
	//
	// Set membership, not a linear scan per entry: at the 10,000-entry batch
	// the hub allows, a nested loop would be 100M comparisons for a step that
	// should be free.
	missingSet := make(map[string]struct{}, len(reconciled.Missing))
	for _, p := range reconciled.Missing {
		missingSet[p] = struct{}{}
	}
	missing := make([]*LedgerEntry, 0, len(reconciled.Missing))
	for _, e := range pending {
		if _, ok := missingSet[e.Path]; ok {
			missing = append(missing, e)
		}
	}

	a.sendAll(ctx, missing, res)
	return nil
}

// Discover walks local storage and adds files the ledger does not know about.
//
// §8.2 says to walk the manifest, which does not exist on a standalone spoke —
// and even in cluster mode it is a different subsystem's index. Walking storage
// works identically everywhere, and compaction output and backfilled files just
// appear as new rows on the next pass.
func (a *Agent) Discover(ctx context.Context) (int, error) {
	d, err := NewDiscoverer(a.ledger, a.backend, a.hubID, a.logger)
	if err != nil {
		return 0, err
	}
	d.SetCompactionDeferEpoch(a.compactionDeferEpoch)
	d.SetNamespaceExcluder(a.namespaceExcluder)
	return d.Discover(ctx)
}

// sendAll streams the missing files, bounded by MaxConcurrent.
func (a *Agent) sendAll(ctx context.Context, missing []*LedgerEntry, res *RunResult) {
	type outcome struct {
		sent      bool
		partial   bool
		skipped   bool
		failed    bool
		bytesSent int64
	}

	sem := make(chan struct{}, a.maxConcurrent)
	results := make(chan outcome, len(missing))

	for _, e := range missing {
		// Checked before the select, not as one of its cases. With both a
		// cancelled context and a free semaphore slot ready, select picks
		// uniformly at random — so a cancelled pass would still launch about
		// half its remaining transfers, which is the opposite of "stop".
		if ctx.Err() != nil {
			// The contact window closed. Whatever is in flight finishes or
			// fails on its own, and its ledger state is correct either way.
			results <- outcome{}
			continue
		}
		select {
		case <-ctx.Done():
			results <- outcome{}
			continue
		case sem <- struct{}{}:
		}

		go func(entry *LedgerEntry) {
			defer func() { <-sem }()
			sent, partial, skipped, n, err := a.sendOne(ctx, entry)
			results <- outcome{sent: sent, partial: partial, skipped: skipped, failed: err != nil, bytesSent: n}
		}(e)
	}

	for range missing {
		o := <-results
		switch {
		case o.sent:
			res.Sent++
		case o.partial:
			res.Partial++
		case o.skipped:
			res.Skipped++
		case o.failed:
			res.Failed++
		}
		res.BytesSent += o.bytesSent
	}
}

// sendOne transfers a single file, resuming from its checkpoint if it has one.
func (a *Agent) sendOne(ctx context.Context, e *LedgerEntry) (sent, partial, skipped bool, bytesSent int64, err error) {
	if err := a.ledger.MarkInFlight(ctx, a.hubID, e.Path); err != nil {
		// Usually a concurrent pass already claimed it. Not an error worth
		// failing the run over.
		a.logger.Debug().Err(err).Str("path", e.Path).Msg("Could not claim a file for transfer")
		return false, false, false, 0, err
	}

	offset := e.BytesSent
	body, err := a.openAt(ctx, e.Path, offset)
	if err != nil {
		if a.skipIfVanished(ctx, e) {
			return false, false, true, 0, nil
		}
		a.fail(ctx, e, fmt.Sprintf("open: %v", err))
		return false, false, false, 0, err
	}
	defer body.Close()

	res, err := a.transport.PutFile(ctx, a.hubID, e, body, offset)
	if err != nil {
		// openAt streams through an io.Pipe, so a source file deleted by
		// compaction or retention surfaces HERE, as a transfer error, not at
		// open. Without this check the row burns its whole retry budget on a
		// file that no longer exists and then sits terminally failed.
		if a.skipIfVanished(ctx, e) {
			return false, false, true, 0, nil
		}
		// A resume the hub cannot honour: it no longer holds the prefix our
		// checkpoint refers to. That happens when a hub restarts, sweeps its
		// staging area, or runs on a backend that cannot append at all. The
		// file is fine — only the checkpoint is stale — so clear it and let
		// the next pass start from zero. Without this the agent would retry
		// the same impossible offset until it gave up on healthy data.
		if isStaleCheckpoint(err) && offset > 0 {
			a.logger.Info().
				Str("path", e.Path).
				Int64("stale_offset", offset).
				Msg("Hub no longer holds our partial; restarting this file from the beginning")
			if resetErr := a.ledger.RecordProgress(ctx, a.hubID, e.Path, 0); resetErr != nil {
				a.logger.Warn().Err(resetErr).Str("path", e.Path).Msg("Could not clear a stale resume checkpoint")
			}
		}
		a.fail(ctx, e, err.Error())
		return false, false, false, 0, err
	}
	// A hub is remote code as far as the spoke is concerned; an inconsistent
	// answer must not become ledger state.
	if err := res.Validate(e); err != nil {
		a.fail(ctx, e, fmt.Sprintf("invalid hub response: %v", err))
		return false, false, false, 0, err
	}

	switch {
	case res.Outcome.Done():
		if err := a.ledger.MarkSynced(ctx, a.hubID, e.Path); err != nil {
			return false, false, false, 0, err
		}
		// A file the hub already had cost no bytes; only count a real transfer.
		if res.Outcome == OutcomeCommitted {
			return true, false, false, res.BytesAccepted - offset, nil
		}
		return true, false, false, 0, nil

	case res.Outcome == OutcomePartial:
		// Record the hub's offset, not our own guess: the two can disagree,
		// and appending at the wrong place would corrupt the file.
		if err := a.ledger.RecordProgress(ctx, a.hubID, e.Path, res.BytesAccepted); err != nil {
			a.logger.Warn().Err(err).Str("path", e.Path).Msg("Could not record a resume checkpoint")
		}
		a.fail(ctx, e, "transfer ended early; will resume")
		return false, true, false, res.BytesAccepted - offset, nil

	case res.Outcome == OutcomeConflict:
		// Terminal by design. Retrying cannot resolve a content disagreement,
		// and MarkFailed with a cap of 1 stops it immediately rather than
		// burning attempts on something a human must look at.
		a.logger.Error().
			Str("path", e.Path).
			Str("hub_sha256", res.TheirSHA256).
			Msg("Sync conflict on transfer; the hub holds different content")
		if err := a.ledger.MarkFailed(ctx, a.hubID, e.Path, "conflict: hub holds different content", 1); err != nil {
			a.logger.Warn().Err(err).Str("path", e.Path).Msg("Could not mark a conflicted file failed")
		}
		return false, false, false, 0, nil

	default:
		// Checksum mismatch or backpressure: retryable, so let the attempt
		// counter decide when to give up.
		a.fail(ctx, e, string(res.Outcome))
		return false, false, false, 0, nil
	}
}

// isStaleCheckpoint reports whether an error means the hub cannot resume from
// the offset we hold.
//
// Matched on the error text because the condition arises inside a transport
// implementation rather than from a sentinel the interface defines — a real
// HTTP transport surfaces it as a status code, and a future one may not use
// either. Treated as a hint: the worst case of a false negative is the
// pre-existing behaviour, and of a false positive is one redundant full
// re-send of a file that was going to be retried anyway.
func isStaleCheckpoint(err error) bool {
	if errors.Is(err, storage.ErrResumeNotSupported) {
		return true
	}
	return strings.Contains(err.Error(), "no staged prefix")
}

// fail records a transfer failure against the attempt cap.
func (a *Agent) fail(ctx context.Context, e *LedgerEntry, msg string) {
	if err := a.ledger.MarkFailed(ctx, a.hubID, e.Path, msg, a.maxAttempts); err != nil {
		a.logger.Warn().Err(err).Str("path", e.Path).Msg("Could not record a transfer failure")
	}
}

// skipIfVanished checks whether a transfer failed because the source file no
// longer exists (compaction or retention deleted it after discovery), and if
// so marks the entry skipped. Returns true when the entry was skipped.
//
// The direction of the check is deliberate: Exists must POSITIVELY report the
// file gone. On an Exists error nothing is skipped — a transient storage
// error must never terminally skip a file that still exists, so uncertainty
// falls through to the normal retry path.
func (a *Agent) skipIfVanished(ctx context.Context, e *LedgerEntry) bool {
	present, err := a.backend.Exists(ctx, e.Path)
	if err != nil || present {
		return false
	}
	if err := a.ledger.MarkSkipped(ctx, a.hubID, e.Path,
		"source file removed before delivery (compaction or retention)"); err != nil {
		a.logger.Warn().Err(err).Str("path", e.Path).Msg("Could not mark a vanished file skipped")
		return false
	}
	a.logger.Info().
		Str("path", e.Path).
		Msg("Source file vanished before delivery; marked skipped (compaction or retention removed it)")
	return true
}

// openAt returns a reader positioned at offset.
//
// Streams rather than buffering: a compacted Parquet file can be hundreds of
// megabytes, and an edge box is the machine least able to hold one in memory.
func (a *Agent) openAt(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		var err error
		if offset > 0 {
			err = a.backend.ReadToAt(ctx, path, pw, offset)
		} else {
			err = a.backend.ReadTo(ctx, path, pw)
		}
		_ = pw.CloseWithError(err)
	}()

	return pr, nil
}

// UnfinishedEntries returns files that have not reached the hub — those still
// queued and those that gave up — with the given-up ones first.
//
// The troubleshooting view. It deliberately includes failed entries: a file
// that exhausted its retries is the one an operator most needs to see, and a
// pending-only view would hide exactly that.
//
// Exposed through the agent rather than by handing callers the ledger: the
// handler has no business driving state transitions, and routing reads through
// here keeps the ledger's mutating API out of the HTTP layer entirely.
func (a *Agent) UnfinishedEntries(ctx context.Context, limit int) ([]*LedgerEntry, error) {
	return a.ledger.Unfinished(ctx, a.hubID, limit)
}

// RequeueFailed returns failed (and operator-dismissed) rows to pending.
func (a *Agent) RequeueFailed(ctx context.Context, path string) (int64, error) {
	return a.ledger.RequeueFailed(ctx, a.hubID, path)
}

// DismissFailed moves failed rows to operator-dismissed skipped.
func (a *Agent) DismissFailed(ctx context.Context, path string) (int64, error) {
	return a.ledger.DismissFailed(ctx, a.hubID, path)
}

// EntriesByState lists ledger rows in one explicit state.
func (a *Agent) EntriesByState(ctx context.Context, state SyncState, limit int) ([]*LedgerEntry, error) {
	return a.ledger.EntriesByState(ctx, a.hubID, state, limit)
}

// Status reports what the spoke still has to send.
func (a *Agent) Status(ctx context.Context) (*Stats, error) {
	return a.ledger.Stats(ctx, a.hubID)
}

// isSyncableFile reports whether a storage path is one the spoke should sync.
//
// The sync unit is an immutable Parquet file. Staging areas and dot-prefixed
// paths are excluded — a spoke syncing the hub's own staging directory, or its
// own, would ship transient state that is not data.
func isSyncableFile(p string) bool {
	if !strings.HasSuffix(p, ".parquet") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if strings.HasPrefix(seg, ".") {
			return false
		}
	}
	return validateSyncPath(p) == nil
}

// parseArcPath extracts the Arc namespace from a storage path.
//
// Layout is {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/file.parquet.
// Returns zero values for a path that does not carry them, which is safe: the
// fields are metadata for operators, not part of the sync identity.
func parseArcPath(p string) (database, measurement string, partition time.Time) {
	parts := strings.Split(p, "/")
	if len(parts) >= 2 {
		database, measurement = parts[0], parts[1]
	}
	if len(parts) >= 7 {
		if t, err := time.Parse("2006/01/02/15", parts[2]+"/"+parts[3]+"/"+parts[4]+"/"+parts[5]); err == nil {
			partition = t.UTC()
		}
	}
	return database, measurement, partition
}
