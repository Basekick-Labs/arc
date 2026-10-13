package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
)

// Applying DELETE records to pending files (#1207 d1).
//
// Every file that registers with batch coverage goes through the outbox. A
// file holding batches a live DELETE record covers is rewritten here, before
// it registers, without the rows the record's WHERE matches: a claimant's or
// republisher's replay, a WAL recovery, a writer's own flush of rows it
// acknowledged before the DELETE. The manifest refuses one that was not
// (raft.RefusedDeleted); it then comes back here. The rewrite is the
// DELETE's own (same SQL over the same Parquet), so a replayed row is
// deleted exactly when the DELETE would have deleted it.

// DeleteRewriteResult is what applying records to one file did. Matched rows
// were removed; Left remain. When some rows matched and some remain, the
// survivors are in a new file (NewPath); the old one is untouched.
type DeleteRewriteResult struct {
	Matched int64
	Left    int64
	NewPath string
	Size    int64
	SHA256  string
}

// DeleteRewriter rewrites the local Parquet file at path without the rows
// any of wheres matches ("" matches every row).
type DeleteRewriter func(ctx context.Context, path string, wheres []string) (DeleteRewriteResult, error)

// ErrDeleteRewriteBusy: a DELETE is rewriting files; try again later.
var ErrDeleteRewriteBusy = errors.New("a delete is rewriting files")

// DeleteRecordHooks connect the outbox to the manifest's DELETE records and
// to the rewriter. Records, Settle, Instance and Remove are the coordinator's;
// Rewrite is the DELETE handler's.
type DeleteRecordHooks struct {
	Records  func(cov []raft.InstanceCoverage, after uint64) ([]raft.DeleteRecord, bool)
	Settle   func(ctx context.Context, p raft.SettleDeletedPayload) error
	Instance func() uint64
	Remove   func(ctx context.Context, path string) error
	Rewrite  DeleteRewriter
}

// SetDeleteRecordHooks installs the hooks. Until they are installed, a file
// the manifest refuses as deleted waits.
func (r *RegistrationOutbox) SetDeleteRecordHooks(h *DeleteRecordHooks) {
	r.deleteHooks.Store(h)
}

type applyOutcome int

const (
	applyRegister applyOutcome = iota // register it (DeleteSeen may have moved)
	applyDefer                        // try again later
	applyGone                         // the entry was replaced or withdrawn
)

// errDeletedRefusal is the attempt error when the only refusals were DELETE
// records this node had not applied yet: the backoff paces the retry.
var errDeletedRefusal = errors.New("file registrations wait for DELETE records to be applied to them")

// applyDeleteRecords applies the live records covering e's batches that were
// not applied to it yet. e is in flight (inside an attempt).
func (r *RegistrationOutbox) applyDeleteRecords(ctx context.Context, e *outboxEntry, cov []raft.InstanceCoverage) applyOutcome {
	h := r.deleteHooks.Load()
	if h == nil || h.Records == nil || len(cov) == 0 {
		return applyRegister
	}
	recs, straddle := h.Records(cov, e.rec.DeleteSeen)
	if len(recs) == 0 {
		return applyRegister
	}
	if straddle {
		// A flush never puts batches in and out of one record's scope in one
		// file; holding it is the only safe answer if one ever does.
		r.logger.Error().Str("path", e.rec.Path).
			Msg("A pending file holds batches on both sides of a DELETE and cannot be rewritten with it; it waits")
		return applyDefer
	}
	if h.Rewrite == nil {
		r.logger.Debug().Str("path", e.rec.Path).Msg("A pending file waits for the DELETE rewriter to be installed")
		return applyDefer
	}
	wheres := make([]string, 0, len(recs))
	ids := make([]uint64, 0, len(recs))
	for _, rec := range recs {
		wheres = append(wheres, rec.Where)
		ids = append(ids, rec.ID)
	}
	seen := ids[len(ids)-1]
	res, err := h.Rewrite(ctx, e.rec.Path, wheres)
	if err != nil {
		if !errors.Is(err, ErrDeleteRewriteBusy) {
			r.logger.Warn().Err(err).Str("path", e.rec.Path).Msg("Applying DELETE records to a pending file failed; retrying")
		}
		return applyDefer
	}
	switch {
	case res.Matched == 0:
		r.mu.Lock()
		e.rec.DeleteSeen = seen
		r.mu.Unlock()
		return applyRegister
	case res.Left > 0:
		return r.replaceWithFiltered(ctx, h, e, res, seen)
	default:
		return r.withdrawDeleted(ctx, h, e, cov, ids)
	}
}

// replaceWithFiltered puts the rewritten file in e's place, as ReplacePending
// does for a DELETE: same coverage, the old path superseded.
func (r *RegistrationOutbox) replaceWithFiltered(ctx context.Context, h *DeleteRecordHooks, e *outboxEntry, res DeleteRewriteResult, seen uint64) applyOutcome {
	old := e.rec.Path
	r.mu.Lock()
	rec := e.rec
	rec.Path, rec.SizeBytes, rec.SHA256 = res.NewPath, res.Size, res.SHA256
	rec.Supersedes, rec.DeleteSeen = old, seen
	rec.Version = maxInt(rec.Version, journalRecordVersionCoverage)
	r.withdrawLocked(e)
	r.enqueueLocked(rec, e.added, true)
	r.appendLocked(rec)
	r.advanceHeadLocked()
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		r.logger.Error().Err(err).Str("path", res.NewPath).Str("replaces", old).
			Msg("Failed to persist a pending file rewritten for a DELETE; a crash before it registers leaves the rewrite unannounced")
		return applyGone
	}
	if h.Remove != nil {
		if err := h.Remove(ctx, old); err != nil {
			r.logger.Warn().Err(err).Str("path", old).Msg("Could not remove a pending file a DELETE rewrite replaced")
		}
	}
	r.logger.Info().Str("path", old).Str("rewrite", res.NewPath).Int64("deleted", res.Matched).
		Msg("Pending file rewritten without rows a DELETE removed")
	return applyGone
}

// withdrawDeleted takes out a pending file a DELETE removed every row of. Its
// batches settle: through this writer's own floors for its current
// instance, through the manifest (SettleDeleted) for any other.
func (r *RegistrationOutbox) withdrawDeleted(ctx context.Context, h *DeleteRecordHooks, e *outboxEntry, cov []raft.InstanceCoverage, ids []uint64) applyOutcome {
	var self uint64
	if h.Instance != nil {
		self = h.Instance()
	}
	hour := e.rec.PartitionTime.UTC().Truncate(time.Hour).Unix()
	for _, c := range cov {
		if c.Instance == self && self != 0 {
			continue
		}
		if h.Settle == nil {
			return applyDefer
		}
		sctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
		err := h.Settle(sctx, raft.SettleDeletedPayload{By: r.nodeID, Hour: hour, Coverage: c, Records: ids})
		cancel()
		if err != nil {
			r.logger.Warn().Err(err).Str("path", e.rec.Path).Msg("Settling batches a DELETE removed every row of failed; retrying")
			return applyDefer
		}
	}
	r.mu.Lock()
	r.withdrawLocked(e)
	r.advanceHeadLocked()
	gcov := e.rec.Cov
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		r.logger.Warn().Err(err).Str("path", e.rec.Path).Msg("Failed to persist a withdrawn file registration")
	}
	gcov.Settle(e.rec.Path, ingest.CoverageWithdrawn)
	if h.Remove != nil {
		if err := h.Remove(ctx, e.rec.Path); err != nil {
			r.logger.Warn().Err(err).Str("path", e.rec.Path).Msg("Could not remove a pending file a DELETE emptied")
		}
	}
	r.logger.Info().Str("path", e.rec.Path).Msg("Pending file withdrawn: a DELETE removed every row of it")
	return applyGone
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// combineDeleteWheres is the WHERE matching a row any of wheres matches ("" =
// every row).
func combineDeleteWheres(wheres []string) string {
	parts := make([]string, 0, len(wheres))
	for _, w := range wheres {
		if strings.TrimSpace(w) == "" {
			return "TRUE"
		}
		parts = append(parts, "("+w+")")
	}
	return strings.Join(parts, " OR ")
}

// CombineDeleteWheres is combineDeleteWheres for the rewriter.
func CombineDeleteWheres(wheres []string) string { return combineDeleteWheres(wheres) }

// proposeSettleDeleted proposes a SettleDeleted payload through the leader.
func (c *Coordinator) proposeSettleDeleted(ctx context.Context, p raft.SettleDeletedPayload) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandSettleDeleted, Payload: raw}); err != nil {
		return fmt.Errorf("settle deleted: %w", err)
	}
	return nil
}

// WithdrawDatabase withdraws every pending registration of database (a
// database deletion): none registers after the files are gone, and their
// batches settle as deleted on purpose rather than lost.
func (r *RegistrationOutbox) WithdrawDatabase(ctx context.Context, database string, record uint64) error {
	r.mu.Lock()
	var paths []string
	for p, e := range r.byPath {
		if !e.done && e.rec.Database == database {
			paths = append(paths, p)
		}
	}
	r.mu.Unlock()
	var errs []error
	for _, p := range paths {
		if _, _, err := r.WithdrawForDelete(ctx, p, record); err != nil && !errors.Is(err, ErrRegistrationParked) {
			errs = append(errs, fmt.Errorf("%s: %w", p, err))
		}
	}
	if record == 0 {
		// A parked file may be the only remaining copy of acknowledged rows.
		// Without a durable DELETE record there is no proof those rows belong
		// to this database deletion, so keep the file and make the caller retry
		// only after the cluster can record the deletion.
		r.mu.Lock()
		for p, e := range r.parked {
			if e.rec.Rec.Database == database {
				errs = append(errs, fmt.Errorf("%s: parked data has no durable database-delete record", p))
			}
		}
		r.mu.Unlock()
		return errors.Join(errs...)
	}
	r.mu.Lock()
	parked := make([]string, 0)
	for p, e := range r.parked {
		if e.rec.Rec.Database == database {
			parked = append(parked, p)
		}
	}
	r.mu.Unlock()
	for _, p := range parked {
		if err := r.withdrawParkedForDelete(ctx, p, record); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", p, err))
		}
	}
	return errors.Join(errs...)
}

// withdrawParkedForDelete removes a parked file only after a durable database
// DELETE covers all of its batches. Its journal intent is fsynced before the
// unlink, so a restart can finish the operation idempotently.
func (r *RegistrationOutbox) withdrawParkedForDelete(ctx context.Context, path string, record uint64) error {
	h := r.deleteHooks.Load()
	if record == 0 || h == nil || h.Records == nil || h.Remove == nil {
		return errors.New("a durable database-delete record and storage hooks are required to remove parked data")
	}
	r.mu.Lock()
	e, ok := r.parked[path]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if pending, ok := r.byPath[path]; ok && !pending.done {
		r.mu.Unlock()
		return errors.New("parked registration is still in flight; retry the database deletion")
	}
	pr := e.rec
	r.mu.Unlock()
	if pr.DeleteRecord != 0 && pr.DeleteRecord != record {
		return fmt.Errorf("parked file already has delete record %d", pr.DeleteRecord)
	}
	if pr.Rec.Cov == nil {
		return errors.New("parked file has no batch coverage; preserving it because the delete scope cannot be verified")
	}
	cov := fileCoverage(pr.Rec.Cov)
	records, _ := h.Records(cov, 0)
	var matched *raft.DeleteRecord
	for i := range records {
		if records[i].ID == record && records[i].Database == pr.Rec.Database && records[i].Measurement == "" && records[i].Where == "" {
			matched = &records[i]
			break
		}
	}
	if matched == nil {
		return fmt.Errorf("delete record %d does not durably cover parked file %s", record, path)
	}
	for _, c := range cov {
		if !matched.Covers(c) {
			return fmt.Errorf("parked file %s has batches outside database-delete record %d; preserving it", path, record)
		}
	}
	var self uint64
	if h.Instance != nil {
		self = h.Instance()
	}
	hour := pr.Rec.PartitionTime.UTC().Truncate(time.Hour).Unix()
	for _, c := range cov {
		if c.Instance == self && self != 0 {
			continue
		}
		if h.Settle == nil {
			return errors.New("the delete settler is not installed; preserving parked data")
		}
		sctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
		err := h.Settle(sctx, raft.SettleDeletedPayload{By: r.nodeID, Hour: hour, Coverage: c, Records: []uint64{record}})
		cancel()
		if err != nil {
			return fmt.Errorf("settle parked batches for delete record %d: %w", record, err)
		}
	}

	r.parkMu.Lock()
	defer r.parkMu.Unlock()
	r.mu.Lock()
	e, ok = r.parked[path]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if e.rec.DeleteRecord != 0 && e.rec.DeleteRecord != record {
		r.mu.Unlock()
		return fmt.Errorf("parked file already has delete record %d", e.rec.DeleteRecord)
	}
	if e.rec.DeleteRecord == 0 {
		pr = e.rec
		pr.Op = parkOpDeleting
		pr.DeleteRecord = record
		if err := r.appendRefused(pr); err != nil {
			r.mu.Unlock()
			return fmt.Errorf("persist parked delete intent: %w", err)
		}
		e.rec = pr
	}
	r.mu.Unlock()

	removePath := e.rec.Rec.Path
	if e.rec.Held != "" {
		removePath = e.rec.Held
	}
	if err := h.Remove(ctx, removePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove parked file %s: %w", removePath, err)
	}
	done := e.rec
	done.Op = parkOpResolved
	done.At = time.Now().UTC()
	if err := r.appendRefused(done); err != nil {
		return fmt.Errorf("persist parked delete completion: %w", err)
	}
	r.mu.Lock()
	delete(r.parked, path)
	r.holdGen++
	r.resolvedSinceCompact++
	compact := r.resolvedSinceCompact > len(r.parked)+16 || len(r.parked) == 0
	if compact {
		r.resolvedSinceCompact = 0
	}
	r.mu.Unlock()
	if compact {
		if err := r.compactRefused(); err != nil {
			r.logger.Warn().Err(err).Msg("Could not compact the refused registration journal")
		}
	}
	e.rec.Rec.Cov.Settle(path, ingest.CoverageDeleted)
	r.publishMetrics()
	return nil
}

// SetDeleteSeen notes that the pending file at path was written with DELETE
// record applied (a DELETE's own rewrite): the outbox does not apply it again.
func (r *RegistrationOutbox) SetDeleteSeen(path string, record uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.byPath[path]; ok && !e.done && e.rec.DeleteSeen < record {
		e.rec.DeleteSeen = record
	}
}

// WithdrawForDelete is Withdraw for a file DELETE record removed every row
// of: batches of another writer process in it settle as deleted in the
// manifest (this process's own settle by its floors). record 0 is a plain
// Withdraw.
func (r *RegistrationOutbox) WithdrawForDelete(ctx context.Context, path string, record uint64) (bool, ReplaceOutcome, error) {
	h := r.deleteHooks.Load()
	if record == 0 || h == nil || h.Settle == nil {
		return r.Withdraw(ctx, path)
	}
	e, outcome, _, err := r.settleOrLock(ctx, path)
	if e == nil {
		if outcome == ReplaceParked && err == nil {
			err = ErrRegistrationParked
		}
		return false, outcome, err
	}
	e.inFlight = true // r.mu held: no attempt takes it while we settle
	r.mu.Unlock()
	var self uint64
	if h.Instance != nil {
		self = h.Instance()
	}
	hour := e.rec.PartitionTime.UTC().Truncate(time.Hour).Unix()
	for _, c := range fileCoverage(e.rec.Cov) {
		if c.Instance == self && self != 0 {
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
		err := h.Settle(sctx, raft.SettleDeletedPayload{By: r.nodeID, Hour: hour, Coverage: c, Records: []uint64{record}})
		cancel()
		if err != nil {
			// Still claimable then; a replay of them is rewritten with the
			// record while it lives.
			r.logger.Warn().Err(err).Str("path", path).Msg("Settling the batches of a deleted pending file failed")
		}
	}
	r.mu.Lock()
	e.inFlight = false
	if e.done {
		r.mu.Unlock()
		return false, ReplaceNotPending, nil
	}
	r.withdrawLocked(e)
	r.advanceHeadLocked()
	cov := e.rec.Cov
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		r.logger.Warn().Err(err).Str("path", path).Msg("Failed to persist a withdrawn file registration")
	}
	cov.Settle(path, ingest.CoverageWithdrawn)
	r.publishMetrics()
	return true, ReplaceSwapped, nil
}
