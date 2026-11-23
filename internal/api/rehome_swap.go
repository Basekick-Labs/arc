package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/readgate"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
)

// The swap (#1825): a misfiled file, the source, is replaced by the files
// its rows are split into, the outputs, on a standalone node with local
// storage. internal/rehome/registry.go has the states and what reads see in
// each; this file is the order of the steps and what a restart does.
//
//  1. Own the source. Then wait for any DELETE in flight and check that the
//     source is still there, unchanged, and that no other operation already
//     leaves it out of reads (a DELETE that could not remove it, a
//     compaction that merged it).
//  2. Copy and split the source in a scratch directory; check the split.
//  3. Stage: the outputs are made durable under the staging root, which no
//     read globs. Journal: staged.
//  4. Journal: committed. Reads leave out every output by name. Wait for the
//     reads from before that: their SQL has no exclusions, and would read
//     the source beside any output put in place before they bind.
//  5. Move the outputs to their final keys. Tell the query caches.
//  6. Journal: latched. Reads leave out the source. Wait for the reads from
//     before that: their SQL leaves out the outputs and reads the source,
//     and would read nothing once the source is gone.
//  7. Remove the source, make the removal durable, end the job.
//
// A wait that does not end keeps the job where it is: before the moves the
// job is undone (nothing has changed for readers); after the latch the
// source stays, left out of reads, and the wait goes on.

// RehomeSwap is what the swap needs from the rest of the process. Every
// function may be nil where its subsystem is off.
type RehomeSwap struct {
	Registry *rehome.Registry
	// Reads is the count of reads in flight (QueryHandler.Reads).
	Reads *readgate.Gate
	// Refusal returns why this deployment cannot run a swap ("" when it
	// can): a cluster, tiering, edge sync, storage that is not local.
	Refusal func() string
	// RewriteLock runs fn while no DELETE rewrites a file.
	RewriteLock func(fn func())
	// OtherExclusions are the files every OTHER source leaves out of reads
	// of a measurement (compaction's open jobs, DELETE's retired files).
	OtherExclusions func(ctx context.Context, prefix string) ([]string, error)
	// CompactionOpen reports a compaction job open on a measurement.
	CompactionOpen func(prefix string) bool
	// BackupRunning reports a backup or a restore in progress.
	BackupRunning func() bool
	// FilePublished tells the query caches a file is in storage for the
	// measurement's hour; InvalidateCaches drops them all.
	FilePublished    func(database, measurement string, partitionTime time.Time)
	InvalidateCaches func()
	// ClearMisfiled forgets the source in compaction's list of misfiled
	// files once it is gone.
	ClearMisfiled func(path string)
}

const (
	// rehomeReadWait bounds one wait for the reads in flight.
	rehomeReadWait = 15 * time.Minute
	// rehomeLatchedRetry is the pause between waits once a job is latched
	// and reads from before the latch are still in flight.
	rehomeLatchedRetry = 10 * time.Second
)

// SetSwap turns the real swap on. Without it a request that is not a dry run
// is refused.
func (h *RehomeHandler) SetSwap(s *RehomeSwap) { h.swap = s }

// mover is the storage this slice runs on: local, with atomic moves.
type rehomeStore interface {
	storage.Backend
	storage.ObjectMover
}

func (h *RehomeHandler) mover() (rehomeStore, bool) {
	m, ok := h.storage.(rehomeStore)
	return m, ok && h.storage.Type() == "local"
}

// swapRefusal is why a swap cannot run on this deployment, or "".
func (h *RehomeHandler) swapRefusal() string {
	if h.swap == nil || h.swap.Registry == nil {
		return "moving a file's rows is not available on this node; send \"dry_run\": true to see where they belong"
	}
	if _, ok := h.mover(); !ok {
		return "moving a file's rows needs local storage in this version; this node's storage is " + h.storage.Type()
	}
	if h.swap.Refusal != nil {
		if why := h.swap.Refusal(); why != "" {
			return why
		}
	}
	if h.swap.BackupRunning != nil && h.swap.BackupRunning() {
		return "a backup or a restore is running; retry once it has ended"
	}
	return ""
}

// outputName names an output: like an ingest file for an hour, and a daily
// file without the batch part for a day (manifest repair adopts the
// compaction names, and an output must not be adoptable).
func outputName(src rehome.Source, at time.Time) string {
	base := fmt.Sprintf("%s_%s_%09d", src.Measurement, at.Format("20060102_150405"), at.Nanosecond())
	if src.Shape == rehome.ShapeDay {
		return base + "_daily.parquet"
	}
	return base + ".parquet"
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

// waitReads flips the read epoch and waits, up to rehomeReadWait, for every
// read that entered before the flip.
func (h *RehomeHandler) waitReads(ctx context.Context) error {
	if h.swap.Reads == nil {
		return nil
	}
	wctx, cancel := context.WithTimeout(ctx, rehomeReadWait)
	defer cancel()
	return h.swap.Reads.Wait(wctx, h.swap.Reads.Flip())
}

// runSwap is a job's work for a real swap.
func (h *RehomeHandler) runSwap(ctx context.Context, job *rehomeJob) (state, reason string, report *rehomeReport) {
	reg := h.swap.Registry
	store, _ := h.mover()
	fail := func(step string, err error) (string, string, *rehomeReport) {
		if why, refused := rehome.IsRefused(err); refused {
			return "refused", why, nil
		}
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", job.Path).Str("step", step).Msg("Re-home job failed")
		if ctx.Err() != nil {
			return "failed", fmt.Sprintf("the job was stopped while it was %s; nothing was changed", step), nil
		}
		return "failed", fmt.Sprintf("the job failed while it was %s; nothing was changed. The server log has the error under this job's id", step), nil
	}
	refuse := func(why string) (string, string, *rehomeReport) { return "refused", why, nil }

	src, err := rehome.ParseKey(job.Path)
	if err != nil {
		return fail("reading the path", err)
	}
	rec := &rehome.Job{ID: job.ID, Database: src.Database, Measurement: src.Measurement,
		Source: rehome.FileRef{Key: src.Key}, CreatedAt: job.StartedAt}

	// 1. Own the source, then look for everyone else.
	if err := reg.Begin(rec); err != nil {
		return refuse(fmt.Sprintf("%s cannot be moved now: %v", src.Key, err))
	}
	// Until the job is staged, undoing it is forgetting it.
	staged := false
	defer func() {
		if !staged {
			reg.Drop(job.ID)
			_ = store.RemoveAll(context.WithoutCancel(ctx), rehome.StagedDir(job.ID))
		}
	}()
	if h.swap.CompactionOpen != nil && h.swap.CompactionOpen(rec.Prefix()) {
		return refuse("a compaction job is open on this measurement; retry once it has ended")
	}
	var size int64
	var checkErr error
	check := func() {
		size, checkErr = h.storage.StatFile(ctx, src.Key)
		if checkErr == nil && size < 0 {
			checkErr = rehome.Refusef("%s is not in storage any more", src.Key)
		}
		if checkErr != nil || h.swap.OtherExclusions == nil {
			return
		}
		excluded, err := h.swap.OtherExclusions(ctx, rec.Prefix())
		if err != nil {
			checkErr = err
			return
		}
		for _, k := range excluded {
			if k == src.Key {
				checkErr = rehome.Refusef("%s is already left out of reads by another operation (a DELETE that could not remove it, or a compaction that merged it); moving its rows would bring them back", src.Key)
				return
			}
		}
	}
	// Under the rewrite lock: a DELETE that was rewriting the source when
	// the job took ownership has finished, and every later one sees the
	// ownership.
	if h.swap.RewriteLock != nil {
		h.swap.RewriteLock(check)
	} else {
		check()
	}
	if checkErr != nil {
		return fail("checking the file", checkErr)
	}

	// 2. Split in scratch.
	prep, step, err := h.prepare(ctx, src.Key)
	if err != nil {
		return fail(step, err)
	}
	defer prep.close()
	if prep.size != size {
		return fail("copying the file to local disk", rehome.Refusef("%s changed while it was being copied", src.Key))
	}
	plan := prep.plan
	report = &rehomeReport{
		Database: src.Database, Measurement: src.Measurement, Shape: string(src.Shape), Directory: src.Dir,
		TimeType: plan.TimeType, Rows: plan.Rows, Bytes: prep.size,
		Misfiled: plan.Misfiled(), MisfiledRows: plan.MisfiledRows(), RemainderRows: plan.RemainderRows,
		Destinations:      plan.Destinations[:min(len(plan.Destinations), rehomeDestinationsListed)],
		DestinationsTotal: len(plan.Destinations),
	}
	if !plan.Misfiled() {
		return "refused", fmt.Sprintf("every row of %s belongs in the directory it sits in; there is nothing to move", src.Key), report
	}
	outs, err := rehome.Split(ctx, prep.db, plan, prep.local, prep.outDir)
	if err != nil {
		return fail("splitting the file and checking the split", err)
	}
	report.SplitChecked, report.Outputs = true, outs

	// 3. Stage.
	refs := make([]rehome.FileRef, len(outs))
	for i, o := range outs {
		sum, err := fileSHA256(o.LocalPath)
		if err != nil {
			return fail("staging the split", err)
		}
		key := o.Dir + "/" + outputName(src, storage.NameTime(time.Now()))
		if n, err := h.storage.StatFile(ctx, key); err != nil || n >= 0 {
			return fail("staging the split", fmt.Errorf("the output key %s is taken (size %d, err %v)", key, n, err))
		}
		refs[i] = rehome.FileRef{Key: key, Staged: fmt.Sprintf("%s/%06d.parquet", rehome.StagedDir(job.ID), i),
			Size: o.Bytes, SHA256: sum, Start: o.Start, Rows: o.Rows, Remainder: o.Remainder}
	}
	if err := reg.SetOutputs(job.ID, refs); err != nil {
		return refuse(fmt.Sprintf("%s cannot be moved now: %v", src.Key, err))
	}
	rec.Source.Size, rec.Source.SHA256 = prep.size, prep.sha256
	if err := reg.SetSource(job.ID, rec.Source); err != nil {
		return fail("staging the split", err)
	}
	for i, o := range outs {
		if err := stageFile(ctx, store, refs[i].Staged, o.LocalPath, o.Bytes); err != nil {
			return fail("staging the split", err)
		}
	}
	if err := reg.Advance(ctx, job.ID, rehome.StateStaged); err != nil {
		return fail("recording the job", err)
	}
	staged = true
	h.hook("staged")

	// From here the job is on disk: what follows is the same code a restart
	// runs, and a failure leaves the record for it.
	if err := h.finishSwap(ctx, job.ID); err != nil {
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", src.Key).Msg("Re-home job stopped; its record is kept and it is taken up again at the next start")
		return "failed", "the job stopped part way; its record is kept, reads still see every row once, and it is taken up again at the next start. The server log has the error under this job's id", report
	}
	h.logger.Info().Str("job", job.ID).Str("path", src.Key).Int64("rows", plan.Rows).
		Int64("moved_rows", plan.MisfiledRows()).Int("outputs", len(outs)).
		Msg("Re-homed a misfiled file: its rows are in the directories they belong to, and the file is removed")
	return "done", "", report
}

// stageFile makes a split output durable under its staged key: by taking the
// scratch file when it is on the storage's filesystem, by copying it when it
// is not.
func stageFile(ctx context.Context, store rehomeStore, stagedKey, localPath string, size int64) error {
	if adopter, ok := store.(storage.FileAdopter); ok {
		err := adopter.AdoptFile(ctx, stagedKey, localPath)
		if err == nil {
			return store.Sync(ctx, stagedKey)
		}
		if !errors.Is(err, storage.ErrAdoptUnsupported) {
			return err
		}
	}
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := store.WriteReader(ctx, stagedKey, f, size); err != nil {
		return err
	}
	return store.Sync(ctx, stagedKey)
}

// errUndone: the job was undone before anything changed for readers.
var errUndone = errors.New("the job was undone: reads in flight did not end in time, and nothing had been moved")

// finishSwap takes a journaled job from wherever it is to its end. It is
// what a new job runs after staging and what a restart runs for each record.
func (h *RehomeHandler) finishSwap(ctx context.Context, id string) error {
	reg := h.swap.Registry
	store, ok := h.mover()
	if !ok {
		return errors.New("storage is not local")
	}
	job, ok := reg.Get(id)
	if !ok {
		return fmt.Errorf("no job %s", id)
	}
	// Removing things must not stop half way because the server is going
	// down: every step below is safe to repeat, none is safe to skip.
	bg := context.WithoutCancel(ctx)

	if job.State == rehome.StateStaged {
		// 4. Reads start leaving the outputs out.
		if err := reg.Advance(ctx, id, rehome.StateCommitted); err != nil {
			return err
		}
		h.hook("committed")
		if err := h.waitReads(ctx); err != nil {
			// Nothing has moved: a read sees the source, with or without
			// the exclusions. Undo.
			if uerr := h.undo(bg, job); uerr != nil {
				return fmt.Errorf("waiting for reads in flight: %w; undoing the job: %v", err, uerr)
			}
			return errUndone
		}
		job.State = rehome.StateCommitted
	}

	if job.State == rehome.StateCommitted {
		// 5. Move. A final key that already holds the output was moved
		// before a restart.
		for _, o := range job.Outputs {
			if err := ctx.Err(); err != nil {
				return err
			}
			moved, err := h.outputInPlace(ctx, store, o)
			if err != nil {
				return err
			}
			if !moved {
				if err := store.MoveObject(ctx, o.Staged, o.Key); err != nil {
					return fmt.Errorf("move %s into place: %w", o.Key, err)
				}
			}
			h.hook("moved")
		}
		// The caches that remember which directories are empty: every
		// output is there now, and the latch below sends reads to them.
		for _, o := range job.Outputs {
			if h.swap.FilePublished != nil {
				h.swap.FilePublished(job.Database, job.Measurement, o.Start)
			}
		}
		if h.swap.InvalidateCaches != nil {
			h.swap.InvalidateCaches()
		}
		// 6. Reads start leaving the source out.
		if err := reg.Advance(ctx, id, rehome.StateLatched); err != nil {
			return err
		}
		h.hook("latched")
		job.State = rehome.StateLatched
	}

	// Latched. Every output must be there before the source goes.
	for _, o := range job.Outputs {
		if n, err := store.StatFile(bg, o.Key); err != nil || n != o.Size {
			return fmt.Errorf("output %s is not in place at its size (%d, want %d, err %v); the source is kept and left out of reads: restore the output or the source by hand", o.Key, n, o.Size, err)
		}
	}
	for {
		err := h.waitReads(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err() // a restart ends every read: recovery removes the source
		}
		h.logger.Warn().Str("job", id).Msg("Re-home: reads from before the switch are still in flight; the file being moved is kept until they end")
		select {
		case <-time.After(rehomeLatchedRetry):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h.hook("drained")

	// 7. The source goes. A file that is already gone was removed before a
	// restart.
	if err := storage.DeleteWithRetry(bg, store, job.Source.Key); err != nil && !storage.IsNotFound(err) {
		return fmt.Errorf("remove %s: %w", job.Source.Key, err)
	}
	// Durable before the record goes: a removal lost to a power cut, with the
	// record gone, would bring the source back beside its outputs.
	if dir := job.Source.Key[:strings.LastIndex(job.Source.Key, "/")]; dir != "" {
		if err := store.Sync(bg, dir); err != nil && !storage.IsNotFound(err) && !os.IsNotExist(err) {
			return fmt.Errorf("make the removal of %s durable: %w", job.Source.Key, err)
		}
	}
	h.hook("removed")
	if err := reg.End(bg, id); err != nil {
		return err
	}
	if err := store.RemoveAll(bg, rehome.StagedDir(id)); err != nil {
		h.logger.Warn().Err(err).Str("job", id).Msg("Re-home: could not remove the job's staging directory; it is removed at the next start")
	}
	if h.swap.InvalidateCaches != nil {
		h.swap.InvalidateCaches()
	}
	if h.swap.ClearMisfiled != nil {
		h.swap.ClearMisfiled(job.Source.Key)
	}
	return nil
}

// outputInPlace reports whether an output is at its final key: there, at the
// journaled size and hash. A final key holding anything else is an error.
func (h *RehomeHandler) outputInPlace(ctx context.Context, store rehomeStore, o rehome.FileRef) (bool, error) {
	n, err := store.StatFile(ctx, o.Key)
	if err != nil {
		return false, fmt.Errorf("stat %s: %w", o.Key, err)
	}
	if n < 0 {
		return false, nil
	}
	sum := sha256.New()
	if err := store.ReadTo(ctx, o.Key, sum); err != nil {
		return false, fmt.Errorf("read %s: %w", o.Key, err)
	}
	if n != o.Size || hex.EncodeToString(sum.Sum(nil)) != o.SHA256 {
		return false, fmt.Errorf("%s exists and is not the job's output", o.Key)
	}
	return true, nil
}

// undo removes a job that has not latched: the outputs it put in place, its
// staged files, its record. The source is untouched.
func (h *RehomeHandler) undo(ctx context.Context, job *rehome.Job) error {
	store, _ := h.mover()
	for _, o := range job.Outputs {
		moved, err := h.outputInPlace(ctx, store, o)
		if err != nil {
			return err
		}
		if moved {
			if err := storage.DeleteWithRetry(ctx, store, o.Key); err != nil && !storage.IsNotFound(err) {
				return err
			}
		}
	}
	if err := h.swap.Registry.End(ctx, job.ID); err != nil {
		return err
	}
	if h.swap.InvalidateCaches != nil {
		h.swap.InvalidateCaches()
	}
	return store.RemoveAll(ctx, rehome.StagedDir(job.ID))
}

// Recover takes up the jobs a stopped process left, and removes staging
// directories that belong to no job. Call it once the query handler and the
// DELETE handler are wired, and before anything that reads data files on a
// schedule or over the network starts: a job found latched has its source
// removed without waiting for reads, because no read from before the stop is
// alive.
func (h *RehomeHandler) Recover(ctx context.Context) error {
	if h.swap == nil || h.swap.Registry == nil {
		return nil
	}
	reg := h.swap.Registry
	store, ok := h.mover()
	jobs := reg.Jobs()
	if len(jobs) > 0 && !ok {
		return fmt.Errorf("%d re-home job(s) are recorded under %s/ and this node's storage is not local: they cannot be taken up", len(jobs), rehome.JournalRoot)
	}
	for _, job := range jobs {
		log := h.logger.With().Str("job", job.ID).Str("path", job.Source.Key).Str("state", string(job.State)).Logger()
		// A job that cannot be taken up stays as it is recorded: its files
		// stay owned, and reads keep leaving out what its state says, which
		// is each row once. It is listed by GET /api/v1/rehome and logged
		// here; the node starts.
		if err := h.recoverJob(ctx, store, job); err != nil {
			log.Error().Err(err).Msg("Re-home: a job found at start could not be taken up. It is kept as recorded: its files stay out of compaction, DELETE and retention, and reads see every row once. Check by hand")
			continue
		}
		log.Info().Msg("Re-home: a job found at start was taken up")
	}
	// Staging directories with no record: a job that stopped before it was
	// journaled.
	if ok {
		keys, err := store.List(ctx, rehome.StagedRoot+"/")
		if err != nil {
			return fmt.Errorf("list %s/: %w", rehome.StagedRoot, err)
		}
		seen := map[string]bool{}
		for _, k := range keys {
			id, _, _ := strings.Cut(strings.TrimPrefix(k, rehome.StagedRoot+"/"), "/")
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			if _, live := reg.Get(id); !live {
				if err := store.RemoveAll(ctx, rehome.StagedDir(id)); err != nil {
					h.logger.Warn().Err(err).Str("job", id).Msg("Re-home: could not remove a staging directory that belongs to no job")
				}
			}
		}
	}
	return nil
}

// hook is a test seam: called after each step, by name.
func (h *RehomeHandler) hook(step string) {
	if h.afterStep != nil {
		h.afterStep(step)
	}
}

// recoverJob takes one recorded job to its end, or undoes it.
func (h *RehomeHandler) recoverJob(ctx context.Context, store rehomeStore, job *rehome.Job) error {
	switch job.State {
	case rehome.StateStaged:
		// Nothing changed for readers: forget it. The request can be sent
		// again.
		return h.undo(ctx, job)
	case rehome.StateCommitted:
		n, err := store.StatFile(ctx, job.Source.Key)
		if err != nil {
			return fmt.Errorf("stat its source: %w", err)
		}
		if n != job.Source.Size {
			// The source is gone or changed with the outputs not all in
			// place: neither side is known to be whole.
			return fmt.Errorf("the file being moved is not as the job recorded it (size %d, recorded %d)", n, job.Source.Size)
		}
		for _, o := range job.Outputs {
			moved, err := h.outputInPlace(ctx, store, o)
			if err != nil {
				return err
			}
			if staged, _ := store.StatFile(ctx, o.Staged); !moved && staged < 0 {
				// An output is neither staged nor in place: the job cannot
				// go on, and the source is whole. Undo.
				h.logger.Error().Str("job", job.ID).Str("output", o.Key).Msg("Re-home: a job found part way had lost one of its outputs; it is undone and the file is as it was")
				return h.undo(ctx, job)
			}
		}
		return h.finishSwap(ctx, job.ID)
	case rehome.StateLatched:
		return h.finishSwap(ctx, job.ID)
	}
	return fmt.Errorf("state %q", job.State)
}
