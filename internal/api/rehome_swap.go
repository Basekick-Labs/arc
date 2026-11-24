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
	// ExportExpected: the Iceberg export is enabled, so a table may name
	// the file a job moves (rehome_export.go). The export itself is bound
	// later, with SetExport.
	ExportExpected bool
	// TieringExpected: tiered storage is enabled in the configuration
	// (rehome_tiering.go). Whether a manager runs is known later
	// (SetTiering). ColdConfigured: a cold tier is enabled too.
	TieringExpected bool
	ColdConfigured  bool
	// ColdFileNames returns the names the query engine reports for the
	// files a glob in cold storage matches (rehome_cold.go). It runs on the
	// engine's own connection, which has the cold store's credentials.
	ColdFileNames func(ctx context.Context, glob string) ([]string, error)
	// Cluster: jobs run through the cluster record (rehome_cluster.go). Set
	// only on a cluster node whose configuration takes part in jobs.
	Cluster RehomeCluster
	// ReadReady reports whether this node holds its cluster read lease.
	ReadReady func() (bool, string)
	// InvalidateLocalCaches drops this node's query caches and the query
	// engine's HTTP cache, without telling other nodes.
	InvalidateLocalCaches func()
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
	if h.swap.Refusal != nil {
		if why := h.swap.Refusal(); why != "" {
			return why
		}
	}
	if h.clusterMode() {
		if why := h.clusterRefusal(); why != "" {
			return why
		}
		if h.swap.BackupRunning != nil && h.swap.BackupRunning() {
			return "a backup or a restore is running; retry once it has ended"
		}
		return ""
	}
	if _, ok := h.mover(); !ok {
		return "moving a file's rows needs local storage in this version; this node's storage is " + h.storage.Type()
	}
	if h.swap.Refusal != nil {
		if why := h.swap.Refusal(); why != "" {
			return why
		}
	}
	if h.lateBound() && !h.isWired() {
		return "the server is still starting; retry in a moment"
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
	if h.clusterMode() {
		return h.runClusterSwap(ctx, job)
	}
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
		Source: rehome.FileRef{Key: src.Key, Tier: job.Tier}, CreatedAt: job.StartedAt}
	cold := rec.Source.Cold()
	read := h.storage.ReadTo

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
		if cold {
			// In cold storage: what must be true of it is tiering's to say
			// (the object, its row, no copy on hot, no DELETE rewrite
			// beside it). Under the rewrite lock like the hot checks: a
			// cold DELETE acts on files under it, and asks who owns them.
			var backend storage.Backend
			size, rec.Source.ColdStore, backend, checkErr = h.coldSource(ctx, src.Key)
			if checkErr != nil {
				return
			}
			read = backend.ReadTo
			if h.swap.OtherExclusions != nil {
				excluded, err := h.swap.OtherExclusions(ctx, rec.Prefix())
				if err != nil {
					checkErr = err
					return
				}
				for _, k := range excluded {
					if k == src.Key {
						checkErr = rehome.Refusef("%s is already left out of reads by another operation; moving its rows would bring them back", src.Key)
						return
					}
				}
			}
			return
		}
		size, checkErr = h.storage.StatFile(ctx, src.Key)
		if checkErr == nil && size < 0 {
			checkErr = rehome.Refusef("%s is not in storage any more", src.Key)
		}
		if checkErr != nil {
			return
		}
		// A file an earlier DELETE rewrote, with the rewrite still beside
		// it: one of the two holds rows that DELETE removed, and DELETE is
		// what tells them apart and removes the stale one. Split, its rows
		// would land under new names that no DELETE can match to the pair,
		// and the removed rows would be back for good.
		siblings, err := h.storage.List(ctx, src.Dir+"/")
		if err != nil {
			checkErr = err
			return
		}
		family := storage.RewriteFamily(src.Key)
		for _, k := range siblings {
			if k != src.Key && storage.RewriteFamily(k) == family {
				checkErr = rehome.Refusef("%s was rewritten by a DELETE and the two copies are both in storage (%s); run a DELETE on the measurement first, which removes the stale one", src.Key, k)
				return
			}
		}
		if h.swap.OtherExclusions == nil {
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
	// The job owns the file now, so tiering leaves it where it is: what
	// tiering says about it here holds for the job's life.
	if checkErr == nil && !cold {
		checkErr = h.checkTieringSource(ctx, src.Key)
	}
	if checkErr != nil {
		return fail("checking the file", checkErr)
	}

	// 2. Split in scratch.
	prep, step, err := h.prepare(ctx, src.Key, read)
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
	if plan.RemainderRows > 0 && h.exportExpected() {
		// The export registers a file under the day of its rows, and leaves
		// out one whose rows have none. In the file as it is those rows may
		// be in the table; split off, they would leave it.
		return "refused", fmt.Sprintf("%s holds %d row(s) with a NULL or infinite time, which the Iceberg export cannot place in a file of their own: moving the others would take these out of the table. Not supported with the Iceberg export in this version", src.Key, plan.RemainderRows), report
	}
	outs, err := rehome.Split(ctx, prep.db, plan, prep.local, prep.outDir)
	if err != nil {
		return fail("splitting the file and checking the split", err)
	}
	report.SplitChecked, report.Outputs = true, outs

	// 3. Stage.
	refs := make([]rehome.FileRef, len(outs))
	for _, o := range outs {
		if err := h.canRecordTier(o.Dir + "/x.parquet"); err != nil {
			return refuse(fmt.Sprintf("%s holds rows that belong in %s, which tiered storage cannot record (%v): with tiered storage on, the files there would not be read. Not supported in this version", src.Key, o.Dir, err))
		}
	}
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
		var coldRefused errColdRefused
		if errors.As(err, &coldRefused) {
			return "refused", coldRefused.why + "; the job was undone before it moved anything, and the file is as it was", report
		}
		var refused errExportRefused
		if errors.As(err, &refused) {
			return "refused", fmt.Sprintf("the Iceberg export cannot publish %s.%s now (%s), so the job was undone before it moved anything; the file is as it was. Send the request again once the export is publishing that table", src.Database, src.Measurement, refused.why), report
		}
		if errors.Is(err, errUndone) {
			h.logger.Warn().Str("job", job.ID).Str("path", src.Key).Msg("Re-home job undone: reads in flight did not end in time; nothing was moved")
			return "failed", fmt.Sprintf("queries in flight did not end within %s, so the job was undone before it moved anything; the file is as it was. Send the request again", rehomeReadWait), report
		}
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", src.Key).Msg("Re-home job stopped part way; its record is kept. POST /api/v1/rehome/<id>/resume takes it up again, and so does the next start")
		return "stopped", "the job stopped part way and its record is kept; its files stay out of compaction, DELETE, retention and backup until it ends. POST /api/v1/rehome/" + job.ID + "/resume takes it up again, and so does the next start. The server log has the error under this job's id", report
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

// errJobKept: a job that has not switched reads cannot go on now, for a
// reason that may pass (tiered storage could not be asked about its file).
// It is kept as recorded.
type errJobKept struct{ why string }

func (e errJobKept) Error() string {
	return "the job is kept as recorded and can be resumed later: " + e.why
}

// errColdRefused: a job on a file in cold storage was undone before it
// changed anything for readers, for the reason given.
type errColdRefused struct{ why string }

func (e errColdRefused) Error() string { return e.why }

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

	// A source in cold storage needs the cold store it was recorded in, at
	// every step from here: without it the job is kept where it is.
	if job.Source.Cold() {
		if _, err := h.coldSourceSize(ctx, job); err != nil {
			return errJobKept{err.Error()}
		}
	}

	if job.State == rehome.StateStaged {
		// A cold file is left out of reads by its cold address. That the
		// query engine reports the file under exactly that address is
		// asked now, while undoing the job costs nothing.
		if job.Source.Cold() {
			if why := h.coldNameCheck(ctx, job); why != "" {
				if uerr := h.undo(bg, job); uerr != nil {
					return fmt.Errorf("%s; undoing the job: %v", why, uerr)
				}
				return errColdRefused{why}
			}
			h.hook("cold-name")
		}
		// With the Iceberg export on: it must be publishing this table,
		// asked while undoing the job costs nothing.
		if h.exportExpected() {
			if why := h.exportReady(ctx, job); why != "" {
				if uerr := h.undo(bg, job); uerr != nil {
					return fmt.Errorf("the Iceberg export cannot publish the table (%s); undoing the job: %v", why, uerr)
				}
				return errExportRefused{why}
			}
			h.hook("export-ready")
		}
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
		if h.swap.FilePublished != nil {
			for _, o := range job.Outputs {
				h.swap.FilePublished(job.Database, job.Measurement, o.Start)
			}
			// And once with an hour that is never the current one: the call
			// does nothing for the current hour, whose absence is not
			// cached, but a day directory's is, and today's daily output
			// lands in one. This moves the measurement's stamp whatever the
			// outputs are, so a read in flight cannot store "absent" for a
			// directory an output is now in.
			h.swap.FilePublished(job.Database, job.Measurement, time.Unix(0, 0).UTC())
		}
		if h.swap.InvalidateCaches != nil {
			h.swap.InvalidateCaches()
		}
		// With tiered storage, reads take a measurement's tiers from its
		// tier rows: the outputs have theirs before reads are sent to them.
		if err := h.recordTierRows(ctx, job); err != nil {
			return err
		}
		// 6. Reads start leaving the source out.
		if err := reg.Advance(ctx, id, rehome.StateLatched); err != nil {
			return err
		}
		h.hook("latched")
		job.State = rehome.StateLatched
	}

	// Latched. A job latched while no tiering manager ran, and taken up
	// with one, has no rows for its outputs yet; recording them is safe to
	// repeat.
	if err := h.recordTierRows(ctx, job); err != nil {
		return err
	}
	// Every output must be there before the source goes.
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

	// With the Iceberg export on, the source stays until the published
	// table does not name it.
	// A source already gone (a stop between its removal and the record's)
	// has nothing left to keep for the table's sake.
	if h.exportExpected() {
		var n int64
		var err error
		if job.Source.Cold() {
			n, err = h.coldSourceSize(bg, job)
		} else {
			n, err = store.StatFile(bg, job.Source.Key)
		}
		if err != nil && !storage.IsNotFound(err) {
			return fmt.Errorf("stat %s: %w", job.Source.Key, err)
		}
		if err == nil && n >= 0 {
			if err := h.waitExport(ctx, job); err != nil {
				return err
			}
			h.hook("exported")
		}
	}

	// 7. The source goes. A file that is already gone was removed before a
	// restart.
	if job.Source.Cold() {
		// From cold storage: the object, then its tier row, then the caches
		// that name it, and not done until cold storage says it is gone
		// (tiering.RemoveRehomedColdFile). A failure keeps the job latched:
		// reads leave the file out, and a resume tries again.
		if err := h.removeColdSource(bg, job); err != nil {
			return err
		}
		h.hook("removed")
	} else {
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
		h.forgetTierRows(bg, id, []string{job.Source.Key})
	}
	if err := reg.End(bg, id); err != nil {
		return err
	}
	// The record's removal made durable too: a record brought back by a
	// power cut would have recovery look for outputs that compaction may
	// have merged since.
	if err := store.Sync(bg, rehome.JournalRoot); err != nil && !storage.IsNotFound(err) && !os.IsNotExist(err) {
		h.logger.Warn().Err(err).Str("job", id).Msg("Re-home: could not make the removal of the job's record durable")
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
	// Their rows too, if the job had got as far as writing them.
	outs := make([]string, len(job.Outputs))
	for i, o := range job.Outputs {
		outs[i] = o.Key
	}
	h.forgetTierRows(ctx, job.ID, outs)
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
//
// With the Iceberg export or tiered storage enabled, a job found past its
// commit is not finished here: it needs them, and they are built later.
// Recover hands those to a goroutine that holds the job slot and does
// nothing to them until Wired is called (recoverLater); until then each is
// as recorded, which reads see as every row once.
func (h *RehomeHandler) Recover(ctx context.Context) error {
	if h.swap == nil || h.swap.Registry == nil {
		return nil
	}
	reg := h.swap.Registry
	store, ok := h.mover()
	var jobs []*rehome.Job
	for _, j := range reg.Jobs() {
		if !j.Cluster { // the cluster participant ends those (#1860)
			jobs = append(jobs, j)
		}
	}
	if len(jobs) > 0 && !ok {
		return fmt.Errorf("%d re-home job(s) are recorded under %s/ and this node's storage is not local: they cannot be taken up", len(jobs), rehome.JournalRoot)
	}
	// A journal from a run under another configuration: this deployment is
	// one a swap is refused on (it became a cluster node, or edge sync was
	// turned on). Finishing a job would put files in place that the cluster
	// manifest or the spokes know nothing of.
	//
	// Tiered storage turned on across the restart is not such a case: the
	// job is taken up once tiering is known (recoverLater), which asks
	// tiering about the job's file first.
	//
	// The Iceberg export turned on or off across the restart is not such a
	// case. Turned on: the job is finished as any job is with the export on
	// (below). Turned off: nothing maintains a table, and the job finishes
	// without asking; a table left from before names the old file until the
	// export is turned on again, as after any change made with it off.
	refused := ""
	if h.swap.Refusal != nil {
		refused = h.swap.Refusal()
	}
	var later []*rehome.Job
	for _, job := range jobs {
		log := h.logger.With().Str("job", job.ID).Str("path", job.Source.Key).Str("state", string(job.State)).Logger()
		if refused == "" && h.lateBound() && job.State != rehome.StateStaged {
			// Past its commit, with the Iceberg export or tiered storage
			// on: the job needs them, and neither exists yet. Reads are
			// right in either state meanwhile.
			later = append(later, job)
			log.Info().Msg("Re-home: a job found at start is taken up once the Iceberg export and tiered storage are running")
			continue
		}
		if refused != "" {
			if job.State == rehome.StateLatched {
				log.Error().Str("why", refused).Msg("Re-home: a job found at start had switched reads to its new files, and this deployment no longer runs such jobs. It is kept as recorded: reads leave the old file out and read the new ones. Finish it under the configuration it was started with, or remove the old file and the record by hand")
				continue
			}
			if err := h.undoIfSourceWhole(ctx, store, job); err != nil {
				log.Error().Err(err).Str("why", refused).Msg("Re-home: a job found at start could not be undone on a deployment that no longer runs such jobs; it is kept as recorded. Check by hand")
				continue
			}
			log.Warn().Str("why", refused).Msg("Re-home: a job found at start was undone, because this deployment no longer runs such jobs; the file is as it was")
			continue
		}
		// A job that cannot be taken up stays as it is recorded: its files
		// stay owned, and reads keep leaving out what its state says, which
		// is each row once. It is listed by GET /api/v1/rehome and logged
		// here; the node starts.
		if err := h.recoverJob(ctx, store, job); err != nil {
			log.Error().Err(err).Msg("Re-home: a job found at start could not be taken up. It is kept as recorded: its files stay out of compaction, DELETE and retention. The error says which file is not as the job recorded it; until that is put right by hand, reads may be missing that file's rows")
			continue
		}
		log.Info().Msg("Re-home: a job found at start was taken up")
	}
	if len(later) > 0 {
		h.recoverLater(later)
	}
	// Staging directories with no record: a job that stopped before it was
	// journaled. Not on a cluster node: a cluster job holds staged copies
	// before this node records it, and SweepLocal settles those once synced
	// (#1860).
	if ok && h.swap.Cluster == nil {
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

// sourceWhole reports whether the file a job is moving is in storage at the
// size the job recorded. While a job is committed, reads see that file and
// leave the outputs out: if it is gone or changed, neither side is whole.
func (h *RehomeHandler) sourceWhole(ctx context.Context, store rehomeStore, job *rehome.Job) error {
	var n int64
	var err error
	if job.Source.Cold() {
		if n, err = h.coldSourceSize(ctx, job); err != nil {
			// Not known to be wrong: cold storage could not be asked.
			return errJobKept{err.Error()}
		}
	} else {
		n, err = store.StatFile(ctx, job.Source.Key)
	}
	if err != nil {
		return fmt.Errorf("stat the file being moved: %w", err)
	}
	if n != job.Source.Size {
		return fmt.Errorf("the file being moved, %s, is not as the job recorded it (size %d, recorded %d): reads are left without its rows until it is restored", job.Source.Key, n, job.Source.Size)
	}
	return nil
}

// undoIfSourceWhole undoes a job that has not latched, provided the file it
// was moving is whole.
func (h *RehomeHandler) undoIfSourceWhole(ctx context.Context, store rehomeStore, job *rehome.Job) error {
	if err := h.sourceWhole(ctx, store, job); err != nil {
		return err
	}
	return h.undo(ctx, job)
}

// resume takes up a job this process holds a record of and is not running:
// one that stopped part way, or one found at start that could not be taken
// up then.
func (h *RehomeHandler) resume(ctx context.Context, id string) error {
	store, ok := h.mover()
	if !ok {
		return errors.New("storage is not local")
	}
	job, ok := h.swap.Registry.Get(id)
	if !ok {
		return rehome.Refusef("no job %s is recorded", id)
	}
	return h.recoverJob(ctx, store, job)
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
		if err := h.sourceWhole(ctx, store, job); err != nil {
			return err
		}
		// Tiering is asked about the file again before the job goes on: it
		// may have been recorded under another configuration, or kept at
		// start because tiering could not be asked. Reads still use the
		// source here, so both answers are safe to act on.
		// (A cold source was asked about by sourceWhole, above: it is in
		// the cold store the job recorded, at its size.)
		if !job.Source.Cold() {
			if cerr, known := h.tieringSource(ctx, job.Source.Key); cerr != nil {
				if !known {
					return errJobKept{cerr.Error()}
				}
				// Cold storage holds the file or a copy: going on would leave
				// it read beside the new files.
				if uerr := h.undo(ctx, job); uerr != nil {
					return fmt.Errorf("%v; undoing the job: %w", cerr, uerr)
				}
				return cerr
			}
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
