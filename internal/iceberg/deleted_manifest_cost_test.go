package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// This file is a MEASUREMENT harness for #1106, not a regression test. It answers the question the
// issue says has to be answered before any fix is chosen: what does a DELETED-only manifest
// actually cost a scan plan?
//
// Mechanism under measurement (iceberg-go v0.6.0, table/snapshot_producers.go:153-194):
// overwriteFiles.existingManifests iterates each parent manifest's entries with
// discardDeleted=true. A manifest that holds only DELETED entries yields nothing, so
// foundDeletedCount == 0 and the manifest is carried into the new snapshot VERBATIM. Arc's
// reconciler produces one such manifest per removal pass, and they accumulate forever.
//
// Run with:
//
//	go test ./internal/iceberg/ -run TestMeasure_DeletedOnlyManifestCost -v -timeout 30m

// requireMeasurementRun gates this whole file. These tests build tables of up to 10 000 Parquet
// files and time commits, so they run for minutes each — and CI runs `go test -tags=duckdb_arrow
// -race ./...` with NO -short, which is how the first version of this file timed out the 10-minute
// race job. A testing.Short() guard is therefore not enough; the opt-in env var is the repo's
// convention for a test too expensive to run unasked (see internal/storage's object-store contract
// tests). The file still compiles everywhere, so gofmt, vet and refactors keep covering it.
//
//	ARC_ICEBERG_MANIFEST_BENCH=1 go test -tags=duckdb_arrow ./internal/iceberg/ -run TestMeasure -v -timeout 60m
func requireMeasurementRun(t *testing.T) {
	t.Helper()
	if os.Getenv("ARC_ICEBERG_MANIFEST_BENCH") != "1" {
		t.Skip("measurement harness; set ARC_ICEBERG_MANIFEST_BENCH=1 to run (minutes per test)")
	}
}

// costRig is a table on a real local backend, sized to order.
type costRig struct {
	exp     *Exporter
	root    string
	dataDir string
	metaDir string
	// db is the catalog handle. Exposed so a test can close it to make a commit fail
	// deterministically (see TestExpireSnapshots_ReportsFailureSoThePassIsNotCached).
	db *sql.DB
}

func newCostRig(t *testing.T, retain int) *costRig {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, "file://"+root, "arc", retain, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "mydb", "cpu", "2026", "07", "14", "15")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// The collapse (#1106) is what this harness measures the need for, so it is OFF by default
	// here: with it on, the growth curves below stop at manifestCollapseThreshold and the numbers
	// in docs/progress/2026-10-07-issue-1106-iceberg-manifest-collapse.md cannot be reproduced.
	// Tests that measure the collapse itself turn it back on.
	exp.collapseThreshold = 0
	return &costRig{exp: exp, root: root, dataDir: dataDir, db: db}
}

// write creates n Arc-style parquet files and returns their paths.
func (r *costRig) write(t *testing.T, prefix string, n int) []string {
	t.Helper()
	base := int64(1_752_500_000_000_000)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		p := filepath.Join(r.dataDir, fmt.Sprintf("%s_%04d.parquet", prefix, i))
		// Spread timestamps within one UTC day so day(time) partition inference succeeds.
		writeArcStyleParquet(t, p, base+int64(i)*1_000_000, 2)
		out = append(out, p)
	}
	return out
}

func (r *costRig) reconcile(t *testing.T, ctx context.Context, files []string) {
	t.Helper()
	refs := make([]FileRef, 0, len(files))
	for _, f := range files {
		refs = append(refs, refOf(t, f))
	}
	sc, err := UnionSchema(ctx, files)
	if err != nil {
		t.Fatalf("UnionSchema: %v", err)
	}
	if err := r.exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, refs); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	tbl := r.load(t, ctx)
	r.metaDir = filepath.Dir(trimFileScheme(tbl.MetadataLocation()))
}

func (r *costRig) load(t *testing.T, ctx context.Context) *icetable.Table {
	t.Helper()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	return tbl
}

func trimFileScheme(s string) string {
	if len(s) > 7 && s[:7] == "file://" {
		return s[7:]
	}
	return s
}

// manifestShape counts the manifests in the current snapshot's list, splitting out the ones that
// hold no live entries at all — the DELETED-only manifests #1106 is about. Counts come from the
// manifest-list entries, so this costs one manifest-list read and no entry scans.
func (r *costRig) manifestShape(t *testing.T, ctx context.Context) (total, deletedOnly, liveEntries int) {
	t.Helper()
	tbl := r.load(t, ctx)
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, 0
	}
	fio, err := tbl.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := snap.Manifests(fio)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		total++
		live := int(m.AddedDataFiles()) + int(m.ExistingDataFiles())
		if live == 0 && m.DeletedDataFiles() > 0 {
			deletedOnly++
		}
		liveEntries += live
	}
	return total, deletedOnly, liveEntries
}

// planCost times PlanFiles, which is what every reader does to plan a scan: it opens the manifest
// list and then every manifest in it.
func (r *costRig) planCost(t *testing.T, ctx context.Context, reps int) (time.Duration, int) {
	t.Helper()
	tbl := r.load(t, ctx)
	// One warm-up so the first run's page-cache miss is not attributed to manifest count.
	if _, err := tbl.Scan().PlanFiles(ctx); err != nil {
		t.Fatalf("PlanFiles warm-up: %v", err)
	}
	var files int
	start := time.Now()
	for i := 0; i < reps; i++ {
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatalf("PlanFiles: %v", err)
		}
		files = len(tasks)
	}
	return time.Since(start) / time.Duration(reps), files
}

func (r *costRig) metadataBytes(t *testing.T) (avroCount int, avroBytes int64) {
	t.Helper()
	ents, err := os.ReadDir(r.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.IsDir() || filepath.Ext(e.Name()) != ".avro" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		avroCount++
		avroBytes += info.Size()
	}
	return
}

// TestMeasure_DeletedOnlyManifestCost grows the DELETED-only manifest count on a table with a
// realistic live file set and reports what it costs to plan a scan.
//
// Shape of each removal pass: drop one live file and add one fresh one, so the live file count
// stays constant and the only thing that grows is the inherited DELETED-only manifest count. That
// isolates the mechanism — a pass that merely shrank the table would also shrink the work PlanFiles
// does on live entries, and the two effects would be impossible to separate.
func TestMeasure_DeletedOnlyManifestCost(t *testing.T) {
	requireMeasurementRun(t)
	ctx := context.Background()
	// retain high enough that expiry never runs during the measurement: expireSnapshots would
	// change the manifest set underneath us and confound the curve.
	r := newCostRig(t, 10_000)

	const liveFiles = 300 // the issue measured a 319-file table
	live := r.write(t, "base", liveFiles)
	r.reconcile(t, ctx, live)

	total, deletedOnly, liveEntries := r.manifestShape(t, ctx)
	plan, planned := r.planCost(t, ctx, 5)
	avroN, avroB := r.metadataBytes(t)
	t.Logf("passes=%3d manifests=%3d deleted_only=%3d live_entries=%4d planned=%4d plan=%8s avro=%3d avro_kb=%6.1f",
		0, total, deletedOnly, liveEntries, planned, plan.Round(time.Microsecond), avroN, float64(avroB)/1024)

	spare := r.write(t, "spare", 64)
	for pass := 1; pass <= 64; pass++ {
		// Swap one file: drop live[pass-1], add spare[pass-1]. Live count stays at liveFiles.
		live = append(live[1:], spare[pass-1])
		r.reconcile(t, ctx, live)

		if pass%8 != 0 && pass != 1 {
			continue
		}
		total, deletedOnly, liveEntries = r.manifestShape(t, ctx)
		plan, planned = r.planCost(t, ctx, 5)
		avroN, avroB = r.metadataBytes(t)
		t.Logf("passes=%3d manifests=%3d deleted_only=%3d live_entries=%4d planned=%4d plan=%8s avro=%3d avro_kb=%6.1f",
			pass, total, deletedOnly, liveEntries, planned, plan.Round(time.Microsecond), avroN, float64(avroB)/1024)
	}
}

// TestMeasure_CollapseBenefitAndCost quantifies the candidate fix. After N swap passes it runs the
// merge-append collapse Arc already owns (manifestMergeOn) and reports what it costs and what it
// buys. This is the number manifestCollapseThreshold is derived from.
//
// It routes the pass's add through the merging producer, which is one shape the collapse can take.
// The SHIPPED collapse (collapseManifests) adds no files at all — an AddFiles with no paths still
// merges — so it rides on any pass, including one with nothing to add. The staged work is the same
// either way (an overwrite, then a merged append), so these figures hold for both.
func TestMeasure_CollapseBenefitAndCost(t *testing.T) {
	requireMeasurementRun(t)
	ctx := context.Background()
	r := newCostRig(t, 10_000)

	const liveFiles = 300
	live := r.write(t, "base", liveFiles)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 64)
	var ordinaryBefore time.Duration
	for pass := 1; pass <= 64; pass++ {
		live = append(live[1:], spare[pass-1])
		passStart := time.Now()
		r.reconcile(t, ctx, live)
		ordinaryBefore = time.Since(passStart)
	}

	total, deletedOnly, _ := r.manifestShape(t, ctx)
	before, _ := r.planCost(t, ctx, 5)
	avroN, avroB := r.metadataBytes(t)
	t.Logf("BEFORE collapse: manifests=%d deleted_only=%d plan=%s avro=%d avro_kb=%.1f",
		total, deletedOnly, before.Round(time.Microsecond), avroN, float64(avroB)/1024)

	// The collapse: one merge-enabled append, here carrying a file so the cost of the merge is
	// measured alongside an ordinary pass's work. collapseManifests ships the no-paths form.
	extra := r.write(t, "collapse", 1)
	live = append(live[1:], extra[0])
	refs := make([]FileRef, 0, len(live))
	for _, f := range live {
		refs = append(refs, refOf(t, f))
	}
	tbl := r.load(t, ctx)
	have, err := r.exp.tableDataFiles(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{}
	for _, ref := range refs {
		want[ref.PhysicalPath] = ref.SizeBytes
	}
	var toAdd, toRemove []string
	for p := range want {
		if _, ok := have[p]; !ok {
			toAdd = append(toAdd, p)
		}
	}
	for p := range have {
		if _, ok := want[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}

	start := time.Now()
	txn := tbl.NewTransaction()
	if len(toRemove) > 0 {
		if err := txn.ReplaceDataFiles(ctx, toRemove, nil, nil); err != nil {
			t.Fatalf("remove-only: %v", err)
		}
	}
	if err := r.exp.addFilesMerging(ctx, txn, toAdd, "mydb", "cpu"); err != nil {
		t.Fatalf("addFilesMerging: %v", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	collapse := time.Since(start)

	// For comparison: what an ordinary (fast-append) pass of the same shape costs.
	extra2 := r.write(t, "ordinary", 1)
	live = append(live[1:], extra2[0])
	ordStart := time.Now()
	r.reconcile(t, ctx, live)
	ordinary := time.Since(ordStart)

	total, deletedOnly, _ = r.manifestShape(t, ctx)
	after, planned := r.planCost(t, ctx, 5)
	avroN, avroB = r.metadataBytes(t)
	t.Logf("AFTER  collapse: manifests=%d deleted_only=%d plan=%s planned=%d avro=%d avro_kb=%.1f",
		total, deletedOnly, after.Round(time.Microsecond), planned, avroN, float64(avroB)/1024)
	t.Logf("COST: collapse commit=%s   ordinary pass %s (pre-collapse) -> %s (post-collapse)   plan %s -> %s",
		collapse.Round(time.Millisecond), ordinaryBefore.Round(time.Millisecond), ordinary.Round(time.Millisecond),
		before.Round(time.Microsecond), after.Round(time.Microsecond))
}

// TestMeasure_CollapseCostByTableSize checks how the collapse scales with the LIVE file count,
// because the merge rewrites the whole manifest set: the benefit is O(manifests shed) but the cost
// is O(files in the table). A threshold tuned on a 300-file rig would be a trap if the cost were
// superlinear on a table ten times the size (the measurementTimeout is 2 minutes).
func TestMeasure_CollapseCostByTableSize(t *testing.T) {
	requireMeasurementRun(t)
	for _, liveFiles := range []int{30, 100, 300, 1000, 2000, 5000, 10_000} {
		t.Run(fmt.Sprintf("files=%d", liveFiles), func(t *testing.T) {
			ctx := context.Background()
			r := newCostRig(t, 10_000)
			live := r.write(t, "base", liveFiles)
			r.reconcile(t, ctx, live)
			// 16 swap passes: enough manifests to make a collapse worth doing.
			spare := r.write(t, "spare", 18)
			for i := 0; i < 16; i++ {
				live = append(live[1:], spare[i])
				r.reconcile(t, ctx, live)
			}
			total, deletedOnly, _ := r.manifestShape(t, ctx)
			before, _ := r.planCost(t, ctx, 3)

			// An ordinary pass of the same shape at the same manifest count, so the collapse's
			// INCREMENT over the pass it rides on is measured rather than inferred. That increment
			// is what manifestCollapseThreshold is derived from; the absolute collapse figure below
			// includes the overwrite half, which an ordinary pass pays too.
			ordinarySpare := r.write(t, "ordinary", 1)
			live = append(live[1:], ordinarySpare[0])
			ordStart := time.Now()
			r.reconcile(t, ctx, live)
			ordinary := time.Since(ordStart)

			live = append(live[1:], spare[16])
			tbl := r.load(t, ctx)
			have, err := r.exp.tableDataFiles(ctx, tbl)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]struct{}{}
			for _, f := range live {
				want[fileURI(f)] = struct{}{}
			}
			var toAdd, toRemove []string
			for p := range want {
				if _, ok := have[p]; !ok {
					toAdd = append(toAdd, p)
				}
			}
			for p := range have {
				if _, ok := want[p]; !ok {
					toRemove = append(toRemove, p)
				}
			}
			start := time.Now()
			txn := tbl.NewTransaction()
			if len(toRemove) > 0 {
				if err := txn.ReplaceDataFiles(ctx, toRemove, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.exp.addFilesMerging(ctx, txn, toAdd, "mydb", "cpu"); err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			collapse := time.Since(start)
			afterTotal, _, _ := r.manifestShape(t, ctx)
			after, planned := r.planCost(t, ctx, 3)
			t.Logf("files=%5d manifests %3d->%2d (deleted_only %2d) plan %8s -> %8s  ordinary=%5s collapse=%5s increment=%5s  planned=%d",
				liveFiles, total, afterTotal, deletedOnly,
				before.Round(time.Microsecond), after.Round(time.Microsecond),
				ordinary.Round(time.Millisecond), collapse.Round(time.Millisecond),
				(collapse - ordinary).Round(time.Millisecond), planned)
		})
	}
}

// TestMeasure_CollapseMarginalCost is the measurement manifestCollapseThreshold actually rests on:
// what does collapsing cost when there is almost nothing to collapse?
//
// TestMeasure_CollapseCostByTableSize compares a collapse pass against an ordinary pass at 33
// manifests, where both are dominated by the 33-manifest scan — and finds the collapse CHEAPER
// above ~2000 live files, because it replaces 33 manifest writes with one. That says collapsing is
// a bargain once the pile exists, but it cannot say what collapsing too eagerly costs. For that the
// pile has to be absent: at 3 manifests an ordinary pass appends one small manifest while a
// collapse rewrites the entire live set. The difference is the per-pass price of a low threshold.
func TestMeasure_CollapseMarginalCost(t *testing.T) {
	requireMeasurementRun(t)
	for _, liveFiles := range []int{300, 1000, 5000, 10_000} {
		t.Run(fmt.Sprintf("files=%d", liveFiles), func(t *testing.T) {
			ctx := context.Background()

			// Two rigs with identical content, so the only difference is whether the pass collapses.
			measure := func(collapse bool) (pass time.Duration, manifests int, plan time.Duration) {
				r := newCostRig(t, 10_000)
				live := r.write(t, "base", liveFiles)
				r.reconcile(t, ctx, live)
				spare := r.write(t, "spare", 2)
				// One swap pass so the table is at the 3 manifests an ordinary pass settles on.
				live = append(live[1:], spare[0])
				r.reconcile(t, ctx, live)
				if collapse {
					r.exp.collapseThreshold = 1
				}
				live = append(live[1:], spare[1])
				start := time.Now()
				r.reconcile(t, ctx, live)
				pass = time.Since(start)
				manifests, _, _ = r.manifestShape(t, ctx)
				plan, _ = r.planCost(t, ctx, 3)
				return pass, manifests, plan
			}

			ordPass, ordManifests, ordPlan := measure(false)
			colPass, colManifests, colPlan := measure(true)
			t.Logf("files=%5d  ordinary pass=%5s (%2d manifests, plan %8s)   collapsing pass=%5s (%2d manifests, plan %8s)   marginal=%5s",
				liveFiles,
				ordPass.Round(time.Millisecond), ordManifests, ordPlan.Round(time.Microsecond),
				colPass.Round(time.Millisecond), colManifests, colPlan.Round(time.Microsecond),
				(colPass - ordPass).Round(time.Millisecond))
		})
	}
}
