package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/gofiber/fiber/v2"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// #1207 d2: a row DELETE racing a compaction, against a real FSM.

// fsmCoordinator is a primary writer whose manifest is an in-memory FSM.
type fsmCoordinator struct {
	t       *testing.T
	fsm     *raft.ClusterFSM
	idx     uint64
	active  bool
	waits   int
	onBatch func(n int) // before batch n (1-based) applies
	batches int
	missing []string
	syncs   int
	syncErr error
	onSync  func()
	db      *database.DuckDB
	root    string
}

func (c *fsmCoordinator) apply(typ raft.CommandType, p any) error {
	raw, err := json.Marshal(p)
	if err != nil {
		c.t.Fatal(err)
	}
	data, _ := json.Marshal(raft.Command{Type: typ, Payload: raw})
	c.idx++
	if res := c.fsm.Apply(&hraft.Log{Index: c.idx, Data: data}); res != nil {
		if err, ok := res.(error); ok {
			return err
		}
	}
	return nil
}

func (c *fsmCoordinator) BatchFileOpsInManifest(ops []raft.BatchFileOp) error {
	c.batches++
	if c.onBatch != nil {
		c.onBatch(c.batches)
	}
	return c.apply(raft.CommandBatchFileOps, raft.BatchFileOpsPayload{Ops: ops})
}
func (c *fsmCoordinator) GetFileEntry(p string) (*raft.FileEntry, bool) { return c.fsm.GetFile(p) }
func (c *fsmCoordinator) IsPrimaryWriter() bool                         { return true }
func (c *fsmCoordinator) Role() string                                  { return "writer" }
func (c *fsmCoordinator) LocalNodeID() string                           { return "w1" }
func (c *fsmCoordinator) CompactionCommitActive() bool                  { return c.active }
func (c *fsmCoordinator) SyncManifest(context.Context) error {
	c.syncs++
	if c.onSync != nil {
		c.onSync()
	}
	return c.syncErr
}
func (c *fsmCoordinator) WaitLocalFiles(context.Context, string, string, time.Duration) ([]string, error) {
	c.waits++
	return c.missing, nil
}

func fileSHA(t *testing.T, full string) (string, int64) {
	t.Helper()
	b, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), int64(len(b))
}

func casEntry(t *testing.T, root, rel string) raft.FileEntry {
	sha, size := fileSHA(t, filepath.Join(root, rel))
	return raft.FileEntry{Path: rel, SHA256: sha, SizeBytes: size, Database: "db", Measurement: "cpu",
		PartitionTime: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), OriginNodeID: "w1", Tier: "hot", CreatedAt: time.Now()}
}

// raceDelete runs a DELETE whose first manifest batch finds that a compaction
// (on w2, which read cpu_a before the DELETE) committed meanwhile: this node
// then drops its copy of the input. missing is what WaitLocalFiles reports.
func raceDelete(t *testing.T, where string, missing []string, extra ...string) (int, string, *fsmCoordinator, string) {
	t.Helper()
	root, backend, db := recordsDuckDB(t)
	const a = "db/cpu/2026/10/11/10/cpu_a.parquet"
	const o = "db/cpu/2026/10/11/10/cpu_o.parquet"
	writeRecordsParquet(t, db, root, a)
	fsm := raft.NewClusterFSM(zerolog.Nop())
	c := &fsmCoordinator{t: t, fsm: fsm, active: true, db: db, root: root}
	for _, n := range []string{"w1", "w2"} {
		if err := c.apply(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: n, Role: "writer", State: "healthy"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.apply(raft.CommandEnableCompactionCommit, raft.EnableCompactionCommitPayload{Nodes: []string{"w1", "w2"}}); err != nil {
		t.Fatal(err)
	}
	ae := casEntry(t, root, a)
	if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: ae}); err != nil {
		t.Fatal(err)
	}
	c.onBatch = func(n int) {
		if n != 1 {
			return
		}
		writeRecordsParquet(t, db, root, o)
		oe := casEntry(t, root, o)
		if err := c.apply(raft.CommandStageCompaction, raft.StageCompactionPayload{Job: "j1", Origin: "w2", Output: oe,
			Inputs: []raft.CompactionInput{{Path: a, SizeBytes: ae.SizeBytes, SHA256: ae.SHA256}}}); err != nil {
			t.Fatal(err)
		}
		if err := c.apply(raft.CommandCommitCompaction, raft.CommitCompactionPayload{Job: "j1", Origin: "w2", Output: o, SHA256: oe.SHA256}); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(root, a)); err != nil {
			t.Fatal(err)
		}
		// The output may not be here yet.
		c.missing = missing
	}
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetCoordinator(c)
	app := fiber.New()
	h.RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"`+where+`","confirm":true`+strings.Join(extra, "")+`}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 20000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw), c, root
}

func onDisk(t *testing.T, root string) []string {
	t.Helper()
	entries, _ := os.ReadDir(filepath.Join(root, "db/cpu/2026/10/11/10"))
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// The compaction commits between the DELETE's scan and its manifest batch:
// the batch is refused, the DELETE scans again and rewrites the output. Before
// d2 the rewrite of the input registered next to the output, which still held
// the deleted rows.
func TestDeleteScansAgainWhenACompactionReplacedTheFile(t *testing.T) {
	code, raw, c, root := raceDelete(t, "host = 'h1'", nil)
	var out DeleteResponse
	_ = json.Unmarshal([]byte(raw), &out)
	if code != 200 || out.DeletedCount != 2 {
		t.Fatalf("delete: %d %s", code, raw)
	}
	if c.waits != 2 {
		t.Fatalf("waited for local files %d times, want before each scan", c.waits)
	}
	// Exactly one registered file, the rewrite of the output, without h1.
	files := c.fsm.GetFilesByDatabase("db")
	if len(files) != 1 || strings.HasSuffix(files[0].Path, "cpu_o.parquet") || strings.HasSuffix(files[0].Path, "cpu_a.parquet") {
		t.Fatalf("registered: %+v", files)
	}
	if n := countParquet(t, c.db, filepath.Join(root, files[0].Path), "host = 'h1'"); n != 0 {
		t.Fatalf("deleted rows in the registered file: %d", n)
	}
	if n := countParquet(t, c.db, filepath.Join(root, files[0].Path), "true"); n != 2 {
		t.Fatalf("surviving rows: %d", n)
	}
	// The refused rewrite of the input was removed, not left for glob reads.
	if names := onDisk(t, root); len(names) != 1 {
		t.Fatalf("files on disk: %v", names)
	}
}

// Every row of the input matched: the full-file delete is refused the same
// way, and the output is deleted instead.
func TestDeleteScansAgainOnAFullFileDelete(t *testing.T) {
	code, raw, c, root := raceDelete(t, "v > 0", nil)
	var out DeleteResponse
	_ = json.Unmarshal([]byte(raw), &out)
	if code != 200 || out.DeletedCount != 4 {
		t.Fatalf("delete: %d %s", code, raw)
	}
	if files := c.fsm.GetFilesByDatabase("db"); len(files) != 0 {
		t.Fatalf("registered: %+v", files)
	}
	if names := onDisk(t, root); len(names) != 0 {
		t.Fatalf("files on disk: %v", names)
	}
}

// The output is not on this node: the DELETE does not scan without it.
func TestDeleteRescanWaitsForTheOutput(t *testing.T) {
	code, raw, c, _ := raceDelete(t, "host = 'h1'", []string{"db/cpu/2026/10/11/10/cpu_o.parquet"})
	if code != 503 || !strings.Contains(raw, "not on this node") || c.waits != 2 {
		t.Fatalf("got %d (waits %d): %s", code, c.waits, raw)
	}
}

// A file that keeps changing is not chased forever: 503, the rows of the
// other files deleted.
func TestDeleteGivesUpOnAFileThatKeepsChanging(t *testing.T) {
	root, backend, db := recordsDuckDB(t)
	fsm := raft.NewClusterFSM(zerolog.Nop())
	c := &fsmCoordinator{t: t, fsm: fsm, active: true}
	if err := c.apply(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: "w1", Role: "writer", State: "healthy"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.apply(raft.CommandEnableCompactionCommit, raft.EnableCompactionCommitPayload{Nodes: []string{"w1"}}); err != nil {
		t.Fatal(err)
	}
	const a = "db/cpu/2026/10/11/10/cpu_a.parquet"
	writeRecordsParquet(t, db, root, a)
	ae := casEntry(t, root, a)
	if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: ae}); err != nil {
		t.Fatal(err)
	}
	// Every batch finds a registered at another checksum.
	c.onBatch = func(int) {
		e, _ := fsm.GetFile(a)
		cp := *e
		cp.SHA256 += "x"
		if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: cp}); err != nil {
			t.Fatal(err)
		}
	}
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetCoordinator(c)
	app := fiber.New()
	h.RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 20000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || c.batches != 3 || !strings.Contains(string(raw), "kept changing") {
		t.Fatalf("got %d after %d batches: %s", resp.StatusCode, c.batches, raw)
	}
}

// The DELETE's ops carry the content it read only with the flag on, never
// to an FSM that ignores them.
func TestDeleteOpsCarryTheContentRead(t *testing.T) {
	root, backend, db := recordsDuckDB(t)
	const a = "db/cpu/2026/10/11/10/cpu_a.parquet"
	for _, active := range []bool{true, false} {
		if err := os.RemoveAll(filepath.Join(root, "db")); err != nil {
			t.Fatal(err)
		}
		writeRecordsParquet(t, db, root, a)
		fsm := raft.NewClusterFSM(zerolog.Nop())
		c := &fsmCoordinator{t: t, fsm: fsm, active: active}
		ae := casEntry(t, root, a)
		if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: ae}); err != nil {
			t.Fatal(err)
		}
		var seen []raft.BatchFileOp
		c.onBatch = func(int) {}
		rec := &opsRecorder{fsmCoordinator: c, seen: &seen}
		h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
		h.SetCoordinator(rec)
		for _, where := range []string{"host = 'h1'", "true"} {
			app := fiber.New()
			h.RegisterRoutes(app)
			req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"`+where+`","confirm":true}`))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := app.Test(req, 20000); err != nil || resp.StatusCode != 200 {
				t.Fatalf("active=%v where=%s: %v %v", active, where, err, resp)
			}
		}
		if len(seen) != 3 {
			t.Fatalf("ops: %d", len(seen))
		}
		var reg raft.RegisterFilePayload
		var del, full raft.DeleteFilePayload
		_ = json.Unmarshal(seen[0].Payload, &reg)
		_ = json.Unmarshal(seen[1].Payload, &del)
		_ = json.Unmarshal(seen[2].Payload, &full)
		want := ""
		if active {
			want = ae.SHA256
		}
		if reg.InheritSHA != want || del.ExpectSHA != want {
			t.Fatalf("active=%v: rewrite ops carry %q/%q, want %q", active, reg.InheritSHA, del.ExpectSHA, want)
		}
		if active && full.ExpectSHA != reg.File.SHA256 || !active && full.ExpectSHA != "" {
			t.Fatalf("active=%v: full delete carries %q", active, full.ExpectSHA)
		}
	}
}

type opsRecorder struct {
	*fsmCoordinator
	seen *[]raft.BatchFileOp
}

func (r *opsRecorder) BatchFileOpsInManifest(ops []raft.BatchFileOp) error {
	*r.seen = append(*r.seen, ops...)
	return r.fsmCoordinator.BatchFileOpsInManifest(ops)
}

// casCluster: a primary w1 and a compactor w2 with compaction commit on, and
// the given files written and registered.
func casCluster(t *testing.T, files ...string) (*fsmCoordinator, *DeleteHandler, string) {
	t.Helper()
	root, backend, db := recordsDuckDB(t)
	c := &fsmCoordinator{t: t, fsm: raft.NewClusterFSM(zerolog.Nop()), active: true, db: db, root: root}
	for _, n := range []string{"w1", "w2"} {
		if err := c.apply(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: n, Role: "writer", State: "healthy"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.apply(raft.CommandEnableCompactionCommit, raft.EnableCompactionCommitPayload{Nodes: []string{"w1", "w2"}}); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		writeRecordsParquet(t, db, root, f)
		if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: casEntry(t, root, f)}); err != nil {
			t.Fatal(err)
		}
	}
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetCoordinator(c)
	return c, h, root
}

// compact commits a compaction of in (on w2) into out, written with the
// same rows.
func (c *fsmCoordinator) compact(job, in, out string) {
	c.t.Helper()
	e, ok := c.fsm.GetFile(in)
	if !ok {
		c.t.Fatalf("%s not registered", in)
	}
	writeRecordsParquet(c.t, c.db, c.root, out)
	oe := casEntry(c.t, c.root, out)
	if err := c.apply(raft.CommandStageCompaction, raft.StageCompactionPayload{Job: job, Origin: "w2", Output: oe,
		Inputs: []raft.CompactionInput{{Path: in, SizeBytes: e.SizeBytes, SHA256: e.SHA256}}}); err != nil {
		c.t.Fatal(err)
	}
	if err := c.apply(raft.CommandCommitCompaction, raft.CommitCompactionPayload{Job: job, Origin: "w2", Output: out, SHA256: oe.SHA256}); err != nil {
		c.t.Fatal(err)
	}
}

func sendDelete(t *testing.T, h *DeleteHandler, body string) (int, DeleteResponse, string) {
	t.Helper()
	app := fiber.New()
	h.RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 20000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out DeleteResponse
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, string(raw)
}

const casA, casB, casO = "db/cpu/2026/10/11/10/cpu_a.parquet", "db/cpu/2026/10/11/10/cpu_b.parquet", "db/cpu/2026/10/11/10/cpu_o.parquet"

// b is compacted after the scan and before the DELETE reaches it, and this
// node still has its copy (an unlink waits deleteGrace): the manifest no
// longer lists b, so the DELETE does not rewrite-and-orphan it, nor delete
// it; it scans again and deletes from the output.
func TestDeleteSkipsAFileCompactedAfterTheScan(t *testing.T) {
	for _, unlinked := range []bool{false, true} {
		c, h, root := casCluster(t, casA, casB)
		c.onBatch = func(n int) {
			if n != 1 {
				return
			}
			c.compact("j1", casB, casO)
			if unlinked {
				if err := os.Remove(filepath.Join(root, casB)); err != nil {
					t.Fatal(err)
				}
			}
		}
		code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`)
		if code != 200 || out.DeletedCount != 4 || out.AffectedFiles != 3 {
			t.Fatalf("unlinked=%v: %d %s", unlinked, code, raw)
		}
		files := c.fsm.GetFilesByDatabase("db")
		if len(files) != 2 {
			t.Fatalf("unlinked=%v: registered %+v", unlinked, files)
		}
		for _, f := range files {
			if n := countParquet(t, c.db, filepath.Join(root, f.Path), "host = 'h1'"); n != 0 {
				t.Fatalf("unlinked=%v: %s keeps deleted rows", unlinked, f.Path)
			}
		}
		if !unlinked {
			// b is the peer delete path's to remove, untouched here.
			if n := countParquet(t, c.db, filepath.Join(root, casB), "host = 'h1'"); n != 2 {
				t.Fatalf("b was rewritten in place: %d", n)
			}
		}
	}
}

// A file in storage the manifest does not list (an output before its
// commit) is left alone: rewriting it would orphan the rewrite.
func TestDeleteLeavesUnregisteredFilesAlone(t *testing.T) {
	c, h, root := casCluster(t, casA)
	writeRecordsParquet(t, c.db, root, casO)
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`)
	if code != 200 || out.DeletedCount != 2 || out.AffectedFiles != 1 {
		t.Fatalf("%d %s", code, raw)
	}
	if n := countParquet(t, c.db, filepath.Join(root, casO), "true"); n != 4 {
		t.Fatalf("the unregistered file was touched: %d rows", n)
	}
	// With the flag off it is scanned, as before.
	c2, h2, root2 := casCluster(t, casA)
	c2.active = false
	writeRecordsParquet(t, c2.db, root2, casO)
	if code, out, raw = sendDelete(t, h2, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`); code != 200 || out.AffectedFiles != 2 {
		t.Fatalf("flag off: %d %s", code, raw)
	}
}

// allow_unreachable holds for the scan after a change too, and the files
// it skipped are reported.
func TestDeleteRescanHonoursAllowUnreachable(t *testing.T) {
	code, raw, _, _ := raceDelete(t, "host = 'h1'", []string{"db/cpu/2026/10/11/10/cpu_o.parquet"}, `,"allow_unreachable":true`)
	var out DeleteResponse
	_ = json.Unmarshal([]byte(raw), &out)
	if code != 200 || len(out.UnreachableFiles) != 1 {
		t.Fatalf("%d %s", code, raw)
	}
}

// The DELETE sends the checksum it scanned, not the one registered when it
// gets to the file: b registered with another content after the scan is
// changed under it, scanned again, and only then rewritten.
func TestDeleteUsesTheChecksumItScanned(t *testing.T) {
	for _, where := range []string{"host = 'h1'", "v > 0"} {
		c, h, _ := casCluster(t, casA, casB)
		c.onBatch = func(n int) {
			if n != 1 {
				return
			}
			e, _ := c.fsm.GetFile(casB)
			cp := *e
			cp.SHA256 = "replaced"
			if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: cp}); err != nil {
				t.Fatal(err)
			}
		}
		code, _, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"`+where+`","confirm":true}`)
		if code != 200 || c.waits != 2 {
			t.Fatalf("%s: %d after %d waits: %s", where, code, c.waits, raw)
		}
	}
}

// A file that failed in one scan is not retried by the next one: it is
// reported once.
func TestDeleteDoesNotRetryAFailedFileAfterAChange(t *testing.T) {
	const b11 = "db/cpu/2026/10/11/11/cpu_b.parquet"
	c, h, root := casCluster(t, casA, b11)
	dir := filepath.Join(root, "db/cpu/2026/10/11/11")
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	c.onBatch = func(n int) {
		if n != 1 {
			return
		}
		c.compact("j1", casA, casO)
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`)
	if code != 207 || len(out.FailedFiles) != 1 || out.DeletedCount != 2 || c.waits != 2 {
		t.Fatalf("%d after %d waits: %s", code, c.waits, raw)
	}
}

// holdingOutbox stands in for the registration outbox: it holds the paths
// in held (pending registration).
type holdingOutbox struct{ held map[string]bool }

func (o *holdingOutbox) ReplacePending(context.Context, string, string, int64, string) (cluster.ReplaceOutcome, raft.FileEntry, error) {
	return cluster.ReplaceSwapped, raft.FileEntry{}, nil
}
func (o *holdingOutbox) Withdraw(context.Context, string) (bool, cluster.ReplaceOutcome, error) {
	return true, cluster.ReplaceSwapped, nil
}
func (o *holdingOutbox) HoldsRegistration(p string) bool { return o.held[p] }

// A file this node is still registering is the DELETE's even though the
// manifest does not list it yet; another unregistered file is not.
func TestDeleteScansFilesStillPendingRegistration(t *testing.T) {
	c, h, root := casCluster(t)
	writeRecordsParquet(t, c.db, root, casA)
	writeRecordsParquet(t, c.db, root, casO)
	h.SetPendingRegistrations(&holdingOutbox{held: map[string]bool{casA: true}})
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`)
	if code != 200 || out.AffectedFiles != 1 || out.DeletedCount != 2 || out.FilesProcessed[0] != "cpu_a.parquet" {
		t.Fatalf("%d %s", code, raw)
	}
}

// The scan catches up with the leader first: a file registered there a
// moment ago (an earlier DELETE's rewrite) is applied here before the
// unregistered files are dropped. Not catching up refuses the DELETE.
func TestDeleteScanCatchesUpWithTheManifest(t *testing.T) {
	c, h, root := casCluster(t)
	writeRecordsParquet(t, c.db, root, casA)
	registered := false
	c.onSync = func() {
		if !registered {
			registered = true
			if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: casEntry(t, root, casA)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`)
	if code != 200 || out.DeletedCount != 2 || c.syncs != 1 {
		t.Fatalf("%d after %d syncs: %s", code, c.syncs, raw)
	}
	c.syncErr = errors.New("no leader")
	if code, _, raw = sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h2'","confirm":true}`); code != 503 || !strings.Contains(raw, "catch up") {
		t.Fatalf("not synced: %d %s", code, raw)
	}
}

// With compaction commit on (stream takeover off), the DELETE waits for the
// manifest files of the measurement before it scans: the inputs of a
// compaction are unregistered once it commits, and its output may not be here
// yet. Without the wait the DELETE answered success having deleted nothing.
func TestDeleteWaitsForManifestFilesBeforeTheScan(t *testing.T) {
	const body = `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`
	c, h, _ := casCluster(t, casA)
	c.missing = []string{casO}
	code, out, raw := sendDelete(t, h, body)
	if code != 503 || c.waits != 1 || len(out.UnreachableFiles) != 1 || out.DeletedCount != 0 {
		t.Fatalf("%d after %d waits: %s", code, c.waits, raw)
	}
	code, out, raw = sendDelete(t, h, strings.Replace(body, `"confirm":true`, `"confirm":true,"allow_unreachable":true`, 1))
	if code != 200 || out.DeletedCount != 2 || len(out.UnreachableFiles) != 1 {
		t.Fatalf("allowed: %d %s", code, raw)
	}
	// A dry run does not wait; with the flag off nothing waits.
	waits := c.waits
	if code, _, raw = sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h2'","dry_run":true}`); code != 200 || c.waits != waits {
		t.Fatalf("dry run: %d %s", code, raw)
	}
	c.active = false
	if code, _, raw = sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h2'","confirm":true}`); code != 200 || c.waits != waits {
		t.Fatalf("flag off: %d %s", code, raw)
	}
}
