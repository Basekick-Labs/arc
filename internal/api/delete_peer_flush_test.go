package api

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1265: in shared storage every writer takes writes, so a DELETE first has
// the other nodes flush the rows of its scope they buffer.

// peerFlushCoordinator is a shared-storage primary whose peers' flush is
// scripted: flush runs as the peers' flush would (writing and registering
// their files), unflushed names the peers that could not.
type peerFlushCoordinator struct {
	*sharedCoordinator
	inactive  bool
	unflushed []string
	err       error
	flush     func()
	scopes    []string
}

func (c *peerFlushCoordinator) PeerFlushActive() bool { return !c.inactive }

func (c *peerFlushCoordinator) FlushPeersForDelete(_ context.Context, database, measurement string) ([]string, error) {
	c.scopes = append(c.scopes, database+"/"+measurement)
	if c.flush != nil {
		c.flush()
	}
	return c.unflushed, c.err
}

func (c *peerFlushCoordinator) HasRaft() bool { return true }

func (c *peerFlushCoordinator) GetFileManifestByDatabase(database string) []*raft.FileEntry {
	return c.fsm.GetFilesByDatabase(database)
}

func (c *peerFlushCoordinator) BatchFileOpsInManifestContext(_ context.Context, ops []raft.BatchFileOp) error {
	return c.BatchFileOpsInManifest(ops)
}

func newDatabasesTestHandler(t *testing.T, root string) *DatabasesHandler {
	t.Helper()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	return NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
}

func sendDatabaseDelete(t *testing.T, h *DatabasesHandler, name, query string) (int, string) {
	t.Helper()
	app := fiber.New()
	h.RegisterRoutes(app)
	url := "/api/v1/databases/" + name + "?confirm=true"
	if query != "" {
		url += "&" + query
	}
	resp, err := app.Test(httptest.NewRequest("DELETE", url, nil), 20000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func peerFlushCluster(t *testing.T, files ...string) (*peerFlushCoordinator, *DeleteHandler, string) {
	t.Helper()
	s, h, root := sharedCluster(t, files...)
	c := &peerFlushCoordinator{sharedCoordinator: s}
	h.SetCoordinator(c)
	return c, h, root
}

// The rows another writer acknowledged before the DELETE and still buffered
// are flushed and registered by it first, and deleted with the rest. Without
// the peer flush they reach storage after the scan and survive.
func TestDeleteFlushesOtherWritersFirst(t *testing.T) {
	c, h, root := peerFlushCluster(t, casA)
	c.flush = func() {
		writeRecordsParquet(t, c.db, root, casB)
		if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: casEntry(t, root, casB)}); err != nil {
			t.Fatal(err)
		}
	}
	code, out, raw := sendDelete(t, h, deleteH1)
	if code != 200 || out.DeletedCount != 4 || len(out.UnflushedNodes) != 0 {
		t.Fatalf("%d %s", code, raw)
	}
	if len(c.scopes) != 1 || c.scopes[0] != "db/cpu" {
		t.Fatalf("peers asked for %v, want db/cpu once", c.scopes)
	}
	for _, p := range onDiskPaths(t, root) {
		if n := countParquet(t, c.db, filepath.Join(root, p), "host = 'h1'"); n != 0 {
			t.Fatalf("%s keeps rows the delete matched", p)
		}
	}
}

// A node that could not flush refuses the DELETE (nothing deleted), named;
// with allow_unreachable it runs, and still names it.
func TestDeleteRefusesWhileANodeCouldNotFlush(t *testing.T) {
	c, h, root := peerFlushCluster(t, casA)
	c.unflushed = []string{"node writer-2 (state dead): no answer"}
	code, out, raw := sendDelete(t, h, deleteH1)
	if code != 503 || len(out.UnflushedNodes) != 1 || !strings.Contains(out.Error, "allow_unreachable") {
		t.Fatalf("%d %s", code, raw)
	}
	if n := countParquet(t, c.db, filepath.Join(root, casA), "host = 'h1'"); n != 2 {
		t.Fatalf("refused, yet rows were deleted: %d left", n)
	}
	code, out, raw = sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true,"allow_unreachable":true}`)
	if code != 200 || out.DeletedCount != 2 || len(out.UnflushedNodes) != 1 {
		t.Fatalf("allowed: %d %s", code, raw)
	}

	c.unflushed, c.err = nil, errors.New("manifest behind")
	if code, _, raw := sendDelete(t, h, deleteH1); code != 503 {
		t.Fatalf("a manifest that could not catch up: %d %s", code, raw)
	}
}

// A dry run changes nothing, so it does not make other nodes flush; nor does
// a cluster outside shared storage.
func TestDeletePeerFlushNotAskedForDryRunOrOutsideSharedStorage(t *testing.T) {
	c, h, _ := peerFlushCluster(t, casA)
	if code, _, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","dry_run":true}`); code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	c.inactive = true
	if code, _, raw := sendDelete(t, h, deleteH1); code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	if len(c.scopes) != 0 {
		t.Fatalf("peers asked for %v", c.scopes)
	}
}

// The database deletion has the other nodes flush the whole database first,
// under a deadline of its own, and is refused while one could not; and it
// leaves a fresh unregistered object (another writer's flush on its way to
// the manifest) and answers 503, rather than delete it.
func TestDatabaseDeleteFlushesOtherNodesFirst(t *testing.T) {
	c, _, root := peerFlushCluster(t, casA)
	h := newDatabasesTestHandler(t, root)
	h.SetCoordinator(c, true)
	c.unflushed = []string{"node writer-2 (state dead): no answer"}
	code, body := sendDatabaseDelete(t, h, "db", "")
	if code != 503 || !strings.Contains(body, "writer-2") {
		t.Fatalf("%d %s", code, body)
	}
	if _, err := os.Stat(filepath.Join(root, casA)); err != nil {
		t.Fatal("refused, yet files were deleted")
	}
	if len(c.scopes) != 1 || c.scopes[0] != "db/" {
		t.Fatalf("peers asked for %v, want the whole database", c.scopes)
	}

	c.unflushed = nil
	writeRecordsParquet(t, c.db, root, casB) // unregistered, just flushed
	code, body = sendDatabaseDelete(t, h, "db", "")
	if code != 503 || !strings.Contains(body, "not registered") {
		t.Fatalf("fresh unregistered object: %d %s", code, body)
	}
	if _, err := os.Stat(filepath.Join(root, casB)); err != nil {
		t.Fatal("the fresh unregistered object was deleted")
	}
	ageObject(t, root, casB)
	c.unflushed = []string{"node writer-2 (state dead): no answer"}
	code, body = sendDatabaseDelete(t, h, "db", "allow_unreachable=true")
	if code != 200 || !strings.Contains(body, "unflushed_nodes") {
		t.Fatalf("allowed: %d %s", code, body)
	}
}

// heldOutbox is this node's registration outbox as a database deletion sees
// it: what it holds, until the deletion withdraws it.
type heldOutbox struct{ held map[string]bool }

func (o *heldOutbox) HoldsRegistration(p string) bool { return o.held[p] }

func (o *heldOutbox) WithdrawDatabase(context.Context, string, uint64) error {
	o.held = map[string]bool{}
	return nil
}

// A fresh unregistered file this node held for registration is its own,
// withdrawn by the deletion: deleted with the database, not a reason to 503.
func TestDatabaseDeleteRemovesItsOwnFreshFiles(t *testing.T) {
	c, _, root := peerFlushCluster(t, casA)
	h := newDatabasesTestHandler(t, root)
	h.SetCoordinator(c, true)
	writeRecordsParquet(t, c.db, root, casB)
	h.SetPendingRegistrations(&heldOutbox{held: map[string]bool{casB: true}})
	if code, body := sendDatabaseDelete(t, h, "db", ""); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if _, err := os.Stat(filepath.Join(root, casB)); err == nil {
		t.Fatal("this node's own file was kept")
	}
}

// A file flushed while the deletion runs (after it checked for unregistered
// files) holds rows written during it: left, not deleted, so its writer
// does not register a file that is gone.
func TestDatabaseDeleteLeavesAFlushThatLandsWhileItRuns(t *testing.T) {
	c, _, root := peerFlushCluster(t, casA)
	h := newDatabasesTestHandler(t, root)
	h.SetCoordinator(c, true)
	h.SetPendingRegistrations(&landingOutbox{land: func() { writeRecordsParquet(t, c.db, root, casO) }})
	if code, body := sendDatabaseDelete(t, h, "db", ""); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if _, err := os.Stat(filepath.Join(root, casA)); err == nil {
		t.Fatal("the registered file was kept")
	}
	if _, err := os.Stat(filepath.Join(root, casO)); err != nil {
		t.Fatal("the flush that landed during the deletion was deleted")
	}
}

// landingOutbox runs land when the deletion withdraws its registrations:
// between its check and its listing.
type landingOutbox struct{ land func() }

func (o *landingOutbox) HoldsRegistration(string) bool { return false }

func (o *landingOutbox) WithdrawDatabase(context.Context, string, uint64) error {
	o.land()
	return nil
}

// listlessBackend hides the object listing, so an object's age comes from
// its name alone.
type listlessBackend struct{ storage.Backend }

// Of unknown age (no listing, not a flush's name) an object is not fresh:
// left alone it would block a DELETE for ever.
func TestFreshUnregisteredOfUnknownAge(t *testing.T) {
	_, h, root := peerFlushCluster(t)
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	h.storage = listlessBackend{backend}
	if h.freshUnregistered(context.Background(), "db/cpu/2026/10/11/10/cpu_a_rewrite_123.parquet") {
		t.Fatal("an object of unknown age read as fresh")
	}
}
