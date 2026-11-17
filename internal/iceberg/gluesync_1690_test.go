package iceberg

// #1690 option B: the Glue sync against an in-memory Glue.

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
	"github.com/rs/zerolog"
)

type fakeGlue struct {
	mu      sync.Mutex
	dbs     map[string]map[string]string
	tables  map[string]*gluetypes.Table // db\x00name
	writes  []string                    // "create db.t", "update db.t", "delete db.t", "createdb db"
	calls   int
	failOn  map[string]error // method -> error, consumed once
	catalog string
	skipArc []bool // SkipArchive of each UpdateTable
}

func newFakeGlue() *fakeGlue {
	return &fakeGlue{dbs: map[string]map[string]string{}, tables: map[string]*gluetypes.Table{}, failOn: map[string]error{}}
}

func (f *fakeGlue) fail(method string) error {
	f.calls++
	if err, ok := f.failOn[method]; ok {
		delete(f.failOn, method)
		return err
	}
	return nil
}

func notFound() error { return &gluetypes.EntityNotFoundException{Message: aws.String("not found")} }

func (f *fakeGlue) GetDatabase(_ context.Context, in *glue.GetDatabaseInput, _ ...func(*glue.Options)) (*glue.GetDatabaseOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("GetDatabase"); err != nil {
		return nil, err
	}
	if p, ok := f.dbs[*in.Name]; ok {
		return &glue.GetDatabaseOutput{Database: &gluetypes.Database{Name: in.Name, Parameters: p}}, nil
	}
	return nil, notFound()
}

func (f *fakeGlue) CreateDatabase(_ context.Context, in *glue.CreateDatabaseInput, _ ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("CreateDatabase"); err != nil {
		return nil, err
	}
	f.dbs[*in.DatabaseInput.Name] = in.DatabaseInput.Parameters
	f.writes = append(f.writes, "createdb "+*in.DatabaseInput.Name)
	return &glue.CreateDatabaseOutput{}, nil
}

func (f *fakeGlue) GetDatabases(_ context.Context, in *glue.GetDatabasesInput, _ ...func(*glue.Options)) (*glue.GetDatabasesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("GetDatabases"); err != nil {
		return nil, err
	}
	out := &glue.GetDatabasesOutput{}
	for n, p := range f.dbs {
		out.DatabaseList = append(out.DatabaseList, gluetypes.Database{Name: aws.String(n), Parameters: p})
	}
	return out, nil
}

func (f *fakeGlue) GetTable(_ context.Context, in *glue.GetTableInput, _ ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("GetTable"); err != nil {
		return nil, err
	}
	if t, ok := f.tables[*in.DatabaseName+"\x00"+*in.Name]; ok {
		c := *t
		return &glue.GetTableOutput{Table: &c}, nil
	}
	return nil, notFound()
}

func (f *fakeGlue) GetTables(_ context.Context, in *glue.GetTablesInput, _ ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("GetTables"); err != nil {
		return nil, err
	}
	out := &glue.GetTablesOutput{}
	for k, t := range f.tables {
		if strings.HasPrefix(k, *in.DatabaseName+"\x00") {
			out.TableList = append(out.TableList, *t)
		}
	}
	return out, nil
}

func (f *fakeGlue) put(db string, in *gluetypes.TableInput, version int) {
	f.tables[db+"\x00"+*in.Name] = &gluetypes.Table{
		Name: in.Name, DatabaseName: aws.String(db), TableType: in.TableType, Parameters: in.Parameters,
		StorageDescriptor: in.StorageDescriptor, Description: in.Description, VersionId: aws.String(strconv.Itoa(version)),
	}
}

func (f *fakeGlue) CreateTable(_ context.Context, in *glue.CreateTableInput, _ ...func(*glue.Options)) (*glue.CreateTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("CreateTable"); err != nil {
		return nil, err
	}
	if _, ok := f.tables[*in.DatabaseName+"\x00"+*in.TableInput.Name]; ok {
		return nil, &gluetypes.AlreadyExistsException{Message: aws.String("exists")}
	}
	f.catalog = aws.ToString(in.CatalogId)
	f.put(*in.DatabaseName, in.TableInput, 1)
	f.writes = append(f.writes, "create "+*in.DatabaseName+"."+*in.TableInput.Name)
	return &glue.CreateTableOutput{}, nil
}

func (f *fakeGlue) UpdateTable(_ context.Context, in *glue.UpdateTableInput, _ ...func(*glue.Options)) (*glue.UpdateTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("UpdateTable"); err != nil {
		return nil, err
	}
	key := *in.DatabaseName + "\x00" + *in.TableInput.Name
	cur, ok := f.tables[key]
	if !ok {
		return nil, notFound()
	}
	if in.VersionId != nil && *in.VersionId != *cur.VersionId {
		return nil, &gluetypes.ConcurrentModificationException{Message: aws.String("version")}
	}
	v, _ := strconv.Atoi(*cur.VersionId)
	f.put(*in.DatabaseName, in.TableInput, v+1)
	f.skipArc = append(f.skipArc, aws.ToBool(in.SkipArchive))
	f.writes = append(f.writes, "update "+*in.DatabaseName+"."+*in.TableInput.Name)
	return &glue.UpdateTableOutput{}, nil
}

func (f *fakeGlue) DeleteTable(_ context.Context, in *glue.DeleteTableInput, _ ...func(*glue.Options)) (*glue.DeleteTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("DeleteTable"); err != nil {
		return nil, err
	}
	delete(f.tables, *in.DatabaseName+"\x00"+*in.Name)
	f.writes = append(f.writes, "delete "+*in.DatabaseName+"."+*in.Name)
	return &glue.DeleteTableOutput{}, nil
}

func (f *fakeGlue) table(db, name string) *gluetypes.Table {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tables[db+"\x00"+name]
}

func (f *fakeGlue) takeWrites() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.writes
	f.writes = nil
	return w
}

type glueRig struct {
	*colocRig
	glue *fakeGlue
	sync *GlueSync
	gate *switchGate
}

type switchGate struct{ open bool }

func (g *switchGate) CanRun() bool { return g.open }

func newGlueRig(t *testing.T) *glueRig {
	t.Helper()
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newPublishedRig(t, root, "", layoutWarehouse)
	fg := newFakeGlue()
	gate := &switchGate{open: true}
	gs := NewGlueSync(GlueSyncConfig{Exporter: r.exp, API: fg, CatalogID: "123456789012", Interval: time.Hour, Gate: gate, Logger: zerolog.Nop()})
	r.exp.SetOnPublished(gs.Notify)
	return &glueRig{colocRig: r, glue: fg, sync: gs, gate: gate}
}

// publish writes another file and runs an export pass, which notifies the sync.
func (r *glueRig) publish(t *testing.T, name string, ts int64) {
	t.Helper()
	writeArcStyleParquet(t, filepath.Join(r.root, "mydb/cpu/2023/11/14/22", name), ts, 5)
	r.pass(t)
}

func (r *glueRig) published(t *testing.T) string {
	t.Helper()
	return mustLoadPublished(t, r.exp, "mydb", "cpu").MetadataLocation
}

func TestGlueSyncRegistersAndFollowsPublishes(t *testing.T) {
	r := newGlueRig(t)
	r.pass(t) // the export's publish notifies the sync
	r.sync.dirtyPass(context.Background())

	tbl := r.glue.table("arc_mydb", "cpu")
	if tbl == nil {
		t.Fatalf("no Glue table; writes %v", r.glue.takeWrites())
	}
	p := tbl.Parameters
	if p["table_type"] != "ICEBERG" || p["metadata_location"] != r.published(t) || p["arc.synced_location"] != p["metadata_location"] ||
		p["arc.managed"] != "true" || p["arc.database"] != "mydb" || p["arc.measurement"] != "cpu" || p["arc.namespace_prefix"] != "arc" {
		t.Fatalf("Glue table parameters = %v", p)
	}
	if !strings.HasSuffix(p["metadata_location"], ".metadata.json") || !strings.Contains(path.Base(p["metadata_location"]), "v") {
		t.Fatalf("Glue points at %s, want a v<N>.metadata.json", p["metadata_location"])
	}
	if aws.ToString(tbl.TableType) != "EXTERNAL_TABLE" || len(tbl.StorageDescriptor.Columns) != 3 {
		t.Fatalf("table type %v, columns %v", aws.ToString(tbl.TableType), tbl.StorageDescriptor.Columns)
	}
	if r.glue.catalog != "123456789012" {
		t.Fatalf("catalog id = %q", r.glue.catalog)
	}

	// An operator's parameter and column comment survive updates.
	r.glue.mu.Lock()
	tbl.Parameters["owner_team"] = "data"
	tbl.StorageDescriptor.Columns[1].Comment = aws.String("the host")
	r.glue.mu.Unlock()
	first := p["metadata_location"]
	r.glue.takeWrites()
	r.publish(t, "cpu_b.parquet", 1_700_000_100_000_000)
	r.sync.dirtyPass(context.Background())
	tbl = r.glue.table("arc_mydb", "cpu")
	p = tbl.Parameters
	if p["metadata_location"] != r.published(t) || p["previous_metadata_location"] != first {
		t.Fatalf("after a publish: metadata %s (published %s), previous %s (want %s)", p["metadata_location"], r.published(t), p["previous_metadata_location"], first)
	}
	if p["owner_team"] != "data" || aws.ToString(tbl.StorageDescriptor.Columns[1].Comment) != "the host" {
		t.Fatalf("the operator's parameter or comment was lost: %v %v", p, tbl.StorageDescriptor.Columns)
	}
	for _, s := range r.glue.skipArc {
		if !s {
			t.Fatal("UpdateTable without SkipArchive: every publish would archive a Glue table version")
		}
	}

	// Steady state: nothing published, nothing written; a repeated notice of
	// the same version makes no Glue call at all.
	r.glue.takeWrites()
	calls := r.glue.calls
	r.sync.dirtyPass(context.Background())
	r.sync.Notify("mydb", "cpu")
	r.sync.dirtyPass(context.Background())
	if w := r.glue.takeWrites(); len(w) != 0 || r.glue.calls != calls {
		t.Fatalf("steady state wrote %v and made %d Glue calls", w, r.glue.calls-calls)
	}
}

func TestGlueSyncLeavesTablesItDidNotSet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(p map[string]string)
	}{
		{"not Arc's", func(p map[string]string) { delete(p, "arc.managed") }},
		{"another warehouse", func(p map[string]string) { p["arc.warehouse"] = "s3://other-bucket/wh" }},
		{"advanced by another writer", func(p map[string]string) {
			p["metadata_location"] = strings.Replace(p["metadata_location"], path.Base(p["metadata_location"]), "00009-abc.metadata.json", 1)
		}},
		{"pointer moved but synced_location kept", func(p map[string]string) {
			p["metadata_location"] = strings.Replace(p["metadata_location"], path.Base(p["metadata_location"]), "v99.metadata.json", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newGlueRig(t)
			r.pass(t)
			r.sync.dirtyPass(context.Background())
			r.glue.mu.Lock()
			tc.mutate(r.glue.tables["arc_mydb\x00cpu"].Parameters)
			r.glue.mu.Unlock()
			r.glue.takeWrites()
			r.publish(t, "cpu_b.parquet", 1_700_000_100_000_000)
			r.sync.dirtyPass(context.Background())
			if w := r.glue.takeWrites(); len(w) != 0 {
				t.Fatalf("Arc wrote %v to a Glue table it must leave alone", w)
			}
		})
	}
}

// A stale exporter (a lease handover) never moves Glue back to an older
// version of the same table while the newer one exists.
func TestGlueSyncNeverMovesBackwards(t *testing.T) {
	r := newGlueRig(t)
	r.pass(t)
	r.sync.dirtyPass(context.Background())
	pub := r.published(t)
	n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path.Base(pub), "v"), ".metadata.json"))
	newer := strings.Replace(pub, path.Base(pub), "v"+strconv.Itoa(n+5)+".metadata.json", 1)
	key, ok := r.exp.warehouseRelKey(newer)
	if !ok {
		t.Fatal("unaddressable")
	}
	body, _ := os.ReadFile(filepath.Join(r.root, filepath.FromSlash(strings.TrimSuffix(key, path.Base(key))), path.Base(pub)))
	if err := r.backend.Write(context.Background(), key, body); err != nil {
		t.Fatal(err)
	}
	r.glue.mu.Lock()
	p := r.glue.tables["arc_mydb\x00cpu"].Parameters
	p["metadata_location"], p["arc.synced_location"] = newer, newer
	r.glue.mu.Unlock()
	r.sync.dropCache()
	r.glue.takeWrites()
	r.sync.Notify("mydb", "cpu")
	r.sync.dirtyPass(context.Background())
	if w := r.glue.takeWrites(); len(w) != 0 {
		t.Fatalf("moved Glue back from v%d: %v", n+5, w)
	}
	// Once the newer version is gone (pruned), the published one is right.
	if err := r.backend.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	r.sync.Notify("mydb", "cpu")
	r.sync.dirtyPass(context.Background())
	if got := r.glue.table("arc_mydb", "cpu").Parameters["metadata_location"]; got != pub {
		t.Fatalf("Glue names %s, want %s", got, pub)
	}
}

// Two Arc names that map to one Glue name: the incumbent keeps syncing, the
// newcomer is refused.
func TestGlueSyncCollisionKeepsTheIncumbent(t *testing.T) {
	r := newGlueRig(t)
	writeArcStyleParquet(t, filepath.Join(r.root, "mydb/a-b/2023/11/14/22/x.parquet"), 1_700_000_000_000_000, 5)
	r.pass(t)
	r.sync.dirtyPass(context.Background()) // registers cpu and a-b
	if got := r.glue.table("arc_mydb", "a_b"); got == nil || got.Parameters["arc.measurement"] != "a-b" {
		t.Fatalf("a-b not registered as a_b: %v", got)
	}
	writeArcStyleParquet(t, filepath.Join(r.root, "mydb/a_b/2023/11/14/22/y.parquet"), 1_700_000_000_000_000, 5)
	r.pass(t)
	r.glue.takeWrites()
	r.sync.dirtyPass(context.Background())
	if got := r.glue.table("arc_mydb", "a_b").Parameters["arc.measurement"]; got != "a-b" {
		t.Fatalf("the newcomer a_b took the Glue table: %s", got)
	}
	writeArcStyleParquet(t, filepath.Join(r.root, "mydb/a-b/2023/11/14/23/z.parquet"), 1_700_000_100_000_000, 5)
	r.pass(t)
	r.sync.dirtyPass(context.Background())
	pub := mustLoadPublished(t, r.exp, "mydb", "a-b").MetadataLocation
	if got := r.glue.table("arc_mydb", "a_b").Parameters["metadata_location"]; got != pub {
		t.Fatalf("the incumbent stopped syncing: Glue %s, published %s", got, pub)
	}
}

func TestGlueSyncDeletesOnlyAfterTwoConfirmedAbsences(t *testing.T) {
	r := newGlueRig(t)
	now := time.Now()
	r.sync.now = func() time.Time { return now }
	r.pass(t)
	r.sync.fullPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("not registered")
	}
	// A table recorded under another namespace prefix is never this
	// listing's to judge.
	r.glue.mu.Lock()
	other := *r.glue.tables["arc_mydb\x00cpu"]
	other.Name = aws.String("old")
	other.Parameters = map[string]string{}
	for k, v := range r.glue.tables["arc_mydb\x00cpu"].Parameters {
		other.Parameters[k] = v
	}
	other.Parameters["arc.measurement"], other.Parameters["arc.namespace_prefix"] = "old", "legacy"
	r.glue.tables["arc_mydb\x00old"] = &other
	r.glue.mu.Unlock()

	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		t.Fatal(err)
	}
	r.sync.fullPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("deleted on the first absence")
	}
	now = now.Add(glueDeleteGap - time.Minute)
	r.sync.fullPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("deleted before the gap")
	}
	now = now.Add(2 * time.Minute)
	r.sync.fullPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") != nil {
		t.Fatal("not deleted after two absences glueDeleteGap apart")
	}
	if r.glue.table("arc_mydb", "old") == nil {
		t.Fatal("deleted a table recorded under another namespace prefix")
	}
	if _, ok := r.glue.dbs["arc_mydb"]; !ok {
		t.Fatal("the Glue database was deleted")
	}
}

func TestGlueSyncNoDeletionWhenGlueOrTheListingFails(t *testing.T) {
	r := newGlueRig(t)
	now := time.Now()
	r.sync.now = func() time.Time { return now }
	r.pass(t)
	r.sync.fullPass(context.Background())
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		t.Fatal(err)
	}
	r.sync.fullPass(context.Background())
	now = now.Add(glueDeleteGap + time.Minute)
	r.glue.failOn["GetTables"] = errors.New("glue down")
	r.sync.fullPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("deleted while Glue's listing failed")
	}
}

func TestGlueSyncRetriesAfterAnOutageWithoutANewPublish(t *testing.T) {
	r := newGlueRig(t)
	r.pass(t)
	r.glue.failOn["CreateTable"] = errors.New("glue down")
	r.sync.dirtyPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") != nil {
		t.Fatal("registered through a failure")
	}
	r.sync.dirtyPass(context.Background()) // the timer's pass, no publish in between
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("not retried")
	}
}

func TestGlueSyncThrottleEndsThePass(t *testing.T) {
	r := newGlueRig(t)
	r.pass(t)
	r.glue.failOn["GetTable"] = &smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down"}
	r.sync.dirtyPass(context.Background())
	r.sync.mu.Lock()
	_, failed := r.sync.failed[glueKey{"mydb", "cpu"}]
	r.sync.mu.Unlock()
	if !failed {
		t.Fatal("a throttled table was not kept for retry")
	}
	r.sync.dirtyPass(context.Background())
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("not registered after the throttle")
	}
}

func TestGlueSyncGateClosedWritesNothing(t *testing.T) {
	r := newGlueRig(t)
	r.gate.open = false
	r.pass(t)
	r.sync.dirtyPass(context.Background())
	r.sync.fullPass(context.Background())
	if w := r.glue.takeWrites(); len(w) != 0 {
		t.Fatalf("a closed gate wrote %v", w)
	}
}

// Start/Stop: a publish reaches Glue through the loop, with no timer tick.
func TestGlueSyncLoopFollowsPublishes(t *testing.T) {
	r := newGlueRig(t)
	r.sync.Start()
	defer r.sync.Stop()
	r.pass(t)
	deadline := time.Now().Add(10 * time.Second)
	for r.glue.table("arc_mydb", "cpu") == nil || r.glue.table("arc_mydb", "cpu").Parameters["metadata_location"] != r.published(t) {
		if time.Now().After(deadline) {
			t.Fatal("the loop did not sync the publish")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGlueName(t *testing.T) {
	for in, want := range map[string]string{"arc_mydb": "arc_mydb", "CPU": "cpu", "a-b.c": "a_b_c", "Ü": "_"} {
		if got, ok := GlueName(in); !ok || got != want {
			t.Fatalf("GlueName(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	if _, ok := GlueName(strings.Repeat("x", 256)); ok {
		t.Fatal("a 256-character name is valid")
	}
	if _, ok := GlueName(""); ok {
		t.Fatal("an empty name is valid")
	}
}
