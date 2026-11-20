package iceberg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/rs/zerolog"
)

// #1778: what one export keeps of its reads.

func TestExportReads_ATailIsHandedOverOnce(t *testing.T) {
	ctx := withExportReads(context.Background())
	if withExportReads(ctx) != ctx {
		t.Fatal("a context that carries the export's reads got a second set")
	}
	r := exportReadsFrom(ctx)
	r.putTail("s3://b/f.parquet", fileTail{base: 10, size: 14, data: []byte("abcd")})
	got, ok := r.takeTail("s3://b/f.parquet")
	if !ok || got.base != 10 || got.size != 14 || string(got.data) != "abcd" {
		t.Fatalf("took %+v, %v", got, ok)
	}
	if _, again := r.takeTail("s3://b/f.parquet"); again {
		t.Error("the tail was handed over twice")
	}
	if r.tailBytes != 0 {
		t.Errorf("%d bytes still counted after the tail was taken", r.tailBytes)
	}
	// Outside an export nothing is kept and nothing panics.
	var none *exportReads
	none.putTail("x", fileTail{data: []byte("a")})
	none.putHint("d", 3)
	none.dropHints()
	if _, ok := none.takeTail("x"); ok {
		t.Error("a tail came back with no export on the context")
	}
	if _, ok := none.takeHint("d"); ok {
		t.Error("a hint came back with no export on the context")
	}
	if exportReadsFrom(context.Background()) != nil {
		t.Error("a bare context carries export reads")
	}
}

func TestExportReads_TailsAreBounded(t *testing.T) {
	r := exportReadsFrom(withExportReads(context.Background()))
	big := make([]byte, exportReadsMaxTailBytes-1)
	r.putTail("a", fileTail{data: big})
	r.putTail("b", fileTail{data: []byte("xy")}) // would pass the bound
	if _, ok := r.takeTail("b"); ok {
		t.Error("a tail past the bound was kept")
	}
	r.putTail("c", fileTail{data: []byte("x")}) // fits exactly
	if _, ok := r.takeTail("c"); !ok {
		t.Error("a tail that fits the bound was not kept")
	}
	// Replacing a path's tail does not count it twice.
	r.putTail("a", fileTail{data: []byte("z")})
	if r.tailBytes != 1 {
		t.Errorf("%d bytes counted after replacing a tail with one byte, want 1", r.tailBytes)
	}
}

func TestExportReads_AHintIsUsedOnceAndDropped(t *testing.T) {
	r := exportReadsFrom(withExportReads(context.Background()))
	r.putHint("db/t/metadata", 7)
	if n, ok := r.takeHint("db/t/metadata"); !ok || n != 7 {
		t.Fatalf("took %d, %v", n, ok)
	}
	if _, ok := r.takeHint("db/t/metadata"); ok {
		t.Error("the hint was handed over twice")
	}
	r.putHint("db/t/metadata", 7)
	r.putHint("db/u/metadata", -1)
	r.dropHints()
	for _, d := range []string{"db/t/metadata", "db/u/metadata"} {
		if _, ok := r.takeHint(d); ok {
			t.Errorf("the hint of %s survived dropHints", d)
		}
	}
}

// countingIO counts the opens that reach the store.
type countingIO struct {
	iceio.IO
	opens int
}

func (c *countingIO) Open(name string) (iceio.File, error) {
	c.opens++
	return c.IO.Open(name)
}

func TestTailIO_ServesTheTailAndGoesToTheStoreOnlyBeforeIt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	content := []byte("0123456789ABCDEFGHIJ")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	under := &countingIO{IO: iceio.LocalFS{}}
	tio := &tailIO{path: path, tail: fileTail{base: 12, size: 20, data: content[12:]}, under: under}

	f, err := tio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() != 20 {
		t.Fatalf("Stat: size %v, err %v; want 20", st, err)
	}
	if end, err := f.Seek(0, io.SeekEnd); err != nil || end != 20 {
		t.Fatalf("Seek to the end: %d, %v; want 20", end, err)
	}
	buf := make([]byte, 4)
	if n, err := f.ReadAt(buf, 16); n != 4 || (err != nil && !errors.Is(err, io.EOF)) || string(buf) != "GHIJ" {
		t.Fatalf("ReadAt(16): %q, %d, %v", buf, n, err)
	}
	if _, err := f.Seek(12, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(f)
	if err != nil || string(all) != "CDEFGHIJ" {
		t.Fatalf("Read from the start of the tail: %q, %v", all, err)
	}
	if under.opens != 0 {
		t.Fatalf("%d opens reached the store for reads inside the tail", under.opens)
	}
	// Before the tail: the store answers, and the bytes are the file's.
	if n, err := f.ReadAt(buf, 2); n != 4 || err != nil || string(buf) != "2345" {
		t.Fatalf("ReadAt(2): %q, %d, %v", buf, n, err)
	}
	if under.opens != 1 {
		t.Errorf("%d opens for a read before the tail, want 1", under.opens)
	}
	// Another file through the same IO is the store's.
	other := filepath.Join(dir, "g.bin")
	if err := os.WriteFile(other, []byte("zz"), 0o600); err != nil {
		t.Fatal(err)
	}
	g, err := tio.Open(other)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
	if under.opens != 2 {
		t.Errorf("another file's open did not reach the store")
	}
}

// The count of small manifests is kept from one append to the next, and
// forgotten by anything else that can commit to the table.
func TestAppend_TheSmallManifestCountFollowsTheTable(t *testing.T) {
	r := newAppendRig(t)
	const key = "mydb\x00cpu"
	names := []string{r.settled()}
	if _, known := r.exp.smallManifests(key); known {
		t.Fatal("a count is held before any append")
	}
	smallNow := func() int {
		n := 0
		for _, m := range r.manifests() {
			if m.Length() < appendMergeTargetBytes {
				n++
			}
		}
		return n
	}
	for i := 0; i < 9; i++ {
		f := r.newFile()
		names = append(names, f)
		r.report(f)
		r.serve()
		held, known := r.exp.smallManifests(key)
		if !known {
			t.Fatalf("append %d left no count", i+1)
		}
		if got := smallNow(); held != got {
			t.Fatalf("after append %d the count held is %d, the table has %d small manifests", i+1, held, got)
		}
		if got := len(r.manifests()); got > appendMergeMinCount {
			t.Fatalf("after append %d the table has %d manifests, want at most %d", i+1, got, appendMergeMinCount)
		}
	}
	r.wantOnce(names...)
	if r.listings() != 1 {
		t.Fatalf("%d listings, want the first pass only: the appends fell back", r.listings())
	}
	// A pass forgets it: it can rewrite the manifests.
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if _, known := r.exp.smallManifests(key); known {
		t.Error("the count survived a reconcile pass")
	}
	// An append that does not finish forgets it too.
	f := r.newFile()
	names = append(names, f)
	r.report(f)
	r.serve()
	if _, known := r.exp.smallManifests(key); !known {
		t.Fatal("no count after an append that followed the pass")
	}
	refs, _, err := r.src.AppendRefs(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"}, []string{appendDir + r.newFile()}, nil)
	if err != nil || len(refs) != 1 {
		t.Fatalf("refs %v, err %v", refs, err)
	}
	if _, err := r.exp.appendFiles(context.Background(), "mydb", "cpu", refs, r.sched.unionFileSchemas, func() bool { return false }); !errors.Is(err, errWriterLost) {
		t.Fatalf("append with the gate closed: %v, want errWriterLost", err)
	}
	if _, known := r.exp.smallManifests(key); known {
		t.Error("the count survived an append that did not commit")
	}
}

// A reported file that has left storage is left out; the others are added in
// the same append.
func TestAppend_PresentFileSchemasLeavesOutAFileThatIsGone(t *testing.T) {
	r := newAppendRig(t)
	r.settled()
	kept, gone := r.newFile(), r.newFile()
	m := Measurement{Database: "mydb", Measurement: "cpu"}
	refs, _, err := r.src.AppendRefs(context.Background(), m, []string{appendDir + kept, appendDir + gone}, nil)
	if err != nil || len(refs) != 2 {
		t.Fatalf("refs %v, err %v", refs, err)
	}
	for _, f := range refs {
		if filepath.Base(f.Key) == gone {
			if err := os.Remove(f.LocalPath); err != nil {
				t.Fatal(err)
			}
		}
	}
	present, sc, err := r.sched.presentFileSchemas(context.Background(), refs, unionSchemaConcurrency)
	if err != nil {
		t.Fatalf("a file that is gone failed the read: %v", err)
	}
	if len(present) != 1 || filepath.Base(present[0].Key) != kept {
		t.Fatalf("present = %v, want only %s", present, kept)
	}
	if len(sc.Fields) == 0 {
		t.Error("no schema for the file that is there")
	}
}

// A local file is stat'ed whatever size the flush reported: the report is
// only trusted where asking costs a request.
func TestAppend_ALocalFileIsStatedWhateverSizeWasReported(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	f := r.newFile()
	before := r.listings()
	r.sched.RequestIngestAppend("mydb", "cpu", appendDir+f, 999) // not its size
	r.serve()
	if r.listings() != before {
		t.Fatal("a wrong reported size made a local append fall back: the reported size was used in place of the stat")
	}
	r.wantOnce(first, f)
	// And one that is gone is left out by that stat.
	gone := r.newFile()
	if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(appendDir), gone)); err != nil {
		t.Fatal(err)
	}
	r.sched.RequestIngestAppend("mydb", "cpu", appendDir+gone, 999)
	r.serve()
	if r.listings() != before {
		t.Fatal("a reported file that is gone made a local append fall back")
	}
	r.wantOnce(first, f)
}

// The hint a pass starts from is the one the file holds, not the newer
// version the adopt check may go on to take: #1097's prune keeps the version
// a reader that followed the hint is opening.
func TestExportReads_ThePassStartsFromTheHintAsWritten(t *testing.T) {
	r := newAppendRig(t)
	r.exp.SetWarehouseAuthoritative(true)
	r.settled()
	for i := 0; i < 2; i++ {
		f := r.newFile()
		r.report(f)
		r.serve()
	}
	hintKey := "arc_mydb.db/cpu/metadata/version-hint.text"
	raw, err := r.be.Read(context.Background(), hintKey)
	if err != nil {
		t.Fatal(err)
	}
	var newest int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%d", &newest); err != nil || newest < 2 {
		t.Fatalf("hint %q, err %v", raw, err)
	}
	// A writer that published v<newest> and stopped before the hint.
	if err := r.be.Write(context.Background(), hintKey, []byte(strconv.Itoa(newest-1))); err != nil {
		t.Fatal(err)
	}
	// Another process: nothing is known to be published here, so the adopt
	// check lists the versions and takes the newest of this lineage.
	exp2, err := NewExporter(r.exp.db, r.be, "file://"+r.root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	exp2.SetWarehouseAuthoritative(true)
	ctx := withExportReads(context.Background())
	exp2.resetHintFailure()
	if _, err := exp2.EnsureTable(ctx, "mydb", "cpu", ArcSchema{}); err != nil {
		t.Fatal(err)
	}
	exp2.hintFailMu.Lock()
	got, unknown := exp2.passHint, exp2.passHintUnknown
	exp2.hintFailMu.Unlock()
	if unknown || got != newest-1 {
		t.Fatalf("the pass started from hint %d (unknown=%v), the file says %d", got, unknown, newest-1)
	}
}
