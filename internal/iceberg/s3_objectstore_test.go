//go:build objectstore

package iceberg

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #939 A1 against a real S3 API (SeaweedFS in CI): the exporter publishes an
// S3-primary measurement through iceberg-go's S3 FileIO with Arc's own
// credentials. The config gate still refuses this combination; these tests
// build the exporter directly.

const objTestSecret = "minioadmin"

func s3Rig(t *testing.T) (*storage.S3Backend, string) {
	t.Helper()
	bucket := os.Getenv("ARC_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("ARC_TEST_S3_BUCKET not set")
	}
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	prefix := fmt.Sprintf("iceberg-a1/%s-%d/", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
	b, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: endpoint, Prefix: prefix,
		AccessKey: objTestSecret, SecretKey: objTestSecret, PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("S3 backend: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		keys, _ := b.List(ctx, "")
		for _, k := range keys {
			_ = b.Delete(ctx, k)
		}
		b.Close()
	})
	return b, endpoint
}

// putParquet writes an Arc-style Parquet file and uploads it under key.
func putParquet(t *testing.T, b storage.Backend, key string, baseTS int64, rows int) {
	t.Helper()
	local := filepath.Join(t.TempDir(), "f.parquet")
	writeArcStyleParquet(t, local, baseTS, rows)
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Write(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
}

type countingSource struct {
	*StorageWalkSource
	footers atomic.Int32
}

func (c *countingSource) FileSchema(ctx context.Context, f FileRef) (ArcSchema, error) {
	c.footers.Add(1)
	return c.StorageWalkSource.FileSchema(ctx, f)
}

func s3Exporter(t *testing.T, b *storage.S3Backend) (*Exporter, *sql.DB, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, b, DefaultWarehouse(b), "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return exp, db, dbPath
}

func liveFiles(t *testing.T, exp *Exporter) map[string]int64 {
	t.Helper()
	ctx := exp.withAWS(context.Background())
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	have, err := exp.tableDataFiles(ctx, lt)
	if err != nil {
		t.Fatal(err)
	}
	return have
}

func tableRows(t *testing.T, exp *Exporter) int64 {
	t.Helper()
	ctx := exp.withAWS(context.Background())
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := lt.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("scan through the S3 FileIO: %v", err)
	}
	defer tbl.Release()
	return tbl.NumRows()
}

// Export, read back through iceberg-go's S3 FileIO, remove files, empty out;
// a decoy AWS_S3_ENDPOINT is ignored; no credential reaches the table
// metadata or the catalog.
func TestS3Export_EndToEnd(t *testing.T) {
	t.Setenv("AWS_S3_ENDPOINT", "http://127.0.0.1:1") // nothing listens: used, it fails the test
	b, _ := s3Rig(t)
	ctx := context.Background()
	base := int64(1_700_000_000_000_000)
	a := "mydb/cpu/2023/11/14/22/cpu_a.parquet"
	bk := "mydb/cpu/2023/11/14/23/cpu_b.parquet"
	putParquet(t, b, a, base, 50)
	putParquet(t, b, bk, base+3_600_000_000, 70)

	exp, _, dbPath := s3Exporter(t, b)
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	sched.runPass(ctx)
	have := liveFiles(t, exp)
	if len(have) != 2 {
		t.Fatalf("table files = %v, want 2", have)
	}
	for uri := range have {
		if !strings.HasPrefix(uri, "s3://") {
			t.Fatalf("table file %q is not an s3:// URI", uri)
		}
	}
	if n := tableRows(t, exp); n != 120 {
		t.Fatalf("rows read through the S3 FileIO = %d, want 120", n)
	}

	// No credential in the warehouse or the catalog.
	keys, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	meta := 0
	for _, k := range keys {
		if !strings.Contains(k, "/metadata/") {
			continue
		}
		meta++
		data, err := b.Read(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(objTestSecret)) {
			t.Fatalf("table metadata %s holds the credential", k)
		}
	}
	if meta == 0 {
		t.Fatal("no table metadata found under the warehouse")
	}
	catalog, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(catalog, []byte(objTestSecret)) {
		t.Fatal("the SQLite catalog holds the credential")
	}

	// A removal, then every file gone: the table follows.
	if err := b.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	sched.runPass(ctx)
	if have := liveFiles(t, exp); len(have) != 1 {
		t.Fatalf("after a removal: table files = %v, want 1", have)
	}
	if n := tableRows(t, exp); n != 70 {
		t.Fatalf("after a removal: rows = %d, want 70", n)
	}
	if err := b.Delete(ctx, bk); err != nil {
		t.Fatal(err)
	}
	sched.runPass(ctx)
	if have := liveFiles(t, exp); len(have) != 0 {
		t.Fatalf("all files gone: table files = %v, want none", have)
	}
}

// A restart reads no footer for an unchanged S3 table, and one per new file.
func TestS3Export_RestartReadsNoFooter(t *testing.T) {
	b, _ := s3Rig(t)
	ctx := context.Background()
	base := int64(1_700_000_000_000_000)
	for i := 0; i < 3; i++ {
		putParquet(t, b, fmt.Sprintf("mydb/cpu/2023/11/14/2%d/cpu_%d.parquet", i, i), base+int64(i)*3_600_000_000, 10)
	}
	exp, _, _ := s3Exporter(t, b)
	first := &countingSource{StorageWalkSource: NewStorageWalkSource(b, "arc", zerolog.Nop())}
	NewScheduler(SchedulerConfig{Exporter: exp, Source: first, Logger: zerolog.Nop()}).runPass(ctx)
	if got := first.footers.Load(); got != 3 {
		t.Fatalf("first export read %d footers, want 3", got)
	}

	restarted := &countingSource{StorageWalkSource: NewStorageWalkSource(b, "arc", zerolog.Nop())}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: restarted, Logger: zerolog.Nop()})
	sched.runPass(ctx)
	if got := restarted.footers.Load(); got != 0 {
		t.Fatalf("restart over an unchanged table read %d footers, want 0", got)
	}
	putParquet(t, b, "mydb/cpu/2023/11/15/00/cpu_new.parquet", base+4*3_600_000_000, 10)
	sched.runPass(ctx)
	if got := restarted.footers.Load(); got != 1 {
		t.Fatalf("one new file read %d footers, want 1", got)
	}
	if have := liveFiles(t, exp); len(have) != 4 {
		t.Fatalf("table files = %d, want 4", len(have))
	}
}

// On an object store a file removed between the two listings sends the
// measurement back to list again; the set it exports is the settled one.
func TestS3Source_RemovalBetweenListings(t *testing.T) {
	b, _ := s3Rig(t)
	ctx := context.Background()
	out := "mydb/cpu/2023/11/14/22/cpu_x_b1_compacted.parquet"
	ins := []string{"mydb/cpu/2023/11/14/22/a.parquet", "mydb/cpu/2023/11/14/22/b.parquet"}
	for _, k := range ins {
		if err := b.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	n := 0
	src.SetPendingOutputs(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		n++
		if n == 1 {
			if err := b.Write(ctx, out, []byte("PAR1")); err != nil {
				t.Fatal(err)
			}
			for _, k := range ins {
				if err := b.Delete(ctx, k); err != nil {
					t.Fatal(err)
				}
			}
		}
		return nil, nil
	})
	files, _, err := src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		got = append(got, filepath.Base(f.PhysicalPath))
		if f.SizeBytes != 4 {
			t.Fatalf("%s: size %d from the listing, want 4", f.PhysicalPath, f.SizeBytes)
		}
	}
	sort.Strings(got)
	if len(got) != 1 || got[0] != filepath.Base(out) {
		t.Fatalf("files = %v, want the compacted output only", got)
	}
	if n != 2 {
		t.Fatalf("listed %d times, want 2", n)
	}
}
