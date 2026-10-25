//go:build objectstore

// The GCS half of the key contract, verified against a real bucket (#1105).
//
// Tagged `objectstore` alongside the S3 and Azure contract tests. Run with
// Application Default Credentials (gcloud auth application-default login, or
// GOOGLE_APPLICATION_CREDENTIALS):
//
//	ARC_TEST_GCS_BUCKET=<bucket> \
//	go test -tags='duckdb_arrow objectstore' -run GCS ./internal/storage/
//
// Every test works under its own run prefix and removes what it wrote, so a
// shared bucket is fine. The bucket must already exist.
package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// gcsBackend builds a backend against ARC_TEST_GCS_BUCKET rooted at prefix
// ("" for the bucket root), and skips when the variable is absent: a GCS
// bucket is paid infrastructure most machines, CI included, do not have (see
// azureBackend for why that is a skip and not a failure).
func gcsBackend(t *testing.T, prefix string) *GCSBackend {
	t.Helper()
	return gcsBackendOn(t, os.Getenv("ARC_TEST_GCS_BUCKET"), prefix)
}

func gcsBackendOn(t *testing.T, bucket, prefix string) *GCSBackend {
	t.Helper()
	if os.Getenv("ARC_TEST_GCS_BUCKET") == "" {
		t.Skip("ARC_TEST_GCS_BUCKET not set")
	}
	b, err := NewGCSBackend(&GCSConfig{Bucket: bucket, Prefix: prefix}, zerolog.Nop())
	if err != nil {
		t.Fatalf("could not build a GCS backend for bucket %q: %v", bucket, err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// gcsRunPrefix is a prefix no other run uses.
func gcsRunPrefix(name string) string {
	return fmt.Sprintf("arc-contract/%s-%d", name, time.Now().UnixNano())
}

// A full round trip through a prefixed backend, and the proof that nothing
// lands outside the prefix: the bucket ROOT is listed through a second,
// unprefixed backend. A backend that prefixed its writes but not its
// listings, or the reverse, passes every same-backend round trip and fails
// there.
func TestGCSPrefixRoundTrip(t *testing.T) {
	prefix := gcsRunPrefix("prefix")
	b := gcsBackend(t, prefix)
	root := gcsBackend(t, "")
	ctx := context.Background()

	keys := []string{
		"contractdb/cpu/2026/10/06/14/a.parquet",
		"contractdb/cpu/2026/10/06/14/b.parquet",
		"contractdb/mem/2026/10/06/15/c.parquet",
	}
	payload := func(k string) []byte { return []byte("payload-" + k) }
	t.Cleanup(func() { _ = b.DeleteBatch(context.Background(), keys) })

	for _, k := range keys {
		if err := b.WriteReader(ctx, k, bytes.NewReader(payload(k)), int64(len(payload(k)))); err != nil {
			t.Fatalf("WriteReader(%s): %v", k, err)
		}
	}
	for _, k := range keys {
		got, err := b.Read(ctx, k)
		if err != nil || !bytes.Equal(got, payload(k)) {
			t.Fatalf("Read(%s) = %q, %v", k, got, err)
		}
		if size, err := b.StatFile(ctx, k); err != nil || size != int64(len(payload(k))) {
			t.Fatalf("StatFile(%s) = %d, %v", k, size, err)
		}
		if ok, err := b.Exists(ctx, k); err != nil || !ok {
			t.Fatalf("Exists(%s) = %v, %v", k, ok, err)
		}
		var tail bytes.Buffer
		if err := b.ReadToAt(ctx, k, &tail, 8); err != nil || tail.String() != string(payload(k)[8:]) {
			t.Fatalf("ReadToAt(%s, 8) = %q, %v", k, tail.String(), err)
		}
	}

	listed, err := b.List(ctx, "contractdb/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(listed)
	if strings.Join(listed, ",") != strings.Join(keys, ",") {
		t.Fatalf("List = %v, want %v", listed, keys)
	}
	objs, err := b.ListObjects(ctx, "contractdb/cpu/")
	if err != nil || len(objs) != 2 {
		t.Fatalf("ListObjects = %v, %v", objs, err)
	}
	for _, o := range objs {
		if o.Size != int64(len(payload(o.Path))) || o.LastModified.IsZero() {
			t.Fatalf("ListObjects entry %+v", o)
		}
	}
	dirs, err := b.ListDirectories(ctx, "contractdb")
	sort.Strings(dirs)
	if err != nil || strings.Join(dirs, ",") != "cpu,mem" {
		t.Fatalf("ListDirectories = %v, %v", dirs, err)
	}
	if ok, err := b.HasObjectsUnderPrefix(ctx, "contractdb/mem/"); err != nil || !ok {
		t.Fatalf("HasObjectsUnderPrefix(mem) = %v, %v", ok, err)
	}
	if ok, err := b.HasObjectsUnderPrefix(ctx, "contractdb/disk/"); err != nil || ok {
		t.Fatalf("HasObjectsUnderPrefix(disk) = %v, %v", ok, err)
	}

	// Through the root: every object sits under the prefix, by its full name.
	all, err := root.List(ctx, prefix+"/")
	if err != nil {
		t.Fatalf("root List: %v", err)
	}
	sort.Strings(all)
	var want []string
	for _, k := range keys {
		want = append(want, prefix+"/"+k)
	}
	if strings.Join(all, ",") != strings.Join(want, ",") {
		t.Fatalf("root sees %v, want %v", all, want)
	}

	if err := b.DeleteBatch(ctx, append([]string{"contractdb/never-written.parquet"}, keys...)); err != nil {
		t.Fatalf("DeleteBatch (with an absent key): %v", err)
	}
	if left, err := root.List(ctx, prefix+"/"); err != nil || len(left) != 0 {
		t.Fatalf("after DeleteBatch root sees %v, %v", left, err)
	}
}

// A missing object and a missing bucket are different answers, although GCS
// gives every object call the same ErrObjectNotExist for both (#1105 spike).
func TestGCSMissingObjectVersusMissingBucket(t *testing.T) {
	ctx := context.Background()
	b := gcsBackend(t, gcsRunPrefix("notfound"))
	if _, err := b.Read(ctx, "db/m/absent.parquet"); !IsNotFound(err) || IsStoreNotFound(err) {
		t.Fatalf("Read of a missing object: %v (IsNotFound %v, IsStoreNotFound %v)", err, IsNotFound(err), IsStoreNotFound(err))
	}
	if size, err := b.StatFile(ctx, "db/m/absent.parquet"); err != nil || size != -1 {
		t.Fatalf("StatFile of a missing object = %d, %v", size, err)
	}
	if ok, err := b.Exists(ctx, "db/m/absent.parquet"); err != nil || ok {
		t.Fatalf("Exists of a missing object = %v, %v", ok, err)
	}
	if err := b.Delete(ctx, "db/m/absent.parquet"); err != nil {
		t.Fatalf("Delete of a missing object: %v", err)
	}

	gone := gcsBackendOn(t, fmt.Sprintf("arc-1105-absent-%d", time.Now().UnixNano()), "")
	if _, err := gone.Read(ctx, "db/m/x.parquet"); !IsStoreNotFound(err) || IsNotFound(err) {
		t.Fatalf("Read in a missing bucket: %v (IsNotFound %v, IsStoreNotFound %v)", err, IsNotFound(err), IsStoreNotFound(err))
	}
	if _, err := gone.StatFile(ctx, "db/m/x.parquet"); !IsStoreNotFound(err) {
		t.Fatalf("StatFile in a missing bucket: %v", err)
	}
	if _, err := gone.Exists(ctx, "db/m/x.parquet"); !IsStoreNotFound(err) {
		t.Fatalf("Exists in a missing bucket: %v", err)
	}
	if _, err := gone.List(ctx, "db/"); !IsStoreNotFound(err) {
		t.Fatalf("List in a missing bucket: %v", err)
	}
	if _, _, err := gone.ObjectSHA(ctx, "db/m/x.parquet"); !IsStoreNotFound(err) {
		t.Fatalf("ObjectSHA in a missing bucket: %v", err)
	}
}

type gcsDyingReader struct {
	r   io.Reader
	err error
}

func (f *gcsDyingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		return n, f.err
	}
	return n, err
}

// A write that goes wrong commits nothing: not a truncated object under a
// valid name. Both the reader failing and the reader supplying fewer bytes
// than declared, below and above the client's 16 MiB chunk.
func TestGCSFailedWriteCommitsNothing(t *testing.T) {
	ctx := context.Background()
	b := gcsBackend(t, gcsRunPrefix("abort"))
	for _, n := range []int{1000, 20 << 20} {
		body := make([]byte, n)
		_, _ = rand.Read(body)
		short := fmt.Sprintf("db/m/short-%d.parquet", n)
		if err := b.WriteReader(ctx, short, bytes.NewReader(body), int64(n)+1); err == nil {
			t.Fatalf("%d bytes declared %d: write succeeded", n, n+1)
		}
		broken := fmt.Sprintf("db/m/broken-%d.parquet", n)
		if err := b.WriteReader(ctx, broken, &gcsDyingReader{bytes.NewReader(body), errors.New("source died")}, int64(n)); err == nil {
			t.Fatalf("%d bytes from a failing reader: write succeeded", n)
		}
		for _, k := range []string{short, broken} {
			if ok, err := b.Exists(ctx, k); err != nil || ok {
				_ = b.Delete(ctx, k)
				t.Fatalf("%s exists after a failed write (%v)", k, err)
			}
		}
	}
}

// What ListObjects hides, ListUnusable reports, and the two partition the
// prefix. A console folder placeholder is neither.
func TestGCSListUnusablePartitionsTheBucket(t *testing.T) {
	ctx := context.Background()
	prefix := gcsRunPrefix("unusable")
	b := gcsBackend(t, prefix)
	put := func(name string, body []byte) {
		t.Helper()
		w := b.client.Bucket(b.bucket).Object(b.prefix + name).NewWriter(ctx)
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("raw put %s: %v", name, err)
		}
		t.Cleanup(func() { _ = b.client.Bucket(b.bucket).Object(b.prefix + name).Delete(context.Background()) })
	}
	put("db/m/good.parquet", []byte("x"))
	put("db/m/bad.parquet.part", []byte("committed bytes"))
	put("db/m//double.parquet", []byte("y"))
	put("db/folder/", nil)

	objs, err := b.ListObjects(ctx, "db/")
	if err != nil || len(objs) != 1 || objs[0].Path != "db/m/good.parquet" {
		t.Fatalf("ListObjects = %v, %v", objs, err)
	}
	un, err := b.ListUnusable(ctx, "db/")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	var got []string
	for _, u := range un {
		if !errors.Is(u.Err, ErrInvalidPath) {
			t.Fatalf("%s: reason %v does not wrap ErrInvalidPath", u.Path, u.Err)
		}
		got = append(got, u.Path)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "db/m//double.parquet,db/m/bad.parquet.part" {
		t.Fatalf("ListUnusable = %v", got)
	}
	if ok, err := b.HasObjectsUnderPrefix(ctx, "db/folder/"); err != nil || ok {
		t.Fatalf("a folder placeholder counts as an object: %v, %v", ok, err)
	}
}

// GCS rewrites inside one location and storage class in a single call, and
// the client exposes no way to force several, so there is no "in parts" case
// to force: forceParts is a no-op.
func TestGCSCopyObject(t *testing.T) {
	copierRoundTrip(t, gcsBackend(t, gcsRunPrefix("copy")), func() {})
}

// ObjectURI names what the backend wrote, and DuckDB reads it with a GCS
// bearer-token secret minted from the backend's own credentials: the read
// path PR 2 wires, proven here against the root this backend reports.
func TestGCSObjectURIReadsThroughDuckDB(t *testing.T) {
	ctx := context.Background()
	b := gcsBackend(t, gcsRunPrefix("duckdb"))
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	local := t.TempDir() + "/x.parquet"
	if _, err := db.Exec("COPY (SELECT range AS v FROM range(1000)) TO '" + local + "' (FORMAT parquet)"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	key := "duckdb/cpu/2026/10/25/15/x.parquet"
	if err := b.Write(ctx, key, data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { _ = b.Delete(context.Background(), key) })

	uri, err := ObjectURI(b, key)
	if err != nil || !strings.HasPrefix(uri, "gs://"+b.bucket+"/"+b.prefix) {
		t.Fatalf("ObjectURI = %q, %v", uri, err)
	}
	tok, err := b.creds.Token(ctx)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	for _, q := range []string{"INSTALL httpfs", "LOAD httpfs",
		"CREATE SECRET g (TYPE GCS, BEARER_TOKEN '" + tok.Value + "')"} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", strings.SplitN(q, " (", 2)[0], err)
		}
	}
	var n int64
	if err := db.QueryRow("SELECT count(*) FROM read_parquet('" + uri + "')").Scan(&n); err != nil || n != 1000 {
		t.Fatalf("read_parquet(%s) = %d, %v", uri, n, err)
	}
}
