package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// The GCS backend driven through its REAL methods against a canned JSON API,
// with no credentials and no network (#1105). The contract test against a real
// bucket is build-tagged and skips without one, so this is what CI runs: the
// startup probe's refusals, the prefix stripping in every listing, pagination,
// DeleteBatch's per-key errors, and the missing-bucket promotion.

type gcsStub struct {
	mu sync.Mutex
	// bucket attributes; missing answers 404 to every bucket and list call.
	missing, forbidden bool
	hns                bool
	softDeleteSeconds  int
	// objects are the full (prefixed) object names in the bucket.
	objects []string
	// deleteStatus answers DELETE for a full name; absent means 204.
	deleteStatus map[string]int
	// flaky answers 503 to that many DELETEs of a full name before
	// deleteStatus applies; deletes counts every DELETE per name.
	flaky   map[string]int
	deletes map[string]int
	// seen records the prefix query of every list call and the number of
	// bucket metadata reads.
	seenPrefixes []string
	bucketReads  int
}

func (s *gcsStub) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		path := strings.TrimPrefix(r.URL.EscapedPath(), "/storage/v1/b/")
		bucket, rest, _ := strings.Cut(path, "/")
		_ = bucket
		notFound := func(msg string) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, `{"error":{"code":404,"message":%q}}`, msg)
		}
		switch {
		case rest == "" && r.Method == http.MethodGet: // bucket metadata
			s.bucketReads++
			if s.missing {
				notFound("The specified bucket does not exist.")
				return
			}
			if s.forbidden {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":{"code":403,"message":"no storage.buckets.get"}}`))
				return
			}
			attrs := map[string]any{"name": bucket}
			if s.hns {
				attrs["hierarchicalNamespace"] = map[string]any{"enabled": true}
			}
			if s.softDeleteSeconds > 0 {
				attrs["softDeletePolicy"] = map[string]any{"retentionDurationSeconds": strconv.Itoa(s.softDeleteSeconds)}
			}
			_ = json.NewEncoder(w).Encode(attrs)
		case rest == "o" && r.Method == http.MethodGet: // list
			if s.missing {
				notFound("The specified bucket does not exist.")
				return
			}
			q := r.URL.Query()
			s.seenPrefixes = append(s.seenPrefixes, q.Get("prefix"))
			s.list(w, q.Get("prefix"), q.Get("delimiter"), q.Get("pageToken"))
		case strings.HasPrefix(rest, "o/") && r.Method == http.MethodGet: // object metadata
			name, _ := url.PathUnescape(strings.TrimPrefix(rest, "o/"))
			for _, o := range s.objects {
				if o == name {
					_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "size": "3", "metadata": map[string]string{"arcsha256": "abc"}})
					return
				}
			}
			if s.missing {
				notFound("The specified bucket does not exist.")
				return
			}
			notFound("No such object")
		case strings.HasPrefix(rest, "o/") && r.Method == http.MethodDelete:
			name, _ := url.PathUnescape(strings.TrimPrefix(rest, "o/"))
			if s.deletes == nil {
				s.deletes = map[string]int{}
			}
			s.deletes[name]++
			if s.flaky[name] > 0 {
				s.flaky[name]--
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"code":503,"message":"try again"}}`))
				return
			}
			if code, ok := s.deleteStatus[name]; ok {
				w.WriteHeader(code)
				_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"stub"}}`, code)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusTeapot)
		}
	}
}

// list serves two items per page, so every listing here pages.
func (s *gcsStub) list(w http.ResponseWriter, prefix, delim, token string) {
	var names, prefixes []string
	seen := map[string]bool{}
	for _, o := range s.objects {
		if !strings.HasPrefix(o, prefix) {
			continue
		}
		if delim != "" {
			if i := strings.Index(o[len(prefix):], delim); i >= 0 {
				p := o[:len(prefix)+i+1]
				if !seen[p] {
					seen[p] = true
					prefixes = append(prefixes, p)
				}
				continue
			}
		}
		names = append(names, o)
	}
	sort.Strings(names)
	start, _ := strconv.Atoi(token)
	end := min(start+2, len(names))
	items := []map[string]any{}
	for _, n := range names[start:end] {
		size := "3"
		if strings.HasSuffix(n, "/") {
			size = "0" // a console folder placeholder is empty
		}
		items = append(items, map[string]any{"name": n, "size": size, "updated": "2026-10-25T10:00:00Z"})
	}
	resp := map[string]any{"items": items}
	if start == 0 && len(prefixes) > 0 {
		resp["prefixes"] = prefixes
	}
	if end < len(names) {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// stubGCSBackend builds a backend rooted at prefix against the stub.
func stubGCSBackend(t *testing.T, s *gcsStub, prefix string, logger zerolog.Logger) (*GCSBackend, error) {
	t.Helper()
	srv := httptest.NewServer(s.handler(t))
	t.Cleanup(srv.Close)
	b, err := newGCSBackend(&GCSConfig{Bucket: "bkt", Prefix: prefix}, logger,
		option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err == nil {
		t.Cleanup(func() { _ = b.Close() })
	}
	return b, err
}

func TestGCSStartupProbe(t *testing.T) {
	t.Run("hierarchical namespace is refused", func(t *testing.T) {
		b, err := stubGCSBackend(t, &gcsStub{hns: true}, "", zerolog.Nop())
		if err == nil || !strings.Contains(err.Error(), "hierarchical namespace") {
			t.Fatalf("HNS bucket: backend %v, err %v", b, err)
		}
	})
	t.Run("soft delete is a warning", func(t *testing.T) {
		var logs bytes.Buffer
		if _, err := stubGCSBackend(t, &gcsStub{softDeleteSeconds: 604800}, "", zerolog.New(&logs)); err != nil {
			t.Fatalf("soft-delete bucket refused: %v", err)
		}
		if !strings.Contains(logs.String(), "soft delete") {
			t.Fatalf("no soft-delete warning in %s", logs.String())
		}
	})
	for _, c := range []struct {
		name string
		stub *gcsStub
		want string
	}{
		{"missing bucket is a warning", &gcsStub{missing: true}, "does not exist"},
		{"forbidden bucket metadata is a warning", &gcsStub{forbidden: true}, "not checked"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var logs bytes.Buffer
			if _, err := stubGCSBackend(t, c.stub, "", zerolog.New(&logs)); err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !strings.Contains(logs.String(), c.want) {
				t.Fatalf("log %s lacks %q", logs.String(), c.want)
			}
		})
	}
}

// Every listing sends the PREFIXED prefix up and hands UNPREFIXED keys back,
// across pages, and drops keys the contract refuses.
func TestGCSListingsStripThePrefixAcrossPages(t *testing.T) {
	s := &gcsStub{objects: []string{
		"inst/db/cpu/2026/a.parquet",
		"inst/db/cpu/2026/b.parquet",
		"inst/db/cpu/2026/c.parquet",
		"inst/db/cpu/2026//bad.parquet",
		"inst/db/cpu/2026/x.parquet.part",
		"inst/db/mem/2026/d.parquet",
		"inst/db/folder/",
		"other/db/cpu/2026/z.parquet",
	}}
	b, err := stubGCSBackend(t, s, "inst", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	keys, err := b.List(ctx, "db/")
	if err != nil {
		t.Fatal(err)
	}
	want := "db/cpu/2026/a.parquet,db/cpu/2026/b.parquet,db/cpu/2026/c.parquet,db/mem/2026/d.parquet"
	if strings.Join(keys, ",") != want {
		t.Fatalf("List = %v, want %s", keys, want)
	}
	objs, err := b.ListObjects(ctx, "db/cpu/")
	if err != nil || len(objs) != 3 || objs[0].Path != "db/cpu/2026/a.parquet" || objs[0].Size != 3 || objs[0].LastModified.IsZero() {
		t.Fatalf("ListObjects = %+v, %v", objs, err)
	}
	un, err := b.ListUnusable(ctx, "db/")
	var unPaths []string
	for _, u := range un {
		unPaths = append(unPaths, u.Path)
	}
	if err != nil || strings.Join(unPaths, ",") != "db/cpu/2026//bad.parquet,db/cpu/2026/x.parquet.part" {
		t.Fatalf("ListUnusable = %v, %v", unPaths, err)
	}
	dirs, err := b.ListDirectories(ctx, "db")
	sort.Strings(dirs)
	if err != nil || strings.Join(dirs, ",") != "cpu,folder,mem" {
		t.Fatalf("ListDirectories = %v, %v", dirs, err)
	}
	if ok, err := b.HasObjectsUnderPrefix(ctx, "db/mem/"); err != nil || !ok {
		t.Fatalf("HasObjectsUnderPrefix(mem) = %v, %v", ok, err)
	}
	if ok, err := b.HasObjectsUnderPrefix(ctx, "db/folder/"); err != nil || ok {
		t.Fatalf("HasObjectsUnderPrefix(folder placeholder) = %v, %v", ok, err)
	}
	for _, p := range s.seenPrefixes {
		if !strings.HasPrefix(p, "inst/") {
			t.Fatalf("a listing asked for %q, outside the backend prefix", p)
		}
	}
}

func TestGCSMissingBucketIsStoreNotFound(t *testing.T) {
	s := &gcsStub{missing: true}
	b, err := stubGCSBackend(t, s, "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b.List(ctx, "db/"); !IsStoreNotFound(err) {
		t.Fatalf("List: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := b.StatFile(ctx, "db/x.parquet"); !IsStoreNotFound(err) || IsNotFound(err) {
			t.Fatalf("StatFile: %v", err)
		}
	}
	// One read at construction, one confirmation; the second StatFile used
	// the cached answer.
	if s.bucketReads != 2 {
		t.Fatalf("bucket metadata read %d times, want 2", s.bucketReads)
	}

	present, err := stubGCSBackend(t, &gcsStub{objects: []string{"db/y.parquet"}}, "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if size, err := present.StatFile(ctx, "db/x.parquet"); err != nil || size != -1 {
		t.Fatalf("StatFile of a missing object = %d, %v", size, err)
	}
	if size, sha, err := present.ObjectSHA(ctx, "db/y.parquet"); err != nil || size != 3 || sha != "abc" {
		t.Fatalf("ObjectSHA = %d %q %v", size, sha, err)
	}
}

// DeleteBatch: already gone is success, a refused key and a failed delete are
// both reported, on the caller's unprefixed spelling, and the rest still go.
func TestGCSDeleteBatchCollectsPerKeyErrors(t *testing.T) {
	s := &gcsStub{deleteStatus: map[string]int{
		"inst/db/gone.parquet":   http.StatusNotFound,
		"inst/db/locked.parquet": http.StatusForbidden,
	}}
	b, err := stubGCSBackend(t, s, "inst", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	err = b.DeleteBatch(context.Background(), []string{"db/ok.parquet", "db/gone.parquet", "db/locked.parquet", "db//bad.parquet"})
	if err == nil {
		t.Fatal("DeleteBatch succeeded")
	}
	msg := err.Error()
	if !strings.Contains(msg, "2 object(s) failed") || !strings.Contains(msg, "db/locked.parquet") || strings.Contains(msg, "inst/db/locked") || strings.Contains(msg, "gone.parquet") {
		t.Fatalf("DeleteBatch error: %s", msg)
	}
	if err := b.Delete(context.Background(), "db/gone.parquet"); err != nil {
		t.Fatalf("Delete of an absent object: %v", err)
	}
}

func TestGCSBackendIdentity(t *testing.T) {
	b, err := stubGCSBackend(t, &gcsStub{}, "inst/abc", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if b.Type() != "gcs" || b.GetPrefix() != "inst/abc/" {
		t.Fatalf("Type %q prefix %q", b.Type(), b.GetPrefix())
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(b.ConfigJSON()), &cfg); err != nil || cfg["bucket"] != "bkt" || cfg["prefix"] != "inst/abc/" {
		t.Fatalf("ConfigJSON = %s", b.ConfigJSON())
	}
	if uri, err := ObjectURI(b, "db/m/x.parquet"); err != nil || uri != "gs://bkt/inst/abc/db/m/x.parquet" {
		t.Fatalf("ObjectURI = %q, %v", uri, err)
	}
	if p := GetLocalBasePath(b, nil, "test", "/fallback"); p != "" {
		t.Fatalf("GetLocalBasePath = %q: a GCS node must never get the local fallback", p)
	}
	if _, err := b.prefixedKey(strings.Repeat("k", MaxKeyLen-len("inst/abc/")+1)); err == nil {
		t.Fatal("a key that fits alone but not with the prefix was accepted")
	}
}

func TestGCSRefusesConfigBeforeAnyRequest(t *testing.T) {
	for _, cfg := range []GCSConfig{{}, {Bucket: "b", Prefix: "a//b"}, {Bucket: "b", Prefix: "../x"}} {
		if b, err := NewGCSBackend(&cfg, zerolog.Nop()); err == nil || b != nil {
			t.Fatalf("NewGCSBackend(%+v) = %v, %v", cfg, b, err)
		}
	}
}

// Every Google spelling of the store is one identity, so a gcs block and an
// S3-interop block naming one bucket overlap; an emulator is its own store.
func TestGCSDestinationIdentity(t *testing.T) {
	gcs := func(endpoint, prefix string) Destination {
		d, err := DestinationFromSpec(BackendSpec{Type: "gcs", GCS: GCSConfig{Bucket: "data", Prefix: prefix, Endpoint: endpoint}})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	interop := func(endpoint, prefix string) Destination {
		d, err := DestinationFromSpec(BackendSpec{Type: "s3", S3: S3Config{Bucket: "data", Prefix: prefix, Endpoint: endpoint}})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	native := gcs("", "arc")
	for _, e := range []string{
		"https://storage.googleapis.com",
		"storage.googleapis.com:443",
		"https://data.storage.googleapis.com",
		"https://storage.googleapis.com/storage/v1/",
		"https://storage.europe-west1.rep.googleapis.com",
		"https://private.googleapis.com",
		"https://restricted.googleapis.com",
		"https://storage-myendpoint.p.googleapis.com",
		"https://www.googleapis.com/storage/v1/",
	} {
		if !native.Overlaps(gcs(e, "arc/cold")) {
			t.Errorf("gcs at %s is not the same store as the default", e)
		}
		if d := interop(e, "arc"); d.Kind != "gcs" || !native.Overlaps(d) {
			t.Errorf("S3 interop at %s = %+v, does not overlap the gcs bucket", e, d)
		}
	}
	for _, e := range []string{"http://localhost:4443", "https://storage.googleapis.com.evil.example", "https://s3.amazonaws.com"} {
		if native.Overlaps(gcs(e, "arc")) {
			t.Errorf("gcs at %s folded onto Google", e)
		}
	}
	if d := interop("", "arc"); d.Kind != "s3" {
		t.Errorf("an S3 block with no endpoint is AWS, got %+v", d)
	}
	if native.Overlaps(gcs("", "arcx")) {
		t.Error("prefix arcx overlaps arc")
	}
}

// A transient answer (503) is retried until it clears; a definitive one (403,
// 404) is not retried at all.
func TestGCSRetriesTransientErrorsOnly(t *testing.T) {
	s := &gcsStub{
		flaky:        map[string]int{"db/busy.parquet": 2},
		deleteStatus: map[string]int{"db/locked.parquet": http.StatusForbidden, "db/gone.parquet": http.StatusNotFound},
	}
	b, err := stubGCSBackend(t, s, "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.Delete(ctx, "db/busy.parquet"); err != nil || s.deletes["db/busy.parquet"] != 3 {
		t.Fatalf("Delete through two 503s: %v after %d requests, want success after 3", err, s.deletes["db/busy.parquet"])
	}
	if err := b.Delete(ctx, "db/locked.parquet"); err == nil || s.deletes["db/locked.parquet"] != 1 {
		t.Fatalf("Delete answered 403: %v after %d requests, want an error after 1", err, s.deletes["db/locked.parquet"])
	}
	if err := b.Delete(ctx, "db/gone.parquet"); err != nil || s.deletes["db/gone.parquet"] != 1 {
		t.Fatalf("Delete answered 404: %v after %d requests, want success after 1", err, s.deletes["db/gone.parquet"])
	}
	s.flaky["db/hopeless.parquet"] = 100
	if err := b.Delete(ctx, "db/hopeless.parquet"); err == nil || s.deletes["db/hopeless.parquet"] != gcsRetryAttempts {
		t.Fatalf("Delete answered 503 forever: %v after %d requests, want an error after %d", err, s.deletes["db/hopeless.parquet"], gcsRetryAttempts)
	}
}
