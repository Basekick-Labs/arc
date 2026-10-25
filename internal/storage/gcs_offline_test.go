package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi"
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
	requesterPays      bool
	retentionSeconds   int
	retentionLocked    bool
	storageClass       string
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
	// rewriteCalls is how many rewrite calls a copy takes; noToken answers an
	// unfinished rewrite without a token. uploads counts upload requests,
	// every one of which answers 503.
	rewriteCalls, rewrites, uploads int
	rewriteTokens                   []string
	noToken                         bool
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
			if s.requesterPays {
				attrs["billing"] = map[string]any{"requesterPays": true}
			}
			if s.retentionSeconds > 0 {
				attrs["retentionPolicy"] = map[string]any{"retentionPeriod": strconv.Itoa(s.retentionSeconds), "isLocked": s.retentionLocked}
			}
			if s.storageClass != "" {
				attrs["storageClass"] = s.storageClass
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
		case strings.Contains(rest, "/rewriteTo/") && r.Method == http.MethodPost:
			s.rewrites++
			s.rewriteTokens = append(s.rewriteTokens, r.URL.Query().Get("rewriteToken"))
			resp := map[string]any{"done": s.rewrites >= s.rewriteCalls}
			if s.rewrites < s.rewriteCalls && !s.noToken {
				resp["rewriteToken"] = fmt.Sprintf("t%d", s.rewrites)
			}
			_ = json.NewEncoder(w).Encode(resp)
		case strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/") && r.Method == http.MethodPost:
			s.uploads++
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":503,"message":"try again"}}`))
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
	// The bare server URL, no "/storage/v1/": GCSEndpointURL has to supply it,
	// as it does for an operator's endpoint.
	b, err := newGCSBackend(&GCSConfig{Bucket: "bkt", Prefix: prefix, Endpoint: srv.URL}, logger,
		option.WithoutAuthentication())
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
	t.Run("requester pays is refused", func(t *testing.T) {
		b, err := stubGCSBackend(t, &gcsStub{requesterPays: true}, "", zerolog.Nop())
		if err == nil || !strings.Contains(err.Error(), "requester pays") {
			t.Fatalf("requester-pays bucket: backend %v, err %v", b, err)
		}
	})
	t.Run("a STANDARD bucket logs no warning", func(t *testing.T) {
		var logs bytes.Buffer
		if _, err := stubGCSBackend(t, &gcsStub{storageClass: "STANDARD"}, "", zerolog.New(&logs)); err != nil {
			t.Fatalf("refused: %v", err)
		}
		if strings.Contains(logs.String(), `"level":"warn"`) {
			t.Fatalf("unexpected warning: %s", logs.String())
		}
	})
	for _, c := range []struct {
		name string
		stub *gcsStub
		want string
	}{
		{"missing bucket is a warning", &gcsStub{missing: true}, "does not exist"},
		{"forbidden bucket metadata is a warning", &gcsStub{forbidden: true}, "NOT checked"},
		{"retention policy is a warning", &gcsStub{retentionSeconds: 86400}, "has a retention policy"},
		{"locked retention policy is a louder warning", &gcsStub{retentionSeconds: 86400, retentionLocked: true}, "LOCKED retention policy"},
		{"non-STANDARD class is a warning", &gcsStub{storageClass: "NEARLINE"}, `"storage_class":"NEARLINE"`},
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
	if d := interop("https://commondatastorage.googleapis.com", "arc"); !native.Overlaps(d) || !strings.Contains(d.String(), "s3 block") {
		t.Errorf("S3 interop on the legacy XML host = %+v (%s)", d, d.String())
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

func TestGCSEndpointURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://storage-psc.p.googleapis.com":             "https://storage-psc.p.googleapis.com/storage/v1/",
		"https://storage-psc.p.googleapis.com/":            "https://storage-psc.p.googleapis.com/storage/v1/",
		"https://storage-psc.p.googleapis.com/storage/v1":  "https://storage-psc.p.googleapis.com/storage/v1/",
		"https://storage-psc.p.googleapis.com/storage/v1/": "https://storage-psc.p.googleapis.com/storage/v1/",
		"localhost:4443":                               "https://localhost:4443/storage/v1/",
		"http://localhost:4443":                        "http://localhost:4443/storage/v1/",
		" https://storage.googleapis.com/storage/v1/ ": "https://storage.googleapis.com/storage/v1/",
	} {
		if got, err := GCSEndpointURL(in); err != nil || got != want {
			t.Errorf("GCSEndpointURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"https://h/%zz/", "ftp://h", "https://", "https://h/?x=1"} {
		if got, err := GCSEndpointURL(bad); err == nil {
			t.Errorf("GCSEndpointURL(%q) = %q, want an error", bad, got)
		}
		if b, err := newGCSBackend(&GCSConfig{Bucket: "b", Endpoint: bad}, zerolog.Nop(), option.WithoutAuthentication()); err == nil || b != nil {
			t.Errorf("newGCSBackend with endpoint %q = %v, %v; want a refusal, not a backend", bad, b, err)
		}
	}
}

func TestGCSUploadChunk(t *testing.T) {
	const g = 256 << 10
	for size, want := range map[int64]int{
		-1: gcsChunkSize, 0: gcsChunkSize, 1: g, g - 1: g, g: 2 * g,
		gcsChunkSize - 1: gcsChunkSize, gcsChunkSize: gcsChunkSize, 1 << 30: gcsChunkSize,
	} {
		if got := gcsUploadChunk(size); got != want {
			t.Errorf("gcsUploadChunk(%d) = %d, want %d", size, got, want)
		}
		// One request when the object fits: the chunk must exceed the size.
		if size > 0 && size < gcsChunkSize && int64(gcsUploadChunk(size)) <= size {
			t.Errorf("gcsUploadChunk(%d) does not exceed the size", size)
		}
	}
}

// An upload stops retrying after gcsUploadRetries transient failures instead
// of running until its context ends.
func TestGCSUploadRetriesAreBounded(t *testing.T) {
	old := gcsUploadRetries
	gcsUploadRetries = 2
	t.Cleanup(func() { gcsUploadRetries = old })
	s := &gcsStub{}
	b, err := stubGCSBackend(t, s, "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Write(context.Background(), "db/x.parquet", []byte("abc")); err == nil || s.uploads != 3 {
		t.Fatalf("Write against a store answering 503: %v after %d requests, want an error after 3", err, s.uploads)
	}
}

// A copy that GCS finishes in several rewrite calls passes each call's token
// to the next; one reported unfinished without a token fails rather than
// restarting forever.
func TestGCSCopyFollowsTheRewriteToken(t *testing.T) {
	s := &gcsStub{rewriteCalls: 3}
	b, err := stubGCSBackend(t, s, "inst", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CopyObject(context.Background(), "a/src.parquet", "a/dst.parquet", 3, "sha"); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if strings.Join(s.rewriteTokens, ",") != ",t1,t2" {
		t.Fatalf("rewrite tokens sent = %q, want \"\", t1, t2", s.rewriteTokens)
	}
	s2 := &gcsStub{rewriteCalls: 3, noToken: true}
	b2, err := stubGCSBackend(t, s2, "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.CopyObject(context.Background(), "a/src.parquet", "a/dst.parquet", 3, "sha"); err == nil || s2.rewrites != 1 {
		t.Fatalf("CopyObject without a token: %v after %d calls, want an error after 1", err, s2.rewrites)
	}
}

// The compaction subprocess builds its backend with SkipProbe: no bucket
// metadata read, so a requester-pays refusal (already applied by the parent)
// cannot fire and no billed request is added per job.
func TestGCSSkipProbeReadsNoBucketMetadata(t *testing.T) {
	s := &gcsStub{requesterPays: true}
	srv := httptest.NewServer(s.handler(t))
	t.Cleanup(srv.Close)
	b, err := newGCSBackend(&GCSConfig{Bucket: "bkt", Endpoint: srv.URL, SkipProbe: true}, zerolog.Nop(),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("SkipProbe backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if s.bucketReads != 0 {
		t.Fatalf("SkipProbe read bucket metadata %d times", s.bucketReads)
	}
}

// stubGCSBackendRole is stubGCSBackend for a bucket in another role.
func stubGCSBackendRole(t *testing.T, s *gcsStub, role GCSRole, logger zerolog.Logger) (*GCSBackend, error) {
	t.Helper()
	srv := httptest.NewServer(s.handler(t))
	t.Cleanup(srv.Close)
	b, err := newGCSBackend(&GCSConfig{Bucket: "bkt", Endpoint: srv.URL, Role: role}, logger, option.WithoutAuthentication())
	if err == nil {
		t.Cleanup(func() { _ = b.Close() })
	}
	return b, err
}

// #1105 part 3: the startup warnings depend on what the bucket is for. A
// colder class is the cold tier's retrieval cost and a backup's choice; soft
// delete matters where compaction deletes continuously, i.e. primary only.
func TestGCSStartupWarningsDependOnRole(t *testing.T) {
	for _, c := range []struct {
		name    string
		stub    *gcsStub
		role    GCSRole
		want    string // "" = no warning at all
		wantNot string
	}{
		{"primary NEARLINE", &gcsStub{storageClass: "NEARLINE"}, GCSRolePrimary, "use STANDARD for Arc's primary storage", ""},
		{"cold NEARLINE", &gcsStub{storageClass: "NEARLINE"}, GCSRoleCold, "Autoclass", "primary storage"},
		{"backup NEARLINE", &gcsStub{storageClass: "NEARLINE"}, GCSRoleBackup, "", ""},
		{"backup soft delete", &gcsStub{softDeleteSeconds: 604800}, GCSRoleBackup, "", ""},
		{"cold soft delete", &gcsStub{softDeleteSeconds: 604800}, GCSRoleCold, "", ""},
		{"backup retention", &gcsStub{retentionSeconds: 86400}, GCSRoleBackup, "deleting a backup", "compaction"},
		{"cold retention", &gcsStub{retentionSeconds: 86400}, GCSRoleCold, "cold-tier files", "compaction"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var logs bytes.Buffer
			if _, err := stubGCSBackendRole(t, c.stub, c.role, zerolog.New(&logs)); err != nil {
				t.Fatalf("refused: %v", err)
			}
			out := logs.String()
			if c.want == "" {
				if strings.Contains(out, `"level":"warn"`) {
					t.Fatalf("unexpected warning: %s", out)
				}
				return
			}
			if !strings.Contains(out, c.want) || (c.wantNot != "" && strings.Contains(out, c.wantNot)) {
				t.Fatalf("log %s: want %q, not %q", out, c.want, c.wantNot)
			}
		})
	}
	t.Run("requester pays is refused in every role", func(t *testing.T) {
		for _, r := range []GCSRole{GCSRolePrimary, GCSRoleCold, GCSRoleBackup} {
			if _, err := stubGCSBackendRole(t, &gcsStub{requesterPays: true}, r, zerolog.Nop()); err == nil {
				t.Fatalf("role %q accepted a requester-pays bucket", r)
			}
		}
	})
}

// With no credentials anywhere, a primary fails construction (a failed start)
// while a cold tier or backup target builds and fails on first use, as S3 and
// Azure do. Built eagerly, a cold tier left the node without its cold backend.
func TestGCSCredentialsLazyOutsidePrimary(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "absent.json"))
	if _, err := GCSCredentials(&GCSConfig{Bucket: "b"}); err == nil {
		t.Fatal("primary with no credentials must fail at construction")
	}
	for _, r := range []GCSRole{GCSRoleCold, GCSRoleBackup} {
		creds, err := GCSCredentials(&GCSConfig{Bucket: "b", Role: r})
		if err != nil || creds == nil {
			t.Fatalf("role %q: %v", r, err)
		}
		if _, err := creds.Token(context.Background()); err == nil || !strings.Contains(err.Error(), "no GCS credentials") {
			t.Fatalf("role %q: first token error %v", r, err)
		}
	}
}

// #1105 part 4: a 429 (GCS's per-object write rate, bucket ramp-up) gets more
// attempts than other transient errors, and every retry waits at least half
// the rate-limit minimum, never close to zero.
func TestGCSRetryRateLimit(t *testing.T) {
	oldMin, oldMax := gcsRateLimitMinBackoff, gcsRateLimitMaxBackoff
	gcsRateLimitMinBackoff, gcsRateLimitMaxBackoff = 20*time.Millisecond, 40*time.Millisecond
	t.Cleanup(func() { gcsRateLimitMinBackoff, gcsRateLimitMaxBackoff = oldMin, oldMax })

	tooMany := &googleapi.Error{Code: http.StatusTooManyRequests}
	calls := 0
	var at []time.Time
	err := gcsRetry(context.Background(), func() error {
		calls++
		at = append(at, time.Now())
		return tooMany
	})
	if err == nil || calls != gcsRateLimitAttempts {
		t.Fatalf("429 forever: %d calls, err %v; want %d calls then the error", calls, err, gcsRateLimitAttempts)
	}
	for i := 1; i < len(at); i++ {
		if gap := at[i].Sub(at[i-1]); gap < gcsRateLimitMinBackoff/2 {
			t.Fatalf("retry %d came after %v, under half the rate-limit minimum", i, gap)
		}
	}

	calls = 0
	if err := gcsRetry(context.Background(), func() error {
		calls++
		if calls < 8 {
			return tooMany
		}
		return nil
	}); err != nil || calls != 8 {
		t.Fatalf("429 seven times then success: %d calls, %v", calls, err)
	}

	calls = 0
	unavailable := &googleapi.Error{Code: http.StatusServiceUnavailable}
	if err := gcsRetry(context.Background(), func() error { calls++; return unavailable }); err == nil || calls != gcsRetryAttempts {
		t.Fatalf("503 forever: %d calls; want the plain %d", calls, gcsRetryAttempts)
	}
}
