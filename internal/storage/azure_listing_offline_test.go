package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The five listings and DeleteBatch, driven through the REAL methods against a
// canned Azure endpoint, with no credentials and no network (#1102).
//
// This exists because the obvious unit test cannot fail. An assertion that
// re-implements `strings.TrimPrefix(name, prefix)` in the test body passes
// whether or not azure.go does the same thing, and the listings are the half
// of the prefix change most likely to regress: five methods, each stripping
// independently, none of them exercised by any test that runs in CI (the
// contract test is build-tagged and skips without an Azure account).
//
// AzureBlobConfig.Endpoint is the seam. The SDK talks ordinary HTTP to it, so
// an httptest server returning the ListBlobs XML drives the pagers, the
// hierarchy delimiter and the batch builder exactly as the service would.
// Deleting any one strip in azure.go fails a case here.

// azureListStub serves the canned ListBlobs responses and records what was
// asked for, so a test can assert on the REQUEST (the prefix sent up) as well
// as on the result (the keys handed back).
type azureListStub struct {
	// blobs are the full blob names in the container, as the service holds
	// them: prefixed.
	blobs []stubBlob
	// seenPrefixes records the `prefix` query parameter of every list call.
	seenPrefixes []string
	// batchBodies records the raw body of every batch submission.
	batchBodies []string
}

type stubBlob struct {
	name string
	size int64
}

func (s *azureListStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case q.Get("comp") == "list":
			prefix := q.Get("prefix")
			s.seenPrefixes = append(s.seenPrefixes, prefix)
			w.Header().Set("Content-Type", "application/xml")
			if d := q.Get("delimiter"); d != "" {
				_, _ = io.WriteString(w, s.hierarchyXML(prefix, d))
				return
			}
			_, _ = io.WriteString(w, s.flatXML(prefix))
		case q.Get("comp") == "batch":
			body, _ := io.ReadAll(r.Body)
			s.batchBodies = append(s.batchBodies, string(body))
			// A 403 ends the call with an error the test ignores: the
			// assertion is on the sub-request keys in the body, which the SDK
			// has already built and sent by now. Returning a hand-rolled
			// multipart batch response would add a second format to keep
			// correct for no extra coverage.
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusForbidden)
		}
	}
}

func (s *azureListStub) flatXML(prefix string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` +
		`<EnumerationResults ServiceEndpoint="http://stub/" ContainerName="cont">`)
	fmt.Fprintf(&b, "<Prefix>%s</Prefix><Blobs>", prefix)
	for _, blob := range s.blobs {
		if !strings.HasPrefix(blob.name, prefix) {
			continue
		}
		fmt.Fprintf(&b, `<Blob><Name>%s</Name><Properties>`+
			`<Last-Modified>Mon, 06 Oct 2026 00:00:00 GMT</Last-Modified>`+
			`<Content-Length>%d</Content-Length><Etag>"0x1"</Etag>`+
			`</Properties></Blob>`, blob.name, blob.size)
	}
	b.WriteString("</Blobs><NextMarker/></EnumerationResults>")
	return b.String()
}

func (s *azureListStub) hierarchyXML(prefix, delimiter string) string {
	// Collapse everything under prefix at the first delimiter, as the service
	// does: a name with a further separator becomes a BlobPrefix, one without
	// stays a Blob.
	seen := map[string]bool{}
	var dirs []string
	var flat []stubBlob
	for _, blob := range s.blobs {
		if !strings.HasPrefix(blob.name, prefix) {
			continue
		}
		rest := strings.TrimPrefix(blob.name, prefix)
		if i := strings.Index(rest, delimiter); i >= 0 {
			d := prefix + rest[:i+len(delimiter)]
			if !seen[d] {
				seen[d] = true
				dirs = append(dirs, d)
			}
			continue
		}
		flat = append(flat, blob)
	}
	sort.Strings(dirs)

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` +
		`<EnumerationResults ServiceEndpoint="http://stub/" ContainerName="cont">`)
	fmt.Fprintf(&b, "<Prefix>%s</Prefix><Delimiter>%s</Delimiter><Blobs>", prefix, delimiter)
	for _, blob := range flat {
		fmt.Fprintf(&b, `<Blob><Name>%s</Name><Properties>`+
			`<Last-Modified>Mon, 06 Oct 2026 00:00:00 GMT</Last-Modified>`+
			`<Content-Length>%d</Content-Length><Etag>"0x1"</Etag>`+
			`</Properties></Blob>`, blob.name, blob.size)
	}
	for _, d := range dirs {
		fmt.Fprintf(&b, "<BlobPrefix><Name>%s</Name></BlobPrefix>", d)
	}
	b.WriteString("</Blobs><NextMarker/></EnumerationResults>")
	return b.String()
}

// newStubbedAzureBackend builds a real AzureBlobBackend talking to the stub.
func newStubbedAzureBackend(t *testing.T, prefix string, blobs []stubBlob) (*AzureBlobBackend, *azureListStub) {
	t.Helper()
	stub := &azureListStub{blobs: blobs}
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)

	// Fake, syntactically valid base64. Never a real credential.
	const fakeKey = "dGhpcy1pcy1ub3QtYS1yZWFsLWtleQ=="
	b, err := NewAzureBlobBackend(&AzureBlobConfig{
		ContainerName: "cont",
		Prefix:        prefix,
		AccountName:   "arctestaccount",
		AccountKey:    fakeKey,
		Endpoint:      srv.URL,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAzureBlobBackend against the stub: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, stub
}

const stubPrefix = "arc/"

func stubContents() []stubBlob {
	return []stubBlob{
		{stubPrefix + "mydb/cpu/2026/10/06/14/a.parquet", 4},
		{stubPrefix + "mydb/cpu/2026/10/06/14/b.parquet", 8},
		{stubPrefix + "mydb/mem/2026/10/06/14/c.parquet", 16},
		// A key the contract refuses, which ListUnusable must report and the
		// other listings must hide. Reported on its UNPREFIXED spelling.
		{stubPrefix + `mydb/cpu/2026/10/06/14/bad\key.parquet`, 32},
	}
}

// List, ListObjects, ListDirectories, ListUnusable and HasObjectsUnderPrefix
// all send the backend prefix up and all hand back keys without it.
func TestAzureListingsStripThePrefix(t *testing.T) {
	ctx := context.Background()

	t.Run("List", func(t *testing.T) {
		b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())
		got, err := b.List(ctx, "mydb/")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		want := []string{
			"mydb/cpu/2026/10/06/14/a.parquet",
			"mydb/cpu/2026/10/06/14/b.parquet",
			"mydb/mem/2026/10/06/14/c.parquet",
		}
		assertKeys(t, "List", got, want)
		assertSentPrefix(t, stub, stubPrefix+"mydb/")
	})

	t.Run("ListObjects", func(t *testing.T) {
		b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())
		objs, err := b.ListObjects(ctx, "mydb/")
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		var got []string
		for _, o := range objs {
			got = append(got, o.Path)
			if o.Size == 0 {
				t.Errorf("ListObjects returned %q with size 0; the stub serves real sizes", o.Path)
			}
		}
		assertKeys(t, "ListObjects", got, []string{
			"mydb/cpu/2026/10/06/14/a.parquet",
			"mydb/cpu/2026/10/06/14/b.parquet",
			"mydb/mem/2026/10/06/14/c.parquet",
		})
		assertSentPrefix(t, stub, stubPrefix+"mydb/")
	})

	t.Run("ListDirectories", func(t *testing.T) {
		b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())
		// No trailing separator from the caller: the method adds one, and the
		// backend prefix goes in front of the result.
		got, err := b.ListDirectories(ctx, "mydb")
		if err != nil {
			t.Fatalf("ListDirectories: %v", err)
		}
		assertKeys(t, "ListDirectories", got, []string{"cpu", "mem"})
		assertSentPrefix(t, stub, stubPrefix+"mydb/")
	})

	t.Run("ListDirectories at the root", func(t *testing.T) {
		b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())
		got, err := b.ListDirectories(ctx, "")
		if err != nil {
			t.Fatalf("ListDirectories: %v", err)
		}
		// "mydb", not "arc" and not "arc/mydb": the backend prefix is not a
		// directory any caller may see.
		assertKeys(t, "ListDirectories", got, []string{"mydb"})
		assertSentPrefix(t, stub, stubPrefix)
	})

	t.Run("ListUnusable", func(t *testing.T) {
		b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())
		objs, err := b.ListUnusable(ctx, "mydb/")
		if err != nil {
			t.Fatalf("ListUnusable: %v", err)
		}
		var got []string
		for _, o := range objs {
			got = append(got, o.Path)
		}
		assertKeys(t, "ListUnusable", got, []string{`mydb/cpu/2026/10/06/14/bad\key.parquet`})
		assertSentPrefix(t, stub, stubPrefix+"mydb/")
	})

	t.Run("HasObjectsUnderPrefix", func(t *testing.T) {
		b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())
		has, err := b.HasObjectsUnderPrefix(ctx, "mydb/")
		if err != nil || !has {
			t.Fatalf("HasObjectsUnderPrefix = %v, %v; want true", has, err)
		}
		assertSentPrefix(t, stub, stubPrefix+"mydb/")

		// A prefix holding nothing must answer false rather than matching the
		// whole container because the backend prefix was dropped.
		b2, stub2 := newStubbedAzureBackend(t, stubPrefix, stubContents())
		has, err = b2.HasObjectsUnderPrefix(ctx, "otherdb/")
		if err != nil {
			t.Fatalf("HasObjectsUnderPrefix: %v", err)
		}
		if has {
			t.Error("HasObjectsUnderPrefix said true for a prefix holding nothing")
		}
		assertSentPrefix(t, stub2, stubPrefix+"otherdb/")
	})
}

// With no prefix configured every listing is byte-identical to the pre-#1102
// behaviour, which is what every existing Azure deployment depends on.
func TestAzureListingsWithNoPrefixAreUnchanged(t *testing.T) {
	ctx := context.Background()
	blobs := []stubBlob{
		{"mydb/cpu/2026/10/06/14/a.parquet", 4},
		{"mydb/mem/2026/10/06/14/c.parquet", 16},
	}
	b, stub := newStubbedAzureBackend(t, "", blobs)

	got, err := b.List(ctx, "mydb/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	assertKeys(t, "List", got, []string{
		"mydb/cpu/2026/10/06/14/a.parquet",
		"mydb/mem/2026/10/06/14/c.parquet",
	})
	assertSentPrefix(t, stub, "mydb/")

	dirs, err := b.ListDirectories(ctx, "")
	if err != nil {
		t.Fatalf("ListDirectories: %v", err)
	}
	assertKeys(t, "ListDirectories", dirs, []string{"mydb"})
}

// DeleteBatch is the one key-taking method whose keys never pass through
// prefixedKey: partitionValidKeys hands them straight to the batch builder.
// The sub-requests must therefore carry PREFIXED blob names.
func TestAzureDeleteBatchSendsPrefixedNames(t *testing.T) {
	ctx := context.Background()
	b, stub := newStubbedAzureBackend(t, stubPrefix, stubContents())

	// The error is expected: the stub answers the batch POST with 403. The
	// assertion is on what was sent, which by then is already on the wire.
	_ = b.DeleteBatch(ctx, []string{
		"mydb/cpu/2026/10/06/14/a.parquet",
		"mydb/mem/2026/10/06/14/c.parquet",
	})

	if len(stub.batchBodies) != 1 {
		t.Fatalf("batch submissions = %d, want 1", len(stub.batchBodies))
	}
	// The SDK percent-encodes the separators in each sub-request line
	// ("/cont/arc%2Fmydb%2F..."), so compare the decoded form: the assertion
	// is about which blob is addressed, not about how the SDK spells a slash.
	targets := batchDeleteTargets(t, stub.batchBodies[0])
	want := []string{
		"/cont/arc/mydb/cpu/2026/10/06/14/a.parquet",
		"/cont/arc/mydb/mem/2026/10/06/14/c.parquet",
	}
	assertKeys(t, "DeleteBatch sub-requests", targets, want)
	for _, got := range targets {
		if strings.HasPrefix(got, "/cont/mydb/") {
			t.Errorf("a sub-request addresses the UNPREFIXED key %q; a batch that drops the prefix deletes at the container root", got)
		}
	}
}

// batchDeleteTargets pulls the decoded request target out of each DELETE
// sub-request line of a batch body. Only those lines: the body also carries a
// SharedKey signature per sub-request, which has no place in test output.
func batchDeleteTargets(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "DELETE ") {
			continue
		}
		target := strings.TrimPrefix(line, "DELETE ")
		if i := strings.Index(target, " "); i >= 0 {
			target = target[:i]
		}
		decoded, err := url.PathUnescape(target)
		if err != nil {
			t.Fatalf("sub-request target %q is not decodable: %v", target, err)
		}
		out = append(out, decoded)
	}
	if len(out) == 0 {
		t.Fatal("the batch body carried no DELETE sub-request")
	}
	return out
}

func assertKeys(t *testing.T, what string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if len(g) != len(w) {
		t.Fatalf("%s returned %v, want %v", what, got, want)
	}
	for i := range w {
		if g[i] != w[i] {
			t.Fatalf("%s returned %v, want %v (a key still carrying %q means the strip is missing)", what, got, want, stubPrefix)
		}
	}
}

func assertSentPrefix(t *testing.T, stub *azureListStub, want string) {
	t.Helper()
	if len(stub.seenPrefixes) == 0 {
		t.Fatalf("no list request reached the service")
	}
	if got := stub.seenPrefixes[0]; got != want {
		t.Errorf("the listing asked the service for prefix %q, want %q", got, want)
	}
}
