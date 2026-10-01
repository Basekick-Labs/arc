//go:build objectstore

package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// On an object store the same partition property must hold, and the two
// exclusions have to behave the opposite way round from local:
//
//   - a ".part" key IS reported, because S3 does not stage writes and does not
//     implement StagingInspector, so nothing else could ever name it;
//   - a zero-length directory marker is NOT reported, because consoles and sync
//     tools write those and they hold no data.
func TestS3ListUnusablePartitionsTheBucket(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()
	const base = "unusable756"

	// Written through the backend: ordinary, addressable.
	good := base + "/cpu/2026/09/12/13/good.parquet"
	if err := b.Write(ctx, good, []byte("PAR1")); err != nil {
		t.Fatalf("write good: %v", err)
	}
	t.Cleanup(func() { _ = b.Delete(ctx, good) })

	// Written past the backend, because the contract is what refuses them.
	unusable := []string{
		base + "/cpu/2026/09/12/13/ba\\d.parquet", // backslash
		base + "/cpu/2026/09/12/13/legacy.part",   // reserved suffix, committed object here
	}
	for _, k := range unusable {
		if err := rawPut(t, b, k, []byte("PAR1-rows")); err != nil {
			t.Fatalf("raw put %q: %v", k, err)
		}
		t.Cleanup(func() { _ = rawDelete(t, b, k) })
	}
	// A zero-length directory marker.
	marker := base + "/cpu/"
	if err := rawPut(t, b, marker, nil); err != nil {
		t.Fatalf("raw put marker: %v", err)
	}
	t.Cleanup(func() { _ = rawDelete(t, b, marker) })

	objs, err := b.ListObjects(ctx, base+"/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	hidden, err := b.ListUnusable(ctx, base+"/")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}

	listed := map[string]bool{}
	for _, o := range objs {
		listed[o.Path] = true
	}
	reported := map[string]bool{}
	for _, o := range hidden {
		if listed[o.Path] {
			t.Errorf("%q is both listed and reported unusable", o.Path)
		}
		reported[o.Path] = true
	}

	if !listed[good] {
		t.Errorf("the ordinary object must be listed, got %v", objs)
	}
	// The invariant is that no committed object is silently unaddressable,
	// and a store can satisfy it two ways. A store that PRESERVES an
	// unusable spelling must report the key, because nothing else can name
	// it. A store that cannot hold the spelling at all — SeaweedFS, like
	// Azure, treats a backslash as a path separator, so the raw PUT above
	// landed under a folded key — leaves nothing unaddressable behind: the
	// folded object is an ordinary listed, readable key, covered by the
	// assertions above. Only a preserved-but-unreported spelling is a
	// failure, and the store's own raw listing is the one witness of what
	// it preserved (HEAD is no witness: a folding store folds lookups too).
	stored := map[string]bool{}
	rawKeys, err := rawList(t, b, base+"/")
	if err != nil {
		t.Fatalf("raw list: %v", err)
	}
	for _, k := range rawKeys {
		stored[k] = true
	}
	for _, k := range unusable {
		if reported[k] {
			continue
		}
		if !stored[b.prefix+k] {
			t.Logf("%q not reported: the store does not preserve this spelling (stored under a folded key)", k)
			continue
		}
		t.Errorf("%q must be reported as unusable; the store preserves the spelling and nothing else can name it. got %v", k, hidden)
	}
	if reported[marker] {
		t.Errorf("a zero-length directory marker must not be reported as lost data")
	}
}

// rawPut writes a key past the backend's validation, with a SigV4-signed PUT,
// so the test can create the shapes the contract exists to refuse.
func rawPut(t *testing.T, b *S3Backend, key string, body []byte) error {
	t.Helper()
	return rawS3(t, b, http.MethodPut, key, body)
}

func rawDelete(t *testing.T, b *S3Backend, key string) error {
	t.Helper()
	return rawS3(t, b, http.MethodDelete, key, nil)
}

// rawList returns the store's own spelling of every key under prefix, past
// the backend's validation and filtering, so a test can ask what the store
// actually preserved. Single page; the test prefixes hold a handful of
// objects.
func rawList(t *testing.T, b *S3Backend, prefix string) ([]string, error) {
	t.Helper()
	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	query := "list-type=2&prefix=" + url.QueryEscape(b.prefix+prefix)
	u := strings.TrimSuffix(endpoint, "/") + "/" + b.bucket + "?" + query

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	payload := sha256.Sum256(nil)
	payloadHex := hex.EncodeToString(payload[:])
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHex)

	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n", req.URL.Host, payloadHex, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		http.MethodGet, "/" + b.bucket, query, canonicalHeaders, signedHeaders, payloadHex,
	}, "\n")
	crHash := sha256.Sum256([]byte(canonicalRequest))

	scope := dateStamp + "/us-east-1/s3/aws4_request"
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(crHash[:])}, "\n")

	mac := func(k, d []byte) []byte { h := hmac.New(sha256.New, k); h.Write(d); return h.Sum(nil) }
	kDate := mac([]byte("AWS4"+"minioadmin"), []byte(dateStamp))
	kRegion := mac(kDate, []byte("us-east-1"))
	kService := mac(kRegion, []byte("s3"))
	kSigning := mac(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(mac(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=minioadmin/%s, SignedHeaders=%s, Signature=%s", scope, signedHeaders, sig))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("raw list: HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var keys []string
	rest := string(raw)
	for {
		start := strings.Index(rest, "<Key>")
		if start < 0 {
			break
		}
		rest = rest[start+len("<Key>"):]
		end := strings.Index(rest, "</Key>")
		if end < 0 {
			break
		}
		var key string
		// Keys arrive XML-escaped; the escapes the contract's spellings can
		// produce are the standard five entities, which xml.Unmarshal
		// handles via a wrapper element.
		if err := xml.Unmarshal([]byte("<k>"+rest[:end]+"</k>"), &key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
		rest = rest[end:]
	}
	return keys, nil
}

func rawS3(t *testing.T, b *S3Backend, method, key string, body []byte) error {
	t.Helper()
	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	full := b.prefix + key
	u := strings.TrimSuffix(endpoint, "/") + "/" + b.bucket + "/" + urlEscapeKey(full)

	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	payload := sha256.Sum256(body)
	payloadHex := hex.EncodeToString(payload[:])
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHex)
	req.Header.Set("host", req.URL.Host)

	canonicalURI := "/" + b.bucket + "/" + urlEscapeKey(full)
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n", req.URL.Host, payloadHex, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{method, canonicalURI, "", canonicalHeaders, signedHeaders, payloadHex}, "\n")
	crHash := sha256.Sum256([]byte(canonicalRequest))

	scope := dateStamp + "/us-east-1/s3/aws4_request"
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(crHash[:])}, "\n")

	mac := func(k, d []byte) []byte { h := hmac.New(sha256.New, k); h.Write(d); return h.Sum(nil) }
	kDate := mac([]byte("AWS4"+"minioadmin"), []byte(dateStamp))
	kRegion := mac(kDate, []byte("us-east-1"))
	kService := mac(kRegion, []byte("s3"))
	kSigning := mac(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(mac(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=minioadmin/%s, SignedHeaders=%s, Signature=%s", scope, signedHeaders, sig))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d", method, key, resp.StatusCode)
	}
	return nil
}

// urlEscapeKey path-escapes each segment, keeping separators, which is what S3
// expects and what url.PathEscape alone does not do.
func urlEscapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}
