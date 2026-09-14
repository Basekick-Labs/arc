//go:build objectstore

package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	for _, k := range unusable {
		if !reported[k] {
			t.Errorf("%q must be reported as unusable; on S3 nothing else can name it. got %v", k, hidden)
		}
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
