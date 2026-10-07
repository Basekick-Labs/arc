//go:build objectstore

// Package storage's key contract, verified against real object stores.
//
// Tagged `objectstore` because it needs live backends. Unit tests cannot find
// what this finds: the behaviours that motivated #743 (MinIO folding a leading
// slash, Azure treating a backslash as a separator) are properties of the
// servers, not of any mock.
//
// Run with (SeaweedFS stands in for S3; see .github/workflows/ci.yml for the
// identities file that supplies the minioadmin credentials these tests use):
//
//	docker run -d --name arc-seaweedfs -p 9000:8333 \
//	  -v /tmp/seaweed-s3.json:/etc/seaweedfs/s3.json:ro \
//	  chrislusf/seaweedfs:4.47 server -s3 -s3.port=8333 -s3.config=/etc/seaweedfs/s3.json
//	ARC_TEST_S3_BUCKET=arctest go test -tags='duckdb_arrow objectstore' ./internal/storage/
//
// CI does this in .github/workflows/ci.yml.
package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestS3WriteReaderUnseekableOverHTTP pins that a body the SDK cannot rewind
// uploads over the plain-HTTP endpoint CI uses: as one PutObject (1 KiB) and
// as a multipart upload (20 MiB, past one 16 MiB part). The SDK refused the
// former client-side before a byte was sent; the latter — CRC32 headers on
// every part and on the completion — had never been observed against a real
// store. The pattern is positional so a mis-ordered or missing part shows.
func TestS3WriteReaderUnseekableOverHTTP(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()

	for _, size := range []int{1024, 20 * 1024 * 1024} {
		key := fmt.Sprintf("contract-unseekable/x/2024/03/15/%d.parquet", size)
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i * 7)
		}
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(data)
			_ = pw.Close()
		}()
		if err := b.WriteReader(ctx, key, pr, int64(size)); err != nil {
			t.Fatalf("WriteReader(%d bytes via io.Pipe): %v", size, err)
		}
		got, err := b.Read(ctx, key)
		if err != nil {
			t.Fatalf("Read(%s): %v", key, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("read back %d bytes for a %d-byte upload, or the content differs", len(got), size)
		}
		_ = b.Delete(ctx, key)
	}
}

func minioBackend(t *testing.T) *S3Backend {
	t.Helper()
	bucket := os.Getenv("ARC_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("ARC_TEST_S3_BUCKET not set")
	}
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	b, err := NewS3Backend(&S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("S3 backend: %v", err)
	}
	return b
}

func TestS3MissingBucketListsEmptyThenListsFirstWrite(t *testing.T) {
	base := minioBackend(t)
	b := &S3Backend{
		client: base.client,
		bucket: fmt.Sprintf("arc-missing-%d", time.Now().UnixNano()),
		logger: zerolog.Nop(),
	}
	ctx := context.Background()

	objects, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List on a fresh bucket: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("List on a fresh bucket returned %v, want empty", objects)
	}

	const key = "contract/first-write.parquet"
	if err := b.Write(ctx, key, []byte("PAR1")); err != nil {
		t.Fatalf("first write to fresh bucket: %v", err)
	}
	t.Cleanup(func() { _ = b.Delete(ctx, key) })
	objects, err = b.List(ctx, "")
	if err != nil {
		t.Fatalf("List after first write: %v", err)
	}
	if len(objects) != 1 || objects[0] != key {
		t.Fatalf("List after first write = %v, want [%s]", objects, key)
	}
}

// TestS3RejectsNonInjectiveKeys pins the contract at the backend that has the
// most to lose from it. Each key here either names one object under two
// spellings, or is refused by the store itself with a remote 400 that says
// nothing useful about which key was wrong.
func TestS3RejectsNonInjectiveKeys(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()

	rejected := []struct{ name, key string }{
		// MinIO strips this, so it is the same object as "coll/x.parquet".
		{"leading separator", "/coll/x.parquet"},
		// Refused by the store with XMinioInvalidResourceName.
		{"parent segment", "db/../etc/x.parquet"},
		{"current segment", "db/./cpu/x.parquet"},
		// Refused with XMinioInvalidObjectName.
		{"empty interior segment", "db//cpu/x.parquet"},
		// A trailing separator names a directory marker, not this object.
		{"trailing separator", "db/cpu/"},
		{"empty", ""},
		// Reserved for local write staging (#744). Refused on every backend so
		// a key stays portable, which does mean a pre-existing ".part" object
		// on S3 or Azure is no longer addressable through Arc. Those backends
		// never stage, so Arc cannot have written one.
		{"reserved staging suffix", "contract/x.parquet.part"},
	}
	for _, tt := range rejected {
		t.Run("reject/"+tt.name, func(t *testing.T) {
			err := b.Write(ctx, tt.key, []byte("payload"))
			if err == nil {
				t.Fatalf("Write(%q) was accepted", tt.key)
			}
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("Write(%q) failed remotely rather than being refused locally: %v", tt.key, err)
			}
		})
	}

	accepted := []struct{ name, key string }{
		{"ordinary", "contract/ok/x.parquet"},
		// Dots inside a segment are an ordinary name on every backend.
		{"dots in a segment", "contract/a..b/x.parquet"},
		{"leading dots in a segment", "contract/..foo/x.parquet"},
	}
	for _, tt := range accepted {
		t.Run("accept/"+tt.name, func(t *testing.T) {
			want := []byte("payload-" + tt.key)
			if err := b.Write(ctx, tt.key, want); err != nil {
				t.Fatalf("Write(%q) = %v, want accepted", tt.key, err)
			}
			got, err := b.Read(ctx, tt.key)
			if err != nil {
				t.Fatalf("Read(%q) = %v", tt.key, err)
			}
			if string(got) != string(want) {
				t.Errorf("Read(%q) = %q, want %q", tt.key, got, want)
			}
		})
	}
}

// TestS3ListPrefixesStillWork guards the other half: the spellings a prefix
// needs are exactly the ones a key may not have, so tightening keys must not
// break enumeration.
func TestS3ListPrefixesStillWork(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()

	if err := b.Write(ctx, "prefixtest/db/cpu/x.parquet", []byte("p")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	for _, prefix := range []string{"", "prefixtest/", "prefixtest/db/", "prefixtest/db"} {
		objs, err := b.List(ctx, prefix)
		if err != nil {
			t.Errorf("List(%q) = %v", prefix, err)
			continue
		}
		if len(objs) == 0 {
			t.Errorf("List(%q) returned nothing; the seeded object should match", prefix)
		}
	}
	if _, err := b.List(ctx, "/"); err == nil {
		t.Error(`List("/") was accepted; use "" for everything`)
	}
}

// TestS3KeysAreInjective is the property, asserted through a real store: two
// distinct accepted keys must never name one object.
func TestS3KeysAreInjective(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()

	keys := []string{
		"inj/a..b/x.parquet", "inj/a_b/x.parquet",
		"inj/..foo/x.parquet", "inj/_foo/x.parquet",
	}
	for _, k := range keys {
		if err := b.Write(ctx, k, []byte("payload-"+k)); err != nil {
			t.Fatalf("Write(%q) = %v", k, err)
		}
	}
	// Read back only after every write, so a collision shows as an earlier
	// payload having been replaced.
	for _, k := range keys {
		got, err := b.Read(ctx, k)
		if err != nil {
			t.Fatalf("Read(%q) = %v", k, err)
		}
		if want := "payload-" + k; string(got) != want {
			t.Errorf("key %q reads back %q; another key names the same object", k, got)
		}
	}
}

// TestS3ListNeverReturnsUnusableKeys is the invariant that keeps the contract
// from breaking its own consumers.
//
// Object stores carry "directory marker" objects whose key ends in a
// separator, created by consoles and sync tools rather than by Arc. Arc feeds
// List output straight into Read, Exists and Delete in a dozen places, so a
// listing that returns a key the backend then refuses is worse than one that
// never returned it: restore drops the file and still reports success,
// compaction's "already gone" skip turns into a hard job failure, and manifest
// recovery retries a permanent error forever.
//
// Creating a marker needs the raw API, because the SDK helpers will not build
// a key ending in "/".
func TestS3ListNeverReturnsUnusableKeys(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()

	if err := b.Write(ctx, "markers/real.parquet", []byte("data")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if err := putRawKey(t, b, "markers/dir/"); err != nil {
		t.Skipf("could not create a directory marker: %v", err)
	}

	keys, err := b.List(ctx, "markers/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var sawReal bool
	for _, k := range keys {
		if err := ValidateKey(k); err != nil {
			t.Errorf("List returned %q, which this backend refuses: %v", k, err)
		}
		// The real test of the invariant: everything listed must be readable.
		if _, err := b.Read(ctx, k); err != nil {
			t.Errorf("List returned %q but Read failed: %v", k, err)
		}
		if k == "markers/real.parquet" {
			sawReal = true
		}
	}
	if !sawReal {
		t.Error("filtering removed a legitimate object")
	}

	objs, err := b.ListObjects(ctx, "markers/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	for _, o := range objs {
		if err := ValidateKey(o.Path); err != nil {
			t.Errorf("ListObjects returned %q, which this backend refuses: %v", o.Path, err)
		}
	}
}

// TestS3HasObjectsUnderPrefix pins the PrefixProber contract (#1084) against a
// real store: a prefix with an object is true, an empty one is false, and a
// prefix whose only key is one ListObjects hides (a directory marker) is
// false, because the probe applies the listing's visibility rule rather than
// answering from the raw key count.
func TestS3HasObjectsUnderPrefix(t *testing.T) {
	b := minioBackend(t)
	ctx := context.Background()

	if err := b.Write(ctx, "probe/real/2026/01/01/00/x.parquet", []byte("data")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	t.Cleanup(func() { _ = b.Delete(ctx, "probe/real/2026/01/01/00/x.parquet") })

	for prefix, want := range map[string]bool{
		"probe/real/": true,
		"probe/rea":   true, // a key prefix on S3, as ListObjects treats it
		"probe/none/": false,
		"probe/":      true,
	} {
		got, err := b.HasObjectsUnderPrefix(ctx, prefix)
		if err != nil {
			t.Errorf("HasObjectsUnderPrefix(%q): %v", prefix, err)
			continue
		}
		if got != want {
			t.Errorf("HasObjectsUnderPrefix(%q) = %v, want %v", prefix, got, want)
		}
	}
	if _, err := b.HasObjectsUnderPrefix(ctx, "/"); err == nil {
		t.Error(`HasObjectsUnderPrefix("/") accepted an invalid prefix`)
	}

	if err := putRawKey(t, b, "probe/marker/dir/"); err != nil {
		t.Skipf("could not create a directory marker: %v", err)
	}
	got, err := b.HasObjectsUnderPrefix(ctx, "probe/marker/")
	if err != nil {
		t.Fatalf("HasObjectsUnderPrefix(probe/marker/): %v", err)
	}
	if got {
		t.Error("a prefix holding only a directory marker answered true; ListObjects hides that key, so the probe must too")
	}
}

// putRawKey writes an object under a key the backend itself would refuse,
// using a signed request rather than the backend, so the test can create the
// state a foreign tool would leave behind.
func putRawKey(t *testing.T, b *S3Backend, key string) error {
	t.Helper()
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	body := []byte{}
	payload := sha256.Sum256(body)
	payloadHex := hex.EncodeToString(payload[:])
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	canonicalURI := "/" + b.bucket + "/" + key
	canonicalReq := strings.Join([]string{
		"PUT", canonicalURI, "",
		"host:" + u.Host,
		"x-amz-content-sha256:" + payloadHex,
		"x-amz-date:" + amzDate,
		"", "host;x-amz-content-sha256;x-amz-date", payloadHex,
	}, "\n")
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	crHash := sha256.Sum256([]byte(canonicalReq))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(crHash[:])
	mac := func(k []byte, d string) []byte { h := hmac.New(sha256.New, k); h.Write([]byte(d)); return h.Sum(nil) }
	signingKey := mac(mac(mac(mac([]byte("AWS4minioadmin"), dateStamp), "us-east-1"), "s3"), "aws4_request")
	sig := hex.EncodeToString(mac(signingKey, toSign))

	req, err := http.NewRequest("PUT", endpoint+canonicalURI, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHex)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=minioadmin/"+scope+
			", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+sig)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("marker PUT returned %d", resp.StatusCode)
	}
	return nil
}
