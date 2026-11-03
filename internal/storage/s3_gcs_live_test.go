//go:build objectstore

package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestS3GCSInterop uses a dedicated prefix in a real GCS bucket. Supply
// GCS_BUCKET, GCS_HMAC_ACCESS_ID and GCS_HMAC_SECRET to run it.
func TestS3GCSInterop(t *testing.T) {
	bucket, access, secret := os.Getenv("GCS_BUCKET"), os.Getenv("GCS_HMAC_ACCESS_ID"), os.Getenv("GCS_HMAC_SECRET")
	if bucket == "" || access == "" || secret == "" {
		t.Skip("requires GCS HMAC credentials and bucket")
	}
	for _, region := range []string{"auto", "us-east-1"} {
		t.Run(region, func(t *testing.T) {
			backend, err := NewS3Backend(&S3Config{Bucket: bucket, AccessKey: access, SecretKey: secret, Region: region, Endpoint: "storage.googleapis.com", UseSSL: true, PathStyle: true, Prefix: fmt.Sprintf("arc-verification/1399/%d/", time.Now().UnixNano())}, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			for _, multipart := range []bool{false, true} {
				if multipart && region != "auto" {
					continue
				}
				name := "nested/space percent% quote' unicode-ñ.parquet"
				body := []byte("GCS interoperability regression")
				if multipart {
					name = "nested/multipart.bin"
					body = bytes.Repeat([]byte("gcs-stream-test!"), 1200000)
				}
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
					defer cancel()
					if multipart {
						err = backend.WriteReader(ctx, name, bytes.NewBuffer(body), int64(len(body)))
					} else {
						err = backend.Write(ctx, name, body)
					}
					if err != nil {
						t.Fatal(err)
					}
					deleted := false
					defer func() {
						if !deleted {
							cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
							defer cancel()
							if err := backend.Delete(cleanup, name); err != nil {
								t.Error(err)
							}
						}
					}()
					digest := sha256.New()
					if err := backend.ReadTo(ctx, name, digest); err != nil {
						t.Fatal(err)
					}
					want := sha256.Sum256(body)
					if !bytes.Equal(digest.Sum(nil), want[:]) {
						t.Fatal("downloaded object differs from input")
					}
					files, err := backend.List(ctx, "")
					if err != nil || len(files) != 1 {
						t.Fatalf("List: count=%d err=%v", len(files), err)
					}
					objects, err := backend.ListObjects(ctx, "")
					if err != nil || len(objects) != 1 {
						t.Fatalf("ListObjects: count=%d err=%v", len(objects), err)
					}
					dirs, err := backend.ListDirectories(ctx, "")
					if err != nil || len(dirs) != 1 {
						t.Fatalf("ListDirectories: count=%d err=%v", len(dirs), err)
					}
					found, err := backend.HasObjectsUnderPrefix(ctx, "")
					if err != nil || !found {
						t.Fatalf("prefix probe: found=%v err=%v", found, err)
					}
					if _, err := backend.ListUnusable(ctx, ""); err != nil {
						t.Fatal(err)
					}
					if err := backend.Delete(ctx, name); err != nil {
						t.Fatal(err)
					}
					deleted = true
					found, err = backend.HasObjectsUnderPrefix(ctx, "")
					if err != nil || found {
						t.Fatalf("cleanup: found=%v err=%v", found, err)
					}
				}()
			}
		})
	}
}
