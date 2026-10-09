package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

func s3ErrorBackend(t *testing.T, code string) *S3Backend {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>` + code + `</Code><Message>request failed</Message></Error>`))
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("key", "secret", "")),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}

	return &S3Backend{
		client: s3.NewFromConfig(cfg, func(options *s3.Options) {
			options.BaseEndpoint = aws.String(server.URL)
			options.UsePathStyle = true
		}),
		bucket: "missing",
		logger: zerolog.Nop(),
	}
}

func TestS3ListsReportMissingBucketAsErrStoreNotFound(t *testing.T) {
	ctx := context.Background()

	// Every listing method must report the missing bucket through the
	// sentinel AND keep the SDK's own error in the chain, so a caller can
	// ask either question. Deciding that a missing bucket means "empty" is
	// the caller's call, not the backend's: internal/pruning drops a whole
	// tier from a query on "verified absent", so a backend that answered
	// empty here would turn a misnamed bucket into silently short results.
	for _, tt := range []struct {
		name string
		call func(*S3Backend) (int, error)
	}{
		{"List", func(b *S3Backend) (int, error) { v, err := b.List(ctx, ""); return len(v), err }},
		{"ListDirectories", func(b *S3Backend) (int, error) { v, err := b.ListDirectories(ctx, ""); return len(v), err }},
		{"ListObjects", func(b *S3Backend) (int, error) { v, err := b.ListObjects(ctx, ""); return len(v), err }},
		{"ListUnusable", func(b *S3Backend) (int, error) { v, err := b.ListUnusable(ctx, ""); return len(v), err }},
		{"HasObjectsUnderPrefix", func(b *S3Backend) (int, error) {
			has, err := b.HasObjectsUnderPrefix(ctx, "")
			if has {
				return 1, err
			}
			return 0, err
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n, err := tt.call(s3ErrorBackend(t, "NoSuchBucket"))
			if !IsStoreNotFound(err) {
				t.Fatalf("%s: err = %v, want ErrStoreNotFound", tt.name, err)
			}
			if !isNoSuchBucketError(err) {
				t.Fatalf("%s: the SDK error left the chain: %v", tt.name, err)
			}
			if n != 0 {
				t.Fatalf("%s returned %d results alongside the error", tt.name, n)
			}
		})
	}
}

// Every listing method, not just two: an over-broad missing-store match would
// otherwise hide an authorization failure on the three that go unasserted.
func TestS3ListsPreserveOtherErrors(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name string
		call func(*S3Backend) error
	}{
		{"List", func(b *S3Backend) error { _, err := b.List(ctx, ""); return err }},
		{"ListDirectories", func(b *S3Backend) error { _, err := b.ListDirectories(ctx, ""); return err }},
		{"ListObjects", func(b *S3Backend) error { _, err := b.ListObjects(ctx, ""); return err }},
		{"ListUnusable", func(b *S3Backend) error { _, err := b.ListUnusable(ctx, ""); return err }},
		{"HasObjectsUnderPrefix", func(b *S3Backend) error { _, err := b.HasObjectsUnderPrefix(ctx, ""); return err }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(s3ErrorBackend(t, "AccessDenied"))
			if err == nil {
				t.Fatalf("%s returned nil error for AccessDenied", tt.name)
			}
			if IsStoreNotFound(err) {
				t.Fatalf("%s reported AccessDenied as a missing store: %v", tt.name, err)
			}
		})
	}
}
