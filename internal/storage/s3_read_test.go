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

func TestS3ListsTreatMissingBucketAsEmpty(t *testing.T) {
	ctx := context.Background()

	t.Run("List", func(t *testing.T) {
		objects, err := s3ErrorBackend(t, "NoSuchBucket").List(ctx, "")
		if err != nil {
			t.Fatalf("List returned an error: %v", err)
		}
		if len(objects) != 0 {
			t.Fatalf("List returned %v, want empty", objects)
		}
	})

	t.Run("ListDirectories", func(t *testing.T) {
		directories, err := s3ErrorBackend(t, "NoSuchBucket").ListDirectories(ctx, "")
		if err != nil {
			t.Fatalf("ListDirectories returned an error: %v", err)
		}
		if len(directories) != 0 {
			t.Fatalf("ListDirectories returned %v, want empty", directories)
		}
	})

	t.Run("ListObjects", func(t *testing.T) {
		objects, err := s3ErrorBackend(t, "NoSuchBucket").ListObjects(ctx, "")
		if err != nil {
			t.Fatalf("ListObjects returned an error: %v", err)
		}
		if len(objects) != 0 {
			t.Fatalf("ListObjects returned %v, want empty", objects)
		}
	})

	t.Run("ListUnusable", func(t *testing.T) {
		objects, err := s3ErrorBackend(t, "NoSuchBucket").ListUnusable(ctx, "")
		if err != nil {
			t.Fatalf("ListUnusable returned an error: %v", err)
		}
		if len(objects) != 0 {
			t.Fatalf("ListUnusable returned %v, want empty", objects)
		}
	})
}

func TestS3ListsPreserveOtherErrors(t *testing.T) {
	_, err := s3ErrorBackend(t, "AccessDenied").List(context.Background(), "")
	if err == nil {
		t.Fatal("List returned nil error for AccessDenied")
	}
}
