package storage

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// headWithoutLength answers every request 200 with the given headers and no
// Content-Length, written raw: net/http would add one itself.
func headWithoutLength(t *testing.T, headers map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("no hijacker")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		writeRawHead(buf, headers)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeRawHead(buf *bufio.ReadWriter, headers map[string]string) {
	_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nConnection: close\r\n")
	for k, v := range headers {
		_, _ = buf.WriteString(k + ": " + v + "\r\n")
	}
	_, _ = buf.WriteString("\r\n")
	_ = buf.Flush()
}

func s3BackendAt(t *testing.T, url string) *S3Backend {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("key", "secret", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return &S3Backend{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(url)
			o.UsePathStyle = true
			o.RetryMaxAttempts = 1
		}),
		bucket: "b",
		logger: zerolog.Nop(),
	}
}

// A HEAD response with no length is an error, not size 0 (#1812): callers
// comparing sizes (compaction recovery, the object finisher) read 0 as the
// wrong object.
func TestS3StatFileWithoutContentLength(t *testing.T) {
	srv := headWithoutLength(t, map[string]string{"ETag": `"abc"`, "Last-Modified": "Mon, 02 Jan 2006 15:04:05 GMT"})
	b := s3BackendAt(t, srv.URL)
	size, err := b.StatFile(context.Background(), "db/m/f.parquet")
	if !errors.Is(err, ErrNoContentLength) {
		t.Fatalf("StatFile = %d, %v; want ErrNoContentLength", size, err)
	}
	if _, _, err := b.ObjectSHA(context.Background(), "db/m/f.parquet"); !errors.Is(err, ErrNoContentLength) {
		t.Fatalf("ObjectSHA error = %v; want ErrNoContentLength", err)
	}
}

// The control: the same response with a length reports it, 0 included.
func TestS3StatFileWithContentLength(t *testing.T) {
	for _, n := range []string{"0", "42"} {
		srv := headWithoutLength(t, map[string]string{"Content-Length": n, "ETag": `"abc"`})
		b := s3BackendAt(t, srv.URL)
		size, err := b.StatFile(context.Background(), "db/m/f.parquet")
		if err != nil || (n == "0" && size != 0) || (n == "42" && size != 42) {
			t.Fatalf("Content-Length %s: StatFile = %d, %v", n, size, err)
		}
	}
}

func azureBackendAt(t *testing.T, url string) *AzureBlobBackend {
	t.Helper()
	const fakeKey = "dGhpcy1pcy1ub3QtYS1yZWFsLWtleQ==" // never a real credential
	b, err := NewAzureBlobBackend(&AzureBlobConfig{
		ContainerName: "cont",
		AccountName:   "arctestaccount",
		AccountKey:    fakeKey,
		Endpoint:      url,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestAzureStatFileWithoutContentLength(t *testing.T) {
	srv := headWithoutLength(t, map[string]string{"ETag": `"abc"`, "x-ms-blob-type": "BlockBlob"})
	b := azureBackendAt(t, srv.URL)
	size, err := b.StatFile(context.Background(), "db/m/f.parquet")
	if !errors.Is(err, ErrNoContentLength) {
		t.Fatalf("StatFile = %d, %v; want ErrNoContentLength", size, err)
	}
	if _, _, err := b.ObjectSHA(context.Background(), "db/m/f.parquet"); !errors.Is(err, ErrNoContentLength) {
		t.Fatalf("ObjectSHA error = %v; want ErrNoContentLength", err)
	}
}
