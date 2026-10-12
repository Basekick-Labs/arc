package storage

// #1238: whether a key names no object is read from the SDKs' typed answers,
// never from an error's text. These drive the real S3 and Azure clients
// against a local server, so the error shapes are the SDKs' own.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// A key whose text holds both strings the old classifier matched. Arc's file
// names carry UnixNano stamps, which contain "404" often.
const textTrapKey = "NotFoundEvents/m/2026/10/12/14/x_1760404040404.parquet"

func s3BackendFor(t *testing.T, h http.HandlerFunc) *S3Backend {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	// A literal, not LoadDefaultConfig: nothing from this machine's AWS
	// environment or ~/.aws may reach the client.
	cfg := aws.Config{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("key", "secret", ""),
		RetryMaxAttempts: 1,
	}
	return &S3Backend{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(server.URL)
			o.UsePathStyle = true
		}),
		bucket: "arc404",
		logger: zerolog.Nop(),
	}
}

// s3Answer answers every request with status, an x-amz-request-id holding
// "404", and an S3 XML error body when code is set (HEAD gets no body).
func s3Answer(status int, code string, contentType string, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-amz-request-id", "REQ404NotFound404")
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		if r.Method == http.MethodHead {
			return
		}
		if code != "" {
			_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>m</Message></Error>", code)
			return
		}
		_, _ = w.Write([]byte(body))
	}
}

// hangUp closes every connection without answering: a transport failure
// whose text carries the request URL, and so the trap key.
func hangUp(w http.ResponseWriter, _ *http.Request) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func TestS3ClassifiesAMissingObjectByItsTypeNotItsText(t *testing.T) {
	ctx := context.Background()
	type outcome int
	const (
		absent    outcome = iota // IsNotFound; StatFile -1; Exists false; no error
		storeGone                // an error that IsStoreNotFound
		failure                  // an error that is neither
	)
	for _, tt := range []struct {
		name    string
		handler http.HandlerFunc
		read    outcome // Read, ReadTo, ReadToAt
		stat    outcome // StatFile, Exists (HEAD)
	}{
		{"missing key", s3Answer(404, "NoSuchKey", "application/xml", ""), absent, absent},
		{"missing bucket", s3Answer(404, "NoSuchBucket", "application/xml", ""), storeGone, absent}, // a HEAD cannot tell
		{"proxy 404 page", s3Answer(404, "", "text/html", "<html>404 Not Found</html>"), failure, absent},
		{"slow down whose request id holds 404", s3Answer(503, "SlowDown", "application/xml", ""), failure, failure},
		{"connection dropped", hangUp, failure, failure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := s3BackendFor(t, tt.handler)
			check := func(op string, err error, want outcome) {
				t.Helper()
				switch want {
				case absent:
					if !IsNotFound(err) {
						t.Errorf("%s: err = %v, want a missing object", op, err)
					}
				case storeGone:
					if err == nil || IsNotFound(err) || !IsStoreNotFound(err) {
						t.Errorf("%s: err = %v, want ErrStoreNotFound and not a missing object", op, err)
					}
				case failure:
					if err == nil || IsNotFound(err) || IsStoreNotFound(err) {
						t.Errorf("%s: err = %v, want a plain failure", op, err)
					}
				}
			}
			_, err := b.Read(ctx, textTrapKey)
			check("Read", err, tt.read)
			check("ReadTo", b.ReadTo(ctx, textTrapKey, &bytes.Buffer{}), tt.read)
			check("ReadToAt", b.ReadToAt(ctx, textTrapKey, &bytes.Buffer{}, 4), tt.read)

			size, err := b.StatFile(ctx, textTrapKey)
			exists, existsErr := b.Exists(ctx, textTrapKey)
			if tt.stat == absent {
				if size != -1 || err != nil || exists || existsErr != nil {
					t.Errorf("StatFile = %d, %v; Exists = %v, %v; want -1, nil and false, nil", size, err, exists, existsErr)
				}
			} else if err == nil || existsErr == nil {
				t.Errorf("StatFile = %d, %v; Exists = %v, %v; want errors, not an absent object", size, err, exists, existsErr)
			}
		})
	}
}

func azureBackendFor(t *testing.T, h http.HandlerFunc) *AzureBlobBackend {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	client, err := azblob.NewClientWithNoCredential(server.URL, &azblob.ClientOptions{
		ClientOptions: policy.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	if err != nil {
		t.Fatalf("create Azure client: %v", err)
	}
	return &AzureBlobBackend{client: client, containerName: "arc404", logger: zerolog.Nop()}
}

func azureAnswer(status int, code string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-request-id", "REQ404NotFound404")
		if code != "" {
			w.Header().Set("x-ms-error-code", code)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		if r.Method != http.MethodHead && code != "" {
			_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>m</Message></Error>", code)
		}
	}
}

func TestAzureClassifiesAMissingBlobByItsTypeNotItsText(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name      string
		handler   http.HandlerFunc
		missing   bool // reads are a missing object; StatFile -1, Exists false
		storeGone bool // reads are ErrStoreNotFound; StatFile and Exists fail
		gone      bool // Delete succeeds
	}{
		{"missing blob", azureAnswer(404, "BlobNotFound"), true, false, true},
		{"missing container", azureAnswer(404, "ContainerNotFound"), false, true, true},
		{"404 with no Azure code", azureAnswer(404, ""), false, false, false},
		{"server busy", azureAnswer(503, "ServerBusy"), false, false, false},
		{"connection dropped", hangUp, false, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := azureBackendFor(t, tt.handler)
			_, readErr := b.Read(ctx, textTrapKey)
			reads := map[string]error{
				"Read":     readErr,
				"ReadTo":   b.ReadTo(ctx, textTrapKey, &bytes.Buffer{}),
				"ReadToAt": b.ReadToAt(ctx, textTrapKey, &bytes.Buffer{}, 4),
			}
			for op, err := range reads {
				if err == nil {
					t.Fatalf("%s succeeded against a %s", op, tt.name)
				}
				if IsNotFound(err) != tt.missing || IsStoreNotFound(err) != tt.storeGone {
					t.Errorf("%s: err = %v; IsNotFound=%v IsStoreNotFound=%v, want %v and %v",
						op, err, IsNotFound(err), IsStoreNotFound(err), tt.missing, tt.storeGone)
				}
			}
			size, statErr := b.StatFile(ctx, textTrapKey)
			exists, existsErr := b.Exists(ctx, textTrapKey)
			if tt.missing {
				if size != -1 || statErr != nil || exists || existsErr != nil {
					t.Errorf("StatFile = %d, %v; Exists = %v, %v; want -1, nil and false, nil", size, statErr, exists, existsErr)
				}
			} else if statErr == nil || existsErr == nil {
				t.Errorf("StatFile = %d, %v; Exists = %v, %v; want errors, not an absent blob", size, statErr, exists, existsErr)
			}
			if err := b.Delete(ctx, textTrapKey); (err == nil) != tt.gone {
				t.Errorf("Delete: err = %v, want success=%v", err, tt.gone)
			}
		})
	}
}

// TestIsNotFoundNeverReadsText: the shapes that carried the old false
// positives, built directly.
func TestIsNotFoundNeverReadsText(t *testing.T) {
	portErr := &net.OpError{Op: "read", Net: "tcp",
		Source: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40404},
		Addr:   &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000},
		Err:    errors.New("connection reset by peer")}
	for _, err := range []error{
		fmt.Errorf("failed to read from S3: %w", portErr),
		errors.New("HTTP 404"),
		errors.New("BlobNotFound"),
		errors.New(`Get "http://127.0.0.1:40404/b/` + textTrapKey + `": dial tcp: connection refused`),
		&azcore.ResponseError{StatusCode: 404, ErrorCode: "ContainerNotFound"},
	} {
		if IsNotFound(err) {
			t.Errorf("IsNotFound(%v) = true, want false", err)
		}
	}
	if !strings.Contains(fmt.Sprint(portErr), "404") {
		t.Fatal("precondition: the port error's text must contain 404")
	}
}

// TestLocalReadsOfAMissingFileAreNotFound: ReadTo, ReadToAt and the staged
// read used to answer a bare "file not found: <path>", which IsNotFound
// recognised only when the path happened to contain "404".
func TestLocalReadsOfAMissingFileAreNotFound(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	const missing = "db/m/2026/10/12/14/x_1760000000000.parquet"
	_, readErr := b.Read(ctx, missing)
	for op, err := range map[string]error{
		"Read":       readErr,
		"ReadTo":     b.ReadTo(ctx, missing, &bytes.Buffer{}),
		"ReadToAt":   b.ReadToAt(ctx, missing, &bytes.Buffer{}, 0),
		"ReadStaged": b.ReadStaged(ctx, missing, &bytes.Buffer{}),
	} {
		if !IsNotFound(err) {
			t.Errorf("%s of a missing file: err = %v, want IsNotFound", op, err)
		}
	}
}
