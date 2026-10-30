package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

func TestGCSS3SignerPreservesEncoding(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "https://storage.googleapis.com/example/escaped%20name", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("X-Amz-Meta-Test", "signed")
	signer := gcsS3Signer{signer: v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })}
	err = signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test-secret"}, request, "UNSIGNED-PAYLOAD", "s3", "auto", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if request.Header.Get("Accept-Encoding") != "identity" {
		t.Fatal("wire encoding changed")
	}
	auth := request.Header.Get("Authorization")
	if strings.Contains(auth, "accept-encoding") || !strings.Contains(auth, "host;x-amz-date;x-amz-meta-test") {
		t.Fatalf("unexpected signed headers: %s", auth)
	}
}

func TestGCSS3SignerKeepsCompressedObjectBytes(t *testing.T) {
	var payload bytes.Buffer
	compressed := gzip.NewWriter(&payload)
	_, _ = compressed.Write([]byte("stored compressed object"))
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			return
		}
		if r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("missing identity encoding")
		}
		if strings.Contains(r.Header.Get("Authorization"), "accept-encoding") {
			t.Error("encoding was signed")
		}
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(payload.Bytes())
	}))
	defer server.Close()
	backend, err := NewS3Backend(&S3Config{Bucket: "test", Region: "auto", Endpoint: server.URL, PathStyle: true, AccessKey: "test", SecretKey: "test-secret"}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	options := backend.client.Options()
	options.HTTPSignerV4 = &gcsS3Signer{signer: options.HTTPSignerV4}
	options.AuthSchemes = nil
	// Retain all real SDK middleware and transport behavior.
	backend.client = s3.New(options)
	got, err := backend.Read(context.Background(), "object.gz")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload.Bytes()) {
		t.Fatal("stored object was transparently decompressed")
	}
}

func TestGCSS3OptionsEndpointScope(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		want     bool
	}{
		{"https://storage.googleapis.com", true},
		{"https://STORAGE.GOOGLEAPIS.COM:443", true},
		{"https://storage.googleapis.com.evil.example", false},
		{"https://storage.googleapis.com@other.example", false},
		{"https://s3.us-east-1.amazonaws.com", false},
		{"http://localhost:9000", false},
		{"https://[invalid", false},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			options := s3.Options{RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported}
			configureGCSS3Options(tc.endpoint, &options)
			if got := options.HTTPSignerV4 != nil; got != tc.want {
				t.Fatalf("compatibility enabled=%v, want %v", got, tc.want)
			}
			want := aws.RequestChecksumCalculationWhenSupported
			if tc.want {
				want = aws.RequestChecksumCalculationWhenRequired
			}
			if options.RequestChecksumCalculation != want {
				t.Fatal("unexpected checksum policy")
			}
		})
	}
}

func TestNonGCSS3UploaderRetainsChecksumDefault(t *testing.T) {
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_REQUIRED")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	backend, err := NewS3Backend(&S3Config{Bucket: "test", Region: "us-east-1", Endpoint: server.URL, PathStyle: true, AccessKey: "test", SecretKey: "test-secret"}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if backend.client.Options().RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		t.Fatal("test must exercise non-default client checksum policy")
	}
	if backend.uploader.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenSupported {
		t.Fatal("non-GCS multipart checksum default changed")
	}
}
