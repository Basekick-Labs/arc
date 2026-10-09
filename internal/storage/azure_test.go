package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/rs/zerolog"
)

func TestIsAzureNotFoundError(t *testing.T) {
	respErr404 := &azcore.ResponseError{StatusCode: 404}
	respErr500 := &azcore.ResponseError{StatusCode: 500}
	otherErr := errors.New("other error")

	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{
			name:   "nil error",
			err:    nil,
			expect: false,
		},
		{
			name:   "bare ResponseError 404",
			err:    respErr404,
			expect: true,
		},
		{
			name:   "wrapped ResponseError 404",
			err:    fmt.Errorf("wrapped error: %w", respErr404),
			expect: true,
		},
		{
			name:   "joined ResponseError 404 via errors.Join",
			err:    errors.Join(otherErr, respErr404),
			expect: true,
		},
		{
			name:   "bare ResponseError 500",
			err:    respErr500,
			expect: false,
		},
		{
			name:   "string fallback BlobNotFound",
			err:    errors.New("BlobNotFound"),
			expect: true,
		},
		{
			name:   "string fallback 404",
			err:    errors.New("HTTP 404"),
			expect: true,
		},
		{
			name:   "unrelated error",
			err:    otherErr,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAzureNotFoundError(tt.err)
			if got != tt.expect {
				t.Errorf("isAzureNotFoundError() = %v, want %v", got, tt.expect)
			}
		})
	}
}

// Every listing method reports a missing container through the sentinel and
// keeps the SDK error in the chain; no other error code may be mistaken for
// it. The leniency decision belongs to the caller — see ErrStoreNotFound.
func TestAzureListingsReportMissingContainerAsErrStoreNotFound(t *testing.T) {
	operations := []struct {
		name string
		call func(*AzureBlobBackend) (bool, error)
	}{
		{
			name: "List",
			call: func(b *AzureBlobBackend) (bool, error) {
				items, err := b.List(context.Background(), "")
				return len(items) == 0, err
			},
		},
		{
			name: "ListDirectories",
			call: func(b *AzureBlobBackend) (bool, error) {
				items, err := b.ListDirectories(context.Background(), "")
				return len(items) == 0, err
			},
		},
		{
			name: "ListObjects",
			call: func(b *AzureBlobBackend) (bool, error) {
				items, err := b.ListObjects(context.Background(), "")
				return len(items) == 0, err
			},
		},
		{
			name: "ListUnusable",
			call: func(b *AzureBlobBackend) (bool, error) {
				items, err := b.ListUnusable(context.Background(), "")
				return len(items) == 0, err
			},
		},
		{
			name: "HasObjectsUnderPrefix",
			call: func(b *AzureBlobBackend) (bool, error) {
				hasObjects, err := b.HasObjectsUnderPrefix(context.Background(), "")
				return !hasObjects, err
			},
		},
	}

	for _, tt := range []struct {
		name          string
		errorCode     string
		wantStoreGone bool
	}{
		{name: "missing container", errorCode: "ContainerNotFound", wantStoreGone: true},
		{name: "other not found", errorCode: "BlobNotFound"},
		{name: "authorization failure", errorCode: "AuthorizationFailure"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.Header().Set("x-ms-error-code", tt.errorCode)
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>missing resource</Message></Error>", tt.errorCode)
			}))
			t.Cleanup(server.Close)

			client, err := azblob.NewClientWithNoCredential(server.URL, nil)
			if err != nil {
				t.Fatalf("create Azure client: %v", err)
			}
			backend := &AzureBlobBackend{
				client:        client,
				containerName: "fresh",
				logger:        zerolog.Nop(),
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					isEmpty, err := operation.call(backend)
					if err == nil {
						t.Fatalf("%s returned nil error for %s", operation.name, tt.errorCode)
					}
					if !isEmpty {
						t.Fatalf("%s returned results alongside its error", operation.name)
					}
					if tt.wantStoreGone {
						if !IsStoreNotFound(err) {
							t.Fatalf("%s: err = %v, want ErrStoreNotFound", operation.name, err)
						}
						if !isAzureContainerNotFoundError(err) {
							t.Fatalf("%s: the SDK error left the chain: %v", operation.name, err)
						}
						return
					}
					if IsStoreNotFound(err) {
						t.Fatalf("%s reported %s as a missing container: %v", operation.name, tt.errorCode, err)
					}
				})
			}
		})
	}
}

func TestIsAzureContainerNotFoundErrorDoesNotMatchTransportText(t *testing.T) {
	err := errors.New("GET https://account.example/ContainerNotFound/prefix: connection reset by peer")
	if isAzureContainerNotFoundError(err) {
		t.Fatal("isAzureContainerNotFoundError matched a transport error containing ContainerNotFound")
	}
}
