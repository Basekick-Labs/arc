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

func TestAzureListTreatsMissingContainerAsEmpty(t *testing.T) {
	for _, tt := range []struct {
		name        string
		errorCode   string
		wantEmptyOK bool
	}{
		{name: "missing container", errorCode: "ContainerNotFound", wantEmptyOK: true},
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
			objects, err := backend.List(context.Background(), "")
			if tt.wantEmptyOK {
				if err != nil {
					t.Fatalf("List returned error for missing container: %v", err)
				}
				if len(objects) != 0 {
					t.Fatalf("List returned %v, want empty", objects)
				}
				return
			}
			if err == nil {
				t.Fatalf("List returned nil error for %s", tt.errorCode)
			}
		})
	}
}
