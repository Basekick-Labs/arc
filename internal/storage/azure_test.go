package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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
