//go:build fips

package storage

import (
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

// TestNewGCSBackendRefusesInTheFIPSBuild: the factory backstop for #1587.
func TestNewGCSBackendRefusesInTheFIPSBuild(t *testing.T) {
	b, err := NewBackend(BackendSpec{Type: "gcs", GCS: GCSConfig{Bucket: "b"}}, zerolog.Nop())
	if b != nil || !errors.Is(err, ErrGCSNotInFIPSBuild) {
		t.Fatalf("NewBackend(gcs) = %v, %v; want nil, ErrGCSNotInFIPSBuild", b, err)
	}
}
