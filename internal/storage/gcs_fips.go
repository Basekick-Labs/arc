//go:build fips

package storage

import (
	"context"
	"io"

	"github.com/rs/zerolog"
)

// GCSBackend in the FIPS build is a placeholder that never holds a bucket.
// The native backend (gcs.go) uses Google's API client, which pulls
// golang.org/x/crypto/hkdf through S2A, outside the Go Cryptographic Module
// the FIPS build must link alone (#1587). NewGCSBackend refuses, so no value
// of this type is ever built; it exists so the backend-neutral code that
// names *GCSBackend (type switches, the compaction subprocess) compiles
// unchanged. Every method answers ErrGCSNotInFIPSBuild rather than panicking.
type GCSBackend struct {
	bucket   string
	prefix   string
	endpoint string
	auth     GCSConfig
}

// NewGCSBackend refuses in the FIPS build; see ErrGCSNotInFIPSBuild.
func NewGCSBackend(_ *GCSConfig, _ zerolog.Logger) (*GCSBackend, error) {
	return nil, ErrGCSNotInFIPSBuild
}

func (b *GCSBackend) Write(context.Context, string, []byte) error { return ErrGCSNotInFIPSBuild }
func (b *GCSBackend) WriteReader(context.Context, string, io.Reader, int64) error {
	return ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) WriteReaderWithSHA(context.Context, string, io.Reader, int64, string) error {
	return ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) Read(context.Context, string) ([]byte, error) { return nil, ErrGCSNotInFIPSBuild }
func (b *GCSBackend) ReadTo(context.Context, string, io.Writer) error {
	return ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) ReadToAt(context.Context, string, io.Writer, int64) error {
	return ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) StatFile(context.Context, string) (int64, error) {
	return 0, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) Exists(context.Context, string) (bool, error) {
	return false, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) Delete(context.Context, string) error        { return ErrGCSNotInFIPSBuild }
func (b *GCSBackend) DeleteBatch(context.Context, []string) error { return ErrGCSNotInFIPSBuild }
func (b *GCSBackend) List(context.Context, string) ([]string, error) {
	return nil, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) ListObjects(context.Context, string) ([]ObjectInfo, error) {
	return nil, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) HasObjectsUnderPrefix(context.Context, string) (bool, error) {
	return false, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) ListUnusable(context.Context, string) ([]UnusableObject, error) {
	return nil, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) ListDirectories(context.Context, string) ([]string, error) {
	return nil, ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) ObjectSHA(context.Context, string) (int64, string, error) {
	return 0, "", ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) CopyObject(context.Context, string, string, int64, string) error {
	return ErrGCSNotInFIPSBuild
}
func (b *GCSBackend) Close() error           { return nil }
func (b *GCSBackend) GetBucket() string      { return b.bucket }
func (b *GCSBackend) GetPrefix() string      { return b.prefix }
func (b *GCSBackend) GetEndpoint() string    { return b.endpoint }
func (b *GCSBackend) Credentials() GCSConfig { return b.auth }
func (b *GCSBackend) Type() string           { return "gcs" }
func (b *GCSBackend) ConfigJSON() string     { return "{}" }
