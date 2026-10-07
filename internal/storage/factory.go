package storage

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// BackendSpec is one storage destination, independent of which subsystem wants
// it. Primary storage, the tiering cold tier and the backup destination each
// describe their destination with this and hand it to NewBackend.
//
// Only the fields the named Type uses are read. The S3 and Azure blocks are the
// backends' own config structs rather than copies of them, so there stays
// exactly one definition of what an S3 or an Azure destination is.
type BackendSpec struct {
	// Type names the backend: local, s3, minio, azure or azblob. It is matched
	// case-insensitively after trimming, so a value that came through
	// config.Load (which already lowercases and trims storage.backend) and a
	// hand-built spec resolve the same way.
	Type string

	// LocalPath is the directory a local backend roots at. Read only when Type
	// is local.
	LocalPath string

	// S3 configures an s3 or minio backend. Read only for those types. Prefix
	// included: it is applied to every key the backend touches, so a caller
	// that drops it silently reroots itself at the bucket root.
	S3 S3Config

	// Azure configures an azure or azblob backend. Read only for those types.
	// Prefix included, for the same reason as S3's (#1102): it is applied to
	// every key the backend touches, so a caller that drops it silently
	// reroots itself at the container root.
	Azure AzureBlobConfig
}

// NewBackend builds the storage backend a BackendSpec names.
//
// It is deliberately narrow: it constructs, and does nothing else. It does not
// log, does not register anything for shutdown, and does not decide whether a
// failure is fatal. All three stay with the caller because all three differ per
// call site: primary storage calls log.Fatal and registers the backend for
// Close, the tiering cold tier logs an error and runs on with no cold tier, and
// the backup manager returns the error to its own caller.
//
// On any error it returns a literal nil Backend, never a typed nil pointer
// inside a non-nil interface (#713). That guarantee is the reason this returns
// an interface at all: callers keep the result in a Backend variable and then
// test it with "!= nil", and an interface holding a (*S3Backend)(nil) passes
// that test and then panics on a nil receiver at first use. Every case below
// assigns the constructor result to a concrete local and returns it only once
// the error is known to be nil.
//
// Each backend keeps its own validation: NewS3Backend requires a bucket and
// fails on an unusable prefix, NewAzureBlobBackend requires a container, a
// usable prefix (checked first, before any credential or network work) and a
// usable authentication combination, and NewLocalBackend creates its directory
// with owner-only permissions.
func NewBackend(spec BackendSpec, logger zerolog.Logger) (Backend, error) {
	switch strings.ToLower(strings.TrimSpace(spec.Type)) {
	case "local":
		b, err := NewLocalBackend(spec.LocalPath, logger)
		if err != nil {
			return nil, err
		}
		return b, nil

	case "s3", "minio":
		b, err := NewS3Backend(&spec.S3, logger)
		if err != nil {
			return nil, err
		}
		return b, nil

	case "azure", "azblob":
		b, err := NewAzureBlobBackend(&spec.Azure, logger)
		if err != nil {
			return nil, err
		}
		return b, nil

	default:
		return nil, fmt.Errorf("unsupported storage backend %q (use local, s3, minio, azure or azblob)", spec.Type)
	}
}
