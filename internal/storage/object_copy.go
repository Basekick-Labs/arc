package storage

import (
	"context"
	"io"
	"strings"
)

// ObjectSHAMetadata is the user-metadata name under which an object records
// the SHA-256 of its content (#1207 d2a2b). Letters and digits only: Azure
// metadata names must be C# identifiers, and Azure returns them with other
// casing, so they are compared without case (objectSHA).
const ObjectSHAMetadata = "arcsha256"

// ObjectCopier is an object store that can stage a compaction output and copy
// it into place inside the store (#1207 d2a2b): the S3, Azure and GCS
// backends.
type ObjectCopier interface {
	// WriteReaderWithSHA is WriteReader recording sha as the object's
	// ObjectSHAMetadata.
	WriteReaderWithSHA(ctx context.Context, path string, reader io.Reader, size int64, sha string) error
	// CopyObject copies src (size bytes, content sha) to dst inside the
	// store, recording sha on dst. dst appears only once complete; an
	// existing dst is replaced.
	CopyObject(ctx context.Context, src, dst string, size int64, sha string) error
	// ObjectSHA returns the object's size and recorded sha ("" when it has
	// none); size -1 when there is no object at path.
	ObjectSHA(ctx context.Context, path string) (size int64, sha string, err error)
}

var (
	_ ObjectCopier = (*S3Backend)(nil)
	_ ObjectCopier = (*AzureBlobBackend)(nil)
	_ ObjectCopier = (*GCSBackend)(nil)
)

// shaMetadata is the metadata map recording sha.
func shaMetadata(sha string) map[string]string {
	return map[string]string{ObjectSHAMetadata: sha}
}

// objectSHA finds ObjectSHAMetadata in meta, whatever its casing.
func objectSHA(meta map[string]string) string {
	for k, v := range meta {
		if strings.EqualFold(k, ObjectSHAMetadata) {
			return v
		}
	}
	return ""
}

// streamCopy copies src to dst through this process: the fallback when the
// store refuses a server-side copy.
func streamCopy(ctx context.Context, b interface {
	ReadTo(ctx context.Context, path string, w io.Writer) error
}, write func(r io.Reader) error, src string) error {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(b.ReadTo(ctx, src, pw))
	}()
	err := write(pr)
	_ = pr.CloseWithError(err)
	return err
}
