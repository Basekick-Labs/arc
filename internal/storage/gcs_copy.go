package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

// WriteReaderWithSHA is WriteReader recording sha as custom object metadata.
func (b *GCSBackend) WriteReaderWithSHA(ctx context.Context, path string, reader io.Reader, size int64, sha string) error {
	return b.writeReader(ctx, path, reader, size, shaMetadata(sha))
}

// ObjectSHA returns the size and recorded sha of the object at path; size -1
// when there is none. A missing bucket is an error (ErrStoreNotFound), not an
// absent object.
func (b *GCSBackend) ObjectSHA(ctx context.Context, path string) (int64, string, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return 0, "", err
	}
	attrs, err := b.object(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			if storeErr := b.confirmObjectAbsent(ctx); storeErr != nil {
				return -1, "", fmt.Errorf("stat %s: %w", path, storeErr)
			}
			return -1, "", nil
		}
		return -1, "", fmt.Errorf("stat %s: %w", path, err)
	}
	return attrs.Size, objectSHA(attrs.Metadata), nil
}

// CopyObject copies src to dst inside the bucket with the rewrite API. The
// client's Copier.Run repeats the rewrite call until GCS reports it done, so
// an object of any size copies server side, and dst appears only once the
// copy is complete. An existing dst is replaced.
//
// Metadata is set explicitly rather than carried from src: when the copier
// names any metadata it replaces the source's, so dst records exactly sha.
// The content type is set for the same reason.
func (b *GCSBackend) CopyObject(ctx context.Context, src, dst string, size int64, sha string) error {
	srcKey, err := b.prefixedKey(src)
	if err != nil {
		return err
	}
	dstKey, err := b.prefixedKey(dst)
	if err != nil {
		return err
	}
	c := b.object(dstKey).CopierFrom(b.object(srcKey))
	c.ContentType = gcsContentType(dst)
	c.Metadata = shaMetadata(sha)
	if _, err := c.Run(ctx); err != nil {
		recordStorageError(ctx, err)
		return b.objectError(ctx, fmt.Sprintf("copy %s -> %s", src, dst), err)
	}
	return nil
}
