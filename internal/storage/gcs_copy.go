package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"

	gcs "google.golang.org/api/storage/v1"
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
	var obj *gcs.Object
	err = gcsRetry(ctx, func() (err error) {
		obj, err = b.svc.Objects.Get(b.bucket, key).Fields("size", "metadata").Context(ctx).Do()
		return err
	})
	if err != nil {
		if gcsHTTPCode(err) == http.StatusNotFound {
			if storeErr := b.confirmObjectAbsent(ctx); storeErr != nil {
				return -1, "", fmt.Errorf("stat %s: %w", path, storeErr)
			}
			return -1, "", nil
		}
		return -1, "", fmt.Errorf("stat %s: %w", path, err)
	}
	return int64(obj.Size), objectSHA(obj.Metadata), nil
}

// CopyObject copies src to dst inside the bucket with the rewrite API,
// repeating the call with its rewrite token until GCS reports it done, so an
// object of any size copies server side. dst appears only once the copy is
// complete; an existing dst is replaced.
//
// The destination resource names its metadata and content type: a rewrite
// given a resource takes the destination's metadata from it, not from src, so
// dst records exactly sha.
func (b *GCSBackend) CopyObject(ctx context.Context, src, dst string, size int64, sha string) error {
	srcKey, err := b.prefixedKey(src)
	if err != nil {
		return err
	}
	dstKey, err := b.prefixedKey(dst)
	if err != nil {
		return err
	}
	call := b.svc.Objects.Rewrite(b.bucket, srcKey, b.bucket, dstKey,
		&gcs.Object{ContentType: gcsContentType(dst), Metadata: shaMetadata(sha)}).
		Fields("done", "rewriteToken")
	for {
		var resp *gcs.RewriteResponse
		err := gcsRetry(ctx, func() (err error) {
			resp, err = call.Context(ctx).Do()
			return err
		})
		if err != nil {
			recordStorageError(ctx, err)
			return b.objectError(ctx, fmt.Sprintf("copy %s -> %s", src, dst), err)
		}
		if resp.Done {
			return nil
		}
		call = call.RewriteToken(resp.RewriteToken)
	}
}
