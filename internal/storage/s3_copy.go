package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3MaxSingleCopy is the largest object one CopyObject copies (5 GiB; a
// variable for tests).
var s3MaxSingleCopy int64 = 5 << 30

// s3CopyPartSize is the part size of a multipart copy (a variable for tests).
var s3CopyPartSize int64 = 512 << 20

// WriteReaderWithSHA is WriteReader recording sha as object metadata.
func (b *S3Backend) WriteReaderWithSHA(ctx context.Context, path string, reader io.Reader, size int64, sha string) error {
	return b.writeReader(ctx, path, reader, size, shaMetadata(sha))
}

// ObjectSHA returns the size and recorded sha of the object at path; size -1
// when there is none.
func (b *S3Backend) ObjectSHA(ctx context.Context, path string) (int64, string, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return 0, "", err
	}
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFoundError(err) {
			if storeErr := b.confirmObjectAbsent(ctx); storeErr != nil {
				return -1, "", fmt.Errorf("HeadObject %s: %w", path, storeErr)
			}
			return -1, "", nil
		}
		return -1, "", fmt.Errorf("HeadObject %s: %w", path, err)
	}
	return aws.ToInt64(out.ContentLength), objectSHA(out.Metadata), nil
}

// CopyObject copies src to dst inside the bucket: one CopyObject up to 5 GiB,
// a multipart copy (visible only once completed) above.
func (b *S3Backend) CopyObject(ctx context.Context, src, dst string, size int64, sha string) error {
	srcKey, err := b.prefixedKey(src)
	if err != nil {
		return err
	}
	dstKey, err := b.prefixedKey(dst)
	if err != nil {
		return err
	}
	source := url.PathEscape(b.bucket) + "/" + escapeKey(srcKey)
	contentType := "application/octet-stream"
	if strings.HasSuffix(dst, ".parquet") {
		contentType = "application/vnd.apache.parquet"
	}
	if size <= s3MaxSingleCopy {
		_, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(b.bucket), Key: aws.String(dstKey), CopySource: aws.String(source),
			MetadataDirective: types.MetadataDirectiveReplace, Metadata: shaMetadata(sha), ContentType: aws.String(contentType),
		})
		if err != nil {
			recordStorageError(ctx, err)
			return fmt.Errorf("CopyObject %s -> %s: %w", src, dst, err)
		}
		return nil
	}
	created, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.bucket), Key: aws.String(dstKey), Metadata: shaMetadata(sha), ContentType: aws.String(contentType),
	})
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("CreateMultipartUpload %s: %w", dst, err)
	}
	abort := func(cause error) error {
		_, _ = b.client.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{
			Bucket: aws.String(b.bucket), Key: aws.String(dstKey), UploadId: created.UploadId})
		recordStorageError(ctx, cause)
		return fmt.Errorf("multipart copy %s -> %s: %w", src, dst, cause)
	}
	var parts []types.CompletedPart
	for n, off := int32(1), int64(0); off < size; n, off = n+1, off+s3CopyPartSize {
		end := min(off+s3CopyPartSize, size) - 1
		out, err := b.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: aws.String(b.bucket), Key: aws.String(dstKey), UploadId: created.UploadId, PartNumber: aws.Int32(n),
			CopySource: aws.String(source), CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
		})
		if err != nil {
			return abort(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.CopyPartResult.ETag, PartNumber: aws.Int32(n)})
	}
	if _, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b.bucket), Key: aws.String(dstKey), UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return abort(err)
	}
	return nil
}

// escapeKey URL-escapes each segment of an object key for x-amz-copy-source
// ("+" too: some stores decode it as a space there).
func escapeKey(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = strings.ReplaceAll(url.PathEscape(s), "+", "%2B")
	}
	return strings.Join(segs, "/")
}
