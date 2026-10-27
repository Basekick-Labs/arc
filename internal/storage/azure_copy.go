package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/basekick-labs/arc/internal/metrics"
)

// azureMaxSingleCopy is the largest blob one Put Blob From URL copies
// (5000 MiB; a variable for tests).
var azureMaxSingleCopy int64 = 5000 << 20

// azureCopyBlockSize is the block size of a copy in blocks (a variable for
// tests).
var azureCopyBlockSize int64 = 256 << 20

// azureMetadata converts metadata to the SDK's form (nil stays nil).
func azureMetadata(meta map[string]string) map[string]*string {
	if meta == nil {
		return nil
	}
	out := make(map[string]*string, len(meta))
	for k, v := range meta {
		out[k] = &v
	}
	return out
}

// WriteReaderWithSHA is WriteReader recording sha as blob metadata.
func (b *AzureBlobBackend) WriteReaderWithSHA(ctx context.Context, path string, reader io.Reader, size int64, sha string) error {
	return b.writeReader(ctx, path, reader, size, shaMetadata(sha))
}

// ObjectSHA returns the size and recorded sha of the blob at path; size -1
// when there is none.
func (b *AzureBlobBackend) ObjectSHA(ctx context.Context, path string) (int64, string, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return 0, "", err
	}
	resp, err := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		if isAzureNotFoundError(err) {
			return -1, "", nil
		}
		return -1, "", fmt.Errorf("GetProperties %s: %w", path, err)
	}
	meta := make(map[string]string, len(resp.Metadata))
	for k, v := range resp.Metadata {
		if v != nil {
			meta[k] = *v
		}
	}
	var size int64
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	return size, objectSHA(meta), nil
}

// CopyObject copies src to dst inside the container, synchronously: one Put
// Blob From URL up to 5000 MiB, blocks from URL then one commit above, so dst
// appears only once complete. The source is authorised by a SAS made from
// the account key, the Entra token of managed-identity auth, or the
// configured SAS. A store that refuses the server-side copy gets the bytes
// through this process instead.
func (b *AzureBlobBackend) CopyObject(ctx context.Context, src, dst string, size int64, sha string) error {
	srcKey, err := b.prefixedKey(src)
	if err != nil {
		return err
	}
	dstKey, err := b.prefixedKey(dst)
	if err != nil {
		return err
	}
	cc := b.client.ServiceClient().NewContainerClient(b.containerName)
	err = b.copyFromURL(ctx, cc.NewBlockBlobClient(dstKey), cc.NewBlobClient(srcKey), size, sha, dst)
	if err == nil {
		return nil
	}
	if !copyRefused(err) {
		recordStorageError(ctx, err)
		return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
	}
	b.logger.Warn().Err(err).Str("src", src).Str("dst", dst).
		Msg("Azure refused a server-side copy; copying through this node")
	metrics.Get().IncCompactionCopyFallback()
	return streamCopy(ctx, b, func(r io.Reader) error { return b.writeReader(ctx, dst, r, size, shaMetadata(sha)) }, src)
}

// copySource is the URL a copy reads src from, and the bearer token that
// authorises it ("" when the URL carries its own authorisation), valid for
// at least valid.
func (b *AzureBlobBackend) copySource(ctx context.Context, src *blob.Client, valid time.Duration) (string, string, error) {
	if u, err := src.GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(valid), nil); err == nil {
		return u, "", nil
	}
	if b.tokenCred != nil {
		tok, err := b.tokenCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://storage.azure.com/.default"}})
		if err != nil {
			return "", "", fmt.Errorf("token for the copy source: %w", err)
		}
		return src.URL(), "Bearer " + tok.Token, nil
	}
	// SAS-token auth: the client's URLs carry the SAS.
	return src.URL(), "", nil
}

// copySourceValid is how long a copy source's authorisation lasts: an hour,
// plus a second per MiB to read (a token cannot be stretched, so the blocks
// path asks for one per block).
func copySourceValid(size int64) time.Duration {
	return time.Hour + time.Duration(size>>20)*time.Second
}

func (b *AzureBlobBackend) copyFromURL(ctx context.Context, dst *blockblob.Client, src *blob.Client, size int64, sha, path string) error {
	contentType := "application/octet-stream"
	if strings.HasSuffix(path, ".parquet") {
		contentType = "application/vnd.apache.parquet"
	}
	headers := &blob.HTTPHeaders{BlobContentType: &contentType}
	auth := func(bearer string) *string {
		if bearer == "" {
			return nil
		}
		return &bearer
	}
	meta := azureMetadata(shaMetadata(sha))
	if size <= azureMaxSingleCopy {
		source, bearer, err := b.copySource(ctx, src, copySourceValid(size))
		if err != nil {
			return err
		}
		_, err = dst.UploadBlobFromURL(ctx, source, &blockblob.UploadBlobFromURLOptions{
			CopySourceAuthorization: auth(bearer), Metadata: meta, HTTPHeaders: headers,
		})
		return err
	}
	var ids []string
	for n, off := 0, int64(0); off < size; n, off = n+1, off+azureCopyBlockSize {
		count := min(azureCopyBlockSize, size-off)
		source, bearer, err := b.copySource(ctx, src, copySourceValid(count))
		if err != nil {
			return err
		}
		id := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("arc-copy-%08d", n)))
		if _, err := dst.StageBlockFromURL(ctx, id, source, &blockblob.StageBlockFromURLOptions{
			CopySourceAuthorization: auth(bearer), Range: blob.HTTPRange{Offset: off, Count: count},
		}); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	_, err := dst.CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{Metadata: meta, HTTPHeaders: headers})
	return err
}

// copyRefused reports an authorisation refusal of a copy's source.
func copyRefused(err error) bool {
	var re *azcore.ResponseError
	if !errors.As(err, &re) {
		return false
	}
	return re.StatusCode == http.StatusForbidden || re.StatusCode == http.StatusUnauthorized
}
