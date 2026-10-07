package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// AzureBlobBackend implements the Backend interface for Azure Blob Storage
type AzureBlobBackend struct {
	client        *azblob.Client
	containerName string
	prefix        string // blob-name prefix within the container (validated, with trailing /)
	accountName   string
	accountKey    string // Stored for subprocess credential passing
	endpoint      string
	logger        zerolog.Logger
}

// AzureBlobConfig holds Azure Blob Storage backend configuration
type AzureBlobConfig struct {
	// Connection string authentication (simplest)
	ConnectionString string

	// Account-based authentication
	AccountName string
	AccountKey  string

	// SAS token authentication
	SASToken string

	// Managed Identity authentication (for Azure-hosted deployments)
	UseManagedIdentity bool

	// Container name (required)
	ContainerName string

	// Prefix is a blob-name prefix within the container, applied to every key
	// the backend touches (e.g. "instances/abc123/"). Empty means the
	// container root. Mirrors S3Config.Prefix (#1102).
	Prefix string

	// Custom endpoint (for Azurite testing)
	Endpoint string
}

// NewAzureBlobBackend creates a new Azure Blob Storage backend
func NewAzureBlobBackend(cfg *AzureBlobConfig, logger zerolog.Logger) (*AzureBlobBackend, error) {
	if cfg.ContainerName == "" {
		return nil, fmt.Errorf("Azure container name is required")
	}

	// Validate the prefix rather than repairing it, exactly as the S3 backend
	// does: a prefix that cannot form usable keys must stop the backend from
	// being built, because the fallback would be the container root, which is
	// a different and much larger location rather than a safe default.
	//
	// Before the credential switch and the container probe on purpose: this is
	// a pure config check, it needs no I/O, and failing it first means a
	// misspelled prefix is reported as itself rather than behind whatever the
	// network did next.
	prefix, err := ValidateObjectPrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}

	log := logger.With().Str("component", "azure-storage").Logger()

	var client *azblob.Client
	var endpoint string

	// Try authentication methods in order of preference
	switch {
	case cfg.ConnectionString != "":
		// Connection string authentication
		client, err = azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client from connection string: %w", err)
		}
		log.Info().Msg("Using connection string authentication for Azure Blob Storage")

	case cfg.AccountName != "" && cfg.SASToken != "":
		// SAS token authentication
		if cfg.Endpoint != "" {
			endpoint = cfg.Endpoint
		} else {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AccountName)
		}
		serviceURL := fmt.Sprintf("%s?%s", endpoint, strings.TrimPrefix(cfg.SASToken, "?"))
		client, err = azblob.NewClientWithNoCredential(serviceURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client with SAS token: %w", err)
		}
		log.Info().Msg("Using SAS token authentication for Azure Blob Storage")

	case cfg.AccountName != "" && cfg.AccountKey != "":
		// Shared key authentication
		if cfg.Endpoint != "" {
			endpoint = cfg.Endpoint
		} else {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AccountName)
		}
		cred, credErr := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
		if credErr != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %w", credErr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client with shared key: %w", err)
		}
		log.Info().Msg("Using shared key authentication for Azure Blob Storage")

	case cfg.UseManagedIdentity && cfg.AccountName != "":
		// Managed Identity authentication
		if cfg.Endpoint != "" {
			endpoint = cfg.Endpoint
		} else {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AccountName)
		}
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return nil, fmt.Errorf("failed to create managed identity credential: %w", credErr)
		}
		client, err = azblob.NewClient(endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client with managed identity: %w", err)
		}
		log.Info().Msg("Using managed identity authentication for Azure Blob Storage")

	default:
		return nil, fmt.Errorf("no valid Azure authentication method configured. Provide connection_string, account_name+account_key, account_name+sas_token, or account_name+use_managed_identity")
	}

	backend := &AzureBlobBackend{
		client:        client,
		containerName: cfg.ContainerName,
		prefix:        prefix,
		accountName:   cfg.AccountName,
		accountKey:    cfg.AccountKey,
		endpoint:      endpoint,
		logger:        log,
	}

	// Test connection by checking if container exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	containerClient := client.ServiceClient().NewContainerClient(cfg.ContainerName)
	_, err = containerClient.GetProperties(ctx, nil)
	if err != nil {
		log.Warn().Err(err).Str("container", cfg.ContainerName).Msg("Could not verify container exists (may need to create it)")
	} else {
		log.Info().Str("container", cfg.ContainerName).Msg("Successfully connected to Azure Blob Storage container")
	}

	return backend, nil
}

// prefixedKey validates a storage key and prepends the configured prefix to
// build the blob name. It also enforces the key-length limit after the prefix
// is applied (#1121). The S3 twin is S3Backend.prefixedKey and the two are meant
// to stay diffable.
//
// Every key-taking method routes through this, so the contract the other
// backends enforce (#743) holds here too. One rule matters especially on
// Azure: a backslash IS a path separator there, so "a\\b" and "a/b" would be
// ONE blob. ValidateKey rejects backslash for that reason.
//
// Azure used to have no prefix at all, and the key WAS the blob name. #1102
// gave it one so a backup target and the primary store can share a container,
// and the rule is now the same as S3's: the caller always speaks unprefixed
// keys, this is the only place the prefix is added, and every listing strips
// it again on the way out.
func (b *AzureBlobBackend) prefixedKey(key string) (string, error) {
	return prefixObjectKey(b.prefix, key)
}

// prefixedListPrefix is prefixedKey for enumeration, where "" and a trailing
// separator are legitimate and cannot name an object.
func (b *AzureBlobBackend) prefixedListPrefix(prefix string) (string, error) {
	if err := ValidateListPrefix(prefix); err != nil {
		return "", err
	}
	return b.prefix + prefix, nil
}

// partitionValidKeys splits a batch into the PREFIXED blob names of the keys
// that satisfy the storage contract and one error per key that does not.
//
// DeleteBatch was the only key-taking method on any backend that reached the
// service without validating, and on Azure that is not merely a missed
// rejection: a backslash IS a path separator there, verified against Azurite in
// #743, so "a\b.parquet" and "a/b.parquet" are ONE blob and a batch carrying
// the first would delete the second. Nothing else in the codebase can make a
// delete address a different object than the caller named.
//
// It returns prefixed names because DeleteBatch hands them straight to the
// batch builder, which never passes through prefixedKey (#1102). Validating
// without prefixing here would delete at the container root instead.
//
// Rejections are collected rather than fatal, matching S3: callers such as
// compaction and the reconciler submit whole batches with no per-file fallback,
// so failing the batch on one key would leave every other file undeleted
// forever. Split out as a function because AzureBlobBackend cannot be built
// without live credentials, and this behaviour deserves a test that needs none.
func partitionValidKeys(prefix string, batch []string) (usable []string, rejected []error) {
	usable = make([]string, 0, len(batch))
	for _, path := range batch {
		objectName, err := prefixObjectKey(prefix, path)
		if err != nil {
			rejected = append(rejected, err)
			continue
		}
		usable = append(usable, objectName)
	}
	return usable, rejected
}

// Write writes data to Azure Blob Storage.
func (b *AzureBlobBackend) Write(ctx context.Context, path string, data []byte) error {
	return b.WriteReader(ctx, path, bytes.NewReader(data), int64(len(data)))
}

// WriteReader writes data from a reader to Azure Blob Storage
func (b *AzureBlobBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	start := time.Now()

	// Determine content type
	contentType := "application/octet-stream"
	if strings.HasSuffix(path, ".parquet") {
		contentType = "application/vnd.apache.parquet"
	}

	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlockBlobClient(key)

	_, err = blobClient.UploadStream(ctx, reader, &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &contentType,
		},
	})
	if err != nil {
		recordStorageError(ctx, err)
		b.logger.Error().
			Err(err).
			Str("path", path).
			Int64("size", size).
			Msg("Failed to write to Azure Blob Storage")
		return fmt.Errorf("failed to write to Azure Blob Storage: %w", err)
	}

	// Record metrics. Callers may pass size <= 0 for unknown-size streams —
	// count the write but skip the byte counter rather than subtracting from
	// it. Note: size is the caller-declared length, not bytes observed on the
	// wire — UploadStream ignores it and streams to EOF, so a stale declared
	// size drifts the byte counter (all current callers pass stat-derived sizes).
	metrics.Get().IncStorageWrites()
	if size > 0 {
		metrics.Get().IncStorageWriteBytes(size)
	}

	b.logger.Debug().
		Str("path", path).
		Int64("size", size).
		Str("container", b.containerName).
		Dur("duration", time.Since(start)).
		Msg("Wrote to Azure Blob Storage")

	return nil
}

// Read reads data from Azure Blob Storage
func (b *AzureBlobBackend) Read(ctx context.Context, path string) ([]byte, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return nil, err
	}
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		recordStorageError(ctx, err)
		return nil, fmt.Errorf("failed to read from Azure Blob Storage: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	// io.ReadAll returns the data read so far alongside an error — count
	// bytes transferred even on mid-stream failure (real network egress),
	// consistent with ReadTo/ReadToAt.
	if len(data) > 0 {
		metrics.Get().IncStorageReadBytes(int64(len(data)))
	}
	if err != nil {
		recordStorageError(ctx, err)
		return nil, fmt.Errorf("failed to read Azure blob body: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageReads()

	return data, nil
}

// ReadTo reads data from Azure Blob Storage and writes to a writer
func (b *AzureBlobBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to read from Azure Blob Storage: %w", err)
	}
	defer resp.Body.Close()

	bytesRead, err := io.Copy(writer, resp.Body)
	// Count bytes delivered to the writer even when the copy fails mid-stream —
	// partial transfers are real network egress.
	if bytesRead > 0 {
		metrics.Get().IncStorageReadBytes(bytesRead)
	}
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to copy Azure blob: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageReads()

	return nil
}

// ReadToAt reads data from Azure Blob Storage starting at the given byte
// offset and writes to writer. Uses blob.HTTPRange to skip already-transferred
// bytes. offset=0 fetches the full blob without a Range header.
func (b *AzureBlobBackend) ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key)

	var opts *blob.DownloadStreamOptions
	if offset > 0 {
		opts = &blob.DownloadStreamOptions{
			Range: blob.HTTPRange{Offset: offset},
		}
	}
	resp, err := blobClient.DownloadStream(ctx, opts)
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to read from Azure Blob Storage: %w", err)
	}
	defer resp.Body.Close()

	bytesRead, err := io.Copy(writer, resp.Body)
	// Count bytes delivered to the writer even when the copy fails mid-stream —
	// partial transfers are real network egress.
	if bytesRead > 0 {
		metrics.Get().IncStorageReadBytes(bytesRead)
	}
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to copy Azure blob: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageReads()

	return nil
}

// StatFile returns the byte size of the Azure blob at path, or -1 if not found.
func (b *AzureBlobBackend) StatFile(ctx context.Context, path string) (int64, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return 0, err
	}
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key)

	resp, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		if isAzureNotFoundError(err) {
			return -1, nil
		}
		return -1, fmt.Errorf("GetProperties %s: %w", path, err)
	}
	if resp.ContentLength == nil {
		return 0, nil
	}
	return *resp.ContentLength, nil
}

// List lists blobs with the given prefix
func (b *AzureBlobBackend) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var blobs []string

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &fullPrefix,
	})

	for pager.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, azureListError("failed to list Azure blobs", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name != nil {
				// Strip the prefix so callers see keys relative to the
				// logical root. A listing that returned prefixed keys while
				// writes took unprefixed ones is the shape that corrupts a
				// backup manifest in silence (#1102).
				key := strings.TrimPrefix(*blobItem.Name, b.prefix)
				// See S3Backend.List: a listing never returns a key this
				// backend would refuse (#743).
				if ValidateKey(key) != nil {
					continue
				}
				blobs = append(blobs, key)
			}
		}
	}

	return blobs, nil
}

// Delete deletes a blob from Azure Blob Storage
func (b *AzureBlobBackend) Delete(ctx context.Context, path string) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key)

	_, err = blobClient.Delete(ctx, nil)
	if err != nil {
		// Check if it's a "not found" error - that's okay
		if isAzureNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("failed to delete from Azure Blob Storage: %w", err)
	}

	b.logger.Debug().Str("path", path).Msg("Deleted from Azure Blob Storage")
	return nil
}

// DeleteBatch deletes multiple blobs from Azure Blob Storage using the Blob Batch API.
// Azure supports up to 256 sub-requests per batch. Each batch is submitted as a
// single HTTP request, dramatically reducing API call overhead vs per-file Delete.
//
// Individual sub-request failures are inspected: 404 (blob not found) is treated
// as success (already deleted); any other error is collected and returned so
// callers can fall back to per-file Delete or retry.
func (b *AzureBlobBackend) DeleteBatch(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	const batchSize = 256
	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	var nonFatalErrs []error

	for i := 0; i < len(paths); i += batchSize {
		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		batch := paths[i:end]

		bb, err := containerClient.NewBatchBuilder()
		if err != nil {
			return fmt.Errorf("failed to create Azure batch builder: %w", err)
		}

		usable, rejected := partitionValidKeys(b.prefix, batch)
		nonFatalErrs = append(nonFatalErrs, rejected...)
		for _, key := range usable {
			if err := bb.Delete(key, nil); err != nil {
				// Reported on the UNPREFIXED spelling: the caller passed that
				// one and has no reason to know the blob name (#1111).
				return fmt.Errorf("failed to add delete to Azure batch for %q: %w", strings.TrimPrefix(key, b.prefix), err)
			}
		}
		if len(usable) == 0 {
			// Every key in this batch was refused; there is nothing to submit
			// and SubmitBatch rejects an empty builder.
			continue
		}

		resp, err := containerClient.SubmitBatch(ctx, bb, nil)
		if err != nil {
			return fmt.Errorf("failed to submit Azure batch delete: %w", err)
		}

		// Inspect per-blob results. 404 (blob not found) is normal — the
		// blob may already have been deleted. Any other error is collected
		// and returned so callers can fall back.
		for _, item := range resp.Responses {
			if item.Error == nil {
				continue
			}
			if isAzureNotFoundError(item.Error) {
				continue
			}
			// The service answers with the PREFIXED blob name. Report the
			// unprefixed spelling, so the error and the log field name the key
			// the caller passed — the rule partitionValidKeys already follows
			// for a rejection, and that every listing follows on the way out
			// (#1111).
			blobName := "unknown"
			if item.BlobName != nil {
				blobName = strings.TrimPrefix(*item.BlobName, b.prefix)
			}
			b.logger.Warn().
				Err(item.Error).
				Str("blob", blobName).
				Msg("Azure batch: individual delete failed")
			nonFatalErrs = append(nonFatalErrs, fmt.Errorf("%s: %w", blobName, item.Error))
		}
	}

	if len(nonFatalErrs) > 0 {
		return fmt.Errorf("Azure batch delete: %d blob(s) failed: %w", len(nonFatalErrs), errors.Join(nonFatalErrs...))
	}

	b.logger.Debug().Int("count", len(paths)).Msg("Batch deleted from Azure Blob Storage")
	return nil
}

// Exists checks if a blob exists in Azure Blob Storage
func (b *AzureBlobBackend) Exists(ctx context.Context, path string) (bool, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return false, err
	}
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(key)

	_, err = blobClient.GetProperties(ctx, nil)
	if err != nil {
		if isAzureNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check Azure blob existence: %w", err)
	}

	return true, nil
}

// Close closes the Azure Blob backend (no-op for Azure)
func (b *AzureBlobBackend) Close() error {
	b.logger.Info().Msg("Azure Blob Storage backend closed")
	return nil
}

// GetContainer returns the container name
func (b *AzureBlobBackend) GetContainer() string {
	return b.containerName
}

// GetPrefix returns the blob-name prefix (empty string if none configured)
func (b *AzureBlobBackend) GetPrefix() string {
	return b.prefix
}

// GetAccountName returns the account name
func (b *AzureBlobBackend) GetAccountName() string {
	return b.accountName
}

// GetAccountKey returns the account key (for subprocess credential passing)
// GetAccountKey returns the Azure storage account key. It exists solely for
// subprocess credential passing (compaction workers). The returned value is a
// plaintext secret — callers MUST NOT log it, store it, or transmit it
// outside the subprocess environment.
func (b *AzureBlobBackend) GetAccountKey() string {
	return b.accountKey
}

// Type returns the storage type identifier
func (b *AzureBlobBackend) Type() string {
	return "azure"
}

// ConfigJSON returns the configuration as JSON for subprocess recreation.
//
// Every field emitted here must be parsed and forwarded by the compaction
// subprocess (internal/compaction/subprocess.go). Prefix in particular: it is
// applied to every key the backend touches (prefixedKey), so dropping it
// silently reroots the subprocess at the container root and compaction reads
// and writes the wrong location. It defaults to empty, which is how the same
// omission on the S3 side went unnoticed.
//
// This carries no credentials, and must not start to: the subprocess reads
// the account key from the environment the parent sets.
func (b *AzureBlobBackend) ConfigJSON() string {
	config := map[string]interface{}{
		"container":    b.containerName,
		"prefix":       b.prefix,
		"account_name": b.accountName,
		"endpoint":     b.endpoint,
	}
	data, _ := json.Marshal(config)
	return string(data)
}

// ListDirectories lists immediate subdirectories at a prefix.
// Implements the DirectoryLister interface.
// Uses Azure's hierarchy delimiter feature to efficiently list only "directories" (common prefixes).
func (b *AzureBlobBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	// Ensure prefix ends with / for proper directory listing (unless empty)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var dirs []string
	delimiter := "/"

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsHierarchyPager(delimiter, &container.ListBlobsHierarchyOptions{
		Prefix: &fullPrefix,
	})

	for pager.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, azureListError("failed to list Azure directories", err)
		}

		// BlobPrefixes contains the "directories"
		for _, blobPrefix := range page.Segment.BlobPrefixes {
			if blobPrefix.Name != nil {
				// Extract directory name from the prefix. fullPrefix, not the
				// caller's prefix: the names come back with the backend prefix
				// attached, and trimming only the caller's half would leave it
				// on the directory name (see S3Backend.ListDirectories).
				// e.g., "arc/mydb/cpu/" -> "cpu"
				dir := strings.TrimPrefix(*blobPrefix.Name, fullPrefix)
				dir = strings.TrimSuffix(dir, "/")
				if dir != "" && !strings.HasPrefix(dir, ".") {
					dirs = append(dirs, dir)
				}
			}
		}
	}

	return dirs, nil
}

// ListObjects lists blobs with their metadata at a prefix.
// Implements the ObjectLister interface.
func (b *AzureBlobBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var objects []ObjectInfo

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &fullPrefix,
	})

	for pager.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, azureListError("failed to list Azure blobs", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name != nil {
				key := strings.TrimPrefix(*blobItem.Name, b.prefix)
				// See S3Backend.List: a listing never returns a key this
				// backend would refuse (#743).
				if ValidateKey(key) != nil {
					continue
				}
				info := ObjectInfo{
					Path: key,
				}
				if blobItem.Properties != nil {
					if blobItem.Properties.ContentLength != nil {
						info.Size = *blobItem.Properties.ContentLength
					}
					if blobItem.Properties.LastModified != nil {
						info.LastModified = *blobItem.Properties.LastModified
					}
				}
				objects = append(objects, info)
			}
		}
	}

	return objects, nil
}

// HasObjectsUnderPrefix implements PrefixProber over the flat blob pager: the
// first blob whose name the contract accepts answers true, and the pager is
// only advanced while a whole page was blobs ListObjects would hide. See
// S3Backend.HasObjectsUnderPrefix.
func (b *AzureBlobBackend) HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return false, err
	}
	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &fullPrefix,
	})
	for pager.More() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		page, err := pager.NextPage(ctx)
		if err != nil {
			return false, azureListError("failed to list Azure blobs", err)
		}
		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name != nil && ValidateKey(strings.TrimPrefix(*blobItem.Name, b.prefix)) == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

// ListUnusable implements UnusableLister.
//
// Returns exactly what ListObjects drops, so the two partition the container.
// Like S3 and unlike local, ".part" blobs are reported: Azure does not stage
// writes and does not implement StagingInspector, so such a blob is an ordinary
// committed object that #744's reserved suffix made unaddressable.
func (b *AzureBlobBackend) ListUnusable(ctx context.Context, prefix string) ([]UnusableObject, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var objects []UnusableObject

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &fullPrefix,
	})

	for pager.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, azureListError("failed to list Azure blobs", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}
			key := strings.TrimPrefix(*blobItem.Name, b.prefix)
			reason := ValidateKey(key)
			if reason == nil {
				continue // ListObjects returns it
			}
			info := UnusableObject{Path: key, Err: reason}
			if blobItem.Properties != nil {
				if blobItem.Properties.ContentLength != nil {
					info.Size = *blobItem.Properties.ContentLength
				}
				if blobItem.Properties.LastModified != nil {
					info.LastModified = *blobItem.Properties.LastModified
				}
			}
			// Zero-length directory-marker blob. Heuristic, same as S3: the
			// suffix alone is not proof, so the size is part of the test.
			// Note ADLS Gen2 hierarchical-namespace directories do NOT carry a
			// trailing separator, but they also pass ValidateKey, so they never
			// reach here.
			if info.Size == 0 && strings.HasSuffix(info.Path, "/") {
				continue
			}
			objects = append(objects, info)
		}
	}

	return objects, nil
}

// isAzureContainerNotFoundError reports Azure's answer for a container that
// does not exist. Matched on the structured response code only: the string
// "ContainerNotFound" can appear in the request URL of a transport error (a
// storage prefix of that name plus a connection reset), and treating that as
// a missing container would hide a network failure.
func isAzureContainerNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var responseError *azcore.ResponseError
	if errors.As(err, &responseError) {
		return responseError.ErrorCode == "ContainerNotFound"
	}
	return false
}

// azureListError wraps a listing failure, tagging a missing container with
// ErrStoreNotFound while keeping the SDK error in the chain. Mirrors
// listError in s3.go; every paginated listing here routes through it.
func azureListError(what string, err error) error {
	if isAzureContainerNotFoundError(err) {
		return fmt.Errorf("%s: %w: %w", what, err, ErrStoreNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// isAzureNotFoundError checks if an error indicates the blob doesn't exist
func isAzureNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for Azure-specific error response using standard errors.As (handles multi-errors/errors.Join)
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == 404
	}

	// Fallback to string matching
	errStr := err.Error()
	return strings.Contains(errStr, "BlobNotFound") ||
		strings.Contains(errStr, "404") ||
		strings.Contains(errStr, "NotFound")
}
