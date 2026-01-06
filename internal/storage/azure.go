package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/rs/zerolog"
)

// AzureBlobBackend implements the Backend interface for Azure Blob Storage
type AzureBlobBackend struct {
	client        *azblob.Client
	containerName string
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

	// Custom endpoint (for Azurite testing)
	Endpoint string
}

// NewAzureBlobBackend creates a new Azure Blob Storage backend
func NewAzureBlobBackend(cfg *AzureBlobConfig, logger zerolog.Logger) (*AzureBlobBackend, error) {
	if cfg.ContainerName == "" {
		return nil, fmt.Errorf("Azure container name is required")
	}

	log := logger.With().Str("component", "azure-storage").Logger()

	var client *azblob.Client
	var err error
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

// Write writes data to Azure Blob Storage
func (b *AzureBlobBackend) Write(ctx context.Context, path string, data []byte) error {
	return b.WriteReader(ctx, path, bytes.NewReader(data), int64(len(data)))
}

// WriteReader writes data from a reader to Azure Blob Storage
func (b *AzureBlobBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	start := time.Now()

	// Determine content type
	contentType := "application/octet-stream"
	if strings.HasSuffix(path, ".parquet") {
		contentType = "application/vnd.apache.parquet"
	}

	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlockBlobClient(path)

	_, err := blobClient.UploadStream(ctx, reader, &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &contentType,
		},
	})
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("path", path).
			Int64("size", size).
			Msg("Failed to write to Azure Blob Storage")
		return fmt.Errorf("failed to write to Azure Blob Storage: %w", err)
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
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(path)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read from Azure Blob Storage: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read Azure blob body: %w", err)
	}

	return data, nil
}

// ReadTo reads data from Azure Blob Storage and writes to a writer
func (b *AzureBlobBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(path)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to read from Azure Blob Storage: %w", err)
	}
	defer resp.Body.Close()

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy Azure blob: %w", err)
	}

	return nil
}

// List lists blobs with the given prefix
func (b *AzureBlobBackend) List(ctx context.Context, prefix string) ([]string, error) {
	var blobs []string

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list Azure blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name != nil {
				blobs = append(blobs, *blobItem.Name)
			}
		}
	}

	return blobs, nil
}

// Delete deletes a blob from Azure Blob Storage
func (b *AzureBlobBackend) Delete(ctx context.Context, path string) error {
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(path)

	_, err := blobClient.Delete(ctx, nil)
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

// DeleteBatch deletes multiple blobs from Azure Blob Storage
// Azure Blob Storage supports batch delete via the Batch API
func (b *AzureBlobBackend) DeleteBatch(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	// Azure Blob batch operations can handle up to 256 operations per batch
	const batchSize = 256

	for i := 0; i < len(paths); i += batchSize {
		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}

		batch := paths[i:end]

		// Delete each blob individually (Azure SDK batch delete requires more setup)
		// For simplicity, we use individual deletes in parallel concept
		for _, path := range batch {
			if err := b.Delete(ctx, path); err != nil {
				b.logger.Warn().Err(err).Str("path", path).Msg("Failed to delete blob in batch")
				// Continue with other deletions
			}
		}
	}

	b.logger.Debug().Int("count", len(paths)).Msg("Batch deleted from Azure Blob Storage")
	return nil
}

// Exists checks if a blob exists in Azure Blob Storage
func (b *AzureBlobBackend) Exists(ctx context.Context, path string) (bool, error) {
	blobClient := b.client.ServiceClient().NewContainerClient(b.containerName).NewBlobClient(path)

	_, err := blobClient.GetProperties(ctx, nil)
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

// GetAccountName returns the account name
func (b *AzureBlobBackend) GetAccountName() string {
	return b.accountName
}

// GetAccountKey returns the account key (for subprocess credential passing)
func (b *AzureBlobBackend) GetAccountKey() string {
	return b.accountKey
}

// Type returns the storage type identifier
func (b *AzureBlobBackend) Type() string {
	return "azure"
}

// ConfigJSON returns the configuration as JSON for subprocess recreation
func (b *AzureBlobBackend) ConfigJSON() string {
	config := map[string]interface{}{
		"container":    b.containerName,
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

	var dirs []string
	delimiter := "/"

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsHierarchyPager(delimiter, &container.ListBlobsHierarchyOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list Azure directories: %w", err)
		}

		// BlobPrefixes contains the "directories"
		for _, blobPrefix := range page.Segment.BlobPrefixes {
			if blobPrefix.Name != nil {
				// Extract directory name from the prefix
				// e.g., "mydb/cpu/" -> "cpu"
				dir := strings.TrimPrefix(*blobPrefix.Name, prefix)
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
	var objects []ObjectInfo

	containerClient := b.client.ServiceClient().NewContainerClient(b.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list Azure blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name != nil {
				info := ObjectInfo{
					Path: *blobItem.Name,
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

// isAzureNotFoundError checks if an error indicates the blob doesn't exist
func isAzureNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for Azure-specific error response
	var respErr *azcore.ResponseError
	if ok := isResponseError(err, &respErr); ok {
		return respErr.StatusCode == 404
	}

	// Fallback to string matching
	errStr := err.Error()
	return strings.Contains(errStr, "BlobNotFound") ||
		strings.Contains(errStr, "404") ||
		strings.Contains(errStr, "NotFound")
}

// isResponseError checks if err is an azcore.ResponseError
func isResponseError(err error, target **azcore.ResponseError) bool {
	if err == nil {
		return false
	}
	// Use errors.As pattern
	for err != nil {
		if re, ok := err.(*azcore.ResponseError); ok {
			*target = re
			return true
		}
		// Try to unwrap
		if unwrapper, ok := err.(interface{ Unwrap() error }); ok {
			err = unwrapper.Unwrap()
		} else {
			break
		}
	}
	return false
}
