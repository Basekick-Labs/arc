package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog"
)

// Multipart upload thresholds
const (
	// Files larger than this will use multipart upload (100MB)
	multipartThreshold = 100 * 1024 * 1024
	// Part size for multipart upload (16MB - balances memory usage and performance)
	multipartPartSize = 16 * 1024 * 1024
	// Concurrency for multipart upload
	multipartConcurrency = 5
)

// S3Backend implements the Backend interface for S3 and MinIO storage
type S3Backend struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	region   string
	endpoint string
	pathStyle bool
	logger   zerolog.Logger
}

// S3Config holds S3 backend configuration
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string // Custom endpoint for MinIO (e.g., "http://localhost:9000")
	AccessKey string
	SecretKey string
	UseSSL    bool
	PathStyle bool // Use path-style addressing (required for MinIO)
}

// NewS3Backend creates a new S3/MinIO backend
func NewS3Backend(cfg *S3Config, logger zerolog.Logger) (*S3Backend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket name is required")
	}

	log := logger.With().Str("component", "s3-storage").Logger()

	// Build AWS config options
	var opts []func(*config.LoadOptions) error

	// Set region
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts = append(opts, config.WithRegion(region))

	// Configure credentials
	accessKey := cfg.AccessKey
	secretKey := cfg.SecretKey

	// Fall back to environment variables
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
		log.Info().Msg("Using static credentials for S3")
	} else {
		log.Info().Msg("Using default credential chain for S3 (environment, IAM role, etc.)")
	}

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Build S3 client options
	var s3Opts []func(*s3.Options)

	// Custom endpoint for MinIO
	if cfg.Endpoint != "" {
		endpoint := cfg.Endpoint
		// Ensure endpoint has protocol
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			if cfg.UseSSL {
				endpoint = "https://" + endpoint
			} else {
				endpoint = "http://" + endpoint
			}
		}

		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		log.Info().Str("endpoint", endpoint).Msg("Using custom S3 endpoint")
	}

	// Path-style addressing (required for MinIO)
	if cfg.PathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
		log.Info().Msg("Using path-style S3 addressing (MinIO compatible)")
	}

	// Create S3 client
	client := s3.NewFromConfig(awsCfg, s3Opts...)

	// Create uploader with multipart settings for large files
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = multipartPartSize
		u.Concurrency = multipartConcurrency
	})

	backend := &S3Backend{
		client:   client,
		uploader: uploader,
		bucket:   cfg.Bucket,
		region:   region,
		endpoint: cfg.Endpoint,
		pathStyle: cfg.PathStyle,
		logger:   log,
	}

	// Test connection by checking if bucket exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		log.Warn().Err(err).Str("bucket", cfg.Bucket).Msg("Could not verify bucket exists (may need to create it)")
	} else {
		log.Info().Str("bucket", cfg.Bucket).Msg("Successfully connected to S3 bucket")
	}

	return backend, nil
}

// Write writes data to S3
func (b *S3Backend) Write(ctx context.Context, path string, data []byte) error {
	return b.WriteReader(ctx, path, bytes.NewReader(data), int64(len(data)))
}

// WriteReader writes data from a reader to S3
// For files larger than 100MB, uses multipart upload to avoid OOM
func (b *S3Backend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	start := time.Now()

	// Determine content type
	contentType := "application/octet-stream"
	if strings.HasSuffix(path, ".parquet") {
		contentType = "application/vnd.apache.parquet"
	}

	// Use multipart upload for large files or unknown size
	// This streams data in chunks without loading everything into memory
	if size <= 0 || size >= multipartThreshold {
		return b.writeMultipart(ctx, path, reader, size, contentType, start)
	}

	// For small files with known size, use simple PutObject
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(path),
		Body:          reader,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
	})
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("path", path).
			Int64("size", size).
			Msg("Failed to write to S3")
		return fmt.Errorf("failed to write to S3: %w", err)
	}

	b.logger.Debug().
		Str("path", path).
		Int64("size", size).
		Str("bucket", b.bucket).
		Dur("duration", time.Since(start)).
		Msg("Wrote to S3")

	return nil
}

// writeMultipart handles multipart upload for large files
// This streams data in 16MB chunks without loading the entire file into memory
func (b *S3Backend) writeMultipart(ctx context.Context, path string, reader io.Reader, size int64, contentType string, start time.Time) error {
	_, err := b.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(path),
		Body:        reader,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("path", path).
			Int64("size", size).
			Msg("Failed multipart upload to S3")
		return fmt.Errorf("failed multipart upload to S3: %w", err)
	}

	b.logger.Info().
		Str("path", path).
		Int64("size", size).
		Str("bucket", b.bucket).
		Dur("duration", time.Since(start)).
		Bool("multipart", true).
		Msg("Wrote to S3 via multipart upload")

	return nil
}

// Read reads data from S3
func (b *S3Backend) Read(ctx context.Context, path string) ([]byte, error) {
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read from S3: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 object body: %w", err)
	}

	return data, nil
}

// ReadTo reads data from S3 and writes to a writer
func (b *S3Backend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return fmt.Errorf("failed to read from S3: %w", err)
	}
	defer result.Body.Close()

	_, err = io.Copy(writer, result.Body)
	if err != nil {
		return fmt.Errorf("failed to copy S3 object: %w", err)
	}

	return nil
}

// List lists objects with the given prefix
func (b *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var objects []string
	var continuationToken *string

	for {
		result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				objects = append(objects, *obj.Key)
			}
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return objects, nil
}

// Delete deletes an object from S3
func (b *S3Backend) Delete(ctx context.Context, path string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return fmt.Errorf("failed to delete from S3: %w", err)
	}

	b.logger.Debug().Str("path", path).Msg("Deleted from S3")
	return nil
}

// DeleteBatch deletes multiple objects from S3 efficiently
func (b *S3Backend) DeleteBatch(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	// S3 allows up to 1000 objects per delete request
	const batchSize = 1000

	for i := 0; i < len(paths); i += batchSize {
		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}

		batch := paths[i:end]
		objects := make([]types.ObjectIdentifier, len(batch))
		for j, path := range batch {
			objects[j] = types.ObjectIdentifier{
				Key: aws.String(path),
			}
		}

		_, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.bucket),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete batch from S3: %w", err)
		}
	}

	b.logger.Debug().Int("count", len(paths)).Msg("Batch deleted from S3")
	return nil
}

// Exists checks if an object exists in S3
func (b *S3Backend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		// Check if it's a "not found" error
		var nsk *types.NoSuchKey
		if ok := isNotFoundError(err); ok {
			return false, nil
		}
		// Treat other head object errors as "not found" for compatibility
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		_ = nsk // silence unused variable warning
		return false, fmt.Errorf("failed to check S3 object existence: %w", err)
	}

	return true, nil
}

// isNotFoundError checks if an error indicates the object doesn't exist
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "NotFound") ||
		strings.Contains(errStr, "NoSuchKey") ||
		strings.Contains(errStr, "404")
}

// Close closes the S3 backend (no-op for S3)
func (b *S3Backend) Close() error {
	b.logger.Info().Msg("S3 backend closed")
	return nil
}

// GetBucket returns the bucket name
func (b *S3Backend) GetBucket() string {
	return b.bucket
}

// GetRegion returns the region
func (b *S3Backend) GetRegion() string {
	return b.region
}

// GetS3Path returns the S3 URI for a path
func (b *S3Backend) GetS3Path(path string) string {
	return fmt.Sprintf("s3://%s/%s", b.bucket, path)
}

// GetQueryPath generates S3 path patterns for time-based query pruning
// This enables DuckDB to efficiently scan only relevant partitions
//
// Examples:
//   - GetQueryPath("mydb", "cpu", 2025, 11, 0, 0)   → "s3://bucket/mydb/cpu/2025/11/*/*/*.parquet" (all November)
//   - GetQueryPath("mydb", "cpu", 2025, 11, 25, 0) → "s3://bucket/mydb/cpu/2025/11/25/*/*.parquet" (specific day)
//   - GetQueryPath("mydb", "cpu", 2025, 11, 25, 16) → "s3://bucket/mydb/cpu/2025/11/25/16/*.parquet" (specific hour)
func (b *S3Backend) GetQueryPath(database, measurement string, year, month, day, hour int) string {
	if hour > 0 {
		// Specific hour
		return fmt.Sprintf("s3://%s/%s/%s/%04d/%02d/%02d/%02d/*.parquet",
			b.bucket, database, measurement, year, month, day, hour)
	} else if day > 0 {
		// Specific day, all hours
		return fmt.Sprintf("s3://%s/%s/%s/%04d/%02d/%02d/*/*.parquet",
			b.bucket, database, measurement, year, month, day)
	} else if month > 0 {
		// Specific month, all days and hours
		return fmt.Sprintf("s3://%s/%s/%s/%04d/%02d/*/*/*.parquet",
			b.bucket, database, measurement, year, month)
	} else {
		// Entire year
		return fmt.Sprintf("s3://%s/%s/%s/%04d/*/*/*/*.parquet",
			b.bucket, database, measurement, year)
	}
}

// GetQueryPathRange generates S3 path pattern for a time range
// This is useful for queries like "WHERE time BETWEEN start AND end"
func (b *S3Backend) GetQueryPathRange(database, measurement string, startTime, endTime time.Time) []string {
	var paths []string

	// Generate paths for each day in the range
	current := startTime.Truncate(24 * time.Hour)
	end := endTime.Truncate(24 * time.Hour).Add(24 * time.Hour)

	for current.Before(end) {
		path := fmt.Sprintf("s3://%s/%s/%s/%04d/%02d/%02d/*/*.parquet",
			b.bucket, database, measurement,
			current.Year(), int(current.Month()), current.Day())
		paths = append(paths, path)
		current = current.Add(24 * time.Hour)
	}

	return paths
}

// Type returns the storage type identifier
func (b *S3Backend) Type() string {
	return "s3"
}

// ConfigJSON returns the configuration as JSON for subprocess recreation
func (b *S3Backend) ConfigJSON() string {
	config := map[string]interface{}{
		"bucket":     b.bucket,
		"region":     b.region,
		"endpoint":   b.endpoint,
		"path_style": b.pathStyle,
	}
	data, _ := json.Marshal(config)
	return string(data)
}
