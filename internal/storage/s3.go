package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/basekick-labs/arc/internal/metrics"
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

// ErrBodyLength reports a body that ended before, or continued past, the
// length its caller declared to WriteReader. Nothing is committed: a small
// body is checked before its PutObject is sent, and a multipart upload is
// aborted.
var ErrBodyLength = errors.New("s3: body length does not match the declared size")

// HTTP transport bounds for the AWS SDK client. These cap the per-process
// idle-connection state — the leak we care about is HTTP/2 frame buffers and
// keep-alive metadata accumulating across long retention/delete sweeps. We do
// not touch dial / handshake / response timeouts: those are already correct at
// SDK defaults, and shortening them regresses cold-start and high-RTT setups.
const (
	// Total idle-conn cap across all hosts. Matches Go's http.DefaultTransport
	// default — the actual leak is bounded by MaxIdleConnsPerHost+IdleConnTimeout
	// below; this is set explicitly so the bounds are self-documenting on the
	// transport we ship rather than inherited from a future Go default change.
	s3MaxIdleConns = 100
	// Per-host idle-conn cap. Sized to comfortably accommodate two concurrent
	// multipart uploads (each at multipartConcurrency=5) without churning the
	// pool — at least 2*multipartConcurrency, with headroom.
	s3MaxIdleConnsPerHost = 16
	// How long an idle connection survives in the pool. Matches Go default.
	s3IdleConnTimeout = 90 * time.Second
)

// S3Backend implements the Backend interface for S3 and MinIO storage
type S3Backend struct {
	client    *s3.Client
	uploader  *manager.Uploader
	bucket    string
	prefix    string // path prefix within the bucket (sanitized, with trailing /)
	region    string
	endpoint  string
	pathStyle bool
	useSSL    bool   // stored for subprocess config passing
	accessKey string // stored for subprocess credential passing
	secretKey string // stored for subprocess credential passing
	logger    zerolog.Logger
}

// S3Config holds S3 backend configuration
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string // Custom endpoint for MinIO (e.g., "http://localhost:9000")
	AccessKey string
	SecretKey string
	UseSSL    bool
	PathStyle bool   // Use path-style addressing (required for MinIO)
	Prefix    string // Path prefix within the bucket (e.g., "instances/abc123/")
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

	// Bounded HTTP transport: caps the idle-connection pool so per-connection
	// HTTP/2 frame buffers and keep-alive metadata don't accumulate across
	// long retention/delete sweeps. Honours AWS_CA_BUNDLE / proxy env vars
	// because awshttp.NewBuildableClient builds on http.DefaultTransport.
	httpClient := awshttp.NewBuildableClient().
		WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConns = s3MaxIdleConns
			t.MaxIdleConnsPerHost = s3MaxIdleConnsPerHost
			t.IdleConnTimeout = s3IdleConnTimeout
		})
	opts = append(opts, config.WithHTTPClient(httpClient))

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

	// Validate the prefix rather than repairing it. A prefix that cannot form
	// usable keys must stop the backend from being built: the old fallback was
	// the bucket root, which is a different location, not a safe default.
	prefix, err := ValidateObjectPrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}

	backend := &S3Backend{
		client:    client,
		uploader:  uploader,
		bucket:    cfg.Bucket,
		prefix:    prefix,
		region:    region,
		endpoint:  cfg.Endpoint,
		pathStyle: cfg.PathStyle,
		useSSL:    cfg.UseSSL,
		accessKey: accessKey,
		secretKey: secretKey,
		logger:    log,
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

// WriteReader writes data from a reader to S3.
//
// A body the SDK can rewind (*os.File, *bytes.Reader) goes as one PutObject
// under the multipart threshold and through the Uploader above it. A body it
// cannot rewind — a streaming copy's io.Pipe, an HTTP request body — needs
// different handling on a plain-HTTP endpoint: the SDK signs the payload and
// computes a request checksum before sending, both of which re-read the
// body, and its rewind-free alternative (trailing checksums) exists only over
// TLS. Such a body is read into memory first: up to one part size it becomes
// a right-sized buffer behind the same PutObject, beyond that the Uploader
// buffers 16 MiB parts. A declared size is enforced for an unrewindable body
// (ErrBodyLength on a mismatch, nothing committed), and the source must end
// — EOF or error — once the declared bytes have been read.
func (b *S3Backend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
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

	seekable := isSeekable(reader)
	switch {
	case size <= 0 || size >= multipartThreshold:
		// Large or unknown length: the Uploader streams it in parts.
		if !seekable && size > 0 {
			reader = &exactLengthReader{ctx: ctx, r: reader, want: size}
		}
		return b.writeMultipart(ctx, path, reader, size, contentType, start)
	case !seekable && size > multipartPartSize:
		return b.writeMultipart(ctx, path, &exactLengthReader{ctx: ctx, r: reader, want: size}, size, contentType, start)
	case !seekable:
		// The request never leaves the process if the body is wrong, so this
		// is the caller's error, not a storage error: not counted, and not
		// logged here — every caller logs (or, for edge sync's dropped link,
		// deliberately does not).
		buf, err := spoolExact(ctx, reader, size)
		if err != nil {
			return fmt.Errorf("failed to read body for S3 upload: %w", err)
		}
		reader = bytes.NewReader(buf)
	}

	// For small files with known size, use simple PutObject
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          reader,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
	})
	if err != nil {
		recordStorageError(ctx, err)
		b.logger.Error().
			Err(err).
			Str("path", path).
			Int64("size", size).
			Msg("Failed to write to S3")
		return fmt.Errorf("failed to write to S3: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageWrites()
	metrics.Get().IncStorageWriteBytes(size)

	b.logger.Debug().
		Str("path", path).
		Int64("size", size).
		Str("bucket", b.bucket).
		Dur("duration", time.Since(start)).
		Msg("Wrote to S3")

	return nil
}

// writeMultipart hands the body to the SDK Uploader, which buffers 16 MiB
// parts into memory and sends them as rewindable requests: one PutObject
// when the body ends within the first part, a multipart upload otherwise.
// A failed multipart upload is aborted, but only while ctx is live — a
// cancelled context leaves parts behind, so a bucket that receives these
// should carry an AbortIncompleteMultipartUpload lifecycle rule.
func (b *S3Backend) writeMultipart(ctx context.Context, path string, reader io.Reader, size int64, contentType string, start time.Time) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	out, err := b.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(key),
		Body:        reader,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		// A length mismatch is the caller's error, counted and logged the
		// same way as on the spooled path: by the caller. Everything else is
		// the store's.
		if !errors.Is(err, ErrBodyLength) {
			recordStorageError(ctx, err)
			b.logger.Error().
				Err(err).
				Str("path", path).
				Int64("size", size).
				Msg("Failed streamed upload to S3")
		}
		return fmt.Errorf("failed streamed upload to S3: %w", err)
	}

	// Record metrics. This is also the path for unknown-size streams
	// (size <= 0), where the byte count is unavailable — count the write but
	// skip the byte counter rather than subtracting from it. For an
	// unrewindable body size is enforced on the wire (exactLengthReader);
	// for a rewindable one it is the caller's declared length, which every
	// such caller derives from a stat.
	metrics.Get().IncStorageWrites()
	if size > 0 {
		metrics.Get().IncStorageWriteBytes(size)
	}

	b.logger.Debug().
		Str("path", path).
		Int64("size", size).
		Str("bucket", b.bucket).
		Dur("duration", time.Since(start)).
		Bool("multipart", out != nil && out.UploadID != "").
		Msg("Wrote to S3 via uploader")

	return nil
}

// isSeekable mirrors the SDK's own test: an io.Seeker whose Seek works. A
// body that only claims Seek (a FIFO behind an *os.File) would otherwise
// reach PutObject, where the SDK's own probe fails it with the Seek error
// before any request goes out, instead of being buffered here.
func isSeekable(r io.Reader) bool {
	s, ok := r.(io.Seeker)
	if !ok {
		return false
	}
	_, err := s.Seek(0, io.SeekCurrent)
	return err == nil
}

// spoolExact reads exactly want bytes into memory and confirms the source
// ends there. The source's own errors pass through unchanged (edge sync
// matches its errShortBody with errors.Is); a body that ends early or runs
// long is ErrBodyLength.
func spoolExact(ctx context.Context, r io.Reader, want int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	buf := make([]byte, want)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: body ended after %d of %d bytes", ErrBodyLength, n, want)
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := expectEOF(r, want); err != nil {
		return nil, err
	}
	return buf, nil
}

// expectEOF reads one byte past the declared length: the source must end.
// io.ReadFull so that a spurious (0, nil) — permitted by io.Reader, meaning
// "nothing happened" — is retried rather than taken for the end.
func expectEOF(r io.Reader, want int64) error {
	var probe [1]byte
	n, err := io.ReadFull(r, probe[:])
	if n > 0 {
		return fmt.Errorf("%w: body continues past the declared %d bytes", ErrBodyLength, want)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// exactLengthReader feeds the Uploader an unrewindable body of a declared
// length: it hands over at most want bytes, fails with ErrBodyLength if the
// source ends early or continues past want, and honours ctx between reads
// (the Uploader only checks ctx when it takes a buffer from its pool). The
// source's own errors pass through unchanged.
type exactLengthReader struct {
	ctx  context.Context
	r    io.Reader
	want int64
	n    int64
}

func (e *exactLengthReader) Read(p []byte) (int, error) {
	if err := e.ctx.Err(); err != nil {
		return 0, err
	}
	if e.n >= e.want {
		if err := expectEOF(e.r, e.want); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	if rem := e.want - e.n; int64(len(p)) > rem {
		p = p[:rem]
	}
	n, err := e.r.Read(p)
	e.n += int64(n)
	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, io.EOF):
		if e.n < e.want {
			return n, fmt.Errorf("%w: body ended after %d of %d bytes", ErrBodyLength, e.n, e.want)
		}
		return n, io.EOF
	default:
		return n, err
	}
}

// Read reads data from S3
func (b *S3Backend) Read(ctx context.Context, path string) ([]byte, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return nil, err
	}
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		recordStorageError(ctx, err)
		return nil, fmt.Errorf("failed to read from S3: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	// io.ReadAll returns the data read so far alongside an error — count
	// bytes transferred even on mid-stream failure (real network egress),
	// consistent with ReadTo/ReadToAt.
	if len(data) > 0 {
		metrics.Get().IncStorageReadBytes(int64(len(data)))
	}
	if err != nil {
		recordStorageError(ctx, err)
		return nil, fmt.Errorf("failed to read S3 object body: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageReads()

	return data, nil
}

// ReadTo reads data from S3 and writes to a writer
func (b *S3Backend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to read from S3: %w", err)
	}
	defer result.Body.Close()

	bytesRead, err := io.Copy(writer, result.Body)
	// Count bytes delivered to the writer even when the copy fails mid-stream —
	// partial transfers are real network egress.
	if bytesRead > 0 {
		metrics.Get().IncStorageReadBytes(bytesRead)
	}
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to copy S3 object: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageReads()

	return nil
}

// ReadToAt reads data from S3 starting at the given byte offset and writes to
// writer. Uses an HTTP Range header to skip already-transferred bytes.
// offset=0 fetches the full object (no Range header sent).
func (b *S3Backend) ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if offset > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}
	result, err := b.client.GetObject(ctx, input)
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to read from S3: %w", err)
	}
	defer result.Body.Close()

	bytesRead, err := io.Copy(writer, result.Body)
	// Count bytes delivered to the writer even when the copy fails mid-stream —
	// partial transfers are real network egress.
	if bytesRead > 0 {
		metrics.Get().IncStorageReadBytes(bytesRead)
	}
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to copy S3 object: %w", err)
	}

	// Record metrics
	metrics.Get().IncStorageReads()

	return nil
}

// StatFile returns the byte size of the S3 object at path, or -1 if not found.
func (b *S3Backend) StatFile(ctx context.Context, path string) (int64, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return 0, err
	}
	result, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return -1, nil
		}
		return -1, fmt.Errorf("HeadObject %s: %w", path, err)
	}
	if result.ContentLength == nil {
		return 0, nil
	}
	return *result.ContentLength, nil
}

// List lists objects with the given prefix
func (b *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var objects []string
	var continuationToken *string

	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			if isNoSuchBucketError(err) {
				return []string{}, nil
			}
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				// Strip prefix so callers see paths relative to the logical root
				key := strings.TrimPrefix(*obj.Key, b.prefix)
				// A listing must never hand back a key this backend would
				// refuse (#743). Object stores carry "directory marker"
				// objects whose key ends in a separator, created by consoles
				// and sync tools, and Arc's own callers feed List output
				// straight into Read, Exists and Delete. Returning one turns
				// every such consumer into a failure: restore drops the file
				// and still reports success, compaction's "already gone" skip
				// becomes a hard error, and manifest recovery retries a
				// permanent error forever. They are not data, so they are
				// skipped rather than reported.
				if ValidateKey(key) != nil {
					continue
				}
				objects = append(objects, key)
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
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	_, err = b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete from S3: %w", err)
	}

	b.logger.Debug().Str("path", path).Msg("Deleted from S3")
	return nil
}

// DeleteBatch deletes multiple objects from S3 using the DeleteObjects API.
// S3 supports up to 1000 keys per request. Individual object deletion errors
// (e.g. permission denied, key not found) are inspected: the not-found case
// is treated as success; any other error is collected and returned so callers
// can fall back to per-file Delete or retry.
func (b *S3Backend) DeleteBatch(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	// S3 allows up to 1000 objects per delete request
	const batchSize = 1000
	var nonFatalErrs []error

	for i := 0; i < len(paths); i += batchSize {
		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}

		batch := paths[i:end]
		// Best-effort per key rather than all-or-nothing. Two callers
		// (backup/manager.go, compaction/job.go) return this error with no
		// per-file fallback, so failing the whole batch on one bad key would
		// make a backup permanently undeletable and leave compaction inputs
		// beside their output forever. The keys come from List, so they are
		// whatever is in the bucket, not only what Arc wrote.
		objects := make([]types.ObjectIdentifier, 0, len(batch))
		for _, p := range batch {
			key, kerr := b.prefixedKey(p)
			if kerr != nil {
				nonFatalErrs = append(nonFatalErrs, kerr)
				continue
			}
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}
		if len(objects) == 0 {
			continue
		}

		output, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.bucket),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete batch from S3: %w", err)
		}

		// Inspect per-object errors from the response. With Quiet=true only
		// errors are returned. NotFound is normal (already deleted); other
		// errors are collected and returned.
		for _, e := range output.Errors {
			if e.Code != nil && *e.Code == "NoSuchKey" {
				continue // already deleted, not an error
			}
			key := "unknown"
			if e.Key != nil {
				key = *e.Key
			}
			code := ""
			if e.Code != nil {
				code = *e.Code
			}
			msg := ""
			if e.Message != nil {
				msg = *e.Message
			}
			b.logger.Warn().
				Str("key", key).
				Str("code", code).
				Str("message", msg).
				Msg("S3 batch: individual delete failed")
			nonFatalErrs = append(nonFatalErrs, fmt.Errorf("%s: %s: %s", key, code, msg))
		}
	}

	if len(nonFatalErrs) > 0 {
		return fmt.Errorf("S3 batch delete: %d object(s) failed: %w", len(nonFatalErrs), errors.Join(nonFatalErrs...))
	}

	b.logger.Debug().Int("count", len(paths)).Msg("Batch deleted from S3")
	return nil
}

// Exists checks if an object exists in S3
func (b *S3Backend) Exists(ctx context.Context, path string) (bool, error) {
	key, err := b.prefixedKey(path)
	if err != nil {
		return false, err
	}
	_, err = b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
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

// Missing buckets are normal on the first read of a fresh object store.
func isNoSuchBucketError(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchBucket"
}

// prefixedKey validates a storage key and prepends the configured prefix.
//
// Returning an error is what makes the contract hold: a new method that builds
// an S3 key has to deal with it, rather than silently passing an unvalidated
// string to the SDK. Note this is not a total chokepoint. Arc's READ path does
// not go through Backend at all: storage/util.go builds an s3:// URI that
// DuckDB's read_parquet consumes, and iceberg-go writes metadata through its
// own FileIO. Both are out of scope here (#746).
func (b *S3Backend) prefixedKey(key string) (string, error) {
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return b.prefix + key, nil
}

// prefixedListPrefix is prefixedKey for enumeration, where "" and a trailing
// separator are legitimate and cannot name an object.
func (b *S3Backend) prefixedListPrefix(prefix string) (string, error) {
	if err := ValidateListPrefix(prefix); err != nil {
		return "", err
	}
	return b.prefix + prefix, nil
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

// GetPrefix returns the path prefix (empty string if none configured)
func (b *S3Backend) GetPrefix() string {
	return b.prefix
}

// GetRegion returns the region
func (b *S3Backend) GetRegion() string {
	return b.region
}

// GetAccessKey returns the access key (for subprocess credential passing)
// GetAccessKey returns the S3 access key. It exists solely for subprocess
// credential passing (compaction workers). The returned value is a plaintext
// secret — callers MUST NOT log it, store it, or transmit it outside the
// subprocess environment.
func (b *S3Backend) GetAccessKey() string {
	return b.accessKey
}

// GetSecretKey returns the secret key (for subprocess credential passing)
// GetSecretKey returns the S3 secret key. It exists solely for subprocess
// credential passing (compaction workers). The returned value is a plaintext
// secret — callers MUST NOT log it, store it, or transmit it outside the
// subprocess environment.
func (b *S3Backend) GetSecretKey() string {
	return b.secretKey
}

// Type returns the storage type identifier
func (b *S3Backend) Type() string {
	return "s3"
}

// ConfigJSON returns the configuration as JSON for subprocess recreation
func (b *S3Backend) ConfigJSON() string {
	config := map[string]interface{}{
		"bucket":     b.bucket,
		"prefix":     b.prefix,
		"region":     b.region,
		"endpoint":   b.endpoint,
		"path_style": b.pathStyle,
		"use_ssl":    b.useSSL,
	}
	data, _ := json.Marshal(config)
	return string(data)
}

// ListDirectories lists immediate subdirectories at a prefix.
// Implements the DirectoryLister interface.
// Uses S3's delimiter feature to efficiently list only "directories" (common prefixes).
func (b *S3Backend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	// Ensure prefix ends with / for proper directory listing (unless empty)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var dirs []string
	var continuationToken *string

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			if isNoSuchBucketError(err) {
				return []string{}, nil
			}
			return nil, fmt.Errorf("failed to list S3 directories: %w", err)
		}

		// CommonPrefixes contains the "directories"
		for _, cp := range result.CommonPrefixes {
			if cp.Prefix != nil {
				// Extract directory name from the prefix
				// e.g., "instances/abc/mydb/cpu/" -> "cpu"
				dir := strings.TrimPrefix(*cp.Prefix, fullPrefix)
				dir = strings.TrimSuffix(dir, "/")
				if dir != "" && !strings.HasPrefix(dir, ".") {
					dirs = append(dirs, dir)
				}
			}
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return dirs, nil
}

// ListObjects lists objects with their metadata at a prefix.
// Implements the ObjectLister interface.
func (b *S3Backend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	var continuationToken *string

	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			if isNoSuchBucketError(err) {
				return []ObjectInfo{}, nil
			}
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				key := strings.TrimPrefix(*obj.Key, b.prefix)
				// See List: a listing never returns a key the backend would
				// refuse (#743).
				if ValidateKey(key) != nil {
					continue
				}
				info := ObjectInfo{Path: key}
				if obj.Size != nil {
					info.Size = *obj.Size
				}
				if obj.LastModified != nil {
					info.LastModified = *obj.LastModified
				}
				objects = append(objects, info)
			}
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return objects, nil
}

// HasObjectsUnderPrefix implements PrefixProber: it pages ListObjectsV2 under
// the same prefix ListObjects would use and returns at the first key the
// contract accepts. The continuation token is followed only while every key
// of a page was one ListObjects would hide, so a prefix holding nothing but
// directory markers or foreign keys is still answered correctly, and a prefix
// with data is answered from its first page.
func (b *S3Backend) HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return false, err
	}
	var continuationToken *string
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return false, fmt.Errorf("failed to list S3 objects: %w", err)
		}
		for _, obj := range result.Contents {
			if obj.Key == nil {
				continue
			}
			if ValidateKey(strings.TrimPrefix(*obj.Key, b.prefix)) == nil {
				return true, nil
			}
		}
		if result.IsTruncated == nil || !*result.IsTruncated {
			return false, nil
		}
		continuationToken = result.NextContinuationToken
	}
}

// ListUnusable implements UnusableLister.
//
// Returns exactly what ListObjects drops, so the two partition the bucket.
//
// Unlike the local backend this does NOT exclude ".part" keys. S3 does not
// stage writes and does not implement StagingInspector, so a ".part" object
// here is an ordinary committed object that #744's reserved suffix made
// unaddressable, and this is its only escape hatch.
func (b *S3Backend) ListUnusable(ctx context.Context, prefix string) ([]UnusableObject, error) {
	var objects []UnusableObject
	var continuationToken *string

	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			if isNoSuchBucketError(err) {
				return []UnusableObject{}, nil
			}
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key == nil {
				continue
			}
			key := strings.TrimPrefix(*obj.Key, b.prefix)
			reason := ValidateKey(key)
			if reason == nil {
				continue // ListObjects returns it
			}
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}
			// A zero-length key ending in a separator is a directory marker,
			// written by consoles and sync tools rather than by Arc, and holds
			// no data. This is a heuristic, not a guarantee: a trailing-slash
			// key CAN hold bytes on S3, which is why the size is part of the
			// test rather than the suffix alone.
			if size == 0 && strings.HasSuffix(key, "/") {
				continue
			}
			info := UnusableObject{Path: key, Size: size, Err: reason}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			objects = append(objects, info)
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return objects, nil
}
