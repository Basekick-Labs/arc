package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	gcs "google.golang.org/api/storage/v1"
)

// gcsScope is the OAuth scope every GCS credential is minted for: object
// reads, writes and deletes, and the bucket metadata read of the startup probe.
const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

// gcsDeleteConcurrency bounds DeleteBatch. The JSON API's batch endpoint takes
// a hand-built multipart body; single deletes side by side are simpler and
// carry the same per-key retry and error handling as Delete.
const gcsDeleteConcurrency = 16

// gcsChunkSize is the resumable-upload chunk: the upload buffers this much per
// write, and an object no larger goes in one request.
const gcsChunkSize = 16 << 20

// GCSBackend implements the Backend interface for Google Cloud Storage over
// the native JSON API (#1105). It is not the S3 backend pointed at GCS's XML
// interoperability endpoint: that needs HMAC keys, cannot use Application
// Default Credentials or GKE Workload Identity, and classes the bucket as S3.
//
// It uses the generated JSON API package, not cloud.google.com/go/storage.
// That client links gRPC, xDS and an OpenTelemetry metrics exporter whether or
// not they are used, which added 28.6 MB to the Arc binary against 3.6 MB for
// this package (measured on #1105). Every Arc user would carry that, most of
// them without ever configuring GCS.
//
// It implements ObjectCopier, so cluster mode can stage compaction outputs and
// copy them into place. It deliberately does NOT implement ObjectMover,
// FileAdopter, StagingInspector or AppendingBackend: those describe objects
// that ARE local files (#969, #990, #1158) or a store that stages partial
// writes (#744). A GCS upload is invisible until it is finalised, so there is
// never a partial to inspect, and an object store takes the copier branch.
type GCSBackend struct {
	svc      *gcs.Service
	bucket   string
	prefix   string // object-name prefix within the bucket (validated, with trailing /)
	endpoint string
	// auth keeps the credential configuration the backend was built with, so
	// the compaction subprocess can rebuild it the same way (see Credentials).
	auth GCSConfig
	// creds is the resolved identity. One identity serves the backend's own
	// requests and, through the token it mints, DuckDB's reads.
	creds  *auth.Credentials
	logger zerolog.Logger
	// bucketCheckedAt / bucketMissing cache the bucket probe that tells a
	// missing object from a missing bucket. See confirmObjectAbsent.
	bucketCheckedAt atomic.Int64
	bucketMissing   atomic.Bool
}

// GCSConfig holds Google Cloud Storage backend configuration.
type GCSConfig struct {
	// Bucket is the bucket name (required).
	Bucket string

	// Prefix is an object-name prefix within the bucket, applied to every key
	// the backend touches. Empty means the bucket root. Same contract as
	// S3Config.Prefix and AzureBlobConfig.Prefix.
	Prefix string

	// CredentialsFile is the path of a service-account key file. Only the
	// service-account type is accepted: an arbitrary credential configuration
	// from a file is the shape Google deprecated for loading unvalidated
	// external credentials.
	CredentialsFile string

	// CredentialsJSON is a service-account key's contents. It takes precedence
	// over CredentialsFile. The compaction subprocess receives the key this way
	// so that no key path or content ever appears on its command line.
	CredentialsJSON string

	// With neither of the above, the backend uses Application Default
	// Credentials: GOOGLE_APPLICATION_CREDENTIALS, the gcloud ADC file, or the
	// metadata server (GCE, GKE Workload Identity, Cloud Run).

	// Endpoint overrides the JSON API endpoint (Private Service Connect, an
	// emulator): a host ("https://storage-psc.p.googleapis.com", which gets
	// the JSON API path) or the full base URL. Empty means Google's default.
	// See GCSEndpointURL.
	Endpoint string

	// SkipProbe skips the bucket metadata read at construction. The
	// compaction subprocess sets it: its parent already ran the probe at
	// startup, and one Buckets.Get per compaction job would add a billed
	// request and a failure point to every job for no new information.
	SkipProbe bool
}

// NewGCSBackend creates a new Google Cloud Storage backend.
//
// It probes the bucket's metadata once. A bucket with hierarchical namespace
// is refused: listing and deletes behave differently there (folders are
// resources of their own, and deleting the last object leaves them behind),
// and Arc has not been verified against them. So is a requester-pays bucket,
// which DuckDB's query reads cannot bill. A retention policy, a default
// storage class other than STANDARD and soft delete are warnings: each makes
// compaction's continuous deletes fail or cost more, but none breaks a read.
// A missing bucket, or a principal
// that may not read bucket metadata, is a warning rather than a failure, as on
// Azure: object-only IAM roles are a legitimate deployment, and they cannot
// read the bucket's attributes.
func NewGCSBackend(cfg *GCSConfig, logger zerolog.Logger) (*GCSBackend, error) {
	return newGCSBackend(cfg, logger)
}

// newGCSBackend is NewGCSBackend with client options for tests: when any are
// passed they replace credential resolution, so an offline test can point the
// client (through cfg.Endpoint, the production path) at an httptest server
// with option.WithoutAuthentication.
func newGCSBackend(cfg *GCSConfig, logger zerolog.Logger, testOpts ...option.ClientOption) (*GCSBackend, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("GCS bucket name is required")
	}
	// A pure config check, before any credential or network work, so a
	// misspelled prefix is reported as itself (see NewAzureBlobBackend).
	prefix, err := ValidateObjectPrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}

	log := logger.With().Str("component", "gcs-storage").Logger()

	var creds *auth.Credentials
	opts := testOpts
	if len(opts) == 0 {
		creds, err = GCSCredentials(cfg)
		if err != nil {
			return nil, err
		}
		opts = []option.ClientOption{option.WithAuthCredentials(creds)}
	}
	if cfg.Endpoint != "" {
		endpoint, err := GCSEndpointURL(cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithEndpoint(endpoint))
	}

	if creds != nil && (cfg.CredentialsJSON != "" || cfg.CredentialsFile != "") {
		// A key file is only parsed for its JSON shape; a damaged private key
		// would pass and every request would then fail, with the probe below
		// reporting it as a warning. Minting one token signs with the key, so
		// this refuses it here. Not for Application Default Credentials: on a
		// node whose metadata server is briefly unreachable at boot that would
		// turn a transient into a failed start.
		tctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, terr := creds.Token(tctx)
		cancel()
		if terr != nil {
			return nil, fmt.Errorf("GCS service-account key cannot mint a token: %w", terr)
		}
	}

	svc, err := gcs.NewService(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	b := &GCSBackend{
		svc:      svc,
		bucket:   strings.TrimSpace(cfg.Bucket),
		prefix:   prefix,
		endpoint: cfg.Endpoint,
		auth: GCSConfig{
			CredentialsFile: cfg.CredentialsFile,
			CredentialsJSON: cfg.CredentialsJSON,
		},
		creds:  creds,
		logger: log,
	}

	if cfg.SkipProbe {
		return b, nil
	}
	if err := b.probeBucket(); err != nil {
		return nil, err
	}
	return b, nil
}

// GCSEndpointURL turns a configured endpoint into the base URL the JSON API
// client resolves every call against.
//
// The client uses the string as given and resolves "b/{bucket}/o" against it,
// while uploads go to the absolute "/upload/storage/v1/...". So a spelling
// without the trailing "/storage/v1/" left uploads working and sent every other
// call to the wrong path, where the 404 read as a missing bucket. And a URL
// the client cannot parse panicked inside the first call instead of failing
// construction. So: a scheme is required, a bare host gets the JSON API path,
// and the result always ends in a separator.
func GCSEndpointURL(endpoint string) (string, error) {
	e := strings.TrimSpace(endpoint)
	if !strings.Contains(e, "://") {
		e = "https://" + e
	}
	u, err := url.Parse(e)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("GCS endpoint %q is not a usable http(s) URL", endpoint)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/storage/v1/"
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String(), nil
}

// GCSCredentials resolves the identity cfg names: an explicit service-account
// key, or Application Default Credentials. The backend and DuckDB's query
// secret (internal/database) both resolve through it, so the identity that
// writes the bucket is the identity that reads it.
func GCSCredentials(cfg *GCSConfig) (*auth.Credentials, error) {
	opts := &credentials.DetectOptions{Scopes: []string{gcsScope}}
	key := []byte(cfg.CredentialsJSON)
	if len(key) == 0 && cfg.CredentialsFile != "" {
		data, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read GCS credentials file: %w", err)
		}
		key = data
	}
	if len(key) > 0 {
		// The error is not wrapped: a parse failure of a key file can quote
		// the file's contents.
		creds, err := credentials.NewCredentialsFromJSON(credentials.ServiceAccount, key, opts)
		if err != nil {
			return nil, fmt.Errorf("GCS credentials are not a usable service-account key")
		}
		return creds, nil
	}
	creds, err := credentials.DetectDefault(opts)
	if err != nil {
		return nil, fmt.Errorf("no GCS credentials: set a service-account key or configure Application Default Credentials: %w", err)
	}
	return creds, nil
}

// Every call is retried on a transient failure, including writes and deletes
// that carry no precondition. That is the same exposure the S3 and Azure SDKs
// already have for the same calls, and it is safe for what Arc does with
// them: a retried upload writes the same bytes again (most keys are unique
// per flush, #1282; the fixed ones, such as manifests, version-hint.text and
// the field-schema registry, are whole-object replacements), the client sends
// one idempotency token for every attempt of an upload, CopyObject replaces
// its destination by contract, and a retried delete that finds the object
// gone is a success.
//
// The generated client offers WithRetry on uploads only. Uploads pass
// gcsUploadRetry (bounded: the client's own loop runs until the context ends),
// which also covers each chunk of a resumable upload; every other call goes
// through gcsRetry. Both retry gcsTransient errors.

// gcsRetryAttempts and gcsRetryMaxBackoff bound gcsRetry: six attempts with
// backoff doubling from 100 ms, under 4 s of waiting in all.
var (
	gcsRetryAttempts   = 6
	gcsRetryMaxBackoff = 2 * time.Second
)

// gcsUploadRetries bounds the transient failures one upload absorbs across its
// requests: the opening request, or the single request of a small upload, and
// every chunk of a resumable one. Each resumable chunk is also bounded by the
// client's own 32 s per-chunk deadline.
var gcsUploadRetries = 12

// gcsUploadRetry is the retry predicate for one upload call.
func gcsUploadRetry() func(error) bool {
	var n atomic.Int32
	return func(err error) bool {
		return gcsTransient(err) && int(n.Add(1)) <= gcsUploadRetries
	}
}

// gcsRetry runs fn until it succeeds, fails with an error that is not
// transient, runs out of attempts, or ctx ends.
func gcsRetry(ctx context.Context, fn func() error) error {
	wait := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || attempt >= gcsRetryAttempts || !gcsTransient(err) {
			return err
		}
		// Full jitter, so concurrent deletes in a batch do not retry in step.
		t := time.NewTimer(time.Duration(rand.Int64N(int64(wait)) + 1))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		wait = min(wait*2, gcsRetryMaxBackoff)
	}
}

// gcsTransient reports an error worth retrying: a 408, 429 or 5xx from the
// API, or a transport failure that is not the caller's own cancellation. By
// status code and error type, never by text (#1238).
func gcsTransient(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch code := gcsHTTPCode(err); {
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests, code >= 500:
		return true
	case code != 0:
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// gcsHTTPCode is the HTTP status of a JSON API error, or 0.
func gcsHTTPCode(err error) int {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code
	}
	return 0
}

// probeBucket reads the bucket's attributes once at construction. See
// NewGCSBackend for what it refuses and what it only reports.
func (b *GCSBackend) probeBucket() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var attrs *gcs.Bucket
	err := gcsRetry(ctx, func() (err error) {
		attrs, err = b.svc.Buckets.Get(b.bucket).
			Fields("hierarchicalNamespace", "softDeletePolicy", "billing", "retentionPolicy", "storageClass").
			Context(ctx).Do()
		return err
	})
	switch {
	case gcsHTTPCode(err) == http.StatusNotFound:
		b.logger.Warn().Str("bucket", b.bucket).Msg("GCS bucket does not exist (create it before writing)")
		return nil
	case err != nil:
		b.logger.Warn().Err(err).Str("bucket", b.bucket).
			Msg("Could not read GCS bucket metadata (storage.buckets.get); hierarchical namespace, which Arc does not support, and soft delete were NOT checked")
		return nil
	}
	if attrs.HierarchicalNamespace != nil && attrs.HierarchicalNamespace.Enabled {
		return fmt.Errorf("GCS bucket %q has hierarchical namespace enabled, which Arc does not support; use a bucket without it", b.bucket)
	}
	if attrs.Billing != nil && attrs.Billing.RequesterPays {
		// Every request against a requester-pays bucket must name a project to
		// bill. DuckDB's query reads cannot (its GCS secret carries a token,
		// not a billing project), so ingest would work and every query fail.
		return fmt.Errorf("GCS bucket %q has requester pays enabled, which Arc does not support; disable it or use another bucket", b.bucket)
	}
	if rp := attrs.RetentionPolicy; rp != nil && rp.RetentionPeriod > 0 {
		ev := b.logger.Warn().Str("bucket", b.bucket).
			Dur("retention", time.Duration(rp.RetentionPeriod)*time.Second).
			Bool("locked", rp.IsLocked)
		if rp.IsLocked {
			ev.Msg("GCS bucket has a LOCKED retention policy: compaction, retention and DELETE cannot remove an object until it is older than the period, and the policy can never be shortened or removed")
		} else {
			ev.Msg("GCS bucket has a retention policy: compaction, retention and DELETE cannot remove an object until it is older than the period, so their deletes fail and the old objects stay readable")
		}
	}
	if class := attrs.StorageClass; class != "" && class != "STANDARD" {
		b.logger.Warn().Str("bucket", b.bucket).Str("storage_class", class).
			Msg("GCS bucket's default storage class is not STANDARD: compaction rewrites and deletes recent objects all day, which pays that class's minimum storage duration and retrieval fees on every one; use STANDARD for Arc's primary storage")
	}
	if attrs.SoftDeletePolicy != nil && attrs.SoftDeletePolicy.RetentionDurationSeconds > 0 {
		b.logger.Warn().Str("bucket", b.bucket).
			Dur("retention", time.Duration(attrs.SoftDeletePolicy.RetentionDurationSeconds)*time.Second).
			Msg("GCS bucket has soft delete on: compaction and retention delete objects continuously, and deleted bytes stay billed for the retention period")
	}
	b.logger.Info().Str("bucket", b.bucket).Msg("Successfully connected to Google Cloud Storage bucket")
	return nil
}

// prefixedKey validates a storage key and prepends the configured prefix to
// build the object name, enforcing the key-length limit after the prefix
// (#1121). The S3 and Azure twins are meant to stay diffable with it.
func (b *GCSBackend) prefixedKey(key string) (string, error) {
	return prefixObjectKey(b.prefix, key)
}

// prefixedListPrefix is prefixedKey for enumeration, where "" and a trailing
// separator are legitimate and cannot name an object.
func (b *GCSBackend) prefixedListPrefix(prefix string) (string, error) {
	if err := ValidateListPrefix(prefix); err != nil {
		return "", err
	}
	return b.prefix + prefix, nil
}

func gcsContentType(path string) string {
	if strings.HasSuffix(path, ".parquet") {
		return "application/vnd.apache.parquet"
	}
	return "application/octet-stream"
}

// Write writes data to Google Cloud Storage.
func (b *GCSBackend) Write(ctx context.Context, path string, data []byte) error {
	return b.WriteReader(ctx, path, bytes.NewReader(data), int64(len(data)))
}

// WriteReader writes data from a reader to Google Cloud Storage.
func (b *GCSBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	return b.writeReader(ctx, path, reader, size, nil)
}

// gcsUploadChunk is the chunk for an upload of size bytes: gcsChunkSize, or
// less for a small object, because the client allocates a buffer of the whole
// chunk for every upload and a 2 KB manifest should not cost 16 MiB. One byte
// over the size, rounded up to the client's 256 KiB granule: a chunk exactly
// the object's size never sees EOF and would open a resumable session where
// one request does.
func gcsUploadChunk(size int64) int {
	if size <= 0 || size >= gcsChunkSize {
		return gcsChunkSize
	}
	g := int64(googleapi.MinUploadChunkSize)
	return int(min((size+1+g-1)/g*g, gcsChunkSize))
}

// sizedReader passes r through and fails instead of reaching EOF when the
// bytes it delivered differ from want (when want > 0).
type sizedReader struct {
	r    io.Reader
	want int64
	n    int64
}

func (s *sizedReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.n += int64(n)
	if s.want > 0 && s.n > s.want {
		return n, fmt.Errorf("source supplied more than the %d bytes declared", s.want)
	}
	if err == io.EOF && s.want > 0 && s.n != s.want {
		return n, fmt.Errorf("source supplied %d bytes, expected %d", s.n, s.want)
	}
	return n, err
}

// writeReader is WriteReader with custom metadata on the object (nil: none).
//
// An object appears only when the upload is finalised, and the upload is
// finalised only by the request that carries the reader's EOF. So any failure
// of the reader, including a body that does not match the declared size (as on
// S3, whose PutObject refuses a body of the wrong length), surfaces before
// that request and abandons the upload: a failed write never commits a
// truncated object under a valid name.
//
// The upload is checksummed (CRC32C, computed while streaming; a resumable
// upload sends it with its final request, a single-request upload in the
// object resource), so GCS also refuses to finalise bytes corrupted in
// transit.
func (b *GCSBackend) writeReader(ctx context.Context, path string, reader io.Reader, size int64, meta map[string]string) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	start := time.Now()
	ct := gcsContentType(path)
	body := &sizedReader{r: reader, want: size}
	_, err = b.svc.Objects.Insert(b.bucket, &gcs.Object{Name: key, ContentType: ct, Metadata: meta}).
		Media(body, googleapi.ChunkSize(gcsUploadChunk(size)), googleapi.ContentType(ct), googleapi.EnableAutoChecksum()).
		Fields("name").Context(ctx).WithRetry(nil, gcsUploadRetry()).Do()
	if err != nil {
		recordStorageError(ctx, err)
		b.logger.Error().Err(err).Str("path", path).Int64("size", size).Msg("Failed to write to Google Cloud Storage")
		return fmt.Errorf("failed to write to Google Cloud Storage: %w", err)
	}

	metrics.Get().IncStorageWrites()
	if body.n > 0 {
		metrics.Get().IncStorageWriteBytes(body.n)
	}
	b.logger.Debug().Str("path", path).Int64("size", body.n).Str("bucket", b.bucket).
		Dur("duration", time.Since(start)).Msg("Wrote to Google Cloud Storage")
	return nil
}

// Read reads data from Google Cloud Storage.
func (b *GCSBackend) Read(ctx context.Context, path string) ([]byte, error) {
	var buf bytes.Buffer
	if err := b.ReadTo(ctx, path, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadTo reads data from Google Cloud Storage and writes it to writer.
func (b *GCSBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	return b.ReadToAt(ctx, path, writer, 0)
}

// ReadToAt reads data from Google Cloud Storage starting at offset and writes
// it to writer. offset=0 reads the whole object without a Range header.
func (b *GCSBackend) ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("negative offset %d", offset)
	}
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	var resp *http.Response
	err = gcsRetry(ctx, func() (err error) {
		call := b.svc.Objects.Get(b.bucket, key).Context(ctx)
		if offset > 0 {
			call.Header().Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		resp, err = call.Download()
		return err
	})
	if err != nil {
		recordStorageError(ctx, err)
		return b.objectError(ctx, "failed to read from Google Cloud Storage", err)
	}
	defer resp.Body.Close()

	n, err := io.Copy(writer, resp.Body)
	// Count bytes delivered even when the copy fails mid-stream: partial
	// transfers are real network egress.
	if n > 0 {
		metrics.Get().IncStorageReadBytes(n)
	}
	if err != nil {
		recordStorageError(ctx, err)
		return fmt.Errorf("failed to copy GCS object: %w", err)
	}
	metrics.Get().IncStorageReads()
	return nil
}

// StatFile returns the byte size of the object at path, or -1 if not found.
func (b *GCSBackend) StatFile(ctx context.Context, path string) (int64, error) {
	size, _, err := b.ObjectSHA(ctx, path)
	return size, err
}

// Exists checks if an object exists in Google Cloud Storage.
func (b *GCSBackend) Exists(ctx context.Context, path string) (bool, error) {
	size, err := b.StatFile(ctx, path)
	if err != nil {
		return false, err
	}
	return size >= 0, nil
}

// deleteObject deletes one object by its full name. An object that is already
// gone, or whose bucket is, is what a delete wants: GCS answers both with a
// 404, and either way nothing remains to delete.
func (b *GCSBackend) deleteObject(ctx context.Context, name string) error {
	err := gcsRetry(ctx, func() error { return b.svc.Objects.Delete(b.bucket, name).Context(ctx).Do() })
	if err != nil && gcsHTTPCode(err) != http.StatusNotFound {
		return err
	}
	return nil
}

// Delete deletes an object from Google Cloud Storage.
func (b *GCSBackend) Delete(ctx context.Context, path string) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	if err := b.deleteObject(ctx, key); err != nil {
		return fmt.Errorf("failed to delete from Google Cloud Storage: %w", err)
	}
	b.logger.Debug().Str("path", path).Msg("Deleted from Google Cloud Storage")
	return nil
}

// DeleteBatch deletes many objects, gcsDeleteConcurrency at a time.
//
// Best effort per key, like S3 and Azure: callers such as compaction and the
// backup manager submit whole batches with no per-file fallback, so one bad
// key must not leave every other file undeleted. Keys the contract refuses and
// deletes that fail are collected and returned together.
func (b *GCSBackend) DeleteBatch(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	usable, errs := partitionValidKeys(b.prefix, paths)

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, gcsDeleteConcurrency)
	for _, name := range usable {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(name string) {
			defer func() { <-sem; wg.Done() }()
			err := b.deleteObject(ctx, name)
			if err == nil {
				return
			}
			// Reported on the UNPREFIXED spelling the caller passed (#1111).
			key := strings.TrimPrefix(name, b.prefix)
			b.logger.Warn().Err(err).Str("key", key).Msg("GCS batch: individual delete failed")
			mu.Lock()
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
			mu.Unlock()
		}(name)
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("GCS batch delete interrupted (%d object(s) failed before): %w", len(errs), errors.Join(append([]error{err}, errs...)...))
	}
	if len(errs) > 0 {
		return fmt.Errorf("GCS batch delete: %d object(s) failed: %w", len(errs), errors.Join(errs...))
	}
	b.logger.Debug().Int("count", len(paths)).Msg("Batch deleted from Google Cloud Storage")
	return nil
}

// listPages walks the listing of the full (already prefixed) prefix, with an
// optional delimiter, calling fn per page until it returns false. A missing
// bucket is ErrStoreNotFound.
func (b *GCSBackend) listPages(ctx context.Context, fullPrefix, delimiter string, fn func(*gcs.Objects) bool) error {
	call := b.svc.Objects.List(b.bucket).Prefix(fullPrefix).Projection("noAcl").
		Fields("items(name,size,updated)", "prefixes", "nextPageToken")
	if delimiter != "" {
		call = call.Delimiter(delimiter)
	}
	// Paged by hand rather than with Pages, so each page is retried on its
	// own instead of a transient error restarting the whole walk.
	var err error
	for token := ""; ; {
		var page *gcs.Objects
		err = gcsRetry(ctx, func() (err error) {
			page, err = call.PageToken(token).Context(ctx).Do()
			return err
		})
		if err != nil {
			break
		}
		if !fn(page) || page.NextPageToken == "" {
			return nil
		}
		token = page.NextPageToken
	}
	switch {
	case gcsHTTPCode(err) == http.StatusNotFound:
		// A listing addresses the bucket, so its 404 is the bucket's.
		return fmt.Errorf("failed to list GCS objects: %w: %w", err, ErrStoreNotFound)
	default:
		return fmt.Errorf("failed to list GCS objects: %w", err)
	}
}

// eachObject calls fn for every object under the full listing prefix, with its
// key relative to the backend prefix, until fn returns false.
func (b *GCSBackend) eachObject(ctx context.Context, fullPrefix string, fn func(key string, info ObjectInfo) bool) error {
	return b.listPages(ctx, fullPrefix, "", func(page *gcs.Objects) bool {
		for _, o := range page.Items {
			key := strings.TrimPrefix(o.Name, b.prefix)
			info := ObjectInfo{Path: key, Size: int64(o.Size)}
			if t, err := time.Parse(time.RFC3339Nano, o.Updated); err == nil {
				info.LastModified = t
			}
			if !fn(key, info) {
				return false
			}
		}
		return true
	})
}

// List lists objects with the given prefix. Like every listing here it never
// returns a key this backend would refuse (#743, #1121): see S3Backend.List.
func (b *GCSBackend) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var keys []string
	err = b.eachObject(ctx, fullPrefix, func(key string, _ ObjectInfo) bool {
		if _, err := b.prefixedKey(key); err == nil {
			keys = append(keys, key)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// ListObjects lists objects with their metadata at a prefix.
func (b *GCSBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var objects []ObjectInfo
	err = b.eachObject(ctx, fullPrefix, func(key string, info ObjectInfo) bool {
		if _, err := b.prefixedKey(key); err == nil {
			objects = append(objects, info)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// HasObjectsUnderPrefix implements PrefixProber: the first object whose name
// the contract accepts answers true. See S3Backend.HasObjectsUnderPrefix.
func (b *GCSBackend) HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return false, err
	}
	found := false
	err = b.eachObject(ctx, fullPrefix, func(key string, _ ObjectInfo) bool {
		_, kerr := b.prefixedKey(key)
		found = kerr == nil
		return !found
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// ListUnusable implements UnusableLister: exactly what ListObjects drops, so
// the two partition the bucket. ".part" objects are reported, as on S3 and
// Azure: GCS does not stage writes, so such an object is an ordinary committed
// one that #744's reserved suffix made unaddressable.
func (b *GCSBackend) ListUnusable(ctx context.Context, prefix string) ([]UnusableObject, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var objects []UnusableObject
	err = b.eachObject(ctx, fullPrefix, func(key string, info ObjectInfo) bool {
		_, reason := b.prefixedKey(key)
		if reason == nil {
			return true // ListObjects returns it
		}
		// A zero-length name ending in a separator is a folder placeholder
		// (the Cloud Console's "Create folder" writes one). Heuristic, as on
		// S3: the size is part of the test, not the suffix alone.
		if info.Size == 0 && strings.HasSuffix(key, "/") {
			return true
		}
		objects = append(objects, UnusableObject{Path: key, Size: info.Size, LastModified: info.LastModified, Err: reason})
		return true
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// ListDirectories lists immediate subdirectories at a prefix, from the
// listing's delimiter prefixes.
func (b *GCSBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var dirs []string
	err = b.listPages(ctx, fullPrefix, "/", func(page *gcs.Objects) bool {
		for _, p := range page.Prefixes {
			// Trim fullPrefix, not the caller's prefix: the names come back
			// with the backend prefix attached (see S3Backend.ListDirectories).
			dir := strings.TrimSuffix(strings.TrimPrefix(p, fullPrefix), "/")
			if dir != "" && !strings.HasPrefix(dir, ".") {
				dirs = append(dirs, dir)
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return dirs, nil
}

// Close closes the GCS backend (no-op: the service holds no connections of
// its own beyond the shared HTTP transport).
func (b *GCSBackend) Close() error {
	b.logger.Info().Msg("Google Cloud Storage backend closed")
	return nil
}

// GetBucket returns the bucket name.
func (b *GCSBackend) GetBucket() string { return b.bucket }

// GetPrefix returns the object-name prefix, with its trailing separator
// (empty string if none configured). The query path reads it duck-typed to
// parse prefixed database and measurement names (#1108).
func (b *GCSBackend) GetPrefix() string { return b.prefix }

// GetEndpoint returns the configured endpoint override ("" for Google's).
func (b *GCSBackend) GetEndpoint() string { return b.endpoint }

// Credentials returns the credential configuration the backend was built with
// (the key file path and key contents, nothing else), so the compaction
// subprocess will be able to rebuild it with the SAME identity (wired in the
// primary-storage stage of #1105; subprocess.go refuses gcs until then). Empty fields mean
// Application Default Credentials, which the subprocess inherits through its
// environment and the metadata server.
//
// CredentialsJSON is a plaintext secret: callers MUST NOT log, store or
// transmit it anywhere but the subprocess's job input.
func (b *GCSBackend) Credentials() GCSConfig { return b.auth }

// Type returns the storage type identifier.
func (b *GCSBackend) Type() string { return "gcs" }

// ConfigJSON returns the configuration as JSON for subprocess recreation.
// Every field must be parsed by the compaction subprocess once it accepts gcs
// (the primary-storage stage of #1105); the prefix above
// all, or the subprocess reroots at the bucket root (see
// AzureBlobBackend.ConfigJSON). It carries no credentials and must not start
// to.
func (b *GCSBackend) ConfigJSON() string {
	data, _ := json.Marshal(map[string]interface{}{
		"bucket":   b.bucket,
		"prefix":   b.prefix,
		"endpoint": b.endpoint,
	})
	return string(data)
}

// How long one bucket-probe answer is trusted; see S3's bucketConfirmTTL.
const (
	gcsBucketConfirmTTL = time.Minute
	gcsBucketMissingTTL = 5 * time.Second
)

// confirmObjectAbsent tells a missing object from a missing bucket. Every
// object call (read, metadata, delete) answers both with a 404, and the media
// download's 404 carries no reason at all (verified against real GCS on
// #1105). Only the bucket's own metadata and a listing address the bucket.
//
// A bucket the probe finds missing is ErrStoreNotFound, which is what a
// listing of it reports too. Any other answer leaves the object absent: the
// bucket exists, or the principal may not read bucket metadata (403, normal
// with object-only IAM roles), or the probe failed. 200, 403 and missing are
// cached; a failure is not, so the next miss asks again. The same rule as
// S3Backend.confirmObjectAbsent (#1242).
func (b *GCSBackend) confirmObjectAbsent(ctx context.Context) error {
	if at := b.bucketCheckedAt.Load(); at != 0 {
		ttl := gcsBucketConfirmTTL
		if b.bucketMissing.Load() {
			ttl = gcsBucketMissingTTL
		}
		if time.Since(time.Unix(0, at)) < ttl {
			if b.bucketMissing.Load() {
				return fmt.Errorf("bucket %s: %w", b.bucket, ErrStoreNotFound)
			}
			return nil
		}
	}
	err := gcsRetry(ctx, func() error {
		_, err := b.svc.Buckets.Get(b.bucket).Fields("name").Context(ctx).Do()
		return err
	})
	code := gcsHTTPCode(err)
	missing := code == http.StatusNotFound
	if err == nil || missing || code == http.StatusForbidden {
		b.bucketMissing.Store(missing)
		b.bucketCheckedAt.Store(time.Now().UnixNano())
	}
	if missing {
		return fmt.Errorf("bucket %s: %w: %w", b.bucket, err, ErrStoreNotFound)
	}
	return nil
}

// objectError wraps a failed object call. A 404 is ErrStoreNotFound when the
// bucket is gone (confirmObjectAbsent) and ErrObjectNotFound otherwise, which
// is how IsNotFound recognises a missing GCS object: by the sentinel this
// wraps, not by the API error's code, because the same 404 from a listing
// means the bucket.
func (b *GCSBackend) objectError(ctx context.Context, what string, err error) error {
	if gcsHTTPCode(err) == http.StatusNotFound {
		if storeErr := b.confirmObjectAbsent(ctx); storeErr != nil {
			return fmt.Errorf("%s: %w", what, storeErr)
		}
		return fmt.Errorf("%s: %w: %w", what, ErrObjectNotFound, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}
