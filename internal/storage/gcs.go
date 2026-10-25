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
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/storage"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// gcsScope is the OAuth scope every GCS credential is minted for: object
// reads, writes and deletes, and the bucket metadata read of the startup probe.
const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

// gcsDeleteConcurrency bounds DeleteBatch. The JSON API has no batch delete
// the Go client exposes, so a batch is single deletes run side by side.
const gcsDeleteConcurrency = 16

// GCSBackend implements the Backend interface for Google Cloud Storage over
// the native JSON API (#1105). It is not the S3 backend pointed at GCS's XML
// interoperability endpoint: that needs HMAC keys, cannot use Application
// Default Credentials or GKE Workload Identity, and classes the bucket as S3.
//
// It implements ObjectCopier, so cluster mode can stage compaction outputs and
// copy them into place. It deliberately does NOT implement ObjectMover,
// FileAdopter, StagingInspector or AppendingBackend: those describe objects
// that ARE local files (#969, #990, #1158) or a store that stages partial
// writes (#744). A GCS upload is invisible until it is finalised, so there is
// never a partial to inspect, and an object store takes the copier branch.
type GCSBackend struct {
	client   *storage.Client
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
	// emulator). Empty means Google's default.
	Endpoint string
}

// NewGCSBackend creates a new Google Cloud Storage backend.
//
// It probes the bucket's metadata once. A bucket with hierarchical namespace
// is refused: listing and deletes behave differently there (folders are
// resources of their own, and deleting the last object leaves them behind),
// and Arc has not been verified against them. A missing bucket, or a principal
// that may not read bucket metadata, is a warning rather than a failure, as on
// Azure: object-only IAM roles are a legitimate deployment, and they cannot
// read the bucket's attributes.
func NewGCSBackend(cfg *GCSConfig, logger zerolog.Logger) (*GCSBackend, error) {
	return newGCSBackend(cfg, logger)
}

// newGCSBackend is NewGCSBackend with client options for tests: when any are
// passed they replace credential resolution entirely, so an offline test can
// point the client at an httptest server with option.WithoutAuthentication.
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
		creds, err = gcsCredentials(cfg)
		if err != nil {
			return nil, err
		}
		opts = []option.ClientOption{option.WithAuthCredentials(creds)}
		if cfg.Endpoint != "" {
			opts = append(opts, option.WithEndpoint(cfg.Endpoint))
		}
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	// Retry every operation, not only the ones the client deems idempotent.
	// The default leaves precondition-less writes and deletes unretried, and
	// Arc sets no preconditions. That is safe here: output keys are unique per
	// flush and per compaction (#1282), CopyObject replaces its destination by
	// contract, and Delete treats "already gone" as success.
	client.SetRetry(storage.WithPolicy(storage.RetryAlways))

	b := &GCSBackend{
		client:   client,
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

	if err := b.probeBucket(); err != nil {
		client.Close()
		return nil, err
	}
	return b, nil
}

// gcsCredentials resolves the identity cfg names: an explicit service-account
// key, or Application Default Credentials.
func gcsCredentials(cfg *GCSConfig) (*auth.Credentials, error) {
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

// probeBucket reads the bucket's attributes once at construction. See
// NewGCSBackend for what it refuses and what it only reports.
func (b *GCSBackend) probeBucket() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	attrs, err := b.client.Bucket(b.bucket).Attrs(ctx)
	switch {
	case errors.Is(err, storage.ErrBucketNotExist):
		b.logger.Warn().Str("bucket", b.bucket).Msg("GCS bucket does not exist (create it before writing)")
		return nil
	case err != nil:
		b.logger.Warn().Err(err).Str("bucket", b.bucket).
			Msg("Could not read GCS bucket metadata; hierarchical-namespace and soft-delete settings were not checked")
		return nil
	}
	if attrs.HierarchicalNamespace != nil && attrs.HierarchicalNamespace.Enabled {
		return fmt.Errorf("GCS bucket %q has hierarchical namespace enabled, which Arc does not support; use a bucket without it", b.bucket)
	}
	if attrs.SoftDeletePolicy != nil && attrs.SoftDeletePolicy.RetentionDuration > 0 {
		b.logger.Warn().Str("bucket", b.bucket).Dur("retention", attrs.SoftDeletePolicy.RetentionDuration).
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

func (b *GCSBackend) object(name string) *storage.ObjectHandle {
	return b.client.Bucket(b.bucket).Object(name)
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

// writeReader is WriteReader with custom metadata on the object (nil: none).
//
// An object appears only when the upload is finalised, which Writer.Close
// does. So the one thing this must never do is call Close on an upload that
// went wrong: Close after a failed source reader commits whatever arrived, a
// truncated object under a valid name. Every failure cancels the writer's
// context instead, which abandons the upload. A declared size that the reader
// does not match is such a failure, as it is on S3, whose PutObject refuses a
// body of the wrong length.
//
// The client checksums the upload (CRC32C) and, with the default chunking,
// sends the checksum with the final request, so GCS refuses to finalise bytes
// that were corrupted in transit.
func (b *GCSBackend) writeReader(ctx context.Context, path string, reader io.Reader, size int64, meta map[string]string) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	start := time.Now()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := b.object(key).NewWriter(wctx)
	w.ContentType = gcsContentType(path)
	w.Metadata = meta

	n, err := io.Copy(w, reader)
	if err == nil && size > 0 && n != size {
		err = fmt.Errorf("source supplied %d bytes, expected %d", n, size)
	}
	if err == nil {
		err = w.Close()
	} else {
		cancel()
		_ = w.Close()
	}
	if err != nil {
		recordStorageError(ctx, err)
		b.logger.Error().Err(err).Str("path", path).Int64("size", size).Msg("Failed to write to Google Cloud Storage")
		return fmt.Errorf("failed to write to Google Cloud Storage: %w", err)
	}

	metrics.Get().IncStorageWrites()
	if n > 0 {
		metrics.Get().IncStorageWriteBytes(n)
	}
	b.logger.Debug().Str("path", path).Int64("size", n).Str("bucket", b.bucket).
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
// it to writer. offset=0 reads the whole object.
func (b *GCSBackend) ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("negative offset %d", offset)
	}
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	r, err := b.object(key).NewRangeReader(ctx, offset, -1)
	if err != nil {
		recordStorageError(ctx, err)
		return b.objectError(ctx, "failed to read from Google Cloud Storage", err)
	}
	defer r.Close()

	n, err := io.Copy(writer, r)
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

// Delete deletes an object from Google Cloud Storage. An object that is
// already gone, or whose bucket is, is what Delete wants: GCS answers both
// with the same 404, and either way nothing remains to delete.
func (b *GCSBackend) Delete(ctx context.Context, path string) error {
	key, err := b.prefixedKey(path)
	if err != nil {
		return err
	}
	if err := b.object(key).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
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
			err := b.object(name).Delete(ctx)
			if err == nil || errors.Is(err, storage.ErrObjectNotExist) {
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
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("GCS batch delete: %d object(s) failed: %w", len(errs), errors.Join(errs...))
	}
	b.logger.Debug().Int("count", len(paths)).Msg("Batch deleted from Google Cloud Storage")
	return nil
}

// eachObject calls fn for every object under the full (already prefixed)
// listing prefix, with its key relative to the backend prefix, until fn
// returns false. A missing bucket is ErrStoreNotFound.
func (b *GCSBackend) eachObject(ctx context.Context, fullPrefix string, fn func(key string, attrs *storage.ObjectAttrs) bool) error {
	q := &storage.Query{Prefix: fullPrefix, Projection: storage.ProjectionNoACL}
	if err := q.SetAttrSelection([]string{"Name", "Size", "Updated"}); err != nil {
		return err
	}
	it := b.client.Bucket(b.bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return gcsListError("failed to list GCS objects", err)
		}
		if !fn(strings.TrimPrefix(attrs.Name, b.prefix), attrs) {
			return nil
		}
	}
}

// List lists objects with the given prefix. Like every listing here it never
// returns a key this backend would refuse (#743, #1121): see S3Backend.List.
func (b *GCSBackend) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix, err := b.prefixedListPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var keys []string
	err = b.eachObject(ctx, fullPrefix, func(key string, _ *storage.ObjectAttrs) bool {
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
	err = b.eachObject(ctx, fullPrefix, func(key string, attrs *storage.ObjectAttrs) bool {
		if _, err := b.prefixedKey(key); err == nil {
			objects = append(objects, ObjectInfo{Path: key, Size: attrs.Size, LastModified: attrs.Updated})
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
	err = b.eachObject(ctx, fullPrefix, func(key string, _ *storage.ObjectAttrs) bool {
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
	err = b.eachObject(ctx, fullPrefix, func(key string, attrs *storage.ObjectAttrs) bool {
		_, reason := b.prefixedKey(key)
		if reason == nil {
			return true // ListObjects returns it
		}
		// A zero-length name ending in a separator is a folder placeholder
		// (the Cloud Console's "Create folder" writes one). Heuristic, as on
		// S3: the size is part of the test, not the suffix alone.
		if attrs.Size == 0 && strings.HasSuffix(key, "/") {
			return true
		}
		objects = append(objects, UnusableObject{Path: key, Size: attrs.Size, LastModified: attrs.Updated, Err: reason})
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
	q := &storage.Query{Prefix: fullPrefix, Delimiter: "/", Projection: storage.ProjectionNoACL}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, err
	}
	var dirs []string
	it := b.client.Bucket(b.bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return dirs, nil
		}
		if err != nil {
			return nil, gcsListError("failed to list GCS directories", err)
		}
		if attrs.Prefix == "" {
			continue // an object directly under prefix, not a directory
		}
		// Trim fullPrefix, not the caller's prefix: the names come back with
		// the backend prefix attached (see S3Backend.ListDirectories).
		dir := strings.TrimSuffix(strings.TrimPrefix(attrs.Prefix, fullPrefix), "/")
		if dir != "" && !strings.HasPrefix(dir, ".") {
			dirs = append(dirs, dir)
		}
	}
}

// Close closes the GCS client.
func (b *GCSBackend) Close() error {
	b.logger.Info().Msg("Google Cloud Storage backend closed")
	return b.client.Close()
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
// subprocess can rebuild it with the SAME identity. Empty fields mean
// Application Default Credentials, which the subprocess inherits through its
// environment and the metadata server.
//
// CredentialsJSON is a plaintext secret: callers MUST NOT log, store or
// transmit it anywhere but the subprocess's job input.
func (b *GCSBackend) Credentials() GCSConfig { return b.auth }

// Type returns the storage type identifier.
func (b *GCSBackend) Type() string { return "gcs" }

// ConfigJSON returns the configuration as JSON for subprocess recreation.
// Every field must be parsed by the compaction subprocess; the prefix above
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
// object call (read, metadata, delete) answers both with ErrObjectNotExist:
// verified against real GCS on #1105, where even the JSON API's "The
// specified bucket does not exist" is classed as an object error by the
// client. Only the bucket's own metadata and a listing say ErrBucketNotExist.
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
	_, err := b.client.Bucket(b.bucket).Attrs(ctx)
	missing := errors.Is(err, storage.ErrBucketNotExist)
	var gerr *googleapi.Error
	conclusive := err == nil || missing || (errors.As(err, &gerr) && gerr.Code == http.StatusForbidden)
	if conclusive {
		b.bucketMissing.Store(missing)
		b.bucketCheckedAt.Store(time.Now().UnixNano())
	}
	if missing {
		// Wraps the bucket's answer, never the object's: the object's
		// ErrObjectNotExist would read as a missing object to IsNotFound.
		return fmt.Errorf("bucket %s: %w: %w", b.bucket, err, ErrStoreNotFound)
	}
	return nil
}

// objectError wraps a failed object call. A not-found becomes
// ErrStoreNotFound when the bucket is gone (confirmObjectAbsent), and stays a
// missing object otherwise.
func (b *GCSBackend) objectError(ctx context.Context, what string, err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) {
		if storeErr := b.confirmObjectAbsent(ctx); storeErr != nil {
			return fmt.Errorf("%s: %w", what, storeErr)
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}

// gcsListError wraps a listing failure, tagging a missing bucket with
// ErrStoreNotFound while keeping the client's error in the chain.
func gcsListError(what string, err error) error {
	if errors.Is(err, storage.ErrBucketNotExist) {
		return fmt.Errorf("%s: %w: %w", what, err, ErrStoreNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// isGCSNotFoundError reports the GCS client saying an object does not exist.
// By sentinel, never by text (#1238). A missing bucket never reaches a caller
// carrying this sentinel: objectError replaces it with ErrStoreNotFound.
func isGCSNotFoundError(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}
