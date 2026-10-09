package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"time"
)

// ErrResumeNotSupported is returned by AppendingBackend.AppendReader on
// backends that do not support append writes (S3, Azure Blob Storage).
// Callers should delete any partial file and retry from byte zero.
var ErrResumeNotSupported = errors.New("storage: resume not supported by this backend")

// ErrStoreNotFound reports that the bucket or container itself does not
// exist — not that a key is missing inside it.
//
// It exists because "the store is not there" and "the store is there and
// empty" mean different things to different callers, and only the caller
// knows which. A fresh deployment reading before its first write wants
// "no data yet": most S3-compatible stores create the bucket on the first
// authenticated write, so the state heals itself and a 500 on the read is
// pure noise (#945). But a caller that DECIDES from a listing — drops a tier
// from a query, retires a metadata row, reports a backup complete — must
// never read a missing store as "verified empty", because that turns a
// misconfigured bucket name into a silently wrong answer.
//
// So the backends report it and the few read paths that want leniency opt in
// with IsStoreNotFound. Everything else keeps failing as it always did. The
// listing methods wrap it alongside the SDK's own error, so both chains stay
// inspectable.
var ErrStoreNotFound = errors.New("storage: bucket or container does not exist")

// IsStoreNotFound reports whether err is a backend saying its bucket or
// container does not exist. Use it instead of matching the SDKs' shapes:
// S3 answers NoSuchBucket, Azure answers a ContainerNotFound ResponseError.
func IsStoreNotFound(err error) bool {
	return errors.Is(err, ErrStoreNotFound)
}

// ErrObjectNotFound reports that a key names no object.
//
// It exists so a caller can tell "that object is not there" apart from "the
// store would not answer" with ONE round trip. Without it the only portable
// way to ask was Exists-then-Read, which doubles the request count on a path
// that runs once per object — the backup listing reads a manifest per backup,
// and on a remote destination with a few hundred backups the extra HEAD per
// backup is what exhausts an API handler's budget on a perfectly healthy
// store.
//
// Each backend wraps it from its own not-found shape; IsNotFound is the
// predicate to use rather than string-matching, because the shapes differ
// (an *fs.PathError locally, NoSuchKey on S3, a 404 ResponseError on Azure).
var ErrObjectNotFound = errors.New("file not found")

// IsNotFound reports whether err is a backend saying the key names no object.
//
// Tolerant on purpose: it accepts the wrapped sentinel, a wrapped fs
// not-exist, and the per-backend shapes the two SDKs produce, because not
// every error path in this package has been routed through the sentinel and a
// false "no" here would turn an absent object into a hard failure.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrObjectNotFound) || errors.Is(err, fs.ErrNotExist) {
		return true
	}
	return isNotFoundError(err) || isAzureNotFoundError(err)
}

// Backend defines the interface for storage backends (local, S3, MinIO)
type Backend interface {
	// Write writes data to the specified path
	Write(ctx context.Context, path string, data []byte) error

	// WriteReader writes data from a reader to the specified path (for large files)
	WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error

	// Read reads data from the specified path
	Read(ctx context.Context, path string) ([]byte, error)

	// ReadTo reads data from the specified path and writes it to the writer
	ReadTo(ctx context.Context, path string, writer io.Writer) error

	// ReadToAt reads data from path starting at the given byte offset and writes
	// to writer. offset=0 starts at the beginning (equivalent to ReadTo).
	// Returns an error if offset is negative or >= file size.
	ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error

	// StatFile returns the byte size of the committed file at path.
	// Returns -1 (and nil error) if the committed file does not exist. Staged
	// partials are exposed separately through StagingInspector.
	// Returns a non-nil error only for unexpected backend failures.
	StatFile(ctx context.Context, path string) (int64, error)

	// List lists all objects with the given prefix
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete deletes the object at the specified path
	Delete(ctx context.Context, path string) error

	// Exists checks if an object exists at the specified path
	Exists(ctx context.Context, path string) (bool, error)

	// Close closes any resources held by the backend
	Close() error

	// Type returns the storage type identifier ("local", "s3", etc.)
	// Used for subprocess serialization
	Type() string

	// ConfigJSON returns the configuration as JSON for subprocess recreation
	// Used for subprocess serialization
	ConfigJSON() string
}

// AppendingBackend is an optional extension of Backend for backends that
// support appending bytes to an existing file. Callers type-assert Backend to
// AppendingBackend before calling AppendReader; if the assertion fails, the
// backend does not support resumable writes and the caller should fall back to
// a full re-fetch.
//
// Only local-SSD backends implement this. S3 and Azure Blob Storage do not
// support append on block objects and return ErrResumeNotSupported instead.
type AppendingBackend interface {
	Backend
	// AppendReader appends bytes from reader to the existing file at path.
	// appendSize is the number of bytes expected from reader (informational;
	// implementations may ignore it). Returns ErrResumeNotSupported if the
	// backend cannot append.
	AppendReader(ctx context.Context, path string, reader io.Reader, appendSize int64) error
}

// StagingInspector exposes a backend's in-progress write staging area.
//
// Backends that stage a write before committing it (only LocalBackend does)
// leave a partial file behind when a transfer is interrupted, which is what
// lets the cluster puller and the edge-sync receiver resume from the last
// committed byte instead of refetching. Those callers need to size, hash and
// discard that partial.
//
// They used to do it by appending ".part" to the key and calling StatFile,
// ReadTo and Delete, which reached around the abstraction and, worse, put the
// staging file in the same namespace as committed objects: the staging file of
// key "x" WAS the committed object "x.part", so writing one destroyed the
// other (#744). These methods name the staging area explicitly so the key
// namespace can reserve it.
//
// A backend that does not stage does not implement this. Callers type-assert
// and treat a failed assertion as "there is never a partial", which is exactly
// true for S3 and Azure.
type StagingInspector interface {
	Backend

	// StagedSize returns the byte size of the staged partial for key, or -1
	// (with a nil error) when there is none.
	StagedSize(ctx context.Context, key string) (int64, error)

	// ReadStaged writes the staged partial for key to writer.
	ReadStaged(ctx context.Context, key string, writer io.Writer) error

	// DeleteStaged removes the staged partial for key. Removing one that does
	// not exist is not an error.
	DeleteStaged(ctx context.Context, key string) error

	// ListStaged returns metadata for staged partials whose key has the given
	// prefix, so a caller can reclaim abandoned ones. Staged partials are
	// invisible to List by design, so this is the way to find them. Every key
	// it returns is accepted by DeleteStaged: a partial whose key DeleteStaged
	// refuses is omitted and surfaces through UnusableLister instead (#772).
	ListStaged(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// DirectoryLister lists immediate subdirectories at a prefix.
// This is useful for SHOW DATABASES/TABLES commands.
type DirectoryLister interface {
	ListDirectories(ctx context.Context, prefix string) ([]string, error)
}

// BatchDeleter supports efficient batch deletion of multiple objects.
// Implementations should handle batching internally (e.g., S3 supports up to 1000 objects per batch).
type BatchDeleter interface {
	DeleteBatch(ctx context.Context, paths []string) error
}

// ErrAdoptUnsupported reports that AdoptFile cannot move the named local file
// into the backend: the rename is impossible for this pair of paths (most
// often because they are on different filesystems), or the source filesystem
// will not let Arc normalize the file mode. The caller must fall back to
// WriteReader. The underlying cause is wrapped for logging.
//
// Nothing has been MOVED: the source file is still at localPath with its
// contents intact, which is what makes a fallback copy safe. Its permission
// bits may have been set to 0600 before the attempt failed, so a caller that
// cares about the source mode must not assume it is unchanged.
var ErrAdoptUnsupported = errors.New("storage: local file cannot be adopted in place")

// FileAdopter is implemented by a backend whose objects ARE plain local files,
// so a file the caller has already written on the same filesystem can be moved
// into place instead of copied through WriteReader.
//
// On success the source file at localPath is CONSUMED: it no longer exists and
// the caller must not read it afterwards. That is the point of the interface
// and the reason it is opt-in rather than part of Backend. A backend that
// copies bytes must not implement it, and a caller that still needs the file
// must not use it.
//
// ErrAdoptUnsupported means "this pair of paths cannot be adopted, copy
// instead" and is not a failure. Any other error is a real one.
type FileAdopter interface {
	AdoptFile(ctx context.Context, path, localPath string) error
}

// ObjectInfo provides metadata about a storage object.
type ObjectInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
}

// ObjectLister lists objects with their metadata.
// This is useful for retention policies that need to check file ages.
type ObjectLister interface {
	ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// PrefixProber answers "is there at least one listable object under this
// prefix" without listing the prefix.
//
// ListObjects is the wrong tool for that question on a large prefix: it walks
// every file (local) or pages through every key (S3, Azure) and returns them
// all, when the caller only needs to know whether the first one exists. The
// backup API asks it for up to 256 database names inside one request timeout,
// and a database can hold millions of files, so the probe has to stop at the
// first hit. Implementations apply the same prefix contract (ValidateListPrefix)
// and the same visibility rule as ListObjects: an object the listing would
// hide (a dot-prefixed name, a key the contract refuses, a staging partial) is
// not a hit, so "false" from the probe and "empty" from ListObjects agree. A
// prefix that names nothing at all is false with a nil error.
//
// Optional: callers type-assert and fall back to ListObjects when the backend
// does not implement it (test fakes that embed the Backend interface, for
// instance). LocalBackend, S3Backend and AzureBlobBackend all implement it.
type PrefixProber interface {
	HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error)
}

// UnusableObject is an object that exists in the store but that no listing
// returns, so nothing driven by a listing can see it.
type UnusableObject struct {
	Path         string
	Size         int64
	LastModified time.Time
	// Err is why the object is unlistable. It wraps ErrInvalidPath when the
	// key contract is the reason, so callers can classify with errors.Is the
	// way the reconciliation sweeps do.
	Err error
}

// UnusableLister enumerates objects that ListObjects deliberately hides.
//
// Listings drop keys the backend would then refuse, because handing one back
// turns every List-then-Read caller into a failure (#743, #744). That is the
// right trade for the caller and the wrong one for the operator: on local
// storage the dropped entries are real Parquet files holding real rows, the
// query path still serves them because read_parquet globs the filesystem, and
// nothing anywhere could name them. A backup would then omit them and report
// success, which is the failure #743's own commit message called worse than the
// bug it was fixing.
//
// This is the counterpart #744 already established for the case it created:
// when it hid write-staging partials it added ListStaged to reclaim them. The
// contract drop had no equivalent (#756).
//
// The set is defined by OBSERVATION, not by predicate: it is what a full
// listing sees and ListObjects does not return. Re-deriving it from ValidateKey
// would miss the entries a listing skips for other reasons, and would drift
// again the next time a listing grows a filter.
//
// Excluded, because they are provably not data an operator can lose:
//   - Arc's own in-flight ".arc-*.tmp" writes, which is why listings skip
//     dot-prefixed names at all.
//   - On backends that stage, a ".part" staging path for an addressable key.
//     That covers an in-flight WriteReader and an abandoned partial alike, and
//     reporting either would be a permanent false alarm carrying advice that
//     would publish a truncated file. Object stores do not stage, so a ".part"
//     key there is an ordinary committed object and IS reported.
type UnusableLister interface {
	Backend

	// ListUnusable returns objects under prefix that ListObjects omits.
	// An empty result is the healthy case.
	ListUnusable(ctx context.Context, prefix string) ([]UnusableObject, error)
}

// DirectoryRemover removes an empty directory.
// This is used to clean up database directories after all files are deleted.
// For object storage (S3, Azure), this is typically a no-op since directories don't exist as objects.
type DirectoryRemover interface {
	RemoveDirectory(ctx context.Context, path string) error
}
