package edgesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/basekick-labs/arc/internal/storage"
)

// bundleSink is where a BundleWriter puts a bundle's files: a directory on
// removable media (the air-gap path) or a prefix in a relay bucket (#1685).
// Paths are bundle-relative and slash-separated ("data/db/m/…/f.parquet",
// "entries.jsonl", "manifest.json"), already validated by the writer.
type bundleSink interface {
	// putStream writes one file whose bytes fill produces. size is the
	// ledger's size for it; an object store may refuse a body of another
	// length (storage.ErrBodyLength).
	putStream(ctx context.Context, rel string, size int64, fill func(io.Writer) error) error
	// putBytes writes one small file in a single write.
	putBytes(ctx context.Context, rel string, data []byte) error
	// abort removes everything this sink wrote. Only called before the
	// manifest is written.
	abort(ctx context.Context) error
	// location names the bundle for logs and the export result.
	location() string
}

// dirSink writes a bundle directory, fsyncing each file: without the fsync a
// bundle can be reported complete while its bytes are still in the page cache,
// and the next thing that happens to a bundle is usually an unplugged drive.
type dirSink struct {
	dir string
}

// newDirSink creates parent/name, refusing one that already exists.
//
// Mkdir on the leaf, not stat-then-MkdirAll: MkdirAll succeeds on an existing
// directory, so a stat check leaves a window where two exports could
// interleave into one bundle. Mkdir returns EEXIST atomically. 0700: a bundle
// holds the spoke's telemetry and sits on removable media.
func newDirSink(parent, name string) (*dirSink, error) {
	dir := filepath.Join(parent, name)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, fmt.Errorf("edgesync: create bundle parent: %w", err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("edgesync: create bundle directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, dataDir), 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("edgesync: create bundle data directory: %w", err)
	}
	return &dirSink{dir: dir}, nil
}

func (s *dirSink) putStream(_ context.Context, rel string, _ int64, fill func(io.Writer) error) error {
	dest := filepath.Join(s.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return fmt.Errorf("edgesync: create bundle subdirectory: %w", err)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("edgesync: create %s: %w", dest, err)
	}
	defer f.Close()
	if err := fill(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("edgesync: sync %s: %w", dest, err)
	}
	return nil
}

func (s *dirSink) putBytes(ctx context.Context, rel string, data []byte) error {
	return s.putStream(ctx, rel, int64(len(data)), func(w io.Writer) error {
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("edgesync: write %s: %w", rel, err)
		}
		return nil
	})
}

func (s *dirSink) abort(context.Context) error { return os.RemoveAll(s.dir) }
func (s *dirSink) location() string            { return s.dir }

// objectSink writes a bundle under a key prefix of a storage backend.
type objectSink struct {
	backend storage.Backend
	base    string // ends in "/"

	mu      sync.Mutex
	written []string
}

func (s *objectSink) record(key string) {
	s.mu.Lock()
	s.written = append(s.written, key)
	s.mu.Unlock()
}

// putStream pipes fill into WriteReader, so a data file streams from spoke
// storage into the bucket without touching the edge box's disk.
//
// Both pipe ends are closed with the other side's error: a failed upload must
// unblock the reader goroutine (or it leaks, blocked on a write nobody
// drains), and a failed read must fail the upload rather than commit a short
// object.
func (s *objectSink) putStream(ctx context.Context, rel string, size int64, fill func(io.Writer) error) error {
	key := s.base + rel
	// Recorded before the write: a failed upload may still have committed
	// (the error came after), and abort must remove it.
	s.record(key)
	pr, pw := io.Pipe()
	fillErr := make(chan error, 1)
	go func() {
		err := fill(pw)
		_ = pw.CloseWithError(err) // nil closes cleanly: EOF for the reader
		fillErr <- err
	}()
	upErr := s.backend.WriteReader(ctx, key, pr, size)
	if upErr != nil {
		_ = pr.CloseWithError(upErr)
	} else {
		_ = pr.Close()
	}
	fErr := <-fillErr
	if fErr != nil && !errors.Is(fErr, io.ErrClosedPipe) && (upErr == nil || !errors.Is(fErr, upErr)) {
		// The read side failed (a missing or unreadable source): that is the
		// cause, whatever the upload then reported.
		return fErr
	}
	if upErr != nil {
		return fmt.Errorf("edgesync: upload %s: %w", key, upErr)
	}
	return nil
}

func (s *objectSink) putBytes(ctx context.Context, rel string, data []byte) error {
	key := s.base + rel
	s.record(key)
	if err := s.backend.WriteReader(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("edgesync: upload %s: %w", key, err)
	}
	return nil
}

// abort deletes every key this sink wrote. Best effort per key; the first
// error is returned so the caller can log it. A leftover is a manifest-less
// prefix, which the hub never imports and the next relay pass sweeps.
func (s *objectSink) abort(ctx context.Context) error {
	s.mu.Lock()
	keys := append([]string(nil), s.written...)
	s.mu.Unlock()
	var first error
	for _, k := range keys {
		if err := s.backend.Delete(ctx, k); err != nil && !storage.IsNotFound(err) && first == nil {
			first = err
		}
	}
	return first
}

func (s *objectSink) location() string { return s.base }
