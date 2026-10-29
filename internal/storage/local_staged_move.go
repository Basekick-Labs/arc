package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ObjectMover is a backend that can make a written object durable, move one
// object to another key without copying, and remove everything under a key
// prefix. The local backend implements it; the staged compaction commit
// (#1207 d2a2a) uses it to hold a compaction output under a reserved key until
// the cluster commits it, then move it into place.
type ObjectMover interface {
	// Sync makes the object at key and its directory entry durable.
	Sync(ctx context.Context, key string) error
	// MoveObject renames src to dst (same filesystem; atomic) and makes the
	// rename durable.
	MoveObject(ctx context.Context, src, dst string) error
	// RemoveAll removes key and everything under it. A missing key is not an
	// error.
	RemoveAll(ctx context.Context, key string) error
}

var _ ObjectMover = (*LocalBackend)(nil)

// Sync fsyncs the file at key, then its directory, so a crash after Sync
// returns keeps both the bytes and the name.
func (b *LocalBackend) Sync(ctx context.Context, key string) error {
	fullPath, err := b.validatePath(key)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	if err := syncPath(fullPath); err != nil {
		return err
	}
	return b.syncDirs(filepath.Dir(fullPath))
}

// syncDirs fsyncs dir and each parent up to the storage root, so a directory
// created for the object survives a crash along with it.
func (b *LocalBackend) syncDirs(dir string) error {
	root := filepath.Clean(b.basePath)
	for {
		if err := syncPath(dir); err != nil {
			return err
		}
		if dir == root || len(dir) <= len(root) {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

// MoveObject renames src to dst within the storage root and fsyncs dst's
// directory (and src's, when it differs). dst's directory is created.
func (b *LocalBackend) MoveObject(ctx context.Context, src, dst string) error {
	srcPath, err := b.validatePath(src)
	if err != nil {
		return fmt.Errorf("invalid source path: %w", err)
	}
	dstPath, err := b.validatePath(dst)
	if err != nil {
		return fmt.Errorf("invalid destination path: %w", err)
	}
	info, err := os.Lstat(srcPath)
	if err != nil {
		return fmt.Errorf("failed to stat %s: %w", src, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to move %s: not a regular file", src)
	}
	if err := b.ensureDir(filepath.Dir(dstPath)); err != nil {
		return err
	}
	if err := renameFile(srcPath, dstPath); err != nil {
		return fmt.Errorf("failed to move %s to %s: %w", src, dst, err)
	}
	if err := b.syncDirs(filepath.Dir(dstPath)); err != nil {
		return err
	}
	if filepath.Dir(srcPath) != filepath.Dir(dstPath) {
		if err := syncPath(filepath.Dir(srcPath)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// RemoveAll removes key and everything under it.
func (b *LocalBackend) RemoveAll(ctx context.Context, key string) error {
	fullPath, err := b.validatePath(key)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	if fullPath == filepath.Clean(b.basePath) {
		return errors.New("refusing to remove the storage root")
	}
	if err := os.RemoveAll(fullPath); err != nil {
		return fmt.Errorf("failed to remove %s: %w", key, err)
	}
	b.dirMu.Lock()
	for d := range b.dirCache {
		if d == fullPath || len(d) > len(fullPath) && d[:len(fullPath)+1] == fullPath+string(filepath.Separator) {
			delete(b.dirCache, d)
		}
	}
	b.dirMu.Unlock()
	return nil
}

func syncPath(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s: %w", p, err)
	}
	return nil
}
