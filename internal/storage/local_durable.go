package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// Durability of local writes (#1618).
//
// A write that returns has to survive a power loss, not just a process crash:
// the ingest flush checkpoints the WAL as soon as storage.Write returns, and
// recovery skips checkpointed entries without looking at storage, so a file
// the kernel had not written back yet would take acknowledged rows with it.
// Compaction and the DELETE rewrite delete their sources on the same signal.
//
// Every publishing write therefore fsyncs the file before it is renamed or
// linked to its final name, and fsyncs the directory after, so both the bytes
// and the name are on disk when the call returns. Directories created for a
// write are synced into their parents (ensureDir).
//
// When the directory fsync fails after the rename, the bytes are durable and
// only the name is in doubt, and what to do with the file depends on whether
// the caller still holds another copy:
//
//   - Write keeps it and returns the error (notDurable). Its callers include
//     the ingest flush with the WAL off, where the file is the only copy of
//     the rows, and writers that overwrite a fixed key (Iceberg's
//     version-hint, schema anchors, backup indexes), where deleting it would
//     delete the previous version too. With the WAL on, the flush then
//     replays the rows, so they are read twice until compaction dedups them:
//     a duplicate, never a loss.
//   - WriteReader, AppendReader and AdoptFile remove it when the key did not
//     exist before the rename (unpublish). Their callers keep the source
//     (compaction inputs, the cold copy, a peer, a backup) and retry, and a
//     file left in place would be read beside the inputs it replaces. A key
//     that existed is kept, for the same reason as Write.

// syncFileFn fsyncs an open file. A variable so tests can order and fail it.
var syncFileFn = func(f *os.File) error { return f.Sync() }

// syncDirFn fsyncs a directory. A variable for the same reason.
var syncDirFn = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// dirSyncUnsupported reports whether err is a filesystem refusing to fsync a
// directory at all, rather than failing to: some FUSE and network mounts
// (CIFS among them) answer EINVAL, ENOTSUP or ENOSYS. There is nothing to wait
// for on those, and failing every write would make the mount unusable. EIO
// and every other errno are real failures. Never applied to file fsyncs.
func dirSyncUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.ENOSYS)
}

// syncDir makes the entries in dir durable.
func (b *LocalBackend) syncDir(dir string) error {
	err := syncDirFn(dir)
	if err == nil {
		return nil
	}
	if dirSyncUnsupported(err) {
		b.dirSyncWarn.Do(func() {
			b.logger.Warn().Err(err).Str("dir", dir).
				Msg("The storage volume does not support fsync on directories; a power loss can lose the names of files written just before it")
		})
		return nil
	}
	return fmt.Errorf("failed to sync directory %s: %w", dir, err)
}

// syncFileAt fsyncs the file at path through a fresh read-only descriptor.
// fsync applies to the inode, so this covers bytes written through any other
// descriptor, including one another process (DuckDB) already closed. On Linux
// before 4.16 a writeback error that happened before this open may go
// unreported; the writers here all fsync their own descriptor where they have
// one.
func syncFileAt(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = syncFileFn(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// notDurable reports a file left in place whose directory entry could not be
// made durable.
func (b *LocalBackend) notDurable(fullPath string, cause error) {
	b.logger.Error().Err(cause).Str("path", fullPath).
		Msg("Wrote a file but could not sync its directory: the file is in place and its bytes are on disk, but a power loss could lose its name")
}

// unpublish removes a file that was renamed into place but could not be made
// durable, unless existed says the key held a file before the rename: that
// one is kept (notDurable), because removing it would also remove the
// version it replaced. Best effort: the error being returned is the
// directory sync's, and a removal that also fails is logged.
func (b *LocalBackend) unpublish(fullPath string, existed bool, cause error) {
	if existed {
		b.notDurable(fullPath, cause)
		return
	}
	if err := os.Remove(fullPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		b.logger.Error().Err(err).AnErr("cause", cause).Str("path", fullPath).
			Msg("Could not remove a file whose write failed to become durable; it stays visible to queries")
	}
}

// keyExists reports whether something is at fullPath, for unpublish.
func keyExists(fullPath string) bool {
	_, err := os.Lstat(fullPath)
	return err == nil
}

// ensureDir creates dir (and any missing parent below the storage root) and
// makes each new directory durable in its parent before returning, so a file
// written into it is not lost with its directory. Directories already known
// durable are cached, to keep sustained writes from re-checking them.
//
// The creation and its fsyncs run outside dirMu: partition directories are
// per hour, so at an hour boundary every active measurement misses at once,
// and a held lock would queue all writes on the node behind their fsyncs.
// Concurrent callers for the same directory share one creation (dirFlight),
// so none returns before the directory is durable.
func (b *LocalBackend) ensureDir(dir string) error {
	if b.dirCached(dir) {
		return nil
	}
	_, err, _ := b.dirFlight.Do(dir, func() (any, error) {
		err := b.createDurable(dir)
		if errors.Is(err, fs.ErrNotExist) {
			// An ancestor the cache vouched for was removed under us
			// (retention, an operator). Forget the chain and walk it again.
			b.forgetDir(dir)
			err = b.createDurable(dir)
		}
		return nil, err
	})
	return err
}

// createDurable creates every uncached level of dir top-down and fsyncs each
// level's parent once the level exists, then caches the chain. A level that
// already exists still has its parent synced: it may have been created by
// another process, or by this one before a restart, without that sync.
func (b *LocalBackend) createDurable(dir string) error {
	b.dirMu.RLock()
	gen := b.dirGen
	b.dirMu.RUnlock()
	root := b.basePath
	var chain []string // deepest first
	for d := dir; len(d) > len(root) && !b.dirCached(d); d = filepath.Dir(d) {
		chain = append(chain, d)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		d := chain[i]
		if err := os.Mkdir(d, 0700); err != nil {
			if !errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("failed to create directory: %w", err)
			}
			if info, serr := os.Stat(d); serr != nil || !info.IsDir() {
				return fmt.Errorf("failed to create directory: %s exists and is not a directory", d)
			}
		}
		if err := b.syncDir(filepath.Dir(d)); err != nil {
			return err
		}
	}
	b.dirMu.Lock()
	defer b.dirMu.Unlock()
	if b.dirGen != gen {
		// A removal ran while this chain was created outside the lock, and
		// may have taken part of it. Caching it now could vouch for a
		// directory that is gone; the next write re-checks instead.
		return nil
	}
	for _, d := range chain {
		if len(b.dirCache) >= maxDirCacheEntries {
			for cachedDir := range b.dirCache {
				delete(b.dirCache, cachedDir)
				break
			}
		}
		b.dirCache[d] = true
	}
	return nil
}

func (b *LocalBackend) dirCached(dir string) bool {
	b.dirMu.RLock()
	defer b.dirMu.RUnlock()
	return b.dirCache[dir]
}

// forgetDir drops dir and its ancestors from the cache, for a write that found
// dir missing on disk: any of them may be what was removed.
func (b *LocalBackend) forgetDir(dir string) {
	root := b.basePath
	b.dirMu.Lock()
	for d := dir; len(d) > len(root); d = filepath.Dir(d) {
		delete(b.dirCache, d)
	}
	b.dirGen++
	b.dirMu.Unlock()
}
