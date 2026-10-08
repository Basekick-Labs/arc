package replicaview

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

const PinsPrefix = ".replica-pins/"

// PinnedFiles keeps local immutable file versions alive even when a compaction
// subprocess or retention worker unlinks the ordinary storage key. Hard links
// share the same data blocks; they do not copy Parquet data or append another
// ingestion journal. Each logical key has its own pin, even if bytes coincide.
type PinnedFiles struct {
	root        *os.Root
	mu          sync.Mutex
	durableDirs map[string]bool
}

func OpenPinnedFiles(directory string) (*PinnedFiles, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	return &PinnedFiles{root: root, durableDirs: make(map[string]bool)}, nil
}
func (p *PinnedFiles) Close() error { p.mu.Lock(); defer p.mu.Unlock(); return p.root.Close() }

func pinKey(key, hash string) (string, error) {
	if key == "" || path.IsAbs(key) || path.Clean(key) != key || key == ".." || strings.HasPrefix(key, "../") || strings.HasPrefix(key, PinsPrefix) || strings.ContainsAny(key, "\\\x00") {
		return "", fmt.Errorf("invalid replica pin source")
	}
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("invalid replica pin checksum")
	}
	return PinsPrefix + key + "/" + strings.ToLower(hash) + ".parquet", nil
}

// Pin verifies the LINKED inode, so a concurrent rename of the source cannot
// associate metadata from one version with bytes from another. Callers publish
// the returned ReadPath only after this succeeds.
func (p *PinnedFiles) Pin(key, expectedSHA string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	destination, err := pinKey(key, expectedSHA)
	if err != nil {
		return "", err
	}
	directory := path.Dir(destination)
	if err := p.root.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	created := false
	// An existing pin remains usable after another process unlinks the source.
	// Check it before Link: Link may report a missing source even if the
	// destination already exists. Hash verification below still checks bytes.
	if _, err := p.root.Stat(destination); os.IsNotExist(err) {
		if err := p.root.Link(key, destination); err != nil {
			if !os.IsExist(err) {
				return "", fmt.Errorf("pin replica source: %w", err)
			}
		} else {
			created = true
		}
	} else if err != nil {
		return "", err
	}
	fail := func(err error) (string, error) {
		if created {
			_ = p.root.Remove(destination)
		}
		return "", err
	}
	info, err := p.root.Lstat(destination)
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(fmt.Errorf("replica pin is not a regular file"))
	}
	f, err := p.root.Open(destination)
	if err != nil {
		return fail(err)
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, f)
	closeErr := f.Close()
	if copyErr != nil {
		return fail(copyErr)
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), expectedSHA) {
		return fail(fmt.Errorf("replica pin checksum mismatch"))
	}
	// Persist every new directory component, including the hard-link entry.
	// This is once per flushed/pulled file, outside the ingestion admission path.
	var synced []string
	for {
		dir, err := p.root.Open(directory)
		if err != nil {
			return fail(err)
		}
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if syncErr != nil {
			return fail(syncErr)
		}
		if closeErr != nil {
			return fail(closeErr)
		}
		// A known-durable parent only needs its updated directory entry
		// synced. Its ancestors were already persisted by an earlier Pin.
		// Serializing Pin/Remove prevents a concurrent creator from treating
		// another call's not-yet-synced directory as a durable ancestor.
		wasDurable := p.durableDirs[directory]
		synced = append(synced, directory)
		if directory == "." || wasDurable {
			break
		}
		directory = path.Dir(directory)
	}
	for _, dir := range synced {
		p.durableDirs[dir] = true
	}
	return destination, nil
}

// Remove requires the logical key and expected version, rather than accepting
// an arbitrary path. The caller must first retire the source and wait for all
// snapshot leases on that key to close.
func (p *PinnedFiles) Remove(key, hash string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	destination, err := pinKey(key, hash)
	if err != nil {
		return err
	}
	err = p.root.Remove(destination)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for directory := path.Dir(destination); strings.HasPrefix(directory, PinsPrefix); directory = path.Dir(directory) {
		if err := p.root.Remove(directory); err != nil {
			break
		} // shared/nonempty parents stay
		delete(p.durableDirs, directory)
	}
	return nil
}
