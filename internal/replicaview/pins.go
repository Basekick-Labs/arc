package replicaview

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

const PinsPrefix = ".replica-pins/"

// PinnedFiles keeps local immutable file versions alive even when a compaction
// subprocess or retention worker unlinks the ordinary storage key. Hard links
// share the same data blocks; they do not copy Parquet data or append another
// ingestion journal. Each logical key has its own pin, even if bytes coincide.
type PinnedFiles struct{ root *os.Root }

func OpenPinnedFiles(directory string) (*PinnedFiles, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	return &PinnedFiles{root: root}, nil
}
func (p *PinnedFiles) Close() error { return p.root.Close() }

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
	destination, err := pinKey(key, expectedSHA)
	if err != nil {
		return "", err
	}
	directory := path.Dir(destination)
	if err := p.root.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	created := false
	if err := p.root.Link(key, destination); err != nil {
		if !os.IsExist(err) {
			return "", fmt.Errorf("pin replica source: %w", err)
		}
	} else {
		created = true
	}
	fail := func(err error) (string, error) {
		if created {
			_ = p.root.Remove(destination)
		}
		return "", err
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
		if directory == "." {
			break
		}
		directory = path.Dir(directory)
	}
	return destination, nil
}

// Remove requires the logical key and expected version, rather than accepting
// an arbitrary path. The caller must first retire the source and wait for all
// snapshot leases on that key to close.
func (p *PinnedFiles) Remove(key, hash string) error {
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
	}
	return nil
}
