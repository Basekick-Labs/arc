package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Instances on disk (#1207 S4b): which writer instances still have tracked
// or covered entries in the WAL directory, quarantined files included. A
// writer announcing a new process reports them as held, so the cluster does
// not treat their unflushed batches as lost.

type instancesCacheKey struct {
	path  string
	size  int64
	mtime time.Time
}

var (
	instancesCacheMu sync.Mutex
	instancesCache   = map[instancesCacheKey][]uint64{}
)

// InstancesOnDisk returns the instances of the intact tracked and covered
// entries of every WAL file in dir (*.wal and quarantined *.failed) except
// skip (the active file, whose instance the caller knows), sorted. A file's
// answer is cached by its size and modification time. A file that is gone, or
// not a WAL file at all (a damaged quarantined one), holds nothing; only an
// I/O error fails the call.
func InstancesOnDisk(dir, skip string, logger zerolog.Logger) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := map[uint64]bool{}
	live := map[instancesCacheKey]bool{}
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || !(strings.HasSuffix(name, ".wal") || (strings.Contains(name, ".wal") && strings.HasSuffix(name, ".failed"))) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(dir, name)
		if path == skip {
			continue
		}
		key := instancesCacheKey{path, info.Size(), info.ModTime()}
		live[key] = true
		instancesCacheMu.Lock()
		insts, ok := instancesCache[key]
		instancesCacheMu.Unlock()
		if !ok {
			ids, err := NewReader(path, logger).ReadTrackedIdentities()
			switch {
			case err == nil:
			case errors.Is(err, os.ErrNotExist):
				continue // purged meanwhile
			case errors.Is(err, errBadWALMagic):
				logger.Error().Str("file", name).Msg("A file in the WAL directory is not a readable WAL file; its writer instance cannot be determined")
			default:
				return nil, err
			}
			set := map[uint64]bool{}
			for _, id := range ids {
				if len(id) == 32 {
					if inst, err := strconv.ParseUint(id[:16], 16, 64); err == nil {
						set[inst] = true
					}
				}
			}
			for inst := range set {
				insts = append(insts, inst)
			}
			instancesCacheMu.Lock()
			instancesCache[key] = insts
			instancesCacheMu.Unlock()
		}
		for _, inst := range insts {
			seen[inst] = true
		}
	}
	instancesCacheMu.Lock()
	for k := range instancesCache {
		if filepath.Dir(k.path) == dir && !live[k] {
			delete(instancesCache, k)
		}
	}
	instancesCacheMu.Unlock()
	out := make([]uint64, 0, len(seen))
	for inst := range seen {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}
