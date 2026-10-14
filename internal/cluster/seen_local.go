package cluster

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// seenLocalJournal is the file, in raft_data_dir, that keeps the set of local
// files this node saw registered (#1071).
const seenLocalJournal = "seen-local.log"

const seenLocalHeader = "arc-seen-local v1 "

// seenRecord identifies the local file a path held when this node saw it
// registered: the file must still be that one (size, mtime, inode) before the
// stale-local sweep may remove it, and lsn is the manifest index of the
// registration, so a manifest that has not applied that far yet never makes it
// look unlisted. sha is the first 64 bits of the registered checksum: a later
// registration with another one means the content changed.
type seenRecord struct {
	size  int64
	mtime int64 // UnixNano
	ino   uint64
	lsn   uint64
	sha   uint64
}

// shaPrefix is the first 64 bits of a hex SHA-256 (0 if it is not one).
func shaPrefix(sha string) uint64 {
	if len(sha) < 16 {
		return 0
	}
	v, err := strconv.ParseUint(sha[:16], 16, 64)
	if err != nil {
		return 0
	}
	return v
}

// seenLocal is that set, in memory and in an append-only journal rewritten
// when it grows past 1.5 times the live records. Losing the journal's tail in
// a crash is safe: a file whose record was lost is never removed, and a
// removal whose record was lost is found gone (or a different file) and
// dropped. A write error stops tracking until a restart (fail open: nothing
// is removed then).
type seenLocal struct {
	mu      sync.Mutex
	set     map[string]seenRecord
	dir     string
	cluster string
	f       *os.File
	w       *bufio.Writer
	records int
	dirty   bool
	failed  error
}

// openSeenLocal loads the journal in dir, keeping it only if it belongs to
// cluster, and rewrites it.
func openSeenLocal(dir, cluster string) (*seenLocal, error) {
	s := &seenLocal{set: map[string]seenRecord{}, dir: dir, cluster: cluster}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", dir, err)
	}
	if tmps, _ := filepath.Glob(filepath.Join(dir, seenLocalJournal+".tmp-*")); len(tmps) > 0 {
		for _, t := range tmps {
			_ = os.Remove(t)
		}
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.rewriteLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *seenLocal) load() error {
	f, err := os.Open(filepath.Join(s.dir, seenLocalJournal))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open %s: %w", seenLocalJournal, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	first := true
	for sc.Scan() {
		line := sc.Text()
		if first {
			first = false
			if line != seenLocalHeader+s.cluster {
				return nil // another cluster's, or unreadable: start empty
			}
			continue
		}
		applySeenLine(s.set, line)
	}
	if err := sc.Err(); err != nil {
		// A line too long to be ours ends the read; what came before stands.
		return nil
	}
	return nil
}

// applySeenLine applies one journal line; a torn or unknown line is skipped.
func applySeenLine(set map[string]seenRecord, line string) {
	parts := strings.Split(line, "\t")
	switch {
	case len(parts) == 2 && parts[0] == "d":
		delete(set, parts[1])
	case len(parts) == 7 && parts[0] == "a" && parts[1] != "":
		size, e1 := strconv.ParseInt(parts[2], 10, 64)
		mtime, e2 := strconv.ParseInt(parts[3], 10, 64)
		ino, e3 := strconv.ParseUint(parts[4], 10, 64)
		lsn, e4 := strconv.ParseUint(parts[5], 10, 64)
		sha, e5 := strconv.ParseUint(parts[6], 10, 64)
		if e1 == nil && e2 == nil && e3 == nil && e4 == nil && e5 == nil {
			set[parts[1]] = seenRecord{size: size, mtime: mtime, ino: ino, lsn: lsn, sha: sha}
		}
	}
}

func addLine(path string, r seenRecord) string {
	return "a\t" + path + "\t" + strconv.FormatInt(r.size, 10) + "\t" + strconv.FormatInt(r.mtime, 10) +
		"\t" + strconv.FormatUint(r.ino, 10) + "\t" + strconv.FormatUint(r.lsn, 10) +
		"\t" + strconv.FormatUint(r.sha, 10) + "\n"
}

// rewriteLocked writes the live set to a new journal and swaps it in.
func (s *seenLocal) rewriteLocked() error {
	if s.f != nil {
		_ = s.f.Close()
		s.f, s.w = nil, nil
	}
	tmp, err := os.CreateTemp(s.dir, seenLocalJournal+".tmp-*")
	if err != nil {
		return s.failLocked(err)
	}
	w := bufio.NewWriterSize(tmp, 256<<10)
	_, err = w.WriteString(seenLocalHeader + s.cluster + "\n")
	for p, r := range s.set {
		if err != nil {
			break
		}
		_, err = w.WriteString(addLine(p, r))
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	final := filepath.Join(s.dir, seenLocalJournal)
	if err == nil {
		err = os.Rename(tmp.Name(), final)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return s.failLocked(err)
	}
	_ = syncDir(s.dir)
	f, err := os.OpenFile(final, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return s.failLocked(err)
	}
	s.f, s.w = f, bufio.NewWriterSize(f, 64<<10)
	s.records = len(s.set)
	s.dirty = false
	return nil
}

// failLocked stops tracking: the set is emptied so nothing is removed on the
// strength of a journal that no longer keeps up.
func (s *seenLocal) failLocked(err error) error {
	if s.failed == nil {
		s.failed = err
	}
	s.set = map[string]seenRecord{}
	if s.f != nil {
		_ = s.f.Close()
		s.f, s.w = nil, nil
	}
	return err
}

func (s *seenLocal) appendLocked(line string) {
	if s.failed != nil {
		return
	}
	if _, err := s.w.WriteString(line); err != nil {
		_ = s.failLocked(err)
		return
	}
	s.records++
	s.dirty = true
	if live := len(s.set); s.records > live+max(live/2, 4096) {
		_ = s.rewriteLocked()
	}
}

// add records path as registered and present with identity r; a record
// already holding r writes nothing.
func (s *seenLocal) add(path string, r seenRecord) {
	if strings.ContainsAny(path, "\t\n") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return
	}
	if old, ok := s.set[path]; ok && old == r {
		return
	}
	s.set[path] = r
	s.appendLocked(addLine(path, r))
}

// remove forgets path; a path not in the set writes nothing.
func (s *seenLocal) remove(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[path]; !ok || s.failed != nil {
		return
	}
	delete(s.set, path)
	s.appendLocked("d\t" + path + "\n")
}

func (s *seenLocal) get(path string) (seenRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.set[path]
	return r, ok
}

// snapshot copies the set (the sweep works on the copy).
func (s *seenLocal) snapshot() map[string]seenRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]seenRecord, len(s.set))
	for p, r := range s.set {
		out[p] = r
	}
	return out
}

func (s *seenLocal) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.set)
}

// err reports why tracking stopped, nil while it runs.
func (s *seenLocal) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// sync flushes and fsyncs what was appended since the last sync.
func (s *seenLocal) sync() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty || s.failed != nil {
		return
	}
	if err := s.w.Flush(); err != nil {
		_ = s.failLocked(err)
		return
	}
	if err := s.f.Sync(); err != nil {
		_ = s.failLocked(err)
		return
	}
	s.dirty = false
}

var errSeenLocalClosed = errors.New("stale-local tracking closed")

// close syncs and closes the journal; later adds and removes do nothing (a
// delete worker that outlived its drain bound may still report one).
func (s *seenLocal) close() {
	s.sync()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		_ = s.f.Close()
		s.f, s.w = nil, nil
	}
	if s.failed == nil {
		s.failed = errSeenLocalClosed
	}
}

// unlisted returns the tracked paths for which listed answers false, under
// one hold of the set's lock and without copying the set.
func (s *seenLocal) unlisted(listed func(path string) bool) map[string]seenRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]seenRecord{}
	for p, r := range s.set {
		if !listed(p) {
			out[p] = r
		}
	}
	return out
}
