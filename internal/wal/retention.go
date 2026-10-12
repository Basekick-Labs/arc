package wal

import (
	"os"
	"path/filepath"
	"sort"
)

// WAL retention for covered entries (#1207 S4c).
//
// A covered entry (0x04) stays in the WAL until the file its rows were flushed
// into is registered in the cluster manifest, across restarts too: if the
// registration is refused because another writer took over the batch, the
// rows above the takeover point must be re-flushed from these entries (the
// re-flush is a later stage of #1207), and the entry is the only record of
// which batch each row belongs to. The registration outbox knows which
// batches are still waiting; it answers through a CoverageHold.
//
// Order is by file name, which is rotation order within a process and across
// restarts unless the clock steps backwards between them — the assumption
// recovery has always made.
//
// Three places delete WAL files, and each consults the hold:
//   - PurgeFlushed stops at a file of this process holding a held batch
//     (per-file coverage recorded as entries are written);
//   - recovery retains a file holding a held batch, and every file after it
//     in the pass (their checkpoints can be the only proof for the retained
//     file's entries), and hands them to the writer;
//   - ReleaseRetained deletes those files, in name order, once nothing in
//     them is held.

// CoverageHold reports whether any batch of instance and buffer key with a
// coverage sequence in [lo, hi] is still waiting for its file to register.
// Implementations are immutable snapshots, safe for concurrent use.
type CoverageHold interface {
	HoldsAny(instance uint64, key string, lo, hi uint64) bool
}

// CoverageSpan is a run of coverage sequences of one instance and key.
type CoverageSpan struct {
	Instance uint64
	Key      string
	Lo, Hi   uint64
}

// RetainedFile is a WAL file recovery kept for registrations still pending,
// or because an earlier file of the same pass was kept for them. Coverage is
// the covered batches its entries belong to (possibly none held, for a file
// kept for its position).
type RetainedFile struct {
	Path     string
	Coverage []CoverageSpan
}

// coverageSet collects coverage sequences per (instance, key) as merged
// intervals. Batches of a key are minted in order, so a file's set stays as
// small as the number of interleaved keys.
type coverageSet map[coverageSetKey][][2]uint64

type coverageSetKey struct {
	instance uint64
	key      string
}

func (s coverageSet) add(instance uint64, key string, cseq uint64) {
	k := coverageSetKey{instance, key}
	v := s[k]
	i := sort.Search(len(v), func(i int) bool { return v[i][1]+1 >= cseq })
	if i < len(v) && v[i][0] <= cseq+1 && v[i][1]+1 >= cseq {
		if cseq < v[i][0] {
			v[i][0] = cseq
		}
		if cseq > v[i][1] {
			v[i][1] = cseq
			if i+1 < len(v) && v[i+1][0] <= cseq+1 {
				v[i][1] = v[i+1][1]
				v = append(v[:i+1], v[i+2:]...)
			}
		}
		s[k] = v
		return
	}
	v = append(v, [2]uint64{})
	copy(v[i+1:], v[i:])
	v[i] = [2]uint64{cseq, cseq}
	s[k] = v
}

func (s coverageSet) held(h CoverageHold) bool {
	if h == nil {
		return false
	}
	for k, spans := range s {
		for _, sp := range spans {
			if h.HoldsAny(k.instance, k.key, sp[0], sp[1]) {
				return true
			}
		}
	}
	return false
}

func (s coverageSet) spans() []CoverageSpan {
	var out []CoverageSpan
	for k, v := range s {
		for _, sp := range v {
			out = append(out, CoverageSpan{Instance: k.instance, Key: k.key, Lo: sp[0], Hi: sp[1]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Instance != b.Instance {
			return a.Instance < b.Instance
		}
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.Lo < b.Lo
	})
	return out
}

func spansHeld(spans []CoverageSpan, h CoverageHold) bool {
	if h == nil {
		return false
	}
	for _, sp := range spans {
		if h.HoldsAny(sp.Instance, sp.Key, sp.Lo, sp.Hi) {
			return true
		}
	}
	return false
}

// SetCoverageHold installs the source of the hold. hold is called once per
// purge pass, outside the writer lock; nil (or a nil result) holds nothing.
func (w *Writer) SetCoverageHold(hold func() CoverageHold) {
	w.mu.Lock()
	w.coverageHold = hold
	w.mu.Unlock()
}

func (w *Writer) currentHold() CoverageHold {
	w.mu.Lock()
	hold := w.coverageHold
	w.mu.Unlock()
	if hold == nil {
		return nil
	}
	return hold()
}

// AdoptRetained takes over the files recovery retained. Files this process
// created are left to PurgeFlushed; a file already adopted keeps its first
// summary (a later pass reads the same entries).
func (w *Writer) AdoptRetained(files []RetainedFile) {
	if len(files) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.adopted == nil {
		w.adopted = make(map[string]RetainedFile)
	}
	for _, f := range files {
		if _, own := w.created[f.Path]; own {
			continue
		}
		if _, ok := w.adopted[f.Path]; !ok {
			w.adopted[f.Path] = f
		}
	}
}

// RetainedFiles reports how many files recovery retained that are still kept.
func (w *Writer) RetainedFiles() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.adopted)
}

// HeldFiles reports the WAL files kept because batches in them are waiting
// for their files to register: files of this process holding a held batch,
// and files recovery retained. While registrations cannot land (a lost
// quorum, a start without the cluster) every later file is kept too, by the
// purge order; arc_wal_dir_bytes shows that growth.
func (w *Writer) HeldFiles() int {
	hold := w.currentHold()
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(w.adopted)
	for _, path := range w.fileOrder {
		if state := w.fileSeqs[path]; state != nil && state.cov.held(hold) {
			n++
		}
	}
	return n
}

// ReleaseRetained deletes adopted files whose batches are no longer held. It
// walks every WAL file this process did not create in name order — the order
// recovery replays and retains them — and stops at the first one it must keep:
// a held file, or an earlier-process file it was never given (recovery kept it
// for a failure and will process it again). Stopping, not skipping, keeps the
// proof ordering: a later file can hold the only checkpoint for an earlier
// one's entries. Returns the number of files deleted.
func (w *Writer) ReleaseRetained() (int, error) {
	w.mu.Lock()
	none := len(w.adopted) == 0
	w.mu.Unlock()
	if none {
		return 0, nil
	}
	listed, err := listWALFilesFn(w.config.WALDir)
	if err != nil {
		return 0, err
	}
	hold := w.currentHold()
	sort.Strings(listed)
	w.mu.Lock()
	present := make(map[string]struct{}, len(listed))
	var candidates []string
	stopped := false
	for _, path := range listed {
		present[path] = struct{}{}
		if _, own := w.created[path]; own || stopped {
			continue
		}
		f, ok := w.adopted[path]
		if !ok || spansHeld(f.Coverage, hold) {
			stopped = true
			continue
		}
		candidates = append(candidates, path)
	}
	// Recovery deleted or quarantined some on its own.
	for path := range w.adopted {
		if _, ok := present[path]; !ok {
			delete(w.adopted, path)
		}
	}
	w.mu.Unlock()

	deleted := 0
	for _, path := range candidates {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return deleted, err // stop: a later file may hold this one's proof
		}
		deleted++
		w.mu.Lock()
		delete(w.adopted, path)
		w.mu.Unlock()
	}
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Str("last", filepath.Base(candidates[deleted-1])).
			Msg("Deleted retained WAL files whose batches are registered")
	}
	return deleted, nil
}
