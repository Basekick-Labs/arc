package edgesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// MemoryTransport is an in-process SyncTransport backed by a map.
//
// It exists for two reasons. First, it keeps SyncTransport honest: an
// interface with no implementation is a guess, and writing this one is what
// surfaced the fact that PutFile needs a result type rather than a bare error.
// Second, the agent (PR 8) needs a hub it can drive deterministically —
// exercising lost acks, conflicts, mid-stream drops, and backpressure against
// a real HTTP server means either a fragile fake server or no test at all.
//
// It implements the same identity rule as a real hub (§6.1): a path is
// absent (write), present with the same digest (no-op), or present with a
// different digest (conflict, never overwrite). It is NOT a hub — it does no
// authentication, no namespacing, and keeps bytes in memory.
type MemoryTransport struct {
	mu sync.Mutex

	// files is the hub's contents, keyed by hub ID then path.
	files map[string]map[string]*memFile

	// staging holds partially-received files, keyed by hub ID then path.
	//
	// A SEPARATE map, not a reserved key inside files: sharing the namespace
	// meant a crafted hubID could address the staging bucket as a real hub,
	// and a staged file (which has no digest until it commits) then surfaced
	// as a phantom conflict with an empty SHA256 — an operator-escalating
	// alarm on a file that was merely mid-transfer.
	staging map[string]map[string]*memFile

	// failures queues scripted outcomes per path so a test can make the next
	// PutFile answer partial/backpressure/mismatch without needing a real
	// failing link.
	failures map[string][]*PutResult

	closed bool
}

type memFile struct {
	sha256 string
	size   int64
	bytes  []byte
}

// NewMemoryTransport returns an empty in-memory hub.
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		files:    make(map[string]map[string]*memFile),
		staging:  make(map[string]map[string]*memFile),
		failures: make(map[string][]*PutResult),
	}
}

// Seed pre-populates the hub with a file, as though a previous sync had
// delivered it. Used to set up the lost-ack and conflict paths.
func (m *MemoryTransport) Seed(hubID, path, sha256Hex string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hubLocked(hubID)[path] = &memFile{sha256: sha256Hex, size: size}
}

// ScriptPut queues results for the next PutFile calls on (hubID, path),
// letting a test drive the partial/backpressure/mismatch branches
// deterministically. Queued results are consumed in order; once empty,
// PutFile behaves normally.
//
// Keyed by hub as well as path: with a path-only key, a script intended for
// one hub would be consumed by a transfer to another, so a "hub A throttles,
// hub B does not" test would silently assert the opposite.
func (m *MemoryTransport) ScriptPut(hubID, path string, results ...*PutResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := scriptKey(hubID, path)
	m.failures[key] = append(m.failures[key], results...)
}

// scriptKey namespaces a scripted result by hub and path. The NUL separator
// keeps a hubID containing the delimiter from addressing another hub's queue.
func scriptKey(hubID, path string) string {
	if hubID == "" {
		hubID = DefaultHubID
	}
	return hubID + "\x00" + path
}

// Has reports whether the hub holds a path, and its digest.
func (m *MemoryTransport) Has(hubID, path string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.hubLocked(hubID)[path]
	if !ok {
		return "", false
	}
	return f.sha256, true
}

// Close makes every subsequent call return ErrTransportClosed.
func (m *MemoryTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// hubLocked returns the hub's committed-file map, creating it if needed.
// Caller holds mu.
func (m *MemoryTransport) hubLocked(hubID string) map[string]*memFile {
	if hubID == "" {
		hubID = DefaultHubID
	}
	h, ok := m.files[hubID]
	if !ok {
		h = make(map[string]*memFile)
		m.files[hubID] = h
	}
	return h
}

// stagingLocked returns the hub's partial-file map, creating it if needed.
// Caller holds mu.
func (m *MemoryTransport) stagingLocked(hubID string) map[string]*memFile {
	if hubID == "" {
		hubID = DefaultHubID
	}
	h, ok := m.staging[hubID]
	if !ok {
		h = make(map[string]*memFile)
		m.staging[hubID] = h
	}
	return h
}

// Reconcile partitions the pending set exactly as a hub would: matching digest
// is present, differing digest is a conflict, unknown path is missing.
func (m *MemoryTransport) Reconcile(ctx context.Context, hubID string, pending []*LedgerEntry) (*ReconcileResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrTransportClosed
	}

	hub := m.hubLocked(hubID)
	res := &ReconcileResult{}

	for _, e := range pending {
		if e == nil {
			return nil, errors.New("edgesync: nil entry in reconcile batch")
		}
		// An empty path is a valid map key, so without this check it would
		// silently classify against hub[""] and land in one of the result
		// lists — where ReconcileResult.Validate would then reject the whole
		// batch with a confusing error about the result rather than the input.
		if e.Path == "" {
			return nil, errors.New("edgesync: entry with an empty path in reconcile batch")
		}
		switch f, ok := hub[e.Path]; {
		case !ok:
			res.Missing = append(res.Missing, e.Path)
		case f.sha256 == e.SHA256:
			res.Present = append(res.Present, e.Path)
		default:
			res.Conflicts = append(res.Conflicts, Conflict{
				Path:        e.Path,
				TheirSHA256: f.sha256,
			})
		}
	}

	return res, nil
}

// PutFile stores the streamed bytes, applying the §6.1 identity rule and
// verifying the digest before committing — the same verify-before-commit
// ordering a real hub uses, so a mismatch never lands as stored content.
func (m *MemoryTransport) PutFile(ctx context.Context, hubID string, entry *LedgerEntry, body io.Reader, offset int64) (*PutResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, errors.New("edgesync: PutFile requires an entry")
	}
	if entry.Path == "" {
		return nil, errors.New("edgesync: PutFile requires a path")
	}
	// The digest is the integrity anchor — without it there is nothing to
	// verify against, and committing anyway would defeat the whole
	// verify-before-commit ordering.
	if entry.SHA256 == "" {
		return nil, fmt.Errorf("edgesync: PutFile for %q requires a sha256", entry.Path)
	}
	if offset < 0 || offset > entry.SizeBytes {
		return nil, fmt.Errorf("edgesync: offset %d out of range for %q (size %d)",
			offset, entry.Path, entry.SizeBytes)
	}

	// Phase 1 — decide under the lock whether this transfer should read a body
	// at all, and grab the staged prefix if resuming.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrTransportClosed
	}
	// A scripted result short-circuits before the body is touched, so a test
	// can simulate a hub that rejects without reading.
	if key := scriptKey(hubID, entry.Path); len(m.failures[key]) > 0 {
		res := m.failures[key][0]
		m.failures[key] = m.failures[key][1:]
		m.mu.Unlock()
		if res == nil {
			return nil, fmt.Errorf("edgesync: scripted nil result for %q", entry.Path)
		}
		if err := res.Validate(entry); err != nil {
			return nil, fmt.Errorf("edgesync: scripted result for %q is invalid: %w", entry.Path, err)
		}
		return res, nil
	}

	// §6.1: identical content at the same path is an idempotent no-op, and a
	// differing digest is refused rather than overwritten. Decided before
	// reading the body — a duplicate costs no transfer, and a conflict must
	// not consume bytes it will discard.
	if existing, exists := m.hubLocked(hubID)[entry.Path]; exists {
		m.mu.Unlock()
		if existing.sha256 == entry.SHA256 {
			return &PutResult{
				Outcome:       OutcomeAlreadyPresent,
				BytesAccepted: existing.size,
			}, nil
		}
		return &PutResult{
			Outcome:     OutcomeConflict,
			TheirSHA256: existing.sha256,
		}, nil
	}

	// A resumed transfer carries only the tail, so the prefix must already be
	// staged from an earlier partial attempt.
	var prefix []byte
	if offset > 0 {
		staged, ok := m.stagingLocked(hubID)[entry.Path]
		if !ok || int64(len(staged.bytes)) < offset {
			m.mu.Unlock()
			return nil, fmt.Errorf("edgesync: resume from %d for %q but no staged prefix",
				offset, entry.Path)
		}
		// Copy: the staged buffer may be replaced by a concurrent attempt
		// while this one is reading its body outside the lock.
		prefix = append([]byte(nil), staged.bytes[:offset]...)
	}
	m.mu.Unlock()

	// Phase 2 — read the body WITHOUT the lock. A real hub streams bytes to
	// storage here; holding a mutex across that I/O would serialize every
	// concurrent transfer.
	// io.ReadAll is acceptable ONLY because this transport is test-only and
	// its files are small. It is NOT a model for the HTTPS transport, which
	// must tee the stream into a SHA-256 hasher and write to a temp path as
	// bytes arrive (§5.2) — buffering a multi-hundred-MB parquet file in
	// memory on a hub receiving from many spokes is how the receive path
	// falls over.
	tail, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("edgesync: read body for %q: %w", entry.Path, err)
	}

	full := make([]byte, 0, len(prefix)+len(tail))
	full = append(full, prefix...)
	full = append(full, tail...)

	// Verify before taking the lock: a mismatch never reaches the commit path
	// at all, so corrupt bytes cannot become stored content.
	var got string
	if int64(len(full)) >= entry.SizeBytes {
		sum := sha256.Sum256(full)
		got = hex.EncodeToString(sum[:])
		if got != entry.SHA256 {
			return &PutResult{Outcome: OutcomeChecksumMismatch}, nil
		}
	}

	// Phase 3 — commit under the lock, RE-CHECKING the identity rule.
	//
	// The re-check is the whole point: between the phase-1 decision and here,
	// another transfer for the same path may have committed. Without it, two
	// concurrent PutFile calls both observe "absent", both read their bodies,
	// and the second silently overwrites the first with DIFFERENT content —
	// the exact never-overwrite violation §6.1 calls an alarm. That race is
	// invisible to -race (every map access is locked; the bug is in the
	// interleaving, not the memory access), so it has to be reasoned about
	// rather than detected.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrTransportClosed
	}

	if existing, exists := m.hubLocked(hubID)[entry.Path]; exists {
		if existing.sha256 == entry.SHA256 {
			return &PutResult{
				Outcome:       OutcomeAlreadyPresent,
				BytesAccepted: existing.size,
			}, nil
		}
		return &PutResult{
			Outcome:     OutcomeConflict,
			TheirSHA256: existing.sha256,
		}, nil
	}

	// Short of the declared size means the stream ended early — stage what
	// arrived so the next attempt resumes rather than restarting.
	if int64(len(full)) < entry.SizeBytes {
		staged := m.stagingLocked(hubID)
		// Keep the LONGEST prefix. A shorter later attempt must not move the
		// checkpoint backward: a spoke holding the older, longer offset would
		// then resume past what the hub has and get a hard error with no
		// defined recovery.
		if prev, ok := staged[entry.Path]; !ok || len(full) > len(prev.bytes) {
			staged[entry.Path] = &memFile{bytes: full, size: int64(len(full))}
		} else {
			full = prev.bytes
		}
		return &PutResult{
			Outcome:       OutcomePartial,
			BytesAccepted: int64(len(full)),
		}, nil
	}

	m.hubLocked(hubID)[entry.Path] = &memFile{
		sha256: got,
		size:   int64(len(full)),
		bytes:  full,
	}
	delete(m.stagingLocked(hubID), entry.Path)

	return &PutResult{
		Outcome:       OutcomeCommitted,
		BytesAccepted: int64(len(full)),
	}, nil
}

// BackpressureResult builds the result a hub returns when it wants the spoke
// to slow down. Provided so tests and future transports produce a valid one
// (a zero delay would busy-loop).
func BackpressureResult(d time.Duration) *PutResult {
	if d <= 0 {
		d = time.Second
	}
	return &PutResult{Outcome: OutcomeBackpressure, RetryAfter: d}
}

// Compile-time check that MemoryTransport satisfies the interface.
var _ SyncTransport = (*MemoryTransport)(nil)
