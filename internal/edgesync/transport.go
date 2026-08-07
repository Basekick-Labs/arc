package edgesync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// SyncTransport moves reconcile requests and file bytes from a spoke to a hub.
//
// It exists so the sync agent is written once against an abstract hub and the
// wire format stays swappable. Phase 1 ships HTTPS; an S3/Azure relay (where
// spoke and hub never connect directly, exchanging objects through a shared
// bucket) and a sneakernet bundle (a signed directory carried on physical
// media) are planned, and both reuse the reconcile/identity/idempotency logic
// unchanged because none of it is HTTP-specific.
//
// Implementations must be safe for concurrent use: the agent transfers several
// files at once (§8.2, sync.max_concurrent_files defaults to 2).
type SyncTransport interface {
	// Reconcile asks the hub which of the spoke's pending files it already
	// holds. This is ONE round-trip for the whole backlog regardless of size —
	// the property that makes a long disconnection survivable, since 5,000
	// pending files cost one request rather than 5,000.
	//
	// The signature materializes both lists, which is fine at phase-1 scale
	// and keeps the interface simple. The WIRE format is where streaming
	// matters: a spoke returning from a months-long outage can present
	// hundreds of thousands of entries (~20MB compressed), so the HTTPS
	// implementation must stream the request and response bodies rather than
	// buffering them, even though the values it produces are materialized.
	// If a deployment ever outgrows an in-memory pending slice, this becomes
	// an iterator — a breaking change deliberately deferred until real.
	Reconcile(ctx context.Context, hubID string, pending []*LedgerEntry) (*ReconcileResult, error)

	// PutFile streams one file's bytes to the hub, resuming from offset.
	//
	// body must yield the file's content starting at that offset; the caller
	// owns opening, seeking, and closing it. offset is absolute within the
	// file, so a resumed transfer sends only the remaining tail.
	//
	// Implementations must read ONLY the content-describing fields of entry —
	// Path, SHA256, SizeBytes, Database, Measurement, PartitionTime. The rest
	// (State, Attempts, BytesSent, SyncedAt, LastError, ID) is spoke-private
	// bookkeeping that no hub has any business seeing, and a transport that
	// branched on it would be reading state a sneakernet bundle or S3 relay
	// cannot meaningfully carry. The whole entry is passed rather than a
	// narrower descriptor to avoid a lossy conversion at every call site.
	//
	// A non-nil error means the transfer did not complete. A nil error with a
	// result whose Outcome is not OutcomeCommitted or OutcomeAlreadyPresent
	// means the hub answered deliberately (partial, conflict, backpressure)
	// and the caller must act on the Outcome rather than assume success.
	PutFile(ctx context.Context, hubID string, entry *LedgerEntry, body io.Reader, offset int64) (*PutResult, error)
}

// ReconcileResult is the hub's answer to a reconcile request: a partition of
// the spoke's pending set into what must be sent, what is already there, and
// what disagrees.
type ReconcileResult struct {
	// Missing lists storage-relative paths the hub does not have. These are
	// what the agent streams, newest-first.
	Missing []string

	// Present lists paths the hub already holds with a matching SHA256. The
	// agent advances these straight to synced without sending a byte.
	//
	// This is the lost-ack recovery path: a transfer that completed but whose
	// acknowledgment never arrived leaves the spoke believing the file is
	// pending. Reconcile discovers the truth in bulk, which is why a lost ack
	// costs one redundant entry in the next batch rather than a re-upload.
	Present []string

	// Conflicts lists paths the hub holds with a DIFFERENT SHA256.
	//
	// This is an alarm, not a retry: it means either two spokes are writing
	// the same namespaced path (a spoke_id collision) or one side's bytes are
	// corrupt. The agent must not resend — overwriting would destroy whichever
	// copy is correct. Surfacing conflicts here catches the whole backlog at
	// once, rather than discovering them one 409 at a time during transfer.
	Conflicts []Conflict
}

// Conflict is one same-path-different-content disagreement between spoke and
// hub.
type Conflict struct {
	Path string // storage-relative path, as the spoke knows it
	// TheirSHA256 is the digest the hub holds. The spoke's own digest is in
	// its ledger entry for the same path; the pair is what an operator needs
	// to work out which side is wrong.
	TheirSHA256 string
}

// PutOutcome is the machine-readable result of a PutFile call.
//
// It is a distinct type rather than an HTTP status because the caller's
// response differs per outcome and must not depend on parsing an error
// string — and because a non-HTTP transport (S3 relay, sneakernet bundle) has
// to express the same set without inventing status codes.
type PutOutcome string

const (
	// OutcomeCommitted — the hub verified the checksum and durably stored the
	// file. Advance the ledger to synced.
	OutcomeCommitted PutOutcome = "committed"

	// OutcomeAlreadyPresent — the hub already had this exact content at this
	// path, so the write was a no-op. Treated identically to committed: this
	// is what makes redelivery harmless and turns at-least-once delivery into
	// exactly-once effect.
	OutcomeAlreadyPresent PutOutcome = "already_present"

	// OutcomePartial — the hub accepted a prefix but not the whole file,
	// typically because the link dropped mid-stream. PutResult.BytesAccepted
	// is the new resume checkpoint; the file stays pending.
	OutcomePartial PutOutcome = "partial"

	// OutcomeConflict — same path, different content. The hub refused to
	// overwrite. Do NOT retry: this needs an operator, not a backoff.
	OutcomeConflict PutOutcome = "conflict"

	// OutcomeChecksumMismatch — the bytes arrived corrupted and the hub
	// discarded them without committing. Retrying is correct and worthwhile:
	// the corruption may be in flight, or in the spoke's own storage (edge
	// hardware in the field is exactly where bit-rot shows up).
	OutcomeChecksumMismatch PutOutcome = "checksum_mismatch"

	// OutcomeBackpressure — the hub is overloaded and asked the spoke to slow
	// down. PutResult.RetryAfter carries how long to wait. Not a failure: at
	// fan-in scale with many spokes, this is the hub's normal flow control.
	OutcomeBackpressure PutOutcome = "backpressure"
)

// Retryable reports whether re-attempting the transfer unchanged could
// succeed.
//
// Conflict is the one outcome that is emphatically not retryable — resending
// cannot resolve a content disagreement and would risk overwriting good data.
// Committed and AlreadyPresent are "not retryable" only because there is
// nothing left to do.
func (o PutOutcome) Retryable() bool {
	switch o {
	case OutcomePartial, OutcomeChecksumMismatch, OutcomeBackpressure:
		return true
	default:
		return false
	}
}

// Done reports whether the hub now holds the file, so the ledger entry can
// advance to synced.
func (o PutOutcome) Done() bool {
	return o == OutcomeCommitted || o == OutcomeAlreadyPresent
}

// PutResult is the outcome of one PutFile call.
type PutResult struct {
	Outcome PutOutcome

	// BytesAccepted is the absolute offset the hub has durably accepted, and
	// becomes the ledger's resume checkpoint. Meaningful for OutcomePartial;
	// equals the file size for a committed transfer.
	BytesAccepted int64

	// RetryAfter is how long the hub asked the spoke to wait. Only set for
	// OutcomeBackpressure.
	RetryAfter time.Duration

	// TheirSHA256 is the hub's digest for the path. Only set for
	// OutcomeConflict, where it is the evidence an operator needs.
	TheirSHA256 string
}

// ErrTransportClosed is returned by a transport that has been shut down.
var ErrTransportClosed = errors.New("edgesync: transport closed")

// Validate reports whether the result is internally consistent for its
// outcome.
//
// A transport is remote code as far as the agent is concerned — a buggy or
// hostile hub could answer OutcomePartial with a negative offset, or claim
// backpressure with no delay, and the agent would write nonsense into the
// ledger. Implementations should call this before returning, and the agent
// should call it on anything it receives.
func (r *PutResult) Validate(entry *LedgerEntry) error {
	if r == nil {
		return errors.New("edgesync: nil put result")
	}

	switch r.Outcome {
	case OutcomeCommitted, OutcomeAlreadyPresent, OutcomePartial,
		OutcomeConflict, OutcomeChecksumMismatch, OutcomeBackpressure:
	default:
		return fmt.Errorf("edgesync: unknown put outcome %q", r.Outcome)
	}

	if r.BytesAccepted < 0 {
		return fmt.Errorf("edgesync: negative bytes accepted (%d)", r.BytesAccepted)
	}
	if entry != nil && r.BytesAccepted > entry.SizeBytes {
		return fmt.Errorf("edgesync: hub accepted %d bytes, more than the file's %d",
			r.BytesAccepted, entry.SizeBytes)
	}

	// Fields that belong to exactly one outcome. A hub setting them elsewhere
	// is confused about its own answer, and the caller would act on a delay
	// or a digest that means nothing.
	if r.RetryAfter != 0 && r.Outcome != OutcomeBackpressure {
		return fmt.Errorf("edgesync: retry delay set on a %q outcome", r.Outcome)
	}
	if r.TheirSHA256 != "" && r.Outcome != OutcomeConflict {
		return fmt.Errorf("edgesync: hub sha256 set on a %q outcome", r.Outcome)
	}

	switch r.Outcome {
	case OutcomeCommitted, OutcomeAlreadyPresent:
		// A terminal success must account for the whole file. The agent
		// advances the ledger to synced on Done(), writing BytesAccepted as
		// the final byte count — and because synced is terminal, an
		// under-report is never corrected and silently poisons lag reporting.
		if entry != nil && r.BytesAccepted != entry.SizeBytes {
			return fmt.Errorf("edgesync: %q outcome accepted %d of %d bytes",
				r.Outcome, r.BytesAccepted, entry.SizeBytes)
		}
	case OutcomePartial:
		// A "partial" that accepted everything is a contradiction, and one
		// that accepted nothing gives the caller no progress to record —
		// both would leave the agent looping without advancing.
		if entry != nil && r.BytesAccepted >= entry.SizeBytes {
			return fmt.Errorf("edgesync: partial outcome accepted the whole file (%d of %d)",
				r.BytesAccepted, entry.SizeBytes)
		}
	case OutcomeConflict:
		// Without the hub's digest a conflict is unactionable: an operator
		// cannot tell a spoke_id collision from corruption.
		if r.TheirSHA256 == "" {
			return errors.New("edgesync: conflict outcome without the hub's sha256")
		}
		if entry != nil && r.TheirSHA256 == entry.SHA256 {
			return fmt.Errorf("edgesync: conflict outcome but sha256 matches (%s) — not a conflict",
				r.TheirSHA256)
		}
	case OutcomeBackpressure:
		// Zero would busy-loop against an overloaded hub, which is the
		// opposite of what backpressure is for.
		if r.RetryAfter <= 0 {
			return errors.New("edgesync: backpressure outcome without a retry delay")
		}
		// The hub declined to take the file; claiming progress would advance
		// a checkpoint past bytes it never accepted.
		if r.BytesAccepted != 0 {
			return fmt.Errorf("edgesync: backpressure outcome accepted %d bytes", r.BytesAccepted)
		}
	case OutcomeChecksumMismatch:
		// The bytes were discarded, so nothing was accepted. A non-zero count
		// would move the resume checkpoint forward over content the hub threw
		// away, and the retry would skip it.
		if r.BytesAccepted != 0 {
			return fmt.Errorf("edgesync: checksum-mismatch outcome accepted %d bytes", r.BytesAccepted)
		}
	}

	return nil
}

// Validate reports whether the reconcile result is internally consistent.
//
// The agent drives ledger state directly from these lists, so a hub that
// reports the same path as both missing and present would produce
// contradictory transitions. Checking here keeps that from reaching the
// ledger.
func (r *ReconcileResult) Validate() error {
	if r == nil {
		return errors.New("edgesync: nil reconcile result")
	}

	seen := make(map[string]string, len(r.Missing)+len(r.Present)+len(r.Conflicts))

	for _, list := range []struct {
		name  string
		paths []string
	}{
		{"missing", r.Missing},
		{"present", r.Present},
	} {
		for _, p := range list.paths {
			if p == "" {
				return fmt.Errorf("edgesync: empty path in %s", list.name)
			}
			if prev, dup := seen[p]; dup {
				return fmt.Errorf("edgesync: path %q reported as both %s and %s", p, prev, list.name)
			}
			seen[p] = list.name
		}
	}

	for _, c := range r.Conflicts {
		if c.Path == "" {
			return errors.New("edgesync: empty path in conflicts")
		}
		if c.TheirSHA256 == "" {
			return fmt.Errorf("edgesync: conflict for %q without the hub's sha256", c.Path)
		}
		if prev, dup := seen[c.Path]; dup {
			return fmt.Errorf("edgesync: path %q reported as both %s and conflicts", c.Path, prev)
		}
		seen[c.Path] = "conflicts"
	}

	return nil
}
