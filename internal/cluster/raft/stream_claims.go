package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Stream claims (#1207 S4a).
//
// When a standby writer takes over from a primary that died, it claims the
// dead primary instance's batches up to W (per buffer key) before it writes
// anything, and later replays the copy it received. From then on the dead
// instance's own registrations of rows at or below W are refused: the rows
// belong to the claimant, and registering both copies would duplicate them.
//
// Every rule here is applied by every node's FSM, so it must be the same
// pure function of the log everywhere — including on nodes that only read.
// An older build skips unknown commands without an error the cluster can
// see, and ignores the coverage field of a registration. Claims are
// therefore inert until the StreamClaims flag is applied, and the flag is
// applied only when every node in the cluster runs a build that supports it.

// CapabilityStreamClaims is advertised in the join request by a build that
// applies stream claims. A writer advertises it only when it also runs with
// cluster.wal_coverage, so the flag implies every writer stamps coverage.
const CapabilityStreamClaims = "stream_claims_v1"

const (
	// Bounds on claims state, which every node keeps and snapshots.
	maxStreamClaims        = 1024
	maxStreamClaimKeys     = 4096
	maxStreamClaimKeyLen   = 512
	maxStreamClaimKeysAll  = 1 << 16 // across every claim, bounding replicated state
	MaxTakeOverPayloadSize = 64 << 10
	streamClaimTTL         = 7 * 24 * time.Hour
	maxConflictOpsReported = 64
)

// FileCoverage is the part of a file's coverage the claim rule needs: per
// writer instance and buffer key, the lowest batch the file holds. Truncated
// is set when the journal left ranges out, so the lowest batch is unknown.
// ClaimEpoch is set on files a takeover replay registers: they are exempt
// from the claim with that epoch.
type FileCoverage struct {
	Instance   uint64 `json:"i"`
	Key        string `json:"k"`
	MinCSeq    uint64 `json:"c"`
	Truncated  bool   `json:"t,omitempty"`
	ClaimEpoch uint64 `json:"e,omitempty"`
}

// StreamClaim is one instance's batches up to W (per key) claimed by By.
type StreamClaim struct {
	Instance uint64            `json:"instance"`
	Origin   string            `json:"origin"`
	By       string            `json:"by"`
	Epoch    uint64            `json:"epoch"`
	W        map[string]uint64 `json:"w"`
	At       time.Time         `json:"at"`
}

// EnableStreamClaimsPayload turns the flag on. Nodes lists the node ids the
// proposer saw advertising the capability; Apply refuses unless every node
// in this FSM's table is among them, which every node decides the same way
// from the node table (old snapshots carry it).
type EnableStreamClaimsPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// TakeOverStreamPayload claims an instance's batches. Now is the proposer's
// clock: Apply never reads its own.
type TakeOverStreamPayload struct {
	Instance uint64            `json:"instance"`
	Origin   string            `json:"origin"`
	By       string            `json:"by"`
	Epoch    uint64            `json:"epoch"`
	W        map[string]uint64 `json:"w"`
	Now      time.Time         `json:"now"`
}

// streamClaimsState is the snapshot form.
type streamClaimsState struct {
	Enabled bool          `json:"enabled"`
	Claims  []StreamClaim `json:"claims,omitempty"`
}

const streamClaimConflictText = "stream claim conflict"

// StreamClaimConflictError refuses registrations of rows another writer has
// claimed. Ops are the indexes of the refused operations in the batch (0 for
// a single registration). Terminal: retrying cannot succeed.
type StreamClaimConflictError struct {
	Ops []int
}

func (e *StreamClaimConflictError) Error() string {
	ops, _ := json.Marshal(e.Ops)
	return fmt.Sprintf("%s: ops %s", streamClaimConflictText, ops)
}

// IsStreamClaimConflict reports a claim conflict, from the FSM error itself
// or from its text as a forwarded refusal carries it.
func IsStreamClaimConflict(err error) bool {
	var ce *StreamClaimConflictError
	return errors.As(err, &ce) || (err != nil && strings.Contains(err.Error(), streamClaimConflictText))
}

// StreamClaimConflictOps returns the refused op indexes of a claim conflict,
// typed or as text.
func StreamClaimConflictOps(err error) []int {
	var ce *StreamClaimConflictError
	if errors.As(err, &ce) {
		return ce.Ops
	}
	if err == nil {
		return nil
	}
	text := err.Error()
	i := strings.Index(text, streamClaimConflictText+": ops ")
	if i < 0 {
		return nil
	}
	rest := text[i+len(streamClaimConflictText+": ops "):]
	end := strings.IndexByte(rest, ']')
	if end < 0 {
		return nil
	}
	var ops []int
	if json.Unmarshal([]byte(rest[:end+1]), &ops) != nil {
		return nil
	}
	return ops
}

// StreamClaimsEnabled reports whether the flag is applied.
func (f *ClusterFSM) StreamClaimsEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.streamClaims
}

// StreamClaimCount reports how many claims are held.
func (f *ClusterFSM) StreamClaimCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.claims)
}

// StreamClaims returns a copy of the claims, ordered by instance.
func (f *ClusterFSM) StreamClaims() []StreamClaim {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.claimsListLocked()
}

func (f *ClusterFSM) claimsListLocked() []StreamClaim {
	out := make([]StreamClaim, 0, len(f.claims))
	for _, c := range f.claims {
		cc := *c
		cc.W = make(map[string]uint64, len(c.W))
		for k, v := range c.W {
			cc.W[k] = v
		}
		out = append(out, cc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Instance < out[j].Instance })
	return out
}

func (f *ClusterFSM) applyEnableStreamClaims(payload []byte) interface{} {
	var p EnableStreamClaimsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable stream claims payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.streamClaims {
		return nil
	}
	listed := make(map[string]bool, len(p.Nodes))
	for _, n := range p.Nodes {
		listed[n] = true
	}
	var missing []string
	for id := range f.nodes {
		if !listed[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("enable stream claims: nodes %v are not known to support them", missing)
	}
	f.streamClaims = true
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Stream claims enabled")
	return nil
}

func (f *ClusterFSM) applyTakeOverStream(payload []byte) interface{} {
	if len(payload) > MaxTakeOverPayloadSize {
		return fmt.Errorf("take over stream: payload of %d bytes exceeds %d", len(payload), MaxTakeOverPayloadSize)
	}
	var p TakeOverStreamPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal take over stream payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case !f.streamClaims:
		return errors.New("take over stream: stream claims are not enabled")
	case p.Instance == 0 || p.Origin == "" || p.By == "" || p.Epoch == 0 || p.Now.IsZero():
		return errors.New("take over stream: instance, origin, by, epoch and now are required")
	case p.By == p.Origin:
		return errors.New("take over stream: a node cannot claim its own instance")
	case len(p.W) == 0 || len(p.W) > maxStreamClaimKeys:
		return fmt.Errorf("take over stream: %d keys, want 1 to %d", len(p.W), maxStreamClaimKeys)
	}
	for k := range p.W {
		if len(k) == 0 || len(k) > maxStreamClaimKeyLen {
			return fmt.Errorf("take over stream: key length %d, want 1 to %d", len(k), maxStreamClaimKeyLen)
		}
	}
	by, ok := f.nodes[p.By]
	if !ok || by.Role != "writer" {
		return fmt.Errorf("take over stream: %s is not a writer in the cluster", p.By)
	}
	if old := f.claims[p.Instance]; old != nil && p.Epoch <= old.Epoch {
		return fmt.Errorf("take over stream: epoch %d is not after the current claim epoch %d", p.Epoch, old.Epoch)
	}
	// Expire by the proposer's clock, then bound.
	for inst, c := range f.claims {
		if p.Now.Sub(c.At) > streamClaimTTL {
			delete(f.claims, inst)
		}
	}
	if _, exists := f.claims[p.Instance]; !exists && len(f.claims) >= maxStreamClaims {
		return fmt.Errorf("take over stream: %d claims already held", maxStreamClaims)
	}
	total := len(p.W)
	for inst, c := range f.claims {
		if inst != p.Instance {
			total += len(c.W)
		}
	}
	if total > maxStreamClaimKeysAll {
		return fmt.Errorf("take over stream: %d keys across all claims exceed %d", total, maxStreamClaimKeysAll)
	}
	if f.claims == nil {
		f.claims = make(map[uint64]*StreamClaim)
	}
	w := make(map[string]uint64, len(p.W))
	for k, v := range p.W {
		w[k] = v
	}
	f.claims[p.Instance] = &StreamClaim{Instance: p.Instance, Origin: p.Origin, By: p.By, Epoch: p.Epoch, W: w, At: p.Now}
	f.logger.Info().Str("origin", p.Origin).Str("by", p.By).Uint64("epoch", p.Epoch).Int("keys", len(w)).
		Msg("Stream claim applied: the claimant now owns the instance batches it holds")
	return nil
}

// claimConflictLocked reports whether registering p would register rows
// another writer has claimed. Caller holds f.mu (read or write).
func (f *ClusterFSM) claimConflictLocked(p RegisterFilePayload) bool {
	if !f.streamClaims || len(f.claims) == 0 || len(p.Coverage) == 0 {
		return false
	}
	// A retry of a registration that already committed is not a conflict:
	// the file is the one already in the manifest.
	if e, ok := f.files[p.File.Path]; ok && e.SHA256 == p.File.SHA256 && e.SizeBytes == p.File.SizeBytes && e.OriginNodeID == p.File.OriginNodeID {
		return false
	}
	for _, cov := range p.Coverage {
		c := f.claims[cov.Instance]
		if c == nil {
			continue
		}
		if cov.ClaimEpoch != 0 && cov.ClaimEpoch == c.Epoch {
			continue // the claimant's own replay
		}
		w, ok := c.W[cov.Key]
		if !ok {
			continue
		}
		if cov.Truncated || cov.MinCSeq <= w {
			return true
		}
	}
	return false
}
