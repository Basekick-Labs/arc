package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

// EnableStreamClaimsPayload turns the flag on. Nodes lists the node ids the
// proposer saw advertising the capability; Apply refuses unless every node
// in this FSM's table is among them, which every node decides the same way
// from the node table (old snapshots carry it).
type EnableStreamClaimsPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// streamClaimsState is the snapshot form. Claims is the S4a shape, never
// written by a cluster (the flag was never enabled before #1207 S4b) and
// ignored on restore.
type streamClaimsState struct {
	Enabled  bool              `json:"enabled"`
	Claims   []json.RawMessage `json:"claims,omitempty"`
	Takeover *takeoverSnapshot `json:"takeover,omitempty"`
}

// StreamClaimsEnabled reports whether the flag is applied.
func (f *ClusterFSM) StreamClaimsEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.streamClaims
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

// applyTakeOverStream refuses the S4a claim command: it was never proposed
// (nothing in the S4a builds did) and its watermark rule is gone. A claim is
// TakeOverStreamV2.
func (f *ClusterFSM) applyTakeOverStream(payload []byte) interface{} {
	return errors.New("take over stream: superseded by take over stream v2")
}
