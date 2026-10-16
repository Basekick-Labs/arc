package cluster

import (
	"context"
	"fmt"
	"time"
)

// Local copies of vanished files (#1207 S4b1c2b).
//
// A file deleted from the manifest as vanished was on no node anyone could
// reach, and its rows were published again from a standby copy. A node that
// was down then, or the origin back with the volume the cluster wrote off,
// may still hold it. Queries read every local file, so that copy would
// duplicate the republished rows: it is removed as soon as this node's
// manifest is current, and checked again every pass.

const (
	vanishedSweepEvery = 30 * time.Second
	// vanishedResweepEvery: the list unchanged, a pass runs this often
	// (a copy restored from a backup, say).
	vanishedResweepEvery = 10 * time.Minute
)

// vanishedSweepState is what one node's sweep remembers between passes.
type vanishedSweepState struct {
	swept   map[string]bool // paths handed to the delete workers
	lastSig string          // the list the last complete pass swept
	lastAt  time.Time
}

func (c *Coordinator) vanishedSweepLoop(ctx context.Context) {
	t := time.NewTicker(vanishedSweepEvery)
	defer t.Stop()
	st := &vanishedSweepState{swept: map[string]bool{}}
	for {
		c.vanishedSweep(ctx, st)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// vanishedSweep removes the local copies of listed vanished files that are
// not in the manifest (again) and not waiting to register here.
func (c *Coordinator) vanishedSweep(ctx context.Context, st *vanishedSweepState) {
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() {
		return
	}
	if c.localNode.Role == RoleWriter && c.pendingRegistration.Load() == nil {
		return // a file waiting to register here would not be recognised yet
	}
	list := fsm.VanishedFiles()
	if len(list) == 0 {
		return
	}
	sig := fmt.Sprintf("%d/%s", len(list), list[len(list)-1].Path)
	if sig == st.lastSig && time.Since(st.lastAt) < vanishedResweepEvery {
		return
	}
	swept := st.swept
	sctx, cancel := context.WithTimeout(ctx, claimantSyncBudget)
	err := c.SyncManifest(sctx)
	cancel()
	if err != nil {
		return
	}
	listed := make(map[string]bool, len(list))
	for _, v := range list {
		listed[v.Path] = true
		if swept[v.Path] {
			continue
		}
		if _, ok := fsm.GetFile(v.Path); ok {
			continue // registered again: not the vanished file
		}
		if pending := c.pendingRegistration.Load(); pending != nil && (*pending)(v.Path) {
			continue
		}
		size, err := c.storage.StatFile(ctx, v.Path)
		if err != nil || size != v.SizeBytes {
			continue
		}
		swept[v.Path] = true
		c.logger.Warn().Str("path", v.Path).
			Msg("Removing a local copy of a file the cluster found on no node: its rows were published again from a standby copy")
		c.enqueueLocalDelete(v.Path, "vanished-copy")
	}
	for p := range swept {
		if !listed[p] {
			delete(swept, p)
		}
	}
	st.lastSig, st.lastAt = sig, time.Now()
}
