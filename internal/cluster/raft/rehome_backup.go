package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Backup marks (#1857). A backup (or a restore) copies the bucket's files
// while it runs; a re-home job that moved a file under it would leave the
// copy with the file and the files replacing it, or neither. The node running
// one keeps a mark in the cluster record, renewed while it runs, and a job is
// not staged while a live mark exists: a node that took over as primary writer
// sees a backup still running on the former one.

// BackupMarkLease: a mark not renewed for this long is ignored (its node
// stopped without ending it). The holder renews it every BackupMarkLease/3.
const BackupMarkLease = 90 * time.Second

// RehomeBackupRunning is the conflict reason of a stage refused for a mark.
const RehomeBackupRunning = "backup-running"

// BackupMark is one backup or restore running somewhere.
type BackupMark struct {
	ID        string    `json:"id"`
	Node      string    `json:"node"`
	RenewedAt time.Time `json:"renewed_at"`
}

// BackupMarkPayload begins, renews (Renew) or ends (End) a mark. A renew of a
// mark that is gone or lapsed is refused: the backup has not been protected
// throughout, and a mark must not come back to life over a job staged
// meanwhile.
type BackupMarkPayload struct {
	ID    string    `json:"id"`
	Node  string    `json:"node"`
	Now   time.Time `json:"now"`
	Renew bool      `json:"renew,omitempty"`
	End   bool      `json:"end,omitempty"`
}

// ErrBackupMarkLapsed: a renew found its mark gone or lapsed.
var ErrBackupMarkLapsed = errors.New("backup mark lapsed")

func (f *ClusterFSM) applyBackupMark(payload []byte) interface{} {
	var p BackupMarkPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal backup mark payload: %w", err)
	}
	if p.ID == "" || p.Node == "" || (!p.End && p.Now.IsZero()) {
		return fmt.Errorf("backup mark: id, node and now are required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !p.End {
		// Lapsed marks go (the proposer's clock, as every check of them).
		for k, m := range f.backupMarks {
			if p.Now.Sub(m.RenewedAt) >= BackupMarkLease {
				delete(f.backupMarks, k)
			}
		}
	}
	if p.End {
		if m, ok := f.backupMarks[p.ID]; ok && m.Node == p.Node {
			delete(f.backupMarks, p.ID)
		}
		return nil
	}
	if _, known := f.nodes[p.Node]; !known {
		return fmt.Errorf("backup mark: %q is not a member", p.Node)
	}
	if p.Renew {
		m, ok := f.backupMarks[p.ID]
		if !ok || m.Node != p.Node {
			return fmt.Errorf("%w: %s", ErrBackupMarkLapsed, p.ID)
		}
		m.RenewedAt = p.Now
		return nil
	}
	if _, exists := f.backupMarks[p.ID]; exists {
		return fmt.Errorf("backup mark %s exists", p.ID)
	}
	f.backupMarks[p.ID] = &BackupMark{ID: p.ID, Node: p.Node, RenewedAt: p.Now}
	return nil
}

// backupRunningLocked reports a mark renewed within BackupMarkLease of now.
func (f *ClusterFSM) backupRunningLocked(now time.Time) bool {
	for _, m := range f.backupMarks {
		if now.Sub(m.RenewedAt) < BackupMarkLease {
			return true
		}
	}
	return false
}

// backupMarksNodeRemovedLocked drops a removed node's marks.
func (f *ClusterFSM) backupMarksNodeRemovedLocked(id string) {
	for k, m := range f.backupMarks {
		if m.Node == id {
			delete(f.backupMarks, k)
		}
	}
}

// BackupMarks returns copies of the marks, ordered by id.
func (f *ClusterFSM) BackupMarks() []BackupMark {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]BackupMark, 0, len(f.backupMarks))
	for _, m := range f.backupMarks {
		out = append(out, *m)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}
