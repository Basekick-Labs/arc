package raft

import "errors"

// ErrManifestApply is the sentinel returned (wrapped) by every manifest
// mutation that failed to commit through Raft — RegisterFile, DeleteFile,
// BatchFileOps, UpdateFile. Callers can `errors.Is(err, ErrManifestApply)`
// to recognize a Raft-side failure (quorum loss, leader unreachable,
// commit timeout) without having to substring-match wrapped error
// messages. Lives in the leaf raft package so consumers (cluster,
// reconciliation, retention, compaction) can all import it without
// re-introducing import cycles.
var ErrManifestApply = errors.New("raft: manifest apply failed")

// ErrBarrierUnsupported is wrapped by a follower's manifest barrier when the
// leader refuses the barrier command itself (a leader whose build predates
// it). The follower cannot sync its manifest through that leader at all, as
// opposed to failing to reach it in time. A leaf sentinel so the reconciler
// can tell the two apart without importing cluster (#1214).
var ErrBarrierUnsupported = errors.New("raft: the leader does not accept the manifest barrier")
