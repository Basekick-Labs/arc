package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// The staged compaction commit, compaction side (#1207 d2a2a). With the
// cluster's compaction commit on and local storage, a job writes its output
// to a staged key instead of its final path and leaves its inputs alone; the
// CompletionWatcher stages it in the cluster, waits until another node holds
// the output, and commits it (register the output and delete the inputs in
// one entry, only if the inputs are unchanged). Nothing is lost if the
// compactor dies at any step: until the commit the inputs are the data, and
// after it the output is on at least two nodes.

// StagedRoot is the reserved storage root of staged outputs. It must match
// raft.StagedCompactionRoot (the cluster package checks).
const StagedRoot = "_compaction_staged"

// StagedKey is where job's output waits for its commit. It must match
// raft.StagedCompactionKey.
func StagedKey(jobID, outputPath string) string {
	return StagedRoot + "/" + jobID + "/" + path.Base(outputPath)
}

// StagedJobDir is the directory holding job's staged output.
func StagedJobDir(jobID string) string {
	return StagedRoot + "/" + jobID
}

// StagedJob is a staged job as the watcher hands it to the cluster.
type StagedJob struct {
	JobID     string
	StagedKey string
	Output    CompactedOutput
	Inputs    []StagedInput
}

// StagedOutcome is how the cluster answered a stage or commit.
type StagedOutcome int

const (
	// StagedOK: the command applied.
	StagedOK StagedOutcome = iota
	// StagedCommitted: the job's output is registered by this job (a commit
	// that applied earlier).
	StagedCommitted
	// StagedRetry: not now (a pause, the staged table full, holders not yet
	// there, no leader); try again later.
	StagedRetry
	// StagedRefused: never (an input changed, the output path is taken);
	// the job is dropped.
	StagedRefused
)

// StagedStatus is a job's state in the cluster.
type StagedStatus struct {
	Staged     bool // staged, not committed
	Committed  bool // its output is registered by this job
	Holders    int
	MinHolders int
}

// StagedCommitter is the cluster side of the staged commit.
type StagedCommitter interface {
	Stage(ctx context.Context, job StagedJob) (StagedOutcome, error)
	Commit(ctx context.Context, job StagedJob) (StagedOutcome, error)
	Abort(ctx context.Context, jobID, reason string) error
	// Status reads the local view; call SyncManifest first when it decides.
	Status(job StagedJob) StagedStatus
	SyncManifest(ctx context.Context) error
	// Registered reports whether path is in the cluster manifest.
	Registered(path string) bool
	// HoldsRegistration reports whether this node's registration outbox
	// still owns path (pending, parked, or just confirmed).
	HoldsRegistration(path string) bool
	// Adopt moves this node's staged copy of a committed job's output into
	// place, through the same path a peer uses; it returns once the output
	// is at its final path, or with an error.
	Adopt(ctx context.Context, job StagedJob) error
	// ForcePull pulls the committed output from another node (this node's
	// staged copy is gone).
	ForcePull(job StagedJob)
	// OwnStaged lists the uncommitted jobs this node staged.
	OwnStaged() []string
}

// InputLookup reports how the cluster manifest lists path: its size and
// checksum, and false when it is not a hot registered file or belongs to a
// staged job (#1207 d2a2a).
type InputLookup func(path string) (size int64, sha string, ok bool)

// runStaged is the end of Job.Run for a staged job (#1207 d2a2a): the output
// goes to its staged key and is made durable, the inputs stay, and the
// completion manifest moves to "staged" (only once the output is durable, so
// a manifest in that state always has its output). No crash-recovery storage
// manifest is written: nothing here deletes inputs, so there is nothing to
// recover, and the staged job keeps them out of other jobs.
func (j *Job) runStaged(ctx context.Context, compactedFile, compactedKey, outputSHA string) error {
	mover, ok := j.StorageBackend.(storage.ObjectMover)
	if !ok || !j.clusterMode() || outputSHA == "" {
		return j.fail(fmt.Errorf("staged compaction needs a local backend in cluster mode"))
	}
	inputs := make([]StagedInput, 0, len(j.compactedFiles))
	for _, f := range j.compactedFiles {
		in, ok := j.inputs[f]
		if !ok {
			return j.fail(fmt.Errorf("staged compaction: input %s is not described by the cluster manifest", f))
		}
		inputs = append(inputs, in)
	}
	stagedKey := StagedKey(j.JobID, compactedKey)
	if err := j.uploadFile(ctx, compactedFile, stagedKey); err != nil {
		return j.fail(fmt.Errorf("failed to write the staged output: %w", err))
	}
	if err := mover.Sync(ctx, stagedKey); err != nil {
		return j.fail(fmt.Errorf("failed to make the staged output durable: %w", err))
	}
	now := time.Now().UTC()
	out := CompactedOutput{
		Path: compactedKey, SHA256: outputSHA, SizeBytes: j.BytesAfter,
		Database: canonicalManifestDatabase(j.Database), Measurement: j.Measurement,
		PartitionTime: j.PartitionTime, Tier: j.Tier, CreatedAt: now,
	}
	m := &CompletionManifest{
		JobID: j.JobID, Database: canonicalManifestDatabase(j.Database), Measurement: j.Measurement,
		PartitionPath: j.PartitionPath, Tier: j.Tier, State: CompletionStateStaged,
		StagedKey: stagedKey, StagedOutput: &out, Inputs: inputs,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := writeCompletionManifest(j.CompletionDir, m); err != nil {
		return j.fail(fmt.Errorf("failed to write the staged completion manifest: %w", err))
	}
	j.logger.Info().Str("staged_key", stagedKey).Str("output", compactedKey).Int("inputs", len(inputs)).
		Msg("Compaction output staged; the cluster commits it")
	return j.complete()
}

// SetStagedCommit wires the staged compaction commit (#1207 d2a2a). mode
// reports whether a job launched now is staged; lookup describes a candidate
// file as the cluster manifest lists it; locallyStaged reports an input of a
// job this node staged that the cluster has not seen yet.
func (m *Manager) SetStagedCommit(mode func() bool, lookup InputLookup, locallyStaged func(path string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stagedMode, m.inputLookup, m.locallyStaged = mode, lookup, locallyStaged
}

// stagedCandidate reports whether candidate runs staged and, if so, the files
// it may compact: each listed hot in the cluster manifest at its size on this
// node, owned by no staged job here or in the cluster, with the size and
// checksum the job stages.
func (m *Manager) stagedCandidate(ctx context.Context, candidate Candidate) (bool, []string, map[string]StagedInput) {
	m.mu.Lock()
	mode, lookup, local := m.stagedMode, m.inputLookup, m.locallyStaged
	m.mu.Unlock()
	if mode == nil || lookup == nil || !mode() {
		return false, nil, nil
	}
	inputs := make(map[string]StagedInput, len(candidate.Files))
	var files []string
	for _, f := range candidate.Files {
		size, sha, ok := lookup(f)
		if !ok || (local != nil && local(f)) {
			continue
		}
		if got, err := m.StorageBackend.StatFile(ctx, f); err != nil || got != size {
			continue
		}
		inputs[f] = StagedInput{Path: f, SizeBytes: size, SHA256: sha}
		files = append(files, f)
	}
	return true, files, inputs
}

// BeforeStagedCommit runs the compacted-output observer for a staged job
// about to commit, before its output can appear at its final path (#610).
func (m *Manager) BeforeStagedCommit(output string) {
	m.notifyCompactedOutput(output)
}

// FinishStaged runs, for a committed staged job, what a legacy job runs when
// its subprocess returns: the consumed-inputs observer (its error keeps the
// job for a retry) and the cache invalidation.
func (m *Manager) FinishStaged(inputs []string) error {
	if err := m.notifyConsumedInputs(inputs); err != nil {
		return err
	}
	m.mu.Lock()
	onComplete := m.onCompactionComplete
	m.mu.Unlock()
	if onComplete != nil {
		onComplete()
	}
	return nil
}

// runJobInSubprocess is RunJobInSubprocess; tests replace it so a cycle never
// starts a real subprocess (in a test that is the test binary itself).
var runJobInSubprocess = RunJobInSubprocess

// maxJobIDLen keeps a job id within what the cluster accepts for a staged
// compaction (#1207 d2a2a), and a file name: a long database and partition
// path would otherwise make one the cluster refuses every time.
const maxJobIDLen = 200

// boundJobID shortens a job id over maxJobIDLen to a prefix and a checksum
// of the whole id, keeping it unique.
func boundJobID(id string) string {
	if len(id) <= maxJobIDLen {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return id[:maxJobIDLen-17] + "_" + hex.EncodeToString(sum[:8])
}
