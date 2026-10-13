package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Compaction commit (#1207 d2).
//
// A compaction used to register its output and delete its inputs in two Raft
// entries, after its subprocess had already unlinked the inputs, and nothing
// checked that the inputs were still the files it read. Two things went
// wrong:
//
//   - a row DELETE that rewrote an input while the job ran left the job's
//     output (built from the input before the DELETE) registered next to the
//     rewrite: the deleted rows came back and the others were doubled;
//   - peers unlinked their copies of the inputs as soon as the deletes
//     applied, while the output existed only on the compactor until a peer
//     pulled it: losing the compactor's disk in that window lost the rows on
//     every node.
//
// With the CompactionCommit flag on, a job is staged first. The FSM records
// the output and the exact inputs (each at the size and checksum the job
// read), peers pull the staged output (local storage), and the commit then
// registers the output and deletes the inputs in one entry, only if every
// input is still registered unchanged. A commit that finds an input changed
// is refused: the inputs were never touched, so the job is simply dropped.
//
// Row DELETE batches become compare-and-swap with the flag on: a rewrite
// registers only while the file it rewrites is still registered at the
// checksum the DELETE read, and a delete with an expected checksum applies
// only to that content. Between the two, a compaction and a DELETE of the
// same file cannot both win.
//
// The flag is applied only when every node supports it (an older build skips
// the new commands and ignores the new fields, so its manifest would accept
// what every other node refuses).

// CapabilityCompactionCommit is advertised at join by every build that
// applies the rules above.
const CapabilityCompactionCommit = "compaction_commit_v1"

// Bounds on the replicated compaction state.
const (
	// maxStagedCompactions bounds the staged jobs. A compactor runs a few
	// jobs at a time and a staged job lives for seconds, so reaching it means
	// jobs are not being committed or aborted.
	maxStagedCompactions = 1024
	// MaxCompactionInputs bounds the inputs of one job (compaction's
	// max_files_per_batch is far below it).
	MaxCompactionInputs = 10000
	// maxCompactionJobLen bounds a job id, which is also a delete reason.
	maxCompactionJobLen = 256
)

// Compaction conflict reasons: why a staged-commit command was refused.
const (
	// CompactionInputsChanged: an input is no longer registered at the size
	// and checksum the job read (a DELETE, retention, tiering or another
	// compaction got there first).
	CompactionInputsChanged = "inputs-changed"
	// CompactionUnknownJob: no staged job by that id, and its output is not
	// registered by it either.
	CompactionUnknownJob = "unknown-job"
	// CompactionPaused: a cluster compaction pause started after the job was
	// staged, or is in force.
	CompactionPaused = "paused"
	// CompactionOutputExists: the output path is registered by something
	// other than this job.
	CompactionOutputExists = "output-exists"
	// CompactionJobExists: the job id is staged with another output.
	CompactionJobExists = "job-exists"
	// CompactionFull: maxStagedCompactions reached.
	CompactionFull = "full"
	// CompactionCommitted: the job's output is already registered by it.
	CompactionCommitted = "committed"
	// CompactionNotFinalizing / CompactionFinalizing: a finalize or revert of
	// a job that is not finalizing, or an abort of one that is.
	CompactionNotFinalizing = "not-finalizing"
	CompactionFinalizing    = "finalizing"
	// CompactionFinalized: a revert of a job whose output is in the bucket
	// already (finalized), or a cleanup of a job that is not.
	CompactionFinalized    = "finalized"
	CompactionNotFinalized = "not-finalized"
	// CompactionNotOrigin: a finalize or revert by a node other than the
	// job's compactor, without the leader's override.
	CompactionNotOrigin = "not-origin"
	// CompactionOutputGone: a revert of a job whose output left the manifest
	// (a DELETE, retention or DROP removed it): its inputs are not wanted
	// back.
	CompactionOutputGone = "output-gone"
	// CompactionInputsRegistered: a revert whose input paths are registered
	// again.
	CompactionInputsRegistered = "inputs-registered"
)

// CompactionInput is one input of a staged job as the job read it.
type CompactionInput struct {
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// StagedCompaction is a compaction job between StageCompaction and its end.
//
// Object marks a job on an object store: there is no staged copy to pull (the
// bucket is the durable store). Its states after the commit:
//
//   - Finalizing: the output is registered and the inputs are not, but the
//     output may not be in the bucket yet (its compactor uploads it now) and
//     the input objects are still there, so reads see the inputs' rows.
//     Inputs keep their full entries: RevertCompaction registers them again
//     if the output never reaches the bucket.
//   - Finalized (FinalizeCompaction): the output is in the bucket; the input
//     objects are deleted next, by any node (CleanupCompaction ends the job).
//
// Finalize and revert are proposed by the job's compactor, or by the leader
// with Override once that compactor is gone; nothing else can race them.
type StagedCompaction struct {
	Job        string      `json:"job"`
	Origin     string      `json:"origin"`
	Output     FileEntry   `json:"output"`
	Inputs     []FileEntry `json:"inputs"`
	Object     bool        `json:"object,omitempty"`
	Holders    []string    `json:"holders,omitempty"`
	Finalizing bool        `json:"finalizing,omitempty"`
	Finalized  bool        `json:"finalized,omitempty"`
	// PauseGen is the compaction pause generation when the job was staged:
	// a pause that started since (a restore) refuses its commit.
	PauseGen uint64    `json:"pause_gen,omitempty"`
	At       time.Time `json:"at"`
	Index    uint64    `json:"index"`
}

// EnableCompactionCommitPayload turns the flag on; see
// EnableStreamClaimsPayload for the Nodes rule.
type EnableCompactionCommitPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// StageCompactionPayload stages a job. Output is the entry the commit will
// register (its OriginNodeID and CompactionJob are set by the FSM).
type StageCompactionPayload struct {
	Job    string            `json:"job"`
	Origin string            `json:"origin"`
	Output FileEntry         `json:"output"`
	Inputs []CompactionInput `json:"inputs"`
	Object bool              `json:"object,omitempty"`
	Now    time.Time         `json:"now"`
}

// CompactionHeldPayload: Node holds the staged output of Job, at SHA256,
// durably (local storage).
type CompactionHeldPayload struct {
	Job    string `json:"job"`
	Node   string `json:"node"`
	SHA256 string `json:"sha256"`
}

// CommitCompactionPayload commits a staged job. Output and SHA256 name the
// output so a commit retried after it applied is recognised; Now is the
// proposer's clock, judged against a pause's expiry.
type CommitCompactionPayload struct {
	Job    string    `json:"job"`
	Origin string    `json:"origin"`
	Output string    `json:"output"`
	SHA256 string    `json:"sha256"`
	Now    time.Time `json:"now"`
}

// AbortCompactionPayload drops a staged (not finalizing) job. Any node may
// propose it: the inputs of a staged job were never touched.
type AbortCompactionPayload struct {
	Job    string `json:"job"`
	By     string `json:"by"`
	Reason string `json:"reason,omitempty"`
}

// FinalizeCompactionPayload: a finalizing job's output is in the bucket.
// Output names it, so a finalize retried after the job ended is recognised.
// Override: proposed by the leader for a compactor that is gone (never
// forwarded).
type FinalizeCompactionPayload struct {
	Job      string `json:"job"`
	By       string `json:"by"`
	Output   string `json:"output"`
	Override bool   `json:"override,omitempty"`
}

// RevertCompactionPayload undoes a finalizing job whose output never reached
// the bucket: the output entry is removed and the inputs registered again.
// Override as for FinalizeCompactionPayload.
type RevertCompactionPayload struct {
	Job      string `json:"job"`
	By       string `json:"by"`
	Override bool   `json:"override,omitempty"`
}

// CleanupCompactionPayload ends a finalized job: its input objects are gone.
// Any node may propose it (deleting an unregistered input object again is
// harmless).
type CleanupCompactionPayload struct {
	Job string `json:"job"`
	By  string `json:"by"`
}

// compactionCommitState is the snapshot form; nil until the flag was applied.
type compactionCommitState struct {
	Enabled bool                `json:"enabled"`
	Staged  []*StagedCompaction `json:"staged,omitempty"`
}

// CompactionConflictError is a refused staged-commit command.
type CompactionConflictError struct {
	Job    string
	Reason string
}

const compactionConflictText = "compaction commit refused"

func (e *CompactionConflictError) Error() string {
	return fmt.Sprintf("%s: %s: job %s", compactionConflictText, e.Reason, e.Job)
}

// CompactionConflictReason returns the reason of a refused staged-commit
// command, typed or as text (a forwarded apply returns the text), or "".
func CompactionConflictReason(err error) string {
	var ce *CompactionConflictError
	if errors.As(err, &ce) {
		return ce.Reason
	}
	if err == nil {
		return ""
	}
	text := err.Error()
	i := strings.Index(text, compactionConflictText+": ")
	if i < 0 {
		return ""
	}
	rest := text[i+len(compactionConflictText)+2:]
	if j := strings.Index(rest, ": job "); j >= 0 {
		return rest[:j]
	}
	return ""
}

// ManifestChangedError refuses a row DELETE batch whose file is no longer
// registered at the content the DELETE read (#1207 d2).
type ManifestChangedError struct {
	Paths []string
}

const manifestChangedText = "manifest changed under the operation"

func (e *ManifestChangedError) Error() string {
	return fmt.Sprintf("%s: paths %s", manifestChangedText, strings.Join(e.Paths, ", "))
}

// IsManifestChanged reports a ManifestChangedError, typed or as text.
func IsManifestChanged(err error) bool {
	var me *ManifestChangedError
	return errors.As(err, &me) || (err != nil && strings.Contains(err.Error(), manifestChangedText))
}

// CompactionCommitEnabled reports whether the flag is applied.
func (f *ClusterFSM) CompactionCommitEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.compactionCommit
}

// StagedCompactions returns a copy of every staged job, by job id.
func (f *ClusterFSM) StagedCompactions() []StagedCompaction {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]StagedCompaction, 0, len(f.staged))
	for _, s := range f.staged {
		out = append(out, copyStaged(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job < out[j].Job })
	return out
}

// StagedCompactionCount returns the number of staged jobs.
func (f *ClusterFSM) StagedCompactionCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.staged)
}

// StagedCompactionByJob returns a copy of one staged job.
func (f *ClusterFSM) StagedCompactionByJob(job string) (StagedCompaction, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, ok := f.staged[job]
	if !ok {
		return StagedCompaction{}, false
	}
	return copyStaged(s), true
}

func copyStaged(s *StagedCompaction) StagedCompaction {
	c := *s
	c.Inputs = append([]FileEntry(nil), s.Inputs...)
	c.Holders = append([]string(nil), s.Holders...)
	return c
}

func (f *ClusterFSM) applyEnableCompactionCommit(payload []byte) interface{} {
	var p EnableCompactionCommitPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable compaction commit payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.compactionCommit {
		return nil
	}
	listed := map[string]bool{}
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
		return fmt.Errorf("enable compaction commit: nodes %v are not known to support it", missing)
	}
	f.compactionCommit = true
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Compaction commit enabled")
	return nil
}

// inputUnchangedLocked reports whether in is registered, hot, at its size
// and checksum.
func (f *ClusterFSM) inputUnchangedLocked(in CompactionInput) bool {
	e, ok := f.files[in.Path]
	return ok && e.SizeBytes == in.SizeBytes && e.SHA256 == in.SHA256 && (e.Tier == "" || e.Tier == "hot")
}

func (f *ClusterFSM) applyStageCompaction(payload []byte, logIndex uint64) interface{} {
	var p StageCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal stage compaction payload: %w", err)
	}
	if p.Job == "" || len(p.Job) > maxCompactionJobLen || p.Origin == "" {
		return errors.New("stage compaction: job and origin are required")
	}
	if err := ValidateManifestPath(p.Output.Path); err != nil {
		return f.rejectManifestPath("stage compaction", p.Output.Path, logIndex, err)
	}
	if p.Output.CreatedAt.IsZero() || p.Output.SHA256 == "" {
		return errors.New("stage compaction: output created_at and sha256 are required")
	}
	if len(p.Inputs) == 0 || len(p.Inputs) > MaxCompactionInputs {
		return fmt.Errorf("stage compaction: %d inputs (want 1..%d)", len(p.Inputs), MaxCompactionInputs)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionCommit {
		return errors.New("stage compaction: compaction commit is not enabled")
	}
	if s, ok := f.staged[p.Job]; ok {
		if s.Origin == p.Origin && s.Output.Path == p.Output.Path && s.Output.SHA256 == p.Output.SHA256 {
			return nil
		}
		return &CompactionConflictError{Job: p.Job, Reason: CompactionJobExists}
	}
	if e, ok := f.files[p.Output.Path]; ok {
		if e.CompactionJob == p.Job && e.SHA256 == p.Output.SHA256 {
			return &CompactionConflictError{Job: p.Job, Reason: CompactionCommitted}
		}
		return &CompactionConflictError{Job: p.Job, Reason: CompactionOutputExists}
	}
	if f.compactionPause.Active && p.Now.Before(f.compactionPause.ExpiresAt) {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionPaused}
	}
	if len(f.staged) >= maxStagedCompactions {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionFull}
	}
	seen := map[string]bool{}
	var inputs []FileEntry
	for _, in := range p.Inputs {
		if seen[in.Path] || in.Path == p.Output.Path {
			return fmt.Errorf("stage compaction: input %q listed twice or as the output", in.Path)
		}
		seen[in.Path] = true
		if !f.inputUnchangedLocked(in) {
			return &CompactionConflictError{Job: p.Job, Reason: CompactionInputsChanged}
		}
		e := f.files[in.Path]
		if e.Database != p.Output.Database || e.Measurement != p.Output.Measurement {
			return fmt.Errorf("stage compaction: input %q is not in %s/%s", in.Path, p.Output.Database, p.Output.Measurement)
		}
		inputs = append(inputs, *e)
	}
	out := p.Output
	out.OriginNodeID = p.Origin
	out.CompactionJob = p.Job
	out.Coverage, out.CoverageInherited, out.LSN = nil, false, 0
	if out.Tier == "" {
		out.Tier = "hot"
	}
	f.staged[p.Job] = &StagedCompaction{
		Job: p.Job, Origin: p.Origin, Output: out, Inputs: inputs, Object: p.Object,
		PauseGen: f.compactionPause.Generation, At: p.Now, Index: logIndex,
	}
	f.logger.Debug().Str("job", p.Job).Str("output", out.Path).Int("inputs", len(inputs)).Bool("object", p.Object).
		Msg("Compaction staged")
	return nil
}

func (f *ClusterFSM) applyCompactionHeld(payload []byte) interface{} {
	var p CompactionHeldPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal compaction held payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionCommit {
		return errors.New("compaction held: compaction commit is not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok || s.Finalizing {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionUnknownJob}
	}
	if s.Object || p.SHA256 != s.Output.SHA256 {
		return fmt.Errorf("compaction held: job %s does not take a copy at %s", p.Job, p.SHA256)
	}
	if _, known := f.nodes[p.Node]; !known || p.Node == s.Origin {
		return fmt.Errorf("compaction held: %q is not another member", p.Node)
	}
	for _, h := range s.Holders {
		if h == p.Node {
			return nil
		}
	}
	s.Holders = append(s.Holders, p.Node)
	sort.Strings(s.Holders)
	return nil
}

func (f *ClusterFSM) applyCommitCompaction(payload []byte, logIndex uint64) interface{} {
	var p CommitCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal commit compaction payload: %w", err)
	}
	f.mu.Lock()
	if !f.compactionCommit {
		f.mu.Unlock()
		return errors.New("commit compaction: compaction commit is not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok || s.Finalizing {
		// A commit retried after it applied (its reply lost to a leader
		// change) finds its own output registered: done.
		e, reg := f.files[p.Output]
		done := reg && e.CompactionJob == p.Job && e.SHA256 == p.SHA256
		if ok && s.Finalizing && s.Output.Path == p.Output && s.Output.SHA256 == p.SHA256 {
			done = true
		}
		f.mu.Unlock()
		if done {
			return nil
		}
		return &CompactionConflictError{Job: p.Job, Reason: CompactionUnknownJob}
	}
	if s.Origin != p.Origin || s.Output.Path != p.Output || s.Output.SHA256 != p.SHA256 {
		f.mu.Unlock()
		return fmt.Errorf("commit compaction: job %s was staged by %s for %s", p.Job, s.Origin, s.Output.Path)
	}
	pause := f.compactionPause
	if pause.Generation != s.PauseGen || (pause.Active && p.Now.Before(pause.ExpiresAt)) {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionPaused}
	}
	for _, in := range s.Inputs {
		if !f.inputUnchangedLocked(CompactionInput{Path: in.Path, SizeBytes: in.SizeBytes, SHA256: in.SHA256}) {
			f.mu.Unlock()
			return &CompactionConflictError{Job: p.Job, Reason: CompactionInputsChanged}
		}
	}
	if _, reg := f.files[s.Output.Path]; reg {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionOutputExists}
	}
	var events []func()
	if emit := f.applyRegisterFileLocked(RegisterFilePayload{File: s.Output}, logIndex, nil); emit != nil {
		events = append(events, emit)
	}
	reason := "compaction:" + p.Job
	for _, in := range s.Inputs {
		if emit := f.applyDeleteFileLocked(DeleteFilePayload{Path: in.Path, Reason: reason}); emit != nil {
			events = append(events, emit)
		}
	}
	if s.Object {
		s.Finalizing, s.Holders = true, nil
	} else {
		delete(f.staged, p.Job)
	}
	f.mu.Unlock()
	for _, emit := range events {
		emit()
	}
	return nil
}

func (f *ClusterFSM) applyAbortCompaction(payload []byte) interface{} {
	var p AbortCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal abort compaction payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionCommit {
		return errors.New("abort compaction: compaction commit is not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok {
		return nil
	}
	if s.Finalizing {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionFinalizing}
	}
	delete(f.staged, p.Job)
	f.logger.Info().Str("job", p.Job).Str("by", p.By).Str("reason", p.Reason).Msg("Staged compaction aborted")
	return nil
}

func (f *ClusterFSM) applyFinalizeCompaction(payload []byte) interface{} {
	var p FinalizeCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal finalize compaction payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionCommit {
		return errors.New("finalize compaction: compaction commit is not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok {
		// Retried after the job ended: done while the output is still the
		// one this job registered (a revert removed it).
		if e, reg := f.files[p.Output]; reg && e.CompactionJob == p.Job {
			return nil
		}
		return &CompactionConflictError{Job: p.Job, Reason: CompactionUnknownJob}
	}
	if !s.Finalizing {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotFinalizing}
	}
	if s.Origin != p.By && !p.Override {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotOrigin}
	}
	s.Finalized = true
	return nil
}

func (f *ClusterFSM) applyCleanupCompaction(payload []byte) interface{} {
	var p CleanupCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal cleanup compaction payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionCommit {
		return errors.New("cleanup compaction: compaction commit is not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok {
		return nil
	}
	if !s.Finalized {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotFinalized}
	}
	delete(f.staged, p.Job)
	return nil
}

func (f *ClusterFSM) applyRevertCompaction(payload []byte, logIndex uint64) interface{} {
	var p RevertCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal revert compaction payload: %w", err)
	}
	f.mu.Lock()
	if !f.compactionCommit {
		f.mu.Unlock()
		return errors.New("revert compaction: compaction commit is not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionUnknownJob}
	}
	if !s.Finalizing {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotFinalizing}
	}
	if s.Finalized {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionFinalized}
	}
	if s.Origin != p.By && !p.Override {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotOrigin}
	}
	if e, reg := f.files[s.Output.Path]; !reg || e.CompactionJob != p.Job || e.SHA256 != s.Output.SHA256 {
		f.mu.Unlock()
		return &CompactionConflictError{Job: p.Job, Reason: CompactionOutputGone}
	}
	for _, in := range s.Inputs {
		if _, reg := f.files[in.Path]; reg {
			f.mu.Unlock()
			return &CompactionConflictError{Job: p.Job, Reason: CompactionInputsRegistered}
		}
	}
	var events []func()
	if emit := f.applyDeleteFileLocked(DeleteFilePayload{Path: s.Output.Path, Reason: "compaction-revert:" + p.Job}); emit != nil {
		events = append(events, emit)
	}
	for _, in := range s.Inputs {
		if emit := f.applyRegisterFileLocked(RegisterFilePayload{File: in}, logIndex, nil); emit != nil {
			events = append(events, emit)
		}
		// The input's batches stayed settled through the commit (a
		// compaction delete keeps them), so its coverage comes back as the
		// record it was, with the index untouched.
		if r := f.files[in.Path]; r != nil {
			r.Coverage, r.CoverageInherited = in.Coverage, in.CoverageInherited
		}
	}
	delete(f.staged, p.Job)
	f.mu.Unlock()
	f.logger.Warn().Str("job", p.Job).Str("by", p.By).Int("inputs", len(s.Inputs)).
		Msg("Compaction reverted: its output never reached storage, its inputs are registered again")
	for _, emit := range events {
		emit()
	}
	return nil
}

// compactionNodeRemovedLocked drops what a removed node held in staged jobs:
// its holder entries, and the jobs it staged that are not committed (their
// inputs were never touched; nothing will commit them). A finalizing job
// stays for the leader to finish.
func (f *ClusterFSM) compactionNodeRemovedLocked(id string) {
	for job, s := range f.staged {
		if s.Origin == id && !s.Finalizing {
			delete(f.staged, job)
			continue
		}
		kept := s.Holders[:0]
		for _, h := range s.Holders {
			if h != id {
				kept = append(kept, h)
			}
		}
		s.Holders = kept
	}
}

// manifestCASRefusalsLocked returns the paths a row DELETE batch read at a
// content that changed since (flag on): a rewrite whose source is gone or
// changed, a delete whose file is gone or changed. Empty when the flag is off.
func (f *ClusterFSM) manifestCASRefusalsLocked(ops []BatchFileOp, decoded []any) []string {
	if !f.compactionCommit {
		return nil
	}
	var refused []string
	for i, op := range ops {
		switch op.Type {
		case CommandRegisterFile:
			rp := decoded[i].(RegisterFilePayload)
			if rp.InheritFrom != "" && !f.registeredAtLocked(rp.InheritFrom, rp.InheritSHA) {
				refused = append(refused, rp.InheritFrom)
			}
		case CommandDeleteFile:
			dp := decoded[i].(DeleteFilePayload)
			if dp.ExpectSHA != "" && !f.registeredAtLocked(dp.Path, dp.ExpectSHA) {
				refused = append(refused, dp.Path)
			}
		}
	}
	return refused
}

// registeredAtLocked reports whether path is registered, at sha when sha is
// set.
func (f *ClusterFSM) registeredAtLocked(path, sha string) bool {
	e, ok := f.files[path]
	return ok && (sha == "" || e.SHA256 == sha)
}

func (f *ClusterFSM) snapshotCompactionCommitLocked() *compactionCommitState {
	if !f.compactionCommit && len(f.staged) == 0 {
		return nil
	}
	st := &compactionCommitState{Enabled: f.compactionCommit}
	for _, s := range f.staged {
		c := copyStaged(s)
		st.Staged = append(st.Staged, &c)
	}
	sort.Slice(st.Staged, func(i, j int) bool { return st.Staged[i].Job < st.Staged[j].Job })
	return st
}

func (f *ClusterFSM) restoreCompactionCommitLocked(st *compactionCommitState) {
	f.compactionCommit, f.staged = false, map[string]*StagedCompaction{}
	if st == nil {
		return
	}
	f.compactionCommit = st.Enabled
	for _, s := range st.Staged {
		if s != nil && s.Job != "" {
			c := copyStaged(s)
			f.staged[s.Job] = &c
		}
	}
}
