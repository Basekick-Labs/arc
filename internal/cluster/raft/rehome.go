package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Re-home on a cluster whose nodes share one object store (#1825 slice 4).
//
// A re-home job replaces one misfiled data file (the source) with files in the
// directories its rows belong to (the outputs). Every node reads the same
// bucket, and what it leaves out of a read comes from this record, so the job's
// steps are Raft entries, and a step that changes what a node's reads must
// leave out waits for every node to say it has drained the reads from before:
//
//	staged     the record: the source as the manifest has it, the outputs
//	           uploaded under the staging root. Reads change nothing.
//	committed  reads leave every output out by name and read the source.
//	           Each node acks once its reads from before the apply ended.
//	published  the outputs are at their final keys (the finisher copied
//	           them); reads still leave them out. Each node acks once it has
//	           dropped its query caches, which may remember their directories
//	           as empty.
//	latched    one entry: the outputs are registered, the source's entry is
//	           deleted (reason rehome:<job>), and reads leave the source out
//	           and read the outputs. Each node acks once its reads from before
//	           ended. Then the source's object can go, and the job ends.
//	reverting  from staged, committed or published, before any storage is
//	           deleted: reads go on leaving the outputs out while the finisher
//	           deletes them.
//	reverted   the outputs are deleted. Reads still leave them out, and their
//	           keys stay owned, for RehomeRevertedGrace more: a copy by a
//	           finisher that lost its lease and did not know it can still land
//	           in that time. Then the finisher deletes the keys again and ends
//	           it.
//
// The FSM refuses an advance until every node the step waits for has acked
// the current state, and refuses any manifest change to a path a job owns
// except its own latch. A job is driven by the holder of its finisher lease:
// its origin from the stage, and whoever claims it after the lease lapsed.
//
// Applied only once every node supports it, and only with compaction commit
// on: the refusals rely on its content checks, and a compaction that did not
// stage could merge the source under the job.

// CapabilityRehome is advertised at join by every build that applies the
// rules above.
const CapabilityRehome = "rehome_v1"

// CapabilityRehomeInScope is advertised by a node whose configuration a job
// can run under (no tiered storage, no Iceberg export, no edge sync: their
// state is per node, and a job does not account for it). StageRehome refuses
// while any node lacks it.
const CapabilityRehomeInScope = "rehome_in_scope_v1"

// Re-home job states.
const (
	RehomeStaged    = "staged"
	RehomeCommitted = "committed"
	RehomePublished = "published"
	RehomeLatched   = "latched"
	RehomeReverting = "reverting"
	RehomeReverted  = "reverted"
)

// Bounds on the replicated re-home state.
const (
	// maxRehomeJobs bounds the jobs in flight (one runs at a time per node).
	maxRehomeJobs = 64
	// MaxRehomeOutputs bounds the outputs of one job, so its stage entry
	// stays far under the forward frame.
	MaxRehomeOutputs = 500
	// RehomeStagedRoot is the reserved storage root of a job's uploaded
	// outputs before they are copied to their final keys.
	RehomeStagedRoot = "_rehome_staged"
	// maxRehomeStageBytes bounds a stage entry's encoded size, well under
	// the forward frame (file_registrar.go).
	maxRehomeStageBytes = 128 << 10
	// RehomeCopyTimeout bounds one copy of an output to its final key: a
	// driver must give up on a copy it began by then.
	RehomeCopyTimeout = 10 * time.Minute
	// RehomeRevertedGrace is how long a reverted job keeps its output keys
	// owned and left out of reads: a driver that lost its lease without
	// knowing it can still have a copy in flight for its lease plus one
	// copy.
	RehomeRevertedGrace = FinisherLease + RehomeCopyTimeout
)

// Re-home conflict reasons.
const (
	RehomeSourceChanged = "source-changed"
	RehomeUnknownJob    = "unknown-job"
	RehomeJobExists     = "job-exists"
	RehomeOwned         = "owned"
	RehomeOutputExists  = "output-exists"
	RehomeFull          = "full"
	RehomeWrongState    = "wrong-state"
	RehomeAcksMissing   = "acks-missing"
	RehomeNotFinisher   = "not-finisher"
	RehomeClaimed       = "claimed"
	RehomePaused        = "paused"
	RehomeTooSoon       = "too-soon"
	RehomeOutOfScope    = "out-of-scope"
)

// RehomeConflictError is a re-home command refused for a reason a caller acts
// on.
type RehomeConflictError struct {
	Job    string
	Reason string
}

const rehomeConflictText = "re-home refused"

func (e *RehomeConflictError) Error() string {
	return fmt.Sprintf("%s: %s: job %s", rehomeConflictText, e.Reason, e.Job)
}

// RehomeConflictReason returns the reason of a refused re-home command, typed
// or as text (a forwarded apply returns the text), or "".
func RehomeConflictReason(err error) string {
	var ce *RehomeConflictError
	if errors.As(err, &ce) {
		return ce.Reason
	}
	if err == nil {
		return ""
	}
	msg := err.Error()
	i := strings.Index(msg, rehomeConflictText+": ")
	if i < 0 {
		return ""
	}
	rest := msg[i+len(rehomeConflictText)+2:]
	if j := strings.Index(rest, ": job "); j >= 0 {
		return rest[:j]
	}
	return ""
}

// RehomeOutput is one output of a job.
type RehomeOutput struct {
	Key           string    `json:"key"`
	StagedKey     string    `json:"staged_key"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
	PartitionTime time.Time `json:"partition_time"`
}

// RehomeJob is a job between StageRehome and its end.
type RehomeJob struct {
	ID          string `json:"id"`
	Origin      string `json:"origin"`
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	// Source is the source's manifest entry as it was when the job was
	// staged; the latch requires it unchanged.
	Source  FileEntry      `json:"source"`
	Outputs []RehomeOutput `json:"outputs"`
	State   string         `json:"state"`
	// StateIndex is the log index of the entry that set State.
	StateIndex uint64 `json:"state_index"`
	// AckWanted is the nodes the current state waits for (the node table
	// when it was set, less nodes removed since); Acks those that acked it.
	AckWanted []string        `json:"ack_wanted,omitempty"`
	Acks      map[string]bool `json:"acks,omitempty"`
	// FinisherNode holds the job's lease since ClaimedAt (proposer clock).
	FinisherNode string    `json:"finisher_node,omitempty"`
	ClaimedAt    time.Time `json:"claimed_at,omitempty"`
	// Dropped is the nodes removed from the cluster while the current state
	// waited for their ack. The driver waits for such a node to have stopped
	// reading (the read lease, cluster/read_lease.go) before it goes on.
	Dropped []string `json:"dropped,omitempty"`
	// RevertedAt is when a reverting job's outputs were deleted (the
	// proposer's clock).
	RevertedAt time.Time `json:"reverted_at,omitempty"`
	// PauseGen is the compaction pause generation when the job was staged:
	// a pause since (a restore) refuses every advance but a revert.
	PauseGen uint64    `json:"pause_gen,omitempty"`
	At       time.Time `json:"at"`
	Index    uint64    `json:"index"`
}

// Copy returns a deep copy.
func (j *RehomeJob) Copy() RehomeJob {
	c := *j
	c.Outputs = append([]RehomeOutput(nil), j.Outputs...)
	c.AckWanted = append([]string(nil), j.AckWanted...)
	c.Dropped = append([]string(nil), j.Dropped...)
	if j.Acks != nil {
		c.Acks = make(map[string]bool, len(j.Acks))
		for k, v := range j.Acks {
			c.Acks[k] = v
		}
	}
	return c
}

// MissingAcks returns the nodes the current state still waits for.
func (j *RehomeJob) MissingAcks() []string {
	var out []string
	for _, n := range j.AckWanted {
		if !j.Acks[n] {
			out = append(out, n)
		}
	}
	return out
}

// Payloads.

// EnableRehomePayload turns the flag on; see EnableStreamClaimsPayload for
// the Nodes rule.
type EnableRehomePayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// StageRehomePayload stages a job. Source names the file at the size and
// checksum the job read it.
type StageRehomePayload struct {
	Job         string          `json:"job"`
	Origin      string          `json:"origin"`
	Database    string          `json:"database"`
	Measurement string          `json:"measurement"`
	Source      CompactionInput `json:"source"`
	Outputs     []RehomeOutput  `json:"outputs"`
	Now         time.Time       `json:"now"`
}

// AdvanceRehomePayload moves a job to To. By must hold the job's lease (or,
// with no lease taken, be its origin).
type AdvanceRehomePayload struct {
	Job string    `json:"job"`
	By  string    `json:"by"`
	To  string    `json:"to"`
	Now time.Time `json:"now"`
}

// AckRehomePayload: Node has done what the job's State asks of it.
type AckRehomePayload struct {
	Job   string `json:"job"`
	Node  string `json:"node"`
	State string `json:"state"`
}

// ClaimRehomeFinisherPayload takes or renews a job's lease.
type ClaimRehomeFinisherPayload struct {
	Job string    `json:"job"`
	By  string    `json:"by"`
	Now time.Time `json:"now"`
}

// EndRehomePayload ends a latched job (its source object and staged objects
// deleted); moves a reverting one to reverted (its outputs and staged objects
// deleted); and ends a reverted one FinisherLease after that (its output keys
// deleted again).
type EndRehomePayload struct {
	Job string    `json:"job"`
	By  string    `json:"by"`
	Now time.Time `json:"now"`
}

// rehomeState is the snapshot form; nil until the flag was applied.
type rehomeState struct {
	Enabled bool         `json:"enabled"`
	Jobs    []*RehomeJob `json:"jobs,omitempty"`
	// Gen is the log index of the last change to what jobs leave out.
	Gen uint64 `json:"gen,omitempty"`
}

// RehomeStagedKey is where an output waits for its copy to its final key.
func RehomeStagedKey(job string, i int) string {
	return fmt.Sprintf("%s/%s/%06d.parquet", RehomeStagedRoot, job, i)
}

// rehomeTransitions lists the states each state may advance to.
var rehomeTransitions = map[string][]string{
	RehomeStaged:    {RehomeCommitted, RehomeReverting},
	RehomeCommitted: {RehomePublished, RehomeReverting},
	RehomePublished: {RehomeLatched, RehomeReverting},
}

// rehomeNeedsAcks reports whether leaving state to to waits for every node's
// ack of state. A revert does not: reads leave the outputs out throughout.
func rehomeNeedsAcks(state, to string) bool {
	return to != RehomeReverting && (state == RehomeCommitted || state == RehomePublished)
}

func (f *ClusterFSM) applyEnableRehome(payload []byte, logIndex uint64) interface{} {
	var p EnableRehomePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable re-home payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rehomeEnabled {
		return nil
	}
	if !f.compactionCommit {
		return errors.New("enable re-home: compaction commit is not enabled")
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
		return fmt.Errorf("enable re-home: nodes %v are not known to support it", missing)
	}
	f.rehomeEnabled = true
	f.rehomeGen = logIndex
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Re-home enabled")
	return nil
}

func (f *ClusterFSM) applyStageRehome(payload []byte, logIndex uint64) interface{} {
	var p StageRehomePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal stage re-home payload: %w", err)
	}
	if !validCompactionJob(p.Job) || p.Origin == "" || p.Database == "" || p.Measurement == "" {
		return errors.New("stage re-home: a job id of letters, digits, _ - . (not leading .), an origin, a database and a measurement are required")
	}
	if err := ValidateManifestPath(p.Source.Path); err != nil {
		return f.rejectManifestPath("stage re-home", p.Source.Path, logIndex, err)
	}
	if len(payload) > maxRehomeStageBytes {
		return fmt.Errorf("stage re-home: entry of %d bytes (max %d)", len(payload), maxRehomeStageBytes)
	}
	if len(p.Outputs) == 0 || len(p.Outputs) > MaxRehomeOutputs {
		return fmt.Errorf("stage re-home: %d outputs (want 1..%d)", len(p.Outputs), MaxRehomeOutputs)
	}
	if p.Now.IsZero() {
		return errors.New("stage re-home: now is required")
	}
	prefix := p.Database + "/" + p.Measurement + "/"
	seen := map[string]bool{p.Source.Path: true}
	for i, o := range p.Outputs {
		if err := ValidateManifestPath(o.Key); err != nil {
			return f.rejectManifestPath("stage re-home", o.Key, logIndex, err)
		}
		if !strings.HasPrefix(o.Key, prefix) {
			return fmt.Errorf("stage re-home: output %q is not in %s", o.Key, prefix)
		}
		if o.StagedKey != RehomeStagedKey(p.Job, i) {
			return fmt.Errorf("stage re-home: output %d is staged at %q, want %q", i, o.StagedKey, RehomeStagedKey(p.Job, i))
		}
		if o.SizeBytes <= 0 || o.SHA256 == "" {
			return fmt.Errorf("stage re-home: output %q has no size or checksum", o.Key)
		}
		if seen[o.Key] {
			return fmt.Errorf("stage re-home: %q listed twice, or as the source", o.Key)
		}
		seen[o.Key] = true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rehomeEnabled {
		return errors.New("stage re-home: re-home is not enabled")
	}
	if _, known := f.nodes[p.Origin]; !known {
		return fmt.Errorf("stage re-home: origin %q is not a member", p.Origin)
	}
	if j, ok := f.rehomeJobs[p.Job]; ok {
		if j.Origin == p.Origin && j.Source.Path == p.Source.Path && j.Source.SHA256 == p.Source.SHA256 {
			return nil // a retry of this stage
		}
		return &RehomeConflictError{Job: p.Job, Reason: RehomeJobExists}
	}
	if len(f.rehomeJobs) >= maxRehomeJobs {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeFull}
	}
	if f.compactionPause.Active && p.Now.Before(f.compactionPause.ExpiresAt) {
		return &RehomeConflictError{Job: p.Job, Reason: RehomePaused}
	}
	// Stream takeover settles files by their coverage, and the latch
	// replaces a covered file with uncovered ones: out of scope.
	if f.streamClaims {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeOutOfScope}
	}
	for _, n := range f.nodes {
		if !containsString(n.Capabilities, CapabilityRehomeInScope) {
			return &RehomeConflictError{Job: p.Job, Reason: RehomeOutOfScope}
		}
	}
	if !f.inputUnchangedLocked(p.Source) {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeSourceChanged}
	}
	src := *f.files[p.Source.Path]
	if src.Database != p.Database || src.Measurement != p.Measurement {
		return fmt.Errorf("stage re-home: source %q is not in %s", p.Source.Path, prefix)
	}
	for path := range seen {
		if _, owned := f.stagedPaths[path]; owned {
			return &RehomeConflictError{Job: p.Job, Reason: RehomeOwned}
		}
		if _, owned := f.rehomePaths[path]; owned {
			return &RehomeConflictError{Job: p.Job, Reason: RehomeOwned}
		}
		if path == p.Source.Path {
			continue
		}
		if _, reg := f.files[path]; reg {
			return &RehomeConflictError{Job: p.Job, Reason: RehomeOutputExists}
		}
	}
	j := &RehomeJob{
		ID: p.Job, Origin: p.Origin, Database: p.Database, Measurement: p.Measurement,
		Source: src, Outputs: append([]RehomeOutput(nil), p.Outputs...),
		State: RehomeStaged, StateIndex: logIndex,
		FinisherNode: p.Origin, ClaimedAt: p.Now,
		PauseGen: f.compactionPause.Generation, At: p.Now, Index: logIndex,
	}
	f.rehomeJobs[p.Job] = j
	f.indexRehomeLocked(j)
	f.rehomeChangedLocked(j, logIndex, true)
	return nil
}

func (f *ClusterFSM) applyAdvanceRehome(payload []byte, logIndex uint64) interface{} {
	var p AdvanceRehomePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal advance re-home payload: %w", err)
	}
	if p.By == "" || p.Now.IsZero() {
		return errors.New("advance re-home: by and now are required")
	}
	f.mu.Lock()
	if !f.rehomeEnabled {
		f.mu.Unlock()
		return errors.New("advance re-home: re-home is not enabled")
	}
	j, ok := f.rehomeJobs[p.Job]
	if !ok {
		f.mu.Unlock()
		return &RehomeConflictError{Job: p.Job, Reason: RehomeUnknownJob}
	}
	if j.State == p.To {
		f.mu.Unlock()
		return nil // a retry after it applied
	}
	if !f.rehomeDriverLocked(j, p.By) {
		f.mu.Unlock()
		return &RehomeConflictError{Job: p.Job, Reason: RehomeNotFinisher}
	}
	allowed := false
	for _, to := range rehomeTransitions[j.State] {
		if to == p.To {
			allowed = true
		}
	}
	if !allowed {
		f.mu.Unlock()
		return &RehomeConflictError{Job: p.Job, Reason: RehomeWrongState}
	}
	if p.To != RehomeReverting {
		if f.compactionPause.Generation != j.PauseGen || (f.compactionPause.Active && p.Now.Before(f.compactionPause.ExpiresAt)) {
			f.mu.Unlock()
			return &RehomeConflictError{Job: p.Job, Reason: RehomePaused}
		}
	}
	if rehomeNeedsAcks(j.State, p.To) && len(j.MissingAcks()) > 0 {
		f.mu.Unlock()
		return &RehomeConflictError{Job: p.Job, Reason: RehomeAcksMissing}
	}
	var events []func()
	if p.To == RehomeLatched {
		// The source must be as the job read it: nothing else could change
		// it (its path is owned), but a retry of this entry must not run
		// twice against a manifest that moved.
		cur, reg := f.files[j.Source.Path]
		if !reg || cur.SHA256 != j.Source.SHA256 || cur.SizeBytes != j.Source.SizeBytes {
			f.mu.Unlock()
			return &RehomeConflictError{Job: p.Job, Reason: RehomeSourceChanged}
		}
		for _, o := range j.Outputs {
			if _, reg := f.files[o.Key]; reg {
				f.mu.Unlock()
				return &RehomeConflictError{Job: p.Job, Reason: RehomeOutputExists}
			}
		}
		// Built here from the job, not sent: a retry or replay registers the
		// same entries, and the latch entry stays small.
		for _, o := range j.Outputs {
			entry := FileEntry{
				Path: o.Key, SHA256: o.SHA256, SizeBytes: o.SizeBytes,
				Database: j.Database, Measurement: j.Measurement, PartitionTime: o.PartitionTime,
				OriginNodeID: j.Origin, Tier: "hot", CreatedAt: p.Now,
			}
			if emit := f.applyRegisterFileLocked(RegisterFilePayload{File: entry}, logIndex, nil); emit != nil {
				events = append(events, emit)
			}
		}
		if emit := f.applyDeleteFileLocked(DeleteFilePayload{Path: j.Source.Path, Reason: RehomeDeleteReason(j.ID)}); emit != nil {
			events = append(events, emit)
		}
	}
	j.State, j.StateIndex = p.To, logIndex
	j.Acks, j.AckWanted, j.Dropped = nil, nil, nil
	if p.To != RehomeReverting {
		for id := range f.nodes {
			j.AckWanted = append(j.AckWanted, id)
		}
		sort.Strings(j.AckWanted)
	}
	f.rehomeChangedLocked(j, logIndex, true)
	f.mu.Unlock()
	for _, emit := range events {
		emit()
	}
	return nil
}

// RehomeDeleteReason is the delete reason of a job's source at its latch.
func RehomeDeleteReason(job string) string { return "rehome:" + job }

func (f *ClusterFSM) applyAckRehome(payload []byte, logIndex uint64) interface{} {
	var p AckRehomePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal ack re-home payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rehomeEnabled {
		return errors.New("ack re-home: re-home is not enabled")
	}
	j, ok := f.rehomeJobs[p.Job]
	if !ok {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeUnknownJob}
	}
	if j.State != p.State {
		return nil // for a state the job has left: nothing to record
	}
	wanted := false
	for _, n := range j.AckWanted {
		if n == p.Node {
			wanted = true
		}
	}
	if !wanted || j.Acks[p.Node] {
		return nil
	}
	if j.Acks == nil {
		j.Acks = map[string]bool{}
	}
	j.Acks[p.Node] = true
	f.rehomeChangedLocked(j, logIndex, false)
	return nil
}

func (f *ClusterFSM) applyClaimRehomeFinisher(payload []byte, logIndex uint64) interface{} {
	var p ClaimRehomeFinisherPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal claim re-home finisher payload: %w", err)
	}
	if p.By == "" || p.Now.IsZero() {
		return errors.New("claim re-home finisher: by and now are required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rehomeEnabled {
		return errors.New("claim re-home finisher: re-home is not enabled")
	}
	j, ok := f.rehomeJobs[p.Job]
	if !ok {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeUnknownJob}
	}
	if j.FinisherNode != "" && j.FinisherNode != p.By && p.Now.Sub(j.ClaimedAt) <= FinisherLease {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeClaimed}
	}
	j.FinisherNode, j.ClaimedAt = p.By, p.Now
	f.rehomeChangedLocked(j, logIndex, false)
	return nil
}

func (f *ClusterFSM) applyEndRehome(payload []byte, logIndex uint64) interface{} {
	var p EndRehomePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal end re-home payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rehomeEnabled {
		return errors.New("end re-home: re-home is not enabled")
	}
	j, ok := f.rehomeJobs[p.Job]
	if !ok {
		return nil // a retry after it applied
	}
	if !f.rehomeDriverLocked(j, p.By) {
		return &RehomeConflictError{Job: p.Job, Reason: RehomeNotFinisher}
	}
	switch j.State {
	case RehomeLatched:
		if len(j.MissingAcks()) > 0 {
			return &RehomeConflictError{Job: p.Job, Reason: RehomeAcksMissing}
		}
	case RehomeReverting:
		if p.Now.IsZero() {
			return errors.New("end re-home: now is required")
		}
		j.State, j.StateIndex, j.RevertedAt = RehomeReverted, logIndex, p.Now
		f.rehomeChangedLocked(j, logIndex, true)
		return nil
	case RehomeReverted:
		if p.Now.Sub(j.RevertedAt) < RehomeRevertedGrace {
			return &RehomeConflictError{Job: p.Job, Reason: RehomeTooSoon}
		}
	default:
		return &RehomeConflictError{Job: p.Job, Reason: RehomeWrongState}
	}
	delete(f.rehomeJobs, p.Job)
	for path, id := range f.rehomePaths {
		if id == p.Job {
			delete(f.rehomePaths, path)
		}
	}
	f.rehomeGen = logIndex
	if cb := f.onRehomeEnded; cb != nil {
		c := j.Copy()
		f.pendingEvents = append(f.pendingEvents, func() { cb(c) })
	}
	return nil
}

// rehomeDriverLocked reports whether by may drive the job: it holds the
// lease (the origin from the stage, or the last claimant).
func (f *ClusterFSM) rehomeDriverLocked(j *RehomeJob, by string) bool {
	return by != "" && j.FinisherNode == by
}

// rehomeChangedLocked queues the callback for a change of a job. states:
// the change may change what reads leave out (a state, the job's set), which
// moves the generation; an ack or a claim does not.
func (f *ClusterFSM) rehomeChangedLocked(j *RehomeJob, logIndex uint64, states bool) {
	if states {
		f.rehomeGen = logIndex
	}
	if cb := f.onRehomeChanged; cb != nil {
		c := j.Copy()
		f.pendingEvents = append(f.pendingEvents, func() { cb(c) })
	}
}

// indexRehomeLocked records the paths a job owns: its source and outputs.
func (f *ClusterFSM) indexRehomeLocked(j *RehomeJob) {
	f.rehomePaths[j.Source.Path] = j.ID
	for _, o := range j.Outputs {
		f.rehomePaths[o.Key] = j.ID
	}
}

// rehomeOwnsLocked reports whether a re-home job owns path. Every manifest
// change to such a path is refused but the job's own latch.
func (f *ClusterFSM) rehomeOwnsLocked(path string) bool {
	if !f.rehomeEnabled {
		return false
	}
	_, ok := f.rehomePaths[path]
	return ok
}

// rehomeNodeRemovedLocked drops a removed node from what jobs wait for, and
// records it in Dropped when it had not acked: the driver then waits for it to
// have stopped reading.
func (f *ClusterFSM) rehomeNodeRemovedLocked(id string, logIndex uint64) {
	for _, j := range f.rehomeJobs {
		kept := j.AckWanted[:0]
		was := false
		for _, n := range j.AckWanted {
			if n == id {
				was = true
				continue
			}
			kept = append(kept, n)
		}
		if !was {
			continue
		}
		j.AckWanted = kept
		if !j.Acks[id] {
			j.Dropped = append(j.Dropped, id)
		}
		delete(j.Acks, id)
		f.rehomeChangedLocked(j, logIndex, false)
	}
}

// rehomeReferencesLocked reports whether a job names id (for
// ReferencesNode).
func (f *ClusterFSM) rehomeReferencesLocked(id string) bool {
	for _, j := range f.rehomeJobs {
		for _, n := range j.AckWanted {
			if n == id {
				return true
			}
		}
	}
	return false
}

func (f *ClusterFSM) snapshotRehomeLocked() *rehomeState {
	if !f.rehomeEnabled && len(f.rehomeJobs) == 0 {
		return nil
	}
	st := &rehomeState{Enabled: f.rehomeEnabled, Gen: f.rehomeGen}
	for _, j := range f.rehomeJobs {
		c := j.Copy()
		st.Jobs = append(st.Jobs, &c)
	}
	sort.Slice(st.Jobs, func(a, b int) bool { return st.Jobs[a].ID < st.Jobs[b].ID })
	return st
}

func (f *ClusterFSM) restoreRehomeLocked(st *rehomeState) {
	prev := f.rehomeJobs
	f.rehomeEnabled, f.rehomeGen = false, 0
	f.rehomeJobs, f.rehomePaths = map[string]*RehomeJob{}, map[string]string{}
	if st != nil {
		f.rehomeEnabled, f.rehomeGen = st.Enabled, st.Gen
		for _, j := range st.Jobs {
			if j != nil && j.ID != "" {
				c := j.Copy()
				f.rehomeJobs[j.ID] = &c
				f.indexRehomeLocked(&c)
			}
		}
	}
	// Announced as the commands would have: every job present, and every
	// job gone. A node that restored a job's state from a snapshot must still
	// ack it.
	for _, j := range f.rehomeJobs {
		if cb := f.onRehomeChanged; cb != nil {
			c := j.Copy()
			f.pendingEvents = append(f.pendingEvents, func() { cb(c) })
		}
	}
	for id, j := range prev {
		if _, kept := f.rehomeJobs[id]; !kept && f.onRehomeEnded != nil {
			c, cb := j.Copy(), f.onRehomeEnded
			f.pendingEvents = append(f.pendingEvents, func() { cb(c) })
		}
	}
}

// SetRehomeCallbacks registers what runs after a job changes (staged,
// advanced, acked, claimed; and every job present after a snapshot restore)
// and after one ends. Callbacks must not block.
func (f *ClusterFSM) SetRehomeCallbacks(changed func(RehomeJob), ended func(RehomeJob)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRehomeChanged, f.onRehomeEnded = changed, ended
}

// RehomeEnabled reports whether the re-home flag is applied.
func (f *ClusterFSM) RehomeEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.rehomeEnabled
}

// RehomeJobs returns copies of the jobs in flight, ordered by id.
func (f *ClusterFSM) RehomeJobs() []RehomeJob {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]RehomeJob, 0, len(f.rehomeJobs))
	for _, j := range f.rehomeJobs {
		out = append(out, j.Copy())
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

// RehomeJob returns a copy of one job.
func (f *ClusterFSM) RehomeJob(id string) (RehomeJob, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	j, ok := f.rehomeJobs[id]
	if !ok {
		return RehomeJob{}, false
	}
	return j.Copy(), true
}

// RehomeOwns reports whether a re-home job owns path (its source or an
// output). False while the flag is off.
func (f *ClusterFSM) RehomeOwns(path string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.rehomeOwnsLocked(path)
}

// RehomeExcludedFor returns, for the jobs under prefix ("db/measurement/"),
// the keys a read must leave out now, and the generation they belong to, in
// one read: committed, published and reverting jobs' outputs; latched jobs'
// sources.
func (f *ClusterFSM) RehomeExcludedFor(prefix string) (keys []string, gen uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, j := range f.rehomeJobs {
		if j.Database+"/"+j.Measurement+"/" != prefix {
			continue
		}
		switch j.State {
		case RehomeCommitted, RehomePublished, RehomeReverting, RehomeReverted:
			for _, o := range j.Outputs {
				keys = append(keys, o.Key)
			}
		case RehomeLatched:
			keys = append(keys, j.Source.Path)
		}
	}
	sort.Strings(keys)
	return keys, f.rehomeGen
}

// RehomeOpen reports whether a job under prefix may leave files out of reads
// (any state but staged).
func (f *ClusterFSM) RehomeOpen(prefix string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, j := range f.rehomeJobs {
		if j.Database+"/"+j.Measurement+"/" == prefix && j.State != RehomeStaged {
			return true
		}
	}
	return false
}

// RehomeGeneration moves whenever what a job leaves out of reads may change:
// a stage, an advance, an end.
func (f *ClusterFSM) RehomeGeneration() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.rehomeGen
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
