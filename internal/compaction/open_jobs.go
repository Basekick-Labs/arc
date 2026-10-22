package compaction

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/storage"
)

// OpenJobs is the set of compaction jobs whose output and inputs can both
// be in storage at once, so that queries can read exactly one of them.
//
// A legacy (non-staged) job writes its output to its final key in the
// partition and only then deletes its inputs (Job.Run). A query between the
// two reads every row twice; a continuous query in that window writes the
// doubled aggregate back for good. If the delete fails, the window lasts until
// the next cycle's recovery. The parent registers a job before its subprocess
// can write the output (it chooses the output name), and keeps the entry
// until the inputs are known to be gone: the subprocess reports
// SourcesDeleted, or the job's crash-recovery manifest no longer exists.
//
// For each entry a query reads one side only (ExcludedFor): while the output
// is absent, the output (it may appear before the read lists the directory);
// once it is present, the inputs the manifest says it holds. Either is exact
// whatever the read lists, because the output never disappears while its
// inputs exist.
type OpenJobs struct {
	backend storage.Backend
	mu      sync.Mutex
	jobs    map[string]*openJob // by output key
	gen     atomic.Uint64
}

type openJob struct {
	output       string   // storage key of the output
	manifestPath string   // the job's crash-recovery manifest
	candidates   []string // the parent's input list (a superset of what the output holds)
	inputs       []string // the manifest's input list, once read
	running      bool     // the subprocess has not reported yet
}

// NewOpenJobs returns an empty registry over backend.
func NewOpenJobs(backend storage.Backend) *OpenJobs {
	return &OpenJobs{backend: backend, jobs: map[string]*openJob{}}
}

// Generation changes whenever the set of jobs changes; a cache of SQL built
// with the exclusions keys on it.
func (a *OpenJobs) Generation() uint64 {
	if a == nil {
		return 0
	}
	return a.gen.Load()
}

func (a *OpenJobs) register(output, manifestPath string, candidates []string) {
	a.mu.Lock()
	a.jobs[output] = &openJob{output: output, manifestPath: manifestPath, candidates: candidates, running: true}
	a.mu.Unlock()
	a.gen.Add(1)
}

// finish records the subprocess's report. With its inputs deleted the job is
// over; otherwise the entry stays until its manifest is gone (sync).
func (a *OpenJobs) finish(output string, sourcesDeleted bool) {
	a.mu.Lock()
	j, ok := a.jobs[output]
	if ok {
		if sourcesDeleted {
			delete(a.jobs, output)
		} else {
			j.running = false
		}
	}
	a.mu.Unlock()
	if ok && sourcesDeleted {
		a.gen.Add(1)
	}
}

// sync makes the registry agree with the crash-recovery manifests in storage:
// a manifest is a job whose inputs may not all be gone; a finished entry
// without one is over. Running entries are kept (their manifest may not be
// written yet).
func (a *OpenJobs) sync(manifests map[string]*Manifest) {
	a.mu.Lock()
	changed := false
	for path, man := range manifests {
		if man == nil || man.OutputPath == "" {
			continue
		}
		if j, ok := a.jobs[man.OutputPath]; ok {
			j.inputs, j.manifestPath = man.InputFiles, path
			continue
		}
		a.jobs[man.OutputPath] = &openJob{output: man.OutputPath, manifestPath: path, inputs: man.InputFiles}
		changed = true
	}
	for out, j := range a.jobs {
		if j.running {
			continue
		}
		if _, ok := manifests[j.manifestPath]; !ok {
			delete(a.jobs, out)
			changed = true
		}
	}
	a.mu.Unlock()
	if changed {
		a.gen.Add(1)
	}
}

// pruneFinished drops finished jobs whose manifest no longer exists: their
// inputs were deleted (by recovery, after a failed delete). One existence
// check per finished job, so a cycle lists nothing.
func (a *OpenJobs) pruneFinished(ctx context.Context) {
	a.mu.Lock()
	var check []*openJob
	for _, j := range a.jobs {
		if !j.running {
			check = append(check, j)
		}
	}
	a.mu.Unlock()
	changed := false
	for _, j := range check {
		if j.manifestPath == "" {
			continue
		}
		exists, err := a.backend.Exists(ctx, j.manifestPath)
		if err != nil || exists {
			continue
		}
		a.mu.Lock()
		if cur, ok := a.jobs[j.output]; ok && cur == j && !cur.running {
			delete(a.jobs, j.output)
			changed = true
		}
		a.mu.Unlock()
	}
	if changed {
		a.gen.Add(1)
	}
}

// ExcludedFor returns the storage keys under prefix ("db/measurement/") that
// a read must leave out, so it sees each active job's rows once. Empty in the
// common case of no job on that measurement.
func (a *OpenJobs) ExcludedFor(ctx context.Context, prefix string) []string {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	var jobs []*openJob
	for out, j := range a.jobs {
		if strings.HasPrefix(out, prefix) {
			jobs = append(jobs, j)
		}
	}
	a.mu.Unlock()
	var out []string
	for _, j := range jobs {
		size, err := a.backend.StatFile(ctx, j.output)
		if err != nil || size < 0 {
			// Absent (or unknown): leave the output out; it may appear
			// before the read lists the directory.
			out = append(out, j.output)
			continue
		}
		out = append(out, a.inputsOf(ctx, j)...)
	}
	return out
}

// inputsOf is the job's input list from its manifest, read once. A manifest
// gone with the output in place means the inputs were deleted: nothing to
// leave out. An unreadable manifest falls back to the parent's candidates.
func (a *OpenJobs) inputsOf(ctx context.Context, j *openJob) []string {
	a.mu.Lock()
	if j.inputs != nil {
		in := j.inputs
		a.mu.Unlock()
		return in
	}
	path, cand := j.manifestPath, j.candidates
	a.mu.Unlock()
	data, err := a.backend.Read(ctx, path)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil
		}
		return cand
	}
	var man Manifest
	if json.Unmarshal(data, &man) != nil {
		return cand
	}
	a.mu.Lock()
	j.inputs = man.InputFiles
	a.mu.Unlock()
	return man.InputFiles
}
