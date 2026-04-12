package compaction

// Completion manifests are the Phase 4 cross-process handoff from the
// compaction subprocess to the parent-side CompletionWatcher. They are NOT
// the same thing as the existing crash-recovery Manifest type in manifest.go:
//
//   - Manifest (manifest.go) lives in the storage backend under
//     _compaction_state/ and tracks "did this job upload successfully, and
//     if it crashed mid-delete, what sources still need cleaning up?" That
//     one survives pod restarts because storage is durable.
//
//   - CompletionManifest (this file) lives on LOCAL DISK at
//     {TempDirectory}/.completion/pending/{JobID}.json and tracks "did this
//     job finish and if so, what RegisterFile/DeleteFile commands should
//     the parent apply to the Raft manifest?" It intentionally does NOT
//     use storage because (a) the handoff is single-host and doesn't need
//     durability across pod restarts (if the pod restarts, the subprocess
//     is dead and the next compaction cycle re-runs the job), (b) putting
//     it in shared storage would race between multiple nodes picking up
//     each other's pending completions.
//
// State machine:
//
//     writing_output → output_written → sources_deleted
//
// The subprocess writes the manifest atomically (tmp+rename) at each state
// transition. The parent-side watcher picks up manifests in state
// output_written or sources_deleted and issues the corresponding Raft
// commands. Manifests in state writing_output are still in progress and
// are ignored by the watcher; if a subprocess crashes in this state, the
// stale manifest is swept by CleanupOrphanedCompletionManifests at startup.
//
// CompletionManifest is intentionally local-disk only and does NOT use the
// storage.Backend interface. Phase 4 is a cluster/cross-process concern;
// shared-storage durability is a different problem solved by manifest.go.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CompletionState is the lifecycle phase of a CompletionManifest. The
// subprocess advances the state in place as it progresses through the job;
// the watcher applies manifests whose state is output_written or later.
type CompletionState string

const (
	// CompletionStateWritingOutput means the subprocess has started the job
	// but has not yet successfully written the compacted file to storage.
	// Manifests in this state are NOT picked up by the watcher — they may
	// be stale (subprocess crashed) and will be cleaned up separately.
	CompletionStateWritingOutput CompletionState = "writing_output"

	// CompletionStateOutputWritten means the compacted file has been
	// durably uploaded to storage and its SHA-256 is known. Source files
	// have NOT yet been deleted. The watcher applies RegisterFile in this
	// state, but does NOT yet apply DeleteFile for sources because they
	// may still be present in storage — deleting them from the manifest
	// while they still exist on disk would leave orphans. The watcher
	// waits for the sources_deleted state before issuing DeleteFile.
	CompletionStateOutputWritten CompletionState = "output_written"

	// CompletionStateSourcesDeleted means source files have been deleted
	// from storage. The watcher applies both RegisterFile and DeleteFile
	// in this state and then removes the manifest file.
	CompletionStateSourcesDeleted CompletionState = "sources_deleted"
)

// CompactedOutput describes a single compacted file produced by a job. All
// fields are required when the enclosing manifest is in state
// output_written or sources_deleted — the watcher converts this struct to
// a raft.FileEntry via the cluster-side bridge.
//
// Outputs is a slice on the manifest (not a single value) because a future
// compaction strategy (Phase 5+) may split a partition into multiple
// outputs. Phase 4 always populates exactly one entry per manifest.
type CompactedOutput struct {
	Path          string    `json:"path"`           // storage-relative path (e.g. "mydb/cpu/2026/04/11/14/compacted_....parquet")
	SHA256        string    `json:"sha256"`         // hex-encoded 64 chars; bridged into raft.FileEntry.SHA256
	SizeBytes     int64     `json:"size_bytes"`     // authoritative size after upload
	Database      string    `json:"database"`       // bridged into raft.FileEntry.Database
	Measurement   string    `json:"measurement"`    // bridged into raft.FileEntry.Measurement
	PartitionTime time.Time `json:"partition_time"` // bridged into raft.FileEntry.PartitionTime
	Tier          string    `json:"tier"`           // "hot", "cold", etc.
	CreatedAt     time.Time `json:"created_at"`     // bridged into raft.FileEntry.CreatedAt
}

// CompletionManifest is the durable handoff from the compaction subprocess
// to the parent-side watcher. One file per job. Written atomically via
// tmp-file-plus-rename; never appended to.
//
// The watcher treats CompletionManifest as append-only by convention: the
// subprocess only rewrites it at state transitions, and the watcher only
// reads it. If the watcher picks up a manifest mid-transition (possible if
// the subprocess is slow between json.Marshal and os.Rename), the read
// fails cleanly because the final path only exists after the rename
// completes — readers see either the pre-state or the post-state, never
// a torn value.
type CompletionManifest struct {
	JobID          string            `json:"job_id"`          // matches Job.JobID; unique per attempt
	Database       string            `json:"database"`        // for operator debugging; Outputs[].Database is authoritative
	Measurement    string            `json:"measurement"`     // for operator debugging
	PartitionPath  string            `json:"partition_path"`  // for operator debugging
	Tier           string            `json:"tier"`            // for operator debugging
	State          CompletionState   `json:"state"`           // lifecycle phase; drives watcher behavior
	Outputs        []CompactedOutput `json:"outputs"`         // set when State >= output_written
	DeletedSources []string          `json:"deleted_sources"` // set when State == sources_deleted; storage-relative paths
	CreatedAt      time.Time         `json:"created_at"`      // when the manifest was first written
	UpdatedAt      time.Time         `json:"updated_at"`      // bumped on every state transition
}

// writeCompletionManifest serializes m to {dir}/{JobID}.json atomically via
// write-tmp + fsync + rename. The function is safe to call multiple times
// for the same job — each call replaces the previous on-disk contents.
//
// dir is created with 0700 permissions if it doesn't exist. The final
// manifest file is 0600 (owner read/write only) because it contains job
// metadata that operators shouldn't need to inspect via non-root.
//
// Returns an error if dir creation fails, marshal fails, fsync fails, or
// rename fails. On any error the .tmp file is best-effort removed so the
// next attempt starts clean.
//
// NOTE: JobID MUST NOT contain path separators, null bytes, or relative
// segments. The caller (Job.Run) already generates JobID as
// "{database}_{partition_path_with_slashes_replaced}_{nanos}" at
// [job.go:161], so this is enforced upstream. We reject it here as a
// defensive check anyway — a JobID with "/" would let the subprocess
// accidentally write outside its allocated directory, and a JobID with
// ".." would escape the completion dir on `filepath.Join`. This is
// defense-in-depth against a future refactor (or a malicious subprocess)
// that bypasses the upstream generator.
func writeCompletionManifest(dir string, m *CompletionManifest) error {
	if m == nil {
		return fmt.Errorf("completion manifest: nil manifest")
	}
	if m.JobID == "" {
		return fmt.Errorf("completion manifest: empty JobID")
	}
	if err := validateJobID(m.JobID); err != nil {
		return fmt.Errorf("completion manifest: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("completion manifest: mkdir %s: %w", dir, err)
	}

	final := filepath.Join(dir, m.JobID+".json")
	tmp := final + ".tmp"

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("completion manifest: marshal: %w", err)
	}

	// Write-tmp + fsync + rename: the standard POSIX atomic-file-replace
	// pattern. On any failure after OpenFile, best-effort remove the .tmp
	// so the next attempt doesn't inherit half a file.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("completion manifest: open tmp %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("completion manifest: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("completion manifest: fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("completion manifest: close tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("completion manifest: rename %s -> %s: %w", tmp, final, err)
	}
	return nil
}

// readCompletionManifest reads and parses a completion manifest file. The
// caller passes the full path including the .json suffix. Returns an error
// if the file does not exist, cannot be read, or contains invalid JSON.
//
// Readers must be tolerant of partial writes because the watcher polls
// concurrently with subprocesses that are actively writing. The standard
// tmp-file-plus-rename pattern used by writeCompletionManifest guarantees
// this function either sees the fully-flushed previous state or the
// fully-flushed new state, never a mix — but ONLY if the caller passes the
// final (non-.tmp) path. Passing a .tmp path would race with the writer.
// listPendingCompletionManifests filters .tmp files out for this reason.
func readCompletionManifest(path string) (*CompletionManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("completion manifest: read %s: %w", path, err)
	}
	var m CompletionManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("completion manifest: unmarshal %s: %w", path, err)
	}
	return &m, nil
}

// listPendingCompletionManifests enumerates all non-.tmp JSON files under
// dir, sorted by filename (which is JobID-prefixed and therefore roughly
// time-ordered via the nanosecond suffix in JobID). The watcher processes
// manifests in the returned order so older completions are applied before
// newer ones — useful when a batch of manifests has accumulated during a
// watcher outage.
//
// Returns an empty slice (NOT an error) if dir does not exist. This is the
// expected state on a fresh node or on a node that has never run
// compaction, and treating it as an error would spam the watcher logs.
//
// Paths in the returned slice are absolute (joined with dir).
func listPendingCompletionManifests(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("completion manifest: readdir %s: %w", dir, err)
	}
	var out []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Skip the tmp-file-plus-rename scratch files — they are in-flight
		// writes and racing on them is a torn-read bug waiting to happen.
		if strings.HasSuffix(name, ".tmp") {
			continue
		}
		// Only consider .json; anything else in the dir is foreign.
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	sort.Strings(out)
	return out, nil
}

// validateJobID rejects path separators, null bytes, relative segments,
// and leading dots so a completion-manifest filename cannot escape the
// pending directory via `filepath.Join`. Upstream JobID generation in
// NewJob already sanitizes the partition path, but this is the one place
// a malicious or buggy caller could inject a path traversal — the check
// has to live here, not upstream, because upstream may change.
//
// Rejected:
//   - forward slashes, backslashes
//   - null bytes (C-string truncation defense)
//   - ".." anywhere in the ID (even inside a longer string like "foo..bar")
//   - leading dot (avoids hidden files, `.` / `..`)
//   - empty string
//
// Everything else — alphanumerics, underscores, dashes, unicode letters —
// is fine. We deliberately don't restrict to a narrow allowlist because
// the generator uses database/partition names which can contain a wide
// range of characters.
func validateJobID(id string) error {
	if id == "" {
		return fmt.Errorf("JobID must not be empty")
	}
	if strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("JobID %q must not contain path separators", id)
	}
	if strings.ContainsRune(id, 0) {
		return fmt.Errorf("JobID must not contain null bytes")
	}
	if strings.Contains(id, "..") {
		return fmt.Errorf("JobID %q must not contain '..'", id)
	}
	if strings.HasPrefix(id, ".") {
		return fmt.Errorf("JobID %q must not start with '.'", id)
	}
	return nil
}

// deleteCompletionManifest removes a completion manifest after the watcher
// has successfully applied its Raft commands. Idempotent — deleting a
// non-existent file is not an error — because the watcher may retry
// applying a manifest whose previous apply succeeded but whose deletion
// failed, and re-deleting is the safe recovery path.
func deleteCompletionManifest(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("completion manifest: remove %s: %w", path, err)
	}
	return nil
}
