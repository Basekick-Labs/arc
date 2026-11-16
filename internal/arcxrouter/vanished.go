package arcxrouter

// No build tag: this file is plain Go so the evidence check is compiled and
// tested by the default build, which never links the arcx engine.

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// VanishedMessage is what a client is told when a file arcx was reading was
// removed while it ran. It is the text the DuckDB path answers for the same
// condition (api.vanishedPartitionMessage; a test pins the two together), and
// it names no storage path.
const VanishedMessage = "data read by this query was removed while it ran (retention, compaction or a delete); retry the query"

// FileVanishedError is an engine error that happened because a file in the
// query's plan was removed while the engine read it: compaction deleting a
// batch's inputs, retention, or a DELETE (#1357). Error is the client-safe
// VanishedMessage; the engine's own text, which names the path, stays in
// Cause for logs.
type FileVanishedError struct {
	Path  string
	Cause error
}

func (e *FileVanishedError) Error() string { return VanishedMessage }

func (e *FileVanishedError) Unwrap() error { return e.Cause }

// VanishedPath lets the api package recognise the error through errors.As
// with an interface of its own, so the stock (non-arcx) build does not have
// to import this package.
func (e *FileVanishedError) VanishedPath() string { return e.Path }

// vanishedPlanFile returns the planned file an engine error is about, when
// that file no longer exists. Evidence is two-part: the file's absolute path
// (built by planEngine, so a string Arc chose) occurs in the error text, and
// the file system says it is gone. Neither alone is enough: arcx's message
// format is not a contract, and on a busy measurement some planned file is
// often gone by the time a long scan fails for an unrelated reason.
//
// Lstat, not Stat: a dangling symlink in the plan is still there, so its
// error stays an error instead of a 503 every retry would repeat.
func vanishedPlanFile(err error, basePath string, entries []string) (string, bool) {
	if err == nil {
		return "", false
	}
	text := err.Error()
	for _, e := range entries {
		p := basePath + "/" + e
		if !strings.Contains(text, p) {
			continue
		}
		if _, serr := os.Lstat(p); errors.Is(serr, fs.ErrNotExist) {
			return p, true
		}
	}
	return "", false
}

// vanishCheckReader reports a mid-stream engine error caused by a removed
// plan file as a *FileVanishedError. Everything else, the batches included,
// passes straight to the engine's reader: Release, Retain, Next, RecordBatch
// and Record through embedding. The caller's runtime.KeepAlive on this
// wrapper keeps the FFI-backed reader it holds reachable.
type vanishCheckReader struct {
	array.RecordReader
	basePath   string
	entries    []string
	onVanished func(path string, cause error)

	once sync.Once
	err  error
}

// wrapVanishCheck wraps an engine reader for the plan it was built from.
// onVanished, if set, is called once when the evidence is found.
func wrapVanishCheck(r array.RecordReader, basePath string, entries []string, onVanished func(path string, cause error)) array.RecordReader {
	return &vanishCheckReader{RecordReader: r, basePath: basePath, entries: entries, onVanished: onVanished}
}

// Err returns nil without touching the file system when the stream had no
// error, so a successful (or capped, or cancelled) stream pays nothing. The
// classification runs once; later calls return the same error.
func (r *vanishCheckReader) Err() error {
	inner := r.RecordReader.Err()
	if inner == nil {
		return nil
	}
	r.once.Do(func() {
		r.err = inner
		if p, ok := vanishedPlanFile(inner, r.basePath, r.entries); ok {
			if r.onVanished != nil {
				r.onVanished(p, inner)
			}
			r.err = &FileVanishedError{Path: p, Cause: inner}
		}
	})
	return r.err
}
