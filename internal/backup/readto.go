package backup

import (
	"fmt"
	"io"
	"os"
)

// createTempFile creates a per-file staging file. The caller supplies the
// operation-specific pattern so backup and restore retain distinct temp-file
// names while sharing the test seam.
//
// Wrapping the returned *os.File in trackingWriter costs the source-to-temp
// io.Copy's zero-copy fast path (copy_file_range needs an *os.File
// destination), so that hop runs through a buffered loop. This is deliberate:
// the operation is disk-bound, and the alternative is not knowing whether a
// ReadTo failure came from the source or the local temp destination.
var createTempFile = func(pattern string) (*os.File, error) {
	return os.CreateTemp("", pattern)
}

// trackingWriter records the first error the destination returned.
//
// Every backend's ReadTo is an io.Copy into the caller's writer, so a full temp
// filesystem surfaces as a ReadTo error that is indistinguishable, by the error
// alone, from the source being unreadable. The recorded error lets
// classifyReadToFailure attribute the failure to the right side.
type trackingWriter struct {
	w   io.Writer
	err error
}

func (t *trackingWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if err != nil && t.err == nil {
		t.err = err
	}
	return n, err
}

// classifyReadToFailure turns a ReadTo failure into a destination error or a
// caller-specific source-read error.
func classifyReadToFailure(srcPath string, readErr, writeErr, sourceReadErr error, sourceName string) error {
	if writeErr != nil {
		return fmt.Errorf("failed to write temp file while reading %s from %s: %w", srcPath, sourceName, writeErr)
	}
	return fmt.Errorf("failed to read from %s: %w: %w", sourceName, sourceReadErr, readErr)
}
