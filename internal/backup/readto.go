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
// trackingWriter preserves the source-to-temp io.Copy fast path through
// io.ReaderFrom while retaining destination-write error attribution.
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

// ReadFrom delegates to the underlying reader when available. If the copy
// fails, a write probe distinguishes a destination failure from a source read
// failure; failed temp files are discarded by the caller.
func (t *trackingWriter) ReadFrom(r io.Reader) (int64, error) {
	readerFrom, ok := t.w.(io.ReaderFrom)
	if !ok {
		return io.Copy(struct{ io.Writer }{t}, r)
	}

	n, err := readerFrom.ReadFrom(r)
	if err != nil {
		var probe [1]byte
		if written, writeErr := t.w.Write(probe[:]); writeErr != nil {
			if t.err == nil {
				t.err = writeErr
			}
		} else if written != len(probe) && t.err == nil {
			t.err = io.ErrShortWrite
		}
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
