package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// io.Copy prefers WriterTo, which lets the test retain the actual destination
// file and assert it was closed before AppendReader returned. Keeping the
// *os.File reachable also prevents its finalizer from masking a missing close.
type appendLifecycleReader struct {
	file            *os.File
	copyErr         error
	closeDuringCopy bool
}

func (r *appendLifecycleReader) Read([]byte) (int, error) {
	panic("io.Copy must use WriterTo")
}

func (r *appendLifecycleReader) WriteTo(dst io.Writer) (int64, error) {
	r.file = dst.(*os.File)
	n, err := r.file.Write([]byte("-tail"))
	if err != nil {
		return int64(n), err
	}
	if r.closeDuringCopy {
		// Force AppendReader's Close to return os.ErrClosed without needing
		// a filesystem fault or an injectable opener in production code.
		if err := r.file.Close(); err != nil {
			return int64(n), err
		}
	}
	return int64(n), r.copyErr
}

func TestAppendReaderLifecycle(t *testing.T) {
	copyFailure := errors.New("injected copy failure")
	for _, tc := range []struct {
		name                                        string
		partial, copyFails, closeFails, renameFails bool
	}{
		{name: "complete"},
		{name: "partial", partial: true},
		{name: "copy error", copyFails: true},
		{name: "close error complete", closeFails: true},
		{name: "close error partial", closeFails: true, partial: true},
		{name: "copy and close errors", copyFails: true, closeFails: true},
		{name: "rename error", renameFails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, key, staging, final := prepareAppendReaderIssue750(t)
			committedPath := final
			if tc.renameFails {
				// A non-empty directory makes promotion fail on every platform.
				if err := os.Mkdir(final, 0700); err != nil {
					t.Fatal(err)
				}
				committedPath = filepath.Join(final, "keep")
			}
			const committed = "previous committed data"
			if err := os.WriteFile(committedPath, []byte(committed), 0600); err != nil {
				t.Fatal(err)
			}

			reader := &appendLifecycleReader{closeDuringCopy: tc.closeFails}
			if tc.copyFails {
				reader.copyErr = copyFailure
			}
			t.Cleanup(func() {
				// Reclaim a leaked handle if an assertion catches a regression.
				if reader.file != nil {
					_ = reader.file.Close()
				}
			})
			expected := int64(len("-tail"))
			if tc.partial {
				expected++
			}
			err := backend.AppendReader(context.Background(), key, reader, expected)
			switch {
			case tc.copyFails:
				if !errors.Is(err, copyFailure) {
					t.Errorf("copy error not preserved: %v", err)
				}
			case tc.closeFails:
				if !errors.Is(err, os.ErrClosed) {
					t.Errorf("close error not returned: %v", err)
				}
			case tc.renameFails:
				if err == nil {
					t.Error("rename error not returned")
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
			}
			if reader.file == nil {
				t.Fatal("copy did not expose the destination file")
			}
			if _, err := reader.file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Errorf("file left open after AppendReader: %v", err)
			}

			promoted := !tc.partial && !tc.copyFails && !tc.closeFails && !tc.renameFails
			if promoted {
				data, err := os.ReadFile(final)
				if err != nil || string(data) != "prefix-tail" {
					t.Fatalf("promoted data = %q, error = %v", data, err)
				}
				if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("staging file remains after promotion: %v", err)
				}
				return
			}
			data, err := os.ReadFile(staging)
			if err != nil || string(data) != "prefix-tail" {
				t.Errorf("staging data = %q, error = %v", data, err)
			}
			data, err = os.ReadFile(committedPath)
			if err != nil || string(data) != committed {
				t.Errorf("committed data changed before promotion: %q, error = %v", data, err)
			}
		})
	}
}
