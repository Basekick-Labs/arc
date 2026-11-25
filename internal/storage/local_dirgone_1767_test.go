package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
	"testing"
)

// TestDirGoneOn pins the classification on every platform: CI runs Linux,
// where EINVAL never means the directory went, so the darwin rule is only
// covered here (#1767).
func TestDirGoneOn(t *testing.T) {
	einval := []struct {
		name string
		err  error
	}{
		{"open", &os.PathError{Op: "open", Path: "/r/d/.arc-1.tmp", Err: syscall.EINVAL}},
		{"rename", &os.LinkError{Op: "rename", Old: "/t/a", New: "/r/d/a", Err: syscall.EINVAL}},
		// createDurable wraps os.Mkdir's error with %w.
		{"mkdir wrapped", fmt.Errorf("failed to create directory: %w", &os.PathError{Op: "mkdir", Path: "/r/d/e", Err: syscall.EINVAL})},
		{"rename wrapped", fmt.Errorf("cannot rename: %w", &os.LinkError{Op: "rename", Old: "/t/a", New: "/r/d/a", Err: syscall.EINVAL})},
	}
	for _, c := range einval {
		if !dirGoneOn("darwin", c.err) {
			t.Errorf("darwin, %s EINVAL: not treated as a removed directory", c.name)
		}
		for _, goos := range []string{"linux", "windows", "freebsd"} {
			if dirGoneOn(goos, c.err) {
				t.Errorf("%s, %s EINVAL: treated as a removed directory", goos, c.name)
			}
		}
	}
	enoent := []error{
		&os.PathError{Op: "open", Path: "/r/d/x", Err: syscall.ENOENT},
		&os.LinkError{Op: "rename", Old: "/t/a", New: "/r/d/a", Err: syscall.ENOENT},
		fmt.Errorf("failed to create directory: %w", &os.PathError{Op: "mkdir", Path: "/r/d/e", Err: syscall.ENOENT}),
		fs.ErrNotExist,
	}
	for _, err := range enoent {
		for _, goos := range []string{"darwin", "linux", "windows"} {
			if !dirGoneOn(goos, err) {
				t.Errorf("%s: %v not treated as a removed directory", goos, err)
			}
		}
	}
	other := []error{
		nil,
		&os.PathError{Op: "open", Path: "/r/d/x", Err: syscall.EACCES},
		&os.PathError{Op: "open", Path: "/r/d/x", Err: syscall.ENOSPC},
		&os.PathError{Op: "rmdir", Path: "/r/d", Err: syscall.ENOTEMPTY},
		&os.LinkError{Op: "rename", Old: "/t/a", New: "/r/d/a", Err: syscall.EXDEV},
		errors.New("invalid argument"),
	}
	for _, err := range other {
		if dirGoneOn("darwin", err) {
			t.Errorf("darwin: %v treated as a removed directory", err)
		}
	}
}
