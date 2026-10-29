//go:build !(linux || darwin)

package wal

import (
	"errors"
	"os"
)

// preallocate is not available here: the reserve is written instead.
func preallocate(*os.File, int64) error {
	return errors.New("preallocation is not available on this platform")
}
