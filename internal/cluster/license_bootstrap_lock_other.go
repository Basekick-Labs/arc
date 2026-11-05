//go:build !linux && !darwin && !freebsd && !windows

package cluster

import (
	"errors"
	"os"
)

func acquireLicenseBootLock(string) (*os.File, error) {
	return nil, errors.New("license bootstrap process locking is unsupported on this platform")
}
