//go:build linux || darwin || freebsd

package cluster

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func acquireLicenseBootLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("license bootstrap state already owned by another process: %w", err)
	}
	return f, nil
}
