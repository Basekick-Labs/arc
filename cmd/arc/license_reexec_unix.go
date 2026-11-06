//go:build linux || darwin || freebsd

package main

import (
	"os"
	"syscall"
)

// Replace only after every component completed graceful shutdown. Exec retains
// the supervisor/container identity while rebuilding initialization-only state.
func replaceLicenseProcess() error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	return syscall.Exec(executable, os.Args, os.Environ())
}
