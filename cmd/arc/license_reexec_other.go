//go:build !linux && !darwin && !freebsd

package main

import "fmt"

func replaceLicenseProcess() error {
	return fmt.Errorf("automatic license lifecycle process replacement is unsupported on this platform")
}
