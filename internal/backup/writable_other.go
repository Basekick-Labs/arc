//go:build windows

package backup

// No access(2) to ask on Windows; the write itself reports any failure.
func checkWritable(string) error       { return nil }
func checkDirAcceptsFile(string) error { return nil }
