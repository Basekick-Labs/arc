//go:build !windows

package backup

import "golang.org/x/sys/unix"

// checkWritable reports whether this process may write the file at path, by
// access(2): no side effects, and EROFS on a read-only mount such as a
// Kubernetes ConfigMap.
func checkWritable(path string) error {
	return unix.Access(path, unix.W_OK)
}

// checkDirAcceptsFile reports whether this process may create a file in dir:
// write and search permission on it.
func checkDirAcceptsFile(dir string) error {
	return unix.Access(dir, unix.W_OK|unix.X_OK)
}
