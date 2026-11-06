//go:build windows

package license

import (
	"os"

	"golang.org/x/sys/windows"
)

func publishInstallationIdentity(source, target string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	// Without REPLACE_EXISTING this remains exclusive. Windows has no POSIX
	// directory fsync; use its write-through publication after syncing data.
	return windows.MoveFileEx(from, to, windows.MOVEFILE_WRITE_THROUGH)
}

func syncInstallationIdentityDirectory(string) error { return nil }

func installationIdentityPermissionsSecure(os.FileMode) bool {
	// Go's Unix mode bits do not describe Windows ACLs. Creation requests
	// 0600 and inherits the installation directory's Windows access policy.
	return true
}
