//go:build !windows

package license

import "os"

func publishInstallationIdentity(source, target string) error {
	return os.Link(source, target)
}

func syncInstallationIdentityDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func installationIdentityPermissionsSecure(mode os.FileMode) bool {
	return mode.Perm() == 0o600
}
