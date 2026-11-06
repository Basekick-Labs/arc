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
	// Kubernetes fsGroup may add trusted deployment-group access when a
	// volume is remounted. Keep that identity without chmod/regeneration;
	// creation still uses 0600. Execution and access by others stay forbidden.
	switch mode.Perm() {
	case 0o600, 0o640, 0o660:
		return true
	default:
		return false
	}
}
