package license

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const installationIdentityFileName = "license_installation.json"
const maxInstallationIdentityBytes = 4 << 10

var errInstallationIdentityAbsent = errors.New("installation identity does not exist")

type installationIdentity struct {
	Version     int    `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

// loadOrCreateInstallationFingerprint pins the first hardware fingerprint to
// this installation's per-node persistent directory. It must complete before
// activation: a changed pod MAC/hostname must not consume another machine slot.
// With no persistent directory, callers retain the legacy hardware behavior.
//
// We deliberately never import identity from license_cache.json. A copied
// signed entitlement is not proof that this is its original installation. On
// upgrade, stable hardware keeps its existing fingerprint; an already-changed
// legacy fingerprint cannot be reclaimed automatically without server proof.
// Copying the identity file itself clones the installation; this file is not
// an attestation against an operator who controls the persistent volume.
func loadOrCreateInstallationFingerprint(cacheDir string) (string, error) {
	return loadOrCreateInstallationFingerprintWithGenerator(cacheDir, GenerateMachineFingerprint)
}

func loadOrCreateInstallationFingerprintWithGenerator(cacheDir string, generate func() (string, error)) (string, error) {
	if cacheDir == "" {
		return generate()
	}
	dir, err := filepath.Abs(cacheDir)
	if err != nil {
		return "", fmt.Errorf("installation identity directory: %w", err)
	}
	if err := prepareInstallationIdentityDirectory(dir); err != nil {
		return "", err
	}
	path := filepath.Join(dir, installationIdentityFileName)
	if fingerprint, err := readInstallationIdentity(path); err == nil {
		// Another process may have published the complete file just before
		// this read; complete its directory barrier before using that identity.
		if err := syncInstallationIdentityDirectory(dir); err != nil {
			return "", fmt.Errorf("sync installation identity directory: %w", err)
		}
		return fingerprint, nil
	} else if !errors.Is(err, errInstallationIdentityAbsent) {
		return "", err
	}
	fingerprint, err := generate()
	if err != nil {
		return "", fmt.Errorf("generate installation fingerprint: %w", err)
	}
	if !ValidateFingerprint(fingerprint) {
		return "", errors.New("generated installation fingerprint is invalid")
	}
	data, err := json.Marshal(installationIdentity{Version: 1, Fingerprint: fingerprint})
	if err != nil {
		return "", fmt.Errorf("encode installation identity: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".license-installation-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create installation identity: %w", err)
	}
	defer os.Remove(tmp.Name())
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(append(data, '\n'))
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return "", fmt.Errorf("persist installation identity: %w", err)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close installation identity: %w", closeErr)
	}
	// Exclusive publication exposes only a complete, synced file and never
	// replaces another process's winner. A partial O_EXCL target write would
	// allow readers to mistake an in-progress file for persistent corruption.
	if err := publishInstallationIdentity(tmp.Name(), path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("publish installation identity: %w", err)
		}
		fingerprint, err = readInstallationIdentity(path)
		if err != nil {
			return "", err
		}
	}
	if err := syncInstallationIdentityDirectory(dir); err != nil {
		// Never remove a published identity after a failed barrier: a sibling
		// process may already have read it. A subsequent attempt must use it.
		return "", fmt.Errorf("sync installation identity directory: %w", err)
	}
	return fingerprint, nil
}

func prepareInstallationIdentityDirectory(dir string) error {
	// Record only directory entries this creation may add. Existing auth/data
	// directories can be root-owned and writable through a Kubernetes fsGroup;
	// neither change their permissions nor sync unrelated ancestors/rootfs.
	var parents []string
	for current := dir; ; current = filepath.Dir(current) {
		if _, err := os.Stat(current); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat installation identity directory: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return errors.New("installation identity directory has no existing parent")
		}
		parents = append(parents, parent)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create installation identity directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("installation identity directory must be a real directory: %s", dir)
	}
	// Persist newly created links from the nearest pre-existing parent down.
	// The final identity's own directory is synced after exclusive publication.
	for i := len(parents) - 1; i >= 0; i-- {
		if err := syncInstallationIdentityDirectory(parents[i]); err != nil {
			return fmt.Errorf("sync installation identity parent directory: %w", err)
		}
	}
	return nil
}

func readInstallationIdentity(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", errInstallationIdentityAbsent
	}
	if err != nil {
		return "", fmt.Errorf("stat installation identity: %w", err)
	}
	if !info.Mode().IsRegular() || !installationIdentityPermissionsSecure(info.Mode()) {
		return "", errors.New("installation identity must be a regular private file")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open installation identity: %w", err)
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", errors.New("installation identity changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxInstallationIdentityBytes+1))
	if err != nil {
		return "", fmt.Errorf("read installation identity: %w", err)
	}
	if len(data) > maxInstallationIdentityBytes {
		return "", errors.New("installation identity exceeds size limit")
	}
	var identity installationIdentity
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return "", fmt.Errorf("decode installation identity: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return "", errors.New("installation identity contains trailing data")
	}
	if identity.Version != 1 || !ValidateFingerprint(identity.Fingerprint) {
		return "", errors.New("installation identity has an unsupported version or invalid fingerprint")
	}
	return identity.Fingerprint, nil
}
