package license

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func installationTestFingerprint(value int) string {
	return fmt.Sprintf("sha256:%064x", value)
}

func TestInstallationIdentityRetainsFirstHardwareFingerprint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new", "node")
	first, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
		return installationTestFingerprint(1), nil
	})
	if err != nil || first != installationTestFingerprint(1) {
		t.Fatalf("initial identity: %q, %v", first, err)
	}
	path := filepath.Join(dir, installationIdentityFileName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A separate call must load from disk, without consulting the replacement
	// pod's hardware or extending/replacing the already assigned identity.
	for i := 0; i < 3; i++ {
		loaded, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
			t.Error("persisted installation consulted changed hardware")
			return installationTestFingerprint(2), nil
		})
		if err != nil || loaded != first {
			t.Fatalf("reloaded identity: %q, %v", loaded, err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("loading changed persistent identity: %v", err)
	}
	if runtime.GOOS != "windows" {
		for path, want := range map[string]os.FileMode{dir: 0o700, path: 0o600} {
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != want {
				t.Fatalf("incorrect identity permissions for %s: %v", path, err)
			}
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 || files[0].Name() != installationIdentityFileName {
		t.Fatalf("temporary identity files leaked: %v, %v", files, err)
	}
}

func TestInstallationIdentityWithoutDirectoryRetainsLegacyFingerprint(t *testing.T) {
	want, err := GenerateMachineFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	got, err := loadOrCreateInstallationFingerprint("")
	if err != nil || got != want {
		t.Fatalf("no-directory behavior differs from legacy fingerprint: %q, %v", got, err)
	}
	got, err = loadOrCreateInstallationFingerprint(t.TempDir())
	if err != nil || got != want {
		t.Fatalf("stable hardware upgrade changed the legacy fingerprint: %q, %v", got, err)
	}
}

func TestInstallationIdentityPreservesExistingDirectoryPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows directory access is controlled by ACLs")
	}
	for _, mode := range []os.FileMode{0o750, 0o770} {
		t.Run(fmt.Sprintf("mode=%o", mode), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, mode); err != nil {
				t.Fatal(err)
			}
			if _, err := loadOrCreateInstallationFingerprint(dir); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(dir)
			if err != nil || info.Mode().Perm() != mode {
				t.Fatalf("existing auth/data directory mode changed: %v", err)
			}
			identity, err := os.Stat(filepath.Join(dir, installationIdentityFileName))
			if err != nil || identity.Mode().Perm() != 0o600 {
				t.Fatalf("identity file did not retain private permissions: %v", err)
			}
		})
	}
}

func TestInstallationIdentityPreservesTrustedGroupPermissionsAfterRemount(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows file access is controlled by ACLs")
	}
	for _, tc := range []struct {
		mode os.FileMode
		want bool
	}{
		{0o600, true}, {0o640, true}, {0o660, true},
		{0o644, false}, {0o664, false}, {0o666, false}, {0o604, false},
		{0o700, false}, {0o610, false}, {0o601, false},
		{0o400, false}, {0o620, false},
	} {
		t.Run(fmt.Sprintf("mode=%o", tc.mode), func(t *testing.T) {
			dir := t.TempDir()
			want := installationTestFingerprint(1)
			if got, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
				return want, nil
			}); err != nil || got != want {
				t.Fatalf("initial private identity: %q, %v", got, err)
			}
			path := filepath.Join(dir, installationIdentityFileName)
			created, err := os.Stat(path)
			if err != nil || created.Mode().Perm() != 0o600 {
				t.Fatalf("identity must initially be 0600: %v", err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatal(err)
			}
			got, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
				t.Error("an existing identity must never consult changed hardware or regenerate")
				return installationTestFingerprint(2), nil
			})
			if tc.want {
				if err != nil || got != want {
					t.Fatalf("trusted group remount lost the identity: %q, %v", got, err)
				}
			} else if err == nil || got != "" {
				t.Fatalf("unsafe identity mode accepted: %q, %v", got, err)
			}
			retained, err := os.Stat(path)
			if err != nil || retained.Mode().Perm() != tc.mode {
				t.Fatalf("loading changed existing file permissions: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("loading changed existing identity bytes: %v", err)
			}
		})
	}
}

func TestInstallationIdentityCorruptionNeverRegenerates(t *testing.T) {
	valid := installationTestFingerprint(1)
	for name, data := range map[string]string{
		"empty":       "",
		"malformed":   "{",
		"null":        "null",
		"old_version": `{"version":0,"fingerprint":"` + valid + `"}`,
		"new_version": `{"version":2,"fingerprint":"` + valid + `"}`,
		"missing_fp":  `{"version":1}`,
		"bad_prefix":  `{"version":1,"fingerprint":"md5:` + strings.Repeat("a", 64) + `"}`,
		"bad_hash":    `{"version":1,"fingerprint":"sha256:` + strings.Repeat("z", 64) + `"}`,
		"short_hash":  `{"version":1,"fingerprint":"sha256:ab"}`,
		"uppercase":   `{"version":1,"fingerprint":"sha256:` + strings.Repeat("A", 64) + `"}`,
		"extra_field": `{"version":1,"fingerprint":"` + valid + `","other":1}`,
		"trailing":    `{"version":1,"fingerprint":"` + valid + `"} {}`,
		"oversize":    `{"version":1,"fingerprint":"` + valid + `"}` + strings.Repeat(" ", maxInstallationIdentityBytes),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, installationIdentityFileName)
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
				t.Error("corrupt identity must not regenerate")
				return installationTestFingerprint(2), nil
			})
			if err == nil || got != "" {
				t.Fatalf("corrupt identity accepted: %q, %v", got, err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || string(after) != data {
				t.Fatalf("corrupt identity was overwritten: %v", readErr)
			}
		})
	}
}

func TestInstallationIdentityRejectsNonRegularFile(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(fmt.Sprintf("symlink=%v", symlink), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, installationIdentityFileName)
			if symlink {
				target := filepath.Join(t.TempDir(), "identity.json")
				if err := os.WriteFile(target, []byte(`{"version":1,"fingerprint":"`+installationTestFingerprint(8)+`"}`), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Skipf("symlink unavailable on this platform: %v", err)
				}
			} else if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if got, err := loadOrCreateInstallationFingerprint(dir); err == nil || got != "" {
				t.Fatalf("non-regular identity accepted: %q, %v", got, err)
			}
		})
	}
}

func TestInstallationIdentityConcurrentCreatorsUseOneCompleteWinner(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "concurrent", "node")
	type result struct {
		fingerprint string
		err         error
	}
	const workers = 24
	start, results := make(chan struct{}), make(chan result, workers)
	for worker := 1; worker <= workers; worker++ {
		go func(id int) {
			<-start
			fingerprint, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
				return installationTestFingerprint(id), nil
			})
			results <- result{fingerprint, err}
		}(worker)
	}
	close(start)
	var winner string
	for i := 0; i < workers; i++ {
		got := <-results
		if got.err != nil {
			t.Errorf("concurrent creator failed: %v", got.err)
			continue
		}
		if winner == "" {
			winner = got.fingerprint
		}
		if got.fingerprint != winner || !ValidateFingerprint(got.fingerprint) {
			t.Errorf("creator returned a different/partial identity: %q, winner %q", got.fingerprint, winner)
		}
	}
	persisted, err := readInstallationIdentity(filepath.Join(dir, installationIdentityFileName))
	if err != nil || persisted != winner || winner == "" {
		t.Fatalf("published identity differs from the successful callers: %q, %v", persisted, err)
	}
}

func TestInstallationIdentityCreationFailuresStayClosed(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("existing-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := loadOrCreateInstallationFingerprint(blocked); err == nil || got != "" {
		t.Fatalf("unwritable destination returned a fingerprint: %q, %v", got, err)
	}
	for name, generate := range map[string]func() (string, error){
		"generation_error":  func() (string, error) { return "", errors.New("hardware unavailable") },
		"invalid_generated": func() (string, error) { return "not-a-fingerprint", nil },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if got, err := loadOrCreateInstallationFingerprintWithGenerator(dir, generate); err == nil || got != "" {
				t.Fatalf("creation failure returned a fingerprint: %q, %v", got, err)
			}
			if _, err := os.Stat(filepath.Join(dir, installationIdentityFileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed generation created an identity: %v", err)
			}
		})
	}
}

func TestInstallationIdentityConcurrentCorruptWinnerIsNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, installationIdentityFileName)
	got, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) {
		// Publish after the helper observed absence, before its exclusive link.
		if err := os.WriteFile(path, []byte("corrupt-winner"), 0o600); err != nil {
			t.Fatal(err)
		}
		return installationTestFingerprint(1), nil
	})
	if err == nil || got != "" {
		t.Fatalf("corrupt concurrent winner was accepted: %q, %v", got, err)
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil || string(data) != "corrupt-winner" {
		t.Fatalf("corrupt concurrent winner was overwritten: %v", readErr)
	}
}

func TestInstallationIdentitySeparateVolumesNeverAdoptCopiedCache(t *testing.T) {
	foreign := installationTestFingerprint(999)
	lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: "ARC-TEST-COPIED-CACHE",
		MachineFingerprint: foreign, ExpiresAt: time.Now().Add(time.Hour)})
	cache := []byte(fmt.Sprintf(`{"license_file":%q,"license_signature":%q}`, lf, sig))
	for node := 1; node <= 2; node++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, cacheFileName), cache, 0o600); err != nil {
			t.Fatal(err)
		}
		want := installationTestFingerprint(node)
		got, err := loadOrCreateInstallationFingerprintWithGenerator(dir, func() (string, error) { return want, nil })
		if err != nil || got != want || got == foreign {
			t.Fatalf("node adopted a cache's foreign identity: %q, %v", got, err)
		}
		after, err := os.ReadFile(filepath.Join(dir, cacheFileName))
		if err != nil || !bytes.Equal(after, cache) {
			t.Fatalf("identity creation modified the entitlement cache: %v", err)
		}
	}
}
