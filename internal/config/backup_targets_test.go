package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeArcToml drops an arc.toml in a fresh working directory so Load() reads
// it. Load() searches ".", so the chdir is the only way to control the file.
func writeArcToml(t *testing.T, body string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "arc.toml"), []byte(body), 0o600); err != nil {
		t.Fatalf("write arc.toml: %v", err)
	}
	t.Chdir(dir)
}

// mustLoadError runs Load and requires a failure whose text contains want.
//
// The text, not merely "an error": Load has dozens of refusal paths and a bare
// non-nil check would pass against a completely different rejection — which is
// how a key-length test once passed against its own bug.
func mustLoadError(t *testing.T, want string) string {
	t.Helper()
	_, err := Load()
	if err == nil {
		t.Fatalf("Load() succeeded, want an error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Load() error = %q, want it to contain %q", err.Error(), want)
	}
	return err.Error()
}

// TestBackupTargetNameRefusesTheIssuesOwnHyphenatedExample pins the name rule
// and the reason for it.
//
// #1085 spells a target "audit-bucket" in its own example, so anyone
// implementing from the issue will write that, and it cannot be allowed: a
// hyphen cannot appear in an environment variable name, so a hyphenated target
// could be set in a config file and never afterwards overridden from the
// environment — which is how a credential stays in a config file for good. The
// refusal has to say what to write instead, so the suggested spelling is
// asserted as a literal.
func TestBackupTargetNameRefusesTheIssuesOwnHyphenatedExample(t *testing.T) {
	writeArcToml(t, `
[backup]
default_target = "audit-bucket"
[backup.targets.audit-bucket]
type = "s3"
s3_bucket = "acme"
`)
	got := mustLoadError(t, "invalid backup target name")
	for _, want := range []string{`"audit-bucket"`, `"audit_bucket"`, "environment variable"} {
		if !strings.Contains(got, want) {
			t.Errorf("refusal = %q, want it to contain %q", got, want)
		}
	}
}

// TestBackupTargetDiscoveryFromTheEnvironmentAlone: a deployment that puts
// nothing in a config file still has to be able to configure a target, and a
// name cannot be discovered from a map that does not exist — which is what
// backup.target_names is for.
func TestBackupTargetDiscoveryFromTheEnvironmentAlone(t *testing.T) {
	writeArcToml(t, "") // no [backup.targets] stanza at all

	t.Setenv("ARC_BACKUP_TARGET_NAMES", "audit")
	t.Setenv("ARC_BACKUP_TARGETS_AUDIT_TYPE", "s3")
	t.Setenv("ARC_BACKUP_TARGETS_AUDIT_S3_BUCKET", "acme-arc-audit-backups")
	t.Setenv("ARC_BACKUP_TARGETS_AUDIT_S3_PREFIX", "arc/")
	t.Setenv("ARC_BACKUP_DEFAULT_TARGET", "audit")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	target := cfg.Backup.DefaultBackupTarget()
	if target == nil {
		t.Fatal("DefaultBackupTarget() = nil, want the audit target discovered from ARC_BACKUP_TARGET_NAMES")
	}
	if target.Name != "audit" {
		t.Errorf("target name = %q, want \"audit\"", target.Name)
	}
	if target.Type != "s3" {
		t.Errorf("target type = %q, want \"s3\"", target.Type)
	}
	if target.S3Bucket != "acme-arc-audit-backups" {
		t.Errorf("target bucket = %q, want \"acme-arc-audit-backups\"", target.S3Bucket)
	}
	// The default that is NOT in the environment still has to land, which is
	// what setBackupTargetDefaults is for.
	if target.S3Region != "us-east-1" {
		t.Errorf("target region = %q, want \"us-east-1\" (the per-target default)", target.S3Region)
	}
	if !target.S3UseSSL {
		t.Error("target s3_use_ssl = false, want true (the per-target default)")
	}
	prefix, err := target.KeyPrefix()
	if err != nil {
		t.Fatalf("KeyPrefix(): %v", err)
	}
	if prefix != "arc/" {
		t.Errorf("KeyPrefix() = %q, want \"arc/\"", prefix)
	}
}

// TestBackupTargetFieldsFromTheEnvironmentOverrideTheFile: every FIELD is a
// literal dotted key so AutomaticEnv resolves it. If a field were read through
// the viper map instead, the file value would win and a credential rotated in
// the environment would be silently ignored.
func TestBackupTargetFieldsFromTheEnvironmentOverrideTheFile(t *testing.T) {
	writeArcToml(t, `
[backup]
default_target = "audit"
[backup.targets.audit]
type = "s3"
s3_bucket = "from-the-file"
`)
	t.Setenv("ARC_BACKUP_TARGETS_AUDIT_S3_BUCKET", "from-the-environment")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	target := cfg.Backup.DefaultBackupTarget()
	if target == nil {
		t.Fatal("DefaultBackupTarget() = nil, want the audit target")
	}
	if target.S3Bucket != "from-the-environment" {
		t.Errorf("target bucket = %q, want \"from-the-environment\"", target.S3Bucket)
	}
}

// TestBackupDefaultTargetMustNameADiscoveredTarget: both halves of the
// mismatch are refusals rather than a fall back to backup.local_path, because
// the fallback writes the backup somewhere the operator did not ask for and
// the only signal is a log line nobody reads until a restore.
func TestBackupDefaultTargetMustNameADiscoveredTarget(t *testing.T) {
	t.Run("default_target names nothing", func(t *testing.T) {
		writeArcToml(t, `
[backup]
default_target = "audit"
[backup.targets.other]
type = "local"
local_path = "/tmp/arc-other-backups"
`)
		got := mustLoadError(t, "backup.default_target")
		if !strings.Contains(got, "other") {
			t.Errorf("refusal = %q, want it to name the discovered set (\"other\")", got)
		}
	})

	t.Run("default_target with no targets at all", func(t *testing.T) {
		writeArcToml(t, `
[backup]
default_target = "audit"
`)
		got := mustLoadError(t, "no backup target is configured")
		for _, want := range []string{"[backup.targets.audit]", "ARC_BACKUP_TARGET_NAMES=audit"} {
			if !strings.Contains(got, want) {
				t.Errorf("refusal = %q, want it to contain %q", got, want)
			}
		}
	})

	t.Run("a target nothing points at", func(t *testing.T) {
		writeArcToml(t, `
[backup.targets.audit]
type = "local"
local_path = "/tmp/arc-audit-backups"
`)
		mustLoadError(t, "backup.default_target is not set")
	})
}

// TestBackupRefusesMoreThanOneTarget: B2b-1 routes to exactly one
// destination, so a second target would be silently ignored — a destination an
// operator believes they configured.
func TestBackupRefusesMoreThanOneTarget(t *testing.T) {
	writeArcToml(t, `
[backup]
default_target = "audit"
[backup.targets.audit]
type = "local"
local_path = "/tmp/arc-audit-backups"
[backup.targets.archive]
type = "local"
local_path = "/tmp/arc-archive-backups"
`)
	got := mustLoadError(t, "only one is supported in this release")
	for _, want := range []string{"archive", "audit"} {
		if !strings.Contains(got, want) {
			t.Errorf("refusal = %q, want it to name %q", got, want)
		}
	}
}

// TestBackupTargetOverlapWithPrimaryStorageIsRefusedAtEveryPrefixSpelling
// exercises every spelling of the same prefix, because "arc" and "arc/" are
// one byte apart and name one location (#534's corollary).
//
// What the refusal protects is recorded on checkBackupDestinationOverlap and
// is NOT cleanupPartialBackupWrite's premise, which an earlier draft of these
// comments claimed: a destination inside the storage root is re-copied by
// every subsequent backup, and the reconciliation sweep deletes the backups.
func TestBackupTargetOverlapWithPrimaryStorageIsRefusedAtEveryPrefixSpelling(t *testing.T) {
	for _, c := range []struct{ storagePrefix, targetPrefix string }{
		{"", ""},
		{"", "arc"},
		{"arc", "arc"},
		{"arc/", "arc"},
		{"arc", "arc/"},
		{"arc/", "arc/"},
		{"arc", "arc/backups"},
		{"arc/", "arc/backups/"},
	} {
		t.Run("storage="+c.storagePrefix+" target="+c.targetPrefix, func(t *testing.T) {
			writeArcToml(t, `
[storage]
backend = "s3"
s3_bucket = "acme"
s3_prefix = "`+c.storagePrefix+`"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "s3"
s3_bucket = "acme"
s3_prefix = "`+c.targetPrefix+`"
`)
			got := mustLoadError(t, "overlaps primary storage")
			if !strings.Contains(got, "backup target audit") {
				t.Errorf("refusal = %q, want it to name the target", got)
			}
			if !strings.Contains(got, "s3://acme") {
				t.Errorf("refusal = %q, want it to name the bucket", got)
			}
		})
	}
}

// TestBackupTargetSharingABucketWithDisjointPrefixesIsAllowed is the
// false-positive half. One bucket with unrelated prefixes is the shape an
// operator picks on purpose, and a refusal here would be a refusal nobody
// believes — which is worse than no check, because the check that matters is
// the one in the test above.
func TestBackupTargetSharingABucketWithDisjointPrefixesIsAllowed(t *testing.T) {
	for _, c := range []struct{ storagePrefix, targetPrefix string }{
		{"hot", "backups"},
		{"hot/", "backups/"},
		{"arc/hot", "arc/backups"},
		// The #534 sibling shape: "arc-backups" must not read as a child of
		// "arc".
		{"arc", "arc-backups"},
	} {
		t.Run("storage="+c.storagePrefix+" target="+c.targetPrefix, func(t *testing.T) {
			writeArcToml(t, `
[storage]
backend = "s3"
s3_bucket = "acme"
s3_prefix = "`+c.storagePrefix+`"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "s3"
s3_bucket = "acme"
s3_prefix = "`+c.targetPrefix+`"
`)
			if _, err := Load(); err != nil {
				t.Fatalf("Load() = %v, want success: one bucket with disjoint prefixes is legitimate", err)
			}
		})
	}
}

// TestBackupTargetOverlapWithTheColdTierIsRefused: the same primitives, a
// different pair. The cold tier's prefix comes from the same kind of config
// block, so the two can land in one bucket by a one-line mistake.
func TestBackupTargetOverlapWithTheColdTierIsRefused(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "s3"
s3_bucket = "acme-hot"
[tiered_storage]
enabled = true
[tiered_storage.cold]
enabled = true
backend = "s3"
s3_bucket = "acme-archive"
s3_prefix = "cold"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "s3"
s3_bucket = "acme-archive"
s3_prefix = "cold/backups"
`)
	got := mustLoadError(t, "overlaps the tiered-storage cold tier")
	if !strings.Contains(got, "s3://acme-archive/cold") {
		t.Errorf("refusal = %q, want it to name the cold tier location", got)
	}
}

// TestBackupTargetOverlapIgnoresADisabledColdTier: the runtime enters the
// cold-tier path only under tiered_storage.enabled and then cold.enabled, so
// refusing a disabled cold block would be a false-positive boot failure of
// exactly the shape the existing cold validation avoids.
func TestBackupTargetOverlapIgnoresADisabledColdTier(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "s3"
s3_bucket = "acme-hot"
[tiered_storage]
enabled = false
[tiered_storage.cold]
enabled = true
backend = "s3"
s3_bucket = "acme-archive"
s3_prefix = "cold"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "s3"
s3_bucket = "acme-archive"
s3_prefix = "cold/backups"
`)
	if _, err := Load(); err != nil {
		t.Fatalf("Load() = %v, want success: a disabled cold tier is never built, so it cannot be overlapped", err)
	}
}

// TestBackupLocalPathOverlapWithTheStorageRootIsRefused: the hazard predates
// targets. "./data/arc" and "./data/backups" are one typo apart, and a backup
// directory inside the storage root is both swept by reconciliation and copied
// into the next backup.
func TestBackupLocalPathOverlapWithTheStorageRootIsRefused(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "local"
local_path = "./data/arc"
[backup]
local_path = "./data/arc/backups"
`)
	got := mustLoadError(t, "overlaps primary storage")
	if !strings.Contains(got, "backup.local_path") {
		t.Errorf("refusal = %q, want it to name backup.local_path", got)
	}
}

// TestBackupDestinationOverlapIsNotCheckedWhenTheAPIIsOff: cmd/arc/main.go
// builds no destination when backup.enabled is false, so refusing an overlap
// nothing would ever write to is a false-positive boot failure.
func TestBackupDestinationOverlapIsNotCheckedWhenTheAPIIsOff(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "local"
local_path = "./data/arc"
[backup]
enabled = false
local_path = "./data/arc/backups"
`)
	if _, err := Load(); err != nil {
		t.Fatalf("Load() = %v, want success: with backup.enabled=false there is no destination to overlap", err)
	}
}

// TestDefaultsDoNotOverlap guards the one configuration every deployment runs
// in: a default Load must not refuse itself.
func TestDefaultsDoNotOverlap(t *testing.T) {
	writeArcToml(t, "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with no config = %v, want success", err)
	}
	if cfg.Backup.LocalPath != "./data/backups" {
		t.Errorf("Backup.LocalPath = %q, want \"./data/backups\"", cfg.Backup.LocalPath)
	}
	if cfg.Storage.LocalPath != "./data/arc" {
		t.Errorf("Storage.LocalPath = %q, want \"./data/arc\"", cfg.Storage.LocalPath)
	}
	if cfg.Backup.DefaultTarget != "" || cfg.Backup.Targets != nil {
		t.Errorf("default config has a target (%q, %v), want none", cfg.Backup.DefaultTarget, cfg.Backup.Targets)
	}
}

// TestDefaultIncludeConfigIsFalseOnlyForARemoteTarget: arc.toml holds the
// target's own credentials, so the default must not copy it into the store
// those credentials unlock. A LOCAL target keeps the old default, because a
// local directory is not the thing the credentials open.
func TestDefaultIncludeConfigIsFalseOnlyForARemoteTarget(t *testing.T) {
	cases := []struct {
		name       string
		targetType string
		want       bool
	}{
		{"no target at all", "", true},
		{"local target", "local", true},
		{"s3 target", "s3", false},
		{"minio target", "minio", false},
		{"azure target", "azure", false},
		{"azblob target", "azblob", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := BackupConfig{}
			if c.targetType != "" {
				b.DefaultTarget = "audit"
				b.Targets = map[string]BackupTargetConfig{
					"audit": {Name: "audit", Type: c.targetType},
				}
			}
			if got := b.DefaultIncludeConfig(); got != c.want {
				t.Errorf("DefaultIncludeConfig() with a %q target = %v, want %v", c.targetType, got, c.want)
			}
		})
	}
}

// TestBackupTargetRequiredFields: a destination missing the field that names
// it would be built against the provider default and fail at the first write,
// long after the operator stopped watching.
func TestBackupTargetRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		stanza string
		want   string
	}{
		{
			name:   "no type",
			stanza: "s3_bucket = \"acme\"",
			want:   "backup.targets.audit.type is not set",
		},
		{
			name:   "unknown type",
			stanza: "type = \"gcs\"",
			want:   "backup.targets.audit.type \"gcs\" is invalid",
		},
		{
			name:   "local with no path",
			stanza: "type = \"local\"",
			want:   "backup.targets.audit.local_path is empty",
		},
		{
			name:   "s3 with no bucket",
			stanza: "type = \"s3\"",
			want:   "backup.targets.audit.s3_bucket is empty",
		},
		{
			name:   "azure with no container",
			stanza: "type = \"azure\"\nazure_account_name = \"acct\"",
			want:   "backup.targets.audit.azure_container is empty",
		},
		{
			name:   "azure with no credentials",
			stanza: "type = \"azure\"\nazure_container = \"arc\"",
			want:   "neither backup.targets.audit.azure_account_name nor backup.targets.audit.azure_connection_string is set",
		},
		{
			name:   "unusable prefix",
			stanza: "type = \"s3\"\ns3_bucket = \"acme\"\ns3_prefix = \"a//b\"",
			want:   "invalid backup.targets.audit.s3_prefix",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			writeArcToml(t, "[backup]\ndefault_target = \"audit\"\n[backup.targets.audit]\n"+c.stanza+"\n")
			mustLoadError(t, c.want)
		})
	}
}

// TestEmptyBackupLocalPathWithNoTargetIsRefusedAsItself: an empty
// backup.local_path has to be refused for what it IS, not through the overlap
// check.
//
// storage.LocalDestination("") resolves through filepath.Abs, which answers the
// WORKING DIRECTORY, and the default storage root "./data/arc" lies under it —
// so without this guard the overlap refusal fires and names a path the operator
// never set. Measured before the fix: "backup.local_path is /srv/arc, which
// overlaps primary storage at /srv/arc/data/arc". It is also the configuration
// that used to boot (NewManager logged at Error and the backup API was
// skipped), so getting the message right matters on upgrade.
func TestEmptyBackupLocalPathWithNoTargetIsRefusedAsItself(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "local"
local_path = "./data/arc"
[backup]
enabled = true
local_path = ""
`)
	got := mustLoadError(t, "backup.local_path is empty")
	if strings.Contains(got, "overlaps primary storage") {
		t.Errorf("refusal = %q, want it to report the empty value rather than an overlap against the working directory", got)
	}
	for _, want := range []string{"backup.default_target", "backup.enabled=false"} {
		if !strings.Contains(got, want) {
			t.Errorf("refusal = %q, want it to offer %q as a way out", got, want)
		}
	}
}

// TestOverlapIsRefusedAcrossTwoSpellingsOfOneStore drives the missed refusal
// through the real Load(), because that is where it was missed: the unit test
// for Destination.Overlaps passed while this configuration booted.
//
// Same bucket, same prefix, one store spelled two ways — the exact
// configuration the refusal exists to stop, and it used to be accepted.
func TestOverlapIsRefusedAcrossTwoSpellingsOfOneStore(t *testing.T) {
	cases := []struct {
		name            string
		storageEndpoint string
		targetEndpoint  string
	}{
		{"AWS by omission vs the regional endpoint", "", "s3.us-east-1.amazonaws.com"},
		{"the regional endpoint vs AWS by omission", "s3.us-east-1.amazonaws.com", ""},
		{"AWS by omission vs virtual-hosted addressing", "", "acme.s3.amazonaws.com"},
		{"two regional spellings", "s3.us-east-1.amazonaws.com", "s3-us-east-1.amazonaws.com"},
		{"a scheme on one side only", "s3.amazonaws.com", "https://s3.amazonaws.com"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			writeArcToml(t, `
[storage]
backend = "s3"
s3_bucket = "acme"
s3_prefix = "arc"
s3_endpoint = "`+c.storageEndpoint+`"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "s3"
s3_bucket = "acme"
s3_prefix = "arc"
s3_endpoint = "`+c.targetEndpoint+`"
`)
			mustLoadError(t, "overlaps primary storage")
		})
	}
}

// TestOverlapIsRefusedAcrossTwoSpellingsOfOneAzureAccount is the Azure twin,
// also through Load(): one account reached by a connection string on one side
// and named outright on the other.
//
// A connection string is how AccountName comes to be empty —
// NewAzureBlobBackend authenticates from the string alone — so this is the
// Load-valid pair in which the account has to be DERIVED from a blob endpoint
// rather than read off a key. Giving the target both an account name and an
// endpoint would have proven nothing, because the account name wins and the
// derivation would never run.
func TestOverlapIsRefusedAcrossTwoSpellingsOfOneAzureAccount(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "azure"
azure_connection_string = "BlobEndpoint=https://acmearc.blob.core.windows.net;SharedAccessSignature=sv=2021-08-06&ss=b&srt=sco&sp=rwl"
azure_container = "arcdata"
azure_prefix = "hot"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "azure"
azure_account_name = "acmearc"
azure_container = "arcdata"
azure_prefix = "hot/backups"
`)
	mustLoadError(t, "overlaps primary storage")
}

// TestANonAWSTargetBesideAnAWSPrimaryIsAllowed keeps the fold from going too
// far: a MinIO bucket and an AWS bucket of the same name are two buckets, and
// refusing that pair would be a false positive on the one check whose value is
// being believed.
func TestANonAWSTargetBesideAnAWSPrimaryIsAllowed(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "s3"
s3_bucket = "acme"
s3_prefix = "arc"
[backup]
default_target = "audit"
[backup.targets.audit]
type = "minio"
s3_bucket = "acme"
s3_prefix = "arc"
s3_endpoint = "minio.internal:9000"
s3_path_style = true
`)
	if _, err := Load(); err != nil {
		t.Fatalf("Load() = %v, want success: an AWS bucket and a MinIO bucket of the same name are two buckets", err)
	}
}

// TestAnOverLongTargetPrefixIsRefusedAtLoadNotByVanishing closes the band in
// which the whole backup interface used to disappear.
//
// Measured before the fix: 968 bytes boots, 1019 boots, 1020 is refused by the
// prefix validator — and in between, NewManager failed, cmd/arc/main.go logged
// at Error and skipped route registration, so every backup route answered 404
// with nothing in the response to say why. Every other target-shape mistake is
// a load-time refusal; "the feature vanished" is not a diagnosis an operator
// can reach.
func TestAnOverLongTargetPrefixIsRefusedAtLoadNotByVanishing(t *testing.T) {
	// A legal prefix of exactly n bytes: 200-byte segments, since one segment
	// is capped far below this.
	build := func(n int) string {
		var sb strings.Builder
		for sb.Len()+201 <= n {
			sb.WriteString(strings.Repeat("a", 200))
			sb.WriteString("/")
		}
		sb.WriteString(strings.Repeat("b", n-sb.Len()))
		return sb.String()
	}
	load := func(t *testing.T, prefix string) error {
		t.Helper()
		writeArcToml(t, "[backup]\ndefault_target = \"audit\"\n[backup.targets.audit]\ntype = \"s3\"\ns3_bucket = \"acme\"\ns3_prefix = \""+prefix+"\"\n")
		_, err := Load()
		return err
	}

	// 967 bytes plus the trailing separator the validator adds is 968, the
	// maximum. Spelled as literals rather than computed from the constant the
	// production code uses.
	if err := load(t, build(967)); err != nil {
		t.Fatalf("Load() with a 968-byte effective prefix = %v, want success (that is the maximum)", err)
	}

	for _, n := range []int{968, 1000, 1018} {
		got := load(t, build(n))
		if got == nil {
			t.Errorf("Load() with a %d-byte prefix succeeded; the backup API would have vanished at runtime instead", n+1)
			continue
		}
		for _, want := range []string{
			"backup.targets.audit.s3_prefix",
			"968-byte maximum",
			"up to 51 bytes",
			"1019-byte object name limit",
		} {
			if !strings.Contains(got.Error(), want) {
				t.Errorf("refusal for a %d-byte prefix does not contain %q; got: %v", n+1, want, got)
			}
		}
	}
}

// TestADisabledBackupAPIDoesNotRefuseTheBoot: with the interface off, nothing
// reads a target, so a stray default_target left behind after disabling it must
// not stop a node from booting. Same gate, same reason, as the cold tier.
func TestADisabledBackupAPIDoesNotRefuseTheBoot(t *testing.T) {
	writeArcToml(t, `
[backup]
enabled = false
default_target = "audit"
`)
	if _, err := Load(); err != nil {
		t.Fatalf("Load() = %v, want success: with backup.enabled=false no target is ever read", err)
	}
}

// TestAnEmptyStorageLocalPathIsRefusedAsItself is the symmetric guard to
// backup.local_path's: storage.LocalDestination("") resolves to the WORKING
// DIRECTORY, so the overlap message would name a path the operator never set,
// and the working directory contains almost everything.
func TestAnEmptyStorageLocalPathIsRefusedAsItself(t *testing.T) {
	writeArcToml(t, `
[storage]
backend = "local"
local_path = ""
[backup]
enabled = true
local_path = "./data/backups"
`)
	got := mustLoadError(t, "storage.local_path is empty")
	if strings.Contains(got, "overlaps primary storage") {
		t.Errorf("refusal = %q, want it to report the empty value rather than an overlap against the working directory", got)
	}
}
