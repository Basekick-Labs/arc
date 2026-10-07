package config

// Backup destinations (#1085 stage B2b-1).
//
// Before this, a backup had exactly one destination: a local directory named
// by backup.local_path. A named target makes that destination configurable and
// lets it be an object store, which is the point — a backup on the same disk
// as the data it backs up is not a backup.
//
// B2b-1 allows exactly ONE target. Per-database routing, several targets and
// the index that resolves them are B2b-2. The refusal of a second target is
// here rather than left to misbehave, because a silently-ignored second target
// is a destination an operator believes they configured.
//
// Shape: [backup.targets.<name>] in a file, every field also settable as
// ARC_BACKUP_TARGETS_<NAME>_<FIELD>. Field names mirror [tiered_storage.cold]
// exactly, because that is the one existing block in this file that describes
// an object-store destination and an operator who has configured one should
// not have to learn a second vocabulary.

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/spf13/viper"
)

// backupTargetName is the accepted spelling of a target name.
//
// Lowercase, digits and underscore only, matched after viper has lowercased
// the key — so case is NOT a hazard and the earlier claim that an uppercase
// name "could never be addressed again" was wrong: both the file and the
// environment are lowercased, so "Audit" and "audit" are one target and both
// work.
//
// The real rule is the HYPHEN. It cannot appear in an environment variable
// name, so a hyphenated target could be set in a file and then never
// overridden from the environment — which is how a credential ends up in a
// config file for good.
//
// #1085's own example spells a target "audit-bucket", and that name is
// REFUSED for exactly that reason. Anyone reading the issue will copy it, so
// the refusal says what to write instead.
var backupTargetName = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// BackupTargetConfig is one configured backup destination.
//
// Credentials are here because an object-store destination needs them and
// because [tiered_storage.cold] already carries its own; it is also why
// include_config defaults to false for a remote target (see
// BackupConfig.DefaultIncludeConfig): arc.toml holds these fields verbatim,
// and copying it into the very destination they address puts the credentials
// for a backup store inside the backups.
type BackupTargetConfig struct {
	// Name is the target as an operator spells it, already validated against
	// backupTargetName. It is recorded in each backup's manifest.
	Name string
	// Type names the backend: local, s3, minio, azure or azblob. Required —
	// there is deliberately no default, because the whole reason a target
	// exists is that its destination is not the built-in one.
	Type string

	LocalPath string

	S3Bucket    string
	S3Region    string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool
	S3PathStyle bool
	S3Prefix    string

	AzureContainer          string
	AzurePrefix             string
	AzureConnectionString   string
	AzureAccountName        string
	AzureAccountKey         string
	AzureSASToken           string
	AzureEndpoint           string
	AzureUseManagedIdentity bool
}

// IsRemote reports whether the target is an object store rather than a
// directory on this machine.
func (t BackupTargetConfig) IsRemote() bool {
	switch strings.ToLower(strings.TrimSpace(t.Type)) {
	case "s3", "minio", "azure", "azblob":
		return true
	}
	return false
}

// BackendSpec renders the target as the storage factory's destination
// description, so a target is constructed by exactly the same code as primary
// storage and the cold tier (internal/storage/factory.go).
func (t BackupTargetConfig) BackendSpec() storage.BackendSpec {
	return storage.BackendSpec{
		Type:      t.Type,
		LocalPath: t.LocalPath,
		S3: storage.S3Config{
			Bucket:    t.S3Bucket,
			Region:    t.S3Region,
			Endpoint:  t.S3Endpoint,
			AccessKey: t.S3AccessKey,
			SecretKey: t.S3SecretKey,
			UseSSL:    t.S3UseSSL,
			PathStyle: t.S3PathStyle,
			Prefix:    t.S3Prefix,
		},
		Azure: storage.AzureBlobConfig{
			ConnectionString:   t.AzureConnectionString,
			AccountName:        t.AzureAccountName,
			AccountKey:         t.AzureAccountKey,
			SASToken:           t.AzureSASToken,
			ContainerName:      t.AzureContainer,
			Prefix:             t.AzurePrefix,
			Endpoint:           t.AzureEndpoint,
			UseManagedIdentity: t.AzureUseManagedIdentity,
		},
	}
}

// KeyPrefix is the object-key prefix every object this target stores carries,
// with its trailing separator, or "" for a local target and for an object
// target rooted at the bucket. It is what the backup manager reserves key
// headroom for: on an object store the stored object name is prefix+key, and
// nothing else in the stack bounds the two together.
//
// Returns an error for a prefix the backend would refuse, so a caller cannot
// quietly reserve headroom for a prefix that will never be used.
func (t BackupTargetConfig) KeyPrefix() (string, error) {
	switch strings.ToLower(strings.TrimSpace(t.Type)) {
	case "s3", "minio":
		return storage.ValidateObjectPrefix(t.S3Prefix)
	case "azure", "azblob":
		return storage.ValidateObjectPrefix(t.AzurePrefix)
	default:
		return "", nil
	}
}

// discoverBackupTargetNames returns the configured target names, sorted.
//
// Two sources, unioned, on the cluster.seeds precedent:
//
//   - the keys of the backup.targets map in a config file. This is the only
//     viper map read in this package; everything else, including every FIELD
//     of a target, is a literal dotted key so that AutomaticEnv resolves it.
//     The map is read for NAMES alone.
//   - backup.target_names, comma-separated. A name cannot be discovered from
//     a map that does not exist, so a deployment configured entirely from the
//     environment needs this to say which names to look for. Parsed through
//     parseStringSlice for the reason recorded there: GetStringSlice does not
//     split a comma-separated environment value.
func discoverBackupTargetNames(v *viper.Viper) ([]string, error) {
	seen := map[string]bool{}
	for name := range v.GetStringMap("backup.targets") {
		seen[name] = true
	}
	for _, name := range parseStringSlice(v.GetString("backup.target_names")) {
		seen[name] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		if !backupTargetName.MatchString(name) {
			return nil, fmt.Errorf(
				"invalid backup target name %q: use lowercase letters, digits and underscore only, at most 64 characters (a hyphen cannot appear in an environment variable, so a hyphenated target could never be overridden from the environment; write %q instead)",
				name, suggestTargetName(name))
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// suggestTargetName repairs a refused name into a legal one, so the refusal can
// say what to write. Truncated to the limit as well as folded, because the
// suggestion has to be a name that is actually accepted — a 70-character name
// with hyphens would otherwise be answered with a 70-character suggestion that
// is refused for the other reason.
func suggestTargetName(name string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(name) {
		switch {
		case (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_':
			b.WriteRune(c)
		default:
			b.WriteRune('_')
		}
		if b.Len() == 64 {
			break
		}
	}
	if b.Len() == 0 {
		return "audit"
	}
	return b.String()
}

// setBackupTargetDefaults registers the per-field defaults for one discovered
// target.
//
// These cannot live in setDefaults, which runs before any config file has been
// read and therefore before any target name is known. The values mirror
// [tiered_storage.cold]'s defaults one for one; type has none on purpose (see
// BackupTargetConfig.Type).
func setBackupTargetDefaults(v *viper.Viper, name string) {
	k := "backup.targets." + name + "."
	v.SetDefault(k+"type", "")
	v.SetDefault(k+"local_path", "")
	v.SetDefault(k+"s3_bucket", "")
	v.SetDefault(k+"s3_region", "us-east-1")
	v.SetDefault(k+"s3_endpoint", "")
	v.SetDefault(k+"s3_access_key", "")
	v.SetDefault(k+"s3_secret_key", "")
	v.SetDefault(k+"s3_use_ssl", true)
	v.SetDefault(k+"s3_path_style", false)
	v.SetDefault(k+"s3_prefix", "")
	v.SetDefault(k+"azure_container", "")
	v.SetDefault(k+"azure_prefix", "")
	v.SetDefault(k+"azure_connection_string", "")
	v.SetDefault(k+"azure_account_name", "")
	v.SetDefault(k+"azure_account_key", "")
	v.SetDefault(k+"azure_sas_token", "")
	v.SetDefault(k+"azure_endpoint", "")
	v.SetDefault(k+"azure_use_managed_identity", false)
}

// loadBackupTargets discovers and reads every configured backup target.
//
// Every field is a literal dotted key, so ARC_BACKUP_TARGETS_AUDIT_S3_BUCKET
// overrides the file value of [backup.targets.audit].s3_bucket exactly as the
// flat blocks elsewhere in this file behave. Values are trimmed here rather
// than at the use site for the reason the primary and cold blocks are: a
// copy-pasted trailing space passes an emptiness check and then produces an
// opaque connection failure.
func loadBackupTargets(v *viper.Viper) (map[string]BackupTargetConfig, error) {
	names, err := discoverBackupTargetNames(v)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}
	targets := make(map[string]BackupTargetConfig, len(names))
	for _, name := range names {
		setBackupTargetDefaults(v, name)
		k := "backup.targets." + name + "."
		get := func(field string) string { return strings.TrimSpace(v.GetString(k + field)) }
		targets[name] = BackupTargetConfig{
			Name:                    name,
			Type:                    strings.ToLower(get("type")),
			LocalPath:               get("local_path"),
			S3Bucket:                get("s3_bucket"),
			S3Region:                get("s3_region"),
			S3Endpoint:              get("s3_endpoint"),
			S3AccessKey:             v.GetString(k + "s3_access_key"),
			S3SecretKey:             v.GetString(k + "s3_secret_key"),
			S3UseSSL:                v.GetBool(k + "s3_use_ssl"),
			S3PathStyle:             v.GetBool(k + "s3_path_style"),
			S3Prefix:                get("s3_prefix"),
			AzureContainer:          get("azure_container"),
			AzurePrefix:             get("azure_prefix"),
			AzureConnectionString:   get("azure_connection_string"),
			AzureAccountName:        get("azure_account_name"),
			AzureAccountKey:         v.GetString(k + "azure_account_key"),
			AzureSASToken:           v.GetString(k + "azure_sas_token"),
			AzureEndpoint:           get("azure_endpoint"),
			AzureUseManagedIdentity: v.GetBool(k + "azure_use_managed_identity"),
		}
	}
	return targets, nil
}

// prefixKeyName is the prefix key an operator edits for a target of this type,
// so a refusal names the line rather than the concept.
func prefixKeyName(targetType string) string {
	switch strings.ToLower(strings.TrimSpace(targetType)) {
	case "azure", "azblob":
		return "azure_prefix"
	default:
		return "s3_prefix"
	}
}

// targetNames returns the configured target names, sorted — the "discovered
// set" a refusal message names.
func (c *BackupConfig) targetNames() []string {
	names := make([]string, 0, len(c.Targets))
	for name := range c.Targets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// validateBackupTargets checks every configured target and resolves
// backup.default_target.
//
// Required-field checks mirror the primary and cold blocks: an object-store
// destination with no bucket or container would be built against the provider
// default and fail at the first write, long after the operator stopped
// watching.
func (c *Config) validateBackupTargets() error {
	b := &c.Backup
	if len(b.Targets) > 1 {
		return fmt.Errorf(
			"backup.targets names %d targets (%s) but only one is supported in this release; per-database routing across several targets is not implemented yet, and a second target would be silently ignored",
			len(b.Targets), strings.Join(b.targetNames(), ", "))
	}

	for _, name := range b.targetNames() {
		t := b.Targets[name]
		key := "backup.targets." + name + "."
		switch t.Type {
		case "":
			return fmt.Errorf("%stype is not set; set it to \"local\", \"s3\", \"minio\", \"azure\" or \"azblob\"", key)
		case "local":
			if t.LocalPath == "" {
				return fmt.Errorf("%stype is \"local\" but %slocal_path is empty; set %slocal_path", key, key, key)
			}
		case "s3", "minio":
			if t.S3Bucket == "" {
				return fmt.Errorf("%stype is %q but %ss3_bucket is empty; set %ss3_bucket", key, t.Type, key, key)
			}
			if err := c.checkObjectPrefix(key+"s3_prefix", t.S3Prefix); err != nil {
				return err
			}
		case "azure", "azblob":
			if t.AzureConnectionString == "" && t.AzureAccountName == "" {
				return fmt.Errorf("%stype is %q but neither %sazure_account_name nor %sazure_connection_string is set; provide one", key, t.Type, key, key)
			}
			if t.AzureContainer == "" {
				return fmt.Errorf("%stype is %q but %sazure_container is empty; set %sazure_container", key, t.Type, key, key)
			}
			if err := c.checkObjectPrefix(key+"azure_prefix", t.AzurePrefix); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%stype %q is invalid; must be \"local\", \"s3\", \"minio\", \"azure\" or \"azblob\"", key, t.Type)
		}
		// The prefix has to leave room for the keys a backup writes under it,
		// and that is a LOAD-TIME refusal rather than a NewManager error.
		// Between 969 and 1019 bytes a prefix passes ValidateObjectPrefix,
		// fails NewManager, and cmd/arc/main.go then logs at Error and skips
		// route registration — so every backup route answers 404 and the whole
		// interface silently disappears. Every other target-shape mistake in
		// this function refuses the boot; "the feature vanished" is not a
		// diagnosis an operator can reach.
		// KeyPrefix cannot fail here — checkObjectPrefix above ran the same
		// ValidateObjectPrefix over the same value, and a local target has no
		// prefix at all — so this is the ordinary defensive return, not a
		// check for a disagreement between two validators. The VALUE is what
		// is wanted: the bound below needs the normalised prefix, trailing
		// separator included, because that is what the backend will apply.
		prefix, err := t.KeyPrefix()
		if err != nil {
			return fmt.Errorf("invalid prefix for backup target %q: %w", name, err)
		}
		if err := storage.CheckBackupTargetPrefix(key+prefixKeyName(t.Type), prefix); err != nil {
			return err
		}
	}

	// default_target resolution. Both failures below are errors rather than a
	// fall back to backup.local_path, for one reason: the fallback would write
	// the backup to a destination the operator did not ask for, and the only
	// signal would be a log line nobody reads until a restore.
	switch {
	case b.DefaultTarget == "" && len(b.Targets) > 0:
		return fmt.Errorf(
			"backup.targets defines %s but backup.default_target is not set, so no backup would be written there; set backup.default_target to the target name",
			strings.Join(b.targetNames(), ", "))
	case b.DefaultTarget != "" && len(b.Targets) == 0:
		return fmt.Errorf(
			"backup.default_target is %q but no backup target is configured; define [backup.targets.%s] in the config file, or set ARC_BACKUP_TARGET_NAMES=%s together with the target fields when configuring from the environment",
			b.DefaultTarget, b.DefaultTarget, b.DefaultTarget)
	case b.DefaultTarget != "":
		if _, ok := b.Targets[b.DefaultTarget]; !ok {
			return fmt.Errorf(
				"backup.default_target is %q but the configured backup targets are: %s",
				b.DefaultTarget, strings.Join(b.targetNames(), ", "))
		}
	}
	return nil
}

// DefaultBackupTarget returns the target every backup is written to, or nil
// when none is configured and the destination is backup.local_path as before.
func (c *BackupConfig) DefaultBackupTarget() *BackupTargetConfig {
	if c.DefaultTarget == "" {
		return nil
	}
	t, ok := c.Targets[c.DefaultTarget]
	if !ok {
		return nil
	}
	return &t
}

// DefaultIncludeConfig is whether a backup should copy arc.toml when the
// request did not say.
//
// False for a remote default target. arc.toml carries that target's own
// credentials verbatim (BackupTargetConfig), so the default would put the keys
// to the backup store inside every backup held in it. An operator who wants it
// anyway says so per request and is warned; the default is the safe one
// because it is the one nobody chooses.
func (c *BackupConfig) DefaultIncludeConfig() bool {
	t := c.DefaultBackupTarget()
	return t == nil || !t.IsRemote()
}

// checkBackupDestinationOverlap refuses a configuration in which the backup
// destination and another writer of the same store are the same place.
//
// A load-time error and never a warning, because of what the overlapping
// configuration DOES. Stated concretely rather than by appeal to an
// invariant — an earlier draft of this comment justified the refusal as the
// thing that upholds internal/backup.cleanupPartialBackupWrite's premise, and
// that was the wrong framing: that function's keys are all "<backupID>/…" for
// an ID the run minted, a shape no other writer in Arc produces, so the key
// namespace carries the premise whatever the configuration says.
//
// The real hazards, both permanent and both silent:
//
//   - A BACKUP THAT RE-COPIES ITSELF. The data listing returns every .parquet
//     under the storage root, so a destination inside it is inventoried by the
//     next backup, which copies the previous one in full, and the one after
//     that copies both. On a cluster those files are absent from the Raft
//     manifest, so the unregistered-skip count climbs, and past the skip ratio
//     every replace-mode restore is refused.
//   - THE RECONCILIATION SWEEP DELETING THE BACKUPS. Its managed-path
//     heuristic (looksLikeManagedPath) triggers at seven path segments and a
//     backup data key has nine, so with reconciliation enabled and
//     manifest_only_dry_run off the sweep removes them after the grace window.
//
// On a shared object store the same two follow from a prefix that is a parent
// of the other, plus one more: the backup listing and the cold tier become one
// listing.
//
// Checked at load, not in main.go, for the reason recorded on
// checkObjectPrefix: the message has to name paths, buckets and prefixes, and
// installErrSanitizer masks quoted spans in everything the logger emits. A
// load-time error is printed before the logger exists.
//
// NOTE the limit of this guarantee: a backup.Manager constructed directly,
// which every test in internal/backup does, never passes through Load and so
// is not covered. cmd/arc/main.go is the only non-test caller.
//
// Applied to backup.local_path as well as to a target, because the hazard
// predates targets: "./data/arc" and "./data/backups" are one typo apart.
func (c *Config) checkBackupDestinationOverlap() error {
	dest, destKey, err := c.backupDestination()
	if err != nil {
		return err
	}

	// Each of these three can only fail on a prefix, and every prefix key has
	// already been through checkObjectPrefix earlier in Load, so these are
	// unreachable. Wrapped anyway: an unwrapped "storage prefix ... is not
	// usable" would not say WHICH of three config blocks it came from, and a
	// message that cannot be acted on is the failure mode checkObjectPrefix
	// exists to avoid.
	primary, err := c.primaryStorageDestination()
	if err != nil {
		return fmt.Errorf("cannot resolve the primary storage location to check it against the backup destination: %w", err)
	}
	if dest.Overlaps(primary) {
		return fmt.Errorf(
			"%s is %s, which overlaps primary storage at %s; every subsequent backup would then copy the previous one, because the data listing returns every Parquet file under the storage root, and on a cluster that climbs until the skip ratio refuses every replace-mode restore. With reconciliation enabled and manifest_only_dry_run off, its sweep deletes the backups outright. Point the destination outside the storage root",
			destKey, dest.String(), primary.String())
	}

	// Only when the cold tier would actually be built: the runtime enters the
	// cold-tier path under tiered_storage.enabled and then cold.enabled, so
	// checking a disabled cold block would refuse a configuration the runtime
	// ignores entirely — the same false-positive boot failure the cold
	// validation above avoids.
	if c.TieredStorage.Enabled && c.TieredStorage.Cold.Enabled {
		cold, err := c.coldTierDestination()
		if err != nil {
			return fmt.Errorf("cannot resolve the tiered-storage cold tier location to check it against the backup destination: %w", err)
		}
		if dest.Overlaps(cold) {
			return fmt.Errorf(
				"%s is %s, which overlaps the tiered-storage cold tier at %s; the two would share one listing, so each would enumerate the other objects, and a migrated file would be copied into the backup as data. Use a different bucket or a prefix that is not a parent of the other",
				destKey, dest.String(), cold.String())
		}
	}
	return nil
}

// backupDestination is where a backup is written, plus the configuration key
// that names it so a refusal tells the operator which line to edit.
func (c *Config) backupDestination() (storage.Destination, string, error) {
	if t := c.Backup.DefaultBackupTarget(); t != nil {
		key := "backup target " + t.Name
		d, err := storage.DestinationFromSpec(t.BackendSpec())
		if err != nil {
			return storage.Destination{}, key, fmt.Errorf("cannot resolve the location of %s: %w", key, err)
		}
		return d, key, nil
	}
	if c.Backup.LocalPath == "" {
		// NOT merely a missing required field: storage.LocalDestination("")
		// resolves through filepath.Abs, which answers the WORKING DIRECTORY,
		// and the default storage root "./data/arc" is under it — so the
		// overlap check would refuse the boot naming a path the operator never
		// set. Caught by the same ResolveExistingPath("") hazard the Iceberg
		// containment check guards against (see localDestinationPath in
		// internal/backup/warehouse.go). Refused as itself instead.
		return storage.Destination{}, "backup.local_path", fmt.Errorf(
			"backup.enabled is true but backup.local_path is empty and no backup target is configured; set backup.local_path, configure a target and point backup.default_target at it, or set backup.enabled=false")
	}
	d := storage.LocalDestination(c.Backup.LocalPath)
	return d, "backup.local_path", nil
}

// primaryStorageDestination is where ingested data lands.
//
// An empty storage.local_path is refused as itself for the same reason an
// empty backup.local_path is: storage.LocalDestination("") resolves through
// filepath.Abs, which answers the WORKING DIRECTORY, so the overlap message
// would name a path the operator never set — and the working directory
// contains almost everything, so it would also refuse configurations that are
// fine. The primary backend would fail to serve from "" anyway.
func (c *Config) primaryStorageDestination() (storage.Destination, error) {
	if c.Storage.Backend == "local" && strings.TrimSpace(c.Storage.LocalPath) == "" {
		return storage.Destination{}, fmt.Errorf(
			"storage.backend is \"local\" but storage.local_path is empty; set storage.local_path")
	}
	return storage.DestinationFromSpec(storage.BackendSpec{
		Type:      c.Storage.Backend,
		LocalPath: c.Storage.LocalPath,
		S3: storage.S3Config{
			Bucket:   c.Storage.S3Bucket,
			Endpoint: c.Storage.S3Endpoint,
			Prefix:   c.Storage.S3Prefix,
		},
		Azure: storage.AzureBlobConfig{
			ConnectionString: c.Storage.AzureConnectionString,
			AccountName:      c.Storage.AzureAccountName,
			ContainerName:    c.Storage.AzureContainer,
			Prefix:           c.Storage.AzurePrefix,
			Endpoint:         c.Storage.AzureEndpoint,
		},
	})
}

// coldTierDestination is where tiering migrates aged files. Only meaningful
// when the cold tier is enabled; the caller gates on that.
func (c *Config) coldTierDestination() (storage.Destination, error) {
	cold := c.TieredStorage.Cold
	return storage.DestinationFromSpec(storage.BackendSpec{
		Type: cold.Backend,
		S3: storage.S3Config{
			Bucket:   cold.S3Bucket,
			Endpoint: cold.S3Endpoint,
			Prefix:   cold.S3Prefix,
		},
		Azure: storage.AzureBlobConfig{
			ConnectionString: cold.AzureConnectionString,
			AccountName:      cold.AzureAccountName,
			ContainerName:    cold.AzureContainer,
			Prefix:           cold.AzurePrefix,
			Endpoint:         cold.AzureEndpoint,
		},
	})
}
