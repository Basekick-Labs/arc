package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// validateEdgeSyncRelay checks edge_sync.spoke.relay.* and
// edge_sync.import.relay.* (#1685) for the side that is enabled.
func (c *Config) validateEdgeSyncRelay() error {
	if s := c.EdgeSync.Spoke; s.Relay.Enabled {
		const k = "edge_sync.spoke.relay"
		if s.SpokeID == "" {
			return fmt.Errorf("%s.enabled=true requires edge_sync.spoke.spoke_id", k)
		}
		if s.HubID == "" {
			return fmt.Errorf("%s.enabled=true requires edge_sync.spoke.hub_id (the hub the bundles are destined for)", k)
		}
		if s.Secret == "" {
			return fmt.Errorf("%s.enabled=true requires the ARC_EDGE_SYNC_SPOKE_SECRET environment variable: it signs every bundle, and the hub refuses an unsigned one", k)
		}
		if s.Relay.AckTimeout <= s.Relay.Interval {
			return fmt.Errorf("%s.ack_timeout (%s) must be longer than %s.interval (%s): a bundle would be taken back before the hub could answer it",
				k, s.Relay.AckTimeout, k, s.Relay.Interval)
		}
		if err := c.checkRelayTarget(k, s.Relay.Target); err != nil {
			return err
		}
	}
	if h := c.EdgeSync.Import.Relay; h.Enabled {
		const k = "edge_sync.import.relay"
		if c.EdgeSync.HubID == "" {
			return fmt.Errorf("%s.enabled=true requires edge_sync.hub_id: a bundle names the hub it is destined for, and one addressed elsewhere is left alone", k)
		}
		if h.StagingDir == "" {
			return fmt.Errorf("%s.staging_dir must be set: bundles are downloaded there to be verified before import", k)
		}
		if c.EdgeSync.Import.MaxFiles < 0 {
			return fmt.Errorf("edge_sync.import.max_files must be >= 0 (got %d); 0 means \"use the default\"", c.EdgeSync.Import.MaxFiles)
		}
		if err := c.checkRelayTarget(k, h.Target); err != nil {
			return err
		}
		if err := c.checkRelayStagingDir(k, h.StagingDir); err != nil {
			return err
		}
	}
	return nil
}

// checkRelayTarget validates a relay bucket and refuses one that overlaps
// this node's own storage: a relay under the storage root would be
// discovered as data (the spoke would sync its own outbox) or queried as a
// database (the hub would read spokes' bundles as tables).
func (c *Config) checkRelayTarget(k string, t BackupTargetConfig) error {
	switch t.Type {
	case "local":
		if t.LocalPath == "" {
			return fmt.Errorf("%s.type is \"local\" but %s.local_path is empty", k, k)
		}
		if !filepath.IsAbs(t.LocalPath) {
			return fmt.Errorf("%s.local_path %q must be absolute: spoke and hub must agree on it", k, t.LocalPath)
		}
	case "s3", "minio":
		if t.S3Bucket == "" {
			return fmt.Errorf("%s.type is %q but %s.s3_bucket is empty", k, t.Type, k)
		}
	case "azure", "azblob":
		if t.AzureContainer == "" {
			return fmt.Errorf("%s.type is %q but %s.azure_container is empty", k, t.Type, k)
		}
	case "":
		return fmt.Errorf("%s.enabled=true requires %s.type: \"local\", \"s3\", \"minio\", \"azure\" or \"azblob\"", k, k)
	case "gcs":
		return fmt.Errorf("%s.type \"gcs\" is not supported for the relay yet; use \"s3\" (GCS's S3 interoperability API works), \"azure\" or \"local\"", k)
	default:
		return fmt.Errorf("%s.type %q is invalid; must be \"local\", \"s3\", \"minio\", \"azure\" or \"azblob\"", k, t.Type)
	}
	if _, err := t.KeyPrefix(); err != nil {
		return fmt.Errorf("%s.%s: %w", k, prefixKeyName(t.Type), err)
	}

	relay, err := storage.DestinationFromSpec(t.BackendSpec())
	if err != nil {
		return fmt.Errorf("cannot resolve the location of %s: %w", k, err)
	}
	primary, err := c.primaryStorageDestination()
	if err != nil {
		return fmt.Errorf("cannot resolve the primary storage location to check it against %s: %w", k, err)
	}
	if relay.Overlaps(primary) {
		return fmt.Errorf("%s is %s, which overlaps primary storage at %s; the relay's bundles would be read as this node's own data. Use a different bucket or a prefix that is not a parent of the other",
			k, relay.String(), primary.String())
	}
	if c.TieredStorage.Enabled && c.TieredStorage.Cold.Enabled {
		cold, err := c.coldTierDestination()
		if err != nil {
			return fmt.Errorf("cannot resolve the cold tier location to check it against %s: %w", k, err)
		}
		if relay.Overlaps(cold) {
			return fmt.Errorf("%s is %s, which overlaps the cold tier at %s; use a different bucket or prefix", k, relay.String(), cold.String())
		}
	}
	if c.Backup.Enabled {
		dests, err := c.backupDestinations()
		if err != nil {
			return err
		}
		for _, d := range dests {
			if relay.Overlaps(d.dest) {
				return fmt.Errorf("%s is %s, which overlaps %s at %s; use a different bucket or prefix", k, relay.String(), d.key, d.dest.String())
			}
		}
	}
	return nil
}

// checkRelayStagingDir refuses a hub staging directory inside a local
// storage root: staged bundles would appear as a database while they wait.
func (c *Config) checkRelayStagingDir(k, dir string) error {
	if c.Storage.Backend != "local" {
		return nil
	}
	root, err := filepath.Abs(c.Storage.LocalPath)
	if err != nil {
		return nil
	}
	staging, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("%s.staging_dir %q: %w", k, dir, err)
	}
	if staging == root || strings.HasPrefix(staging, root+string(filepath.Separator)) {
		return fmt.Errorf("%s.staging_dir %q is inside storage.local_path %q; staged bundles would be read as data. Put it beside the storage root, not in it", k, dir, c.Storage.LocalPath)
	}
	return nil
}
