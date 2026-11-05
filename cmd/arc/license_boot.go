package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
)

func prepareLicenseOSSConfig(cfg *config.Config, record, local *cluster.LicenseBootRecord) error {
	if local == nil || local.Generation != record.Generation || local.InventorySHA256 != record.InventorySHA256 || local.Owner != record.Owner || local.Controller != record.Controller || !reflect.DeepEqual(local.Participants, record.Participants) {
		return errors.New("local prepared namespace differs from committed certificate")
	}
	return applyLicensePreparedIdentity(cfg, local)
}

// Prepared state pins identity even if a crash prevented local seal persistence.
// It never authorizes an OSS boot or changes cluster enablement/data roots.
func loadLicensePreparedIdentity(cfg *config.Config) error {
	local, err := cluster.ReadLicenseBootRecord(filepath.Join(cfg.Cluster.RaftDataDir, "license-lifecycle.json.prepared"), cfg.Cluster.ClusterName)
	if err != nil {
		return err
	}
	if local == nil {
		return nil
	}
	return applyLicensePreparedIdentity(cfg, local)
}

func applyLicensePreparedIdentity(cfg *config.Config, local *cluster.LicenseBootRecord) error {
	if local.LocalNodeID != "" {
		if cfg.Cluster.NodeID != "" && cfg.Cluster.NodeID != local.LocalNodeID {
			return errors.New("configured node identity differs from prepared namespace")
		}
		cfg.Cluster.NodeID = local.LocalNodeID
	}
	return nil
}

func configureLicenseOSSPaths(cfg *config.Config, local *cluster.LicenseBootRecord) {
	cfg.Cluster.Enabled = false
	cfg.Cluster.SharedStorageMode = false
	cfg.Cluster.Role = "standalone"
	cfg.Storage.Backend = "local"
	cfg.Storage.LocalPath = local.StoragePath
	cfg.WAL.Directory = local.WALPath
	cfg.Database.TempDirectory = filepath.Join(filepath.Dir(local.StoragePath), "query-temp")
	cfg.Compaction.TempDirectory = filepath.Join(filepath.Dir(local.StoragePath), "compaction-temp")
	if cfg.Iceberg.Enabled {
		// The predecessor catalog embeds absolute file/S3 locations and its
		// original credentials context. Never reopen it against a new backend.
		// Retain it for recovery; this generation publishes an independent view.
		cfg.Iceberg.CatalogDBPath = filepath.Join(filepath.Dir(local.StoragePath), "iceberg-catalog.db")
		path := filepath.ToSlash(filepath.Join(local.StoragePath, ".arc-iceberg-warehouse"))
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		warehouse := url.URL{Scheme: "file", Path: path}
		cfg.Iceberg.Warehouse = warehouse.String()
	}
}

// Expiration and authority refreshes do not change constructed components.
// Capabilities or resource limits do, and require rebuilding the runtime.
func licenseRuntimeShape(l *license.License) string {
	if l == nil {
		return ""
	}
	features := append([]string(nil), l.Features...)
	sort.Strings(features)
	raw, _ := json.Marshal(struct {
		Tier            license.Tier
		Cores, Machines int
		Features        []string
	}{l.Tier, l.MaxCores, l.MaxMachines, features})
	return string(raw)
}
