package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// SealLicenseOSS materializes only the sealed manifest into a fresh namespace.
// Old files, compacted inputs, journals and configured roots are never removed.
func (c *Coordinator) SealLicenseOSS(ctx context.Context, s raft.LicenseLifecycleState, base, walBase string) error {
	if s.Mode != "preparing" {
		return fmt.Errorf("only drained participants may seal OSS data")
	}
	if err := c.SyncManifest(ctx); err != nil {
		return err
	}
	base, err := filepath.Abs(base)
	if err != nil {
		return err
	}
	walBase, err = filepath.Abs(walBase)
	if err != nil {
		return err
	}
	target := licenseNamespace(base, c.cfg.ClusterName, c.localNode.ID, s.Generation+1, "storage")
	walTarget := licenseNamespace(walBase, c.cfg.ClusterName, c.localNode.ID, s.Generation+1, "wal")
	if err := os.MkdirAll(target, 0700); err != nil {
		return err
	}
	fetch, err := filereplication.NewFetchClient(filereplication.FetchClient{SelfNodeID: c.localNode.ID, ClusterName: c.cfg.ClusterName, SharedSecret: c.cfg.SharedSecret, TLSConfig: c.ClusterTLSConfig()})
	if err != nil {
		return err
	}
	dirs := map[string]bool{}
	for dir := target; ; dir = filepath.Dir(dir) {
		dirs[dir] = true
		if dir == base {
			break
		}
	}
	files := c.GetFileManifest()
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	for _, entry := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !raft.HotTier(entry.Tier) {
			continue
		}
		if err := raft.ValidateManifestPath(entry.Path); err != nil {
			return err
		}
		if entry.SHA256 == "" {
			return fmt.Errorf("manifest lacks checksum for %s", entry.Path)
		}
		dest := filepath.Join(target, filepath.FromSlash(entry.Path))
		for dir := filepath.Dir(dest); dir != filepath.Dir(target); dir = filepath.Dir(dir) {
			dirs[dir] = true
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
			return err
		}
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		hash := sha256.New()
		sink := io.MultiWriter(out, hash)
		err = c.storage.ReadTo(ctx, entry.Path, sink)
		if err != nil {
			// A local non-replica owner can fetch from the registered origin.
			if _, e := out.Seek(0, io.SeekStart); e != nil {
				out.Close()
				return e
			}
			if e := out.Truncate(0); e != nil {
				out.Close()
				return e
			}
			hash.Reset()
			origin, ok := c.raftFSM.GetNode(entry.OriginNodeID)
			if !ok || origin.ID == c.localNode.ID {
				out.Close()
				return fmt.Errorf("owner lacks manifest file %s: %w", entry.Path, err)
			}
			_, err = fetch.Fetch(ctx, origin.Address, entry, sink, 0, nil)
		}
		if err == nil && hex.EncodeToString(hash.Sum(nil)) != entry.SHA256 {
			err = fmt.Errorf("sealed file checksum mismatch: %s", entry.Path)
		}
		if err == nil {
			err = out.Sync()
		}
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	for dir := range dirs {
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = d.Sync()
		d.Close()
		if err != nil {
			return err
		}
	}
	raw, err := json.Marshal(files)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	participants := map[string]string{}
	for _, id := range s.Participants {
		n, ok := c.raftFSM.GetNode(id)
		if !ok {
			return fmt.Errorf("participant disappeared: %s", id)
		}
		participants[id] = n.Address
	}
	record := &LicenseBootRecord{Version: 1, ClusterName: c.cfg.ClusterName, Generation: s.Generation + 1, Controller: s.Controller, Owner: s.Owner, Participants: participants, StoragePath: target, WALPath: walTarget, InventorySHA256: hex.EncodeToString(digest[:])}
	record.LocalNodeID = c.localNode.ID
	// Prepared is not boot authority; only a subsequently committed certificate
	// may be installed at LicenseBootPath.
	return WriteLicenseBootRecord(c.LicenseBootPath()+".prepared", record)
}

func licenseNamespace(base, clusterName, nodeID string, generation uint64, kind string) string {
	identity, _ := json.Marshal([]string{clusterName, nodeID})
	digest := sha256.Sum256(identity)
	return filepath.Join(base, ".arc-license-oss", hex.EncodeToString(digest[:]), strconv.FormatUint(generation, 10), kind)
}

func (c *Coordinator) PersistLicenseOSS(s raft.LicenseLifecycleState) error {
	if s.Mode != "sealed" || len(s.Certificate) == 0 {
		return fmt.Errorf("missing sealed license certificate")
	}
	var record LicenseBootRecord
	if err := json.Unmarshal(s.Certificate, &record); err != nil {
		return err
	}
	if record.Generation != s.Generation || record.Owner != s.Owner || record.Controller != s.Controller {
		return fmt.Errorf("license certificate does not match committed owner")
	}
	// Local durability is not global permission to leave Raft. A process
	// restarting before global release must rejoin the predecessor consensus.
	return WriteLicenseBootRecord(c.LicenseBootPath()+".sealed", &record)
}

func (c *Coordinator) authorizeLicenseOSS(s raft.LicenseLifecycleState) error {
	record, err := ReadLicenseBootRecord(c.LicenseBootPath()+".sealed", c.cfg.ClusterName)
	if err != nil {
		return err
	}
	if record == nil || record.Generation != s.Generation || record.Owner != s.Owner || record.Controller != s.Controller {
		return fmt.Errorf("local sealed certificate is not durable")
	}
	return WriteLicenseBootRecord(c.LicenseBootPath(), record)
}
