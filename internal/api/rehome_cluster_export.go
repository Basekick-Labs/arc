package api

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Re-home on a cluster with the Iceberg export on (#1870). One node exports:
// the compactor lease holder. Each node with the export on does the export's
// part in its own steps, on its own ack, which every driver (whatever its
// release) already waits for:
//
//	committed  the exporting node checks its table can be published (a pass
//	           now); any other node checks that an exporter exists. Failing,
//	           the node does not ack, and the job reverts on its ack timeout.
//	latched    the exporting node holds its ack until the published table no
//	           longer names the source. A node that started holding keeps
//	           holding if the lease moves away meanwhile, until the table is
//	           confirmed (the lease came back) or the wait is waived on it
//	           (POST /api/v1/rehome/<id>/resume {"skip_export_wait": true}).

// clusterExportState is this node's holds and waivers, per job.
type clusterExportState struct {
	mu      sync.Mutex
	holding map[string]bool
	waived  map[string]bool
}

func (s *clusterExportState) init() {
	if s.holding == nil {
		s.holding, s.waived = map[string]bool{}, map[string]bool{}
	}
}

func (s *clusterExportState) forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	delete(s.holding, id)
	delete(s.waived, id)
}

// exporterPresence is the cluster's answer to "does a node export now"
// (cluster.Coordinator.IcebergExporterPresent).
type exporterPresence interface {
	IcebergExporterPresent() bool
}

var errNoExporter = errors.New("the Iceberg export is on here and no node exports now (the compactor lease is not held, or its holder does not run the export)")

// clusterExportReady is this node's check before it acks committed.
func (h *RehomeHandler) clusterExportReady(ctx context.Context, j raft.RehomeJob) error {
	if !h.exportExpected() {
		return nil
	}
	if why := h.exportNotStarted(); why != "" {
		return errors.New(why)
	}
	export := h.exportFunc()
	if export == nil {
		return errors.New("the Iceberg export is not running on this node yet")
	}
	actx, cancel := context.WithTimeout(ctx, rehomeExportAsk)
	ans, err := export(actx, j.Database, j.Measurement, "")
	cancel()
	switch {
	case err != nil:
		return fmt.Errorf("the Iceberg export did not answer: %w", err)
	case ans.NotExporter:
		if p, ok := h.swap.Cluster.(exporterPresence); ok && !p.IcebergExporterPresent() {
			return errNoExporter
		}
		return nil
	case !ans.Clear:
		return fmt.Errorf("the Iceberg export cannot publish %s.%s: %s", j.Database, j.Measurement, ans.Why)
	}
	return nil
}

// clusterExportHold is this node's hold before it acks latched: nil once the
// published table does not name the source, when this node does not export
// and never held for the job, or when the wait was waived here.
func (h *RehomeHandler) clusterExportHold(ctx context.Context, j raft.RehomeJob) error {
	if !h.exportExpected() {
		return nil
	}
	s := &h.clusterExport
	s.mu.Lock()
	s.init()
	waived, holding := s.waived[j.ID], s.holding[j.ID]
	s.mu.Unlock()
	if waived {
		h.logger.Warn().Str("job", j.ID).Str("path", j.Source.Path).
			Msg("Re-home: going on without waiting for the Iceberg export, as asked. Until the export next commits this table, it names a file that is gone, and readers of the table fail on it")
		return nil
	}
	if why := h.exportNotStarted(); why != "" {
		if holding {
			return fmt.Errorf("waiting for the Iceberg export: %s", why)
		}
		return nil // this node cannot export: not its hold
	}
	export := h.exportFunc()
	if export == nil {
		return errors.New("the Iceberg export is not running on this node yet")
	}
	actx, cancel := context.WithTimeout(ctx, rehomeExportAsk)
	ans, err := export(actx, j.Database, j.Measurement, j.Source.Path)
	cancel()
	if err != nil {
		return fmt.Errorf("the Iceberg export did not answer: %w", err)
	}
	if ans.Clear {
		s.mu.Lock()
		delete(s.holding, j.ID)
		s.mu.Unlock()
		return nil
	}
	if ans.NotExporter && !holding {
		return nil
	}
	s.mu.Lock()
	s.holding[j.ID] = true
	s.mu.Unlock()
	return fmt.Errorf("waiting for the Iceberg export to publish %s.%s without %s (%s); POST /api/v1/rehome/%s/resume {\"skip_export_wait\": true} on this node ends the wait",
		j.Database, j.Measurement, j.Source.Path, ans.Why, j.ID)
}

// waiveClusterExport waives this node's export hold for a cluster job.
func (h *RehomeHandler) waiveClusterExport(id string) bool {
	if h.swap == nil || h.swap.Cluster == nil {
		return false
	}
	if _, live := h.swap.Cluster.RehomeJob(id); !live {
		return false
	}
	s := &h.clusterExport
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	s.waived[id] = true
	return true
}
