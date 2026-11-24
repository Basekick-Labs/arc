package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

// clusterExportState is this node's waivers and pacing, per job.
type clusterExportState struct {
	mu     sync.Mutex
	waived map[string]bool
	// next: when this node next asks the export for a job; lastErr is the
	// answer given until then (a hold is retried every participant tick,
	// and each question to the exporting node is a pass).
	next    map[string]time.Time
	lastErr map[string]error
	pause   map[string]time.Duration
}

func (s *clusterExportState) init() {
	if s.waived == nil {
		s.waived, s.next, s.lastErr, s.pause = map[string]bool{}, map[string]time.Time{}, map[string]error{}, map[string]time.Duration{}
	}
}

func (s *clusterExportState) forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	delete(s.waived, id)
	delete(s.next, id)
	delete(s.lastErr, id)
	delete(s.pause, id)
}

// RehomeExportCheckFunc answers, without a pass, whether the published table
// names the data file at fileKey (iceberg.Scheduler.CheckPublished): published
// says the version read is the one the discovery files name.
type RehomeExportCheckFunc func(ctx context.Context, database, measurement, fileKey string) (published, named bool, err error)

// SetExportCheck binds the read-only published-table check (#1870).
func (h *RehomeHandler) SetExportCheck(f RehomeExportCheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.exportCheck = f
}

func (h *RehomeHandler) exportCheckFunc() RehomeExportCheckFunc {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.exportCheck
}

// exporterPresence is the cluster's answer to "does a node export now"
// (cluster.Coordinator.IcebergExporterPresent).
type exporterPresence interface {
	IcebergExporterPresent() bool
}

var errNoExporter = errors.New("the Iceberg export is on here and no node exports now (the compactor lease is not held, or its holder does not run the export)")

func (h *RehomeHandler) exporterPresent() bool {
	p, ok := h.swap.Cluster.(exporterPresence)
	return !ok || p.IcebergExporterPresent()
}

// clusterExportReady is this node's check before it acks committed.
func (h *RehomeHandler) clusterExportReady(ctx context.Context, j raft.RehomeJob) error {
	if !h.exportExpected() {
		return nil
	}
	if !h.isWired() {
		return errors.New("the Iceberg export on this node is not running yet")
	}
	if why := h.exportNotStarted(); why != "" {
		// This node cannot export; another must.
		if !h.exporterPresent() {
			return errNoExporter
		}
		return nil
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
		if !h.exporterPresent() {
			return errNoExporter
		}
		return nil
	case !ans.Clear:
		return fmt.Errorf("the Iceberg export cannot publish %s.%s: %s", j.Database, j.Measurement, ans.Why)
	}
	return nil
}

// clusterExportHold is this node's hold before it acks latched, nil once:
//   - this node exports and its pass published the table without the source;
//   - on a shared warehouse, this node does not export and the published
//     table (read, no pass) does not name the source: whichever node
//     exports, and however the lease moved, every node waits for the same
//     published table;
//   - on local storage, this node does not export and some node does (each
//     node publishes its own warehouse, and only the exporting node's is
//     current, #1756);
//   - the wait was waived on this node.
func (h *RehomeHandler) clusterExportHold(ctx context.Context, j raft.RehomeJob) error {
	if !h.exportExpected() {
		return nil
	}
	s := &h.clusterExport
	s.mu.Lock()
	s.init()
	waived := s.waived[j.ID]
	next, last := s.next[j.ID], s.lastErr[j.ID]
	s.mu.Unlock()
	if waived {
		h.logger.Warn().Str("job", j.ID).Str("path", j.Source.Path).
			Msg("Re-home: going on without waiting for the Iceberg export, as asked. Until the export next commits this table, it names a file that is gone, and readers of the table fail on it")
		return nil
	}
	if !h.isWired() {
		return errors.New("the Iceberg export on this node is not running yet")
	}
	if last != nil && time.Now().Before(next) {
		return last
	}
	err := h.clusterExportHoldOnce(ctx, j)
	s.mu.Lock()
	if err == nil {
		delete(s.next, j.ID)
		delete(s.lastErr, j.ID)
		delete(s.pause, j.ID)
	} else {
		p := s.pause[j.ID]
		if p = p * 2; p < rehomeExportRetry {
			p = rehomeExportRetry
		}
		if p > rehomeExportRetryMax {
			p = rehomeExportRetryMax
		}
		s.pause[j.ID], s.next[j.ID], s.lastErr[j.ID] = p, time.Now().Add(p), err
	}
	s.mu.Unlock()
	return err
}

func (h *RehomeHandler) clusterExportHoldOnce(ctx context.Context, j raft.RehomeJob) error {
	hold := func(why string) error {
		return fmt.Errorf("waiting for the Iceberg export to publish %s.%s without %s (%s); POST /api/v1/rehome/%s/resume {\"skip_export_wait\": true} on this node ends this node's wait",
			j.Database, j.Measurement, j.Source.Path, why, j.ID)
	}
	running := h.exportNotStarted() == ""
	if running {
		export := h.exportFunc()
		if export == nil {
			return errors.New("the Iceberg export is not running on this node yet")
		}
		actx, cancel := context.WithTimeout(ctx, rehomeExportAsk)
		ans, err := export(actx, j.Database, j.Measurement, j.Source.Path)
		cancel()
		switch {
		case err != nil:
			return hold("the export did not answer: " + err.Error())
		case ans.Clear:
			return nil
		case !ans.NotExporter:
			return hold(ans.Why)
		}
	}
	// Local storage, or a node whose export never started (its scheduler
	// cannot read the table): the exporting node holds.
	if !h.sharedStore() || !running {
		if !h.exporterPresent() {
			return hold("no node exports now")
		}
		return nil
	}
	check := h.exportCheckFunc()
	if check == nil {
		return hold("this node cannot read the published table")
	}
	actx, cancel := context.WithTimeout(ctx, rehomeExportAsk)
	published, named, err := check(actx, j.Database, j.Measurement, j.Source.Path)
	cancel()
	switch {
	case err != nil:
		return hold("the published table could not be read: " + err.Error())
	case named:
		return hold("the published table still names it; the node that exports removes it at its next pass")
	case !published:
		return hold("the table's current version is not the published one yet")
	}
	return nil
}

// waiveClusterExport waives this node's export wait for a cluster job. held
// reports whether this node was waiting for it.
func (h *RehomeHandler) waiveClusterExport(id string) (ok, held bool) {
	if h.swap == nil || h.swap.Cluster == nil {
		return false, false
	}
	if _, live := h.swap.Cluster.RehomeJob(id); !live {
		return false, false
	}
	s := &h.clusterExport
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	_, held = s.lastErr[id]
	s.waived[id] = true
	return true, held
}
