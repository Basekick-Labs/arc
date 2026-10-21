package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Continuous queries, cluster-wide (#1324 stage 3).
//
// The definitions live here like retention policies, with the same adoption
// (a CQ that never ran is never activated without an admin choosing it). On
// top of that each CQ carries its watermark: the end of the last window the
// primary aggregated. The primary sets it after every run with a
// compare-and-set (SetCQWatermark), so a new primary continues where the old
// one stopped instead of from its own stale or empty value.
//
// CQs have their own flag and capability, for the reason retention does: a
// build without them would refuse these commands and drop this state from its
// snapshots.

// CapabilityClusterCQ is advertised at join by every build that applies the
// continuous query commands.
const CapabilityClusterCQ = "cluster_policies_cq_v1"

// PolicyKindCQ is the continuous query kind.
const PolicyKindCQ = "cq"

// MaxSeedCQs bounds the continuous queries one seed installs (and one node's
// answer to a collect). A CQ carries its SQL, so fewer fit than retention
// policies.
const MaxSeedCQs = 1000

// Bounds the FSM applies to a CQ's fields; the API applies its full rules.
const (
	maxCQNameLen     = 256
	maxCQQueryLen    = 64 * 1024
	maxCQIntervalLen = 64
	maxCQTagColumns  = 256
)

// CQEntry is one continuous query. LastProcessed is its watermark, nil until
// the first run; it is never part of the definition.
type CQEntry struct {
	ID                     int64             `json:"id"`
	Name                   string            `json:"name"`
	Description            *string           `json:"description,omitempty"`
	Database               string            `json:"database"`
	SourceMeasurement      string            `json:"source_measurement"`
	DestinationMeasurement string            `json:"destination_measurement"`
	Query                  string            `json:"query"`
	Interval               string            `json:"interval"`
	TagColumns             []string          `json:"tag_columns,omitempty"`
	RetentionDays          *int              `json:"retention_days,omitempty"`
	DeleteSourceAfterDays  *int              `json:"delete_source_after_days,omitempty"`
	IsActive               bool              `json:"is_active"`
	LastProcessed          *time.Time        `json:"last_processed,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
	UpdatedAt              time.Time         `json:"updated_at"`
	Sources                []RetentionSource `json:"sources,omitempty"`
}

// CreateCQPayload creates a continuous query; the FSM numbers it.
type CreateCQPayload struct {
	Name                   string    `json:"name"`
	Description            *string   `json:"description,omitempty"`
	Database               string    `json:"database"`
	SourceMeasurement      string    `json:"source_measurement"`
	DestinationMeasurement string    `json:"destination_measurement"`
	Query                  string    `json:"query"`
	Interval               string    `json:"interval"`
	TagColumns             []string  `json:"tag_columns,omitempty"`
	RetentionDays          *int      `json:"retention_days,omitempty"`
	DeleteSourceAfterDays  *int      `json:"delete_source_after_days,omitempty"`
	IsActive               bool      `json:"is_active"`
	Now                    time.Time `json:"now"`
}

// UpdateCQPayload changes the fields it carries. A nullable field is written
// only with its Set flag (nil then clears it).
type UpdateCQPayload struct {
	ID                       int64     `json:"id"`
	Name                     *string   `json:"name,omitempty"`
	Database                 *string   `json:"database,omitempty"`
	SourceMeasurement        *string   `json:"source_measurement,omitempty"`
	DestinationMeasurement   *string   `json:"destination_measurement,omitempty"`
	Query                    *string   `json:"query,omitempty"`
	Interval                 *string   `json:"interval,omitempty"`
	IsActive                 *bool     `json:"is_active,omitempty"`
	SetDescription           bool      `json:"set_description,omitempty"`
	Description              *string   `json:"description,omitempty"`
	SetTagColumns            bool      `json:"set_tag_columns,omitempty"`
	TagColumns               []string  `json:"tag_columns,omitempty"`
	SetRetentionDays         bool      `json:"set_retention_days,omitempty"`
	RetentionDays            *int      `json:"retention_days,omitempty"`
	SetDeleteSourceAfterDays bool      `json:"set_delete_source_after_days,omitempty"`
	DeleteSourceAfterDays    *int      `json:"delete_source_after_days,omitempty"`
	Now                      time.Time `json:"now"`
}

// DeleteCQPayload removes a continuous query.
type DeleteCQPayload struct {
	ID int64 `json:"id"`
}

// EnableCQPoliciesPayload turns the CQ flag on; Nodes as in
// EnableClusterPoliciesPayload.
type EnableCQPoliciesPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// SetCQWatermarkPayload moves a CQ's watermark from From (nil: unset) to To,
// only if it still is From: a compare-and-set, so a manual reset and a
// scheduled run never undo each other. Proposer is the node that ran the CQ;
// it must be the primary writer (see applySetCQWatermark). Shared says the
// cluster runs in shared-storage mode, where the primary is the Raft leader
// and the FSM designates none.
type SetCQWatermarkPayload struct {
	Proposer string     `json:"proposer"`
	Shared   bool       `json:"shared,omitempty"`
	ID       int64      `json:"id"`
	From     *time.Time `json:"from,omitempty"`
	To       time.Time  `json:"to"`
}

// ErrInvalidCQ: a continuous query the cluster state refuses.
var ErrInvalidCQ = errors.New("invalid continuous query")

// ErrCQWatermarkMoved: the watermark is no longer the From a SetCQWatermark
// read.
var ErrCQWatermarkMoved = errors.New("the continuous query's watermark moved")

// ErrCQWatermarkNotPrimary: a SetCQWatermark from a node that is not the
// primary writer.
var ErrCQWatermarkNotPrimary = errors.New("only the primary writer sets a continuous query's watermark")

// IsCQWatermarkMoved reports ErrCQWatermarkMoved, typed or as text.
func IsCQWatermarkMoved(err error) bool {
	return err != nil && (errors.Is(err, ErrCQWatermarkMoved) || strings.Contains(err.Error(), ErrCQWatermarkMoved.Error()))
}

// CQWatermarkTime is a watermark as stored: UTC, whole seconds (the
// precision the window bounds are written into the query with).
func CQWatermarkTime(t time.Time) time.Time { return t.UTC().Truncate(time.Second) }

// CQPoliciesEnabled reports whether the CQ flag is applied.
func (f *ClusterFSM) CQPoliciesEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterPolicies && f.cqEnabled
}

func (f *ClusterFSM) applyEnableCQPolicies(payload []byte) interface{} {
	var p EnableCQPoliciesPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable continuous query policies payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.clusterPolicies {
		return errors.New("enable continuous query policies: cluster policies are not enabled")
	}
	if f.cqEnabled {
		return nil
	}
	listed := map[string]bool{}
	for _, n := range p.Nodes {
		listed[n] = true
	}
	var missing []string
	for id := range f.nodes {
		if !listed[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("enable continuous query policies: nodes %v are not known to support it", missing)
	}
	f.cqEnabled = true
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Cluster-wide continuous queries enabled")
	return nil
}

// CQ returns a copy of continuous query id.
func (f *ClusterFSM) CQ(id int64) (CQEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.cqs[id]
	if !ok {
		return CQEntry{}, false
	}
	return copyCQ(e), true
}

// CQByName returns a copy of the continuous query named name.
func (f *ClusterFSM) CQByName(name string) (CQEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, e := range f.cqs {
		if e.Name == name {
			return copyCQ(e), true
		}
	}
	return CQEntry{}, false
}

// CQs returns a copy of every continuous query, by ID.
func (f *ClusterFSM) CQs() []CQEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cqsLocked()
}

func (f *ClusterFSM) cqsLocked() []CQEntry {
	out := make([]CQEntry, 0, len(f.cqs))
	for _, e := range f.cqs {
		out = append(out, copyCQ(e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// CQMirror is what a node's CQ mirror reconciles to.
type CQMirror struct {
	CQs []CQEntry
}

// CQMirrorState returns the continuous queries a mirror reconciles to.
func (f *ClusterFSM) CQMirrorState() CQMirror {
	return CQMirror{CQs: f.CQs()}
}

func copyCQ(e *CQEntry) CQEntry {
	c := *e
	if e.Description != nil {
		v := *e.Description
		c.Description = &v
	}
	if e.RetentionDays != nil {
		v := *e.RetentionDays
		c.RetentionDays = &v
	}
	if e.DeleteSourceAfterDays != nil {
		v := *e.DeleteSourceAfterDays
		c.DeleteSourceAfterDays = &v
	}
	if e.LastProcessed != nil {
		v := *e.LastProcessed
		c.LastProcessed = &v
	}
	c.TagColumns = append([]string(nil), e.TagColumns...)
	c.Sources = append([]RetentionSource(nil), e.Sources...)
	return c
}

// validCQFields applies the rules that never change between releases; the
// API keeps its full validation (SQL, names, tag columns).
func validCQFields(e *CQEntry) error {
	check := func(name, v string, max int) error {
		if v == "" || len(v) > max {
			return fmt.Errorf("%w: %s of 1 to %d bytes is required", ErrInvalidCQ, name, max)
		}
		return nil
	}
	for _, c := range []struct {
		name, v string
		max     int
	}{
		{"a name", e.Name, maxCQNameLen},
		{"a database", e.Database, maxPolicyDatabaseLen},
		{"a source_measurement", e.SourceMeasurement, maxCQNameLen},
		{"a destination_measurement", e.DestinationMeasurement, maxCQNameLen},
		{"a query", e.Query, maxCQQueryLen},
		{"an interval", e.Interval, maxCQIntervalLen},
	} {
		if err := check(c.name, c.v, c.max); err != nil {
			return err
		}
	}
	if e.Description != nil && len(*e.Description) > maxCQQueryLen {
		return fmt.Errorf("%w: a description of at most %d bytes is required", ErrInvalidCQ, maxCQQueryLen)
	}
	if len(e.TagColumns) > maxCQTagColumns {
		return fmt.Errorf("%w: at most %d tag columns", ErrInvalidCQ, maxCQTagColumns)
	}
	return nil
}

func (f *ClusterFSM) cqNameTakenLocked(name string, except int64) bool {
	for id, e := range f.cqs {
		if id != except && e.Name == name {
			return true
		}
	}
	return false
}

func (f *ClusterFSM) applyCreateCQ(payload []byte) interface{} {
	var p CreateCQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create continuous query payload: %w", err)
	}
	e := &CQEntry{
		Name: p.Name, Description: p.Description, Database: p.Database, SourceMeasurement: p.SourceMeasurement,
		DestinationMeasurement: p.DestinationMeasurement, Query: p.Query, Interval: p.Interval,
		TagColumns: p.TagColumns, RetentionDays: p.RetentionDays, DeleteSourceAfterDays: p.DeleteSourceAfterDays,
		IsActive: p.IsActive, CreatedAt: p.Now, UpdatedAt: p.Now,
	}
	if err := validCQFields(e); err != nil {
		return fmt.Errorf("create continuous query: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindCQ); err != nil {
		return fmt.Errorf("create continuous query: %w", err)
	}
	if f.cqNameTakenLocked(p.Name, 0) {
		return fmt.Errorf("create continuous query %q: %w", p.Name, ErrPolicyNameTaken)
	}
	f.cqSeq++
	e.ID = f.cqSeq
	f.cqs[e.ID] = e
	return nil
}

func (f *ClusterFSM) applyUpdateCQ(payload []byte) interface{} {
	var p UpdateCQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update continuous query payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindCQ); err != nil {
		return fmt.Errorf("update continuous query: %w", err)
	}
	prev, ok := f.cqs[p.ID]
	if !ok {
		return fmt.Errorf("update continuous query %d: %w", p.ID, ErrPolicyNotFound)
	}
	// Merged against the CQ as it is when the update applies, like
	// retention's partial update.
	next := copyCQ(prev)
	for _, s := range []struct {
		v   *string
		dst *string
	}{
		{p.Name, &next.Name}, {p.Database, &next.Database}, {p.SourceMeasurement, &next.SourceMeasurement},
		{p.DestinationMeasurement, &next.DestinationMeasurement}, {p.Query, &next.Query}, {p.Interval, &next.Interval},
	} {
		if s.v != nil {
			*s.dst = *s.v
		}
	}
	if p.IsActive != nil {
		next.IsActive = *p.IsActive
	}
	if p.SetDescription {
		next.Description = p.Description
	}
	if p.SetTagColumns {
		next.TagColumns = p.TagColumns
	}
	if p.SetRetentionDays {
		next.RetentionDays = p.RetentionDays
	}
	if p.SetDeleteSourceAfterDays {
		next.DeleteSourceAfterDays = p.DeleteSourceAfterDays
	}
	if err := validCQFields(&next); err != nil {
		return fmt.Errorf("update continuous query %d: %w", p.ID, err)
	}
	if f.cqNameTakenLocked(next.Name, p.ID) {
		return fmt.Errorf("update continuous query %d to %q: %w", p.ID, next.Name, ErrPolicyNameTaken)
	}
	next.UpdatedAt = p.Now
	f.cqs[p.ID] = &next
	return nil
}

func (f *ClusterFSM) applyDeleteCQ(payload []byte) interface{} {
	var p DeleteCQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete continuous query payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindCQ); err != nil {
		return fmt.Errorf("delete continuous query: %w", err)
	}
	if _, ok := f.cqs[p.ID]; !ok {
		return fmt.Errorf("delete continuous query %d: %w", p.ID, ErrPolicyNotFound)
	}
	delete(f.cqs, p.ID)
	return nil
}

// applySetCQWatermark is the watermark's compare-and-set. Who may set it is
// decided here, the same way on every node, against the state the command
// applies to: in local-storage mode only the designated primary writer (so a
// demoted primary, even one that is the Raft leader, is refused once its
// demotion applied, and nobody is accepted between a demotion and the next
// promotion); in shared-storage mode, where the primary is the leader, a
// writer. Shared is stamped by the proposing leader from its own config, and
// the forward layer refuses a forwarded watermark that claims it (or any
// forwarded one in shared-storage mode).
func (f *ClusterFSM) applySetCQWatermark(payload []byte) interface{} {
	var p SetCQWatermarkPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal set continuous query watermark payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindCQ); err != nil {
		return fmt.Errorf("set continuous query watermark: %w", err)
	}
	if p.Shared {
		// Shared storage: the primary is the leader, and a designation the
		// state may still carry (a cluster converted from local storage)
		// means nothing. Only the leader's own proposal gets here.
		if n, ok := f.nodes[p.Proposer]; !ok || n.Role != "writer" {
			return fmt.Errorf("set continuous query watermark from %q (not a writer): %w", p.Proposer, ErrCQWatermarkNotPrimary)
		}
	} else if f.primaryWriterID == "" || p.Proposer != f.primaryWriterID {
		return fmt.Errorf("set continuous query watermark from %q (primary %q): %w", p.Proposer, f.primaryWriterID, ErrCQWatermarkNotPrimary)
	}
	e, ok := f.cqs[p.ID]
	if !ok {
		return fmt.Errorf("set continuous query watermark of %d: %w", p.ID, ErrPolicyNotFound)
	}
	switch {
	case p.From == nil && e.LastProcessed == nil:
	case p.From != nil && e.LastProcessed != nil && CQWatermarkTime(*p.From).Equal(*e.LastProcessed):
	default:
		return fmt.Errorf("set continuous query watermark of %d: %w", p.ID, ErrCQWatermarkMoved)
	}
	to := CQWatermarkTime(p.To)
	e.LastProcessed = &to
	return nil
}

// validCQSeed checks a CQ seed before it is applied, as validRetentionSeed.
func validCQSeed(p *SeedPoliciesPayload) error {
	if len(p.Tiering) > 0 || len(p.Retention) > 0 {
		return errors.New("tiering or retention rows in a continuous query seed")
	}
	if len(p.CQ) > MaxSeedCQs {
		return fmt.Errorf("%d continuous queries exceed %d", len(p.CQ), MaxSeedCQs)
	}
	if p.CQFloor < 1 {
		return errors.New("a continuous query seed needs a floor of at least 1")
	}
	names := make(map[string]bool, len(p.CQ))
	ids := make(map[int64]bool, len(p.CQ))
	for i := range p.CQ {
		e := &p.CQ[i]
		if err := validCQFields(e); err != nil {
			return fmt.Errorf("continuous query %q: %w", e.Name, err)
		}
		if names[e.Name] {
			return fmt.Errorf("continuous query %q listed twice", e.Name)
		}
		names[e.Name] = true
		if p.CQKeepIDs {
			if e.ID <= 0 || e.ID >= p.CQFloor || ids[e.ID] {
				return fmt.Errorf("continuous query %q: kept ID %d must be unique and in [1, %d)", e.Name, e.ID, p.CQFloor)
			}
			ids[e.ID] = true
		}
	}
	return nil
}

// seedCQLocked installs a validated CQ seed.
func (f *ClusterFSM) seedCQLocked(p *SeedPoliciesPayload) {
	f.cqs = make(map[int64]*CQEntry, len(p.CQ))
	if f.cqSeq < p.CQFloor-1 {
		f.cqSeq = p.CQFloor - 1
	}
	for i := range p.CQ {
		c := copyCQ(&p.CQ[i])
		if !p.CQKeepIDs {
			f.cqSeq++
			c.ID = f.cqSeq
		}
		if c.LastProcessed != nil {
			t := CQWatermarkTime(*c.LastProcessed)
			c.LastProcessed = &t
		}
		if c.CreatedAt.IsZero() {
			c.CreatedAt = p.Now
		}
		if c.UpdatedAt.IsZero() {
			c.UpdatedAt = p.Now
		}
		f.cqs[c.ID] = &c
	}
}

func (f *ClusterFSM) snapshotCQLocked(st *clusterPoliciesState) {
	if !f.cqEnabled {
		return
	}
	st.CQEnabled = true
	st.CQSeq = f.cqSeq
	for _, e := range f.cqsLocked() {
		c := e
		st.CQs = append(st.CQs, &c)
	}
}

func (f *ClusterFSM) restoreCQLocked(st *clusterPoliciesState) {
	f.cqEnabled = false
	f.cqs = map[int64]*CQEntry{}
	f.cqSeq = 0
	if st == nil {
		return
	}
	f.cqEnabled = st.CQEnabled
	f.cqSeq = st.CQSeq
	for _, e := range st.CQs {
		if e != nil && e.ID > 0 {
			c := copyCQ(e)
			f.cqs[c.ID] = &c
		}
	}
}
