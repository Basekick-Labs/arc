// Package iceberg publishes Arc's existing Parquet files as Apache Iceberg tables so
// external engines (Spark, Trino, Snowflake, DuckDB) can read Arc's data directly, without
// changing Arc's ingest write path. It is a periodic *reconciler*: it diffs Arc's durable
// file set (tiering metadata / cluster manifest) against the current Iceberg table state and
// commits the delta via iceberg-go's AddFiles (register-existing-Parquet, no rewrite) and
// Delete. Because it is driven by Arc's durable source of truth — not a transient event
// stream — a failed or missed commit self-heals on the next reconcile tick.
//
// Verified in Phase 0/0b: iceberg-go v0.6.0 + the mattn sqlite3 catalog registers Arc's
// field-ID-less Parquet (AddFiles auto-emits schema.name-mapping.default) and a non-DuckDB
// engine (PyIceberg) reads it back correctly. Arc's `time` column is TIMESTAMP_MICROS with
// isAdjustedToUTC=1, which MUST map to Iceberg timestamptz (not timestamp) or AddFiles fails.
package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// FileRef is one Arc data file as the reconciler sees it, decoupled from the durable source
// (tiering tier_files in OSS, Raft manifest in cluster). PhysicalPath is the fully-qualified
// location iceberg-go reads (file://… local, s3://bucket/prefix/… cold).
type FileRef struct {
	PhysicalPath string
	SizeBytes    int64
}

// Exporter maintains Iceberg tables that mirror Arc's Parquet file set. One Exporter per
// process; ReconcileMeasurement is the unit of work (one measurement's table).
type Exporter struct {
	catalog   *sqlcat.Catalog
	backend   storage.Backend // for writing version-hint.text alongside table metadata
	warehouse string          // warehouse root URI (file://… or s3://bucket/prefix)
	nsPrefix  string          // namespace for Iceberg tables (e.g. "arc")
	retain    int             // snapshots + metadata versions to keep per table (0 = keep all)
	logger    zerolog.Logger
}

// NewExporter builds an Exporter backed by a SQL (SQLite) Iceberg catalog on the given
// *sql.DB. Pass Arc's existing shared SQLite handle (mattn sqlite3) so the catalog rides the
// same DB as auth/tiering/retention — no second SQLite implementation, no extra service.
// warehouse is the object-store/local root under which table metadata is written
// (file://… or s3://bucket/prefix). backend is used to write version-hint.text next to the
// table metadata (local or S3), enabling directory-based readers to discover the current
// metadata without an exact filename.
func NewExporter(db *sql.DB, backend storage.Backend, warehouse, nsPrefix string, retain int, logger zerolog.Logger) (*Exporter, error) {
	if nsPrefix == "" {
		nsPrefix = "arc"
	}
	cat, err := sqlcat.NewCatalog("arc", db, sqlcat.SQLite, iceberg.Properties{
		"warehouse": warehouse,
	})
	if err != nil {
		return nil, fmt.Errorf("create iceberg sql catalog: %w", err)
	}
	return &Exporter{
		catalog:   cat,
		backend:   backend,
		warehouse: strings.TrimSuffix(warehouse, "/"),
		nsPrefix:  nsPrefix,
		retain:    retain,
		logger:    logger.With().Str("component", "iceberg-exporter").Logger(),
	}, nil
}

// tableIdent maps an Arc (database, measurement) to an Iceberg table identifier under the
// exporter namespace. Namespace = "<nsPrefix>_<database>" so multiple Arc databases coexist.
func (e *Exporter) tableIdent(database, measurement string) icetable.Identifier {
	return icetable.Identifier{e.nsPrefix + "_" + database, measurement}
}

// ArcSchema describes the typed columns of one measurement, as derived from a Parquet file's
// schema (see schema_from_parquet.go). The reconciler passes this to EnsureTable.
type ArcSchema struct {
	Fields []ArcField
}

// ArcField is one column: Name + the Arc/Arrow physical type mapped to an Iceberg type.
type ArcField struct {
	Name string
	Type iceberg.Type
}

// EnsureTable creates the Iceberg table for (database, measurement) if absent, using the
// given schema and a day(time) partition spec. Idempotent: returns the existing table if the
// namespace/table already exist. name-mapping is auto-set by iceberg-go on first AddFiles.
//
// Schema evolution (a measurement gaining a column) is handled by the reconciler re-deriving
// the schema; EnsureTable here is create-or-load. Evolving an existing table's schema is a
// follow-up (Iceberg supports UpdateSchema) — flagged in the plan, not in v1's create path.
func (e *Exporter) EnsureTable(ctx context.Context, database, measurement string, sc ArcSchema) (*icetable.Table, error) {
	ident := e.tableIdent(database, measurement)
	ns := icetable.Identifier{ident[0]}

	// Namespace create is idempotent-ish; ignore "already exists".
	if err := e.catalog.CreateNamespace(ctx, ns, nil); err != nil && !isAlreadyExists(err) {
		return nil, fmt.Errorf("create namespace %v: %w", ns, err)
	}

	if tbl, err := e.catalog.LoadTable(ctx, ident); err == nil {
		// Table exists — evolve its schema to cover any new columns in `sc` (Arc's
		// per-measurement schema can grow over time). Missing columns are added as optional,
		// so older narrow files stay compatible. No-op when already a superset.
		return e.evolveSchema(ctx, tbl, sc)
	}

	// Build the Iceberg schema with stable field IDs (1..N).
	fields := make([]iceberg.NestedField, len(sc.Fields))
	timeFieldID := -1
	for i, f := range sc.Fields {
		id := i + 1
		fields[i] = iceberg.NestedField{ID: id, Name: f.Name, Type: f.Type, Required: false}
		if f.Name == "time" {
			timeFieldID = id
		}
	}
	schema := iceberg.NewSchema(0, fields...)

	// day(time) partition spec — Arc's per-hour files fall within one day, and daily-compacted
	// files span exactly one day, so day() is the correct granularity for both tiers. hour()
	// would REJECT daily files (they span 24 hour-buckets). See eval doc.
	var createOpts []icecatalog.CreateTableOpt
	if timeFieldID > 0 {
		spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{timeFieldID},
			FieldID:   1000,
			Name:      "time_day",
			Transform: iceberg.DayTransform{},
		})
		createOpts = append(createOpts, icecatalog.WithPartitionSpec(&spec))
	}
	// Bound iceberg-go's own metadata-file history so NNNNN-*.metadata.json don't accumulate
	// forever: enable delete-after-commit and cap previous versions at `retain`. (Our own
	// v<N>.metadata.json copies are pruned separately in pruneOldVersionFiles.)
	if e.retain > 0 {
		createOpts = append(createOpts, icecatalog.WithProperties(iceberg.Properties{
			icetable.MetadataDeleteAfterCommitEnabledKey: "true",
			icetable.MetadataPreviousVersionsMaxKey:      strconv.Itoa(e.retain),
		}))
	}

	tbl, err := e.catalog.CreateTable(ctx, ident, schema, createOpts...)
	if err != nil {
		return nil, fmt.Errorf("create iceberg table %v: %w", ident, err)
	}
	e.writeVersionHint(ctx, tbl)
	e.logger.Info().Str("database", database).Str("measurement", measurement).Msg("Created Iceberg table")
	return tbl, nil
}

// evolveSchema adds any columns present in `sc` but missing from the table's current schema,
// as optional columns (so older files that lack them stay compatible). Returns the reloaded
// table. No-op (and no snapshot) when the table already covers every column.
func (e *Exporter) evolveSchema(ctx context.Context, tbl *icetable.Table, sc ArcSchema) (*icetable.Table, error) {
	current := tbl.Schema()
	var missing []ArcField
	for _, f := range sc.Fields {
		if _, ok := current.FindFieldByName(f.Name); !ok {
			missing = append(missing, f)
		}
	}
	if len(missing) == 0 {
		return tbl, nil
	}
	txn := tbl.NewTransaction()
	upd := txn.UpdateSchema(false /* caseSensitive */, false /* allowIncompatibleChanges */)
	for _, f := range missing {
		upd = upd.AddColumn([]string{f.Name}, f.Type, "", false /* required=false => optional */, nil)
	}
	if err := upd.Commit(); err != nil {
		return nil, fmt.Errorf("evolve schema (adding %d columns): %w", len(missing), err)
	}
	evolved, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit schema evolution: %w", err)
	}

	// CRITICAL: refresh schema.name-mapping.default to cover the new columns. iceberg-go
	// auto-sets the name-mapping on the first AddFiles, but UpdateSchema.AddColumn does NOT
	// extend it — so an evolution-added column has no field-id AND no mapping entry, and
	// external readers fail with "does not have a field-id, and no field-mapping exists"
	// (Arc's Parquet carries no field IDs). Derive the mapping from the table's ACTUAL
	// post-evolution schema (authoritative field IDs, which UpdateSchema assigns) and set it
	// in a follow-up transaction, matching iceberg-go's own post-AddFiles behavior.
	nmJSON, err := json.Marshal(evolved.Schema().NameMapping())
	if err != nil {
		return nil, fmt.Errorf("marshal name mapping: %w", err)
	}
	txn2 := evolved.NewTransaction()
	if err := txn2.SetProperties(iceberg.Properties{icetable.DefaultNameMappingKey: string(nmJSON)}); err != nil {
		return nil, fmt.Errorf("set name mapping after evolution: %w", err)
	}
	evolved, err = txn2.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit name mapping after evolution: %w", err)
	}
	e.writeVersionHint(ctx, evolved)
	e.logger.Info().Int("added_columns", len(missing)).Msg("Evolved Iceberg table schema")
	return evolved, nil
}

// ReconcileMeasurement makes the Iceberg table's data-file set equal `current`: it AddFiles
// the files present in Arc but absent from the table, and Deletes the files present in the
// table but absent from Arc. One transaction. Idempotent — if the sets already match, it is a
// no-op (no new snapshot). This is the core of the reconciler design.
func (e *Exporter) ReconcileMeasurement(ctx context.Context, database, measurement string, sc ArcSchema, current []FileRef) error {
	tbl, err := e.EnsureTable(ctx, database, measurement, sc)
	if err != nil {
		return err
	}

	want := make(map[string]struct{}, len(current))
	for _, f := range current {
		want[f.PhysicalPath] = struct{}{}
	}
	have, err := e.tableDataFiles(ctx, tbl)
	if err != nil {
		return fmt.Errorf("read current iceberg data files: %w", err)
	}

	var toAdd []string
	for p := range want {
		if _, ok := have[p]; !ok {
			toAdd = append(toAdd, p)
		}
	}
	var toRemove []string
	for p := range have {
		if _, ok := want[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	// Deterministic order (stable snapshots, easier debugging).
	sort.Strings(toAdd)
	sort.Strings(toRemove)

	if len(toAdd) == 0 && len(toRemove) == 0 {
		return nil // already converged — no snapshot
	}

	txn := tbl.NewTransaction()
	// ReplaceDataFiles is the metadata-only primitive that both drops files by path and adds
	// files by path in one snapshot — exactly the reconcile diff. (Transaction.Delete is a
	// ROW-level predicate that rewrites partially-matching files — wrong here; we drop whole
	// files that Arc already removed from storage.) When only adding, filesToDelete is empty.
	if err := txn.ReplaceDataFiles(ctx, toRemove, toAdd, nil); err != nil {
		return fmt.Errorf("iceberg ReplaceDataFiles (add=%d remove=%d): %w", len(toAdd), len(toRemove), err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("iceberg commit (add=%d remove=%d): %w", len(toAdd), len(toRemove), err)
	}
	// Expire old snapshots (+ orphaned manifests/data) so snapshot history and metadata don't
	// grow unbounded. Best-effort — a failure here doesn't undo the successful reconcile; the
	// next pass retries. Returns the possibly-newer table so version-hint points at it.
	committed = e.expireSnapshots(ctx, committed, database, measurement)
	e.writeVersionHint(ctx, committed)
	e.pruneOldVersionFiles(ctx, committed)
	e.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int("added", len(toAdd)).Int("removed", len(toRemove)).
		Msg("Reconciled Iceberg table")
	return nil
}

// expireSnapshots keeps the last `retain` snapshots for the table (0 = keep all → no-op),
// deleting older snapshots and their orphaned manifests/data files. Best-effort: on any
// failure it logs and returns the input table unchanged.
func (e *Exporter) expireSnapshots(ctx context.Context, tbl *icetable.Table, database, measurement string) *icetable.Table {
	if e.retain <= 0 {
		return tbl
	}
	// Only expire when there's more history than we intend to keep, to avoid pointless commits.
	if snaps := tbl.Metadata().Snapshots(); len(snaps) <= e.retain {
		return tbl
	}
	txn := tbl.NewTransaction()
	// WithRetainLast is a FLOOR, not a cap: iceberg-go only expires a snapshot when it is BOTH
	// older than maxSnapshotAgeMs AND beyond the retain-last count. With the default age
	// (~5 days) nothing expires regardless of count. Pass WithOlderThan(0) so the age gate is
	// always satisfied and retain-last becomes the effective cap ("keep the last N, expire the
	// rest"). Cluster-mode note: this is fine because the reconciler is writer-gated (single
	// writer), so no concurrent reader-vs-expire race beyond Iceberg's own snapshot isolation.
	if err := txn.ExpireSnapshots(icetable.WithRetainLast(e.retain), icetable.WithOlderThan(0)); err != nil {
		e.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).
			Msg("ExpireSnapshots failed (non-fatal)")
		return tbl
	}
	expired, err := txn.Commit(ctx)
	if err != nil {
		e.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).
			Msg("ExpireSnapshots commit failed (non-fatal)")
		return tbl
	}
	return expired
}

// pruneOldVersionFiles keeps only the newest `retain` v<M>.metadata.json copies and deletes
// the rest. iceberg-go prunes its own NNNNN-*.metadata.json (via delete-after-commit) but does
// not know about the v<N> copies we write for directory-based readers, so we prune them here.
// Scan-based (not arithmetic) so it's robust to non-contiguous version numbers — each reconcile
// commits twice (ReplaceDataFiles + ExpireSnapshots), so versions advance by more than one.
// Best-effort; never deletes the current version.
func (e *Exporter) pruneOldVersionFiles(ctx context.Context, tbl *icetable.Table) {
	if e.backend == nil || e.retain <= 0 {
		return
	}
	cur, dirKey, ok := e.parseVersionAndMetaDir(tbl.MetadataLocation())
	if !ok {
		return
	}
	keys, err := e.backend.List(ctx, dirKey+"/")
	if err != nil {
		return
	}
	// Collect version numbers of existing v<M>.metadata.json copies.
	type vfile struct {
		n   int
		key string
	}
	var vs []vfile
	for _, k := range keys {
		base := path.Base(k)
		if !strings.HasPrefix(base, "v") || !strings.HasSuffix(base, ".metadata.json") {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(base, "v"), ".metadata.json")
		if n, err := strconv.Atoi(numStr); err == nil {
			vs = append(vs, vfile{n: n, key: path.Join(dirKey, base)})
		}
	}
	if len(vs) <= e.retain {
		return
	}
	// Keep the newest `retain`; delete the rest. Never delete the current version.
	sort.Slice(vs, func(i, j int) bool { return vs[i].n > vs[j].n }) // descending
	curN, _ := strconv.Atoi(cur)
	for _, v := range vs[e.retain:] {
		if v.n == curN {
			continue
		}
		if err := e.backend.Delete(ctx, v.key); err != nil {
			e.logger.Debug().Err(err).Str("key", v.key).Msg("prune old v<N> (non-fatal)")
		}
	}
}

// writeVersionHint publishes the Hadoop-catalog discovery files next to the table's current
// metadata so DIRECTORY-based readers (Spark's hadoop-format load, DuckDB's dir-level
// iceberg_scan) can find the current metadata without being handed the exact filename:
//
//   - metadata/version-hint.text — the version integer (e.g. "4").
//   - metadata/v<N>.metadata.json — a copy of the current metadata under the Hadoop-convention
//     filename. REQUIRED in addition to the hint: iceberg-go/the SQL catalog write
//     "NNNNN-<uuid>.metadata.json", but Spark and DuckDB resolve the hint strictly to
//     "v<N>.metadata.json" (verified empirically — both fail "metadata file for version N
//     missing" with only the hint). Catalog-aware readers (PyIceberg, iceberg-go) are
//     unaffected; they use the catalog's pointer and ignore these files.
//
// Best-effort: failures are logged, not fatal — the SQL catalog remains the source of truth
// and the next reconcile rewrites these. Works for local and S3 warehouses via the backend.
func (e *Exporter) writeVersionHint(ctx context.Context, tbl *icetable.Table) {
	if e.backend == nil {
		return
	}
	metaLoc := tbl.MetadataLocation() // e.g. file:///…/metadata/00004-<uuid>.metadata.json
	version, dirKey, ok := e.parseVersionAndMetaDir(metaLoc)
	if !ok {
		e.logger.Debug().Str("metadata", metaLoc).Msg("Could not derive version-hint location; skipping")
		return
	}
	// Copy the current metadata to v<N>.metadata.json (Hadoop-convention name).
	metaKey, kOK := e.warehouseRelKey(metaLoc)
	if kOK {
		if body, err := e.backend.Read(ctx, metaKey); err != nil {
			e.logger.Warn().Err(err).Str("key", metaKey).Msg("Failed to read current metadata for v<N> copy (non-fatal)")
		} else {
			vKey := path.Join(dirKey, "v"+version+".metadata.json")
			if err := e.backend.Write(ctx, vKey, body); err != nil {
				e.logger.Warn().Err(err).Str("key", vKey).Msg("Failed to write v<N>.metadata.json (non-fatal)")
			}
		}
	}
	// Write the version-hint pointer — just the integer, NO trailing newline (DuckDB reads the
	// file verbatim and would look for "v<N>\n.metadata.json" otherwise; verified empirically).
	hintKey := path.Join(dirKey, "version-hint.text")
	if err := e.backend.Write(ctx, hintKey, []byte(version)); err != nil {
		e.logger.Warn().Err(err).Str("key", hintKey).Msg("Failed to write version-hint.text (non-fatal)")
	}
}

// warehouseRelKey converts a full metadata URI to a STORAGE-RELATIVE key (backend Read/Write
// operate on relative keys). Returns ok=false if the URI isn't under the warehouse root.
//
//	metaLoc="file:///wh/arc_db.db/cpu/metadata/00004-<uuid>.metadata.json", warehouse="file:///wh"
//	-> "arc_db.db/cpu/metadata/00004-<uuid>.metadata.json"
func (e *Exporter) warehouseRelKey(metaLoc string) (string, bool) {
	rel := strings.TrimPrefix(metaLoc, e.warehouse)
	if rel == metaLoc {
		return "", false
	}
	return strings.TrimPrefix(rel, "/"), true
}

// parseVersionAndMetaDir derives, from a full metadata-file URI, the version integer (e.g.
// "4" from 00004-<uuid>.metadata.json) and the STORAGE-RELATIVE key of the metadata/ directory
// it lives in. Returns ok=false if not under the warehouse or the filename doesn't match.
func (e *Exporter) parseVersionAndMetaDir(metaLoc string) (version, dirKey string, ok bool) {
	rel, ok := e.warehouseRelKey(metaLoc)
	if !ok {
		return "", "", false
	}
	base := path.Base(rel) // 00004-<uuid>.metadata.json
	if !strings.HasSuffix(base, ".metadata.json") {
		return "", "", false
	}
	prefix, _, found := strings.Cut(base, "-") // "00004"
	if !found {
		return "", "", false
	}
	n, err := strconv.Atoi(prefix)
	if err != nil {
		return "", "", false
	}
	return strconv.Itoa(n), path.Dir(rel), true
}

// tableDataFiles returns the set of LIVE physical data-file paths in the table's current
// snapshot. Uses Scan().PlanFiles, which resolves the current snapshot's live files honoring
// deletes across snapshots — NOT AllManifests, which returns manifests from superseded
// snapshots too and would report files a later ReplaceDataFiles has already dropped.
func (e *Exporter) tableDataFiles(ctx context.Context, tbl *icetable.Table) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	if tbl.CurrentSnapshot() == nil {
		return out, nil // empty table
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		out[task.File.FilePath()] = struct{}{}
	}
	return out, nil
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "exist")
}
