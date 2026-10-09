// Package iceberg publishes Arc's existing Parquet files as Apache Iceberg tables so
// external engines (Spark, Trino, Snowflake, DuckDB) can read Arc's data directly, without
// changing Arc's ingest write path. It is a periodic *reconciler*: it diffs Arc's durable
// file set (the storage backend walk) against the current Iceberg table state and commits the
// delta via iceberg-go's ReplaceDataFiles and AddFiles (register/deregister existing Parquet
// by path — no data rewrite). Because it is driven by Arc's durable storage — not a transient event stream —
// a failed or missed commit self-heals on the next reconcile tick.
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
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// FileRef is one Arc data file as the reconciler sees it, decoupled from the durable source
// (tiering tier_files in OSS, Raft manifest in cluster). PhysicalPath is the fully-qualified
// location iceberg-go reads (file://… local, s3://bucket/prefix/… cold). SizeBytes is the
// file's current size on storage; together with the path it is the reconcile key, because a
// delete rewrite now publishes a new immutable path, while a backup restore can still replace
// the bytes represented by a live path.
type FileRef struct {
	PhysicalPath string
	SizeBytes    int64
}

// Exporter maintains Iceberg tables that mirror Arc's Parquet file set. One Exporter per
// process; ReconcileMeasurement is the unit of work (one measurement's table).
type Exporter struct {
	db          *sql.DB
	catalog     *sqlcat.Catalog
	catalogName string
	backend     storage.Backend // for writing version-hint.text alongside table metadata
	warehouse   string          // warehouse root URI (file://… or s3://bucket/prefix)
	nsPrefix    string          // namespace for Iceberg tables (e.g. "arc")
	retain      int             // snapshots + metadata versions to keep per table (0 = keep all)
	logger      zerolog.Logger
	// namespaceMigrationDryRun is set once during wiring, before the scheduler starts.
	namespaceMigrationDryRun bool

	// hintWarned records metadata directories already warned about for
	// unaddressable version-hint publishing, so a permanent configuration
	// property is reported once per table rather than every reconcile pass.
	hintWarnMu sync.Mutex
	hintWarned map[string]struct{}

	// resolveOnce caches the symlink-resolved warehouse and storage-root
	// URIs for warehouseRelKey (#639 item 5); resolved lazily because the
	// directories may not exist at construction.
	resolveOnce       sync.Once
	resolvedWarehouse string
	resolvedRoot      string

	// hintFailure records that a discovery-file write failed anywhere during the
	// current ReconcileMeasurementWithHint call, including inside EnsureTable
	// (which publishes on table creation and schema evolution). The reconciler
	// is single-writer and reconciles one measurement at a time, so a plain flag
	// reset at the start of each call is sufficient; the mutex only guards
	// against a future concurrent caller.
	hintFailMu  sync.Mutex
	hintFailure bool

	// collapseThreshold is the manifest count at which a pass also collapses the manifest set
	// (see manifestCollapseThreshold). A field rather than a bare constant so a test can raise it
	// out of reach or lower it to 1 without a mutable package variable; nothing outside this
	// package sets it.
	collapseThreshold int

	// orphanSweepEnabled gates sweepOrphanMetadata (iceberg.orphan_sweep_enabled).
	// orphanGrace is the minimum age an unreachable .avro must reach before the
	// sweep deletes it. Both are set once at wiring time (ConfigureOrphanSweep)
	// and read-only afterwards, so they need no lock.
	orphanSweepEnabled bool
	orphanGrace        time.Duration
}

// resetHintFailure clears the per-reconcile discovery-file failure flag.
func (e *Exporter) resetHintFailure() {
	e.hintFailMu.Lock()
	e.hintFailure = false
	e.hintFailMu.Unlock()
}

// noteHintFailure records that a discovery-file write failed.
func (e *Exporter) noteHintFailure() {
	e.hintFailMu.Lock()
	e.hintFailure = true
	e.hintFailMu.Unlock()
}

// hintFailed reports whether any discovery-file write failed since the last reset.
func (e *Exporter) hintFailed() bool {
	e.hintFailMu.Lock()
	defer e.hintFailMu.Unlock()
	return e.hintFailure
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
		db:          db,
		catalog:     cat,
		catalogName: "arc",
		backend:     backend,
		warehouse:   strings.TrimSuffix(warehouse, "/"),
		// resolvedWarehouse/resolvedRoot are computed lazily on first
		// warehouseRelKey call (the directories may not exist yet here).
		nsPrefix: nsPrefix,
		retain:   retain,
		logger:   logger.With().Str("component", "iceberg-exporter").Logger(),
		// Safe defaults for an Exporter used without ConfigureOrphanSweep (tests,
		// future callers): the sweep is on, with the grace floor. Never leave the
		// grace at zero by default — an ungraced sweep would be free to delete a
		// manifest written moments ago by a commit that has not landed yet.
		orphanSweepEnabled:       true,
		orphanGrace:              minOrphanGrace,
		collapseThreshold:        manifestCollapseThreshold,
		namespaceMigrationDryRun: true,
	}, nil
}

// minOrphanGrace is the floor for how old an unreachable manifest must be before
// sweepOrphanMetadata deletes it. What it covers is a commit that wrote its
// manifests and then died before its metadata.json landed: those files are
// seconds old, so an hour is generous. OrphanGraceFor widens it for a deployment
// that stretches the reconcile interval past half of this.
//
// What it does NOT cover, because the age is the FILE's and not the age of its
// unreachability: a directory reader that resolved a v<N>.metadata.json moments
// before the pass that retired that version. Such a manifest is typically hours
// old by then, so it is past the grace the instant it becomes unreachable, and
// this pass deletes it. That race is bounded by the same cushion as before this
// change — pruneOldVersionFiles keeps retain+1 v<N> copies so the version a
// reader just resolved is not the one being retired (see the keep comment there)
// — not by this grace. Making the grace cover it would mean remembering when
// each file was first seen unreachable, which is per-table state the reconciler
// deliberately does not keep: it would be lost on restart, and a scheme that
// needs two consecutive sweeps cannot fire at all on a table the scheduler
// fingerprint-gates away (the design this replaced).
const minOrphanGrace = time.Hour

// OrphanGraceFor returns the orphan-sweep grace for a reconcile interval.
func OrphanGraceFor(reconcileInterval time.Duration) time.Duration {
	if g := 2 * reconcileInterval; g > minOrphanGrace {
		return g
	}
	return minOrphanGrace
}

// ConfigureOrphanSweep sets whether the metadata orphan sweep runs and how old an
// unreachable manifest must be before it is deleted. A non-positive grace is
// refused in favour of the floor rather than honoured: a caller that forgets to
// compute one must not end up with an ungraced deleter.
//
// Call it during wiring, before the reconcile scheduler starts. The fields it
// writes are read without a lock by the reconcile goroutine, which is safe only
// because that goroutine does not exist yet.
func (e *Exporter) ConfigureOrphanSweep(enabled bool, grace time.Duration) {
	e.orphanSweepEnabled = enabled
	if grace < minOrphanGrace {
		grace = minOrphanGrace
	}
	e.orphanGrace = grace
}

// tableIdent maps an Arc (database, measurement) to an Iceberg table identifier under the
// exporter namespace. A local database uses one namespace component; an edge-sync spoke
// pseudo-database "{spoke}/{db}" uses two (#1129), so its boundary is never folded into a dot.
func (e *Exporter) tableIdent(database, measurement string) icetable.Identifier {
	namespace, err := namespaceIdentifier(e.nsPrefix, database)
	if err != nil {
		return nil // callers validate dynamic database names before reaching the catalog
	}
	return append(namespace, measurement)
}

// namespaceIdentifier preserves the spoke/database boundary. iceberg-go v0.7.0 supports dotted
// components through its encoded namespace key; source.go recognizes the encoded warehouse dir.
func namespaceIdentifier(nsPrefix, database string) (icetable.Identifier, error) {
	if strings.Contains(nsPrefix, ".") {
		return nil, fmt.Errorf("iceberg namespace prefix %q contains a dot", nsPrefix)
	}
	if strings.Contains(database, "/") {
		spoke, db, ok := strings.Cut(database, "/")
		if !ok || spoke == "" || db == "" || strings.Contains(db, "/") || strings.Contains(db, ".") {
			return nil, fmt.Errorf("invalid spoke database %q: expected {spoke}/{dot-free database}", database)
		}
		return icetable.Identifier{nsPrefix + "_" + spoke, db}, nil
	}
	if database == "" {
		return nil, fmt.Errorf("database name is empty")
	}
	return icetable.Identifier{nsPrefix + "_" + database}, nil
}

func checkNamespaceAddressable(nsPrefix, database string) error {
	_, err := namespaceIdentifier(nsPrefix, database)
	return err
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

// TableExists reports whether the Iceberg table for (database, measurement) exists, swallowing
// catalog lookup errors and returning false.
func (e *Exporter) TableExists(ctx context.Context, database, measurement string) bool {
	exists, _ := e.tableExists(ctx, database, measurement)
	return exists
}

// tableExists distinguishes a missing table from a catalog failure. Callers such as the reconciler
// need the (bool, error) form so a transient catalog error is not interpreted and cached as a
// confirmed missing table.
func (e *Exporter) tableExists(ctx context.Context, database, measurement string) (bool, error) {
	if err := checkNamespaceAddressable(e.nsPrefix, database); err != nil {
		return false, err
	}
	_, err := e.catalog.LoadTable(ctx, e.tableIdent(database, measurement))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		return false, nil
	}
	return false, err
}

// EnsureTable creates the Iceberg table for (database, measurement) if absent, using the
// given schema and a day(time) partition spec. Idempotent: returns the existing table if the
// namespace/table already exist. name-mapping is auto-set by iceberg-go on first AddFiles.
//
// Schema evolution (a measurement gaining a column) is handled by the reconciler re-deriving
// the schema; EnsureTable here is create-or-load. Evolving an existing table's schema is a
// follow-up (Iceberg supports UpdateSchema) — flagged in the plan, not in v1's create path.
//
// A catalog row that exists but cannot be loaded (its metadata file is missing or unreadable)
// is an error, never a create: see the load gate below (#637).
func (e *Exporter) EnsureTable(ctx context.Context, database, measurement string, sc ArcSchema) (*icetable.Table, error) {
	if err := checkNamespaceAddressable(e.nsPrefix, database); err != nil {
		return nil, err
	}
	ident := e.tableIdent(database, measurement)
	ns := ident[:len(ident)-1]

	// Namespace create is idempotent-ish; ignore "already exists".
	if err := e.catalog.CreateNamespace(ctx, ns, nil); err != nil && !isAlreadyExists(err) {
		return nil, fmt.Errorf("create namespace %v: %w", ns, err)
	}

	tbl, err := e.catalog.LoadTable(ctx, ident)
	if err == nil {
		// Table exists — evolve its schema to cover any new columns in `sc` (Arc's
		// per-measurement schema can grow over time). Missing columns are added as optional,
		// so older narrow files stay compatible. No-op when already a superset.
		tbl, err = e.evolveSchema(ctx, tbl, sc)
		if err != nil {
			return nil, err
		}
		return e.healRetentionProperties(ctx, tbl), nil
	}
	if !errors.Is(err, icecatalog.ErrNoSuchTable) {
		// The catalog has a row for this table but the table cannot be loaded. Falling
		// through to CreateTable used to write a fresh metadata file into the warehouse and
		// then fail on the catalog's primary key — on every pass, forever, leaving another
		// orphan file each time (#637: a backup restored without its outside-root warehouse).
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("iceberg table %v is in the catalog but its metadata file is missing: %w; "+
				"if this node was restored from a backup, restore the Iceberg warehouse files too (iceberg.warehouse), "+
				"otherwise delete the table's row from the iceberg_tables SQLite table and the reconciler recreates it", ident, err)
		}
		return nil, fmt.Errorf("iceberg table %v is in the catalog but cannot be loaded: %w", ident, err)
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

	tbl, err = e.catalog.CreateTable(ctx, ident, schema, createOpts...)
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
		existing, ok := current.FindFieldByName(f.Name)
		if !ok {
			missing = append(missing, f)
			continue
		}
		// A column already in the table must keep its type. Neither of the schema-derivation
		// paths can catch this: UnionSchema only compares the CURRENT pass's files against each
		// other, and MergeSchemas compares against the in-memory cache, which is empty after a
		// restart or an empty-out. So a measurement whose column changes type across those
		// boundaries (e.g. `value` was long, all old files age out, Arc restarts, new files
		// arrive with value as double) would derive a self-consistent schema, find the column
		// "present", skip it, and register type-incompatible files into the table. Fail the
		// measurement instead — the table stays readable and the next pass retries.
		if !existing.Type.Equals(f.Type) {
			return nil, fmt.Errorf("column %q type mismatch: Iceberg table has %s, new Parquet files have %s "+
				"(the measurement's column type changed; Iceberg cannot represent both in one column)",
				f.Name, existing.Type, f.Type)
		}
	}
	if len(missing) == 0 {
		// Self-heal: a prior evolution may have committed the schema but then failed to commit the
		// follow-up name-mapping update (txn2 below). In that state the schema already covers every
		// column, so we return here without ever re-setting the mapping — leaving an evolution-added
		// column with no field-mapping, which breaks external readers permanently. Reconcile the
		// stored name-mapping against the table's actual schema whenever they diverge.
		return e.healNameMapping(ctx, tbl)
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
	//
	// Routed through the same helper as the self-heal path so the "never write a null mapping"
	// guard lives in exactly one place — these two sites did the same marshal+SetProperties and
	// drifted apart once already.
	evolved, err = e.setNameMappingFromSchema(ctx, evolved)
	if err != nil {
		return nil, err
	}
	e.writeVersionHint(ctx, evolved)
	e.logger.Info().Int("added_columns", len(missing)).Msg("Evolved Iceberg table schema")
	return evolved, nil
}

// setNameMappingFromSchema writes schema.name-mapping.default derived from the table's current
// schema, and is the ONLY place that property is set. Returns the table unchanged (no commit)
// when the mapping already matches or would be invalid.
//
// The "null" guard is the reason this is shared: NameMapping is a []MappedField, so a nil
// mapping marshals to the literal "null". Writing schema.name-mapping.default="null" is not a
// valid mapping and would break the external readers the property exists to serve — strictly
// worse than leaving it unset.
func (e *Exporter) setNameMappingFromSchema(ctx context.Context, tbl *icetable.Table) (*icetable.Table, error) {
	nmJSON, err := json.Marshal(tbl.Schema().NameMapping())
	if err != nil {
		return nil, fmt.Errorf("marshal name mapping: %w", err)
	}
	if string(nmJSON) == "null" {
		e.logger.Warn().Msg("Iceberg name-mapping: schema produced an empty mapping, leaving property unset")
		return tbl, nil
	}
	if tbl.Properties()[icetable.DefaultNameMappingKey] == string(nmJSON) {
		return tbl, nil // already consistent — no commit
	}
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{icetable.DefaultNameMappingKey: string(nmJSON)}); err != nil {
		return nil, fmt.Errorf("set name mapping: %w", err)
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit name mapping: %w", err)
	}
	return updated, nil
}

// healNameMapping ensures the stored schema.name-mapping.default matches the table's current
// schema. It only commits when they actually differ, so it is a cheap no-op on the common path.
// This closes the gap where a schema evolution committed but its follow-up mapping update did
// not (see evolveSchema): once the columns are in the schema, evolveSchema's len(missing)==0
// early-return would otherwise never repair the mapping, permanently breaking external readers
// for the evolution-added column. Best-effort: on any failure it logs and returns tbl unchanged.
func (e *Exporter) healNameMapping(ctx context.Context, tbl *icetable.Table) (*icetable.Table, error) {
	before := tbl.Properties()[icetable.DefaultNameMappingKey]
	healed, err := e.setNameMappingFromSchema(ctx, tbl)
	if err != nil {
		// Best-effort: this runs on the steady-state path, so a transient catalog failure must
		// not fail the reconcile. The next pass retries.
		e.logger.Warn().Err(err).Msg("Iceberg name-mapping heal failed (non-fatal) — will retry next pass")
		return tbl, nil
	}
	if healed.Properties()[icetable.DefaultNameMappingKey] != before {
		e.writeVersionHint(ctx, healed)
		e.logger.Info().Msg("Iceberg name-mapping healed to match current schema")
	}
	return healed, nil
}

// healRetentionProperties keeps an existing table's metadata-file retention in sync with
// the current configuration. It only commits when a property differs, so unchanged settings
// do not create a metadata version on every reconcile pass.
//
// Best-effort: a failure here must not fail the reconcile, since the data-file set is what
// the pass exists to converge. The retry is NOT the next tick, though — the scheduler skips a
// measurement whose fingerprint is unchanged before it ever reaches EnsureTable, so a failed
// property commit waits for the next pass that actually reconciles THIS measurement (a file-set
// change, or the restart that any config change implies, which empties the fingerprint cache).
func (e *Exporter) healRetentionProperties(ctx context.Context, tbl *icetable.Table) *icetable.Table {
	if e.retain < 1 {
		return tbl
	}

	desired := iceberg.Properties{
		icetable.MetadataDeleteAfterCommitEnabledKey: "true",
		icetable.MetadataPreviousVersionsMaxKey:      strconv.Itoa(e.retain),
	}
	updates := make(iceberg.Properties, len(desired))
	for key, value := range desired {
		if tbl.Properties()[key] != value {
			updates[key] = value
		}
	}
	if len(updates) == 0 {
		return tbl
	}

	txn := tbl.NewTransaction()
	if err := txn.SetProperties(updates); err != nil {
		e.logger.Warn().Err(err).Msg("Iceberg retention property heal failed (non-fatal) — retried on the next pass that reconciles this measurement")
		return tbl
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		e.logger.Warn().Err(err).Msg("Iceberg retention property heal failed (non-fatal) — retried on the next pass that reconciles this measurement")
		return tbl
	}
	e.writeVersionHint(ctx, updated)
	e.logger.Info().Int("retain_snapshots", e.retain).Msg("Iceberg retention properties reconciled")
	return updated
}

// ReconcileMeasurement makes the Iceberg table's data-file set equal `current`: it AddFiles
// the files present in Arc but absent from the table, and Deletes the files present in the
// table but absent from Arc. One transaction. Idempotent — if the sets already match, it is a
// no-op (no new snapshot). This is the core of the reconciler design.
func (e *Exporter) ReconcileMeasurement(ctx context.Context, database, measurement string, sc ArcSchema, current []FileRef) error {
	_, err := e.ReconcileMeasurementWithHint(ctx, database, measurement, sc, current)
	return err
}

// ReconcileMeasurementWithHint is ReconcileMeasurement plus whether the pass fully
// SETTLED: the reader discovery files (version-hint.text, v<N>.metadata.json) were
// published, AND any snapshot-history floor this pass owed actually committed.
//
// The scheduler needs this separately from the error. Both steps are best-effort with
// respect to the committed snapshot, but if either fails the caller must NOT cache the
// measurement's fingerprint, or a file set that then goes quiet is never revisited: the
// hint would stay stale forever, and metadata that names deleted data files would stay
// published (#1092).
//
// The return is true when there was nothing to do for either step, which is the ordinary
// converged case.
func (e *Exporter) ReconcileMeasurementWithHint(ctx context.Context, database, measurement string, sc ArcSchema, current []FileRef) (settled bool, err error) {
	// EnsureTable publishes the discovery files itself when it creates or evolves
	// the table, so start from whether those writes succeeded. Otherwise a
	// creation-time failure would be invisible here and the fingerprint would be
	// cached over an unpublished hint.
	e.resetHintFailure()
	tbl, err := e.EnsureTable(ctx, database, measurement, sc)
	if err != nil {
		return false, err
	}
	hintOK := !e.hintFailed()

	want := make(map[string]int64, len(current))
	for _, f := range current {
		want[f.PhysicalPath] = f.SizeBytes
	}
	have, err := e.tableDataFiles(ctx, tbl)
	if err != nil {
		return false, fmt.Errorf("read current iceberg data files: %w", err)
	}

	// The diff key is (path, size). A path on both sides with a different size is a file
	// whose bytes changed without changing its path — for example, a backup restore replacing
	// an existing object. The manifest entry still describes the old content (record_count,
	// file_size_in_bytes, column bounds), so external engines keep counting deleted rows and
	// can mis-plan reads against the shorter file. Such a path is dropped and re-registered
	// in the same commit.
	var toAdd, toRemove, rewritten []string
	for p, size := range want {
		haveSize, ok := have[p]
		switch {
		case !ok:
			toAdd = append(toAdd, p)
		case haveSize != size:
			rewritten = append(rewritten, p)
			toRemove = append(toRemove, p)
			toAdd = append(toAdd, p)
		}
	}
	for p := range have {
		if _, ok := want[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	// Deterministic order (stable snapshots, easier debugging).
	sort.Strings(toAdd)
	sort.Strings(toRemove)

	if len(toAdd) == 0 && len(toRemove) == 0 {
		// Already converged — no new snapshot. Still republish the discovery
		// files: a previous pass may have committed the snapshot but failed to
		// write them, and this is the path that pass's retry lands on.
		//
		// Sweep here too. A measurement whose file set has stopped changing is
		// skipped by the scheduler's fingerprint gate before it ever reaches this
		// function, so for such a table THIS is the pass that reclaims: the first
		// one after a restart, whose fingerprint cache is cold.
		e.sweepOrphanMetadata(ctx, tbl)
		return e.writeVersionHint(ctx, tbl) && hintOK, nil
	}

	// Metadata-only: files are dropped and added by path in one commit. (Transaction.Delete
	// is a ROW-level predicate that rewrites partially-matching files — wrong here; we drop
	// whole files that Arc already removed from storage.) A rewritten path appears in both
	// lists and is removed before it is added, inside the same transaction.
	//
	// The manifest collapse (#1106) is decided HERE, not inside, for two reasons: it reads the
	// CURRENT snapshot and the pass cannot be re-driven once ReplaceDataFiles has staged, and
	// len(want) — the live file set this pass is driving the table to — is only in scope here.
	collapse := e.shouldCollapse(ctx, tbl, len(want), database, measurement)
	committed, skipped, err := e.replaceDataFilesResilient(ctx, tbl, toRemove, toAdd, database, measurement, collapse, len(want))
	if err != nil {
		return false, err
	}
	if len(skipped) > 0 {
		// Some files could not be partitioned (day-straddling data — see day() spec). They are
		// omitted from the table but logged loudly; the rest of the measurement still exports.
		e.logger.Error().
			Str("database", database).Str("measurement", measurement).
			Int("skipped", len(skipped)).Strs("files", skipped).
			Msg("Iceberg: skipped files that could not be partition-mapped (day-straddling time range)")
	}
	// Expire old snapshots so snapshot history and metadata don't grow unbounded, and floor
	// history to the current snapshot when this pass removed any path. A removed path is one
	// Arc already deleted from the primary backend, so every older snapshot names a file that
	// is gone and time travel to it fails on the read (#1092). Flooring does not lose
	// readable history: it drops snapshots that were already unreadable.
	//
	// toRemove, not toRemove-minus-rewritten, is the trigger. A rewritten path is in both
	// lists and still present in storage, but with different bytes, so the older snapshots'
	// record_count and bounds describe content that no longer exists there — the original
	// #1092 symptom.
	//
	// Best-effort for the retention case: a failure doesn't undo the successful reconcile, and
	// the next pass retries. NOT best-effort for the floor — see the expireOK handling below,
	// because only a pass that removes something asks to floor, so there may be no next pass.
	committed, expireOK := e.expireSnapshots(ctx, committed, database, measurement, expireModeFor(len(toRemove)))
	hintOK = e.writeVersionHint(ctx, committed) && !e.hintFailed()
	e.pruneOldVersionFiles(ctx, committed)
	// After pruneOldVersionFiles, so the on-disk metadata set the sweep reads for
	// reachability is the one this pass settled on, not one version stale.
	e.sweepOrphanMetadata(ctx, committed)
	e.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int("added", len(toAdd)-len(rewritten)).Int("removed", len(toRemove)-len(rewritten)).
		Int("reregistered", len(rewritten)).
		Bool("hint_published", hintOK).
		Bool("history_settled", expireOK).
		Msg("Reconciled Iceberg table")
	// A failed floor must not be cached as settled. The scheduler keys its skip gate on the
	// value returned here, and only a pass that REMOVES a path asks to floor — so unlike the
	// ordinary retention case there is no guarantee of a next pass to retry on: once the file
	// set goes quiet the measurement is skipped entirely, leaving metadata that names deleted
	// files. Reporting not-settled keeps the fingerprint uncached so the next tick re-drives
	// this measurement, exactly as hintOK already does for a failed version hint.
	return hintOK && expireOK, nil
}

// expireModeFor maps a pass's removal count onto the expiry mode. Separate so the tests can
// pin the mapping without driving a whole reconcile.
func expireModeFor(removed int) expireMode {
	if removed > 0 {
		return floorHistory
	}
	return retainConfigured
}

// replaceDataFilesResilient applies the reconcile diff in ONE commit. The ordinary pass is a
// single ReplaceDataFiles (one overwrite snapshot for removes and adds together, or a plain
// append when nothing is removed), exactly as before #633. It tolerates two iceberg-go
// refusals:
//
//   - Partition inference. iceberg-go infers each file's day() partition value from its
//     Parquet time min/max and ERRORS ("more than one value for partition field") when one
//     file's data straddles a UTC-day boundary (rare: backfill or a flush crossing midnight).
//     Left unhandled, one such file wedges the whole measurement's export on every pass. The
//     fallback adds files one at a time and skips (and returns) the ones that can't be mapped.
//
//   - Stale DELETED history ("cannot add files that are already referenced by table").
//     A removed file's DELETED manifest entry is checked against new adds, so a path that left
//     the table and comes back is refused: an in-place rewrite (#633), a backup restore of a key
//     that had been removed, or history from before this release. On iceberg-go v0.6.0 the
//     tombstone was carried into EVERY later snapshot, so such a path could never come back at
//     all; since v0.7.0 the overwrite producer drops a manifest with no surviving entries, so a
//     tombstone is shed on the next overwrite and the refusal is rare rather than permanent. The
//     refusal wording is unchanged, so this branch still fires when it applies. For those adds the transaction
//     enables Iceberg manifest merging (commit.manifest-merge.enabled) and retries with the
//     duplicate check off: the merge-append rewrites the manifests keeping DELETED entries
//     only of its own snapshot, so the stale entries are gone once the commit lands and a later
//     removal of the path sees exactly one entry again. Such a pass lands as two snapshots in
//     the one commit (the removes as an overwrite, then the merged append), so readers of the
//     current snapshot never see the path absent; only time travel to the intermediate
//     snapshot does, until it expires. The property is switched back off in the same
//     transaction — see manifestMergeOn for why it must not stay on.
//
// When collapse is set the transaction also gets a merge-enabled append that rewrites the whole
// manifest set into one (#1106), staged after the diff and before Commit, whichever of the paths
// above staged it. The caller decides — see shouldCollapse for the conditions. It is cleared if
// the "already referenced" branch ran, because that branch's merged add has already done it.
//
// Returns the committed table and the paths skipped for partition reasons.
func (e *Exporter) replaceDataFilesResilient(ctx context.Context, tbl *icetable.Table, toRemove, toAdd []string, database, measurement string, collapse bool, liveAfter int) (*icetable.Table, []string, error) {
	txn := tbl.NewTransaction()
	// ReplaceDataFiles validates both lists against the current snapshot before it stages
	// anything, so on the "already referenced" refusal the transaction is still clean and can
	// be re-driven as remove-then-merged-add.
	err := txn.ReplaceDataFiles(ctx, toRemove, toAdd, nil)
	if isAlreadyReferencedError(err) {
		err = nil
		if len(toRemove) > 0 {
			err = txn.ReplaceDataFiles(ctx, toRemove, nil, nil)
		}
		if err == nil {
			err = e.addFilesMerging(ctx, txn, toAdd, database, measurement)
			if err == nil {
				// That WAS the collapse: addFilesMerging runs the same merge producer with the
				// same properties, so the manifest set is already one manifest. A collapse on top
				// would stage a third snapshot whose merge is a no-op and still write a manifest
				// list and an .avro for it.
				collapse = false
			}
		}
	}
	if err != nil {
		if !isPartitionInferenceError(err) {
			return nil, nil, fmt.Errorf("iceberg ReplaceDataFiles (add=%d remove=%d): %w", len(toAdd), len(toRemove), err)
		}
		// The staged transaction is dropped: iceberg-go already wrote its manifests to the
		// warehouse, and no commit will ever reference them. sweepOrphanMetadata reclaims them
		// once they pass its grace window (#835) — it names this case as a reason it exists. The
		// collapse adds nothing here: it is staged only after staging succeeded, so a transaction
		// dropped on this branch contains no collapse work. The property toggle lives only in the
		// dropped transaction.
		return e.replaceDataFilesOneByOne(ctx, tbl, toRemove, toAdd, database, measurement, collapse, liveAfter)
	}
	if collapse {
		if err := e.collapseManifests(ctx, txn, database, measurement); err != nil {
			return nil, nil, err
		}
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("iceberg commit (add=%d remove=%d): %w", len(toAdd), len(toRemove), err)
	}
	return committed, nil, nil
}

// replaceDataFilesOneByOne is the partition-inference fallback: a fresh transaction with the
// removes, then one AddFiles per file so a straddling file is skipped instead of failing the
// batch. Files refused for stale DELETED history are collected and added in ONE merged step at
// the end (each merged add rewrites the manifest set, so they must not go one by one).
//
// collapse carries the caller's manifest-collapse decision (#1106) into this path, and is honoured
// here for the reason given at its use below. A merged add for stale DELETED history clears it,
// since that add already collapses the set.
func (e *Exporter) replaceDataFilesOneByOne(ctx context.Context, tbl *icetable.Table, toRemove, toAdd []string, database, measurement string, collapse bool, liveAfter int) (*icetable.Table, []string, error) {
	txn := tbl.NewTransaction()
	if len(toRemove) > 0 {
		if err := txn.ReplaceDataFiles(ctx, toRemove, nil, nil); err != nil {
			return nil, nil, fmt.Errorf("iceberg remove-only (remove=%d): %w", len(toRemove), err)
		}
	}
	var skipped, referenced []string
	for _, f := range toAdd {
		err := txn.AddFiles(ctx, []string{f}, nil, false)
		switch {
		case err == nil:
		case isPartitionInferenceError(err):
			skipped = append(skipped, f)
		case isAlreadyReferencedError(err):
			referenced = append(referenced, f)
		default:
			return nil, nil, fmt.Errorf("iceberg AddFiles(%s): %w", f, err)
		}
	}
	if len(referenced) > 0 {
		// A straddling file is never registered, so it never carries DELETED history: a
		// partition error here is not expected, and if it ever happens the pass fails like
		// any other add error and is retried next tick.
		if err := e.addFilesMerging(ctx, txn, referenced, database, measurement); err != nil {
			return nil, nil, fmt.Errorf("iceberg add with merge (files=%d): %w", len(referenced), err)
		}
		collapse = false // the merged add already collapsed the set — see replaceDataFilesResilient
	}
	// liveAfter counted the files the pass INTENDED to register; the ones skipped here never make
	// it into the table, so a pass whose every survivor is unmappable leaves the table empty after
	// all and must not collapse — see shouldCollapse for what an all-DELETED merge does. The
	// caller's guard cannot see this: it runs before the skips are known.
	if collapse && liveAfter-len(skipped) <= 0 {
		collapse = false
	}
	if collapse {
		// This path is the package's heaviest manifest producer — one snapshot AND one manifest
		// per added file — and it is sticky: a straddling file is never registered, so it is back
		// in toAdd on every later pass and the measurement takes this path for good. Exempting it
		// from the collapse would exempt exactly the table that needs it most.
		if err := e.collapseManifests(ctx, txn, database, measurement); err != nil {
			return nil, nil, err
		}
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("iceberg commit (resilient, skipped=%d): %w", len(skipped), err)
	}
	return committed, skipped, nil
}

// addFilesMerging adds paths that iceberg-go refused as "already referenced" (stale DELETED
// history) by enabling manifest merging for this transaction only and adding with the
// duplicate check off. The reconcile diff is the authority on the live file set (it comes from
// PlanFiles, which yields live entries only), so the check can only be tripped by DELETED
// history — including the removal staged one call earlier in this same transaction, which is
// the rewrite case. The toggle is staged metadata: a dropped transaction leaves the table as it was.
func (e *Exporter) addFilesMerging(ctx context.Context, txn *icetable.Transaction, paths []string, database, measurement string) error {
	if err := txn.SetProperties(manifestMergeOn()); err != nil {
		return fmt.Errorf("enable manifest merge: %w", err)
	}
	if err := txn.AddFiles(ctx, paths, nil, true); err != nil {
		return err // keep the partition-inference identity for the caller
	}
	if err := txn.SetProperties(manifestMergeOff()); err != nil {
		return fmt.Errorf("disable manifest merge: %w", err)
	}
	e.logger.Info().
		Str("database", database).Str("measurement", measurement).Int("files", len(paths)).
		Msg("Iceberg: re-registered paths that still had DELETED manifest history (merged manifests for this commit)")
	return nil
}

// manifestCollapseThreshold is the number of data manifests in the current snapshot at which a
// reconcile pass also collapses them into one (see collapseManifests). Every figure below is
// reproducible from internal/iceberg/deleted_manifest_cost_test.go on iceberg-go v0.7.0; measured
// local backend, Apple silicon, so read the ratios rather than the absolute milliseconds.
//
// What the pile costs, per accumulated manifest: ~0.24 ms per reconcile pass and ~0.105 ms per
// reader PlanFiles.
//
// What a collapse costs, as the marginal cost over the ordinary pass it replaces, measured where
// there is nothing to collapse yet (4 manifests) so the pile's own scan cost cannot flatter it:
// 5 ms at 300 live files, 8 ms at 1000, 33 ms at 5000, 66 ms at 10 000 — roughly 2 ms + 6.4 ms per
// 1000 live files, linear in table size because it rewrites the whole live set into one manifest.
//
// A removal pass adds ONE manifest (the rewritten manifest holding the removed entry, plus this
// pass's additions, minus the one the overwrite producer now drops when nothing survives in it), so
// between collapses the count averages ~T/2 and the amortised per-pass cost is 0.12*T + C(N)/T,
// minimised at T = sqrt(C(N)/0.12): 6.5 at 300 live files, 8.2 at 1000, 16.6 at 5000, 23.5 at
// 10 000. Twelve is within ~1.25x of optimal across that whole range.
//
// Reader plan time is paid per query rather than per pass, which would favour a lower threshold, but
// at 4 manifests it is already indistinguishable from one (1.53 vs 1.34 ms at 300 files, 18.4 vs
// 17.9 ms at 5000), so there is nothing to buy below ~10.
//
// On iceberg-go v0.6.0 a removal pass added TWO manifests and each cost ~1.6 ms per pass, which put
// the optimum at 4.5-12.3 and made this constant worth about six passes of headroom instead of
// twelve. v0.7.0 drops the DELETED-only manifest the overwrite producer used to carry forward
// (apache/iceberg-go#1153, #1393) and parallelises the manifest scan. The collapse is still needed:
// the remaining one-per-pass growth is still unbounded, and v0.7.0 does not reduce the .avro count
// on disk at all (258 files after 64 passes on both versions) — that is the orphan sweep's job.
//
// It is deliberately not a config key: the only consequence of a wrong value is a slower or a more
// frequent collapse, never data loss, so there is no operator decision to expose. Contrast
// iceberg.orphan_sweep_enabled, which gates irreversible deletion.
const manifestCollapseThreshold = 12

// shouldCollapse reports whether this pass should also collapse the table's manifest set.
// liveAfter is the number of data files the pass is driving the table to — len(want) at the call
// site, not the count it has now.
//
// It reads the manifest list of the CURRENT snapshot: one local file open (Iceberg export requires
// storage.backend="local"), uncached and side-effect free. It must be called before the pass
// stages anything, because after a successful ReplaceDataFiles the transaction can no longer be
// re-driven.
//
// Two conditions beyond the threshold, both load-bearing:
//
//   - liveAfter > 0. A pass that empties the measurement (the scheduler's all-data-files-gone
//     branch reconciles the table to EMPTY rather than leave it pointing at deleted paths) stages
//     an overwrite in which EVERY entry is DELETED and attributed to the overwrite's snapshot, not
//     the collapse's. manifestMergeManager.createManifest keeps a DELETED entry only when it
//     belongs to the snapshot being written, so the merge would drop all of them and write an
//     empty manifest, which ManifestWriter.Close refuses (ErrEmptyManifest). On iceberg-go v0.6.0
//     that error failed the pass, and the scheduler caches no fingerprint for a failed pass, so
//     the SAME pass failed on every tick from then on: the table pointed at deleted files forever,
//     the exact harm that branch exists to prevent.
//
//     On v0.7.0 that error is no longer REACHABLE from here, and the reason is worth stating so
//     nobody removes the guard on the strength of a passing test. Reaching it needs at least two
//     inherited data manifests whose entries are all foreign-DELETED; v0.7.0 drops a manifest with
//     no surviving entries instead of carrying it forward, so an emptying pass inherits exactly
//     one (this pass's own tombstone) and mergeBin takes its single-manifest passthrough without
//     ever calling createManifest. The guard is kept as belt-and-braces: it costs one comparison,
//     it encodes the intent (do not run a merge with nothing to merge), and reachability returns
//     with a multi-spec table or any upstream change to that drop. Its tests no longer
//     discriminate on v0.7.0 and say so on themselves.
//
//   - Only DATA manifests are counted. The merge producer merges data manifests and passes delete
//     manifests through untouched, so counting those would compare a number the collapse cannot
//     reduce against the threshold — and a table carrying threshold-many unmergeable manifests
//     would then collapse on EVERY pass, which is the O(files)-of-metadata-per-pass cost that
//     manifestMergeOn's comment exists to rule out. Arc writes no delete files; another engine
//     writing merge-on-read deletes into the table would.
//
// Fails open: a nil snapshot or any read error means "no collapse", which is exactly the behaviour
// before this change. A table whose metadata cannot be inspected is not one to start rewriting
// manifests on.
func (e *Exporter) shouldCollapse(ctx context.Context, tbl *icetable.Table, liveAfter int, database, measurement string) bool {
	if e.collapseThreshold <= 0 || liveAfter == 0 {
		return false
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return false
	}
	fio, err := tbl.FS(ctx)
	if err != nil {
		e.logger.Debug().Err(err).
			Str("database", database).Str("measurement", measurement).
			Msg("Iceberg: cannot open table filesystem to count manifests, skipping collapse")
		return false
	}
	manifests, err := snap.Manifests(fio)
	if err != nil {
		e.logger.Debug().Err(err).
			Str("database", database).Str("measurement", measurement).
			Msg("Iceberg: cannot read manifest list, skipping collapse")
		return false
	}
	data := 0
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData {
			data++
		}
	}
	return data >= e.collapseThreshold
}

// collapseManifests appends a merge-enabled append to the transaction that rewrites every data
// manifest of the table into one (#1106). Since iceberg-go v0.7.0 the overwrite producer already
// drops a manifest left with no surviving entries, so what this reclaims is the one-per-pass
// growth that remains: each pass's additions land in their own manifest and nothing merges them. It adds NO files: AddFiles with no paths still commits, because iceberg-go skips only
// the new-manifest producer when nothing was appended and runs the merge over the inherited set
// regardless. So the collapse rides on any pass — mixed, add-only or removal-only — which matters
// because retention alone produces removal-only passes on a measurement that gained no file since
// the last tick, and those are exactly the passes that accumulate the pile with no relief.
//
// It lands as its own snapshot after whatever the pass staged, inside the same commit, so readers
// of the current snapshot never observe an intermediate state. The duplicate check is off because
// there is nothing to check: no paths are being added.
//
// The property toggle is staged metadata — a dropped transaction leaves the table as it was.
func (e *Exporter) collapseManifests(ctx context.Context, txn *icetable.Transaction, database, measurement string) error {
	if err := txn.SetProperties(manifestMergeOn()); err != nil {
		return fmt.Errorf("iceberg enable manifest merge for collapse: %w", err)
	}
	if err := txn.AddFiles(ctx, nil, nil, true); err != nil {
		return fmt.Errorf("iceberg collapse manifests: %w", err)
	}
	if err := txn.SetProperties(manifestMergeOff()); err != nil {
		return fmt.Errorf("iceberg disable manifest merge after collapse: %w", err)
	}
	e.logger.Info().
		Str("database", database).Str("measurement", measurement).Int("threshold", e.collapseThreshold).
		Msg("Iceberg: collapsed the manifest set (merged every data manifest into one)")
	return nil
}

// manifestMergeOn is the property set that makes iceberg-go's append use its merge producer
// for the commit in flight: merge whenever the new manifest shares a bin with at least one
// existing one, and make the bin big enough (1 GiB, ~3M entries) that every manifest of the
// table lands in it — a manifest alone in a bin is never rewritten, and a stale DELETED-only
// manifest left alone would make the next removal of that path fail. The merged manifest
// keeps DELETED entries only of the snapshot being written, which is what sheds the history.
//
// It is enabled per transaction, never left on the table: with merging on, EVERY append commit
// would rewrite the whole manifest set, O(files) of metadata per pass, with nothing to show for it
// on a table whose manifest count is already low. That is the cost the threshold in
// manifestCollapseThreshold exists to ration — and the reason shouldCollapse counts only the
// manifests the merge can actually shed, since a count it cannot reduce would re-create exactly
// this per-pass rewrite.
//
// Merging leaves the table's live file list in one manifest, so every later removal pass rewrites
// that one manifest. Measured: that is not a new cost — an Arc table has had that shape since its
// first commit put the whole file set in one manifest, and post-collapse pass cost tracks manifest
// count on the same curve as pre-collapse (see the plan for #1106). The superseded manifests are
// reclaimed by sweepOrphanMetadata (#835), except where an operator has turned that sweep off.
//
// manifestMergeOff writes iceberg-go's own defaults back under the min-count and target-size keys
// rather than leaving these values as the table's standing policy. iceberg-go v0.7.0 does add
// Transaction.RemoveProperties, so the keys could now be deleted instead — writing the defaults is
// kept because it is equivalent for any reader (an unset key resolves to the same default) and
// does not depend on a method v0.6.0 lacked, which keeps this readable against both versions.
func manifestMergeOn() iceberg.Properties {
	return iceberg.Properties{
		icetable.ManifestMergeEnabledKey:    "true",
		icetable.ManifestMinMergeCountKey:   "2",
		icetable.ManifestTargetSizeBytesKey: "1073741824",
	}
}

// manifestMergeOff restores the default (fast-append) producer for later commits, and puts
// iceberg-go's own defaults back under the min-count/target-size keys. v0.6.0 has no
// RemoveProperties on a transaction, so those two keys cannot be deleted once set; left at
// manifestMergeOn's values they would stay on the table as its standing merge policy. They are
// inert for Arc (only newMergeAppendFilesProducer reads them, and it is never constructed while
// merging is off) but they are visible table properties, and another engine writing the table
// would honour a 1 GiB manifest target and a min-merge-count of 2 as if Arc had chosen them for
// it. Before the manifest collapse only #633 tables ever carried them; now nearly every exported
// table would.
func manifestMergeOff() iceberg.Properties {
	return iceberg.Properties{
		icetable.ManifestMergeEnabledKey:    "false",
		icetable.ManifestMinMergeCountKey:   strconv.Itoa(icetable.ManifestMinMergeCountDefault),
		icetable.ManifestTargetSizeBytesKey: strconv.Itoa(icetable.ManifestTargetSizeBytesDefault),
	}
}

// isAlreadyReferencedError reports iceberg-go's refusal to add a path that some manifest entry
// of the current snapshot already names — including entries with status DELETED, which is the
// case this package has to tolerate (see replaceDataFilesResilient).
func isAlreadyReferencedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "cannot add files that are already referenced by table")
}

// isPartitionInferenceError reports whether an error is iceberg-go's day()-partition
// inference failure for a file whose time range spans more than one partition value.
func isPartitionInferenceError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "more than one value for partition field")
}

// expireSnapshots keeps the last `retain` snapshots for the table (0 = keep all → no-op),
// deleting older snapshots and their orphaned manifests/data files. Best-effort: on any
// failure it logs and returns the input table unchanged.
// floorHistory asks expireSnapshots to keep ONLY the snapshot this pass committed,
// overriding iceberg.retain_snapshots. The caller passes it when the pass removed any
// path, because such a path is gone from the primary backend and every older snapshot
// still names it (#1092).
type expireMode bool

const (
	retainConfigured expireMode = false
	floorHistory     expireMode = true
)

// expireSnapshots trims snapshot history. It returns the possibly-newer table and
// whether the trim it was asked for actually landed; a caller that asked to floor and
// got false must not treat the pass as settled (see ReconcileMeasurementWithHint).
//
// mode is load-bearing for BOTH gates below, not just the iceberg-go call. The
// pointless-commit guard compares against the SAME effective value: checking a floor
// flag after a guard that still read e.retain would make the floor a no-op in the
// default configuration, since retain_snapshots is 10 and a table with 10 or fewer
// snapshots returns early. The reproducing table in #1092 had three.
func (e *Exporter) expireSnapshots(ctx context.Context, tbl *icetable.Table, database, measurement string, mode expireMode) (*icetable.Table, bool) {
	if e.retain <= 0 {
		return tbl, true
	}
	effective := e.retain
	if mode == floorHistory {
		effective = 1
	}
	before := len(tbl.Metadata().Snapshots())
	// Only expire when there's more history than we intend to keep, to avoid pointless commits.
	if before <= effective {
		return tbl, true
	}
	txn := tbl.NewTransaction()
	// WithRetainLast is a FLOOR, not a cap: iceberg-go only expires a snapshot when it is BOTH
	// older than maxSnapshotAgeMs AND beyond the retain-last count. iceberg-go v0.7.0 defaults
	// that age to 5 days (v0.6.0 defaulted it to effectively forever, so this comment used to
	// overstate the case), and moved the keys to history.expire.* with legacy fallbacks. Either
	// way the default is not what Arc wants. Pass WithOlderThan(0) so the age gate is
	// always satisfied and retain-last becomes the effective cap ("keep the last N, expire the
	// rest"). Cluster-mode note: this is fine because the reconciler is writer-gated (single
	// writer), so no concurrent reader-vs-expire race beyond Iceberg's own snapshot isolation.
	//
	// WithPostCommit(false) is load-bearing (#632). iceberg-go defaults it to true,
	// which makes Commit run orphan deletion as a post-commit hook: it os.Removes the
	// expiring snapshots' manifest lists, manifests AND the data files they reference,
	// joining every failure into the error Commit returns.
	//
	// That is wrong for Arc twice over. First, files leave an Arc table precisely
	// BECAUSE Arc already deleted them (compaction, retention, the delete API), so the
	// hook reports ENOENT for a commit that actually succeeded — and the list grows
	// every pass, because each expiring manifest carries entries for every file ever
	// removed. Second, and worse, postCommit=true hands iceberg-go physical delete
	// authority over Arc's primary data files: any future listing bug that transiently
	// omits live files for `retain` generations would turn into silent deletion of
	// customer data by the exporter. Arc owns the data-file lifecycle; the exporter
	// must never delete data files.
	//
	// The residue is expired metadata files (manifest lists, manifests) that are no
	// longer referenced. pruneOldVersionFiles already bounds the v<N>.metadata.json
	// copies; the rest is one manifest list plus one small manifest per commit — except
	// a commit that re-registered rewritten paths, which leaves a merged manifest the
	// size of the table's live file list behind (see manifestMergeOn). sweepOrphanMetadata
	// (#835, on by default) reclaims those, bounded by the reachability rule it documents:
	// every .avro any on-disk metadata.json can still resolve has to survive.
	//
	// No ref-level retention is set anywhere in Arc, so WithRetainLast is what decides:
	// iceberg-go resolves cmp.Or(ref.MinSnapshotsToKeep, option, property) and only the
	// option is populated here.
	if err := txn.ExpireSnapshots(
		icetable.WithRetainLast(effective),
		icetable.WithOlderThan(0),
		icetable.WithPostCommit(false),
	); err != nil {
		e.logger.Error().Err(err).Str("database", database).Str("measurement", measurement).
			Bool("floor_history", mode == floorHistory).
			Msg("Iceberg ExpireSnapshots failed (non-fatal) — snapshot history grows until it recovers")
		return tbl, false
	}
	expired, err := txn.Commit(ctx)
	if err != nil {
		e.logger.Error().Err(err).Str("database", database).Str("measurement", measurement).
			Bool("floor_history", mode == floorHistory).
			Msg("Iceberg ExpireSnapshots commit failed (non-fatal) — snapshot history grows until it recovers")
		return tbl, false
	}
	// Report dropped history only when the floor is what dropped it AND snapshots
	// actually went away. Gating on the mode alone would warn on a pass that expired
	// nothing — most visibly at retain_snapshots=1, where the floor equals the
	// configured value and this function is doing its ordinary job.
	if after := len(expired.Metadata().Snapshots()); mode == floorHistory && effective < e.retain && after < before {
		e.logger.Warn().
			Str("database", database).Str("measurement", measurement).
			Int("snapshots_before", before).Int("snapshots_after", after).
			Int("retain_snapshots", e.retain).
			Msg("Iceberg snapshot history floored to the current snapshot: this pass removed data files from the table, and Arc had already deleted them from storage, so every older snapshot referenced files that are gone. Time travel to them would have failed on the read. Arc does not support Iceberg time travel across compaction, retention or a partial DELETE")
	}
	return expired, true
}

// pruneOldVersionFiles keeps only the newest `retain` v<M>.metadata.json copies and deletes
// the rest. iceberg-go prunes its own NNNNN-*.metadata.json (via delete-after-commit) but does
// not know about the v<N> copies we write for directory-based readers, so we prune them here.
// Scan-based (not arithmetic) so it's robust to non-contiguous version numbers — a reconcile
// pass commits more than once (the file-set commit, then ExpireSnapshots), so versions advance
// by more than one.
// Best-effort; never deletes the current version.
func (e *Exporter) pruneOldVersionFiles(ctx context.Context, tbl *icetable.Table) {
	if e.backend == nil || e.retain <= 0 {
		return
	}
	cur, dirKey, ok := e.parseVersionAndMetaDir(tbl.MetadataLocation())
	if !ok {
		return
	}
	// A metadata location directly under the warehouse root gives dirKey "."
	// or "", so dirKey+"/" would be "./" or "/". Both listed empty before the
	// storage key contract existed; both are refused by it now (#743). There
	// is nothing to prune in that shape, so skip rather than report.
	if dirKey == "" || dirKey == "." {
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
		base := filepath.Base(k) // filepath.Base: local-backend keys may be backslash-separated on Windows
		if !strings.HasPrefix(base, "v") || !strings.HasSuffix(base, ".metadata.json") {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(base, "v"), ".metadata.json")
		if n, err := strconv.Atoi(numStr); err == nil {
			vs = append(vs, vfile{n: n, key: path.Join(dirKey, base)})
		}
	}
	// Keep one more than `retain` to cover the read-vs-prune race: a directory
	// reader fetches version-hint.text and only then opens v<N>.metadata.json.
	// If a commit lands in that window, keeping exactly `retain` can delete the
	// version the reader just resolved — sharpest at retain=1, where every
	// commit would invalidate the immediately-preceding version. The extra file
	// is a few KB and bounds the window to one full reconcile interval.
	keep := e.retain + 1
	if len(vs) <= keep {
		return
	}
	// Keep the newest `keep`; delete the rest. Never delete the current version.
	sort.Slice(vs, func(i, j int) bool { return vs[i].n > vs[j].n }) // descending
	curN, _ := strconv.Atoi(cur)
	for _, v := range vs[keep:] {
		if v.n == curN {
			continue
		}
		if err := e.backend.Delete(ctx, v.key); err != nil {
			e.logger.Debug().Err(err).Str("key", v.key).Msg("prune old v<N> (non-fatal)")
		}
	}
}

// sweepOrphanMetadata deletes the manifest lists and manifests under the table's metadata
// directory that NO metadata.json still on disk can reach (#835).
//
// Why anything is orphaned at all: expireSnapshots runs WithPostCommit(false) on purpose (#632 —
// iceberg-go's post-commit hook would also delete Arc's DATA files, which the exporter must never
// be allowed to do), and nothing else deletes the .avro files an expired snapshot leaves behind.
// A dropped transaction leaves the same residue: iceberg-go writes its manifests inside the
// snapshot producer, before Commit, so a refusal after that point (see replaceDataFilesResilient)
// abandons files nothing reclaims. Unbounded on disk, and copied into every backup.
//
// Reachability is computed from the metadata files ON DISK, never from the current snapshot alone.
// Arc deliberately keeps older entry points a reader can resolve: retain+1 v<N>.metadata.json
// copies (see pruneOldVersionFiles) and iceberg-go's own NNNNN-*.metadata.json log. A reader that
// resolves any of them gets that file's snapshots, so every .avro any on-disk metadata.json can
// reach has to survive. The set of on-disk metadata files only ever shrinks (delete-after-commit
// and pruneOldVersionFiles, both of which have already run by the time this is called), so a
// manifest this sweep spares stays spared, and one it deletes can never become referenced again.
//
// Deletions are restricted to names ending ".avro" DIRECTLY under the table's metadata directory.
// That is what keeps the blast radius off everything else: Arc's data files are .parquet and live
// outside the warehouse table directory, version-hint.text and *.metadata.json are not .avro, and
// Iceberg statistics are Puffin — so none of them are deletable here regardless of reachability.
//
// Every failure fails CLOSED. A listing error, an unreadable or unparsable metadata.json, or a
// manifest list that cannot be opened all yield an incomplete reachable set, and an incomplete
// reachable set must never authorise a delete — a deleted manifest is the only record of which
// data files a snapshot held, and nothing regenerates it.
//
// Best-effort with respect to the reconcile: this runs after the pass has committed, so a failure
// here never undoes a snapshot. Single-writer, like the rest of the reconcile path.
func (e *Exporter) sweepOrphanMetadata(ctx context.Context, tbl *icetable.Table) {
	if !e.orphanSweepEnabled || e.backend == nil {
		return
	}
	// Ages come from the optional ObjectLister extension. Without it there is no way to tell a
	// manifest abandoned long ago from one a commit wrote a moment ago, so do not sweep at all.
	lister, ok := e.backend.(storage.ObjectLister)
	if !ok {
		return
	}
	_, dirKey, ok := e.parseVersionAndMetaDir(tbl.MetadataLocation())
	if !ok {
		return
	}
	// A metadata location directly under the warehouse root gives "." or "", both of which the
	// storage key contract refuses as prefixes (#743) — same guard as pruneOldVersionFiles.
	if dirKey == "" || dirKey == "." {
		return
	}

	objs, err := lister.ListObjects(ctx, dirKey+"/")
	if err != nil {
		// Warn, not Debug: Debug is invisible at Arc default log level, and a listing that keeps
		// failing (a permission problem, a key the contract refuses) makes the whole sweep
		// permanently inert while the metadata directory grows.
		e.logger.Warn().Err(err).Str("dir", dirKey).Msg("Iceberg orphan sweep: listing the metadata directory failed, skipping (nothing deleted)")
		return
	}

	// Split the listing into entry points (metadata.json) and candidates (.avro old enough to be
	// past the grace). Candidates are collected first so a directory whose .avro files are ALL
	// young costs one listing and no metadata reads at all. That is the young-table case, not the
	// steady state: once a table has history, its retained versions keep old-but-reachable
	// manifests around, so most passes do walk every metadata.json here. The walk stays cheap
	// because reachableManifestNames dedupes the manifest-list reads, which are the I/O.
	var metaKeys []string
	var candidates []string
	young := 0
	for _, o := range objs {
		// ListObjects is recursive. Only sweep the metadata directory itself; a nested directory
		// is not ours to reason about, and its basenames are not in the reachable set.
		//
		// path.Dir/path.Base, not filepath.*: backend keys are slash-separated on every platform
		// (LocalBackend.ListObjects runs filepath.ToSlash before returning them), so the slash
		// forms are correct here and on Windows. Note this differs from pruneOldVersionFiles above.
		//
		// This filter and the candidate filter below read the SAME key shape, which is load-bearing
		// for safety: if the shapes ever diverged, the mismatch would yield zero candidates and an
		// early return, never an empty reachable set authorising a mass delete.
		if path.Dir(o.Path) != dirKey {
			continue
		}
		base := path.Base(o.Path)
		switch {
		case strings.HasSuffix(base, ".metadata.json"):
			metaKeys = append(metaKeys, o.Path)
		case strings.HasSuffix(base, ".avro"):
			if time.Since(o.LastModified) < e.orphanGrace {
				young++
				continue
			}
			candidates = append(candidates, o.Path)
		}
	}
	if len(candidates) == 0 {
		return
	}

	reachable, err := e.reachableManifestNames(ctx, tbl, metaKeys)
	if err != nil {
		// Loud, not Debug: a sweep that cannot establish reachability reclaims nothing, every
		// pass, and an operator watching the metadata directory grow deserves the reason.
		//
		// It can also stay that way. Recovery needs the unreadable metadata.json to be retired by
		// pruneOldVersionFiles or delete-after-commit, which takes further commits on this table —
		// and a table whose file set has stopped changing gets no further passes at all (the
		// scheduler fingerprint-gates it). So a warehouse restored with its .avro set lagging its
		// metadata.json set can leave this table unswept until the files are reconciled by hand.
		// That is the fail-closed direction and the right one, but it is not self-healing.
		e.logger.Warn().Err(err).Str("dir", dirKey).
			Msg("Iceberg orphan sweep: could not establish the reachable manifest set, skipping (nothing deleted)")
		return
	}

	deleted, failed := 0, 0
	for _, key := range candidates {
		if _, live := reachable[path.Base(key)]; live {
			continue
		}
		if err := e.backend.Delete(ctx, key); err != nil {
			e.logger.Debug().Err(err).Str("key", key).Msg("Iceberg orphan sweep: delete failed (non-fatal)")
			failed++
			continue
		}
		deleted++
	}
	if deleted > 0 || failed > 0 {
		e.logger.Info().Str("dir", dirKey).
			Int("deleted", deleted).Int("failed", failed).
			Int("reachable", len(reachable)).Int("within_grace", young).
			Msg("Iceberg orphan sweep: reclaimed unreachable manifest files")
	}
}

// reachableManifestNames returns the BASENAMES of every manifest list and manifest that any of the
// given metadata.json keys can reach. An error means the set is incomplete and the caller must not
// delete anything.
//
// Basenames, not full paths, on purpose. Paths inside metadata are absolute URIs (file://…) while
// backend keys are storage-root-relative, and converting between the two is the exact arithmetic
// that shipped broken in #534. Within one table's metadata directory a basename is unique — each
// commit mints a fresh UUID for both shapes, "<commitUUID>-m<N>.avro" and
// "snap-<snapshotID>-<attempt>-<commitUUID>.avro" — and iceberg-go writes both into that same
// directory (the location provider's metadata path, which Arc never overrides). If a reachable
// manifest ever did live elsewhere, its basename is still in this set, so a same-named local file
// is KEPT: the failure direction is over-retention, never deletion of something live.
// Paths in the errors below are deliberately NOT %q: the caller logs them through .Err(err), and
// Arc masks quoted spans in every logged error (internal/logger/errsanitize.go), which would blank
// out the one detail an operator needs.
func (e *Exporter) reachableManifestNames(ctx context.Context, tbl *icetable.Table, metaKeys []string) (map[string]struct{}, error) {
	fio, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("open table filesystem: %w", err)
	}
	reachable := make(map[string]struct{})
	// The same snapshot appears in many of these files; without this the walk would re-read every
	// manifest list once per metadata version that mentions it.
	listsSeen := make(map[string]struct{})
	for _, key := range metaKeys {
		raw, err := e.backend.Read(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read metadata %s: %w", key, err)
		}
		md, err := icetable.ParseMetadataBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("parse metadata %s: %w", key, err)
		}
		for _, snap := range md.Snapshots() {
			if snap.ManifestList == "" {
				continue
			}
			if _, dup := listsSeen[snap.ManifestList]; dup {
				continue
			}
			listsSeen[snap.ManifestList] = struct{}{}
			reachable[path.Base(snap.ManifestList)] = struct{}{}
			manifests, err := snap.Manifests(fio)
			if err != nil {
				return nil, fmt.Errorf("read manifest list %s (snapshot %d of %s): %w",
					snap.ManifestList, snap.SnapshotID, key, err)
			}
			for _, m := range manifests {
				reachable[path.Base(m.FilePath())] = struct{}{}
			}
		}
	}
	return reachable, nil
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
// Best-effort with respect to the reconcile: a failure here never undoes the committed
// snapshot, and the SQL catalog remains the source of truth. It DOES return false so the
// caller can decline to cache the measurement's fingerprint, which is what makes the retry
// real — without that, a measurement whose file set then goes quiet is never revisited and
// the hint stays stale indefinitely. Works for local and S3 warehouses via the backend.
//
// Returns true when there was nothing to do (no backend) or everything was written.
func (e *Exporter) writeVersionHint(ctx context.Context, tbl *icetable.Table) bool {
	if e.backend == nil {
		return true
	}
	metaLoc := tbl.MetadataLocation() // e.g. file:///…/metadata/00004-<uuid>.metadata.json
	// iceberg-go writes metadata files and directories at umask permissions
	// (its LocalFS uses 0o777-masked creates), while every Arc-written file
	// is 0600 (#639 item 7). Harden the freshly committed table's metadata
	// tree after each commit; bounded by the retain cap and snapshot expiry,
	// so this walk is O(retained versions), not O(history).
	e.hardenLocalMetadataPerms(metaLoc)
	version, dirKey, ok := e.parseVersionAndMetaDir(metaLoc)
	if !ok {
		// Reachable when iceberg.warehouse points outside the storage root: the
		// backend cannot address the metadata directory, so directory-based
		// readers (DuckDB iceberg_scan on a bare path, Spark hadoop-format) can
		// never discover this table. Catalog-based readers are unaffected. This
		// is a permanent property of the configuration, not a transient failure,
		// so warn once per table rather than returning false and retrying a
		// write that can never succeed.
		e.warnHintUnaddressableOnce(metaLoc)
		return true
	}
	okAll := true
	defer func() {
		if !okAll {
			e.noteHintFailure()
		}
	}()
	// Copy the current metadata to v<N>.metadata.json (Hadoop-convention name).
	metaKey, kOK := e.warehouseRelKey(metaLoc)
	if !kOK {
		okAll = false
	} else {
		if body, err := e.backend.Read(ctx, metaKey); err != nil {
			e.logger.Warn().Err(err).Str("key", metaKey).Msg("Failed to read current metadata for v<N> copy (will retry next pass)")
			okAll = false
		} else {
			vKey := path.Join(dirKey, "v"+version+".metadata.json")
			if err := e.backend.Write(ctx, vKey, body); err != nil {
				e.logger.Warn().Err(err).Str("key", vKey).Msg("Failed to write v<N>.metadata.json (will retry next pass)")
				okAll = false
			}
		}
	}
	if !okAll {
		return false
	}
	// Write the version-hint pointer — just the integer, NO trailing newline (DuckDB reads the
	// file verbatim and would look for "v<N>\n.metadata.json" otherwise; verified empirically).
	hintKey := path.Join(dirKey, "version-hint.text")
	if err := e.backend.Write(ctx, hintKey, []byte(version)); err != nil {
		e.logger.Warn().Err(err).Str("key", hintKey).Msg("Failed to write version-hint.text (will retry next pass)")
		okAll = false
	}
	return okAll
}

// warnHintUnaddressableOnce warns that version-hint publishing is disabled for a
// metadata directory, at most once per directory, so a permanent configuration
// property does not produce a line on every reconcile pass.
func (e *Exporter) warnHintUnaddressableOnce(metaLoc string) {
	dir := path.Dir(metaLoc)
	e.hintWarnMu.Lock()
	_, warned := e.hintWarned[dir]
	if !warned {
		if e.hintWarned == nil {
			e.hintWarned = make(map[string]struct{})
		}
		e.hintWarned[dir] = struct{}{}
	}
	e.hintWarnMu.Unlock()
	if warned {
		return
	}
	e.logger.Warn().
		Str("metadata", metaLoc).
		Str("warehouse", e.warehouse).
		Msg("Iceberg warehouse is not under the storage root: version-hint.text and v<N>.metadata.json " +
			"cannot be written, so directory-based readers (DuckDB iceberg_scan, Spark hadoop-format) " +
			"cannot discover this table. Catalog-based readers (PyIceberg, the SQLite catalog) work normally")
}

// warehouseRelKey converts a full metadata URI to a STORAGE-RELATIVE key, since backend
// Read/Write operate on keys relative to the STORAGE ROOT — not to the warehouse. Returns
// ok=false if the URI isn't under the configured warehouse.
//
// The two bases coincide only when iceberg.warehouse is the storage root (the default), so
// trimming the warehouse silently produced a key missing the warehouse's own path segment
// whenever an operator pointed iceberg.warehouse at a SUBDIRECTORY — the version-hint and
// v<N>.metadata.json copies then landed outside the warehouse, where directory-based readers
// (DuckDB, Spark) could not resolve the current snapshot. Gate on the warehouse, trim the root.
//
//	storage root "file:///data", warehouse "file:///data/wh",
//	metaLoc "file:///data/wh/arc_db.db/cpu/metadata/00004-<uuid>.metadata.json"
//	-> "wh/arc_db.db/cpu/metadata/00004-<uuid>.metadata.json"
func (e *Exporter) warehouseRelKey(metaLoc string) (string, bool) {
	// Resolve symlinks before comparing (#639 item 5): the warehouse config,
	// the storage root, and the metadata location can each render the same
	// physical directory through different spellings (a symlinked data dir,
	// an unclean path), and raw string comparison then lands every commit in
	// warn-once "hint unaddressable" mode. Resolution must be SYMMETRIC —
	// all three values or none — and the two constant sides are cached.
	e.resolveOnce.Do(func() {
		e.resolvedWarehouse = decodedURIPath(resolveFileURI(e.warehouse))
		if e.backend != nil {
			e.resolvedRoot = decodedURIPath(resolveFileURI(DefaultWarehouse(e.backend)))
		}
	})
	loc := decodedURIPath(resolveFileURI(metaLoc))
	if !isUnderDir(loc, e.resolvedWarehouse) {
		return "", false
	}
	// No backend (tests/no-op mode): warehouse is the only base we have.
	if e.backend == nil {
		return strings.TrimPrefix(strings.TrimPrefix(loc, e.resolvedWarehouse), "/"), true
	}
	if !isUnderDir(loc, e.resolvedRoot) {
		// Warehouse is outside the storage root entirely — the backend cannot address it.
		return "", false
	}
	return strings.TrimPrefix(strings.TrimPrefix(loc, e.resolvedRoot), "/"), true
}

// decodedURIPath returns the URI's logical path without percent escapes for comparisons and
// backend-relative keys. iceberg-go encodes JSON namespace components in MetadataLocation, while
// local directories and object keys use their decoded names.
func decodedURIPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return raw
	}
	return u.Scheme + "://" + u.Host + u.Path
}

// hardenLocalMetadataPerms chmods a local warehouse table's metadata files to
// 0600 and its directories to 0700, matching Arc's own writes. Local
// warehouses only; object stores have no modes. Best-effort: a chmod failure
// is a consistency nit, never a commit failure.
func (e *Exporter) hardenLocalMetadataPerms(metaLoc string) {
	const scheme = "file://"
	resolved := resolveFileURI(metaLoc)
	if !strings.HasPrefix(resolved, scheme) {
		return
	}
	metaDir := filepath.Dir(filepath.FromSlash(strings.TrimPrefix(resolved, scheme)))
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		return
	}
	// Tighten ONLY: clear group/other bits, never add owner bits. An
	// operator's deliberately restrictive mode (or a test's read-only
	// directory) must stay restrictive; the target is iceberg-go's
	// umask-wide 0644/0755 creates, not modes someone chose.
	tighten := func(path string, info os.FileMode) {
		hardened := info &^ 0o077
		if hardened != info {
			if err := os.Chmod(path, hardened); err != nil {
				e.logger.Debug().Err(err).Str("path", path).Msg("Could not harden metadata permissions")
			}
		}
	}
	for _, ent := range entries {
		if info, err := ent.Info(); err == nil {
			tighten(filepath.Join(metaDir, ent.Name()), info.Mode().Perm())
		}
	}
	// The metadata dir, its table dir, and the namespace dir are all created
	// by iceberg-go at umask permissions; the warehouse root itself is left
	// alone (it may be shared or externally managed).
	for _, dir := range []string{metaDir, filepath.Dir(metaDir), filepath.Dir(filepath.Dir(metaDir))} {
		if info, err := os.Stat(dir); err == nil {
			tighten(dir, info.Mode().Perm())
		}
	}
}

// resolveFileURI canonicalizes a file:// URI for path comparison: scheme
// stripped, symlinks resolved, re-URI'd via localFileURI. When the full path
// does not exist yet, its parent directory is resolved and the base rejoined,
// so a not-yet-written metadata location still canonicalizes consistently
// with its directory. Non-file schemes (s3://, azure://) return unchanged:
// object keys have no symlinks and must keep exact string semantics.
func resolveFileURI(uri string) string {
	const scheme = "file://"
	if !strings.HasPrefix(uri, scheme) {
		return uri
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme != "file" || parsed.Host != "" {
		return uri
	}
	p := filepath.FromSlash(parsed.Path)
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return localFileURI(resolved)
	}
	if resolvedDir, err := filepath.EvalSymlinks(filepath.Dir(p)); err == nil {
		return localFileURI(filepath.Join(resolvedDir, filepath.Base(p)))
	}
	return uri
}

// isUnderDir reports whether p is dir itself or lies beneath it, matching only at a path
// boundary. A bare strings.HasPrefix would accept a sibling whose name merely starts with dir's
// ("file:///data/wh" would match "file:///data/wh-other/…"), which for warehouseRelKey means
// accepting another warehouse's metadata as our own — and pruneOldVersionFiles DELETES files
// under the key it derives, so the gate has to match on real path segments.
func isUnderDir(p, dir string) bool {
	dir = strings.TrimSuffix(dir, "/")
	return p == dir || strings.HasPrefix(p, dir+"/")
}

// parseVersionAndMetaDir derives, from a full metadata-file URI, the version integer (e.g.
// "4" from 00004-<uuid>.metadata.json) and the STORAGE-RELATIVE key of the metadata/ directory
// it lives in. Returns ok=false if not under the warehouse or the filename doesn't match.
func (e *Exporter) parseVersionAndMetaDir(metaLoc string) (version, dirKey string, ok bool) {
	rel, ok := e.warehouseRelKey(metaLoc)
	if !ok {
		return "", "", false
	}
	base := filepath.Base(rel) // 00004-<uuid>.metadata.json; filepath.Base for Windows backslash keys
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

// tableDataFiles returns the LIVE physical data-file paths in the table's current snapshot,
// each with the file_size_in_bytes its manifest entry recorded at registration. Uses
// Scan().PlanFiles, which resolves the current snapshot's live files honoring deletes across
// snapshots — NOT AllManifests, which returns manifests from superseded snapshots too and
// would report files a later removal has already dropped.
func (e *Exporter) tableDataFiles(ctx context.Context, tbl *icetable.Table) (map[string]int64, error) {
	out := make(map[string]int64)
	if tbl.CurrentSnapshot() == nil {
		return out, nil // empty table
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	// One data file can yield SEVERAL tasks since iceberg-go v0.7.0: a Parquet file above
	// read.split.target-size (128 MiB) with at least two split offsets is planned as one task per
	// byte range. This stays correct only because it is a map keyed on the path storing the FILE's
	// size, which is identical across a file's splits — len(tasks), a slice, or task.Length would
	// each double-count. Arc's own files are far below that size; bulk-imported Parquet need not be.
	for _, task := range tasks {
		out[task.File.FilePath()] = task.File.FileSizeBytes()
	}
	return out, nil
}

// isAlreadyExists reports whether a catalog error means "the thing is already there", which the
// idempotent create paths treat as success. Matches on iceberg-go's typed sentinels (the SQL
// catalog wraps them with %w, so errors.Is unwraps correctly) rather than substring-matching the
// message: the old strings.Contains(msg, "exist") also matched "does not exist", so a genuine
// not-found/backend error could be swallowed as success.
func isAlreadyExists(err error) bool {
	return errors.Is(err, icecatalog.ErrNamespaceAlreadyExists) ||
		errors.Is(err, icecatalog.ErrTableAlreadyExists)
}

// DropDatabase removes every Iceberg catalog artifact for an Arc database
// (#639 item 3): each table in its namespace, the namespace itself, and the
// warehouse metadata files under the namespace directory. Called from the
// database-delete API after the data files are gone; that ordering is
// load-bearing — with no files left, a racing reconcile pass can only empty
// tables, never recreate them, whereas catalog-first would let EnsureTable
// resurrect residue mid-cleanup. Idempotent: a database that was never
// exported (or already cleaned) returns nil, so a re-run of the delete
// converges.
//
// Spoke-namespace pseudo-databases contain a path separator and are refused:
// their namespace naming does not follow this mapping, and their lifecycle is
// owned by edge-sync.
func (e *Exporter) DropDatabase(ctx context.Context, database string) error {
	if strings.ContainsAny(database, "/\\") {
		return fmt.Errorf("refusing to drop namespaced database %q from the Iceberg catalog", database)
	}
	ns := icetable.Identifier{e.nsPrefix + "_" + database}

	// The SQL catalog's DropNamespace refuses non-empty namespaces, so drop
	// every table first. Collect the first error but keep going: partial
	// cleanup converges on the next delete attempt.
	var firstErr error
	for ident, err := range e.catalog.ListTables(ctx, ns) {
		if err != nil {
			if errors.Is(err, icecatalog.ErrNoSuchNamespace) {
				return nil // never exported; nothing to clean
			}
			return fmt.Errorf("list tables for drop: %w", err)
		}
		if err := e.catalog.DropTable(ctx, ident); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("drop table %v: %w", ident, err)
		}
	}
	if err := e.catalog.DropNamespace(ctx, ns); err != nil && !errors.Is(err, icecatalog.ErrNoSuchNamespace) && firstErr == nil {
		firstErr = fmt.Errorf("drop namespace: %w", err)
	}

	// Warehouse holds ONLY metadata (data files are Arc's own tree, already
	// deleted by the caller); remove the namespace directory's objects via
	// the backend so local and object-store warehouses behave alike.
	if e.backend != nil {
		nsDirURI := e.warehouse + "/" + e.nsPrefix + "_" + database + ".db"
		// relDir == "" means the namespace directory IS the storage root, so
		// relDir+"/" would be "/", which the storage key contract refuses
		// (#743). It listed empty before, and enumerating the whole root to
		// delete it is not what this is for, so skip it.
		if relDir, ok := e.warehouseRelKey(nsDirURI); ok && relDir != "" {
			keys, err := e.backend.List(ctx, relDir+"/")
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("list warehouse metadata: %w", err)
			}
			dirs := map[string]bool{relDir: true}
			for _, key := range keys {
				if err := e.backend.Delete(ctx, key); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("delete warehouse object %s: %w", key, err)
				}
				for d := path.Dir(key); len(d) > len(relDir); d = path.Dir(d) {
					dirs[d] = true
				}
			}
			// RemoveDirectory is non-recursive, so sweep the now-empty
			// directory tree deepest-first (object stores no-op here).
			if remover, ok := e.backend.(storage.DirectoryRemover); ok {
				ordered := make([]string, 0, len(dirs))
				for d := range dirs {
					ordered = append(ordered, d)
				}
				sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
				for _, d := range ordered {
					_ = remover.RemoveDirectory(ctx, d)
				}
			}
		}
	}
	return firstErr
}
