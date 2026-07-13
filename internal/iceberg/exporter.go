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
	"fmt"
	"sort"
	"strings"

	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
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
	catalog  *sqlcat.Catalog
	nsPrefix string // namespace for Iceberg tables (e.g. "arc")
	logger   zerolog.Logger
}

// NewExporter builds an Exporter backed by a SQL (SQLite) Iceberg catalog on the given
// *sql.DB. Pass Arc's existing shared SQLite handle (mattn sqlite3) so the catalog rides the
// same DB as auth/tiering/retention — no second SQLite implementation, no extra service.
// warehouse is the object-store/local root under which table metadata is written
// (file://… or s3://bucket/prefix).
func NewExporter(db *sql.DB, warehouse, nsPrefix string, logger zerolog.Logger) (*Exporter, error) {
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
		catalog:  cat,
		nsPrefix: nsPrefix,
		logger:   logger.With().Str("component", "iceberg-exporter").Logger(),
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
		return tbl, nil // already exists
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

	tbl, err := e.catalog.CreateTable(ctx, ident, schema, createOpts...)
	if err != nil {
		return nil, fmt.Errorf("create iceberg table %v: %w", ident, err)
	}
	e.logger.Info().Str("database", database).Str("measurement", measurement).Msg("Created Iceberg table")
	return tbl, nil
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
	if _, err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("iceberg commit (add=%d remove=%d): %w", len(toAdd), len(toRemove), err)
	}
	e.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int("added", len(toAdd)).Int("removed", len(toRemove)).
		Msg("Reconciled Iceberg table")
	return nil
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
