package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// encodedNamespacePrefix is the marker iceberg-go v0.7.0's SQL catalog puts in
// front of a JSON-encoded namespace key. It encodes a namespace whose component
// contains a dot rather than storing the plain dotted string, because that
// string belongs to a different namespace (catalog/sql: namespaceStorageKeys
// refuses the legacy key for exactly that reason).
const encodedNamespacePrefix = "__iceberg_namespace_v1__:"

type namespaceTableRow struct {
	namespace        string
	table            string
	metadataLocation sql.NullString
}

// namespaceMigration is one catalog row to rekey, and the measurement it serves.
type namespaceMigration struct {
	database     string
	measurement  string
	oldNamespace string
	newNamespace string
}

// ConfigureNamespaceMigrationDryRun sets whether the dotted-namespace migration only reports
// its plan or applies it. Call before starting the scheduler. Dry-run is the safe default.
func (e *Exporter) ConfigureNamespaceMigrationDryRun(dryRun bool) {
	e.namespaceMigrationDryRun = dryRun
}

// MigrateDottedNamespaces rekeys catalog rows that an older Arc wrote under a plain dotted
// namespace, so iceberg-go v0.7.0 can address them again (#1129).
//
// It REKEYS THE CATALOG ROW AND MOVES NOTHING ON DISK, and that is the whole design rather than a
// shortcut. Iceberg records manifest and data locations as absolute paths inside the table
// metadata, and nothing in iceberg-go rewrites them when a table's location changes -- a
// SetLocation update only sets the metadata's own location field (table/updates.go,
// setLocationUpdate.Apply). So a migration that relocated the warehouse directory would publish a
// table whose current snapshot still reads its manifest chain out of the OLD tree, leaving the
// copies in the new tree referenced by nothing and the old tree permanently load-bearing while the
// log reported a completed move. Rekeying leaves exactly one tree: the table keeps the location it
// already has, later commits write new metadata versions beside the existing ones, and the legacy
// directory stays excluded from the data walk because isWarehouseDir matches its
// "<nsPrefix>_....db" shape on its first branch.
//
// The returned set names the measurements that must NOT be reconciled on this pass. Until its row
// is rekeyed, a legacy table is unreachable under the identifier tableIdent now builds, so letting
// the scheduler proceed would CREATE A SECOND TABLE beside the readable legacy one -- the
// orphaning this issue exists to prevent. Scheduler.runPass is what acts on it; that coupling is
// the point of the mechanism and is pinned by TestSchedulerSkipsBlockedMeasurements.
func (e *Exporter) MigrateDottedNamespaces(ctx context.Context, measurements []Measurement) (map[string]struct{}, error) {
	blocked := make(map[string]struct{})
	if e.db == nil {
		return blocked, nil
	}

	// Build the legacy-to-current mapping FIRST and do no catalog I/O at all when nothing could
	// have a legacy row. This runs on every reconcile pass of every deployment with Iceberg
	// enabled, and only a deployment with a dotted spoke has anything here to do.
	legacy := e.legacyNamespaceCandidates(measurements)
	if len(legacy) == 0 {
		return blocked, nil
	}

	rows, err := e.namespaceTableRows(ctx)
	if err != nil {
		return blocked, err
	}
	for _, row := range rows {
		migration, matches, affected, err := e.namespaceMigrationForRow(row, legacy)
		if err != nil {
			for _, measurement := range affected {
				blocked[measurementKey(measurement.Database, measurement.Measurement)] = struct{}{}
			}
			// Paths and namespaces here are deliberately NOT %q: the caller logs this through
			// .Err(err) and Arc masks quoted spans in every logged error (logger.installErrSanitizer),
			// which would blank out the one detail naming the row an operator has to fix.
			e.logger.Error().Err(err).Str("namespace", row.namespace).Str("table", row.table).
				Msg("Iceberg dotted-namespace migration could not be planned; leaving the legacy table readable and unreconciled")
			continue
		}
		if !matches {
			continue
		}

		key := measurementKey(migration.database, migration.measurement)
		blocked[key] = struct{}{}
		if e.namespaceMigrationDryRun {
			e.logger.Info().
				Str("database", migration.database).
				Str("measurement", migration.measurement).
				Str("from_namespace", migration.oldNamespace).
				Str("to_namespace", migration.newNamespace).
				Msg("Iceberg dotted-namespace migration planned; no catalog changes made, and this measurement is not exported until it runs. Set iceberg.namespace_migration_dry_run=false to apply it")
			continue
		}
		if err := e.rekeyNamespaceTable(ctx, migration); err != nil {
			e.logger.Error().Err(err).
				Str("database", migration.database).
				Str("measurement", migration.measurement).
				Str("from_namespace", migration.oldNamespace).
				Str("to_namespace", migration.newNamespace).
				Msg("Iceberg dotted-namespace migration failed; the legacy table stays readable and the measurement stays unreconciled until the next pass")
			continue
		}
		e.logger.Info().
			Str("database", migration.database).
			Str("measurement", migration.measurement).
			Str("from_namespace", migration.oldNamespace).
			Str("to_namespace", migration.newNamespace).
			Msg("Migrated Iceberg dotted namespace; the table keeps its existing warehouse directory and metadata")
		delete(blocked, key)
	}
	return blocked, nil
}

// measurementKey is the scheduler's per-measurement map key. NUL-joined because neither a database
// nor a measurement name can contain one, so the halves can never run together ambiguously.
func measurementKey(database, measurement string) string {
	return database + "\x00" + measurement
}

// legacyNamespaceCandidates maps the catalog key an older Arc would have written to the current
// measurements that would land there. Keyed by the LEGACY namespace and table, which is what a
// pre-upgrade catalog row holds.
func (e *Exporter) legacyNamespaceCandidates(measurements []Measurement) map[string][]Measurement {
	candidates := make(map[string][]Measurement)
	for _, measurement := range measurements {
		// Only a name carrying a dot or a separator could have produced a different key: the old
		// scheme was nsPrefix + "_" + strings.ReplaceAll(database, "/", "."), so for every other
		// name the legacy and current keys are the same string and there is nothing to migrate.
		if !strings.ContainsAny(measurement.Database, "./") {
			continue
		}
		namespace, err := namespaceIdentifier(e.nsPrefix, measurement.Database)
		if err != nil {
			continue // not addressable at all; EnsureTable reports it per measurement
		}
		oldNamespace := e.nsPrefix + "_" + strings.ReplaceAll(measurement.Database, "/", ".")
		if oldNamespace == catalogNamespaceKey(namespace) {
			continue
		}
		key := measurementKey(oldNamespace, measurement.Measurement)
		candidates[key] = append(candidates[key], measurement)
	}
	return candidates
}

func (e *Exporter) namespaceTableRows(ctx context.Context) ([]namespaceTableRow, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT table_namespace, table_name, metadata_location
		 FROM iceberg_tables WHERE catalog_name = ?`, e.catalogName)
	if err != nil {
		return nil, fmt.Errorf("list Iceberg catalog tables for namespace migration: %w", err)
	}
	defer rows.Close()

	var out []namespaceTableRow
	for rows.Next() {
		var row namespaceTableRow
		if err := rows.Scan(&row.namespace, &row.table, &row.metadataLocation); err != nil {
			return nil, fmt.Errorf("read Iceberg catalog table for namespace migration: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Iceberg catalog tables for namespace migration: %w", err)
	}
	return out, nil
}

// namespaceMigrationForRow decides what one catalog row needs. The returned measurements are the
// ones to block when the error is non-nil: a row that cannot be planned must not be reconciled
// either, or the scheduler would create a duplicate table beside it.
//
// A row already under the encoded key needs nothing, and there is no half-migrated state to
// resume: the rekey is one transaction, so a row holds either the legacy key or the new one.
func (e *Exporter) namespaceMigrationForRow(row namespaceTableRow, legacy map[string][]Measurement) (namespaceMigration, bool, []Measurement, error) {
	if strings.HasPrefix(row.namespace, encodedNamespacePrefix) {
		return namespaceMigration{}, false, nil, nil
	}
	matches := legacy[measurementKey(row.namespace, row.table)]
	if len(matches) == 0 {
		return namespaceMigration{}, false, nil, nil
	}
	if len(matches) > 1 {
		return namespaceMigration{}, false, matches,
			fmt.Errorf("legacy Iceberg namespace %s maps to more than one current Arc database; refusing an ambiguous migration", row.namespace)
	}
	measurement := matches[0]
	namespace, err := namespaceIdentifier(e.nsPrefix, measurement.Database)
	if err != nil {
		return namespaceMigration{}, false, matches, err
	}
	// A row with no metadata location cannot be loaded at either key. Refuse rather than rekey it:
	// the rekey would succeed, the measurement would be unblocked, and every later pass would fail
	// the #637 load gate instead of naming the broken row once here.
	if !row.metadataLocation.Valid || row.metadataLocation.String == "" {
		return namespaceMigration{}, false, matches,
			fmt.Errorf("legacy Iceberg catalog row has no metadata location, so it cannot be loaded under either namespace key")
	}
	return namespaceMigration{
		database:     measurement.Database,
		measurement:  measurement.Measurement,
		oldNamespace: row.namespace,
		newNamespace: catalogNamespaceKey(namespace),
	}, true, nil, nil
}

// catalogNamespaceKey is the key iceberg-go's SQL catalog stores for a namespace: the plain
// dot-joined string, or a JSON encoding when any component contains a dot (which would otherwise
// be ambiguous with a multi-component namespace).
func catalogNamespaceKey(namespace table.Identifier) string {
	legacy := strings.Join(namespace, ".")
	if !strings.Contains(legacy, encodedNamespacePrefix) && !hasDottedNamespacePart(namespace) {
		return legacy
	}
	encoded, _ := json.Marshal(namespace) // string slices cannot fail to marshal
	return encodedNamespacePrefix + string(encoded)
}

// decodeCatalogNamespace reverses catalogNamespaceKey for an encoded key, accepting only the
// canonical encoding so a hand-edited or truncated key is not mistaken for one of ours.
func decodeCatalogNamespace(namespace string) (table.Identifier, bool) {
	if !strings.HasPrefix(namespace, encodedNamespacePrefix) {
		return nil, false
	}
	var parts table.Identifier
	encoded := strings.TrimPrefix(namespace, encodedNamespacePrefix)
	if err := json.Unmarshal([]byte(encoded), &parts); err != nil || len(parts) == 0 {
		return nil, false
	}
	canonical, err := json.Marshal(parts)
	return parts, err == nil && string(canonical) == encoded
}

func namespaceBelongsToPrefix(parts table.Identifier, prefix string) bool {
	return len(parts) > 0 && strings.HasPrefix(parts[0], prefix+"_")
}

func hasDottedNamespacePart(parts table.Identifier) bool {
	for _, part := range parts {
		if strings.Contains(part, ".") {
			return true
		}
	}
	return false
}

// rekeyNamespaceTable moves one catalog row, and its namespace properties, from the legacy dotted
// key to the encoded one. Nothing on disk is touched; see MigrateDottedNamespaces for why.
//
// Both statements run in one transaction so a crash cannot leave the properties and the table row
// under different namespaces. The metadata_location is part of the WHERE clause and the update is
// required to affect exactly one row, so a row another process changed in the meantime is refused
// rather than overwritten.
func (e *Exporter) rekeyNamespaceTable(ctx context.Context, migration namespaceMigration) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Iceberg catalog namespace migration: %w", err)
	}
	defer tx.Rollback()

	var targetExists int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		e.catalogName, migration.newNamespace, migration.measurement).Scan(&targetExists); err != nil {
		return fmt.Errorf("check target Iceberg table: %w", err)
	}
	if targetExists != 0 {
		return fmt.Errorf("target Iceberg table %s.%s already exists, so rekeying the legacy row would collide with it",
			migration.newNamespace, migration.measurement)
	}
	if err := e.moveNamespaceProperties(ctx, tx, migration.oldNamespace, migration.newNamespace); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE iceberg_tables SET table_namespace = ?
		 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		migration.newNamespace, e.catalogName, migration.oldNamespace, migration.measurement)
	if err != nil {
		return fmt.Errorf("rekey Iceberg table namespace: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm Iceberg table namespace update: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("Iceberg table row changed during migration (%d rows updated, want 1); refusing to update it", changed)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Iceberg catalog namespace migration: %w", err)
	}
	return nil
}

type namespaceProperty struct {
	key   string
	value sql.NullString
}

func (e *Exporter) moveNamespaceProperties(ctx context.Context, tx *sql.Tx, oldNamespace, newNamespace string) error {
	oldProperties, err := namespaceProperties(ctx, tx, e.catalogName, oldNamespace)
	if err != nil {
		return err
	}
	newProperties, err := namespaceProperties(ctx, tx, e.catalogName, newNamespace)
	if err != nil {
		return err
	}
	for _, old := range oldProperties {
		if current, exists := newProperties[old.key]; exists {
			if current.value != old.value {
				return fmt.Errorf("Iceberg namespace property %s conflicts in the target namespace", old.key)
			}
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`,
				e.catalogName, oldNamespace, old.key); err != nil {
				return fmt.Errorf("merge legacy Iceberg namespace property %s: %w", old.key, err)
			}
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE iceberg_namespace_properties SET namespace = ?
			 WHERE catalog_name = ? AND namespace = ? AND property_key = ?`,
			newNamespace, e.catalogName, oldNamespace, old.key); err != nil {
			return fmt.Errorf("move legacy Iceberg namespace property %s: %w", old.key, err)
		}
	}
	return nil
}

func namespaceProperties(ctx context.Context, tx *sql.Tx, catalogName, namespace string) (map[string]namespaceProperty, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT property_key, property_value FROM iceberg_namespace_properties
		 WHERE catalog_name = ? AND namespace = ?`, catalogName, namespace)
	if err != nil {
		return nil, fmt.Errorf("read Iceberg namespace properties: %w", err)
	}
	defer rows.Close()
	properties := make(map[string]namespaceProperty)
	for rows.Next() {
		var property namespaceProperty
		if err := rows.Scan(&property.key, &property.value); err != nil {
			return nil, fmt.Errorf("read Iceberg namespace property: %w", err)
		}
		properties[property.key] = property
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Iceberg namespace properties: %w", err)
	}
	return properties, nil
}
