package iceberg

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/apache/iceberg-go/table"
)

const encodedNamespacePrefix = "__iceberg_namespace_v1__:"

type namespaceTableRow struct {
	namespace        string
	table            string
	metadataLocation sql.NullString
}

type namespaceMigration struct {
	database         string
	measurement      string
	identifier       table.Identifier
	oldNamespace     string
	newNamespace     string
	catalogNamespace string
	metadataLocation string
}

// ConfigureNamespaceMigrationDryRun sets whether the dotted-namespace migration only reports
// its plan or applies it. Call before starting the scheduler. Dry-run is the safe default.
func (e *Exporter) ConfigureNamespaceMigrationDryRun(dryRun bool) {
	e.namespaceMigrationDryRun = dryRun
}

// MigrateDottedNamespaces finds legacy catalog rows that correspond to current Arc measurements.
// A failed or dry-run migration returns the affected measurements in blocked so the scheduler
// will not create a second table beside the still-readable legacy table.
func (e *Exporter) MigrateDottedNamespaces(ctx context.Context, measurements []Measurement) (blocked map[string]struct{}, err error) {
	blocked = make(map[string]struct{})
	if e.db == nil || e.backend == nil {
		return blocked, nil
	}

	legacyMeasurements := make(map[string][]Measurement)
	for _, measurement := range measurements {
		if !strings.ContainsAny(measurement.Database, "./") {
			continue
		}
		ident := e.tableIdent(measurement.Database, measurement.Measurement)
		if len(ident) < 2 {
			continue
		}
		oldNamespace := e.nsPrefix + "_" + strings.ReplaceAll(measurement.Database, "/", ".")
		newNamespace := catalogNamespaceKey(ident[:len(ident)-1])
		if oldNamespace == newNamespace {
			continue
		}
		key := oldNamespace + "\x00" + measurement.Measurement
		legacyMeasurements[key] = append(legacyMeasurements[key], measurement)
	}

	rows, err := e.namespaceTableRows(ctx)
	if err != nil {
		return blocked, err
	}
	for _, row := range rows {
		migration, matches, affected, err := e.namespaceMigrationForRow(row, legacyMeasurements)
		if err != nil {
			for _, measurement := range affected {
				blocked[measurement.Database+"\x00"+measurement.Measurement] = struct{}{}
			}
			e.logger.Error().Err(err).Str("namespace", row.namespace).Str("table", row.table).
				Msg("Iceberg dotted-namespace migration could not be planned; leaving the legacy table readable")
			continue
		}
		if !matches {
			continue
		}

		measurementKey := migration.database + "\x00" + migration.measurement
		blocked[measurementKey] = struct{}{}
		if e.namespaceMigrationDryRun {
			if err := e.checkNamespaceMigrationPlan(ctx, migration); err != nil {
				e.logger.Error().Err(err).
					Str("database", migration.database).
					Str("measurement", migration.measurement).
					Msg("Iceberg dotted-namespace migration dry-run found an unsafe table; no changes made")
				continue
			}
			e.logger.Info().
				Str("database", migration.database).
				Str("measurement", migration.measurement).
				Str("from_namespace", migration.oldNamespace).
				Str("to_namespace", migration.newNamespace).
				Msg("Iceberg dotted-namespace migration planned; no catalog or warehouse changes made")
			continue
		}
		if err := e.applyNamespaceMigration(ctx, migration); err != nil {
			e.logger.Error().Err(err).
				Str("database", migration.database).
				Str("measurement", migration.measurement).
				Str("from_namespace", migration.oldNamespace).
				Str("to_namespace", migration.newNamespace).
				Msg("Iceberg dotted-namespace migration failed; the measurement remains blocked for this pass")
			continue
		}
		delete(blocked, measurementKey)
	}
	return blocked, nil
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

func (e *Exporter) namespaceMigrationForRow(row namespaceTableRow, legacy map[string][]Measurement) (namespaceMigration, bool, []Measurement, error) {
	if strings.HasPrefix(row.namespace, encodedNamespacePrefix) {
		parts, ok := decodeCatalogNamespace(row.namespace)
		if !ok || !namespaceBelongsToPrefix(parts, e.nsPrefix) || !hasDottedNamespacePart(parts) {
			return namespaceMigration{}, false, nil, nil
		}
		database, ok := databaseFromNamespace(parts, e.nsPrefix)
		if !ok {
			return namespaceMigration{}, false, nil, nil
		}
		oldNamespace := strings.Join(parts, ".")
		newNamespace := row.namespace
		if oldNamespace == newNamespace {
			return namespaceMigration{}, false, nil, nil
		}
		if !row.metadataLocation.Valid || row.metadataLocation.String == "" {
			return namespaceMigration{}, false, []Measurement{{Database: database, Measurement: row.table}},
				fmt.Errorf("catalog row has no metadata location")
		}
		if _, _, err := relocatedMetadataLocation(row.metadataLocation.String, oldNamespace, newNamespace, row.table); err != nil {
			return namespaceMigration{}, false, []Measurement{{Database: database, Measurement: row.table}}, err
		}
		if metadataHasNamespaceDirectory(row.metadataLocation.String, newNamespace, row.table) {
			return namespaceMigration{}, false, nil, nil
		}
		return namespaceMigration{
			database: database, measurement: row.table,
			identifier:   append(append(table.Identifier(nil), parts...), row.table),
			oldNamespace: oldNamespace, newNamespace: newNamespace, catalogNamespace: row.namespace,
			metadataLocation: row.metadataLocation.String,
		}, true, nil, nil
	}

	matches := legacy[row.namespace+"\x00"+row.table]
	if len(matches) == 0 {
		return namespaceMigration{}, false, nil, nil
	}
	if len(matches) > 1 {
		return namespaceMigration{}, false, matches,
			fmt.Errorf("legacy namespace %q maps to more than one current Arc database; refusing an ambiguous migration", row.namespace)
	}
	measurement := matches[0]
	ident := e.tableIdent(measurement.Database, measurement.Measurement)
	newNamespace := catalogNamespaceKey(ident[:len(ident)-1])
	if row.namespace == newNamespace {
		return namespaceMigration{}, false, nil, nil
	}
	if !row.metadataLocation.Valid || row.metadataLocation.String == "" {
		return namespaceMigration{}, false, matches, fmt.Errorf("catalog row has no metadata location")
	}
	if _, _, err := relocatedMetadataLocation(row.metadataLocation.String, row.namespace, newNamespace, row.table); err != nil {
		return namespaceMigration{}, false, matches, err
	}
	if metadataHasNamespaceDirectory(row.metadataLocation.String, newNamespace, row.table) {
		return namespaceMigration{}, false, nil, nil
	}
	return namespaceMigration{
		database: measurement.Database, measurement: measurement.Measurement,
		identifier: ident, oldNamespace: row.namespace, newNamespace: newNamespace, catalogNamespace: row.namespace,
		metadataLocation: row.metadataLocation.String,
	}, true, nil, nil
}

func catalogNamespaceKey(namespace table.Identifier) string {
	legacy := strings.Join(namespace, ".")
	if !strings.Contains(legacy, encodedNamespacePrefix) && !hasDottedNamespacePart(namespace) {
		return legacy
	}
	encoded, _ := json.Marshal(namespace) // string slices cannot fail to marshal
	return encodedNamespacePrefix + string(encoded)
}

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

func databaseFromNamespace(parts table.Identifier, prefix string) (string, bool) {
	if !namespaceBelongsToPrefix(parts, prefix) {
		return "", false
	}
	first := strings.TrimPrefix(parts[0], prefix+"_")
	if first == "" {
		return "", false
	}
	switch len(parts) {
	case 1:
		return first, true
	case 2:
		if parts[1] == "" || strings.ContainsAny(parts[1], "/.") || strings.Contains(first, "/") {
			return "", false
		}
		return first + "/" + parts[1], true
	default:
		return "", false
	}
}

func relocatedMetadataLocation(metadataLocation, oldNamespace, newNamespace, tableName string) (metadataURI, tableURI string, err error) {
	u, err := url.Parse(metadataLocation)
	if err != nil {
		return "", "", fmt.Errorf("parse table metadata location %q: %w", metadataLocation, err)
	}
	if u.Scheme == "" {
		return "", "", fmt.Errorf("table metadata location %q has no URI scheme", metadataLocation)
	}
	segments := strings.Split(u.Path, "/")
	oldDir, newDir := oldNamespace+".db", newNamespace+".db"
	foundOld, foundNew := -1, -1
	for i, segment := range segments {
		if segment == oldDir {
			if foundOld >= 0 {
				return "", "", fmt.Errorf("metadata location contains multiple legacy namespace directories")
			}
			foundOld = i
		}
		if segment == newDir {
			if foundNew >= 0 {
				return "", "", fmt.Errorf("metadata location contains multiple target namespace directories")
			}
			foundNew = i
		}
	}
	index := foundOld
	if index >= 0 {
		segments[index] = newDir
	} else if foundNew >= 0 {
		index = foundNew // a prior interrupted attempt may already point into the target tree
	} else {
		return "", "", fmt.Errorf("metadata location is outside the expected legacy and target warehouse paths")
	}
	if index+1 >= len(segments) || segments[index+1] != tableName || index+2 >= len(segments) || segments[index+2] != "metadata" {
		return "", "", fmt.Errorf("metadata location does not match namespace/table/metadata layout")
	}
	metadataURL := *u
	metadataURL.Path = strings.Join(segments, "/")
	metadataURL.RawPath = ""
	tableURL := metadataURL
	tableURL.Path = strings.Join(segments[:index+2], "/")
	return metadataURL.String(), tableURL.String(), nil
}

func metadataHasNamespaceDirectory(metadataLocation, namespace, tableName string) bool {
	u, err := url.Parse(metadataLocation)
	if err != nil || u.Scheme == "" {
		return false
	}
	segments := strings.Split(u.Path, "/")
	for i, segment := range segments {
		if segment == namespace+".db" && i+2 < len(segments) && segments[i+1] == tableName && segments[i+2] == "metadata" {
			return true
		}
	}
	return false
}

func (e *Exporter) checkNamespaceMigrationPlan(ctx context.Context, migration namespaceMigration) error {
	targetMetadata, _, err := relocatedMetadataLocation(
		migration.metadataLocation, migration.oldNamespace, migration.newNamespace, migration.measurement)
	if err != nil {
		return err
	}
	oldMetadataKey, ok := e.warehouseRelKey(migration.metadataLocation)
	if !ok {
		return fmt.Errorf("legacy metadata location is outside the configured Iceberg warehouse or storage root")
	}
	newMetadataKey, ok := e.warehouseRelKey(targetMetadata)
	if !ok {
		return fmt.Errorf("target metadata location is outside the configured Iceberg warehouse or storage root")
	}
	return e.copyAndVerifyTableTree(ctx, path.Dir(path.Dir(oldMetadataKey)), path.Dir(path.Dir(newMetadataKey)))
}

func (e *Exporter) applyNamespaceMigration(ctx context.Context, migration namespaceMigration) error {
	targetMetadata, targetTable, err := relocatedMetadataLocation(
		migration.metadataLocation, migration.oldNamespace, migration.newNamespace, migration.measurement)
	if err != nil {
		return err
	}
	oldMetadataKey, ok := e.warehouseRelKey(migration.metadataLocation)
	if !ok {
		return fmt.Errorf("legacy metadata location is outside the configured Iceberg warehouse or storage root")
	}
	newMetadataKey, ok := e.warehouseRelKey(targetMetadata)
	if !ok {
		return fmt.Errorf("target metadata location is outside the configured Iceberg warehouse or storage root")
	}
	oldTablePrefix := path.Dir(path.Dir(oldMetadataKey))
	newTablePrefix := path.Dir(path.Dir(newMetadataKey))
	if err := e.copyAndVerifyTableTree(ctx, oldTablePrefix, newTablePrefix); err != nil {
		return err
	}

	if migration.catalogNamespace != migration.newNamespace {
		if err := e.rekeyNamespaceTable(ctx, migration); err != nil {
			return err
		}
	}
	_, _, err = e.catalog.CommitTable(ctx, migration.identifier, nil,
		[]table.Update{table.NewSetLocationUpdate(targetTable)})
	if err != nil {
		return fmt.Errorf("commit Iceberg table location update: %w", err)
	}
	migrated, err := e.catalog.LoadTable(ctx, migration.identifier)
	if err != nil {
		return fmt.Errorf("reload migrated Iceberg table: %w", err)
	}
	if !metadataHasNamespaceDirectory(migrated.MetadataLocation(), migration.newNamespace, migration.measurement) {
		return fmt.Errorf("migrated catalog row still points outside its target namespace directory")
	}
	if _, _, err := relocatedMetadataLocation(migrated.MetadataLocation(), migration.newNamespace, migration.newNamespace, migration.measurement); err != nil {
		return fmt.Errorf("verify migrated Iceberg metadata location: %w", err)
	}
	if !e.writeVersionHint(ctx, migrated) {
		return fmt.Errorf("publish migrated Iceberg version-hint files; will retry on the next reconcile pass")
	}
	e.logger.Info().
		Str("database", migration.database).
		Str("measurement", migration.measurement).
		Str("from_namespace", migration.oldNamespace).
		Str("to_namespace", migration.newNamespace).
		Msg("Migrated Iceberg dotted namespace; legacy warehouse files retained for snapshot history")
	return nil
}

func (e *Exporter) copyAndVerifyTableTree(ctx context.Context, oldPrefix, newPrefix string) error {
	oldPrefix = strings.TrimSuffix(oldPrefix, "/")
	newPrefix = strings.TrimSuffix(newPrefix, "/")
	if oldPrefix == "" || newPrefix == "" || oldPrefix == newPrefix {
		return fmt.Errorf("invalid legacy or target table path")
	}
	objects, err := e.backend.List(ctx, oldPrefix+"/")
	if err != nil {
		return fmt.Errorf("list legacy Iceberg table files: %w", err)
	}
	if len(objects) == 0 {
		return fmt.Errorf("legacy Iceberg table directory contains no objects")
	}
	sort.Strings(objects)
	for _, source := range objects {
		if !strings.HasPrefix(source, oldPrefix+"/") {
			return fmt.Errorf("backend returned object %q outside legacy table prefix", source)
		}
		relative, err := relativeTableObjectPath(source, oldPrefix)
		if err != nil {
			return err
		}
		destination := path.Join(newPrefix, relative)
		sourceSize, err := e.backend.StatFile(ctx, source)
		if err != nil || sourceSize < 0 {
			if err == nil {
				err = fmt.Errorf("object does not exist")
			}
			return fmt.Errorf("stat legacy Iceberg object %q: %w", source, err)
		}
		targetSize, err := e.backend.StatFile(ctx, destination)
		if err != nil {
			return fmt.Errorf("stat target Iceberg object %q: %w", destination, err)
		}
		if targetSize >= 0 {
			if targetSize != sourceSize {
				return fmt.Errorf("target Iceberg object %q conflicts with source size (%d != %d)", destination, targetSize, sourceSize)
			}
			sourceHash, err := e.hashObject(ctx, source)
			if err != nil {
				return err
			}
			targetHash, err := e.hashObject(ctx, destination)
			if err != nil {
				return err
			}
			if sourceHash != targetHash {
				return fmt.Errorf("target Iceberg object %q conflicts with legacy contents", destination)
			}
			continue
		}
		if e.namespaceMigrationDryRun {
			continue
		}
		if err := e.copyObject(ctx, source, destination, sourceSize); err != nil {
			return err
		}
		targetSize, err = e.backend.StatFile(ctx, destination)
		if err != nil || targetSize != sourceSize {
			if err == nil {
				err = fmt.Errorf("copied size %d does not match source size %d", targetSize, sourceSize)
			}
			return fmt.Errorf("verify copied Iceberg object %q: %w", destination, err)
		}
		sourceHash, err := e.hashObject(ctx, source)
		if err != nil {
			return err
		}
		targetHash, err := e.hashObject(ctx, destination)
		if err != nil {
			return err
		}
		if sourceHash != targetHash {
			return fmt.Errorf("copied Iceberg object %q failed SHA-256 verification", destination)
		}
	}
	return nil
}

func relativeTableObjectPath(source, tablePrefix string) (string, error) {
	prefix := strings.TrimSuffix(tablePrefix, "/") + "/"
	if !strings.HasPrefix(source, prefix) {
		return "", fmt.Errorf("backend returned object %q outside legacy table prefix", source)
	}
	relative := strings.TrimPrefix(source, prefix)
	cleaned := path.Clean(relative)
	if relative == "" || path.IsAbs(relative) || cleaned != relative || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("backend returned unsafe object path %q under legacy table", source)
	}
	return relative, nil
}

func (e *Exporter) copyObject(ctx context.Context, source, destination string, size int64) error {
	reader, writer := io.Pipe()
	readDone := make(chan error, 1)
	go func() {
		err := e.backend.ReadTo(ctx, source, writer)
		_ = writer.CloseWithError(err)
		readDone <- err
	}()
	writeErr := e.backend.WriteReader(ctx, destination, reader, size)
	if writeErr != nil {
		_ = reader.CloseWithError(writeErr)
	} else {
		_ = reader.Close()
	}
	readErr := <-readDone
	if writeErr != nil {
		return fmt.Errorf("copy Iceberg object %q to %q: %w", source, destination, writeErr)
	}
	if readErr != nil {
		return fmt.Errorf("read legacy Iceberg object %q: %w", source, readErr)
	}
	return nil
}

func (e *Exporter) hashObject(ctx context.Context, key string) (string, error) {
	h := sha256.New()
	if err := e.backend.ReadTo(ctx, key, h); err != nil {
		return "", fmt.Errorf("hash Iceberg object %q: %w", key, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

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
		return fmt.Errorf("target Iceberg table %s.%s already exists", migration.newNamespace, migration.measurement)
	}
	if err := e.moveNamespaceProperties(ctx, tx, migration.oldNamespace, migration.newNamespace); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE iceberg_tables SET table_namespace = ?
		 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?`,
		migration.newNamespace, e.catalogName, migration.catalogNamespace, migration.measurement, migration.metadataLocation)
	if err != nil {
		return fmt.Errorf("rekey Iceberg table namespace: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm Iceberg table namespace update: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("Iceberg table row changed during migration; refusing to update it")
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
				return fmt.Errorf("Iceberg namespace property %q conflicts in the target namespace", old.key)
			}
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`,
				e.catalogName, oldNamespace, old.key); err != nil {
				return fmt.Errorf("merge legacy Iceberg namespace property %q: %w", old.key, err)
			}
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE iceberg_namespace_properties SET namespace = ?
			 WHERE catalog_name = ? AND namespace = ? AND property_key = ?`,
			newNamespace, e.catalogName, oldNamespace, old.key); err != nil {
			return fmt.Errorf("move legacy Iceberg namespace property %q: %w", old.key, err)
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
