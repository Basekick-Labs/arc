package api

import (
	"context"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// flightSQLMetadataHeader selects how Flight SQL metadata names Arc's
// databases. Without it, or with "catalogs", every metadata call answers as
// it always has: a database is a catalog with one empty-named schema. With
// "schemas", there are no catalogs and each database is a schema, the shape
// tools that qualify names as schema.table need (Tableau's JDBC connection
// cannot select an empty-named schema, and a catalog makes JDBC tools write
// three-part names, which Arc does not parse). The JDBC driver sends any URL
// property it does not know as a call header: x-arc-metadata=schemas.
const flightSQLMetadataHeader = "x-arc-metadata"

const (
	flightSQLMetadataCatalogs = "catalogs"
	flightSQLMetadataSchemas  = "schemas"
)

// flightSQLMetadataModeService answers the metadata calls in the shape the
// x-arc-metadata header selects. Every call without the header, and every
// other call, goes to the embedded service unchanged.
type flightSQLMetadataModeService struct {
	*flightSQLService
	logger zerolog.Logger
}

func newFlightSQLMetadataModeService(service *flightSQLService, logger zerolog.Logger) *flightSQLMetadataModeService {
	return &flightSQLMetadataModeService{flightSQLService: service, logger: logger}
}

// flightSchemasMode reports whether the call asked for the schemas shape. A
// call without the header is never an error.
func flightSchemasMode(ctx context.Context) (bool, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(flightSQLMetadataHeader)
	switch {
	case len(values) == 0:
		return false, nil
	case len(values) > 1:
		return false, status.Error(codes.InvalidArgument, "the x-arc-metadata header was sent more than once")
	}
	switch values[0] {
	case flightSQLMetadataSchemas:
		return true, nil
	case flightSQLMetadataCatalogs:
		return false, nil
	default:
		return false, status.Errorf(codes.InvalidArgument, "invalid x-arc-metadata header %q: use %q or %q", values[0], flightSQLMetadataSchemas, flightSQLMetadataCatalogs)
	}
}

func (s *flightSQLMetadataModeService) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if _, err := flightSchemasMode(ctx); err != nil {
		return nil, err
	}
	return s.flightSQLService.GetFlightInfoCatalogs(ctx, desc)
}

// DoGetCatalogs answers no catalogs in the schemas shape.
func (s *flightSQLMetadataModeService) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schemas, err := flightSchemasMode(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !schemas {
		return s.flightSQLService.DoGetCatalogs(ctx)
	}
	s.logger.Debug().Str("call", "GetCatalogs").Msg("Flight SQL metadata in the schemas shape")
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.Catalogs)
	defer builder.Release()
	return flightRecordStream(ctx, schema_ref.Catalogs, builder)
}

func (s *flightSQLMetadataModeService) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if _, err := flightSchemasMode(ctx); err != nil {
		return nil, err
	}
	return s.flightSQLService.GetFlightInfoSchemas(ctx, cmd, desc)
}

// DoGetDBSchemas lists each database as a schema with no catalog. The
// catalog filter is ignored: Arc has no catalogs, and Tableau sends one it
// remembers from earlier connections.
func (s *flightSQLMetadataModeService) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schemas, err := flightSchemasMode(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !schemas {
		return s.flightSQLService.DoGetDBSchemas(ctx, cmd)
	}
	s.logger.Debug().Str("call", "GetDbSchemas").
		Interface("catalog", cmd.GetCatalog()).Interface("schema_pattern", cmd.GetDBSchemaFilterPattern()).
		Msg("Flight SQL metadata in the schemas shape")
	databases, err := s.schemaDatabases(ctx, cmd.GetDBSchemaFilterPattern(), s.token(ctx))
	if err != nil {
		return nil, nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.DBSchemas)
	defer builder.Release()
	for _, database := range databases {
		builder.Field(0).(*array.StringBuilder).AppendNull()
		builder.Field(1).(*array.StringBuilder).Append(database)
	}
	return flightRecordStream(ctx, schema_ref.DBSchemas, builder)
}

func (s *flightSQLMetadataModeService) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if _, err := flightSchemasMode(ctx); err != nil {
		return nil, err
	}
	return s.flightSQLService.GetFlightInfoTables(ctx, cmd, desc)
}

// DoGetTables lists measurements under their database as the schema, with
// no catalog; the catalog filter is ignored as in DoGetDBSchemas. With
// schemas included, a measurement the token cannot read is left out, as in
// the catalogs shape.
func (s *flightSQLMetadataModeService) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schemas, err := flightSchemasMode(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !schemas {
		return s.flightSQLService.DoGetTables(ctx, cmd)
	}
	s.logger.Debug().Str("call", "GetTables").
		Interface("catalog", cmd.GetCatalog()).Interface("schema_pattern", cmd.GetDBSchemaFilterPattern()).
		Interface("table_pattern", cmd.GetTableNameFilterPattern()).Bool("include_schema", cmd.GetIncludeSchema()).
		Msg("Flight SQL metadata in the schemas shape")
	token := s.token(ctx)
	databases, err := s.schemaDatabases(ctx, cmd.GetDBSchemaFilterPattern(), token)
	if err != nil {
		return nil, nil, err
	}
	tableNamePattern := compileFlightPattern(cmd.GetTableNameFilterPattern())
	tableSchema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		tableSchema = schema_ref.TablesWithIncludedSchema
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, tableSchema)
	defer builder.Release()
	if !flightTableTypeMatches(cmd.GetTableTypes()) {
		return flightRecordStream(ctx, tableSchema, builder)
	}
	for _, database := range databases {
		measurements, err := s.listMeasurements(ctx, database, token)
		if err != nil {
			return nil, nil, err
		}
		for _, measurement := range measurements {
			if !matchesFlightPattern(measurement, tableNamePattern) {
				continue
			}
			// The schema first: a row is all five columns or none.
			var schemaBytes []byte
			if cmd.GetIncludeSchema() {
				schemaBytes, err = s.measurementSchema(ctx, database, measurement, token)
				if err != nil {
					if skipFlightTableSchemaError(ctx, err) {
						continue
					}
					return nil, nil, err
				}
			}
			builder.Field(0).(*array.StringBuilder).AppendNull()
			builder.Field(1).(*array.StringBuilder).Append(database)
			builder.Field(2).(*array.StringBuilder).Append(measurement)
			builder.Field(3).(*array.StringBuilder).Append("TABLE")
			if cmd.GetIncludeSchema() {
				builder.Field(4).(*array.BinaryBuilder).Append(schemaBytes)
			}
		}
	}
	return flightRecordStream(ctx, tableSchema, builder)
}

// schemaDatabases is the databases the token can see whose names match the
// schema pattern. A pattern's "_" matches any one character, so "w0_sales"
// also matches "w0-sales": the database named exactly as the pattern comes
// first, because some clients read only the first match.
func (s *flightSQLMetadataModeService) schemaDatabases(ctx context.Context, pattern *string, token string) ([]string, error) {
	all, err := s.listDatabases(ctx, token)
	if err != nil {
		return nil, err
	}
	re := compileFlightPattern(pattern)
	databases := make([]string, 0, len(all))
	for _, database := range all {
		if matchesFlightPattern(database, re) {
			databases = append(databases, database)
		}
	}
	if pattern != nil {
		exact := *pattern
		sort.SliceStable(databases, func(i, j int) bool {
			return databases[i] == exact && databases[j] != exact
		})
	}
	return databases, nil
}

// Arc has no keys. In the schemas shape the key listings answer empty, since
// Tableau ends the connection on an unimplemented call; without it they stay
// unimplemented, as they always were.

func (s *flightSQLMetadataModeService) GetFlightInfoPrimaryKeys(ctx context.Context, ref flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return s.flightSQLService.GetFlightInfoPrimaryKeys(ctx, ref, desc)
	}
	return s.flightInfo(desc, schema_ref.PrimaryKeys), nil
}

func (s *flightSQLMetadataModeService) DoGetPrimaryKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, nil, err
		}
		return s.flightSQLService.DoGetPrimaryKeys(ctx, ref)
	}
	return flightEmptyStream(ctx, schema_ref.PrimaryKeys)
}

func (s *flightSQLMetadataModeService) GetFlightInfoImportedKeys(ctx context.Context, ref flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return s.flightSQLService.GetFlightInfoImportedKeys(ctx, ref, desc)
	}
	return s.flightInfo(desc, schema_ref.ImportedKeys), nil
}

func (s *flightSQLMetadataModeService) DoGetImportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, nil, err
		}
		return s.flightSQLService.DoGetImportedKeys(ctx, ref)
	}
	return flightEmptyStream(ctx, schema_ref.ImportedKeys)
}

func (s *flightSQLMetadataModeService) GetFlightInfoExportedKeys(ctx context.Context, ref flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return s.flightSQLService.GetFlightInfoExportedKeys(ctx, ref, desc)
	}
	return s.flightInfo(desc, schema_ref.ExportedKeys), nil
}

func (s *flightSQLMetadataModeService) DoGetExportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, nil, err
		}
		return s.flightSQLService.DoGetExportedKeys(ctx, ref)
	}
	return flightEmptyStream(ctx, schema_ref.ExportedKeys)
}

func (s *flightSQLMetadataModeService) GetFlightInfoCrossReference(ctx context.Context, ref flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return s.flightSQLService.GetFlightInfoCrossReference(ctx, ref, desc)
	}
	return s.flightInfo(desc, schema_ref.CrossReference), nil
}

func (s *flightSQLMetadataModeService) DoGetCrossReference(ctx context.Context, ref flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if ok, err := flightSchemasMode(ctx); err != nil || !ok {
		if err != nil {
			return nil, nil, err
		}
		return s.flightSQLService.DoGetCrossReference(ctx, ref)
	}
	return flightEmptyStream(ctx, schema_ref.CrossReference)
}

func flightEmptyStream(ctx context.Context, schema *arrow.Schema) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	return flightRecordStream(ctx, schema, builder)
}
