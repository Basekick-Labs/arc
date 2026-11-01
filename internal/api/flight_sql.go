package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/fips"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type FlightSQLServerConfig struct {
	Host        string
	Port        int
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string
}

type FlightSQLServer struct {
	bridge            *fiber.App
	bridgeLn          net.Listener
	flight            flight.Server
	service           *flightSQLService
	config            FlightSQLServerConfig
	logger            zerolog.Logger
	serveDone         chan error
	startMu           sync.Mutex
	started           bool
	shutdownMu        sync.Mutex
	shutdownDone      chan struct{}
	shutdownErr       error
	shutdownRequested bool
}

type flightSQLService struct {
	flightsql.BaseServer
	queryHandler *QueryHandler
	internalKey  string
	baseURL      string
	client       *http.Client
}

type flightSQLIdentity struct {
	token string
}

type flightSQLAuth struct {
	manager *auth.AuthManager
}

type flightStatement struct {
	SQL string `json:"sql"`
}

type flightQueryResponse struct {
	Success bool                `json:"success"`
	Columns []string            `json:"columns"`
	Data    [][]json.RawMessage `json:"data"`
	Error   string              `json:"error"`
}

func NewFlightSQLServer(queryHandler *QueryHandler, config FlightSQLServerConfig, logger zerolog.Logger) (*FlightSQLServer, error) {
	if queryHandler == nil {
		return nil, fmt.Errorf("Flight SQL requires a query handler")
	}
	if config.Port < 1 || config.Port > 65535 {
		return nil, fmt.Errorf("Flight SQL port must be between 1 and 65535")
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate internal Flight SQL credential: %w", err)
	}

	bridge := fiber.New(fiber.Config{DisableStartupMessage: true})
	internalKey := hex.EncodeToString(secret)
	bridge.Use(func(c *fiber.Ctx) error {
		if subtle.ConstantTimeCompare([]byte(c.Get("X-Arc-Flight-Internal-Key")), []byte(internalKey)) != 1 {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		return c.Next()
	})
	bridge.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: queryHandler.authManager}))
	queryHandler.RegisterRoutes(bridge)

	transport := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 4}
	service := &flightSQLService{
		queryHandler: queryHandler,
		internalKey:  internalKey,
		client:       &http.Client{Transport: transport},
	}

	var options []grpc.ServerOption
	if config.TLSEnabled {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Flight SQL TLS keypair: %w", err)
		}
		tlsConfig := fips.HardenTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2"},
		})
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	var middleware []flight.ServerMiddleware
	if queryHandler.authManager != nil {
		middleware = append(middleware, flight.CreateServerBasicAuthMiddleware(&flightSQLAuth{manager: queryHandler.authManager}))
	}
	flightServer := flight.NewServerWithMiddleware(middleware, options...)
	flightServer.RegisterFlightService(flightsql.NewFlightServer(service))

	return &FlightSQLServer{
		bridge:    bridge,
		flight:    flightServer,
		service:   service,
		config:    config,
		logger:    logger,
		serveDone: make(chan error, 1),
	}, nil
}

func (s *FlightSQLServer) Start() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.started {
		return fmt.Errorf("Flight SQL server is already started")
	}
	if s.shutdownRequested {
		return fmt.Errorf("Flight SQL server is shutting down")
	}

	bridgeLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for internal Flight SQL query bridge: %w", err)
	}
	s.bridgeLn = bridgeLn
	s.service.baseURL = "http://" + bridgeLn.Addr().String()
	go func() {
		if err := s.bridge.Listener(bridgeLn); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Error().Err(err).Msg("Internal Flight SQL query bridge stopped")
		}
	}()

	_, addr := ListenAddr(s.config.Host, s.config.Port)
	if err := s.flight.Init(addr); err != nil {
		_ = s.bridge.Shutdown()
		_ = bridgeLn.Close()
		s.bridgeLn = nil
		return fmt.Errorf("listen for Flight SQL on %s: %w", addr, err)
	}
	s.started = true
	go func() {
		err := s.flight.Serve()
		s.serveDone <- err
		close(s.serveDone)
	}()

	protocol := "grpc"
	if s.config.TLSEnabled {
		protocol = "grpcs"
	}
	s.logger.Info().Str("address", addr).Str("protocol", protocol).Msg("Arrow Flight SQL server started")
	return nil
}

func (s *FlightSQLServer) Shutdown(ctx context.Context) error {
	s.startMu.Lock()
	s.shutdownRequested = true
	started := s.started
	s.startMu.Unlock()
	if !started {
		return nil
	}

	s.shutdownMu.Lock()
	if s.shutdownDone == nil {
		s.shutdownDone = make(chan struct{})
		go s.finishShutdown()
	}
	done := s.shutdownDone
	s.shutdownMu.Unlock()

	select {
	case <-done:
		s.shutdownMu.Lock()
		err := s.shutdownErr
		s.shutdownMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *FlightSQLServer) finishShutdown() {
	var shutdownErr error
	s.flight.Shutdown()
	if err := s.bridge.Shutdown(); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown internal Flight SQL query bridge: %w", err))
	}
	if s.bridgeLn != nil {
		_ = s.bridgeLn.Close()
	}
	if transport, ok := s.service.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	if err := <-s.serveDone; err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("Flight SQL server stopped: %w", err))
	}

	s.shutdownMu.Lock()
	s.shutdownErr = shutdownErr
	close(s.shutdownDone)
	s.shutdownMu.Unlock()
}

func (a *flightSQLAuth) Validate(username, password string) (string, error) {
	token := password
	if token == "" {
		token = username
	}
	if token == "" || a.manager.VerifyToken(token) == nil {
		return "", status.Error(codes.Unauthenticated, "invalid Arc token")
	}
	return token, nil
}

func (a *flightSQLAuth) IsValid(token string) (interface{}, error) {
	if token == "" || a.manager.VerifyToken(token) == nil {
		return nil, status.Error(codes.Unauthenticated, "invalid Arc token")
	}
	return &flightSQLIdentity{token: token}, nil
}

func (s *flightSQLService) token(ctx context.Context) string {
	identity, _ := flight.AuthFromContext(ctx).(*flightSQLIdentity)
	if identity == nil {
		return ""
	}
	return identity.token
}

func (s *flightSQLService) GetFlightInfoStatement(_ context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(query.GetTransactionId()) != 0 {
		return nil, status.Error(codes.Unimplemented, "Flight SQL transactions are not supported")
	}
	if err := ValidateSQLRequest(query.GetQuery()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	handle, err := json.Marshal(flightStatement{SQL: query.GetQuery()})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode statement ticket: %v", err)
	}
	ticket, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode statement ticket: %v", err)
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *flightSQLService) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var statement flightStatement
	if err := json.Unmarshal(ticket.GetStatementHandle(), &statement); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid statement ticket")
	}
	if err := ValidateSQLRequest(statement.SQL); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}

	path := "/api/v1/query/arrow"
	if isFlightShowSQL(statement.SQL) {
		path = "/api/v1/query"
	}
	resp, err := s.query(ctx, path, statement.SQL, s.token(ctx))
	if err != nil {
		return nil, nil, err
	}
	if path == "/api/v1/query" {
		defer resp.Body.Close()
		return s.showResponse(ctx, resp)
	}

	reader, err := ipc.NewReader(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, nil, status.Errorf(codes.Internal, "read Arrow query response: %v", err)
	}
	schema := reader.Schema()
	stream := make(chan flight.StreamChunk)
	go func() {
		flight.StreamChunksFromReader(ctx, reader, stream)
		_ = resp.Body.Close()
	}()
	return schema, stream, nil
}

func (s *flightSQLService) showResponse(ctx context.Context, resp *http.Response) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var response flightQueryResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&response); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "decode SHOW response: %v", err)
	}
	if !response.Success {
		return nil, nil, status.Error(codes.Internal, response.Error)
	}
	builder, err := showRecordBuilder(response.Columns)
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	defer builder.Release()
	for _, row := range response.Data {
		if err := appendShowRow(builder, response.Columns, row); err != nil {
			return nil, nil, status.Errorf(codes.Internal, "convert SHOW result: %v", err)
		}
	}
	batch := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
	batch.Release()
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "build SHOW result: %v", err)
	}
	schema := reader.Schema()
	stream := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(ctx, reader, stream)
	return schema, stream, nil
}

func showRecordBuilder(columns []string) (*array.RecordBuilder, error) {
	switch strings.Join(columns, ",") {
	case "database":
		return array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "database", Type: arrow.BinaryTypes.String}}, nil)), nil
	case "database,tier":
		return array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
			{Name: "database", Type: arrow.BinaryTypes.String},
			{Name: "tier", Type: arrow.BinaryTypes.String},
		}, nil)), nil
	case "database,table_name,storage_path,file_count,total_size_mb":
		return array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
			{Name: "database", Type: arrow.BinaryTypes.String},
			{Name: "table_name", Type: arrow.BinaryTypes.String},
			{Name: "storage_path", Type: arrow.BinaryTypes.String},
			{Name: "file_count", Type: arrow.PrimitiveTypes.Int64},
			{Name: "total_size_mb", Type: arrow.PrimitiveTypes.Float64},
		}, nil)), nil
	default:
		return nil, fmt.Errorf("unsupported SHOW result columns: %s", strings.Join(columns, ","))
	}
}

func appendShowRow(builder *array.RecordBuilder, columns []string, row []json.RawMessage) error {
	if len(row) != len(columns) {
		return fmt.Errorf("got %d values for %d columns", len(row), len(columns))
	}
	for i, column := range columns {
		switch column {
		case "database", "tier", "table_name", "storage_path":
			var value string
			if err := json.Unmarshal(row[i], &value); err != nil {
				return err
			}
			builder.Field(i).(*array.StringBuilder).Append(value)
		case "file_count":
			var value int64
			if err := json.Unmarshal(row[i], &value); err != nil {
				return err
			}
			builder.Field(i).(*array.Int64Builder).Append(value)
		case "total_size_mb":
			var value float64
			if err := json.Unmarshal(row[i], &value); err != nil {
				return err
			}
			builder.Field(i).(*array.Float64Builder).Append(value)
		default:
			return fmt.Errorf("unsupported SHOW column %q", column)
		}
	}
	return nil
}

func isFlightShowSQL(sql string) bool {
	normalised := normalizeSQLForShow(sql)
	return showDatabasesPattern.MatchString(normalised) || showTablesPattern.MatchString(normalised)
}

func (s *flightSQLService) query(ctx context.Context, path, sql, token string) (*http.Response, error) {
	body, err := json.Marshal(QueryRequest{SQL: sql})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode query: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "prepare query: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Arc-Flight-Internal-Key", s.internalKey)
	if token != "" {
		req.Header.Set(fiber.HeaderAuthorization, "Bearer "+token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "Arc query request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		var message struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&message)
		if message.Error == "" {
			message.Error = http.StatusText(resp.StatusCode)
		}
		return nil, status.Error(flightCodeForHTTP(resp.StatusCode), message.Error)
	}
	return resp, nil
}

func flightCodeForHTTP(code int) codes.Code {
	switch code {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

func (s *flightSQLService) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfo(desc, schema_ref.Catalogs), nil
}

func (s *flightSQLService) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	response, err := s.listDatabases(ctx, s.token(ctx))
	if err != nil {
		return nil, nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.Catalogs)
	defer builder.Release()
	for _, database := range response {
		builder.Field(0).(*array.StringBuilder).Append(database)
	}
	return flightRecordStream(ctx, schema_ref.Catalogs, builder)
}

func (s *flightSQLService) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfo(desc, schema_ref.DBSchemas), nil
}

func (s *flightSQLService) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	token := s.token(ctx)
	catalogPattern := compileFlightPattern(cmd.GetCatalog())
	dbSchemaPattern := compileFlightPattern(cmd.GetDBSchemaFilterPattern())
	var databases []string
	if catalog := cmd.GetCatalog(); catalog != nil {
		if _, err := s.listMeasurements(ctx, *catalog, token); err != nil {
			return nil, nil, err
		}
		databases = []string{*catalog}
	} else {
		var err error
		databases, err = s.listDatabases(ctx, token)
		if err != nil {
			return nil, nil, err
		}
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.DBSchemas)
	defer builder.Release()
	for _, database := range databases {
		if !matchesFlightPattern(database, catalogPattern) || !matchesFlightPattern("", dbSchemaPattern) {
			continue
		}
		builder.Field(0).(*array.StringBuilder).Append(database)
		builder.Field(1).(*array.StringBuilder).Append("")
	}
	return flightRecordStream(ctx, schema_ref.DBSchemas, builder)
}

func (s *flightSQLService) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfo(desc, schema), nil
}

func (s *flightSQLService) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	token := s.token(ctx)
	catalogPattern := compileFlightPattern(cmd.GetCatalog())
	dbSchemaPattern := compileFlightPattern(cmd.GetDBSchemaFilterPattern())
	tableNamePattern := compileFlightPattern(cmd.GetTableNameFilterPattern())
	var databases []string
	if catalog := cmd.GetCatalog(); catalog != nil {
		databases = []string{*catalog}
	} else {
		var err error
		databases, err = s.listDatabases(ctx, token)
		if err != nil {
			return nil, nil, err
		}
	}
	tableSchema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		tableSchema = schema_ref.TablesWithIncludedSchema
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, tableSchema)
	defer builder.Release()
	for _, database := range databases {
		if !matchesFlightPattern(database, catalogPattern) || !matchesFlightPattern("", dbSchemaPattern) {
			continue
		}
		measurements, err := s.listMeasurements(ctx, database, token)
		if err != nil {
			return nil, nil, err
		}
		for _, measurement := range measurements {
			if !matchesFlightPattern(measurement, tableNamePattern) || !flightTableTypeMatches(cmd.GetTableTypes()) {
				continue
			}
			builder.Field(0).(*array.StringBuilder).Append(database)
			builder.Field(1).(*array.StringBuilder).Append("")
			builder.Field(2).(*array.StringBuilder).Append(measurement)
			builder.Field(3).(*array.StringBuilder).Append("TABLE")
			if cmd.GetIncludeSchema() {
				schemaBytes, err := s.measurementSchema(ctx, database, measurement, token)
				if err != nil {
					return nil, nil, err
				}
				builder.Field(4).(*array.BinaryBuilder).Append(schemaBytes)
			}
		}
	}
	return flightRecordStream(ctx, tableSchema, builder)
}

func (s *flightSQLService) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfo(desc, schema_ref.TableTypes), nil
}

func (s *flightSQLService) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.TableTypes)
	builder.Field(0).(*array.StringBuilder).Append("TABLE")
	defer builder.Release()
	return flightRecordStream(ctx, schema_ref.TableTypes, builder)
}

func (s *flightSQLService) flightInfo(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	var ticket []byte
	if desc != nil {
		ticket = desc.Cmd
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func flightRecordStream(ctx context.Context, schema *arrow.Schema, builder *array.RecordBuilder) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	batch := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	batch.Release()
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "build Flight SQL result: %v", err)
	}
	stream := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(ctx, reader, stream)
	return schema, stream, nil
}

func (s *flightSQLService) listDatabases(ctx context.Context, token string) ([]string, error) {
	resp, err := s.query(ctx, "/api/v1/query", "SHOW DATABASES", token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result flightQueryResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, status.Errorf(codes.Internal, "decode database listing: %v", err)
	}
	if !result.Success {
		return nil, status.Error(codes.PermissionDenied, result.Error)
	}
	var databases []string
	for _, row := range result.Data {
		if len(row) == 0 {
			continue
		}
		var database string
		if err := json.Unmarshal(row[0], &database); err != nil {
			return nil, status.Errorf(codes.Internal, "decode database name: %v", err)
		}
		databases = append(databases, database)
	}
	return databases, nil
}

func (s *flightSQLService) listMeasurements(ctx context.Context, database, token string) ([]string, error) {
	if err := validateIdentifier(database); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid catalog")
	}
	sql := "SHOW TABLES FROM " + quoteFlightIdentifier(database)
	resp, err := s.query(ctx, "/api/v1/query", sql, token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result flightQueryResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, status.Errorf(codes.Internal, "decode measurement listing: %v", err)
	}
	if !result.Success {
		return nil, status.Errorf(codes.PermissionDenied, "cannot list measurements in catalog %q: %s", database, result.Error)
	}
	var measurements []string
	for _, row := range result.Data {
		if len(row) < 2 {
			continue
		}
		var measurement string
		if err := json.Unmarshal(row[1], &measurement); err != nil {
			return nil, status.Errorf(codes.Internal, "decode measurement name: %v", err)
		}
		measurements = append(measurements, measurement)
	}
	return measurements, nil
}

func (s *flightSQLService) measurementSchema(ctx context.Context, database, measurement, token string) ([]byte, error) {
	if err := validateIdentifier(database); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid catalog")
	}
	sql := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0", quoteFlightIdentifier(database), quoteFlightIdentifier(measurement))
	resp, err := s.query(ctx, "/api/v1/query/arrow", sql, token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	reader, err := ipc.NewReader(resp.Body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read measurement schema: %v", err)
	}
	defer reader.Release()
	return flight.SerializeSchema(reader.Schema(), memory.DefaultAllocator), nil
}

func quoteFlightIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func compileFlightPattern(pattern *string) *regexp.Regexp {
	if pattern == nil {
		return nil
	}
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(*pattern); i++ {
		switch (*pattern)[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '\\':
			i++
			if i == len(*pattern) {
				b.WriteString(`\\`)
			} else {
				b.WriteString(regexp.QuoteMeta((*pattern)[i : i+1]))
			}
		default:
			b.WriteString(regexp.QuoteMeta((*pattern)[i : i+1]))
		}
	}
	b.WriteByte('$')
	return regexp.MustCompile(b.String())
}

func matchesFlightPattern(value string, pattern *regexp.Regexp) bool {
	if pattern == nil {
		return true
	}
	return pattern.MatchString(value)
}

func flightTableTypeMatches(types []string) bool {
	if len(types) == 0 {
		return true
	}
	for _, tableType := range types {
		if strings.EqualFold(tableType, "TABLE") || strings.EqualFold(tableType, "BASE TABLE") {
			return true
		}
	}
	return false
}
