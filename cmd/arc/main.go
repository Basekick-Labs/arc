package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/telemetry"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog/log"
)

// Version is set at build time
var Version = "dev"

func main() {
	// Check for subcommands before loading full config
	if len(os.Args) > 1 && os.Args[1] == "compact" {
		runCompactSubcommand(os.Args[2:])
		return
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger.Setup(cfg.Log.Level, cfg.Log.Format)
	log.Info().Str("version", Version).Msg("Starting Arc...")

	// Initialize metrics collector
	metrics.Init(logger.Get("metrics"))

	// Initialize timeseries collector with config
	metrics.InitTimeSeriesCollector(
		cfg.Metrics.TimeseriesRetentionMinutes,
		cfg.Metrics.TimeseriesIntervalSeconds,
	)
	log.Info().
		Int("retention_minutes", cfg.Metrics.TimeseriesRetentionMinutes).
		Int("interval_seconds", cfg.Metrics.TimeseriesIntervalSeconds).
		Msg("Timeseries metrics collector initialized")

	// Initialize shutdown coordinator
	shutdownCoordinator := shutdown.New(30*time.Second, logger.Get("shutdown"))

	// Initialize DuckDB
	dbConfig := &database.Config{
		MaxConnections: cfg.Database.MaxConnections,
		MemoryLimit:    cfg.Database.MemoryLimit,
		ThreadCount:    cfg.Database.ThreadCount,
		EnableWAL:      cfg.Database.EnableWAL,
		// S3 configuration for httpfs extension (enables DuckDB to query S3 directly)
		S3Region:    cfg.Storage.S3Region,
		S3AccessKey: cfg.Storage.S3AccessKey,
		S3SecretKey: cfg.Storage.S3SecretKey,
		S3Endpoint:  cfg.Storage.S3Endpoint,
		S3UseSSL:    cfg.Storage.S3UseSSL,
		S3PathStyle: cfg.Storage.S3PathStyle,
	}

	db, err := database.New(dbConfig, logger.Get("database"))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
	}
	shutdownCoordinator.Register("database", db, shutdown.PriorityDatabase)

	// Test query
	log.Info().Msg("Testing DuckDB connection...")
	rows, err := db.Query("SELECT 'Hello from Arc in Go!' as message, version() as duckdb_version")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to execute test query")
	}
	defer rows.Close()

	var message, version string
	if rows.Next() {
		if err := rows.Scan(&message, &version); err != nil {
			log.Fatal().Err(err).Msg("Failed to scan result")
		}
		log.Info().
			Str("message", message).
			Str("duckdb_version", version).
			Msg("Test query successful")
	}

	// Print database stats
	stats := db.Stats()
	log.Info().
		Int("max_open", stats.MaxOpenConnections).
		Int("open", stats.OpenConnections).
		Int("in_use", stats.InUse).
		Int("idle", stats.Idle).
		Msg("Database connection pool stats")

	// Initialize storage backend
	var storageBackend storage.Backend
	switch cfg.Storage.Backend {
	case "local":
		storageBackend, err = storage.NewLocalBackend(cfg.Storage.LocalPath, logger.Get("storage"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize local storage backend")
		}
		shutdownCoordinator.Register("storage", storageBackend, shutdown.PriorityStorage)
		log.Info().
			Str("backend", "local").
			Str("path", cfg.Storage.LocalPath).
			Msg("Storage backend initialized")

	case "s3", "minio":
		s3Config := &storage.S3Config{
			Bucket:    cfg.Storage.S3Bucket,
			Region:    cfg.Storage.S3Region,
			Endpoint:  cfg.Storage.S3Endpoint,
			AccessKey: cfg.Storage.S3AccessKey,
			SecretKey: cfg.Storage.S3SecretKey,
			UseSSL:    cfg.Storage.S3UseSSL,
			PathStyle: cfg.Storage.S3PathStyle,
		}
		storageBackend, err = storage.NewS3Backend(s3Config, logger.Get("storage"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize S3 storage backend")
		}
		shutdownCoordinator.Register("storage", storageBackend, shutdown.PriorityStorage)
		log.Info().
			Str("backend", cfg.Storage.Backend).
			Str("bucket", cfg.Storage.S3Bucket).
			Str("region", cfg.Storage.S3Region).
			Str("endpoint", cfg.Storage.S3Endpoint).
			Msg("Storage backend initialized")

	case "azure", "azblob":
		azureConfig := &storage.AzureBlobConfig{
			ConnectionString:   cfg.Storage.AzureConnectionString,
			AccountName:        cfg.Storage.AzureAccountName,
			AccountKey:         cfg.Storage.AzureAccountKey,
			SASToken:           cfg.Storage.AzureSASToken,
			ContainerName:      cfg.Storage.AzureContainer,
			Endpoint:           cfg.Storage.AzureEndpoint,
			UseManagedIdentity: cfg.Storage.AzureUseManagedIdentity,
		}
		storageBackend, err = storage.NewAzureBlobBackend(azureConfig, logger.Get("storage"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize Azure Blob Storage backend")
		}
		shutdownCoordinator.Register("storage", storageBackend, shutdown.PriorityStorage)
		log.Info().
			Str("backend", cfg.Storage.Backend).
			Str("container", cfg.Storage.AzureContainer).
			Str("account", cfg.Storage.AzureAccountName).
			Msg("Storage backend initialized")

	default:
		log.Fatal().Str("backend", cfg.Storage.Backend).Msg("Unsupported storage backend (use 'local', 's3', 'minio', 'azure', or 'azblob')")
	}

	// Initialize WAL (if enabled)
	var walWriter *wal.Writer
	if cfg.WAL.Enabled {
		// Run WAL recovery FIRST (before creating new WAL writer)
		recovery := wal.NewRecovery(cfg.WAL.Directory, logger.Get("wal"))
		recoveryStats, err := recovery.Recover(context.Background(), func(ctx context.Context, records []map[string]interface{}) error {
			// Re-ingest recovered records through the ingest pipeline
			// For now, just log - full integration requires ArrowBuffer to be created first
			log.Info().Int("records", len(records)).Msg("WAL recovery: replaying records")
			return nil
		})
		if err != nil {
			log.Error().Err(err).Msg("WAL recovery failed")
		} else if recoveryStats.RecoveredFiles > 0 {
			log.Info().
				Int("files", recoveryStats.RecoveredFiles).
				Int("batches", recoveryStats.RecoveredBatches).
				Int("entries", recoveryStats.RecoveredEntries).
				Int("corrupted", recoveryStats.CorruptedEntries).
				Dur("duration", recoveryStats.RecoveryDuration).
				Msg("WAL recovery complete")
		}

		// Create WAL writer AFTER recovery (so new file isn't recovered)
		walWriter, err = wal.NewWriter(&wal.WriterConfig{
			WALDir:       cfg.WAL.Directory,
			SyncMode:     wal.SyncMode(cfg.WAL.SyncMode),
			MaxSizeBytes: int64(cfg.WAL.MaxSizeMB) * 1024 * 1024,
			MaxAge:       time.Duration(cfg.WAL.MaxAgeSeconds) * time.Second,
			Logger:       logger.Get("wal"),
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize WAL writer")
		}
		shutdownCoordinator.Register("wal", walWriter, shutdown.PriorityWAL)

		log.Info().
			Str("directory", cfg.WAL.Directory).
			Str("sync_mode", cfg.WAL.SyncMode).
			Int("max_size_mb", cfg.WAL.MaxSizeMB).
			Int("max_age_seconds", cfg.WAL.MaxAgeSeconds).
			Msg("WAL enabled")
	} else {
		log.Info().Msg("WAL is DISABLED - data durability relies on immediate Parquet flushes")
	}

	// Initialize Arrow buffer (optionally with WAL)
	arrowBuffer := ingest.NewArrowBuffer(&cfg.Ingest, storageBackend, logger.Get("arrow"))
	if walWriter != nil {
		arrowBuffer.SetWAL(walWriter)
	}
	shutdownCoordinator.Register("arrow-buffer", arrowBuffer, shutdown.PriorityBuffer)

	// Initialize AuthManager (if enabled)
	var authManager *auth.AuthManager
	if cfg.Auth.Enabled {
		authManager, err = auth.NewAuthManager(
			cfg.Auth.DBPath,
			time.Duration(cfg.Auth.CacheTTL)*time.Second,
			cfg.Auth.MaxCacheSize,
			logger.Get("auth"),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize auth manager")
		}
		shutdownCoordinator.Register("auth", authManager, shutdown.PriorityAuth)

		// Create initial admin token if this is first run
		if token, err := authManager.EnsureInitialToken(); err != nil {
			log.Error().Err(err).Msg("Failed to create initial admin token")
		} else if token != "" {
			// Print colorized banner to stderr (bypasses structured logging)
			const (
				cyan   = "\033[96m"
				yellow = "\033[93m"
				bold   = "\033[1m"
				reset  = "\033[0m"
			)
			banner := cyan + "======================================================================" + reset
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, banner)
			fmt.Fprintln(os.Stderr, cyan+bold+"  FIRST RUN - INITIAL ADMIN TOKEN GENERATED"+reset)
			fmt.Fprintln(os.Stderr, banner)
			fmt.Fprintln(os.Stderr, yellow+bold+"  Initial admin API token: "+token+reset)
			fmt.Fprintln(os.Stderr, banner)
			fmt.Fprintln(os.Stderr, cyan+"  SAVE THIS TOKEN! It will not be shown again."+reset)
			fmt.Fprintln(os.Stderr, cyan+"  Use this token to login to the web UI or API."+reset)
			fmt.Fprintln(os.Stderr, cyan+"  You can create additional tokens after logging in."+reset)
			fmt.Fprintln(os.Stderr, banner)
			fmt.Fprintln(os.Stderr)
		}

		log.Info().
			Str("db_path", cfg.Auth.DBPath).
			Int("cache_ttl", cfg.Auth.CacheTTL).
			Int("max_cache_size", cfg.Auth.MaxCacheSize).
			Msg("Authentication enabled")
	} else {
		log.Warn().Msg("Authentication is DISABLED - all endpoints are public")
	}

	// Initialize Compaction (if enabled)
	var compactionScheduler *compaction.Scheduler
	var compactionManager *compaction.Manager
	if cfg.Compaction.Enabled {
		// Build tiers
		var tiers []compaction.Tier

		if cfg.Compaction.HourlyEnabled {
			hourlyTier := compaction.NewHourlyTier(&compaction.HourlyTierConfig{
				StorageBackend: storageBackend,
				MinAgeHours:    cfg.Compaction.HourlyMinAgeHours,
				MinFiles:       cfg.Compaction.HourlyMinFiles,
				Enabled:        true,
				Logger:         logger.Get("compaction"),
			})
			tiers = append(tiers, hourlyTier)
		}

		if cfg.Compaction.DailyEnabled {
			dailyTier := compaction.NewDailyTier(&compaction.DailyTierConfig{
				StorageBackend: storageBackend,
				MinAgeHours:    cfg.Compaction.DailyMinAgeHours,
				MinFiles:       cfg.Compaction.DailyMinFiles,
				Enabled:        true,
				Logger:         logger.Get("compaction"),
			})
			tiers = append(tiers, dailyTier)
		}

		// Create lock manager
		lockManager := compaction.NewLockManager()

		// Create compaction manager (discovers all databases dynamically)
		// Compaction jobs run in subprocesses for memory isolation
		compactionManager = compaction.NewManager(&compaction.ManagerConfig{
			StorageBackend: storageBackend,
			LockManager:    lockManager,
			MaxConcurrent:  cfg.Compaction.MaxConcurrent,
			Tiers:          tiers,
			Logger:         logger.Get("compaction"),
		})

		// Create and start scheduler (using hourly schedule by default)
		var err error
		compactionScheduler, err = compaction.NewScheduler(&compaction.SchedulerConfig{
			Manager:  compactionManager,
			Schedule: cfg.Compaction.HourlySchedule,
			Enabled:  true,
			Logger:   logger.Get("compaction"),
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create compaction scheduler")
		}

		if err := compactionScheduler.Start(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start compaction scheduler")
		}

		// Register shutdown hook for compaction
		shutdownCoordinator.RegisterHook("compaction-scheduler", func(ctx context.Context) error {
			compactionScheduler.Stop()
			return nil
		}, shutdown.PriorityCompaction)

		log.Info().
			Str("hourly_schedule", cfg.Compaction.HourlySchedule).
			Bool("hourly_enabled", cfg.Compaction.HourlyEnabled).
			Bool("daily_enabled", cfg.Compaction.DailyEnabled).
			Int("max_concurrent", cfg.Compaction.MaxConcurrent).
			Msg("Compaction enabled")
	} else {
		log.Info().Msg("Compaction is DISABLED")
	}

	// Initialize Telemetry (if enabled)
	var telemetryCollector *telemetry.Collector
	if cfg.Telemetry.Enabled {
		telemetryCfg := &telemetry.Config{
			Enabled:  true,
			Endpoint: cfg.Telemetry.Endpoint,
			Interval: time.Duration(cfg.Telemetry.IntervalSeconds) * time.Second,
			DataDir:  "./data",
		}
		telemetryCollector, err = telemetry.New(telemetryCfg, Version, logger.Get("telemetry"))
		if err != nil {
			log.Warn().Err(err).Msg("Failed to initialize telemetry (continuing without it)")
		} else {
			telemetryCollector.Start()
			shutdownCoordinator.RegisterHook("telemetry", func(ctx context.Context) error {
				telemetryCollector.Stop()
				return nil
			}, shutdown.PriorityTelemetry)

			log.Info().
				Str("instance_id", telemetryCollector.GetInstanceID()).
				Dur("interval", time.Duration(cfg.Telemetry.IntervalSeconds)*time.Second).
				Msg("Telemetry enabled")
		}
	} else {
		log.Info().Msg("Telemetry is DISABLED (opt-out via ARC_TELEMETRY_ENABLED=false)")
	}

	// Initialize HTTP server
	serverConfig := &api.ServerConfig{
		Port:            cfg.Server.Port,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     120 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}

	server := api.NewServer(serverConfig, logger.Get("server"))

	// Register base routes
	server.RegisterRoutes()

	// Apply auth middleware if enabled
	if authManager != nil {
		middlewareConfig := auth.DefaultMiddlewareConfig()
		middlewareConfig.AuthManager = authManager
		// Add public routes that don't need auth
		middlewareConfig.PublicRoutes = append(middlewareConfig.PublicRoutes, "/health", "/ready", "/api/v1/auth/verify")
		middlewareConfig.PublicPrefixes = append(middlewareConfig.PublicPrefixes, "/metrics", "/debug/pprof")
		server.GetApp().Use(auth.NewMiddleware(middlewareConfig))

		// Register auth routes
		authHandler := api.NewAuthHandler(authManager, logger.Get("auth"))
		authHandler.RegisterRoutes(server.GetApp())
	}

	// Register MessagePack handler with Arrow buffer
	msgpackHandler := api.NewMsgPackHandler(logger.Get("msgpack"), arrowBuffer)
	msgpackHandler.RegisterRoutes(server.GetApp())

	// Register Line Protocol handler
	lineProtocolHandler := api.NewLineProtocolHandler(arrowBuffer, logger.Get("lineprotocol"))
	lineProtocolHandler.RegisterRoutes(server.GetApp())

	// Register Query handler
	queryHandler := api.NewQueryHandler(db, storageBackend, logger.Get("query"))
	queryHandler.RegisterRoutes(server.GetApp())

	// Register Compaction handler (if compaction is enabled)
	if compactionManager != nil {
		compactionHandler := api.NewCompactionHandler(compactionManager, compactionScheduler, logger.Get("compaction"))
		compactionHandler.RegisterRoutes(server.GetApp())
	}

	// Register Delete handler
	deleteHandler := api.NewDeleteHandler(db, storageBackend, &cfg.Delete, logger.Get("delete"))
	deleteHandler.RegisterRoutes(server.GetApp())
	if cfg.Delete.Enabled {
		log.Info().
			Int("confirmation_threshold", cfg.Delete.ConfirmationThreshold).
			Int("max_rows_per_delete", cfg.Delete.MaxRowsPerDelete).
			Msg("Delete operations enabled")
	} else {
		log.Info().Msg("Delete operations DISABLED (set delete.enabled=true in arc.toml to enable)")
	}

	// Register Retention handler
	if cfg.Retention.Enabled {
		retentionHandler, err := api.NewRetentionHandler(storageBackend, db, &cfg.Retention, logger.Get("retention"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize retention handler")
		}
		retentionHandler.RegisterRoutes(server.GetApp())
		shutdownCoordinator.RegisterHook("retention", func(ctx context.Context) error {
			return retentionHandler.Close()
		}, shutdown.PriorityDatabase)
		log.Info().Str("db_path", cfg.Retention.DBPath).Msg("Retention policies enabled")
	} else {
		log.Info().Msg("Retention policies DISABLED")
	}

	// Register Continuous Query handler
	if cfg.ContinuousQuery.Enabled {
		cqHandler, err := api.NewContinuousQueryHandler(db, storageBackend, arrowBuffer, &cfg.ContinuousQuery, logger.Get("cq"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize continuous query handler")
		}
		cqHandler.RegisterRoutes(server.GetApp())
		shutdownCoordinator.RegisterHook("continuous-query", func(ctx context.Context) error {
			return cqHandler.Close()
		}, shutdown.PriorityDatabase)
		log.Info().Str("db_path", cfg.ContinuousQuery.DBPath).Msg("Continuous queries enabled")
	} else {
		log.Info().Msg("Continuous queries DISABLED")
	}

	// Register HTTP server shutdown hook (first to stop accepting new requests)
	shutdownCoordinator.RegisterHook("http-server", func(ctx context.Context) error {
		return server.Shutdown(30 * time.Second)
	}, shutdown.PriorityHTTPServer)

	// Start server
	if err := server.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start HTTP server")
	}

	log.Info().
		Int("port", cfg.Server.Port).
		Str("version", Version).
		Msg("Arc is ready! HTTP server started")

	// Wait for shutdown signal
	sig := shutdownCoordinator.WaitForSignal()
	log.Info().Str("signal", sig.String()).Msg("Initiating graceful shutdown...")

	// Perform graceful shutdown of all components
	if err := shutdownCoordinator.Shutdown(); err != nil {
		log.Error().Err(err).Msg("Shutdown completed with errors")
		os.Exit(1)
	}

	log.Info().Msg("Arc shutdown complete")
}

// runCompactSubcommand handles the "compact" subcommand for subprocess-based compaction.
// This is invoked by the parent process to run compaction jobs in isolation,
// ensuring DuckDB memory is fully released when the subprocess exits.
func runCompactSubcommand(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	jobJSON := fs.String("job", "", "Job configuration as JSON")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	if *jobJSON == "" {
		fmt.Fprintln(os.Stderr, "error: --job flag required")
		os.Exit(1)
	}

	var cfg compaction.SubprocessJobConfig
	if err := json.Unmarshal([]byte(*jobJSON), &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid job config: %v\n", err)
		os.Exit(1)
	}

	// Run compaction job
	result, err := compaction.RunSubprocessJob(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Output result as JSON to stdout for parent process to parse
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to encode result: %v\n", err)
		os.Exit(1)
	}
}
