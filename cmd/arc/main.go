package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/mqtt"
	"github.com/basekick-labs/arc/internal/scheduler"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/telemetry"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/basekick-labs/arc/internal/wal"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
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

	// Validate TLS configuration before starting
	if err := cfg.Server.ValidateTLS(); err != nil {
		fmt.Fprintf(os.Stderr, "TLS configuration error: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger.Setup(cfg.Log.Level, cfg.Log.Format)
	log.Info().Str("version", Version).Msg("Starting Arc...")

	// Validate Enterprise License early (before component initialization)
	// This allows us to apply core limits to DuckDB and ingestion workers
	var licenseClient *license.Client
	if cfg.License.Key != "" {
		log.Info().
			Str("license_key", cfg.License.Key[:min(12, len(cfg.License.Key))]+"...").
			Str("server_url", license.LicenseServerURL).
			Msg("Validating enterprise license")

		var err error
		licenseClient, err = license.NewClient(&license.ClientConfig{
			LicenseKey: cfg.License.Key,
			Logger:     logger.Get("license"),
		})
		if err != nil {
			log.Warn().Err(err).Msg("Failed to initialize license client - enterprise features disabled")
		} else {
			// Activate or verify license at startup
			lic, err := licenseClient.ActivateOrVerify(context.Background())
			if err != nil {
				log.Warn().
					Err(err).
					Str("license_key", cfg.License.Key[:min(12, len(cfg.License.Key))]+"...").
					Msg("License activation/verification failed - enterprise features disabled")
				licenseClient = nil
			} else {
				log.Info().
					Str("tier", string(lic.Tier)).
					Str("status", lic.Status).
					Int("days_remaining", lic.DaysRemaining).
					Int("max_cores", lic.MaxCores).
					Time("expires_at", lic.ExpiresAt).
					Strs("features", lic.Features).
					Msg("Enterprise license verified successfully")

				// Apply core limits from license to config
				// This ensures DuckDB and ingestion workers respect the license
				if lic.MaxCores > 0 {
					machineCores := runtime.NumCPU()

					if machineCores > lic.MaxCores {
						// Limit Go runtime to licensed cores - this is the real enforcement
						previousGOMAXPROCS := runtime.GOMAXPROCS(lic.MaxCores)
						log.Info().
							Int("machine_cores", machineCores).
							Int("licensed_cores", lic.MaxCores).
							Int("gomaxprocs_before", previousGOMAXPROCS).
							Int("gomaxprocs_after", lic.MaxCores).
							Msg("License core limit applied via GOMAXPROCS")

						// Also set DuckDB thread count to match
						cfg.Database.ThreadCount = lic.MaxCores
						log.Info().
							Int("duckdb_threads", lic.MaxCores).
							Msg("License core limit applied to DuckDB threads")

						// Limit ingestion flush workers to licensed cores
						if cfg.Ingest.FlushWorkers > lic.MaxCores {
							cfg.Ingest.FlushWorkers = lic.MaxCores
							log.Info().
								Int("flush_workers", lic.MaxCores).
								Msg("License core limit applied to ingestion flush workers")
						}
					}
				}
			}
		}
	} else {
		log.Warn().Msg("Enterprise license not configured - enterprise features disabled")
	}

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
	log.Info().
		Int("thread_count", cfg.Database.ThreadCount).
		Int("max_connections", cfg.Database.MaxConnections).
		Str("memory_limit", cfg.Database.MemoryLimit).
		Int("machine_cpus", runtime.NumCPU()).
		Msg("Initializing DuckDB with database config")
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
		// Azure Blob Storage configuration for azure extension
		AzureAccountName: cfg.Storage.AzureAccountName,
		AzureAccountKey:  cfg.Storage.AzureAccountKey,
		AzureEndpoint:    cfg.Storage.AzureEndpoint,
		// Query optimization
		EnableS3Cache:     cfg.Query.EnableS3Cache,
		S3CacheSize:       cfg.Query.S3CacheSize,
		S3CacheTTLSeconds: cfg.Query.S3CacheTTLSeconds,
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

	// Initialize WAL writer (if enabled) - recovery happens after ArrowBuffer is ready
	var walWriter *wal.Writer
	var walRecovery *wal.Recovery
	if cfg.WAL.Enabled {
		var err error
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

		// Prepare recovery (will run after ArrowBuffer is created)
		walRecovery = wal.NewRecovery(cfg.WAL.Directory, logger.Get("wal"))

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
	log.Info().
		Int("flush_workers", cfg.Ingest.FlushWorkers).
		Int("shard_count", cfg.Ingest.ShardCount).
		Int("flush_queue_size", cfg.Ingest.FlushQueueSize).
		Msg("Initializing Arrow buffer with ingestion config")
	arrowBuffer := ingest.NewArrowBuffer(&cfg.Ingest, storageBackend, logger.Get("arrow"))
	if walWriter != nil {
		arrowBuffer.SetWAL(walWriter)
	}
	shutdownCoordinator.Register("arrow-buffer", arrowBuffer, shutdown.PriorityBuffer)

	// After ArrowBuffer flushes (priority 30) but before WAL closes (priority 40),
	// purge WAL files since all data has been flushed to storage.
	// This prevents recovery from replaying already-persisted data on next startup.
	if walWriter != nil {
		shutdownCoordinator.RegisterHook("wal-purge", func(ctx context.Context) error {
			deleted, err := walWriter.PurgeAll()
			if err != nil {
				log.Error().Err(err).Msg("Failed to purge WAL files on shutdown")
				return err
			}
			if deleted > 0 {
				log.Info().Int("deleted", deleted).Msg("Purged WAL files after clean buffer flush")
			}
			return nil
		}, 35) // Between PriorityBuffer(30) and PriorityWAL(40)
	}

	// Run WAL recovery NOW that ArrowBuffer is ready
	if walRecovery != nil {
		// Create shared recovery callback to avoid code duplication
		recoveryCallback := createWALRecoveryCallback(arrowBuffer, logger.Get("wal-recovery"))
		columnarCallback := createColumnarRecoveryCallback(arrowBuffer, logger.Get("wal-recovery"))

		recoveryStats, err := walRecovery.RecoverWithOptions(context.Background(), recoveryCallback, &wal.RecoveryOptions{
			BatchSize:        cfg.WAL.RecoveryBatchSize,
			ColumnarCallback: columnarCallback,
		})
		if err != nil {
			log.Error().Err(err).Msg("WAL recovery failed")
		} else if recoveryStats.RecoveredFiles > 0 {
			// Track recovery metrics
			metrics.Get().IncWALRecoveryTotal()
			metrics.Get().IncWALRecoveryRecords(int64(recoveryStats.RecoveredEntries))
			log.Info().
				Int("files", recoveryStats.RecoveredFiles).
				Int("batches", recoveryStats.RecoveredBatches).
				Int("entries", recoveryStats.RecoveredEntries).
				Int("corrupted", recoveryStats.CorruptedEntries).
				Dur("duration", recoveryStats.RecoveryDuration).
				Msg("WAL recovery complete")
		}

		// Start periodic WAL maintenance goroutine.
		// Two modes:
		//   Normal:   purge rotated WAL files older than safeAge (data already in parquet)
		//   Recovery: when a flush failure is detected (S3 outage), replay WAL files
		//             to re-buffer data that was cleared from buffers after failed flush
		walMaintenanceCtx, walMaintenanceCancel := context.WithCancel(context.Background())
		shutdownCoordinator.RegisterHook("wal-periodic-maintenance", func(ctx context.Context) error {
			walMaintenanceCancel()
			return nil
		}, shutdown.PriorityBuffer)

		// Safe age threshold: after this duration, a rotated WAL file's data MUST have
		// been flushed to parquet by the normal buffer flush cycle (MaxBufferAgeMS).
		// We use 3x margin to account for flush worker delays and clock skew.
		safeAge := time.Duration(cfg.Ingest.MaxBufferAgeMS) * time.Millisecond * 3
		if safeAge < 30*time.Second {
			safeAge = 30 * time.Second
		}

		recoveryInterval := time.Duration(cfg.WAL.RecoveryIntervalSeconds) * time.Second
		go func() {
			ticker := time.NewTicker(recoveryInterval)
			defer ticker.Stop()
			walLogger := logger.Get("wal-maintenance")

			for {
				select {
				case <-walMaintenanceCtx.Done():
					return
				case <-ticker.C:
					if arrowBuffer.HasFlushFailure() {
						// Storage failure detected — replay WAL files to recover data
						// that was cleared from buffers after failed flush
						walLogger.Info().Msg("Flush failure detected, attempting WAL recovery")
						recovery := wal.NewRecovery(cfg.WAL.Directory, walLogger)
						activeFile := ""
						if walWriter != nil {
							activeFile = walWriter.CurrentFile()
						}
						stats, err := recovery.RecoverWithOptions(context.Background(), recoveryCallback, &wal.RecoveryOptions{
							SkipActiveFile:   activeFile,
							BatchSize:        cfg.WAL.RecoveryBatchSize,
							ColumnarCallback: columnarCallback,
						})
						if err != nil {
							walLogger.Error().Err(err).Msg("WAL recovery after flush failure failed")
						} else {
							if stats.RecoveredFiles > 0 {
								metrics.Get().IncWALRecoveryTotal()
								metrics.Get().IncWALRecoveryRecords(int64(stats.RecoveredEntries))
								walLogger.Info().
									Int("files", stats.RecoveredFiles).
									Int("entries", stats.RecoveredEntries).
									Msg("WAL recovery after flush failure complete")
							}
							arrowBuffer.ResetFlushFailure()
						}
					} else {
						// Normal operation — purge WAL files old enough that their data
						// has been flushed to parquet by the normal buffer flush cycle
						deleted, err := walWriter.PurgeOlderThan(safeAge)
						if err != nil {
							walLogger.Error().Err(err).Msg("Periodic WAL purge failed")
						} else if deleted > 0 {
							walLogger.Info().Int("deleted", deleted).Msg("Periodic WAL cleanup complete")
						}
					}
				}
			}
		}()
		log.Info().
			Dur("interval", recoveryInterval).
			Dur("safe_age", safeAge).
			Msg("Periodic WAL maintenance enabled")
	}

	// Initialize MQTT Subscription Manager (if enabled)
	var mqttManager mqtt.Manager
	if cfg.MQTT.Enabled {
		// Get encryption key from environment (required for subscriptions with passwords)
		encryptionKey, keyErr := mqtt.GetEncryptionKey()
		if keyErr != nil {
			log.Fatal().Err(keyErr).Msg("Invalid ARC_ENCRYPTION_KEY - must be 32 bytes (base64 or hex encoded)")
		}
		if encryptionKey == nil {
			log.Warn().Msg("ARC_ENCRYPTION_KEY not set - MQTT subscriptions with passwords will be rejected")
		}

		// Create password encryptor (will be NilEncryptor if no key - rejects passwords)
		encryptor, err := mqtt.NewPasswordEncryptor(encryptionKey)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create password encryptor")
		}

		// Create SQLite repository for MQTT subscriptions (use shared DB path from auth config)
		mqttDBPath := cfg.Auth.DBPath
		if mqttDBPath == "" {
			mqttDBPath = "./data/arc.db" // Use shared SQLite database
		}
		mqttRepo, err := mqtt.NewSQLiteRepository(mqttDBPath, encryptionKey, logger.Get("mqtt-repo"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize MQTT repository")
		}

		// Create subscription manager
		mqttManager = mqtt.NewSubscriptionManager(mqttRepo, encryptor, arrowBuffer, logger.Get("mqtt"))

		// Start manager (loads and starts auto_start subscriptions)
		if err := mqttManager.Start(context.Background()); err != nil {
			log.Warn().Err(err).Msg("Some MQTT subscriptions failed to auto-start")
		}

		// Register shutdown hook
		shutdownCoordinator.RegisterHook("mqtt-manager", func(ctx context.Context) error {
			return mqttManager.Shutdown(ctx)
		}, shutdown.PriorityIngest)

		log.Info().Bool("encryption_enabled", encryptionKey != nil).Msg("MQTT subscription manager enabled")
	} else {
		log.Debug().Msg("MQTT subscription manager is disabled")
	}

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

		// Restrict SQLite database file permissions (contains auth tokens, audit logs, etc.)
		if err := os.Chmod(cfg.Auth.DBPath, 0600); err != nil {
			log.Warn().Err(err).Str("path", cfg.Auth.DBPath).Msg("Failed to set database file permissions")
		}

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
	var hourlyScheduler *compaction.Scheduler
	var dailyScheduler *compaction.Scheduler
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
				StorageBackend:       storageBackend,
				MinAgeHours:          cfg.Compaction.DailyMinAgeHours,
				MinFiles:             cfg.Compaction.DailyMinFiles,
				SkipFileAgeCheckDays: cfg.Compaction.DailySkipFileAgeCheckDays,
				Enabled:              true,
				Logger:               logger.Get("compaction"),
			})
			tiers = append(tiers, dailyTier)
		}

		// Create lock manager
		lockManager := compaction.NewLockManager()

		// Parse sort keys from ingest config for compaction
		// Compaction needs to maintain the same sort order as ingested files
		sortKeysConfig, defaultSortKeys, err := config.ParseSortKeys(cfg.Ingest)
		if err != nil {
			log.Warn().Err(err).Msg("Invalid sort keys config for compaction, using defaults")
			sortKeysConfig = make(map[string][]string)
			defaultSortKeys = []string{"time"}
		}

		// Create compaction manager (discovers all databases dynamically)
		// Compaction jobs run in subprocesses for memory isolation
		compactionManager = compaction.NewManager(&compaction.ManagerConfig{
			StorageBackend:  storageBackend,
			LockManager:     lockManager,
			MaxConcurrent:   cfg.Compaction.MaxConcurrent,
			MemoryLimit:     cfg.Database.MemoryLimit, // Use same limit as main DuckDB
			SortKeysConfig:  sortKeysConfig,
			DefaultSortKeys: defaultSortKeys,
			Tiers:           tiers,
			Logger:          logger.Get("compaction"),
		})

		// Cleanup orphaned temp directories from previous runs (e.g., pod crashes)
		if err := compactionManager.CleanupOrphanedTempDirs(); err != nil {
			log.Warn().Err(err).Msg("Failed to cleanup orphaned compaction temp directories")
		}

		// Create hourly scheduler (if hourly tier is enabled)
		if cfg.Compaction.HourlyEnabled {
			hourlyScheduler, err = compaction.NewScheduler(&compaction.SchedulerConfig{
				Manager:   compactionManager,
				Schedule:  cfg.Compaction.HourlySchedule,
				TierNames: []string{"hourly"},
				Enabled:   true,
				Logger:    logger.Get("compaction-hourly"),
			})
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create hourly compaction scheduler")
			}

			if err := hourlyScheduler.Start(); err != nil {
				log.Fatal().Err(err).Msg("Failed to start hourly compaction scheduler")
			}

			shutdownCoordinator.RegisterHook("hourly-compaction-scheduler", func(ctx context.Context) error {
				hourlyScheduler.Stop()
				return nil
			}, shutdown.PriorityCompaction)

			log.Info().
				Str("schedule", cfg.Compaction.HourlySchedule).
				Msg("Hourly compaction scheduler started")
		}

		// Create daily scheduler (if daily tier is enabled)
		if cfg.Compaction.DailyEnabled {
			dailyScheduler, err = compaction.NewScheduler(&compaction.SchedulerConfig{
				Manager:   compactionManager,
				Schedule:  cfg.Compaction.DailySchedule,
				TierNames: []string{"daily"},
				Enabled:   true,
				Logger:    logger.Get("compaction-daily"),
			})
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create daily compaction scheduler")
			}

			if err := dailyScheduler.Start(); err != nil {
				log.Fatal().Err(err).Msg("Failed to start daily compaction scheduler")
			}

			shutdownCoordinator.RegisterHook("daily-compaction-scheduler", func(ctx context.Context) error {
				dailyScheduler.Stop()
				return nil
			}, shutdown.PriorityCompaction)

			log.Info().
				Str("schedule", cfg.Compaction.DailySchedule).
				Msg("Daily compaction scheduler started")
		}

		log.Info().
			Bool("hourly_enabled", cfg.Compaction.HourlyEnabled).
			Str("hourly_schedule", cfg.Compaction.HourlySchedule).
			Bool("daily_enabled", cfg.Compaction.DailyEnabled).
			Str("daily_schedule", cfg.Compaction.DailySchedule).
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

	// Start periodic license validation (if license was validated earlier)
	if licenseClient != nil {
		licenseClient.StartPeriodicValidation(license.ValidationInterval)
		shutdownCoordinator.RegisterHook("license-client", func(ctx context.Context) error {
			licenseClient.Stop()
			return nil
		}, shutdown.PriorityTelemetry)
	}

	// Initialize Cluster Coordinator (Enterprise feature)
	// Clustering enables role-based node separation: writer, reader, compactor
	var clusterCoordinator *cluster.Coordinator
	if cfg.Cluster.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Clustering requires enterprise license - running in standalone mode")
		} else {
			lic := licenseClient.GetLicense()
			if lic == nil || !lic.HasFeature(license.FeatureClustering) {
				log.Warn().Msg("License does not include clustering feature - running in standalone mode")
			} else {
				// Determine API address for this node
				apiAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
				if cfg.Server.Host == "0.0.0.0" {
					apiAddr = fmt.Sprintf(":%d", cfg.Server.Port)
				}

				var err error
				clusterCoordinator, err = cluster.NewCoordinator(&cluster.CoordinatorConfig{
					Config:        &cfg.Cluster,
					LicenseClient: licenseClient,
					Version:       Version,
					APIAddress:    apiAddr,
					Logger:        logger.Get("cluster"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to initialize cluster coordinator - running in standalone mode")
				} else {
					if err := clusterCoordinator.Start(); err != nil {
						log.Error().Err(err).Msg("Failed to start cluster coordinator - running in standalone mode")
						clusterCoordinator = nil
					} else {
						shutdownCoordinator.RegisterHook("cluster-coordinator", func(ctx context.Context) error {
							return clusterCoordinator.Stop()
						}, shutdown.PriorityCompaction) // Stop before compaction

						localNode := clusterCoordinator.GetLocalNode()
						capabilities := localNode.GetCapabilities()
						log.Info().
							Str("node_id", localNode.ID).
							Str("role", string(localNode.Role)).
							Str("cluster", cfg.Cluster.ClusterName).
							Bool("can_ingest", capabilities.CanIngest).
							Bool("can_query", capabilities.CanQuery).
							Bool("can_compact", capabilities.CanCompact).
							Msg("Cluster coordinator started")

						// Wire up WAL replication if enabled
						if cfg.Cluster.ReplicationEnabled && walWriter != nil {
							clusterCoordinator.SetWAL(walWriter)
							if err := clusterCoordinator.StartReplication(); err != nil {
								log.Warn().Err(err).Msg("Failed to start WAL replication")
							} else {
								log.Info().
									Bool("is_writer", capabilities.CanIngest).
									Msg("WAL replication started")
							}
						}
					}
				}
			}
		}
	}

	// Determine node capabilities (for role-based component initialization)
	nodeRole := cluster.RoleStandalone
	if clusterCoordinator != nil {
		nodeRole = clusterCoordinator.GetRole()
	}
	nodeCapabilities := nodeRole.GetCapabilities()

	// Log node role and capabilities (useful for debugging cluster deployments)
	if nodeRole != cluster.RoleStandalone {
		log.Info().
			Str("role", string(nodeRole)).
			Bool("can_ingest", nodeCapabilities.CanIngest).
			Bool("can_query", nodeCapabilities.CanQuery).
			Bool("can_compact", nodeCapabilities.CanCompact).
			Msg("Node running with cluster role")
	}

	// Auto-sync: ensure HTTP write timeout can accommodate query timeout
	if cfg.Query.Timeout > 0 && cfg.Server.WriteTimeout < cfg.Query.Timeout {
		log.Warn().
			Int("write_timeout", cfg.Server.WriteTimeout).
			Int("query_timeout", cfg.Query.Timeout).
			Msg("HTTP write_timeout is less than query timeout - adjusting to match")
		cfg.Server.WriteTimeout = cfg.Query.Timeout
	}

	// Initialize HTTP server
	serverConfig := &api.ServerConfig{
		Port:            cfg.Server.Port,
		ReadTimeout:     time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout:    time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:     time.Duration(cfg.Server.IdleTimeout) * time.Second,
		ShutdownTimeout: time.Duration(cfg.Server.ShutdownTimeout) * time.Second,
		MaxPayloadSize:  cfg.Server.MaxPayloadSize,
		TLSEnabled:      cfg.Server.TLSEnabled,
		TLSCertFile:     cfg.Server.TLSCertFile,
		TLSKeyFile:      cfg.Server.TLSKeyFile,
	}

	server := api.NewServer(serverConfig, logger.Get("server"))

	// Register base routes
	server.RegisterRoutes()

	// Apply auth middleware if enabled
	var rbacManager *auth.RBACManager
	if authManager != nil {
		middlewareConfig := auth.DefaultMiddlewareConfig()
		middlewareConfig.AuthManager = authManager
		// Add public routes that don't need auth
		middlewareConfig.PublicRoutes = append(middlewareConfig.PublicRoutes, "/health", "/ready", "/api/v1/auth/verify", "/api/v1/internal/cache/invalidate")
		middlewareConfig.PublicPrefixes = append(middlewareConfig.PublicPrefixes, "/metrics", "/debug/pprof")
		server.GetApp().Use(auth.NewMiddleware(middlewareConfig))

		// Initialize RBAC Manager (Enterprise feature)
		rbacManager = auth.NewRBACManager(&auth.RBACManagerConfig{
			DB:            authManager.GetDB(),
			LicenseClient: licenseClient,
			Logger:        logger.Get("rbac"),
		})
		if rbacManager.IsRBACEnabled() {
			log.Info().Msg("Enterprise RBAC enabled")
		}

		// Register auth routes
		authHandler := api.NewAuthHandler(authManager, logger.Get("auth"))
		authHandler.SetRBACManager(rbacManager)
		authHandler.RegisterRoutes(server.GetApp())
		authHandler.RegisterTokenMembershipRoutes(server.GetApp())

		// Register RBAC routes (Enterprise feature)
		rbacHandler := api.NewRBACHandler(authManager, rbacManager, logger.Get("rbac"))
		rbacHandler.RegisterRoutes(server.GetApp())
	}

	// Initialize Audit Logging (Enterprise feature - requires valid license)
	// Must be registered before API routes so the middleware captures all requests
	var auditLogger *audit.Logger
	if cfg.AuditLog.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Audit logging requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseAuditLogging() {
			log.Warn().Msg("License does not include audit_logging feature - feature disabled")
		} else {
			auditDB, err := sql.Open("sqlite3", cfg.Auth.DBPath)
			if err != nil {
				log.Error().Err(err).Msg("Failed to open audit database - feature disabled")
			} else {
				auditLogger, err = audit.NewLogger(&audit.LoggerConfig{
					DB:     auditDB,
					Config: &cfg.AuditLog,
					Logger: logger.Get("audit"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create audit logger - feature disabled")
				} else {
					auditLogger.Start()
					shutdownCoordinator.RegisterHook("audit", func(ctx context.Context) error {
						auditLogger.Stop()
						return nil
					}, shutdown.PriorityCompaction)

					// Register audit middleware BEFORE API routes
					server.GetApp().Use(audit.Middleware(auditLogger, cfg.AuditLog.IncludeReads))

					log.Info().
						Int("retention_days", cfg.AuditLog.RetentionDays).
						Bool("include_reads", cfg.AuditLog.IncludeReads).
						Msg("Audit logging enabled")
				}
			}
		}
	}

	// Register MessagePack handler with Arrow buffer
	msgpackHandler := api.NewMsgPackHandler(logger.Get("msgpack"), arrowBuffer, server.GetMaxPayloadSize())
	if authManager != nil && rbacManager != nil {
		msgpackHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	msgpackHandler.RegisterRoutes(server.GetApp())

	// Register Line Protocol handler
	lineProtocolHandler := api.NewLineProtocolHandler(arrowBuffer, logger.Get("lineprotocol"))
	if authManager != nil && rbacManager != nil {
		lineProtocolHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	lineProtocolHandler.RegisterRoutes(server.GetApp())

	// Register Import handler (CSV, Parquet, Line Protocol bulk import)
	importHandler := api.NewImportHandler(db, storageBackend, logger.Get("import"))
	importHandler.SetArrowBuffer(arrowBuffer)
	if authManager != nil && rbacManager != nil {
		importHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	importHandler.RegisterRoutes(server.GetApp())

	// Register Query handler with dedicated query timeout
	queryHandler := api.NewQueryHandler(db, storageBackend, logger.Get("query"), cfg.Query.Timeout)
	if authManager != nil && rbacManager != nil {
		queryHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	queryHandler.RegisterRoutes(server.GetApp())

	// Wire up cluster router to handlers for request forwarding
	// This enables reader nodes to forward writes to writers, and
	// compactor nodes to forward queries to readers/writers
	if clusterCoordinator != nil {
		router := clusterCoordinator.GetRouter()
		if router != nil {
			msgpackHandler.SetRouter(router)
			lineProtocolHandler.SetRouter(router)
			queryHandler.SetRouter(router)
			log.Info().Msg("Cluster router wired to API handlers for request forwarding")
		}
	}

	// Initialize Query Governance (Enterprise feature - requires valid license)
	if cfg.Governance.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Query governance requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseQueryGovernance() {
			log.Warn().Msg("License does not include query_governance feature - feature disabled")
		} else {
			governanceDBPath := cfg.Auth.DBPath
			if governanceDBPath == "" {
				governanceDBPath = "./data/arc.db"
			}
			governanceDB, err := sql.Open("sqlite3", governanceDBPath)
			if err != nil {
				log.Error().Err(err).Msg("Failed to open governance database - feature disabled")
			} else {
				governanceManager, err := governance.NewManager(&governance.ManagerConfig{
					DB:     governanceDB,
					Config: &cfg.Governance,
					Logger: logger.Get("governance"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create governance manager - feature disabled")
				} else {
					governanceManager.Start()
					shutdownCoordinator.RegisterHook("governance", func(ctx context.Context) error {
						return governanceManager.Stop()
					}, shutdown.PriorityCompaction)

					// Wire governance to query handler
					queryHandler.SetGovernance(governanceManager, licenseClient)

					// Register governance API handler
					governanceHandler := api.NewGovernanceHandler(governanceManager, authManager, licenseClient, logger.Get("governance-api"))
					governanceHandler.RegisterRoutes(server.GetApp())

					log.Info().
						Bool("enabled", true).
						Int("default_rate_limit_per_min", cfg.Governance.DefaultRateLimitPerMin).
						Int("default_rate_limit_per_hour", cfg.Governance.DefaultRateLimitPerHour).
						Int("default_max_queries_per_hour", cfg.Governance.DefaultMaxQueriesPerHour).
						Int("default_max_queries_per_day", cfg.Governance.DefaultMaxQueriesPerDay).
						Int("default_max_rows_per_query", cfg.Governance.DefaultMaxRowsPerQuery).
						Msg("Query governance enabled")
				}
			}
		}
	}

	// Initialize Query Management (Enterprise feature - requires valid license)
	if cfg.QueryManagement.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Query management requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseQueryManagement() {
			log.Warn().Msg("License does not include query_management feature - feature disabled")
		} else {
			qmRegistry := queryregistry.NewRegistry(&queryregistry.RegistryConfig{
				HistorySize: cfg.QueryManagement.HistorySize,
			}, logger.Get("query-registry"))

			// Wire registry to query handler
			queryHandler.SetQueryRegistry(qmRegistry)

			// Register query management API handler
			qmHandler := api.NewQueryManagementHandler(qmRegistry, authManager, licenseClient, logger.Get("query-mgmt-api"))
			qmHandler.RegisterRoutes(server.GetApp())

			log.Info().
				Bool("enabled", true).
				Int("history_size", cfg.QueryManagement.HistorySize).
				Msg("Query management enabled")
		}
	}

	// Register Compaction handler (if compaction is enabled)
	if compactionManager != nil {
		compactionHandler := api.NewCompactionHandler(compactionManager, hourlyScheduler, dailyScheduler, logger.Get("compaction"))
		compactionHandler.RegisterRoutes(server.GetApp())

		// Wire post-compaction cache invalidation.
		// After compaction deletes old parquet files, DuckDB's cache_httpfs still holds
		// cached glob results (directory listings) pointing to deleted files, causing 404s.
		// This callback clears all relevant caches in the parent process after each
		// successful compaction job. See: https://github.com/Basekick-Labs/arc/issues/204
		compactionManager.SetOnCompactionComplete(func() {
			// Local invalidation
			db.ClearHTTPCache()
			queryHandler.InvalidateCaches()

			// Distributed invalidation (enterprise clustering)
			// Notify all query-capable nodes to clear their caches too
			if clusterCoordinator != nil {
				registry := clusterCoordinator.GetRegistry()
				localNode := registry.Local()
				if localNode == nil {
					return
				}

				targets := registry.GetReaders()
				targets = append(targets, registry.GetWriters()...)

				for _, node := range targets {
					if node.ID == localNode.ID {
						continue
					}
					go func(n *cluster.Node) {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()

						url := fmt.Sprintf("http://%s/api/v1/internal/cache/invalidate", n.APIAddress)
						req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
						if err != nil {
							log.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to create cache invalidation request")
							return
						}
						req.Header.Set("X-Arc-Internal", "cache-invalidate")

						resp, err := http.DefaultClient.Do(req)
						if err != nil {
							log.Warn().Err(err).Str("node_id", n.ID).Str("address", n.APIAddress).
								Msg("Failed to invalidate cache on remote node")
							return
						}
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						if resp.StatusCode != 204 {
							log.Warn().Int("status", resp.StatusCode).Str("node_id", n.ID).
								Msg("Unexpected status from cache invalidation")
						} else {
							log.Debug().Str("node_id", n.ID).Msg("Remote cache invalidated after compaction")
						}
					}(node)
				}
			}
		})
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

	// Register Databases handler
	databasesHandler := api.NewDatabasesHandler(storageBackend, &cfg.Delete, logger.Get("databases"))
	databasesHandler.RegisterRoutes(server.GetApp())

	// Register Retention handler
	var retentionHandler *api.RetentionHandler
	if cfg.Retention.Enabled {
		var err error
		retentionHandler, err = api.NewRetentionHandler(storageBackend, db, &cfg.Retention, logger.Get("retention"))
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
	var cqHandler *api.ContinuousQueryHandler
	if cfg.ContinuousQuery.Enabled {
		var err error
		cqHandler, err = api.NewContinuousQueryHandler(db, storageBackend, arrowBuffer, &cfg.ContinuousQuery, logger.Get("cq"))
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

	// Initialize CQ Scheduler (Enterprise feature - requires valid license)
	// Scheduler is enabled when continuous_query.enabled=true AND license allows it
	var cqScheduler *scheduler.CQScheduler
	if cfg.ContinuousQuery.Enabled && cqHandler != nil {
		if licenseClient != nil && licenseClient.CanUseCQScheduler() {
			var err error
			cqScheduler, err = scheduler.NewCQScheduler(&scheduler.CQSchedulerConfig{
				CQHandler:     cqHandler,
				LicenseClient: licenseClient,
				Logger:        logger.Get("cq-scheduler"),
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to create CQ scheduler")
			} else {
				if err := cqScheduler.Start(); err != nil {
					log.Error().Err(err).Msg("Failed to start CQ scheduler")
				} else {
					shutdownCoordinator.RegisterHook("cq-scheduler", func(ctx context.Context) error {
						cqScheduler.Stop()
						return nil
					}, shutdown.PriorityCompaction)
					log.Info().Int("job_count", cqScheduler.JobCount()).Msg("CQ scheduler started")
				}
			}
		} else if licenseClient == nil {
			log.Info().Msg("CQ automatic scheduling requires enterprise license")
		} else {
			log.Info().Msg("CQ automatic scheduling not included in license tier")
		}
	}

	// Initialize Retention Scheduler (Enterprise feature - requires valid license)
	// Scheduler is enabled when retention.enabled=true AND license allows it
	var retentionScheduler *scheduler.RetentionScheduler
	if cfg.Retention.Enabled && retentionHandler != nil {
		if licenseClient != nil && licenseClient.CanUseRetentionScheduler() {
			var err error
			retentionScheduler, err = scheduler.NewRetentionScheduler(&scheduler.RetentionSchedulerConfig{
				RetentionHandler: retentionHandler,
				LicenseClient:    licenseClient,
				Schedule:         cfg.Scheduler.RetentionSchedule,
				Logger:           logger.Get("retention-scheduler"),
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to create retention scheduler")
			} else {
				if err := retentionScheduler.Start(); err != nil {
					log.Error().Err(err).Msg("Failed to start retention scheduler")
				} else {
					shutdownCoordinator.RegisterHook("retention-scheduler", func(ctx context.Context) error {
						retentionScheduler.Stop()
						return nil
					}, shutdown.PriorityCompaction)
					log.Info().Str("schedule", cfg.Scheduler.RetentionSchedule).Msg("Retention scheduler started")
				}
			}
		} else if licenseClient == nil {
			log.Info().Msg("Retention automatic scheduling requires enterprise license")
		} else {
			log.Info().Msg("Retention automatic scheduling not included in license tier")
		}
	}

	// Register Scheduler status handler (always register, shows status even if schedulers not running)
	// Note: We must explicitly pass nil interfaces when schedulers are nil, because
	// a nil *CQScheduler passed to an interface is not a nil interface (Go quirk)
	var cqSchedulerInterface api.CQSchedulerInterface
	if cqScheduler != nil {
		cqSchedulerInterface = cqScheduler
	}
	var retentionSchedulerInterface api.RetentionSchedulerInterface
	if retentionScheduler != nil {
		retentionSchedulerInterface = retentionScheduler
	}
	schedulerHandler := api.NewSchedulerHandler(cqSchedulerInterface, retentionSchedulerInterface, licenseClient, logger.Get("scheduler-api"))
	schedulerHandler.RegisterRoutes(server.GetApp())

	// Register Cluster handler (always register, shows status even if clustering not enabled)
	clusterHandler := api.NewClusterHandler(clusterCoordinator, licenseClient, logger.Get("cluster-api"))
	clusterHandler.RegisterRoutes(server.GetApp())

	// Register MQTT handlers (always register, handlers check if manager is nil)
	mqttHandler := api.NewMQTTHandler(mqttManager, authManager, logger.Get("mqtt-api"))
	mqttHandler.RegisterRoutes(server.GetApp())

	// Register MQTT subscription management API (if MQTT is enabled)
	if mqttManager != nil {
		mqttSubHandler := api.NewMQTTSubscriptionHandler(mqttManager, authManager, logger.Get("mqtt-subscriptions-api"))
		mqttSubHandler.RegisterRoutes(server.GetApp())
	}

	// Initialize Tiered Storage (Enterprise feature - requires valid license)
	// 2-tier system: Hot (local) -> Cold (S3/Azure archive)
	var tieringManager *tiering.Manager
	if cfg.TieredStorage.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Tiered storage requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseTieredStorage() {
			log.Warn().Msg("License does not include tiered_storage feature - feature disabled")
		} else {
			// Open SQLite database for tiering metadata (shared with other features)
			tieringDBPath := cfg.Auth.DBPath // Use shared SQLite database
			tieringDB, err := sql.Open("sqlite3", tieringDBPath)
			if err != nil {
				log.Error().Err(err).Msg("Failed to open tiering database - feature disabled")
			} else {
				// Create cold tier backend (S3 or Azure)
				var coldBackend storage.Backend
				cold := cfg.TieredStorage.Cold

				if cold.Enabled {
					switch cold.Backend {
					case "s3":
						s3Config := &storage.S3Config{
							Region:    cold.S3Region,
							Bucket:    cold.S3Bucket,
							Endpoint:  cold.S3Endpoint,
							AccessKey: cold.S3AccessKey,
							SecretKey: cold.S3SecretKey,
							UseSSL:    cold.S3UseSSL,
							PathStyle: cold.S3PathStyle,
						}
						coldBackend, err = storage.NewS3Backend(s3Config, logger.Get("tiering-cold-s3"))
						if err != nil {
							log.Error().Err(err).Msg("Failed to create cold tier S3 backend for tiering")
						}

					case "azure":
						azureConfig := &storage.AzureBlobConfig{
							ConnectionString:   cold.AzureConnectionString,
							AccountName:        cold.AzureAccountName,
							AccountKey:         cold.AzureAccountKey,
							SASToken:           cold.AzureSASToken,
							ContainerName:      cold.AzureContainer,
							Endpoint:           cold.AzureEndpoint,
							UseManagedIdentity: cold.AzureUseManagedIdentity,
						}
						coldBackend, err = storage.NewAzureBlobBackend(azureConfig, logger.Get("tiering-cold-azure"))
						if err != nil {
							log.Error().Err(err).Msg("Failed to create cold tier Azure backend for tiering")
						}
					}
				}

				// Create tiering manager
				tieringManager, err = tiering.NewManager(&tiering.ManagerConfig{
					HotBackend:    storageBackend,
					ColdBackend:   coldBackend,
					DB:            tieringDB,
					Config:        &cfg.TieredStorage,
					LicenseClient: licenseClient,
					Logger:        logger.Get("tiering"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create tiering manager - feature disabled")
				} else {
					// Start tiering manager
					if err := tieringManager.Start(); err != nil {
						log.Error().Err(err).Msg("Failed to start tiering manager")
					} else {
						shutdownCoordinator.RegisterHook("tiering", func(ctx context.Context) error {
							return tieringManager.Stop()
						}, shutdown.PriorityCompaction)

						log.Info().
							Str("schedule", cfg.TieredStorage.MigrationSchedule).
							Bool("cold_enabled", cfg.TieredStorage.Cold.Enabled).
							Int("default_hot_days", cfg.TieredStorage.DefaultHotMaxAgeDays).
							Msg("Tiered storage enabled")
					}
				}
			}
		}
	}

	// Register Tiering API handlers (always register, handlers check if manager is nil)
	if tieringManager != nil {
		tieringHandler := api.NewTieringHandler(tieringManager, authManager, licenseClient, logger.Get("tiering-api"))
		tieringHandler.RegisterRoutes(server.GetApp())

		tieringPoliciesHandler := api.NewTieringPoliciesHandler(tieringManager, authManager, licenseClient, logger.Get("tiering-policies-api"))
		tieringPoliciesHandler.RegisterRoutes(server.GetApp())

		// Wire tiering manager to query handler for multi-tier query routing
		queryHandler.SetTieringManager(tieringManager)
		log.Info().Msg("Tiering manager wired to query handler for multi-tier queries")

		// Wire tiering manager to databases handler for cold-tier database/measurement listing
		databasesHandler.SetTieringManager(tieringManager)
		log.Info().Msg("Tiering manager wired to databases handler for cold-tier listing")

		// Wire tiering manager to arrow buffer for automatic file registration
		arrowBuffer.SetTieringManager(tieringManager)
		log.Info().Msg("Tiering manager wired to arrow buffer for auto-registration")

		// Configure DuckDB with cold tier S3 credentials for direct S3 queries
		// This is needed because DuckDB's httpfs extension needs credentials to query S3 directly
		cold := cfg.TieredStorage.Cold
		if cold.Enabled && cold.Backend == "s3" && cold.S3AccessKey != "" {
			if err := db.ConfigureS3(&database.S3Config{
				Region:    cold.S3Region,
				Endpoint:  cold.S3Endpoint,
				AccessKey: cold.S3AccessKey,
				SecretKey: cold.S3SecretKey,
				UseSSL:    cold.S3UseSSL,
				PathStyle: cold.S3PathStyle,
			}); err != nil {
				log.Warn().Err(err).Msg("Failed to configure DuckDB with cold tier S3 credentials")
			} else {
				log.Info().Msg("DuckDB configured with cold tier S3 credentials for multi-tier queries")
			}
		}
	}

	// Initialize Backup/Restore (OSS feature — no license required)
	if cfg.Backup.Enabled {
		backupManager, err := backup.NewManager(&backup.ManagerConfig{
			DataStorage:  storageBackend,
			BackupPath:   cfg.Backup.LocalPath,
			SQLiteDBPath: cfg.Auth.DBPath,
			ConfigPath:   "arc.toml",
			Logger:       logger.Get("backup"),
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize backup manager")
		} else {
			backupHandler := api.NewBackupHandler(backupManager, authManager, logger.Get("backup-api"))
			backupHandler.RegisterRoutes(server.GetApp())
			log.Info().Str("backup_path", cfg.Backup.LocalPath).Msg("Backup/restore enabled")
		}
	}

	// Register audit API routes (audit logger initialized earlier, before API routes)
	if auditLogger != nil {
		auditHandler := api.NewAuditHandler(auditLogger, authManager, licenseClient, logger.Get("audit-api"))
		auditHandler.RegisterRoutes(server.GetApp())
	}

	// Register HTTP server shutdown hook (first to stop accepting new requests)
	shutdownCoordinator.RegisterHook("http-server", func(ctx context.Context) error {
		return server.Shutdown(30 * time.Second)
	}, shutdown.PriorityHTTPServer)

	// Start server
	if err := server.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start HTTP server")
	}

	protocol := "HTTP"
	if cfg.Server.TLSEnabled {
		protocol = "HTTPS"
	}
	log.Info().
		Int("port", cfg.Server.Port).
		Str("protocol", protocol).
		Str("version", Version).
		Msg("Arc is ready!")

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

// createWALRecoveryCallback creates a reusable WAL recovery callback function.
// This callback replays recovered WAL records through the ArrowBuffer for re-ingestion.
func createWALRecoveryCallback(arrowBuffer *ingest.ArrowBuffer, walLogger zerolog.Logger) wal.RecoveryCallback {
	return func(ctx context.Context, records []map[string]interface{}) error {
		if len(records) == 0 {
			return nil
		}
		for _, rec := range records {
			// Extract measurement from recovered record
			// WAL row format uses underscore-prefixed keys: _measurement, _database
			measurement, _ := rec["_measurement"].(string)
			if measurement == "" {
				measurement, _ = rec["measurement"].(string)
			}
			if measurement == "" {
				measurement, _ = rec["m"].(string)
			}
			if measurement == "" {
				continue // Skip records without measurement
			}

			database, _ := rec["_database"].(string)
			if database == "" {
				database, _ = rec["database"].(string)
			}
			if database == "" {
				database = "default"
			}

			// Build columnar record from recovered data
			columns := make(map[string][]interface{})
			for key, value := range rec {
				if key == "_measurement" || key == "measurement" || key == "m" || key == "_database" || key == "database" {
					continue
				}
				columns[key] = []interface{}{value}
			}

			if err := arrowBuffer.WriteColumnarDirectNoWAL(ctx, database, measurement, columns); err != nil {
				walLogger.Error().Err(err).Str("measurement", measurement).Msg("Failed to replay WAL record")
				return err
			}
		}
		walLogger.Info().Int("records", len(records)).Msg("WAL recovery: replayed records")
		return nil
	}
}

// createColumnarRecoveryCallback creates a WAL recovery callback for columnar entries
// written via the zero-copy AppendRaw path.
func createColumnarRecoveryCallback(arrowBuffer *ingest.ArrowBuffer, walLogger zerolog.Logger) wal.ColumnarRecoveryCallback {
	return func(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
		if database == "" {
			database = "default"
		}
		if err := arrowBuffer.WriteColumnarDirectNoWAL(ctx, database, measurement, columns); err != nil {
			walLogger.Error().Err(err).Str("database", database).Str("measurement", measurement).Msg("Failed to replay columnar WAL entry")
			return err
		}
		rowCount := 0
		for _, col := range columns {
			rowCount = len(col)
			break
		}
		walLogger.Info().Str("database", database).Str("measurement", measurement).Int("rows", rowCount).Msg("WAL recovery: replayed columnar entry")
		return nil
	}
}

// runCompactSubcommand handles the "compact" subcommand for subprocess-based compaction.
// This is invoked by the parent process to run compaction jobs in isolation,
// ensuring DuckDB memory is fully released when the subprocess exits.
func runCompactSubcommand(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	jobJSON := fs.String("job", "", "Job configuration as JSON (deprecated, use --job-stdin)")
	jobStdin := fs.Bool("job-stdin", false, "Read job configuration from stdin")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	var configData []byte
	var err error

	if *jobStdin {
		// Read config from stdin (preferred - avoids argument list too long errors)
		configData, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: failed to read config from stdin: %v\n", err)
			os.Exit(1)
		}
	} else if *jobJSON != "" {
		// Legacy: read from command line argument
		configData = []byte(*jobJSON)
	} else {
		fmt.Fprintln(os.Stderr, "error: --job-stdin or --job flag required")
		os.Exit(1)
	}

	var cfg compaction.SubprocessJobConfig
	if err := json.Unmarshal(configData, &cfg); err != nil {
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

