package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/mqtt"
	"github.com/basekick-labs/arc/internal/scheduler"
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
		middlewareConfig.PublicRoutes = append(middlewareConfig.PublicRoutes, "/health", "/ready", "/api/v1/auth/verify")
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

	// Register Query handler
	queryHandler := api.NewQueryHandler(db, storageBackend, logger.Get("query"))
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

	// Register Compaction handler (if compaction is enabled)
	if compactionManager != nil {
		compactionHandler := api.NewCompactionHandler(compactionManager, hourlyScheduler, dailyScheduler, logger.Get("compaction"))
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

