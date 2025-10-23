"""
Arc Core - High-Performance Time-Series Data Warehouse
Copyright (C) 2025 Basekick Labs

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from fastapi import FastAPI, HTTPException, Query, Body, BackgroundTasks, Request
import json
from fastapi.responses import JSONResponse, Response, ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import asyncio
from datetime import datetime
import logging
import os
import base64
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from .models import (
    InfluxDBConnectionCreate, InfluxDBConnectionResponse,
    StorageConnectionCreate, StorageConnectionResponse,
    ExportJobCreate, ExportJobResponse,
    QueryRequest, QueryResponse,
    ConnectionTestRequest, ConnectionTestResponse,
    HealthResponse, ReadinessResponse,
    ErrorResponse,
    TokenCreateRequest, TokenUpdateRequest, TokenResponse, TokenListResponse
)

from config import ArcConfig
from api.duckdb_engine import DuckDBEngine
from api.config import get_db_path
from api.database import ConnectionManager
from api.scheduler import ExportScheduler
from storage.s3_backend import S3Backend
from api.logging_config import (
    setup_logging, get_logger, RequestIdMiddleware,
    log_api_call, log_query_execution, log_connection_test
)
from api.monitoring import get_metrics_collector, get_memory_profile
from api.logs_endpoint import get_logs_manager
from api.auth import AuthManager, AuthMiddleware
from api.http_json_routes import router as http_json_router
from api.line_protocol_routes import router as line_protocol_router
from api.line_protocol_routes import init_parquet_buffer, start_parquet_buffer, stop_parquet_buffer
from api.msgpack_routes import router as msgpack_router
from api.msgpack_routes import init_arrow_buffer, start_arrow_buffer, stop_arrow_buffer
from api.wal_routes import router as wal_router
from api.compaction_routes import router as compaction_router, init_compaction
<<<<<<< HEAD
from api.delete_routes import router as delete_router  # Rewrite-based DELETE (zero overhead on writes/queries)
=======
# DELETE operations temporarily disabled for performance testing
# from api.delete_routes import router as delete_router
>>>>>>> 00377a145b8e98c258d75bc770764f88a2143a35
from api.retention_routes import router as retention_router
from api.query_cache import init_query_cache, get_query_cache

# Setup structured logging
setup_logging(
    service_name="arc-api",
    level=os.getenv("LOG_LEVEL", "INFO"),
    structured=os.getenv("LOG_FORMAT", "structured") == "structured",
    include_trace=os.getenv("LOG_INCLUDE_TRACE", "false").lower() == "true"
)

logger = get_logger(__name__)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])

app = FastAPI(
    title="Arc Query API",
    version="1.0.0",
    description="A comprehensive data pipeline solution for time-series data management",
    default_response_class=ORJSONResponse,  # 20-50% faster JSON serialization (Rust + SIMD)
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request"},
        404: {"model": ErrorResponse, "description": "Not Found"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"}
    }
)

# Add rate limiter state
app.state.limiter = limiter

# Rate limit exception handler
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Request size limits (100MB for binary uploads, configurable via env)
MAX_REQUEST_SIZE = int(os.getenv("MAX_REQUEST_SIZE_MB", "100")) * 1024 * 1024  # Default 100MB

@app.middleware("http")
async def check_request_size(request: Request, call_next):
    """Middleware to check request body size"""
    if request.method in ["POST", "PUT", "PATCH"]:
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_REQUEST_SIZE:
            return JSONResponse(
                status_code=413,
                content={
                    "error": "Payload Too Large",
                    "detail": f"Request body exceeds maximum size of {MAX_REQUEST_SIZE // 1024 // 1024}MB"
                }
            )
    return await call_next(request)

# Global exception handler
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            error="Validation Error",
            detail=str(exc),
            timestamp=datetime.now()
        ).dict()
    )

# Configure CORS origins from environment
ALLOWED_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,https://localhost:3000,https://onedrive.live.com,https://*.officeapps.live.com,https://excel.officeapps.live.com").split(",")
logger.debug(f"CORS allowed origins: {ALLOWED_ORIGINS}")

# Add request ID middleware first
app.add_middleware(RequestIdMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for Excel add-in compatibility
    allow_credentials=False,  # Can't use credentials with wildcard origins
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Auth setup (must be defined early)
from config_loader import get_config
_arc_config = get_config()
_auth_config = _arc_config.get_auth_config()

AUTH_ENABLED = _auth_config.get("enabled", True)  # Default to enabled
AUTH_ALLOWLIST = [p.strip() for p in _auth_config.get(
    "allowlist",
    "/health,/ready,/docs,/openapi.json,/api/v1/auth/verify"  # Basic health endpoints only
).split(",") if p.strip()]
AUTH_CACHE_TTL = _auth_config.get("cache_ttl", 30)  # Default: 30 seconds
auth_manager = AuthManager(cache_ttl=AUTH_CACHE_TTL)

# Handle seed token from config or environment
default_token = _auth_config.get("default_token") or os.getenv("DEFAULT_API_TOKEN")
if default_token:
    auth_manager.ensure_seed_token(default_token, name="default")
    logger.debug("Using DEFAULT_API_TOKEN from configuration")

app.middleware("http")(AuthMiddleware(auth_manager, enabled=AUTH_ENABLED, allowlist=AUTH_ALLOWLIST))

# Include routers
app.include_router(http_json_router)
app.include_router(line_protocol_router)
app.include_router(msgpack_router)
app.include_router(wal_router)
app.include_router(compaction_router)
<<<<<<< HEAD
app.include_router(delete_router)  # Rewrite-based DELETE (zero overhead on writes/queries)
app.include_router(retention_router)
=======
app.include_router(retention_router)
# DELETE operations temporarily disabled for performance testing
# app.include_router(delete_router)
>>>>>>> 00377a145b8e98c258d75bc770764f88a2143a35

# Global query engine, connection manager, and scheduler
query_engine: Optional[DuckDBEngine] = None
connection_manager = ConnectionManager(db_path=get_db_path())
export_scheduler = ExportScheduler()
metrics_collector = get_metrics_collector()
logs_manager = get_logs_manager()

# Primary worker detection (for log reduction in multi-worker setups)
_primary_worker_lock_fd = None
_is_primary_worker = False

async def reinitialize_query_engine(verbose: bool = True):
    """Reinitialize query engine with current active storage connection

    Args:
        verbose: If True, log at INFO level. If False, log at DEBUG level (reduces multi-worker noise)
    """
    global query_engine

    # Helper to conditionally log
    def log_reinit(message, level='info'):
        if verbose:
            getattr(logger, level)(message)
        else:
            logger.debug(f"[Worker {os.getpid()}] {message}")

    # Close existing connections
    if query_engine:
        query_engine.close()

    # Get active storage connection
    active_storage = connection_manager.get_active_storage_connection()

    if active_storage and active_storage['backend'] == 's3':
        from storage.s3_backend import S3Backend
        s3_backend = S3Backend(
            bucket=active_storage['bucket'],
            region=active_storage.get('region', 'us-east-1'),
            database=active_storage.get('database', 'default'),
            access_key=active_storage.get('access_key'),
            secret_key=active_storage.get('secret_key'),
            use_directory_bucket=active_storage.get('use_directory_bucket', False),
            availability_zone=active_storage.get('availability_zone')
        )
        query_engine = DuckDBEngine(
            storage_backend="s3",
            s3_backend=s3_backend,
            connection_manager=connection_manager
        )
        log_reinit(f"Query engines reinitialized with S3 backend: {active_storage['bucket']}")
    elif active_storage and active_storage['backend'] == 'minio':
        from storage.minio_backend import MinIOBackend
        minio_backend = MinIOBackend(
            endpoint_url=active_storage['endpoint'],
            access_key=active_storage['access_key'],
            secret_key=active_storage['secret_key'],
            bucket=active_storage['bucket'],
            database=active_storage.get('database', 'default')
        )
        query_engine = DuckDBEngine(
            storage_backend="minio",
            minio_backend=minio_backend,
            connection_manager=connection_manager
        )
        log_reinit(f"Query engines reinitialized with MinIO backend: {active_storage['bucket']}")
    elif active_storage and active_storage['backend'] == 'ceph':
        from storage.ceph_backend import CephBackend
        ceph_backend = CephBackend(
            endpoint_url=active_storage['endpoint'],
            access_key=active_storage['access_key'],
            secret_key=active_storage['secret_key'],
            bucket=active_storage['bucket'],
            region=active_storage.get('region', 'us-east-1'),
            database=active_storage.get('database', 'default')
        )
        query_engine = DuckDBEngine(
            storage_backend="ceph",
            ceph_backend=ceph_backend,
            connection_manager=connection_manager
        )
        log_reinit(f"Query engines reinitialized with Ceph backend: {active_storage['bucket']}")
    elif active_storage and active_storage['backend'] == 'gcs':
        from storage.gcs_backend import GCSBackend
        gcs_backend = GCSBackend(
            bucket=active_storage['bucket'],
            database=active_storage.get('database', 'default'),
            project_id=active_storage.get('project_id'),
            credentials_json=active_storage.get('credentials_json'),
            credentials_file=active_storage.get('credentials_file'),
            hmac_key_id=active_storage.get('hmac_key_id'),
            hmac_secret=active_storage.get('hmac_secret')
        )

        # Initialize DuckDB engines with GCS support via signed URLs
        query_engine = DuckDBEngine(
            storage_backend="gcs",
            gcs_backend=gcs_backend,
            connection_manager=connection_manager
        )
        log_reinit(f"Query engines initialized with GCS backend: gs://{active_storage['bucket']} (using native DuckDB GCS support)")
        if active_storage.get('hmac_key_id'):
            log_reinit("GCS queries will use native gs:// access with HMAC authentication")
        else:
            log_reinit("GCS queries will use service account authentication")

    elif active_storage and active_storage['backend'] == 'local':
        from storage.local_backend import LocalBackend
        local_backend = LocalBackend(
            base_path=active_storage.get("base_path", "./data/arc"),
            database=active_storage.get('database', 'default')
        )
        query_engine = DuckDBEngine(
            storage_backend="local",
            local_backend=local_backend,
            connection_manager=connection_manager
        )


    else:
        logger.warning("No active storage connection found")

@app.on_event("startup")
async def startup_event():
    """Initialize query engine on startup"""
    global query_engine, _primary_worker_lock_fd, _is_primary_worker

    # Detect if we should log verbosely (first worker only in multi-worker setup)
    # This reduces log noise when running with many workers (e.g. 42 workers)
    # Use a simple file-based marker to identify the primary worker
    import fcntl
    import tempfile

    primary_worker_lock_file = os.path.join(tempfile.gettempdir(), 'arc_primary_worker.lock')
    is_verbose = False
    lock_fd = None

    try:
        # Try to acquire exclusive lock (only first worker succeeds)
        lock_fd = os.open(primary_worker_lock_file, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        is_verbose = True
        _is_primary_worker = True
        _primary_worker_lock_fd = lock_fd
        logger.info(f"This worker (PID {os.getpid()}) is the primary worker (verbose logging enabled)")
    except (IOError, OSError):
        # Lock already held by another worker - close the file descriptor
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except:
                pass
        is_verbose = False
        _is_primary_worker = False
        logger.debug(f"Worker {os.getpid()} is a secondary worker (reduced logging)")

    # Helper to log only from first worker
    def log_startup(message, level='info'):
        if is_verbose:
            getattr(logger, level)(message)
        else:
            logger.debug(f"[Worker {os.getpid()}] {message}")

    # Load config to check desired backend
    from config_loader import get_config
    arc_config = get_config()
    storage_config = arc_config.get_storage_config()
    desired_backend = storage_config.get('backend', os.getenv('STORAGE_BACKEND', 'minio'))

    # Get active storage connection from database
    active_storage = connection_manager.get_active_storage_connection()

    # Check if active storage backend matches config
    if active_storage and active_storage['backend'] != desired_backend:
        logger.warning(f"Storage backend mismatch: config={desired_backend}, active={active_storage['backend']}")
        log_startup(f"Deactivating old {active_storage['backend']} connection and creating {desired_backend} connection")

        # Deactivate all storage connections
        try:
            import sqlite3
            conn = sqlite3.connect(connection_manager.db_path)
            cursor = conn.cursor()
            cursor.execute('UPDATE storage_connections SET is_active = FALSE')
            conn.commit()
            conn.close()
            log_startup(f"Deactivated all storage connections")
        except Exception as e:
            logger.error(f"Failed to deactivate storage connections: {e}")

        active_storage = None

    # Auto-create storage connection from config if none exists or backend changed
    if not active_storage:
        log_startup(f"No active storage - checking config/env: backend={desired_backend}")

        if desired_backend == 'local':
            log_startup("Auto-creating local filesystem storage connection from config")

            # Retry logic for database lock contention (multiple workers starting simultaneously)
            max_retries = 5
            retry_delay = 0.5  # seconds

            for attempt in range(max_retries):
                try:
                    local_config = storage_config.get('local', {})
                    connection_config = {
                        'name': 'default-local',
                        'backend': 'local',
                        'base_path': local_config.get('base_path', './data/arc'),
                        'database': local_config.get('database', 'default'),
                        'is_active': True
                    }

                    if attempt == 0:
                        log_startup(f"Creating local storage connection: {connection_config['name']} at {connection_config['base_path']}")

                    connection_id = connection_manager.add_storage_connection(connection_config)
                    log_startup(f"✅ Auto-created local storage connection (id={connection_id})")

                    # Refresh active_storage after creating
                    active_storage = connection_manager.get_active_storage_connection()
                    break  # Success, exit retry loop

                except Exception as e:
                    # Race condition: another worker already created the connection
                    if "UNIQUE constraint failed" in str(e):
                        logger.debug("Local storage connection already created by another worker")
                        active_storage = connection_manager.get_active_storage_connection()
                        break  # Success (connection exists), exit retry loop

                    # Database locked: retry with backoff
                    elif "database is locked" in str(e):
                        if attempt < max_retries - 1:
                            import time
                            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                            logger.debug(f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            # Final attempt failed
                            import traceback
                            logger.error(f"Failed to auto-create local storage connection after {max_retries} attempts: {e}")
                            logger.error(f"Traceback: {traceback.format_exc()}")
                            # Try to get connection anyway (another worker may have created it)
                            active_storage = connection_manager.get_active_storage_connection()
                    else:
                        # Other error
                        import traceback
                        logger.error(f"Failed to auto-create local storage connection: {e}")
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        break

        elif desired_backend == 'minio' and os.getenv('MINIO_ENDPOINT'):
            log_startup("Auto-creating MinIO connection from environment variables")
            try:
                # Ensure endpoint has http:// or https:// prefix
                minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
                if not minio_endpoint.startswith(('http://', 'https://')):
                    minio_endpoint = f"http://{minio_endpoint}"

                connection_config = {
                    'name': 'default-minio',
                    'backend': 'minio',
                    'endpoint': minio_endpoint,
                    'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                    'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                    'bucket': os.getenv('MINIO_BUCKET', 'historian'),
                    'database': os.getenv('STORAGE_DATABASE', 'default'),
                    'is_active': True
                }
                log_startup(f"Creating storage connection: {connection_config['name']} at {connection_config['endpoint']}")

                connection_id = connection_manager.add_storage_connection(connection_config)
                log_startup(f"✅ Auto-created MinIO storage connection (id={connection_id})")

                # Refresh active_storage after creating
                active_storage = connection_manager.get_active_storage_connection()
            except Exception as e:
                # Race condition: another worker already created the connection
                # This is expected in multi-worker setups
                if "UNIQUE constraint failed" in str(e):
                    logger.debug("MinIO connection already created by another worker")
                    # Refresh active_storage - should exist now
                    active_storage = connection_manager.get_active_storage_connection()
                else:
                    import traceback
                    logger.error(f"Failed to auto-create MinIO connection: {e}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
        else:
            logger.warning(f"Cannot auto-create storage: backend={desired_backend}, has_endpoint={bool(os.getenv('MINIO_ENDPOINT'))}")

    await reinitialize_query_engine(verbose=is_verbose)

    # Logging moved to individual backend initialization above

    # Start the export scheduler
    export_scheduler.start_scheduler()
    log_startup("Export scheduler started")

    # Initialize and start metrics collection
    metrics_collector.set_dependencies(
        connection_manager=connection_manager,
        export_scheduler=export_scheduler,
        query_engine=query_engine
    )
    metrics_collector.start_collection()

    # Initialize write buffer for line protocol ingestion
    if active_storage:
        # Get the appropriate storage backend
        storage_backend = None

        if active_storage['backend'] == 's3':
            from storage.s3_backend import S3Backend
            storage_backend = S3Backend(
                bucket=active_storage['bucket'],
                region=active_storage.get('region', 'us-east-1'),
                database=active_storage.get('database', 'default'),
                access_key=active_storage.get('access_key'),
                secret_key=active_storage.get('secret_key')
            )
        elif active_storage['backend'] == 'minio':
            from storage.minio_backend import MinIOBackend
            storage_backend = MinIOBackend(
                endpoint_url=active_storage['endpoint'],
                access_key=active_storage['access_key'],
                secret_key=active_storage['secret_key'],
                bucket=active_storage['bucket'],
                database=active_storage.get('database', 'default')
            )
        elif active_storage['backend'] == 'gcs':
            from storage.gcs_backend import GCSBackend
            storage_backend = GCSBackend(
                bucket=active_storage['bucket'],
                database=active_storage.get('database', 'default'),
                project_id=active_storage.get('project_id'),
                credentials_json=active_storage.get('credentials_json'),
                credentials_file=active_storage.get('credentials_file'),
                hmac_key_id=active_storage.get('hmac_key_id'),
                hmac_secret=active_storage.get('hmac_secret')
            )
        elif active_storage['backend'] == 'ceph':
            from storage.ceph_backend import CephBackend
            storage_backend = CephBackend(
                endpoint_url=active_storage['endpoint'],
                access_key=active_storage['access_key'],
                secret_key=active_storage['secret_key'],
                bucket=active_storage['bucket'],
                region=active_storage.get('region', 'us-east-1'),
                database=active_storage.get('database', 'default')
            )
        elif active_storage['backend'] == 'local':
            from storage.local_backend import LocalBackend
            storage_backend = LocalBackend(
                base_path=active_storage.get("base_path", "./data/arc"),
                database=active_storage.get('database', 'default')
            )

        if storage_backend:
            # Get WAL configuration from config loader
            from config_loader import get_config
            arc_config = get_config()
            wal_config = arc_config.get_wal_config()

            # Initialize parquet buffer with configuration
            # Optimized for high throughput: larger buffers, reduce flush overhead
            buffer_config = {
                'max_buffer_size': int(os.getenv('WRITE_BUFFER_SIZE', '50000')),  # Larger = less flush overhead
                'max_buffer_age_seconds': int(os.getenv('WRITE_BUFFER_AGE', '5')),  # Fast timeout to avoid blocking
                'compression': os.getenv('WRITE_COMPRESSION', 'snappy'),
                'wal_enabled': wal_config.get('enabled', False),
                'wal_config': wal_config
            }
            init_parquet_buffer(storage_backend, buffer_config)
            await start_parquet_buffer()
            log_startup("Line protocol write service initialized")

            # Initialize Arrow buffer for MessagePack binary protocol
            init_arrow_buffer(storage_backend, buffer_config)
            await start_arrow_buffer()
            log_startup("MessagePack binary protocol write service initialized (Direct Arrow)")

            # Initialize compaction (only from first worker to avoid duplicate scheduler instances)
            compaction_config = arc_config.get_compaction_config()
            if compaction_config.get('enabled', True):
                from api.database import CompactionLock
                from storage.compaction import CompactionManager
                from storage.compaction_scheduler import CompactionScheduler

                # Initialize compaction lock
                compaction_lock = CompactionLock()

                # Initialize compaction manager
                compaction_manager = CompactionManager(
                    storage_backend=storage_backend,
                    lock_manager=compaction_lock,
                    database=getattr(storage_backend, 'database', 'default'),
                    min_age_hours=compaction_config.get('min_age_hours', 1),
                    min_files=compaction_config.get('min_files', 10),
                    target_size_mb=compaction_config.get('target_file_size_mb', 512),
                    max_concurrent=compaction_config.get('max_concurrent_jobs', 2)
                )

                # Initialize compaction scheduler (only run from first worker)
                # Scheduler enabled if this is the primary worker (compaction already enabled if we're here)
                scheduler_enabled = is_verbose
                compaction_scheduler = CompactionScheduler(
                    compaction_manager=compaction_manager,
                    schedule=compaction_config.get('schedule', '5 * * * *'),
                    enabled=scheduler_enabled
                )

                # Register with API routes (all workers need this for manual triggers)
                init_compaction(compaction_manager, compaction_scheduler)

                # Start scheduler (only runs if enabled=True, i.e., first worker only)
                await compaction_scheduler.start()

                if scheduler_enabled:
                    log_startup(
                        f"Compaction scheduler started: "
                        f"schedule='{compaction_config.get('schedule')}', "
                        f"min_files={compaction_config.get('min_files')}, "
                        f"target_size={compaction_config.get('target_file_size_mb')}MB"
                    )
                elif is_verbose:
                    log_startup("Compaction scheduler disabled in configuration")
                else:
                    logger.debug(f"[Worker {os.getpid()}] Compaction enabled, scheduler running on primary worker")
            else:
                log_startup("Compaction is disabled")

    else:
        logger.warning("No active storage backend - line protocol writes disabled")
    log_startup("Metrics collection started")

    # Initialize query cache
    init_query_cache()
    query_cache = get_query_cache()
    if query_cache:
        log_startup(f"Query cache initialized: TTL={query_cache.ttl_seconds}s, MaxSize={query_cache.max_size}")
    else:
        log_startup("Query cache disabled")

    # Check for first run and generate initial token if needed
    if AUTH_ENABLED:
        initial_token = auth_manager.ensure_initial_token()
        if initial_token:
            # ANSI color codes for terminal output
            CYAN = '\033[96m'
            YELLOW = '\033[93m'
            BOLD = '\033[1m'
            RESET = '\033[0m'

            # Print colorized token message to console (bypasses structured logging)
            import sys
            print(f"\n{CYAN}{'=' * 70}{RESET}", file=sys.stderr)
            print(f"{CYAN}{BOLD}FIRST RUN - INITIAL ADMIN TOKEN GENERATED{RESET}", file=sys.stderr)
            print(f"{CYAN}{'=' * 70}{RESET}", file=sys.stderr)
            print(f"{YELLOW}{BOLD}Initial admin API token: {initial_token}{RESET}", file=sys.stderr)
            print(f"{CYAN}{'=' * 70}{RESET}", file=sys.stderr)
            print(f"{CYAN}SAVE THIS TOKEN! It will not be shown again.{RESET}", file=sys.stderr)
            print(f"{CYAN}Use this token to login to the web UI or API.{RESET}", file=sys.stderr)
            print(f"{CYAN}You can create additional tokens after logging in.{RESET}", file=sys.stderr)
            print(f"{CYAN}{'=' * 70}{RESET}\n", file=sys.stderr)

            # Also log to structured logs (only from first worker)
            if is_verbose:
                logger.warning(f"Initial admin API token generated: {initial_token[:8]}...")
        else:
            log_startup("Auth is ENABLED; endpoints require a valid API token")
    else:
        logger.warning("Auth is DISABLED; endpoints are open (NOT RECOMMENDED for production)")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global _primary_worker_lock_fd, _is_primary_worker

    # Release primary worker lock if we own it
    if _is_primary_worker and _primary_worker_lock_fd is not None:
        try:
            import fcntl
            fcntl.flock(_primary_worker_lock_fd, fcntl.LOCK_UN)
            os.close(_primary_worker_lock_fd)
            logger.info(f"Primary worker (PID {os.getpid()}) releasing lock")
        except Exception as e:
            logger.debug(f"Error releasing primary worker lock: {e}")

    if query_engine:
        query_engine.close()

    # Stop the export scheduler
    export_scheduler.stop_scheduler()
    logger.info("Export scheduler stopped")
    
    # Stop metrics collection
    metrics_collector.stop_collection()
    logger.info("Metrics collection stopped")

    # Stop line protocol write buffer
    await stop_parquet_buffer()
    logger.info("Line protocol write service stopped")

    # Stop MessagePack Arrow buffer
    await stop_arrow_buffer()
    logger.info("MessagePack binary protocol write service stopped")

    # Stop compaction scheduler
    try:
        from api.compaction_routes import compaction_scheduler
        if compaction_scheduler:
            await compaction_scheduler.stop()
            logger.info("Compaction scheduler stopped")
    except Exception as e:
        logger.warning(f"Could not stop compaction scheduler: {e}")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint for load balancers"""
    return HealthResponse(
        status="healthy",
        service="Arc by Basekick Labs",
        version="1.0.0",
        timestamp=datetime.now()
    )

@app.get("/ready", response_model=ReadinessResponse)
async def readiness_check():
    """Readiness check endpoint for Kubernetes"""
    checks = {
        "database": False,
        "api_server": False,
        "scheduler": False
    }
    
    status_details = {}
    
    try:
        # Check database connectivity (SQLite should always be available)
        try:
            # Test if we can access the database
            influx_connections = connection_manager.get_influx_connections()
            storage_connections = connection_manager.get_storage_connections()
            checks["database"] = True
            status_details["database_error"] = None
        except Exception as db_error:
            checks["database"] = False
            status_details["database_error"] = str(db_error)
        
        # Check if API server is functional
        checks["api_server"] = True  # If we're executing this, the API is running
        
        # Check scheduler
        try:
            checks["scheduler"] = export_scheduler.is_running()
            status_details["scheduler_error"] = None
        except Exception as sched_error:
            checks["scheduler"] = False
            status_details["scheduler_error"] = str(sched_error)
        
        # Get connection status (these can be None for fresh deployments)
        active_influx = connection_manager.get_active_influx_connection()
        active_storage = connection_manager.get_active_storage_connection()
        
        # Service is ready if core components are working
        # Connections can be configured later via the UI
        core_ready = checks["database"] and checks["api_server"] and checks["scheduler"]
        
        details = {
            "active_influx_connection": active_influx is not None,
            "active_storage_connection": active_storage is not None,
            "query_engine_initialized": query_engine is not None,
            "scheduler_running": checks["scheduler"],
            "total_influx_connections": len(connection_manager.get_influx_connections()) if checks["database"] else 0,
            "total_storage_connections": len(connection_manager.get_storage_connections()) if checks["database"] else 0
        }
        
        # Add any errors to details
        details.update({k: v for k, v in status_details.items() if v is not None})
        
        message = "Service ready for configuration" if core_ready and not (active_influx and active_storage) else "Service fully operational" if core_ready else "Service not ready"
        
        return ReadinessResponse(
            status="ready" if core_ready else "not_ready",
            checks=checks,
            details=details,
            message=message,
            timestamp=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        return ReadinessResponse(
            status="not_ready",
            checks=checks,
            details={"error": str(e)},
            message="Readiness check failed",
            timestamp=datetime.now()
        )

@app.get("/")
async def root():
    """API information"""
    return {
        "service": "Arc Core",
        "version": "0.1.0-alpha",
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/api/v1/auth/verify")
async def auth_verify(request: Request):
    """Verify if the provided token is valid"""
    if auth_manager.verify_request_header(request.headers):
        return {"valid": True}
    raise HTTPException(status_code=401, detail="Invalid token")




# Token management endpoints (require authentication)

@app.get("/api/v1/auth/tokens", response_model=TokenListResponse)
async def list_tokens(request: Request):
    """List all API tokens (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    tokens = auth_manager.list_tokens()
    return {"tokens": tokens, "count": len(tokens)}


@app.get("/api/v1/auth/tokens/{token_id}", response_model=TokenResponse)
async def get_token(token_id: int, request: Request):
    """Get details about a specific token (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    token_info = auth_manager.get_token_info(token_id)
    if not token_info:
        raise HTTPException(status_code=404, detail="Token not found")

    return token_info


@app.post("/api/v1/auth/tokens", response_model=TokenResponse)
@limiter.limit("10/minute")  # Rate limit: 10 token creations per minute
async def create_token(token_request: TokenCreateRequest, request: Request):
    """Create a new API token (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    # Create the token with permissions
    new_token = auth_manager.create_token(
        name=token_request.name,
        description=token_request.description,
        expires_at=token_request.expires_at,
        permissions=token_request.permissions
    )

    # Get the token info to return - find by name
    tokens = auth_manager.list_tokens()
    token_info = next((t for t in tokens if t["name"] == token_request.name), None)

    if token_info:
        token_info["token"] = new_token  # Include actual token only on creation
        # Convert permissions string to list for response
        if "permissions" in token_info and isinstance(token_info["permissions"], str):
            token_info["permissions"] = token_info["permissions"].split(',')
        return token_info

    raise HTTPException(status_code=500, detail="Failed to create token")


@app.patch("/api/v1/auth/tokens/{token_id}", response_model=TokenResponse)
async def update_token(token_id: int, token_request: TokenUpdateRequest, request: Request):
    """Update token metadata (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    # Update the token with permissions
    updated = auth_manager.update_token(
        token_id=token_id,
        name=token_request.name,
        description=token_request.description,
        expires_at=token_request.expires_at,
        permissions=token_request.permissions
    )

    if not updated:
        raise HTTPException(status_code=404, detail="Token not found")

    # Return updated token info
    token_info = auth_manager.get_token_info(token_id)
    if token_info:
        # Convert permissions string to list for response
        if "permissions" in token_info and isinstance(token_info["permissions"], str):
            token_info["permissions"] = token_info["permissions"].split(',')
        return token_info

    raise HTTPException(status_code=500, detail="Failed to retrieve updated token")


@app.delete("/api/v1/auth/tokens/{token_id}")
async def delete_token(token_id: int, request: Request):
    """Delete an API token (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    if auth_manager.delete_token_by_id(token_id):
        return {"message": "Token deleted successfully", "token_id": token_id}
    raise HTTPException(status_code=404, detail="Token not found")


@app.post("/api/v1/auth/tokens/{token_id}/rotate", response_model=TokenResponse)
async def rotate_token_endpoint(token_id: int, request: Request):
    """Rotate a token - generates new token value while keeping metadata (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    new_token = auth_manager.rotate_token(token_id)
    if not new_token:
        raise HTTPException(status_code=404, detail="Token not found")

    # Get updated token info
    token_info = auth_manager.get_token_info(token_id)
    if token_info:
        token_info["token"] = new_token  # Include new token value (shown once)
        return token_info

    raise HTTPException(status_code=500, detail="Failed to rotate token")


@app.get("/api/v1/auth/cache/stats")
async def get_auth_cache_stats(request: Request):
    """Get authentication cache statistics (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    stats = auth_manager.get_cache_stats()
    return stats


@app.post("/api/v1/auth/cache/invalidate")
async def invalidate_auth_cache(request: Request):
    """Invalidate the authentication cache (requires authentication)"""
    if not auth_manager.verify_request_header(request.headers):
        raise HTTPException(status_code=401, detail="Invalid token")

    auth_manager.invalidate_cache()
    return {
        "message": "Authentication cache invalidated successfully",
        "cache_ttl_seconds": AUTH_CACHE_TTL
    }




@app.post("/api/v1/setup/default-connections")
async def setup_default_connections():
    """Create default connections for quick startup"""
    try:
        results = {"influx": None, "storage": None}
        
        # Check if connections already exist
        existing_influx = connection_manager.get_influx_connections()
        existing_storage = connection_manager.get_storage_connections()
        
        if existing_influx:
            results["influx"] = {"status": "exists", "count": len(existing_influx)}
        else:
            # Create default data source connection based on environment
            data_source = os.getenv("DATA_SOURCE", "influx")
            
            if data_source == "timescale":
                influx_config = {
                    "name": "Default TimescaleDB",
                    "version": "timescale",
                    "host": os.getenv("TIMESCALE_HOST", "timescaledb"),
                    "port": int(os.getenv("TIMESCALE_PORT", "5432")),
                    "database_name": os.getenv("TIMESCALE_DATABASE", "postgres"),
                    "username": os.getenv("TIMESCALE_USERNAME", "postgres"),
                    "password": os.getenv("TIMESCALE_PASSWORD", "password"),
                    "ssl": False
                }
            else:
                influx_config = {
                    "name": "Default InfluxDB",
                    "version": "1x",
                    "host": os.getenv("INFLUX_HOST", "influxdb"),
                    "port": int(os.getenv("INFLUX_PORT", "8086")),
                    "database_name": os.getenv("INFLUX_DATABASE", "historian_test"),
                    "username": os.getenv("INFLUX_USER", "historian"),
                    "password": os.getenv("INFLUX_PASSWORD", "historian123"),
                    "ssl": False
                }
            
            influx_id = connection_manager.create_influx_connection(influx_config)
            connection_manager.set_active_connection("influx", influx_id)
            results["influx"] = {"status": "created", "id": influx_id}
        
        if existing_storage:
            results["storage"] = {"status": "exists", "count": len(existing_storage)}
        else:
            # Create default storage connection based on environment
            storage_backend = os.getenv("STORAGE_BACKEND", "minio")
            
            if storage_backend == "minio":
                storage_config = {
                    "name": "Default MinIO",
                    "backend": "minio",
                    "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
                    "bucket": os.getenv("MINIO_BUCKET", "historian"),
                    "ssl": False
                }
            elif storage_backend == "ceph":
                storage_config = {
                    "name": "Default Ceph",
                    "backend": "ceph",
                    "endpoint": os.getenv("CEPH_ENDPOINT", "http://ceph-demo:7480"),
                    "access_key": os.getenv("CEPH_ACCESS_KEY", "demo"),
                    "secret_key": os.getenv("CEPH_SECRET_KEY", "demo"),
                    "bucket": os.getenv("CEPH_BUCKET", "historian"),
                    "region": os.getenv("CEPH_REGION", "us-east-1"),
                    "ssl": False
                }

            else:
                storage_config = {
                    "name": "Default S3",
                    "backend": "s3",
                    "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
                    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                    "bucket": os.getenv("S3_BUCKET", "historian"),
                    "region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                    "ssl": True
                }
            
            storage_id = connection_manager.create_storage_connection(storage_config)
            connection_manager.set_active_connection("storage", storage_id)
            results["storage"] = {"status": "created", "id": storage_id}
        
        # Reinitialize query engine with new connections
        await reinitialize_query_engine()
        
        return {
            "message": "Default connections setup completed",
            "results": results,
            "next_steps": [
                "Visit the UI to verify connections",
                "Create export jobs to start data pipeline",
                "Use /ready endpoint to check full system status"
            ]
        }
        
    except Exception as e:
        logger.error(f"Setup default connections failed: {e}")
        raise HTTPException(status_code=500, detail=f"Setup failed: {str(e)}")

@app.post("/api/v1/query/estimate")
async def estimate_query(query: QueryRequest):
    """Get query execution estimate (row count and warnings)"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    
    try:
        # Create a COUNT(*) version of the query
        count_sql = f"SELECT COUNT(*) FROM ({query.sql}) AS t"
        
        # Execute count query quickly
        result = await asyncio.wait_for(
            query_engine.execute_query(count_sql, 1), 
            timeout=30.0  # Shorter timeout for estimates
        )
        
        if not result["success"]:
            return {
                "success": False,
                "error": f"Cannot estimate query: {result.get('error', 'Unknown error')}",
                "estimated_rows": None,
                "warning_level": "error"
            }
        
        estimated_rows = result["data"][0][0] if result["data"] and len(result["data"]) > 0 else 0
        
        # Determine warning level
        warning_level = "none"
        warning_message = None
        
        if estimated_rows > 1000000:
            warning_level = "high"
            warning_message = f"⚠️ Large query: {estimated_rows:,} rows. This may take several minutes and use significant memory."
        elif estimated_rows > 100000:
            warning_level = "medium" 
            warning_message = f"⚠️ Medium query: {estimated_rows:,} rows. This may take 30-60 seconds."
        elif estimated_rows > 10000:
            warning_level = "low"
            warning_message = f"📊 {estimated_rows:,} rows. Should complete quickly."
        else:
            warning_message = f"✅ Small query: {estimated_rows:,} rows."
        
        return {
            "success": True,
            "estimated_rows": estimated_rows,
            "warning_level": warning_level,
            "warning_message": warning_message,
            "execution_time_ms": result.get("execution_time_ms", 0)
        }
        
    except asyncio.TimeoutError:
        return {
            "success": False,
            "error": "Query estimation timed out (complex query structure)",
            "estimated_rows": None,
            "warning_level": "error"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Cannot estimate query: {str(e)}",
            "estimated_rows": None,
            "warning_level": "error"
        }

@app.post("/api/v1/query/stream")
async def execute_sql_stream(query: QueryRequest):
    """Execute SQL query and stream results as CSV for large datasets"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    
    try:
        # For streaming, we'll use CSV format which is more efficient
        import io
        import csv
        from fastapi.responses import StreamingResponse
        
        # Execute query with a higher limit for streaming
        result = await asyncio.wait_for(
            query_engine.execute_query(query.sql, min(query.limit, 1000000)), 
            timeout=300.0
        )
        
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error", "Query failed"))
        
        # Create CSV in memory
        def generate_csv():
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow(result.get("columns", []))
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)
            
            # Write data in chunks
            data = result.get("data", [])
            chunk_size = 1000
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i+chunk_size]
                for row in chunk:
                    writer.writerow(row)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
        
        return StreamingResponse(
            generate_csv(),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=query_result.csv",
                "X-Row-Count": str(result.get("row_count", 0)),
                "X-Execution-Time-Ms": str(result.get("execution_time_ms", 0))
            }
        )
        
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Query execution timeout (5 minutes)")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

@app.post("/api/v1/query", response_model=QueryResponse)
async def execute_sql(request: Request, query: QueryRequest):
    """Execute SQL query with caching and comprehensive validation"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")

    # Check cache first
    query_cache = get_query_cache()
    cached_result = None
    cache_age = None

    if query_cache:
        cached_result, cache_age = query_cache.get(query.sql, query.limit)

        if cached_result:
            # Cache hit! Return immediately
            logger.info(
                f"Cache HIT: age={cache_age:.1f}s, rows={cached_result.get('row_count', 0)}, "
                f"sql={query.sql[:60]}..."
            )

            return QueryResponse(
                success=True,
                columns=cached_result.get("columns", []),
                data=cached_result.get("data", []),
                row_count=cached_result.get("row_count", 0),
                execution_time_ms=cached_result.get("execution_time_ms", 0.0),
                timestamp=datetime.now(),
                error=f"✅ Cached result (age: {cache_age:.1f}s)" if cache_age else None
            )

    # Cache miss - execute query
    try:
        # Add timeout for long-running queries (5 minutes)
        result = await asyncio.wait_for(
            query_engine.execute_query(query.sql, query.limit),
            timeout=300.0
        )

        # Log query execution with structured logging
        log_query_execution(
            logger,
            sql=query.sql,
            duration_ms=result.get("execution_time_ms", 0.0),
            row_count=result.get("row_count", 0),
            success=result["success"],
            query_format=query.format,
            limit=query.limit
        )

        if not result["success"]:
            error_msg = result.get("error", "Unknown error")
            if result.get("large_result_warning"):
                raise HTTPException(status_code=413, detail=error_msg)
            else:
                raise HTTPException(status_code=400, detail=error_msg)

        # Cache successful results
        if query_cache and result["success"]:
            cached = query_cache.set(query.sql, query.limit, result)
            if cached:
                logger.debug(f"Result cached: rows={result.get('row_count', 0)}")

        # Add educational warnings for large result sets
        row_count = result.get("row_count", 0)
        warning_message = None

        if row_count > 100000:
            warning_message = f"Large result: {row_count:,} rows returned. For better performance, consider using 'LIMIT' clause or the /query/stream endpoint for CSV export."
            logger.warning(f"Large query result: {row_count:,} rows")
        elif row_count > 10000:
            warning_message = f"Moderate result: {row_count:,} rows returned. Consider using 'LIMIT' if you don't need all rows."

        # Return structured response - no truncation, user gets full control
        return QueryResponse(
            success=True,
            columns=result.get("columns", []),
            data=result.get("data", []),
            row_count=result.get("row_count", 0),
            execution_time_ms=result.get("execution_time_ms", 0.0),
            timestamp=datetime.now(),
            error=warning_message  # Use error field for educational warnings
        )

    except asyncio.TimeoutError:
        logger.error("Query timed out after 5 minutes")
        raise HTTPException(status_code=408, detail="Query timed out after 5 minutes")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

@app.post("/api/v1/query/arrow")
async def execute_sql_arrow(request: Request, query: QueryRequest):
    """Execute SQL query and return Apache Arrow IPC stream (columnar format)

    This endpoint returns data in Apache Arrow IPC format for zero-copy columnar processing.
    Perfect for analytics tools and data pipelines that support Arrow.

    Response format: application/vnd.apache.arrow.stream
    """
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")

    try:
        from fastapi.responses import Response

        # Execute query with Arrow format
        result = await asyncio.wait_for(
            query_engine.execute_query_arrow(query.sql),
            timeout=300.0
        )

        # Log query execution
        log_query_execution(
            logger,
            sql=query.sql,
            duration_ms=result.get("execution_time_ms", 0.0),
            row_count=result.get("row_count", 0),
            success=result["success"],
            query_format="arrow",
            limit=None
        )

        if not result["success"]:
            error_msg = result.get("error", "Unknown error")
            raise HTTPException(status_code=400, detail=error_msg)

        # Return Arrow IPC stream as binary response
        # Note: Schema is embedded in the Arrow IPC stream, no need to send separately
        return Response(
            content=result["arrow_table"],
            media_type="application/vnd.apache.arrow.stream",
            headers={
                "X-Row-Count": str(result.get("row_count", 0)),
                "X-Execution-Time-Ms": str(result.get("execution_time_ms", 0)),
                "X-Wait-Time-Ms": str(result.get("wait_time_ms", 0))
            }
        )

    except asyncio.TimeoutError:
        logger.error("Arrow query timed out after 5 minutes")
        raise HTTPException(status_code=408, detail="Query timed out after 5 minutes")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Arrow query execution error: {e}")
        raise HTTPException(status_code=500, detail=f"Arrow query execution failed: {str(e)}")

@app.get("/api/v1/measurements")
async def list_measurements():
    """List available measurements"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    
    measurements = await query_engine.get_measurements()
    return {"measurements": measurements}

@app.get("/api/v1/query/{measurement}")
async def query_measurement(
    measurement: str,
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    columns: Optional[str] = Query(None, description="Comma-separated column names"),
    where: Optional[str] = Query(None, description="Additional WHERE clause"),
    limit: int = Query(1000, description="Maximum number of rows")
):
    """Query specific measurement with filters"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    
    # Parse columns
    column_list = None
    if columns:
        column_list = [col.strip() for col in columns.split(",")]
    
    result = await query_engine.query_measurement(
        measurement=measurement,
        start_time=start_time,
        end_time=end_time,
        columns=column_list,
        where_clause=where,
        limit=limit
    )
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return result

@app.get("/api/v1/query/{measurement}/csv")
async def query_measurement_csv(
    measurement: str,
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    columns: Optional[str] = Query(None),
    where: Optional[str] = Query(None),
    limit: int = Query(1000)
):
    """Query measurement and return CSV format"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    
    column_list = None
    if columns:
        column_list = [col.strip() for col in columns.split(",")]
    
    result = await query_engine.query_measurement(
        measurement=measurement,
        start_time=start_time,
        end_time=end_time,
        columns=column_list,
        where_clause=where,
        limit=limit
    )
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    
    # Convert to CSV
    import io
    import csv
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(result["columns"])
    
    # Write data
    for row in result["data"]:
        writer.writerow(row)
    
    csv_content = output.getvalue()
    output.close()
    
    return JSONResponse(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={measurement}.csv"}
    )


@app.get("/api/v1/connections/datasource")
async def get_datasource_connections():
    return connection_manager.get_influx_connections()

@app.post("/api/v1/connections/datasource")
async def add_datasource_connection(connection_data: dict):
    if connection_data.get('version') == 'http_json':
        # Route HTTP JSON connections to the HTTP JSON handler
        connection_id = connection_manager.add_http_json_connection(connection_data)
        return {"id": connection_id, "message": "HTTP JSON connection added successfully"}
    else:
        # Route other connections to InfluxDB handler
        connection_id = connection_manager.add_influx_connection(connection_data)
        source_type = "TimescaleDB" if connection_data.get('version') == 'timescale' else "InfluxDB"
        return {"id": connection_id, "message": f"{source_type} connection added successfully"}

@app.delete("/api/v1/connections/datasource/{connection_id}")
async def delete_datasource_connection(connection_id: int, connection_version: str = None):
    try:
        if connection_version == 'http_json':
            # Route HTTP JSON connections to the HTTP JSON handler
            success = connection_manager.delete_http_json_connection(connection_id)
            if not success:
                raise HTTPException(status_code=404, detail="HTTP JSON connection not found")
            return {"message": "HTTP JSON connection deleted successfully"}
        else:
            # Route other connections to InfluxDB handler
            success = connection_manager.delete_influx_connection(connection_id)
            if not success:
                raise HTTPException(status_code=404, detail="Data source connection not found")
            return {"message": "Data source connection deleted successfully"}
    except Exception as e:
        logger.error(f"Failed to delete datasource connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Legacy endpoints for backward compatibility
@app.get("/api/v1/connections/influx")
async def get_influx_connections():
    return connection_manager.get_influx_connections()

@app.post("/api/v1/connections/influx")
async def add_influx_connection(connection_data: dict):
    connection_id = connection_manager.add_influx_connection(connection_data)
    return {"id": connection_id, "message": "Data source connection added successfully"}

@app.get("/api/v1/connections/storage")
async def get_storage_connections():
    return connection_manager.get_storage_connections()

@app.post("/api/v1/connections/storage")
async def add_storage_connection(connection_data: dict):
    connection_id = connection_manager.add_storage_connection(connection_data)
    return {"id": connection_id, "message": "Storage connection added successfully"}

# HTTP JSON Endpoints (Legacy - consider deprecating)
@app.get("/api/v1/connections/http_json")
async def get_http_json_connections():
    return connection_manager.get_http_json_connections()

@app.post("/api/v1/connections/{connection_type}/{connection_id}/activate")
async def activate_connection(connection_type: str, connection_id: int):
    global query_engine
    
    success = connection_manager.set_active_connection(connection_type, connection_id)
    if success:
        # Reinitialize query engine if storage connection changed
        if connection_type == "storage":
            await reinitialize_query_engine()
        return {"message": f"{connection_type.title()} connection activated"}
    else:
        raise HTTPException(status_code=400, detail="Failed to activate connection")

@app.delete("/api/v1/connections/{connection_type}/{connection_id}")
async def delete_connection(connection_type: str, connection_id: int):
    success = connection_manager.delete_connection(connection_type, connection_id)
    if success:
        return {"message": f"{connection_type.title()} connection deleted"}
    else:
        raise HTTPException(status_code=400, detail="Failed to delete connection")

@app.put("/api/v1/connections/datasource/{connection_id}")
async def update_datasource_connection(connection_id: int, connection_data: dict):
    if connection_data.get('version') == 'http_json':
        # Route HTTP JSON connections to the HTTP JSON handler
        success = connection_manager.update_http_json_connection(connection_id, connection_data)
        if success:
            return {"message": "HTTP JSON connection updated successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to update HTTP JSON connection")
    else:
        # Route other connections to InfluxDB handler
        success = connection_manager.update_influx_connection(connection_id, connection_data)
        if success:
            source_type = "TimescaleDB" if connection_data.get('version') == 'timescale' else "InfluxDB"
            return {"message": f"{source_type} connection updated successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to update data source connection")

# Legacy endpoint
@app.put("/api/v1/connections/influx/{connection_id}")
async def update_influx_connection(connection_id: int, connection_data: dict):
    success = connection_manager.update_influx_connection(connection_id, connection_data)
    if success:
        return {"message": "Data source connection updated successfully"}
    else:
        raise HTTPException(status_code=400, detail="Failed to update data source connection")

@app.put("/api/v1/connections/storage/{connection_id}")
async def update_storage_connection(connection_id: int, connection_data: dict):
    success = connection_manager.update_storage_connection(connection_id, connection_data)
    if success:
        return {"message": "Storage connection updated successfully"}
    else:
        raise HTTPException(status_code=400, detail="Failed to update storage connection")

@app.post("/api/v1/connections/{connection_type}/test")
async def test_connection(connection_type: str, connection_data: dict):
    try:
        logger.info(f"Testing {connection_type} connection: {connection_data.get('name', 'unnamed')}")
        
        if connection_type in ["influx", "datasource"]:
            result = connection_manager.test_influx_connection(connection_data)
            source_type = "TimescaleDB" if connection_data.get('version') == 'timescale' else "InfluxDB"
            logger.info(f"{source_type} test result: {result}")
            return result
        elif connection_type == "storage":
            result = connection_manager.test_storage_connection(connection_data)
            logger.info(f"Storage test result: {result}")
            return result
        else:
            raise HTTPException(status_code=400, detail="Unknown connection type")
            
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        raise HTTPException(status_code=500, detail=f"Test failed: {str(e)}")



# Export Job Management Endpoints
@app.get("/api/v1/jobs")
async def get_export_jobs():
    return export_scheduler.get_jobs()

@app.post("/api/v1/jobs")
async def create_export_job(job_config: dict):
    job_id = export_scheduler.create_job(job_config)
    return {"id": job_id, "message": "Export job created successfully"}

@app.put("/api/v1/jobs/{job_id}")
async def update_export_job(job_id: int, job_config: dict):
    success = export_scheduler.update_job(job_id, job_config)
    if success:
        return {"message": "Export job updated successfully"}
    else:
        raise HTTPException(status_code=400, detail="Failed to update export job")

@app.delete("/api/v1/jobs/{job_id}")
async def delete_export_job(job_id: int):
    success = export_scheduler.delete_job(job_id)
    if success:
        return {"message": "Export job deleted successfully"}
    else:
        raise HTTPException(status_code=400, detail="Failed to delete export job")

@app.get("/api/v1/jobs/{job_id}/executions")
async def get_job_executions(job_id: int, limit: int = 50):
    return export_scheduler.get_job_executions(job_id, limit)

@app.get("/api/v1/monitoring/jobs")
async def get_job_monitoring():
    """Get real-time job monitoring data"""
    try:
        jobs = export_scheduler.get_jobs()
        executions = []
        
        # Get recent executions for all jobs
        for job in jobs:
            job_executions = export_scheduler.get_job_executions(job['id'], 20)
            for execution in job_executions:
                execution['job_name'] = job['name']
                execution['job_type'] = job['job_type']
                execution['measurement'] = job.get('measurement')
                executions.append(execution)
        
        # Sort by most recent
        executions.sort(key=lambda x: x['created_at'], reverse=True)
        
        return {
            "jobs": jobs,
            "recent_executions": executions[:50],
            "stats": {
                "total_jobs": len(jobs),
                "active_jobs": len([j for j in jobs if j['is_active']]),
                "running_jobs": len([e for e in executions if e['status'] == 'running']),
                "failed_jobs": len([e for e in executions if e['status'] == 'failed'])
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get monitoring data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get monitoring data: {str(e)}")

@app.post("/api/v1/jobs/{job_id}/cancel")
async def cancel_job(job_id: int):
    try:
        success = export_scheduler.cancel_job(job_id)
        if success:
            return {"message": f"Job {job_id} cancelled successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to cancel job")
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel job: {str(e)}")

@app.post("/api/v1/jobs/{job_id}/run")
async def run_job_now(job_id: int):
    try:
        # Get job details
        jobs = export_scheduler.get_jobs()
        job = next((j for j in jobs if j['id'] == job_id), None)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if not job['is_active']:
            raise HTTPException(status_code=400, detail="Job is not active")
        
        # Execute the job asynchronously (non-blocking)
        logger.info(f"Starting execution for job: {job['name']}")
        
        # Start job in background without awaiting
        import asyncio
        asyncio.create_task(export_scheduler.execute_job_now(job))
        
        return {
            "message": f"Job '{job['name']}' execution started in background",
            "job_id": job_id,
            "status": "triggered"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(e)}")

# Monitoring Endpoints
@app.get("/api/v1/metrics")
async def get_current_metrics():
    """Get current system metrics"""
    return metrics_collector.get_current_metrics()

@app.get("/api/v1/metrics/timeseries/{metric_type}")
async def get_metrics_timeseries(
    metric_type: str,
    duration_minutes: int = Query(default=30, ge=1, le=1440, description="Duration in minutes")
):
    """Get time series metrics data"""
    if metric_type not in ["system", "application", "api"]:
        raise HTTPException(status_code=400, detail="Invalid metric type. Must be: system, application, or api")
    
    return {
        "metric_type": metric_type,
        "duration_minutes": duration_minutes,
        "data": metrics_collector.get_time_series(metric_type, duration_minutes)
    }

@app.get("/api/v1/metrics/endpoints")
async def get_endpoint_metrics():
    """Get API endpoint usage statistics"""
    return {
        "timestamp": datetime.now().isoformat(),
        "endpoint_stats": metrics_collector.get_endpoint_stats()
    }

@app.get("/api/v1/metrics/query-pool")
async def get_query_pool_metrics():
    """Get DuckDB connection pool metrics"""
    if not query_engine:
        raise HTTPException(status_code=500, detail="Query engine not initialized")

    pool_metrics = query_engine.get_pool_metrics()
    connection_stats = query_engine.get_connection_stats()

    return {
        "timestamp": datetime.now().isoformat(),
        "pool": pool_metrics,
        "connections": connection_stats
    }

@app.get("/api/v1/metrics/memory")
async def get_memory_metrics():
    """
    Get detailed memory profiling for the Arc API process

    Returns:
    - Process memory usage (RSS, VMS, shared)
    - Python heap statistics
    - Garbage collector stats
    - Top object types by count
    - Memory optimization recommendations
    """
    return get_memory_profile()

@app.get("/api/v1/logs")
async def get_application_logs(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of log entries to return"),
    level: Optional[str] = Query(default=None, description="Filter by log level (INFO, WARNING, ERROR, DEBUG)"),
    since_minutes: int = Query(default=60, ge=1, le=1440, description="Get logs from the last N minutes")
):
    """Get recent application logs"""
    logs = logs_manager.get_recent_logs(
        limit=limit,
        level_filter=level,
        since_minutes=since_minutes
    )

    return {
        "timestamp": datetime.now().isoformat(),
        "logs": logs,
        "count": len(logs),
        "filters": {
            "limit": limit,
            "level": level,
            "since_minutes": since_minutes
        }
    }

# Avro Schema Management Endpoints

@app.get("/api/v1/avro/schemas")
async def get_avro_schemas(request: Request):
    """Get all Avro schemas (requires authentication)"""
    try:
        schemas = connection_manager.get_avro_schemas()
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Avro schemas: {e}")

@app.post("/api/v1/avro/schemas")
async def create_avro_schema(schema_data: dict, request: Request):
    """Create new Avro schema (requires authentication)"""
    try:
        # Validate required fields
        required_fields = ['topic_pattern', 'schema_name', 'schema_json']
        for field in required_fields:
            if field not in schema_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        # Validate JSON schema format
        import json
        try:
            json.loads(schema_data['schema_json'])
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in schema_json field")

        schema_id = connection_manager.add_avro_schema(schema_data)
        return {"message": "Avro schema created", "schema_id": schema_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Avro schema: {e}")

@app.get("/api/v1/avro/schemas/{schema_id}")
async def get_avro_schema(schema_id: int, request: Request):
    """Get specific Avro schema by ID (requires authentication)"""
    try:
        schemas = connection_manager.get_avro_schemas()
        schema = next((s for s in schemas if s['id'] == schema_id), None)

        if not schema:
            raise HTTPException(status_code=404, detail="Avro schema not found")

        return schema
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Avro schema: {e}")

@app.get("/api/v1/avro/schemas/topic/{topic_name}")
async def get_avro_schema_for_topic(topic_name: str):
    """Get the best matching Avro schema for a topic (public endpoint for system use)"""
    try:
        schema = connection_manager.get_avro_schema_for_topic(topic_name)

        if not schema:
            raise HTTPException(status_code=404, detail=f"No Avro schema found for topic '{topic_name}'")

        return schema
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Avro schema for topic: {e}")

@app.put("/api/v1/avro/schemas/{schema_id}")
async def update_avro_schema(schema_id: int, schema_data: dict, request: Request):
    """Update existing Avro schema (requires authentication)"""
    try:
        # Validate required fields
        required_fields = ['topic_pattern', 'schema_name', 'schema_json']
        for field in required_fields:
            if field not in schema_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        # Validate JSON schema format
        import json
        try:
            json.loads(schema_data['schema_json'])
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in schema_json field")

        success = connection_manager.update_avro_schema(schema_id, schema_data)
        if not success:
            raise HTTPException(status_code=404, detail="Avro schema not found")

        return {"message": "Avro schema updated", "schema_id": schema_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update Avro schema: {e}")

@app.delete("/api/v1/avro/schemas/{schema_id}")
async def delete_avro_schema(schema_id: int, request: Request):
    """Delete Avro schema (requires authentication)"""
    try:
        success = connection_manager.delete_avro_schema(schema_id)
        if not success:
            raise HTTPException(status_code=404, detail="Avro schema not found")

        return {"message": "Avro schema deleted", "schema_id": schema_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete Avro schema: {e}")

# =====================================================
# QUERY CACHE MANAGEMENT ENDPOINTS
# =====================================================

@app.get("/api/v1/cache/stats")
async def get_cache_stats():
    """
    Get query cache statistics and performance metrics

    Returns cache hit rate, utilization, and detailed entry information.
    Useful for monitoring cache effectiveness and tuning TTL/size settings.
    """
    query_cache = get_query_cache()

    if not query_cache:
        return {
            "enabled": False,
            "message": "Query cache is disabled. Set QUERY_CACHE_ENABLED=true to enable."
        }

    return query_cache.stats()

@app.get("/api/v1/cache/health")
async def get_cache_health():
    """
    Health check for query cache

    Returns health status with warnings if:
    - Hit rate is too low (< 20%)
    - Cache is underutilized
    - Too many evictions (cache too small)
    """
    query_cache = get_query_cache()

    if not query_cache:
        return {
            "enabled": False,
            "healthy": True,
            "message": "Cache disabled"
        }

    return query_cache.health_check()

@app.post("/api/v1/cache/clear")
async def clear_cache():
    """
    Clear all cached query results

    Useful after data updates or schema changes to force fresh queries.
    """
    query_cache = get_query_cache()

    if not query_cache:
        return {
            "message": "Query cache is disabled",
            "cleared": 0
        }

    # Get count before clearing
    stats = query_cache.stats()
    count = stats["current_size"]

    query_cache.invalidate()

    return {
        "message": f"Cache cleared: {count} entries removed",
        "cleared": count
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
