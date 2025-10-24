"""
Arc Configuration Loader

Loads configuration from multiple sources with precedence:
1. Environment variables (highest priority)
2. arc.conf file (TOML format)
3. Built-in defaults (lowest priority)

Similar to InfluxDB's configuration approach.
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict, Optional

# Try to import toml (for arc.conf parsing)
try:
    import toml
    HAS_TOML = True
except ImportError:
    HAS_TOML = False
    logging.warning("toml module not found. Install with: pip install toml")

logger = logging.getLogger(__name__)


class ArcConfig:
    """Arc configuration manager"""

    def __init__(self, config_file: str = None):
        """
        Initialize configuration

        Args:
            config_file: Path to arc.conf file (default: ./arc.conf)
        """
        self.config_file = config_file or os.getenv("ARC_CONFIG_FILE", "arc.conf")
        self.config = {}

        # Load configuration in order of precedence
        self._load_defaults()
        self._load_config_file()
        self._load_env_overrides()

    def _load_defaults(self):
        """Load built-in default configuration"""
        self.config = {
            "server": {
                "host": "0.0.0.0",
                "port": 8000,
                "workers": 4,  # Default to multi-worker mode
                "worker_timeout": 120,
                "graceful_timeout": 60,
                "max_requests": 50000,
                "max_requests_jitter": 5000,
                "worker_connections": 1000,
            },
            "auth": {
                "enabled": True,
                "default_token": "",
                "allowlist": "/health,/ready,/docs,/openapi.json,/auth/verify",
                "cache_ttl": 30,  # Token cache TTL in seconds
            },
            "query_cache": {
                "enabled": True,
                "ttl_seconds": 60,
                "max_size": 100,
                "max_result_mb": 10,
            },
            "duckdb": {
                "pool_size": 5,
                "max_queue_size": 100,
                "enable_object_cache": True,
            },
            "delete": {
                "enabled": False,
                "confirmation_threshold": 10000,
                "max_rows_per_delete": 1000000,
                "tombstone_retention_days": 30,
                "audit_enabled": True,
            },
            "ingestion": {
                "buffer_size": 10000,
                "buffer_age_seconds": 60,
                "compression": "snappy",
            },
            "wal": {
                "enabled": False,
                "dir": "./data/wal",
                "sync_mode": "fdatasync",
                "max_size_mb": 100,
                "max_age_seconds": 3600,
            },
            "compaction": {
                "enabled": True,
                "min_age_hours": 1,
                "min_files": 10,
                "target_file_size_mb": 512,
                "schedule": "5 * * * *",
                "max_concurrent_jobs": 2,
                "lock_ttl_hours": 2,
                "temp_dir": "./data/compaction",
                "compression": "zstd",
                "compression_level": 3,
            },
            "storage": {
                "backend": "minio",
                "database": "default",
                "local": {
                    "base_path": "./data/arc",
                    "database": "default",
                },
                "minio": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin123",
                    "bucket": "arc",
                    "use_ssl": False,
                },
            },
            "logging": {
                "level": "INFO",
                "format": "structured",
                "include_trace": False,
            },
            "cors": {
                "origins": "http://localhost:3000,http://atila:3000",
            },
        }

    def _load_config_file(self):
        """Load configuration from arc.conf file"""
        if not HAS_TOML:
            logger.warning("TOML parser not available, skipping arc.conf")
            return

        config_path = Path(self.config_file)

        if not config_path.exists():
            logger.info(f"Config file not found: {self.config_file}, using defaults")
            return

        try:
            file_config = toml.load(config_path)
            logger.info(f"Loaded configuration from: {self.config_file}")

            # Merge file config into defaults (deep merge)
            self._deep_merge(self.config, file_config)

        except Exception as e:
            logger.error(f"Failed to load config file {self.config_file}: {e}")

    def _load_env_overrides(self):
        """Load environment variable overrides"""
        # Map environment variables to config keys
        env_mappings = {
            # Server
            "ARC_HOST": ("server", "host"),
            "ARC_PORT": ("server", "port", int),
            "ARC_WORKERS": ("server", "workers", int),
            "WORKERS": ("server", "workers", int),  # Legacy support
            "HOST": ("server", "host"),              # Legacy support
            "PORT": ("server", "port", int),         # Legacy support

            # Auth
            "AUTH_ENABLED": ("auth", "enabled", lambda x: x.lower() == "true"),
            "DEFAULT_API_TOKEN": ("auth", "default_token"),
            "AUTH_ALLOWLIST": ("auth", "allowlist"),
            "AUTH_CACHE_TTL": ("auth", "cache_ttl", int),

            # Query Cache
            "QUERY_CACHE_ENABLED": ("query_cache", "enabled", lambda x: x.lower() == "true"),
            "QUERY_CACHE_TTL": ("query_cache", "ttl_seconds", int),
            "QUERY_CACHE_MAX_SIZE": ("query_cache", "max_size", int),
            "QUERY_CACHE_MAX_RESULT_MB": ("query_cache", "max_result_mb", int),

            # DuckDB
            "DUCKDB_POOL_SIZE": ("duckdb", "pool_size", int),
            "DUCKDB_MAX_QUEUE_SIZE": ("duckdb", "max_queue_size", int),
            "DUCKDB_ENABLE_OBJECT_CACHE": ("duckdb", "enable_object_cache", lambda x: x.lower() == "true"),

            # Delete Operations
            "DELETE_ENABLED": ("delete", "enabled", lambda x: x.lower() == "true"),
            "DELETE_CONFIRMATION_THRESHOLD": ("delete", "confirmation_threshold", int),
            "DELETE_MAX_ROWS": ("delete", "max_rows_per_delete", int),
            "DELETE_TOMBSTONE_RETENTION_DAYS": ("delete", "tombstone_retention_days", int),
            "DELETE_AUDIT_ENABLED": ("delete", "audit_enabled", lambda x: x.lower() == "true"),

            # Ingestion
            "WRITE_BUFFER_SIZE": ("ingestion", "buffer_size", int),
            "WRITE_BUFFER_AGE": ("ingestion", "buffer_age_seconds", int),
            "WRITE_COMPRESSION": ("ingestion", "compression"),

            # Write-Ahead Log (WAL)
            "WAL_ENABLED": ("wal", "enabled", lambda x: x.lower() == "true"),
            "WAL_DIR": ("wal", "dir"),
            "WAL_SYNC_MODE": ("wal", "sync_mode"),
            "WAL_MAX_SIZE_MB": ("wal", "max_size_mb", int),
            "WAL_MAX_AGE_SECONDS": ("wal", "max_age_seconds", int),

            # Compaction
            "COMPACTION_ENABLED": ("compaction", "enabled", lambda x: x.lower() == "true"),
            "COMPACTION_MIN_AGE_HOURS": ("compaction", "min_age_hours", int),
            "COMPACTION_MIN_FILES": ("compaction", "min_files", int),
            "COMPACTION_TARGET_FILE_SIZE_MB": ("compaction", "target_file_size_mb", int),
            "COMPACTION_SCHEDULE": ("compaction", "schedule"),
            "COMPACTION_MAX_CONCURRENT_JOBS": ("compaction", "max_concurrent_jobs", int),
            "COMPACTION_LOCK_TTL_HOURS": ("compaction", "lock_ttl_hours", int),
            "COMPACTION_TEMP_DIR": ("compaction", "temp_dir"),
            "COMPACTION_COMPRESSION": ("compaction", "compression"),
            "COMPACTION_COMPRESSION_LEVEL": ("compaction", "compression_level", int),

            # Storage
            "STORAGE_BACKEND": ("storage", "backend"),
            "STORAGE_DATABASE": ("storage", "database"),

            # Local filesystem storage
            "STORAGE_LOCAL_BASE_PATH": ("storage", "local", "base_path"),
            "STORAGE_LOCAL_DATABASE": ("storage", "local", "database"),

            # MinIO storage
            "MINIO_ENDPOINT": ("storage", "minio", "endpoint"),
            "MINIO_ACCESS_KEY": ("storage", "minio", "access_key"),
            "MINIO_SECRET_KEY": ("storage", "minio", "secret_key"),
            "MINIO_BUCKET": ("storage", "minio", "bucket"),

            # Logging
            "LOG_LEVEL": ("logging", "level"),
            "LOG_FORMAT": ("logging", "format"),
            "LOG_INCLUDE_TRACE": ("logging", "include_trace", lambda x: x.lower() == "true"),

            # CORS
            "CORS_ORIGINS": ("cors", "origins"),
        }

        for env_var, mapping in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Extract path and converter
                *path, converter = mapping if len(mapping) > 2 else (*mapping, None)

                # Convert value if converter provided
                if converter and callable(converter):
                    try:
                        value = converter(value)
                    except Exception as e:
                        logger.warning(f"Failed to convert {env_var}={value}: {e}")
                        continue

                # Set value in config
                self._set_nested(self.config, path, value)
                logger.debug(f"Environment override: {env_var}={value}")

    def _deep_merge(self, base: Dict, override: Dict):
        """Deep merge override dict into base dict"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def _set_nested(self, config: Dict, path: list, value: Any):
        """Set nested dictionary value"""
        for key in path[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[path[-1]] = value

    def get(self, *path, default=None) -> Any:
        """
        Get configuration value by path

        Args:
            *path: Path to config value (e.g., "server", "workers")
            default: Default value if not found

        Returns:
            Configuration value or default
        """
        value = self.config
        for key in path:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def get_server_config(self) -> Dict[str, Any]:
        """Get server configuration"""
        return self.config.get("server", {})

    def get_cache_config(self) -> Dict[str, Any]:
        """Get query cache configuration"""
        return self.config.get("query_cache", {})

    def get_auth_config(self) -> Dict[str, Any]:
        """Get authentication configuration"""
        return self.config.get("auth", {})

    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration"""
        return self.config.get("storage", {})

    def get_wal_config(self) -> Dict[str, Any]:
        """Get Write-Ahead Log (WAL) configuration"""
        return self.config.get("wal", {})

    def get_compaction_config(self) -> Dict[str, Any]:
        """Get compaction configuration"""
        return self.config.get("compaction", {})

    def get_ingestion_config(self) -> Dict[str, Any]:
        """Get data ingestion configuration"""
        return self.config.get("ingestion", {})

    def get_telemetry_config(self) -> Dict[str, Any]:
        """Get telemetry configuration"""
        return self.config.get("telemetry", {})

    def dump(self) -> Dict[str, Any]:
        """Get complete configuration (for debugging)"""
        return self.config.copy()

    def print_config(self):
        """Print configuration summary"""
        print("=" * 60)
        print("Arc Configuration")
        print("=" * 60)
        print(f"Config file: {self.config_file}")
        print(f"Server: {self.get('server', 'host')}:{self.get('server', 'port')}")
        print(f"Workers: {self.get('server', 'workers')}")
        print(f"Auth enabled: {self.get('auth', 'enabled')}")
        print(f"Cache enabled: {self.get('query_cache', 'enabled')}")
        print(f"Cache TTL: {self.get('query_cache', 'ttl_seconds')}s")
        print(f"Storage backend: {self.get('storage', 'backend')}")
        print("=" * 60)


# Global config instance
_config: Optional[ArcConfig] = None


def load_config(config_file: str = None) -> ArcConfig:
    """
    Load global configuration

    Args:
        config_file: Path to arc.conf file

    Returns:
        ArcConfig instance
    """
    global _config
    _config = ArcConfig(config_file=config_file)
    return _config


def get_config() -> ArcConfig:
    """
    Get global configuration instance

    Returns:
        ArcConfig instance (loads if not already loaded)
    """
    global _config
    if _config is None:
        _config = ArcConfig()
    return _config


if __name__ == "__main__":
    # Test configuration loading
    config = load_config()
    config.print_config()
