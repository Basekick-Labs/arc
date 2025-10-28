"""
Simple storage connection manager for Arc Core.
Handles MinIO, S3, GCS, Ceph, and Local storage backends only.
"""
import sqlite3
import json
import logging
import threading
from typing import Dict, Optional
from pathlib import Path
from contextlib import contextmanager
from queue import Queue, Empty
from .config import get_db_path

logger = logging.getLogger(__name__)


class SQLiteConnectionPool:
    """Thread-safe SQLite connection pool (singleton pattern)"""

    def __init__(self, db_path: str, max_connections: int = 10, timeout: int = 30):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self._pool = Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()
        self._initialized = False

    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new SQLite connection with optimizations"""
        try:
            # Use longer timeout for initial connection (handles multi-worker startup contention)
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,  # Allow use across threads
                timeout=60.0  # Longer timeout for startup race conditions
            )
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            # Set busy timeout (in milliseconds)
            conn.execute(f"PRAGMA busy_timeout={60000}")  # 60 seconds
            # Increase cache size
            conn.execute("PRAGMA cache_size=10000")
            conn.row_factory = sqlite3.Row  # Enable dict-like access

            with self._lock:
                self._created_connections += 1

            logger.debug(f"Created SQLite connection ({self._created_connections}/{self.max_connections})")
            return conn

        except Exception as e:
            logger.error(f"Failed to create SQLite connection: {e}")
            return None

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool with automatic return"""
        conn = None
        try:
            # Try to get existing connection
            try:
                conn = self._pool.get(timeout=1.0)
            except Empty:
                # Create new connection if pool is empty and we haven't hit max
                if self._created_connections < self.max_connections:
                    conn = self._create_connection()
                else:
                    # Wait longer for an available connection
                    conn = self._pool.get(timeout=self.timeout)

            if conn is None:
                raise Exception("Could not obtain database connection")

            # Test connection
            try:
                conn.execute("SELECT 1").fetchone()
            except sqlite3.Error:
                # Connection is stale, create a new one
                logger.warning("Stale connection detected, creating new one")
                conn.close()
                with self._lock:
                    self._created_connections -= 1
                conn = self._create_connection()
                if not conn:
                    raise Exception("Could not create replacement connection")

            # Yield the valid connection
            yield conn

        finally:
            # Return connection to pool
            if conn:
                try:
                    # Reset any ongoing transactions
                    conn.rollback()
                    self._pool.put(conn, timeout=1.0)
                except (Empty, sqlite3.Error):
                    # Pool is full or connection is bad, just close it
                    conn.close()
                    with self._lock:
                        self._created_connections -= 1

    def close_all(self):
        """Close all connections in the pool"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                with self._lock:
                    self._created_connections -= 1
            except Empty:
                break
        logger.info("All SQLite connections closed")


# Global connection pool (singleton)
_storage_pool: Optional[SQLiteConnectionPool] = None
_storage_pool_lock = threading.Lock()


def get_storage_pool(db_path: str = None) -> SQLiteConnectionPool:
    """Get the global storage SQLite connection pool (singleton)"""
    global _storage_pool
    with _storage_pool_lock:
        if _storage_pool is None:
            path = db_path or get_db_path()
            _storage_pool = SQLiteConnectionPool(path)
            logger.debug(f"Initialized storage connection pool for {path}")
        return _storage_pool


class StorageManager:
    """Manages storage backend connections (MinIO/S3/GCS/Ceph/Local)"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_db_path()
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database with storage connections table"""
        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

            # Use singleton connection pool
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                # Create storage connections table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS storage_connections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        backend TEXT NOT NULL,  -- 'minio', 's3', 'gcs', 'ceph', 'local'
                        endpoint TEXT,
                        access_key TEXT,
                        secret_key TEXT,
                        bucket TEXT,
                        region TEXT,
                        path TEXT,  -- For local backend
                        ssl BOOLEAN DEFAULT TRUE,
                        is_active BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                conn.commit()
                logger.debug("Storage manager database initialized")

        except Exception as e:
            logger.error(f"Failed to initialize storage manager database: {e}")
            raise

    def get_active_storage_connection(self) -> Optional[Dict]:
        """Get the currently active storage connection"""
        try:
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT * FROM storage_connections
                    WHERE is_active = 1
                    LIMIT 1
                ''')

                row = cursor.fetchone()

                if row:
                    return dict(row)
                return None

        except Exception as e:
            logger.error(f"Failed to get active storage connection: {e}")
            return None

    def get_storage_connections(self):
        """Get all storage connections"""
        try:
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute('SELECT * FROM storage_connections ORDER BY created_at DESC')
                rows = cursor.fetchall()

                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Failed to get storage connections: {e}")
            return []

    def add_storage_connection(self, config: Dict) -> int:
        """Add a new storage connection"""
        try:
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    INSERT OR IGNORE INTO storage_connections
                    (name, backend, endpoint, access_key, secret_key, bucket, region, path, ssl, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    config.get('name'),
                    config.get('backend'),
                    config.get('endpoint'),
                    config.get('access_key'),
                    config.get('secret_key'),
                    config.get('bucket'),
                    config.get('region'),
                    config.get('path'),
                    config.get('ssl', True),
                    False  # Always insert as inactive initially
                ))

                # Check if insert actually happened (lastrowid will be 0 if ignored due to duplicate)
                if cursor.lastrowid > 0:
                    connection_id = cursor.lastrowid
                    logger.info(f"Added storage connection: {config.get('name')} (ID: {connection_id})")
                else:
                    # Connection already exists, get its ID
                    cursor.execute('SELECT id FROM storage_connections WHERE name = ?', (config.get('name'),))
                    row = cursor.fetchone()
                    connection_id = row[0] if row else None
                    logger.debug(f"Storage connection already exists: {config.get('name')} (ID: {connection_id})")

                # If is_active was requested, activate this connection (and deactivate others)
                if config.get('is_active') and connection_id:
                    cursor.execute('UPDATE storage_connections SET is_active = 0')
                    cursor.execute('UPDATE storage_connections SET is_active = 1 WHERE id = ?', (connection_id,))
                    logger.debug(f"Activated storage connection: {config.get('name')} (ID: {connection_id})")

                conn.commit()
                return connection_id

        except Exception as e:
            logger.error(f"Failed to add storage connection: {e}")
            raise

    def set_active_connection(self, connection_type: str, connection_id: int) -> bool:
        """Set a connection as active (only supports 'storage' type)"""
        if connection_type != 'storage':
            return False

        try:
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                # Deactivate all connections
                cursor.execute('UPDATE storage_connections SET is_active = 0')

                # Activate the specified connection
                cursor.execute(
                    'UPDATE storage_connections SET is_active = 1 WHERE id = ?',
                    (connection_id,)
                )

                conn.commit()

                logger.info(f"Set storage connection {connection_id} as active")
                return True

        except Exception as e:
            logger.error(f"Failed to set active connection: {e}")
            return False

    def create_storage_connection(self, config: Dict) -> int:
        """Create a new storage connection"""
        return self.add_storage_connection(config)

    def test_storage_connection(self, config: Dict) -> Dict:
        """Test a storage connection"""
        try:
            backend = config.get('backend')

            if backend == 'minio':
                from storage.minio_backend import MinIOBackend
                storage = MinIOBackend(
                    endpoint=config.get('endpoint'),
                    access_key=config.get('access_key'),
                    secret_key=config.get('secret_key'),
                    bucket=config.get('bucket'),
                    ssl=config.get('ssl', False)
                )
            elif backend == 's3':
                from storage.s3_backend import S3Backend
                storage = S3Backend(
                    bucket=config.get('bucket'),
                    region=config.get('region', 'us-east-1'),
                    access_key=config.get('access_key'),
                    secret_key=config.get('secret_key')
                )
            elif backend == 'gcs':
                from storage.gcs_backend import GCSBackend
                storage = GCSBackend(
                    bucket=config.get('bucket'),
                    credentials_path=config.get('credentials_path')
                )
            elif backend == 'local':
                from storage.local_backend import LocalBackend
                storage = LocalBackend(
                    base_path=config.get('path', './data')
                )
            else:
                return {"success": False, "message": f"Unknown backend: {backend}"}

            # Try to list objects (basic connectivity test)
            storage.list_objects(prefix='')

            return {"success": True, "message": f"{backend.upper()} connection successful"}

        except Exception as e:
            logger.error(f"Storage connection test failed: {e}")
            return {"success": False, "message": str(e)}

    def update_storage_connection(self, connection_id: int, config: Dict) -> bool:
        """Update an existing storage connection"""
        try:
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    UPDATE storage_connections SET
                        name = ?,
                        backend = ?,
                        endpoint = ?,
                        access_key = ?,
                        secret_key = ?,
                        bucket = ?,
                        region = ?,
                        path = ?,
                        ssl = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    config.get('name'),
                    config.get('backend'),
                    config.get('endpoint'),
                    config.get('access_key'),
                    config.get('secret_key'),
                    config.get('bucket'),
                    config.get('region'),
                    config.get('path'),
                    config.get('ssl', True),
                    connection_id
                ))

                conn.commit()
                affected = cursor.rowcount

                return affected > 0

        except Exception as e:
            logger.error(f"Failed to update storage connection: {e}")
            return False

    def delete_connection(self, connection_type: str, connection_id: int) -> bool:
        """Delete a connection (only supports 'storage' type)"""
        if connection_type != 'storage':
            return False

        try:
            with get_storage_pool(self.db_path).get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute('DELETE FROM storage_connections WHERE id = ?', (connection_id,))

                conn.commit()
                affected = cursor.rowcount

                return affected > 0

        except Exception as e:
            logger.error(f"Failed to delete storage connection: {e}")
            return False
