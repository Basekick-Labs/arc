"""
Connection Pool Manager for efficient database and InfluxDB connections
"""
import sqlite3
import threading
import time
from typing import Dict, Optional, Any
from contextlib import contextmanager
from queue import Queue, Empty
from dataclasses import dataclass
from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient as InfluxDBClient2x
import logging

logger = logging.getLogger(__name__)

@dataclass
class InfluxConnectionConfig:
    """Configuration for InfluxDB connections"""
    host: str
    port: int
    username: str = ""
    password: str = ""
    database: str = ""
    ssl: bool = False
    # InfluxDB 2.x specific
    url: str = ""
    token: str = ""
    org: str = ""
    bucket: str = ""
    version: str = "1x"  # "1x" or "2x"

class SQLiteConnectionPool:
    """Thread-safe SQLite connection pool"""
    
    def __init__(self, db_path: str, max_connections: int = 10, timeout: int = 30):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self._pool = Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()
        
        # Pre-create some connections
        self._initialize_pool()
        
    def _initialize_pool(self):
        """Pre-create initial connections"""
        initial_size = min(3, self.max_connections)  # Create 3 initial connections
        for _ in range(initial_size):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new SQLite connection with optimizations"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,  # Allow use across threads
                timeout=self.timeout
            )
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            # Increase cache size
            conn.execute("PRAGMA cache_size=10000")
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys=ON")
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

class InfluxDBConnectionPool:
    """Thread-safe InfluxDB connection pool"""
    
    def __init__(self, max_connections: int = 5):
        self.max_connections = max_connections
        self._pools: Dict[str, Queue] = {}  # key -> connection pool
        self._configs: Dict[str, InfluxConnectionConfig] = {}  # key -> config
        self._created_counts: Dict[str, int] = {}  # key -> created count
        self._lock = threading.Lock()
    
    def _get_connection_key(self, config: InfluxConnectionConfig) -> str:
        """Generate unique key for connection configuration"""
        if config.version == "2x":
            return f"influx2x:{config.url}:{config.org}:{config.bucket}"
        else:
            return f"influx1x:{config.host}:{config.port}:{config.database}"
    
    def _create_connection(self, config: InfluxConnectionConfig):
        """Create new InfluxDB connection"""
        try:
            if config.version == "2x":
                client = InfluxDBClient2x(
                    url=config.url,
                    token=config.token,
                    org=config.org
                )
                # Test connection
                client.ping()
                return client
            else:
                client = InfluxDBClient(
                    host=config.host,
                    port=config.port,
                    username=config.username,
                    password=config.password,
                    database=config.database,
                    ssl=config.ssl
                )
                # Test connection
                client.ping()
                return client
                
        except Exception as e:
            logger.error(f"Failed to create InfluxDB connection: {e}")
            return None
    
    def register_connection(self, config: InfluxConnectionConfig) -> str:
        """Register a new connection configuration"""
        key = self._get_connection_key(config)
        
        with self._lock:
            if key not in self._pools:
                self._pools[key] = Queue(maxsize=self.max_connections)
                self._configs[key] = config
                self._created_counts[key] = 0
                
                # Pre-create one connection
                conn = self._create_connection(config)
                if conn:
                    self._pools[key].put(conn)
                    self._created_counts[key] = 1
                    
        logger.info(f"Registered InfluxDB connection pool: {key}")
        return key
    
    @contextmanager
    def get_connection(self, key: str):
        """Get connection from specific pool"""
        if key not in self._pools:
            raise ValueError(f"Connection pool '{key}' not registered")
            
        conn = None
        try:
            pool = self._pools[key]
            config = self._configs[key]
            
            # Try to get existing connection
            try:
                conn = pool.get(timeout=1.0)
            except Empty:
                # Create new connection if possible
                if self._created_counts[key] < self.max_connections:
                    conn = self._create_connection(config)
                    if conn:
                        with self._lock:
                            self._created_counts[key] += 1
                else:
                    # Wait for available connection
                    conn = pool.get(timeout=30.0)
            
            if conn is None:
                raise Exception(f"Could not obtain InfluxDB connection for {key}")
                
            yield conn
            
        finally:
            # Return connection to pool
            if conn:
                try:
                    pool.put(conn, timeout=1.0)
                except Empty:
                    # Pool is full, close the connection
                    if hasattr(conn, 'close'):
                        conn.close()
                    with self._lock:
                        self._created_counts[key] -= 1
    
    def close_all(self):
        """Close all connection pools"""
        with self._lock:
            for key, pool in self._pools.items():
                while not pool.empty():
                    try:
                        conn = pool.get_nowait()
                        if hasattr(conn, 'close'):
                            conn.close()
                    except Empty:
                        break
                self._created_counts[key] = 0
        logger.info("All InfluxDB connection pools closed")

# Global connection pools (singletons)
_sqlite_pool: Optional[SQLiteConnectionPool] = None
_influx_pool = InfluxDBConnectionPool()

def get_sqlite_pool() -> SQLiteConnectionPool:
    """Get the global SQLite connection pool"""
    global _sqlite_pool
    if _sqlite_pool is None:
        # Default database path, should be configured via environment or config
        from .config import get_db_path
        db_path = get_db_path()
        _sqlite_pool = SQLiteConnectionPool(db_path)
    return _sqlite_pool

def get_influx_pool() -> InfluxDBConnectionPool:
    """Get the global InfluxDB connection pool"""
    return _influx_pool

def initialize_pools(sqlite_db_path: str):
    """Initialize connection pools with specific configuration"""
    global _sqlite_pool
    _sqlite_pool = SQLiteConnectionPool(sqlite_db_path)
    logger.info("Connection pools initialized")

def close_all_pools():
    """Close all connection pools"""
    if _sqlite_pool:
        _sqlite_pool.close_all()
    _influx_pool.close_all()
    logger.info("All connection pools closed")