"""
DuckDB Connection Pool with Query Queue

Provides thread-safe connection pooling for concurrent query execution
with overflow queue management and health monitoring.

Architecture:
- Connection Pool: Fixed-size pool of DuckDB connections
- Priority Queue: Handles overflow when pool is full
- Health Checks: Monitors connection validity
- Metrics: Tracks pool usage, queue depth, query latency
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from queue import Queue, Empty, Full
from threading import Lock, RLock
from typing import Dict, Optional, Any, List, Callable
import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)


class QueryPriority(Enum):
    """Query priority levels"""
    LOW = 1      # Background/batch queries
    NORMAL = 2   # Standard user queries
    HIGH = 3     # Interactive/dashboard queries
    CRITICAL = 4 # Health checks, system queries


@dataclass
class QueuedQuery:
    """Represents a queued query waiting for execution"""
    sql: str
    priority: QueryPriority
    submitted_at: float
    timeout: float
    callback: Optional[Callable] = None
    query_id: str = field(default_factory=lambda: f"q_{int(time.time() * 1000)}")

    def __lt__(self, other):
        """Priority queue comparison - higher priority first, then FIFO"""
        if self.priority.value != other.priority.value:
            return self.priority.value > other.priority.value
        return self.submitted_at < other.submitted_at

    def is_expired(self) -> bool:
        """Check if query has exceeded timeout"""
        return (time.time() - self.submitted_at) > self.timeout


@dataclass
class ConnectionStats:
    """Statistics for a single connection"""
    connection_id: int
    created_at: float
    total_queries: int = 0
    failed_queries: int = 0
    total_execution_time: float = 0.0
    last_used: Optional[float] = None
    is_healthy: bool = True
    current_query: Optional[str] = None


@dataclass
class PoolMetrics:
    """Pool-wide metrics"""
    pool_size: int
    active_connections: int
    idle_connections: int
    queue_depth: int
    total_queries_executed: int
    total_queries_queued: int
    total_queries_failed: int
    total_queries_timeout: int
    avg_wait_time_ms: float
    avg_execution_time_ms: float
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class DuckDBConnection:
    """Wrapper for DuckDB connection with health tracking"""

    def __init__(self, connection_id: int, configure_fn: Optional[Callable] = None):
        self.stats = ConnectionStats(
            connection_id=connection_id,
            created_at=time.time()
        )
        self.lock = Lock()
        self.conn = duckdb.connect()

        # Apply configuration if provided
        if configure_fn:
            try:
                configure_fn(self.conn)
                logger.debug(f"Connection {connection_id} configured successfully")
            except Exception as e:
                logger.error(f"Failed to configure connection {connection_id}: {e}")
                self.stats.is_healthy = False

    def execute(self, sql: str) -> Any:
        """Execute query with stats tracking"""
        with self.lock:
            if not self.stats.is_healthy:
                raise RuntimeError(f"Connection {self.stats.connection_id} is unhealthy")

            self.stats.current_query = sql[:100]  # Store first 100 chars
            start_time = time.time()

            try:
                result = self.conn.execute(sql).fetchall()
                columns = [desc[0] for desc in self.conn.description]

                execution_time = time.time() - start_time
                self.stats.total_queries += 1
                self.stats.total_execution_time += execution_time
                self.stats.last_used = time.time()
                self.stats.current_query = None

                return result, columns

            except Exception as e:
                self.stats.failed_queries += 1
                self.stats.current_query = None
                raise e

    def execute_arrow(self, sql: str) -> Any:
        """Execute query and return Arrow table (columnar format)"""
        with self.lock:
            if not self.stats.is_healthy:
                raise RuntimeError(f"Connection {self.stats.connection_id} is unhealthy")

            self.stats.current_query = sql[:100]  # Store first 100 chars
            start_time = time.time()

            try:
                arrow_table = self.conn.execute(sql).fetch_arrow_table()

                execution_time = time.time() - start_time
                self.stats.total_queries += 1
                self.stats.total_execution_time += execution_time
                self.stats.last_used = time.time()
                self.stats.current_query = None

                return arrow_table

            except Exception as e:
                self.stats.failed_queries += 1
                self.stats.current_query = None
                raise e

    def health_check(self) -> bool:
        """Verify connection is healthy"""
        try:
            with self.lock:
                self.conn.execute("SELECT 1").fetchall()
                self.stats.is_healthy = True
                return True
        except Exception as e:
            logger.error(f"Connection {self.stats.connection_id} health check failed: {e}")
            self.stats.is_healthy = False
            return False

    def close(self):
        """Close the connection"""
        try:
            with self.lock:
                self.conn.close()
                self.stats.is_healthy = False
        except Exception as e:
            logger.error(f"Error closing connection {self.stats.connection_id}: {e}")


class DuckDBConnectionPool:
    """
    Thread-safe connection pool for DuckDB with query queue management.

    Features:
    - Fixed-size connection pool
    - Priority-based query queue
    - Connection health monitoring
    - Automatic retry on connection failure
    - Comprehensive metrics

    Example:
        pool = DuckDBConnectionPool(pool_size=5, max_queue_size=100)

        # Execute high-priority query
        result = await pool.execute_async(
            "SELECT * FROM my_table LIMIT 1000",
            priority=QueryPriority.HIGH,
            timeout=30.0
        )

        # Get pool metrics
        metrics = pool.get_metrics()
        print(f"Queue depth: {metrics.queue_depth}")
    """

    def __init__(
        self,
        pool_size: int = 5,
        max_queue_size: int = 100,
        health_check_interval: int = 60,
        configure_fn: Optional[Callable] = None
    ):
        """
        Initialize connection pool.

        Args:
            pool_size: Number of DuckDB connections to maintain
            max_queue_size: Maximum queries to queue before rejecting
            health_check_interval: Seconds between health checks
            configure_fn: Function to configure each connection (e.g., S3 settings)
        """
        self.pool_size = pool_size
        self.max_queue_size = max_queue_size
        self.configure_fn = configure_fn
        self.health_check_interval = health_check_interval

        # Connection pool
        self.pool: Queue[DuckDBConnection] = Queue(maxsize=pool_size)
        self.connections: List[DuckDBConnection] = []
        self.pool_lock = RLock()

        # Query queue with priority
        self.query_queue: deque[QueuedQuery] = deque()
        self.queue_lock = Lock()

        # Metrics
        self.total_queries_executed = 0
        self.total_queries_queued = 0
        self.total_queries_failed = 0
        self.total_queries_timeout = 0
        self.wait_times: deque[float] = deque(maxlen=1000)
        self.execution_times: deque[float] = deque(maxlen=1000)

        # Initialize pool
        self._initialize_pool()

        # Start background health checker
        self._health_check_task = None

        logger.debug(f"DuckDB connection pool initialized: {pool_size} connections, max queue: {max_queue_size}")

    def _initialize_pool(self):
        """Create initial pool of connections"""
        for i in range(self.pool_size):
            try:
                conn = DuckDBConnection(connection_id=i, configure_fn=self.configure_fn)
                self.connections.append(conn)
                self.pool.put(conn)
                logger.debug(f"Created DuckDB connection {i+1}/{self.pool_size}")
            except Exception as e:
                logger.error(f"Failed to create DuckDB connection {i}: {e}")
                raise

    def get_connection(self, timeout: float = 5.0) -> Optional[DuckDBConnection]:
        """
        Get a connection from the pool.

        Args:
            timeout: Max seconds to wait for available connection

        Returns:
            DuckDBConnection or None if timeout
        """
        try:
            conn = self.pool.get(timeout=timeout)

            # Verify connection health
            if not conn.stats.is_healthy:
                logger.warning(f"Got unhealthy connection {conn.stats.connection_id}, attempting recovery")
                if not conn.health_check():
                    # Try to recreate connection
                    try:
                        conn.close()
                        conn = DuckDBConnection(
                            connection_id=conn.stats.connection_id,
                            configure_fn=self.configure_fn
                        )
                        with self.pool_lock:
                            self.connections[conn.stats.connection_id] = conn
                    except Exception as e:
                        logger.error(f"Failed to recreate connection: {e}")
                        self.pool.put(conn)  # Return to pool anyway
                        return None

            return conn

        except Empty:
            logger.warning(f"Connection pool exhausted (timeout: {timeout}s)")
            return None

    def return_connection(self, conn: DuckDBConnection):
        """Return a connection to the pool"""
        try:
            self.pool.put_nowait(conn)
        except Full:
            logger.error("Failed to return connection to pool - pool is full!")

    async def execute_async(
        self,
        sql: str,
        priority: QueryPriority = QueryPriority.NORMAL,
        timeout: float = 300.0
    ) -> Dict[str, Any]:
        """
        Execute query asynchronously with priority queue.

        Args:
            sql: SQL query to execute
            priority: Query priority level
            timeout: Max seconds for query execution (including queue time)

        Returns:
            Dict with success, data, columns, execution_time_ms, wait_time_ms
        """
        query = QueuedQuery(
            sql=sql,
            priority=priority,
            submitted_at=time.time(),
            timeout=timeout
        )

        # Try to get connection immediately
        conn = self.get_connection(timeout=0.1)

        if conn is None:
            # Pool exhausted, add to queue
            with self.queue_lock:
                if len(self.query_queue) >= self.max_queue_size:
                    self.total_queries_failed += 1
                    logger.error(f"Query queue full ({self.max_queue_size}), rejecting query")
                    return {
                        "success": False,
                        "error": "Query queue full - system under heavy load",
                        "data": [],
                        "columns": [],
                        "row_count": 0
                    }

                # Add to priority queue
                self.query_queue.append(query)
                self.total_queries_queued += 1
                logger.info(f"Query queued (priority={priority.name}, queue_depth={len(self.query_queue)})")

            # Wait for connection with timeout
            start_wait = time.time()
            while (time.time() - start_wait) < timeout:
                conn = self.get_connection(timeout=1.0)
                if conn:
                    break

                # Check if query expired
                if query.is_expired():
                    with self.queue_lock:
                        try:
                            self.query_queue.remove(query)
                        except ValueError:
                            pass
                    self.total_queries_timeout += 1
                    logger.warning(f"Query {query.query_id} timed out in queue")
                    return {
                        "success": False,
                        "error": f"Query timeout ({timeout}s) - consider reducing query complexity",
                        "data": [],
                        "columns": [],
                        "row_count": 0
                    }

            if conn is None:
                self.total_queries_timeout += 1
                return {
                    "success": False,
                    "error": "Timeout waiting for database connection",
                    "data": [],
                    "columns": [],
                    "row_count": 0
                }

            wait_time = time.time() - start_wait
            self.wait_times.append(wait_time)
        else:
            wait_time = 0.0

        # Execute query
        result = None
        columns = None
        exec_time = 0.0

        try:
            loop = asyncio.get_event_loop()
            start_exec = time.time()

            result, columns = await loop.run_in_executor(None, conn.execute, sql)

            exec_time = time.time() - start_exec
            self.execution_times.append(exec_time)
            self.total_queries_executed += 1

        except Exception as e:
            self.total_queries_failed += 1
            logger.error(f"Query execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0
            }

        finally:
            # OPTIMIZATION: Return connection ASAP (before serialization)
            # This allows other queries to use the connection while we serialize
            self.return_connection(conn)

        # Serialize data for JSON AFTER releasing connection
        serialized_data = []
        for row in result:
            serialized_row = []
            for value in row:
                if hasattr(value, 'isoformat'):
                    serialized_row.append(value.isoformat())
                elif isinstance(value, (int, float, str, bool)) or value is None:
                    serialized_row.append(value)
                else:
                    serialized_row.append(str(value))
            serialized_data.append(serialized_row)

        logger.info(f"Query executed: {len(result)} rows in {exec_time:.3f}s (wait: {wait_time:.3f}s)")

        return {
            "success": True,
            "data": serialized_data,
            "columns": columns,
            "row_count": len(result),
            "execution_time_ms": round(exec_time * 1000, 2),
            "wait_time_ms": round(wait_time * 1000, 2)
        }

    async def execute_arrow_async(
        self,
        sql: str,
        priority: QueryPriority = QueryPriority.NORMAL,
        timeout: float = 300.0
    ) -> Dict[str, Any]:
        """
        Execute query asynchronously and return Apache Arrow table (columnar format).

        Args:
            sql: SQL query to execute
            priority: Query priority level
            timeout: Max seconds for query execution (including queue time)

        Returns:
            Dict with success, arrow_table (bytes), schema, row_count, execution_time_ms, wait_time_ms
        """
        query = QueuedQuery(
            sql=sql,
            priority=priority,
            submitted_at=time.time(),
            timeout=timeout
        )

        # Try to get connection immediately
        conn = self.get_connection(timeout=0.1)

        if conn is None:
            # Pool exhausted, add to queue
            with self.queue_lock:
                if len(self.query_queue) >= self.max_queue_size:
                    self.total_queries_failed += 1
                    logger.error(f"Query queue full ({self.max_queue_size}), rejecting query")
                    return {
                        "success": False,
                        "error": "Query queue full - system under heavy load",
                        "arrow_table": None,
                        "row_count": 0
                    }

                # Add to priority queue
                self.query_queue.append(query)
                self.total_queries_queued += 1
                logger.info(f"Query queued (priority={priority.name}, queue_depth={len(self.query_queue)})")

            # Wait for connection with timeout
            start_wait = time.time()
            while (time.time() - start_wait) < timeout:
                conn = self.get_connection(timeout=1.0)
                if conn:
                    break

                # Check if query expired
                if query.is_expired():
                    with self.queue_lock:
                        try:
                            self.query_queue.remove(query)
                        except ValueError:
                            pass
                    self.total_queries_timeout += 1
                    logger.warning(f"Query {query.query_id} timed out in queue")
                    return {
                        "success": False,
                        "error": f"Query timeout ({timeout}s) - consider reducing query complexity",
                        "arrow_table": None,
                        "row_count": 0
                    }

            if conn is None:
                self.total_queries_timeout += 1
                return {
                    "success": False,
                    "error": "Timeout waiting for database connection",
                    "arrow_table": None,
                    "row_count": 0
                }

            wait_time = time.time() - start_wait
            self.wait_times.append(wait_time)
        else:
            wait_time = 0.0

        # Execute query
        arrow_table = None
        exec_time = 0.0

        try:
            loop = asyncio.get_event_loop()
            start_exec = time.time()

            arrow_table = await loop.run_in_executor(None, conn.execute_arrow, sql)

            exec_time = time.time() - start_exec
            self.execution_times.append(exec_time)
            self.total_queries_executed += 1

        except Exception as e:
            self.total_queries_failed += 1
            logger.error(f"Arrow query execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "arrow_table": None,
                "row_count": 0
            }

        finally:
            # OPTIMIZATION: Return connection ASAP (before serialization)
            # This allows other queries to use the connection while we serialize
            self.return_connection(conn)

        # Serialize Arrow table to IPC format AFTER releasing connection
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, arrow_table.schema)
        writer.write_table(arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        logger.info(f"Arrow query executed: {len(arrow_table)} rows in {exec_time:.3f}s (wait: {wait_time:.3f}s)")

        return {
            "success": True,
            "arrow_table": arrow_bytes,
            "row_count": len(arrow_table),
            "execution_time_ms": round(exec_time * 1000, 2),
            "wait_time_ms": round(wait_time * 1000, 2)
        }

    def get_metrics(self) -> PoolMetrics:
        """Get current pool metrics"""
        active = self.pool_size - self.pool.qsize()

        avg_wait = sum(self.wait_times) / len(self.wait_times) if self.wait_times else 0.0
        avg_exec = sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0.0

        return PoolMetrics(
            pool_size=self.pool_size,
            active_connections=active,
            idle_connections=self.pool.qsize(),
            queue_depth=len(self.query_queue),
            total_queries_executed=self.total_queries_executed,
            total_queries_queued=self.total_queries_queued,
            total_queries_failed=self.total_queries_failed,
            total_queries_timeout=self.total_queries_timeout,
            avg_wait_time_ms=round(avg_wait * 1000, 2),
            avg_execution_time_ms=round(avg_exec * 1000, 2)
        )

    def get_connection_stats(self) -> List[Dict[str, Any]]:
        """Get per-connection statistics"""
        stats = []
        with self.pool_lock:
            for conn in self.connections:
                stats.append({
                    "connection_id": conn.stats.connection_id,
                    "is_healthy": conn.stats.is_healthy,
                    "total_queries": conn.stats.total_queries,
                    "failed_queries": conn.stats.failed_queries,
                    "avg_execution_time_ms": round(
                        (conn.stats.total_execution_time / conn.stats.total_queries * 1000)
                        if conn.stats.total_queries > 0 else 0.0,
                        2
                    ),
                    "last_used": conn.stats.last_used,
                    "current_query": conn.stats.current_query
                })
        return stats

    async def health_check_loop(self):
        """Background task to periodically check connection health"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                logger.debug("Running connection pool health check")

                with self.pool_lock:
                    for conn in self.connections:
                        if not conn.health_check():
                            logger.warning(f"Connection {conn.stats.connection_id} failed health check")

            except asyncio.CancelledError:
                logger.info("Health check loop cancelled")
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")

    def start_health_checks(self):
        """Start background health checking"""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self.health_check_loop())
            logger.info("Connection pool health checks started")

    def stop_health_checks(self):
        """Stop background health checking"""
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None
            logger.info("Connection pool health checks stopped")

    def close(self):
        """Close all connections in pool"""
        self.stop_health_checks()

        with self.pool_lock:
            for conn in self.connections:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection {conn.stats.connection_id}: {e}")

        logger.info("Connection pool closed")
