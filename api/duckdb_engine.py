import asyncio
import logging
from typing import Dict, List, Any, Optional
import time
import os
import multiprocessing
from api.duckdb_pool_simple import SimpleDuckDBPool
from api.partition_pruner import PartitionPruner

logger = logging.getLogger(__name__)

class DuckDBEngine:
    def __init__(self, storage_backend: str = "local", local_backend=None, minio_backend=None, s3_backend=None, ceph_backend=None, gcs_backend=None, connection_manager=None):
        self.storage_backend = storage_backend
        self.local_backend = local_backend
        self.minio_backend = minio_backend
        self.s3_backend = s3_backend
        self.ceph_backend = ceph_backend
        self.gcs_backend = gcs_backend
        self.connection_manager = connection_manager

        # Initialize partition pruner for file-level optimization
        self.partition_pruner = PartitionPruner()
        self.current_sql = None  # Store original SQL for pruning context

        try:
            import duckdb
            self.duckdb = duckdb

            # Create DuckDB connection for initialization (will be replaced by pool)
            self.conn = duckdb.connect()

            # Install and load extensions for S3 access
            try:
                self.conn.execute("INSTALL httpfs")
                self.conn.execute("LOAD httpfs")
                self.conn.execute("INSTALL aws")
                self.conn.execute("LOAD aws")
                logger.debug("DuckDB httpfs and aws extensions loaded")
            except Exception as e:
                logger.warning(f"Failed to load httpfs extension: {e}")
                # Try enabling autoloading as fallback
                try:
                    self.conn.execute("SET autoinstall_known_extensions=1")
                    self.conn.execute("SET autoload_known_extensions=1")
                    logger.debug("DuckDB extension autoloading enabled")
                except Exception as e2:
                    logger.error(f"Failed to enable autoloading: {e2}")

            # Configure for S3/MinIO/Ceph/GCS if available (sync for immediate use)
            if minio_backend or s3_backend or ceph_backend:
                self._configure_s3_sync()
            elif gcs_backend:
                self._configure_gcs_sync()

            # Initialize simplified connection pool (with aggressive memory cleanup)
            pool_size = int(os.getenv('DUCKDB_POOL_SIZE', '5'))

            self.connection_pool = SimpleDuckDBPool(
                pool_size=pool_size,
                configure_fn=self._configure_connection
            )

            logger.info(f"DuckDB engine initialized with simplified connection pool (size={pool_size})")

        except ImportError:
            logger.error("DuckDB not installed. Run: pip install duckdb")
            raise
        except Exception as e:
            logger.error(f"DuckDB initialization failed: {e}")
            raise

    def _configure_connection(self, conn):
        """Configure a single DuckDB connection for the pool"""
        try:
            # Install and load extensions
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
            conn.execute("INSTALL aws")
            conn.execute("LOAD aws")

            # Enable object cache (Parquet metadata caching for repeated queries)
            enable_cache = os.getenv('DUCKDB_ENABLE_OBJECT_CACHE', 'true').lower() == 'true'
            conn.execute(f"SET enable_object_cache={'true' if enable_cache else 'false'}")

            # Performance optimizations: Enable parallelism and set memory limits
            # By default, use all available CPU cores for parallel query execution
            # Set DUCKDB_THREADS=1 to disable parallelism (single-threaded)
            # Set DUCKDB_THREADS=4 to use exactly 4 threads
            threads_env = os.getenv('DUCKDB_THREADS')
            if threads_env:
                threads = int(threads_env)
            else:
                # Default: use all available CPU cores
                threads = multiprocessing.cpu_count()

            conn.execute(f"SET threads={threads}")

            # Set memory limit for DuckDB (only if explicitly configured)
            # No default: Let DuckDB use unlimited memory with garbage collection cleanup
            memory_limit = os.getenv('DUCKDB_MEMORY_LIMIT')
            if memory_limit:
                conn.execute(f"SET memory_limit='{memory_limit}'")

            # OPTIMIZATION: Create macro to auto-cast int64 timestamps
            # Allows queries to use time functions without manual CAST()
            # Converts microseconds (int64) to timestamp automatically
            conn.execute("""
                CREATE MACRO IF NOT EXISTS to_timestamp_us(us BIGINT) AS
                    CAST(CAST(us AS TIMESTAMP) AS TIMESTAMP);
            """)

            # Set timezone to UTC for consistency
            conn.execute("SET TimeZone='UTC'")

            # Log configuration prominently
            logger.info(
                f"\033[1;36mDuckDB Performance Config:\033[0m "
                f"\033[1;32mthreads={threads}\033[0m "
                f"(available_cores={multiprocessing.cpu_count()}), "
                f"\033[1;33mmemory_limit={memory_limit}\033[0m, "
                f"object_cache={enable_cache}"
            )

            # Apply S3/MinIO/Ceph/GCS configuration (with SQL injection protection)
            if self.minio_backend:
                endpoint_host = self.minio_backend.s3_client._endpoint.host.replace('http://', '').replace('https://', '')
                access_key = self.minio_backend.s3_client._request_signer._credentials.access_key
                secret_key = self.minio_backend.s3_client._request_signer._credentials.secret_key

                # Sanitize all values to prevent SQL injection
                endpoint_host_safe = self._sanitize_sql_string(endpoint_host)
                access_key_safe = self._sanitize_sql_string(access_key)
                secret_key_safe = self._sanitize_sql_string(secret_key)

                conn.execute(f"SET s3_endpoint='{endpoint_host_safe}'")
                conn.execute(f"SET s3_access_key_id='{access_key_safe}'")
                conn.execute(f"SET s3_secret_access_key='{secret_key_safe}'")
                conn.execute("SET s3_use_ssl=false")
                conn.execute("SET s3_url_style='path'")
                logger.debug("MinIO S3 configuration applied to DuckDB")

            elif self.ceph_backend:
                endpoint_host = self.ceph_backend.endpoint_url.replace('http://', '').replace('https://', '')

                # Sanitize all values to prevent SQL injection
                endpoint_host_safe = self._sanitize_sql_string(endpoint_host)
                access_key_safe = self._sanitize_sql_string(self.ceph_backend._access_key)
                secret_key_safe = self._sanitize_sql_string(self.ceph_backend._secret_key)

                conn.execute(f"SET s3_endpoint='{endpoint_host_safe}'")
                conn.execute(f"SET s3_access_key_id='{access_key_safe}'")
                conn.execute(f"SET s3_secret_access_key='{secret_key_safe}'")
                conn.execute("SET s3_use_ssl=false")
                conn.execute("SET s3_url_style='path'")
                logger.debug("Ceph S3 configuration applied to DuckDB")

            elif self.s3_backend:
                # Sanitize all values to prevent SQL injection
                access_key_safe = self._sanitize_sql_string(self.s3_backend._access_key)
                secret_key_safe = self._sanitize_sql_string(self.s3_backend._secret_key)
                region_safe = self._sanitize_sql_string(self.s3_backend.region)

                if self.s3_backend.use_directory_bucket:
                    endpoint = f"s3express-{self.s3_backend.availability_zone}.{self.s3_backend.region}.amazonaws.com"
                    endpoint_safe = self._sanitize_sql_string(endpoint)
                    conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{access_key_safe}',
                            SECRET '{secret_key_safe}',
                            REGION '{region_safe}',
                            ENDPOINT '{endpoint_safe}'
                        )
                    """)
                else:
                    conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{access_key_safe}',
                            SECRET '{secret_key_safe}',
                            REGION '{region_safe}'
                        )
                    """)
                logger.debug("AWS S3 configuration applied to DuckDB")

            elif self.gcs_backend:
                if not self.gcs_backend.configure_duckdb(conn):
                    logger.error("Failed to configure DuckDB for GCS access")

        except Exception as e:
            logger.error(f"Connection configuration failed: {e}")
            raise

    def _sanitize_sql_string(self, value: str) -> str:
        """
        Sanitize a string value for use in SQL by escaping single quotes.
        This prevents SQL injection in SET commands where parameterization isn't available.

        Args:
            value: String value to sanitize

        Returns:
            Sanitized string safe for SQL interpolation

        Raises:
            ValueError: If value contains dangerous characters
        """
        if not value:
            raise ValueError("Value cannot be empty")

        # Check for dangerous characters that could break out of quotes
        dangerous_chars = [';', '\x00', '\n', '\r']
        for char in dangerous_chars:
            if char in value:
                raise ValueError(f"Invalid character in value: {repr(char)}")

        # Escape single quotes by doubling them (SQL standard)
        return value.replace("'", "''")

    def _is_delete_enabled(self) -> bool:
        """Check if delete operations are enabled in configuration"""
        try:
            from config_loader import get_config
            config = get_config()
            return config.get("delete", "enabled", default=False)
        except Exception as e:
            logger.debug(f"Could not check delete config: {e}")
            return False

    async def _configure_s3(self):
        """Configure DuckDB for S3/MinIO access"""
        try:
            if self.minio_backend:
                await self.minio_backend.configure_duckdb_s3(self.conn)
            elif self.s3_backend:
                await self.s3_backend.configure_duckdb_s3(self.conn)
            elif self.ceph_backend:
                await self.ceph_backend.configure_duckdb_s3(self.conn)
        except Exception as e:
            logger.warning(f"S3 configuration failed: {e}")
    
    def _configure_s3_sync(self):
        """Synchronous S3/MinIO configuration for immediate use (with SQL injection protection)"""
        try:
            if self.minio_backend:
                # Configure MinIO settings directly
                endpoint_host = self.minio_backend.s3_client._endpoint.host.replace('http://', '').replace('https://', '')
                access_key = self.minio_backend.s3_client._request_signer._credentials.access_key
                secret_key = self.minio_backend.s3_client._request_signer._credentials.secret_key

                # Sanitize all values to prevent SQL injection
                endpoint_host_safe = self._sanitize_sql_string(endpoint_host)
                access_key_safe = self._sanitize_sql_string(access_key)
                secret_key_safe = self._sanitize_sql_string(secret_key)

                self.conn.execute(f"SET s3_endpoint='{endpoint_host_safe}'")
                self.conn.execute(f"SET s3_access_key_id='{access_key_safe}'")
                self.conn.execute(f"SET s3_secret_access_key='{secret_key_safe}'")
                self.conn.execute("SET s3_use_ssl=false")
                self.conn.execute("SET s3_url_style='path'")
                logger.debug("MinIO S3 configuration applied to DuckDB (sync)")

            elif self.ceph_backend:
                # Configure Ceph settings directly
                endpoint_host = self.ceph_backend.endpoint_url.replace('http://', '').replace('https://', '')

                # Sanitize all values to prevent SQL injection
                endpoint_host_safe = self._sanitize_sql_string(endpoint_host)
                access_key_safe = self._sanitize_sql_string(self.ceph_backend._access_key)
                secret_key_safe = self._sanitize_sql_string(self.ceph_backend._secret_key)

                self.conn.execute(f"SET s3_endpoint='{endpoint_host_safe}'")
                self.conn.execute(f"SET s3_access_key_id='{access_key_safe}'")
                self.conn.execute(f"SET s3_secret_access_key='{secret_key_safe}'")
                self.conn.execute("SET s3_use_ssl=false")
                self.conn.execute("SET s3_url_style='path'")
                logger.debug("Ceph S3 configuration applied to DuckDB (sync)")

            elif self.s3_backend:
                # Sanitize all values to prevent SQL injection
                access_key_safe = self._sanitize_sql_string(self.s3_backend._access_key)
                secret_key_safe = self._sanitize_sql_string(self.s3_backend._secret_key)
                region_safe = self._sanitize_sql_string(self.s3_backend.region)

                # Configure S3 using DuckDB SECRET system
                if self.s3_backend.use_directory_bucket:
                    # S3 Express One Zone configuration
                    endpoint = f"s3express-{self.s3_backend.availability_zone}.{self.s3_backend.region}.amazonaws.com"
                    endpoint_safe = self._sanitize_sql_string(endpoint)
                    self.conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{access_key_safe}',
                            SECRET '{secret_key_safe}',
                            REGION '{region_safe}',
                            ENDPOINT '{endpoint_safe}'
                        )
                    """)
                    logger.debug("DuckDB S3 Express SECRET created")
                else:
                    # Standard S3 configuration
                    self.conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{access_key_safe}',
                            SECRET '{secret_key_safe}',
                            REGION '{region_safe}'
                        )
                    """)
                    logger.debug("DuckDB S3 SECRET created")

                logger.debug(f"Directory bucket settings: use_directory_bucket={self.s3_backend.use_directory_bucket}, availability_zone={self.s3_backend.availability_zone}")
                logger.debug("AWS S3 configuration applied to DuckDB (sync)")
        except Exception as e:
            logger.error(f"Sync S3 configuration failed: {e}")
            # Log current S3 settings for debugging
            try:
                if self.s3_backend:
                    logger.error(f"S3 Backend attributes: use_directory_bucket={getattr(self.s3_backend, 'use_directory_bucket', 'NOT_SET')}, availability_zone={getattr(self.s3_backend, 'availability_zone', 'NOT_SET')}")
            except Exception as e:
                logger.debug(f"Could not log S3 backend attributes: {e}")
    
    def _configure_gcs_sync(self):
        """Synchronous GCS configuration for immediate use"""
        try:
            if self.gcs_backend:
                # Configure GCS backend for DuckDB access
                if not self.gcs_backend.configure_duckdb(self.conn):
                    logger.error("Failed to configure DuckDB for GCS access")
                    return

                logger.debug("GCS configuration applied to DuckDB (sync) - using signed URLs for access")
            
        except Exception as e:
            logger.error(f"Sync GCS configuration failed: {e}")
    
    async def execute_query(self, sql: str, limit: int = None) -> Dict[str, Any]:
        """Execute SQL query using simplified DuckDB connection pool

        Args:
            sql: SQL query to execute
            limit: Row limit (deprecated, use LIMIT in SQL)

        Returns:
            Dict with success, data, columns, row_count, execution_time_ms, wait_time_ms
        """
        # Check for SHOW TABLES command BEFORE rewriting SQL (uses old connection)
        if self._is_show_tables_query(sql):
            return await self._execute_show_tables_legacy(sql)

        # Check for SHOW DATABASES command BEFORE rewriting SQL
        if self._is_show_databases_query(sql):
            return await self._execute_show_databases_legacy(sql)

        # Check for DESCRIBE command BEFORE rewriting SQL
        if self._is_describe_query(sql):
            return await self._execute_describe_legacy(sql)

        # Convert SQL for S3 paths (only for regular queries)
        converted_sql = self._convert_sql_to_s3_paths(sql)

        # Use simplified connection pool (with automatic cleanup)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._execute_with_pool, converted_sql)

        return result

    def _execute_with_pool(self, sql: str) -> Dict[str, Any]:
        """Execute query using connection pool (sync wrapper for async)"""
        start_time = time.time()
        try:
            with self.connection_pool.get_connection(timeout=5.0) as conn:
                # Execute query
                query_start = time.time()
                rows = conn.execute(sql).fetchall()
                columns = [desc[0] for desc in conn.description] if conn.description else []
                query_time = time.time() - query_start

                # Convert rows to list immediately
                data = [list(row) for row in rows]
                row_count = len(data)

                # CRITICAL: Delete rows reference immediately to free DuckDB result memory
                del rows

                total_time = time.time() - start_time

                return {
                    "success": True,
                    "data": data,
                    "columns": columns,
                    "row_count": row_count,
                    "execution_time_ms": round(query_time * 1000, 2),
                    "wait_time_ms": round((total_time - query_time) * 1000, 2)
                }
        except TimeoutError as e:
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": round((time.time() - start_time) * 1000, 2)
            }
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": round((time.time() - start_time) * 1000, 2)
            }

    async def execute_query_arrow(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query and return Apache Arrow table (columnar format)

        Args:
            sql: SQL query to execute

        Returns:
            Dict with success, arrow_table (bytes), schema, row_count, execution_time_ms, wait_time_ms
        """
        # Check for SHOW TABLES command BEFORE rewriting SQL
        if self._is_show_tables_query(sql):
            return await self._execute_show_tables_arrow(sql)

        # Check for SHOW DATABASES command BEFORE rewriting SQL
        if self._is_show_databases_query(sql):
            return await self._execute_show_databases_arrow(sql)

        # Check for DESCRIBE command BEFORE rewriting SQL
        if self._is_describe_query(sql):
            return await self._execute_describe_arrow(sql)

        # Convert SQL for S3 paths (only for regular queries)
        converted_sql = self._convert_sql_to_s3_paths(sql)

        # Use simplified connection pool (with automatic cleanup)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._execute_arrow_with_pool, converted_sql)

        return result

    def _execute_arrow_with_pool(self, sql: str) -> Dict[str, Any]:
        """Execute Arrow query using connection pool (sync wrapper for async)"""
        import pyarrow as pa
        start_time = time.time()
        try:
            with self.connection_pool.get_connection(timeout=5.0) as conn:
                # Execute query and get Arrow table
                query_start = time.time()
                arrow_table = conn.execute(sql).fetch_arrow_table()
                query_time = time.time() - query_start

                # Serialize to IPC format
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, arrow_table.schema)
                writer.write_table(arrow_table)
                writer.close()
                arrow_bytes = sink.getvalue().to_pybytes()

                total_time = time.time() - start_time

                return {
                    "success": True,
                    "arrow_table": arrow_bytes,
                    "row_count": len(arrow_table),
                    "execution_time_ms": round(query_time * 1000, 2),
                    "wait_time_ms": round((total_time - query_time) * 1000, 2)
                }
        except TimeoutError as e:
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": round((time.time() - start_time) * 1000, 2)
            }
        except Exception as e:
            logger.error(f"Arrow query execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": round((time.time() - start_time) * 1000, 2)
            }

    async def _execute_show_tables_legacy(self, sql: str) -> Dict[str, Any]:
        """Legacy handler for SHOW TABLES using old connection"""
        start_time = time.time()
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._handle_show_tables_sync, sql)
            execution_time = time.time() - start_time
            result["execution_time_ms"] = round(execution_time * 1000, 2)
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"SHOW TABLES failed in {execution_time:.3f}s: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0,
                "execution_time_ms": round(execution_time * 1000, 2)
            }

    async def _execute_show_databases_legacy(self, sql: str) -> Dict[str, Any]:
        """Legacy handler for SHOW DATABASES using old connection"""
        start_time = time.time()
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._handle_show_databases_sync)
            execution_time = time.time() - start_time
            result["execution_time_ms"] = round(execution_time * 1000, 2)
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"SHOW DATABASES failed in {execution_time:.3f}s: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0,
                "execution_time_ms": round(execution_time * 1000, 2)
            }

    async def _execute_describe_legacy(self, sql: str) -> Dict[str, Any]:
        """Legacy handler for DESCRIBE command - optimized for speed"""
        start_time = time.time()
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._handle_describe_sync, sql)
            execution_time = time.time() - start_time
            result["execution_time_ms"] = round(execution_time * 1000, 2)
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"DESCRIBE failed in {execution_time:.3f}s: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0,
                "execution_time_ms": round(execution_time * 1000, 2)
            }

    async def _execute_describe_arrow(self, sql: str) -> Dict[str, Any]:
        """Arrow handler for DESCRIBE command - optimized for speed"""
        start_time = time.time()
        try:
            import pyarrow as pa
            import io

            # Get schema from Parquet file (fast - no data read)
            loop = asyncio.get_event_loop()
            schema_info = await loop.run_in_executor(None, self._get_table_schema, sql)

            if not schema_info:
                raise Exception("Table not found or no data available")

            # Create Arrow table with schema information
            column_names = pa.array([col["name"] for col in schema_info], type=pa.string())
            column_types = pa.array([col["type"] for col in schema_info], type=pa.string())
            nullables = pa.array([col["nullable"] for col in schema_info], type=pa.string())

            arrow_table = pa.Table.from_arrays(
                [column_names, column_types, nullables],
                names=["column_name", "column_type", "null"]
            )

            # Serialize to Arrow IPC format
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)

            arrow_bytes = sink.getvalue()
            execution_time = time.time() - start_time

            logger.info(f"DESCRIBE (Arrow) completed in {execution_time:.3f}s: {len(schema_info)} columns")

            return {
                "success": True,
                "arrow_table": arrow_bytes,
                "row_count": len(schema_info),
                "execution_time_ms": round(execution_time * 1000, 2),
                "wait_time_ms": 0.0
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"DESCRIBE (Arrow) failed in {execution_time:.3f}s: {e}")
            return {
                "success": False,
                "error": str(e),
                "arrow_table": b"",
                "row_count": 0,
                "execution_time_ms": round(execution_time * 1000, 2),
                "wait_time_ms": 0.0
            }

    # Iceberg support removed; this stub remains to avoid API import breakages if referenced accidentally
    async def query_iceberg_table(self, *args, **kwargs) -> Dict[str, Any]:
        return {
            "success": False,
            "error": "Iceberg support is not available in this build",
            "data": [],
            "columns": [],
            "row_count": 0,
        }
    def _execute_sync(self, sql: str) -> Dict[str, Any]:
        """Synchronous query execution"""
        try:
            # Check for SHOW TABLES command
            if self._is_show_tables_query(sql):
                return self._handle_show_tables_sync(sql)

            # Check for SHOW DATABASES command
            if self._is_show_databases_query(sql):
                return self._handle_show_databases_sync()

            # Convert database.table syntax to S3 paths
            converted_sql = self._convert_sql_to_s3_paths(sql)
            logger.info(f"Executing SQL: {converted_sql}")
            
            # Check if query lacks LIMIT clause and warn about potential large results
            has_limit = 'LIMIT' in converted_sql.upper()
            if not has_limit:
                try:
                    # Quick count check for education purposes
                    count_sql = f"SELECT COUNT(*) FROM ({converted_sql}) AS t"
                    count_result = self.conn.execute(count_sql).fetchone()
                    estimated_rows = count_result[0] if count_result else 0
                    
                    if estimated_rows > 100000:
                        logger.warning(f"Query will return {estimated_rows:,} rows without LIMIT clause. Consider adding 'LIMIT {min(10000, estimated_rows)}' for better performance.")
                    elif estimated_rows > 10000:
                        logger.info(f"Query will return {estimated_rows:,} rows. Consider adding 'LIMIT {min(1000, estimated_rows)}' if you don't need all rows.")
                except Exception as e:
                    logger.debug(f"Could not estimate row count: {e}")
            
            # Execute the actual query
            result = self.conn.execute(converted_sql).fetchall()
            columns = [desc[0] for desc in self.conn.description]

            # OPTIMIZATION: Convert data types for JSON serialization
            # Pre-detect timestamp columns (2-5x faster than hasattr() on every cell)
            from datetime import datetime as dt_type
            timestamp_cols = set()
            if result:
                first_row = result[0]
                for i, value in enumerate(first_row):
                    if isinstance(value, dt_type):
                        timestamp_cols.add(i)

            serialized_data = []
            if timestamp_cols:
                # Fast path: convert only known timestamp columns
                for row in result:
                    serialized_row = list(row)
                    for i in timestamp_cols:
                        val = serialized_row[i]
                        if val is not None:  # Skip isinstance check - we already know it's a timestamp column
                            serialized_row[i] = val.isoformat()
                    serialized_data.append(serialized_row)
            else:
                # No timestamps - just convert tuples to lists
                serialized_data = [list(row) for row in result]
            
            return {
                "success": True,
                "data": serialized_data,
                "columns": columns,
                "row_count": len(result)
            }
            
        except Exception as e:
            raise Exception(f"DuckDB execution failed: {e}")
    
    def _convert_sql_to_s3_paths(self, sql: str) -> str:
        """Convert database.table references to S3 paths

        Storage path structure: {bucket}/{database}/{measurement}/{partitions}/file.parquet
        - Simple reference (cpu) -> current database
        - Qualified reference (production.cpu) -> specified database
        """
        import re

        # Store original SQL for partition pruning
        self.current_sql = sql

        # Extract CTE names to avoid rewriting them
        # Match: WITH cte_name AS (...), other_cte AS (...)
        cte_pattern = r'\bWITH\s+(?:RECURSIVE\s+)?(\w+)\s+AS\s*\('
        cte_names = set(re.findall(cte_pattern, sql, re.IGNORECASE))

        # Also match additional CTEs after commas: ...), cte_name AS (...)
        additional_cte_pattern = r',\s*(\w+)\s+AS\s*\('
        cte_names.update(re.findall(additional_cte_pattern, sql, re.IGNORECASE))

        if cte_names:
            logger.debug(f"Detected CTEs in query, will not rewrite: {cte_names}")

        # First, handle database.table references
        # Allow database/table names that start with digits (e.g., 3am.logs)
        # Note: \w+ matches [a-zA-Z0-9_] but standard regex \w cannot START with digit
        # So we explicitly use [a-zA-Z0-9_]+ which allows digit at start
        pattern_db_table = r'FROM\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'

        def replace_db_table(match):
            database = match.group(1)
            table = match.group(2)

            # Find storage connection that matches the database
            storage_conn = None
            if self.connection_manager:
                for conn in self.connection_manager.get_storage_connections():
                    if conn.get('database', 'default') == database:
                        storage_conn = conn
                        break

            # Use backend (local, minio, s3, etc.)
            backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if not backend:
                logger.error(f"No storage backend available for database: {database}")
                return match.group(0)

            # Determine bucket/base_path and backend type
            if storage_conn:
                bucket = storage_conn.get('bucket') or storage_conn.get('base_path')
                backend_type = storage_conn.get('backend')
            else:
                # No specific connection for this database - use default backend settings
                bucket = getattr(backend, 'bucket', None) or getattr(backend, 'base_path', 'arc')
                backend_type = 'local' if self.local_backend else ('gcs' if self.gcs_backend else 's3')

            if backend_type == 'local' or self.storage_backend == 'local':
                # Use direct filesystem path for local backend
                base_path = self.local_backend.base_path if self.local_backend else bucket
                local_path = f"{base_path}/{database}/{table}/**/*.parquet"

                # Apply partition pruning if enabled
                optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                    local_path, self.current_sql or ''
                )

                if was_optimized:
                    if isinstance(optimized_path, list):
                        # Multiple paths - use DuckDB array syntax
                        paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                        logger.info(f"✨ Partition pruning: Using {len(optimized_path)} targeted paths")
                        return f"FROM read_parquet({paths_str}, union_by_name=true)"
                    else:
                        logger.info(f"✨ Partition pruning: Using optimized path: {optimized_path}")
                        return f"FROM read_parquet('{optimized_path}', union_by_name=true)"
                else:
                    logger.info(f"Using local filesystem path for DuckDB: {local_path}")
                    return f"FROM read_parquet('{local_path}', union_by_name=true)"
            elif backend_type == 'gcs':
                # Use native gs:// URLs with DuckDB GCS support
                gs_path = f"gs://{bucket}/{database}/{table}/**/*.parquet"

                # Apply partition pruning if enabled
                optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                    gs_path, self.current_sql or ''
                )

                if was_optimized:
                    if isinstance(optimized_path, list):
                        paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                        logger.info(f"✨ Partition pruning: Using {len(optimized_path)} targeted GCS paths")
                        return f"FROM read_parquet({paths_str}, union_by_name=true)"
                    else:
                        logger.info(f"✨ Partition pruning: Using optimized GCS path: {optimized_path}")
                        return f"FROM read_parquet('{optimized_path}', union_by_name=true)"
                else:
                    logger.info(f"Using native GCS path for DuckDB: {gs_path}")
                    return f"FROM read_parquet('{gs_path}', union_by_name=true)"
            else:
                # Use S3-compatible path for MinIO/S3/Ceph
                s3_path = f"s3://{bucket}/{database}/{table}/**/*.parquet"

                # Apply partition pruning if enabled
                optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                    s3_path, self.current_sql or ''
                )

                if was_optimized:
                    if isinstance(optimized_path, list):
                        paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                        logger.info(f"✨ Partition pruning: Using {len(optimized_path)} targeted S3 paths")
                        return f"FROM read_parquet({paths_str}, union_by_name=true)"
                    else:
                        logger.info(f"✨ Partition pruning: Using optimized S3 path: {optimized_path}")
                        return f"FROM read_parquet('{optimized_path}', union_by_name=true)"
                else:
                    logger.info(f"Converting {database}.{table} to {s3_path}")
                    return f"FROM read_parquet('{s3_path}', union_by_name=true)"

        converted = re.sub(pattern_db_table, replace_db_table, sql, flags=re.IGNORECASE)

        # Also handle JOIN database.table references
        # Same pattern as FROM but with JOIN keyword
        pattern_join_db_table = r'((?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'

        def replace_join_db_table(match):
            join_type = match.group(1)
            database = match.group(2)
            table = match.group(3)

            # Call the same replace_db_table function but create a fake match object
            fake_match = type('Match', (), {
                'group': lambda _, n: database if n == 1 else (table if n == 2 else f"{database}.{table}")
            })()

            result = replace_db_table(fake_match)
            # Replace "FROM" with the JOIN type
            return result.replace('FROM', join_type, 1)

        converted = re.sub(pattern_join_db_table, replace_join_db_table, converted, flags=re.IGNORECASE)

        # Second, handle simple table names (FROM table_name) using current database
        # Only match table names that are NOT part of database.table (which was already handled)
        # Match: FROM table_name WHERE ...
        # Don't match: FROM database.table or FROM read_parquet(...)
        # Allow table names starting with digits (e.g., 3am)
        pattern_simple = r'\bFROM\s+(?!read_parquet|information_schema|pg_)([a-zA-Z0-9_]+)\b(?!\s*\.|\s*\()'

        def replace_simple_table(match):
            table = match.group(1)

            # Skip CTE names - they are not physical tables
            if table.lower() in {name.lower() for name in cte_names}:
                logger.debug(f"Skipping CTE '{table}' from table rewriting")
                return match.group(0)  # Return original "FROM table"

            # Use actual backend and its database
            backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if backend:
                bucket = getattr(backend, 'bucket', None) or getattr(backend, 'base_path', 'arc')
                database = backend.database
            else:
                bucket = "arc"
                database = "default"

            # Build path with database namespace
            if self.local_backend or self.storage_backend == 'local':
                base_path = self.local_backend.base_path if self.local_backend else bucket
                local_path = f"{base_path}/{database}/{table}/**/*.parquet"

                # Apply partition pruning if enabled
                optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                    local_path, self.current_sql or ''
                )

                if was_optimized:
                    if isinstance(optimized_path, list):
                        paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                        logger.info(f"✨ Partition pruning: Using {len(optimized_path)} targeted paths for '{table}'")
                        return f"FROM read_parquet({paths_str}, union_by_name=true)"
                    else:
                        logger.info(f"✨ Partition pruning: Using optimized path for '{table}': {optimized_path}")
                        return f"FROM read_parquet('{optimized_path}', union_by_name=true)"
                else:
                    logger.info(f"Converting simple table '{table}' to {local_path}")
                    return f"FROM read_parquet('{local_path}', union_by_name=true)"

            elif self.gcs_backend:
                gs_path = f"gs://{bucket}/{database}/{table}/**/*.parquet"

                # Apply partition pruning if enabled
                optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                    gs_path, self.current_sql or ''
                )

                if was_optimized:
                    if isinstance(optimized_path, list):
                        paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                        logger.info(f"✨ Partition pruning: Using {len(optimized_path)} targeted GCS paths for '{table}'")
                        return f"FROM read_parquet({paths_str}, union_by_name=true)"
                    else:
                        logger.info(f"✨ Partition pruning: Using optimized GCS path for '{table}': {optimized_path}")
                        return f"FROM read_parquet('{optimized_path}', union_by_name=true)"
                else:
                    logger.info(f"Converting simple table '{table}' to {gs_path}")
                    return f"FROM read_parquet('{gs_path}', union_by_name=true)"

            else:
                s3_path = f"s3://{bucket}/{database}/{table}/**/*.parquet"

                # Apply partition pruning if enabled
                optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                    s3_path, self.current_sql or ''
                )

                if was_optimized:
                    if isinstance(optimized_path, list):
                        paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                        logger.info(f"✨ Partition pruning: Using {len(optimized_path)} targeted S3 paths for '{table}'")
                        return f"FROM read_parquet({paths_str}, union_by_name=true)"
                    else:
                        logger.info(f"✨ Partition pruning: Using optimized S3 path for '{table}': {optimized_path}")
                        return f"FROM read_parquet('{optimized_path}', union_by_name=true)"
                else:
                    logger.info(f"Converting simple table '{table}' to {s3_path}")
                    return f"FROM read_parquet('{s3_path}', union_by_name=true)"

        converted = re.sub(pattern_simple, replace_simple_table, converted, flags=re.IGNORECASE)

        # Also handle JOIN clauses (LEFT JOIN, INNER JOIN, etc.)
        # Don't match if table name is part of database.table or followed by opening paren
        # Allow table names starting with digits
        pattern_join = r'((?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN)\s+(?!read_parquet|information_schema|pg_)([a-zA-Z0-9_]+)\b(?!\s*\.|\s*\()'

        def replace_join_table(match):
            join_type = match.group(1)  # e.g., "LEFT JOIN"
            table = match.group(2)

            # Skip CTE names
            if table.lower() in {name.lower() for name in cte_names}:
                logger.debug(f"Skipping CTE '{table}' in JOIN from table rewriting")
                return match.group(0)  # Return original "JOIN table"

            # Convert to read_parquet (same logic as FROM)
            backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if backend:
                database = backend.database
                if self.local_backend or self.storage_backend == 'local':
                    base_path = self.local_backend.base_path if self.local_backend else 'arc'
                    local_path = f"{base_path}/{database}/{table}/**/*.parquet"
                    optimized_path, was_optimized = self.partition_pruner.optimize_table_path(
                        local_path, self.current_sql or ''
                    )
                    if was_optimized:
                        if isinstance(optimized_path, list):
                            paths_str = "[" + ", ".join(f"'{p}'" for p in optimized_path) + "]"
                            return f"{join_type} read_parquet({paths_str}, union_by_name=true)"
                        else:
                            return f"{join_type} read_parquet('{optimized_path}', union_by_name=true)"
                    else:
                        return f"{join_type} read_parquet('{local_path}', union_by_name=true)"

            return match.group(0)

        converted = re.sub(pattern_join, replace_join_table, converted, flags=re.IGNORECASE)
        return converted
    
    def _is_show_tables_query(self, sql: str) -> bool:
        """Check if query is a SHOW TABLES command"""
        import re
        # Match SHOW TABLES or SHOW TABLES FROM database (allow hyphens in database names)
        pattern = r'^\s*SHOW\s+TABLES(?:\s+FROM\s+([\w-]+))?\s*;?\s*$'
        return bool(re.match(pattern, sql.strip(), re.IGNORECASE))

    def _is_show_databases_query(self, sql: str) -> bool:
        """Check if query is a SHOW DATABASES command"""
        import re
        pattern = r'^\s*SHOW\s+DATABASES\s*;?\s*$'
        return bool(re.match(pattern, sql.strip(), re.IGNORECASE))

    def _is_describe_query(self, sql: str) -> bool:
        """Check if query is a DESCRIBE command"""
        import re
        # Remove SQL comments (VSCode extension adds "-- SQL\n")
        cleaned_sql = re.sub(r'--.*?(\n|$)', '', sql, flags=re.MULTILINE).strip()
        # Match: DESCRIBE table or DESCRIBE database.table or DESC table
        pattern = r'^\s*(?:DESCRIBE|DESC)\s+([\w_-]+(?:\.[\w_-]+)?)\s*;?\s*$'
        return bool(re.match(pattern, cleaned_sql, re.IGNORECASE))

    def _handle_show_tables_sync(self, sql: str) -> Dict[str, Any]:
        """Handle SHOW TABLES command

        Storage path structure: {bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
        - Database is the namespace (default, production, staging, etc.)
        - Measurement is the table name (cpu, mem, disk, etc.)
        """
        import re

        # Extract database name if specified (allow hyphens in database names)
        pattern = r'^\s*SHOW\s+TABLES(?:\s+FROM\s+([\w-]+))?\s*;?\s*$'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)

        if not match:
            return {
                "success": False,
                "error": "Invalid SHOW TABLES syntax",
                "data": [],
                "columns": [],
                "row_count": 0
            }

        database_filter = match.group(1)
        logger.info(f"Showing tables for database: {database_filter if database_filter else 'current database'}")

        try:
            # Find storage backend
            backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if not backend:
                return {
                    "success": True,
                    "data": [],
                    "columns": ["database", "table_name", "storage_path", "file_count", "total_size_mb"],
                    "row_count": 0
                }

            # Determine which database to query
            target_database = database_filter if database_filter else backend.database

            # Handle local filesystem backend differently
            if self.local_backend:
                from pathlib import Path
                base_path = Path(self.local_backend.base_path)
                database_path = base_path / target_database

                objects = []
                if database_path.exists():
                    # Walk the database directory to find measurement directories
                    for measurement_dir in database_path.iterdir():
                        if measurement_dir.is_dir() and not measurement_dir.name.startswith('.'):
                            # This is a measurement directory, find all parquet files
                            for file_path in measurement_dir.rglob('*.parquet'):
                                # Make path relative to database dir
                                relative_path = file_path.relative_to(database_path)
                                objects.append(str(relative_path))

                logger.info(f"Local filesystem found {len(objects)} files for database {target_database}")

            # If querying a different database than backend's default, use S3 directly
            elif database_filter and database_filter != backend.database:
                # Scan specific database using S3 client directly
                objects = []
                try:
                    paginator = backend.s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(
                        Bucket=backend.bucket,
                        Prefix=f"{target_database}/",  # Specific database prefix
                        MaxKeys=10000
                    ):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                key = obj['Key']
                                # Remove database prefix for consistent parsing
                                if key.startswith(f"{target_database}/"):
                                    relative_key = key[len(f"{target_database}/"):]
                                    objects.append(relative_key)
                except Exception as e:
                    logger.error(f"Failed to list objects for database {target_database}: {e}")
                    return {
                        "success": False,
                        "error": f"Failed to access database {target_database}: {str(e)}",
                        "data": [],
                        "columns": ["database", "table_name", "storage_path", "file_count", "total_size_mb"],
                        "row_count": 0
                    }
            else:
                # Use backend's list_objects for current database
                objects = backend.list_objects(prefix="", max_keys=10000)
                target_database = backend.database

            # Parse objects to find measurements (tables)
            # Path structure: measurement/year/month/day/hour/file.parquet
            tables = {}
            for obj_key in objects:
                parts = obj_key.split('/')

                # Expected format: measurement/year/month/... (database is already filtered)
                if len(parts) >= 1:
                    measurement = parts[0]  # First part is measurement name

                    # Skip year/partition directories (numeric names)
                    if measurement.isdigit():
                        continue

                    # Track table info
                    table_key = f"{target_database}.{measurement}"
                    if table_key not in tables:
                        # Determine storage path based on backend type
                        if self.local_backend:
                            storage_path = f"{self.local_backend.base_path}/{target_database}/{measurement}/"
                        else:
                            storage_path = f"s3://{backend.bucket}/{target_database}/{measurement}/"

                        tables[table_key] = {
                            "database": target_database,
                            "table_name": measurement,
                            "file_count": 0,
                            "total_size": 0,
                            "storage_path": storage_path
                        }

                    # Count files and size if it's a parquet file
                    if obj_key.endswith('.parquet'):
                        tables[table_key]["file_count"] += 1
                        # Note: Size calculation would require additional backend call

            # Format results
            data = []
            for table_info in sorted(tables.values(), key=lambda x: (x["database"], x["table_name"])):
                data.append([
                    table_info["database"],
                    table_info["table_name"],
                    table_info["storage_path"],
                    table_info["file_count"],
                    round(table_info["total_size"] / (1024 * 1024), 2)  # Convert to MB
                ])

            return {
                "success": True,
                "data": data,
                "columns": ["database", "table_name", "storage_path", "file_count", "total_size_mb"],
                "row_count": len(data)
            }

        except Exception as e:
            logger.error(f"Failed to show tables: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0
            }

    def _handle_show_databases_sync(self) -> Dict[str, Any]:
        """Handle SHOW DATABASES command

        Returns all unique database namespaces found in storage by scanning the bucket/filesystem
        """
        try:
            databases = set()

            # Get the backend (regardless of connection manager)
            backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend

            if self.local_backend:
                # Local filesystem: scan base_path for database directories
                from pathlib import Path
                base_path = Path(self.local_backend.base_path)

                if base_path.exists():
                    for item in base_path.iterdir():
                        if item.is_dir() and not item.name.startswith('.'):
                            databases.add(item.name)

                logger.info(f"Local filesystem databases: {sorted(databases)}")

            elif backend:
                # Scan the bucket root to find all database directories
                # Storage structure: {bucket}/{database}/{measurement}/{partitions}
                # We need to list all top-level prefixes in the bucket

                # Use S3 client directly to scan bucket root (bypass database prefix logic)
                try:
                    paginator = backend.s3_client.get_paginator('list_objects_v2')

                    # List objects in bucket root (no prefix)
                    for page in paginator.paginate(
                        Bucket=backend.bucket,
                        Prefix="",  # No prefix - scan entire bucket
                        Delimiter="/",  # Get top-level "directories" only
                        MaxKeys=1000
                    ):
                        # CommonPrefixes contains the top-level directories (databases)
                        if 'CommonPrefixes' in page:
                            for prefix_info in page['CommonPrefixes']:
                                prefix = prefix_info['Prefix']
                                # Remove trailing slash
                                database = prefix.rstrip('/')
                                if database and not database.isdigit():
                                    databases.add(database)

                except Exception as e:
                    logger.error(f"Failed to scan bucket for databases: {e}")
                    # Fallback: return current backend's database
                    databases.add(backend.database)

                # If no databases found, return at least "default"
                if not databases:
                    databases.add("default")
            else:
                # No backend available, return default
                databases.add("default")

            # Format results
            data = [[db] for db in sorted(databases)]

            logger.info(f"SHOW DATABASES found: {sorted(databases)}")

            return {
                "success": True,
                "data": data,
                "columns": ["database"],
                "row_count": len(data)
            }

        except Exception as e:
            logger.error(f"Failed to show databases: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0
            }

    async def _execute_show_databases_arrow(self, sql: str) -> Dict[str, Any]:
        """Native Arrow handler for SHOW DATABASES command"""
        start_time = time.time()
        try:
            import pyarrow as pa
            import io

            # Get databases list (reuse sync logic)
            loop = asyncio.get_event_loop()
            databases = await loop.run_in_executor(None, self._get_databases_list)

            # Create Arrow table from databases list
            # Schema: [database: string]
            database_array = pa.array(sorted(databases), type=pa.string())
            arrow_table = pa.Table.from_arrays([database_array], names=["database"])

            # Serialize to Arrow IPC format (stream)
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)

            arrow_bytes = sink.getvalue()
            execution_time = time.time() - start_time

            logger.info(f"SHOW DATABASES (Arrow) completed in {execution_time:.3f}s: {len(databases)} databases")

            return {
                "success": True,
                "arrow_table": arrow_bytes,
                "row_count": len(databases),
                "execution_time_ms": round(execution_time * 1000, 2),
                "wait_time_ms": 0.0
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"SHOW DATABASES (Arrow) failed in {execution_time:.3f}s: {e}")
            return {
                "success": False,
                "error": str(e),
                "arrow_table": b"",
                "row_count": 0,
                "execution_time_ms": round(execution_time * 1000, 2),
                "wait_time_ms": 0.0
            }

    def _handle_describe_sync(self, sql: str) -> Dict[str, Any]:
        """Handle DESCRIBE command synchronously - optimized for speed"""
        try:
            schema_info = self._get_table_schema(sql)

            if not schema_info:
                return {
                    "success": False,
                    "error": "Table not found or no data available",
                    "data": [],
                    "columns": [],
                    "row_count": 0
                }

            # Format as rows for JSON response
            data = [[col["name"], col["type"], col["nullable"]] for col in schema_info]

            return {
                "success": True,
                "data": data,
                "columns": ["column_name", "column_type", "null"],
                "row_count": len(schema_info)
            }

        except Exception as e:
            logger.error(f"DESCRIBE handler error: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0
            }

    def _get_table_schema(self, sql: str) -> List[Dict[str, Any]]:
        """Get schema from Parquet file - OPTIMIZED for speed (metadata only, no data read)"""
        import re

        # Remove SQL comments (VSCode extension adds "-- SQL\n")
        cleaned_sql = re.sub(r'--.*?(\n|$)', '', sql, flags=re.MULTILINE).strip()

        # Parse table name from DESCRIBE statement
        pattern = r'^\s*(?:DESCRIBE|DESC)\s+([\w_-]+)(?:\.([\w_-]+))?\s*;?\s*$'
        match = re.match(pattern, cleaned_sql, re.IGNORECASE)

        if not match:
            raise Exception("Invalid DESCRIBE syntax")

        # Extract database and measurement
        if match.group(2):
            database = match.group(1)
            measurement = match.group(2)
        else:
            database = 'default'
            measurement = match.group(1)

        # Find a Parquet file
        parquet_file = self._find_parquet_file(database, measurement)

        if not parquet_file:
            raise Exception(f"No data found for table '{measurement}' in database '{database}'")

        # Read ONLY schema from Parquet (no data read - very fast!)
        try:
            import pyarrow.parquet as pq
            # read_schema() is much faster than read_table() - only reads metadata
            schema = pq.read_schema(parquet_file)

            # Convert schema to list of dicts
            schema_info = []
            for field in schema:
                schema_info.append({
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": "YES" if field.nullable else "NO"
                })

            return schema_info

        except Exception as e:
            logger.error(f"Failed to read Parquet schema from {parquet_file}: {e}")
            raise Exception(f"Failed to read table schema: {e}")

    def _find_parquet_file(self, database: str, measurement: str) -> Optional[str]:
        """Find first Parquet file for table - OPTIMIZED to return quickly"""
        try:
            if self.storage_backend == "local":
                import os
                base_path = self.local_backend.base_path if self.local_backend else "./data"
                measurement_path = os.path.join(base_path, database, measurement)

                if not os.path.exists(measurement_path):
                    return None

                # Find first .parquet file (stop at first match for speed)
                for root, dirs, files in os.walk(measurement_path):
                    for file in files:
                        if file.endswith('.parquet'):
                            return os.path.join(root, file)

            elif self.storage_backend in ["s3", "minio", "ceph"]:
                backend = self.s3_backend or self.minio_backend or self.ceph_backend
                if backend:
                    bucket = backend.bucket
                    prefix = f"{database}/{measurement}/"

                    import boto3
                    s3_client = boto3.client('s3',
                        endpoint_url=getattr(backend, 'endpoint_url', None),
                        aws_access_key_id=backend._access_key,
                        aws_secret_access_key=backend._secret_key
                    )

                    # MaxKeys=1 for speed - we only need one file
                    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)

                    if 'Contents' in response:
                        for obj in response['Contents']:
                            if obj['Key'].endswith('.parquet'):
                                return f"s3://{bucket}/{obj['Key']}"

            elif self.storage_backend == "gcs":
                if self.gcs_backend:
                    bucket = self.gcs_backend.bucket
                    prefix = f"{database}/{measurement}/"

                    from google.cloud import storage
                    client = storage.Client()
                    bucket_obj = client.bucket(bucket)
                    # max_results=1 for speed
                    blobs = bucket_obj.list_blobs(prefix=prefix, max_results=1)

                    for blob in blobs:
                        if blob.name.endswith('.parquet'):
                            return f"gs://{bucket}/{blob.name}"

            return None

        except Exception as e:
            logger.error(f"Error finding Parquet file for {database}.{measurement}: {e}")
            return None

    async def _execute_show_tables_arrow(self, sql: str) -> Dict[str, Any]:
        """Native Arrow handler for SHOW TABLES command"""
        start_time = time.time()
        try:
            import pyarrow as pa
            import io
            import re

            # Extract database name if specified
            pattern = r'^\s*SHOW\s+TABLES(?:\s+FROM\s+([\w-]+))?\s*;?\s*$'
            match = re.match(pattern, sql.strip(), re.IGNORECASE)

            if not match:
                return {
                    "success": False,
                    "error": "Invalid SHOW TABLES syntax",
                    "arrow_table": b"",
                    "row_count": 0,
                    "execution_time_ms": 0.0,
                    "wait_time_ms": 0.0
                }

            database_filter = match.group(1)

            # Get tables list (reuse sync logic)
            loop = asyncio.get_event_loop()
            tables_data = await loop.run_in_executor(None, self._get_tables_list, database_filter)

            # Create Arrow table from tables data
            # Schema: [database: string, table_name: string, storage_path: string, file_count: int64, total_size_mb: float64]
            if not tables_data:
                # Empty result - need empty arrays for each field
                arrow_table = pa.Table.from_arrays(
                    [
                        pa.array([], type=pa.string()),
                        pa.array([], type=pa.string()),
                        pa.array([], type=pa.string()),
                        pa.array([], type=pa.int64()),
                        pa.array([], type=pa.float64())
                    ],
                    names=["database", "table_name", "storage_path", "file_count", "total_size_mb"]
                )
            else:
                # Extract columns
                databases = [t["database"] for t in tables_data]
                table_names = [t["table_name"] for t in tables_data]
                storage_paths = [t["storage_path"] for t in tables_data]
                file_counts = [t["file_count"] for t in tables_data]
                total_sizes = [round(t["total_size"] / (1024 * 1024), 2) for t in tables_data]

                arrow_table = pa.Table.from_arrays(
                    [
                        pa.array(databases, type=pa.string()),
                        pa.array(table_names, type=pa.string()),
                        pa.array(storage_paths, type=pa.string()),
                        pa.array(file_counts, type=pa.int64()),
                        pa.array(total_sizes, type=pa.float64())
                    ],
                    names=["database", "table_name", "storage_path", "file_count", "total_size_mb"]
                )

            # Serialize to Arrow IPC format (stream)
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)

            arrow_bytes = sink.getvalue()
            execution_time = time.time() - start_time

            logger.info(f"SHOW TABLES (Arrow) completed in {execution_time:.3f}s: {len(tables_data)} tables")

            return {
                "success": True,
                "arrow_table": arrow_bytes,
                "row_count": len(tables_data),
                "execution_time_ms": round(execution_time * 1000, 2),
                "wait_time_ms": 0.0
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"SHOW TABLES (Arrow) failed in {execution_time:.3f}s: {e}")
            return {
                "success": False,
                "error": str(e),
                "arrow_table": b"",
                "row_count": 0,
                "execution_time_ms": round(execution_time * 1000, 2),
                "wait_time_ms": 0.0
            }

    def _get_databases_list(self) -> set:
        """Get list of databases from storage (synchronous helper)"""
        databases = set()

        # Get the backend
        backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend

        if self.local_backend:
            # Local filesystem: scan base_path for database directories
            from pathlib import Path
            base_path = Path(self.local_backend.base_path)

            if base_path.exists():
                for item in base_path.iterdir():
                    if item.is_dir() and not item.name.startswith('.'):
                        databases.add(item.name)

            logger.info(f"Local filesystem databases: {sorted(databases)}")

        elif backend:
            # Scan the bucket root to find all database directories
            try:
                paginator = backend.s3_client.get_paginator('list_objects_v2')

                for page in paginator.paginate(
                    Bucket=backend.bucket,
                    Prefix="",
                    Delimiter="/",
                    MaxKeys=1000
                ):
                    if 'CommonPrefixes' in page:
                        for prefix_info in page['CommonPrefixes']:
                            prefix = prefix_info['Prefix']
                            database = prefix.rstrip('/')
                            if database and not database.isdigit():
                                databases.add(database)

            except Exception as e:
                logger.error(f"Failed to scan bucket for databases: {e}")
                databases.add(backend.database)

            if not databases:
                databases.add("default")
        else:
            databases.add("default")

        return databases

    def _get_tables_list(self, database_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables from storage (synchronous helper)

        Args:
            database_filter: If specified, only return tables from this database.
                           If None, return tables from the backend's current database.
        """
        try:
            backend = self.local_backend or self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if not backend:
                return []

            # Determine which database to query
            target_database = database_filter if database_filter else backend.database
            logger.info(f"Getting tables list for database: {target_database}")

            # Handle local filesystem backend
            if self.local_backend:
                from pathlib import Path
                base_path = Path(self.local_backend.base_path)
                database_path = base_path / target_database

                objects = []
                if database_path.exists():
                    for measurement_dir in database_path.iterdir():
                        if measurement_dir.is_dir() and not measurement_dir.name.startswith('.'):
                            for file_path in measurement_dir.rglob('*.parquet'):
                                relative_path = file_path.relative_to(database_path)
                                objects.append(str(relative_path))

                logger.info(f"Local filesystem found {len(objects)} files for database {target_database}")

            # S3-compatible backends (MinIO, S3, Ceph, GCS)
            elif database_filter and database_filter != backend.database:
                # Specific database requested (different from backend's default)
                objects = []
                try:
                    paginator = backend.s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(
                        Bucket=backend.bucket,
                        Prefix=f"{target_database}/",
                        MaxKeys=10000
                    ):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                key = obj['Key']
                                if key.startswith(f"{target_database}/"):
                                    relative_key = key[len(f"{target_database}/"):]
                                    objects.append(relative_key)
                    logger.info(f"S3 backend found {len(objects)} files for database {target_database}")
                except Exception as e:
                    logger.error(f"Failed to list objects for database {target_database}: {e}")
                    return []
            else:
                # Use backend's list_objects for current database
                objects = backend.list_objects(prefix="", max_keys=10000)

            # Parse objects to find measurements (tables)
            tables_dict = {}
            for obj_key in objects:
                parts = obj_key.split('/')

                if len(parts) >= 1:
                    measurement = parts[0]

                    # Skip year/partition directories
                    if measurement.isdigit():
                        continue

                    # Track unique measurements
                    if measurement not in tables_dict:
                        # Create table entry
                        if self.local_backend:
                            storage_path = f"{self.local_backend.base_path}/{target_database}/{measurement}/"
                        else:
                            storage_path = f"s3://{backend.bucket}/{target_database}/{measurement}/"

                        tables_dict[measurement] = {
                            "database": target_database,
                            "table_name": measurement,
                            "file_count": 0,
                            "total_size": 0,
                            "storage_path": storage_path
                        }

                    # Count parquet files
                    if obj_key.endswith('.parquet'):
                        tables_dict[measurement]["file_count"] += 1

            tables = list(tables_dict.values())
            logger.info(f"Found {len(tables)} tables in database {target_database}")
            return sorted(tables, key=lambda x: x["table_name"])

        except Exception as e:
            logger.error(f"Failed to get tables list: {e}")
            return []

    async def get_measurements(self) -> List[str]:
        """Get list of available measurements (tables) in current database

        Storage path structure: {bucket}/{database}/{measurement}/{partitions}/file.parquet
        Backend's list_objects() returns paths relative to database (already scoped)
        """
        try:
            backend = self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if not backend:
                return []

            # Backend already scopes to its database
            objects = backend.list_objects(prefix="", max_keys=10000)
            measurements = set()

            for obj_key in objects:
                parts = obj_key.split('/')
                # Path format: measurement/year/month/... (database is already filtered)
                if len(parts) >= 1:
                    measurement = parts[0]  # First part is measurement name
                    # Skip numeric directories (year partitions)
                    if not measurement.isdigit():
                        measurements.add(measurement)

            return sorted(list(measurements))

        except Exception as e:
            logger.error(f"Failed to get measurements: {e}")
            return []
    
    async def query_measurement(
        self,
        measurement: str,
        start_time: str = None,
        end_time: str = None,
        columns: List[str] = None,
        where_clause: str = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Query specific measurement with filters"""
        try:
            # Build SQL query
            select_cols = ", ".join(columns) if columns else "*"
            sql = f"SELECT {select_cols} FROM {measurement}"
            
            conditions = []
            if start_time:
                conditions.append(f"timestamp >= '{start_time}'")
            if end_time:
                conditions.append(f"timestamp < '{end_time}'")
            if where_clause:
                conditions.append(where_clause)
            
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            
            sql += f" LIMIT {limit}"
            
            return await self.execute_query(sql)
            
        except Exception as e:
            logger.error(f"Failed to query measurement {measurement}: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "row_count": 0
            }
    
    def get_pool_metrics(self) -> Dict[str, Any]:
        """Get connection pool metrics"""
        if hasattr(self, 'connection_pool'):
            metrics = self.connection_pool.get_metrics()
            return {
                "pool_size": metrics.pool_size,
                "active_connections": metrics.active_connections,
                "idle_connections": metrics.idle_connections,
                "queue_depth": metrics.queue_depth,
                "total_queries_executed": metrics.total_queries_executed,
                "total_queries_queued": metrics.total_queries_queued,
                "total_queries_failed": metrics.total_queries_failed,
                "total_queries_timeout": metrics.total_queries_timeout,
                "avg_wait_time_ms": metrics.avg_wait_time_ms,
                "avg_execution_time_ms": metrics.avg_execution_time_ms,
                "timestamp": metrics.timestamp
            }
        return {}

    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection pool metrics"""
        if hasattr(self, 'connection_pool'):
            return self.connection_pool.get_metrics()
        return {}

    def close(self):
        """Cleanup"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.close_all()
        if hasattr(self, 'conn'):
            self.conn.close()
