import asyncio
import logging
from typing import Dict, List, Any, Optional
import time
import os
from api.duckdb_pool import DuckDBConnectionPool, QueryPriority

logger = logging.getLogger(__name__)

class DuckDBEngine:
    def __init__(self, storage_backend: str = "local", minio_backend=None, s3_backend=None, ceph_backend=None, gcs_backend=None, connection_manager=None):
        self.storage_backend = storage_backend
        self.minio_backend = minio_backend
        self.s3_backend = s3_backend
        self.ceph_backend = ceph_backend
        self.gcs_backend = gcs_backend
        self.connection_manager = connection_manager

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
                logger.info("DuckDB httpfs and aws extensions loaded")
            except Exception as e:
                logger.warning(f"Failed to load httpfs extension: {e}")
                # Try enabling autoloading as fallback
                try:
                    self.conn.execute("SET autoinstall_known_extensions=1")
                    self.conn.execute("SET autoload_known_extensions=1")
                    logger.info("DuckDB extension autoloading enabled")
                except Exception as e2:
                    logger.error(f"Failed to enable autoloading: {e2}")
            
            # Configure for S3/MinIO/Ceph/GCS if available (sync for immediate use)
            if minio_backend or s3_backend or ceph_backend:
                self._configure_s3_sync()
            elif gcs_backend:
                self._configure_gcs_sync()

            # Initialize connection pool
            pool_size = int(os.getenv('DUCKDB_POOL_SIZE', '5'))
            max_queue_size = int(os.getenv('DUCKDB_MAX_QUEUE_SIZE', '100'))

            self.connection_pool = DuckDBConnectionPool(
                pool_size=pool_size,
                max_queue_size=max_queue_size,
                health_check_interval=60,
                configure_fn=self._configure_connection
            )
            self.connection_pool.start_health_checks()

            logger.info(f"DuckDB engine initialized with connection pool (size={pool_size}, max_queue={max_queue_size})")

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

            # Use DuckDB default settings (no custom tuning)
            # For ClickBench compliance: databases should use default settings
            logger.info("DuckDB using default settings (no custom tuning)")

            # Apply S3/MinIO/Ceph/GCS configuration
            if self.minio_backend:
                endpoint_host = self.minio_backend.s3_client._endpoint.host.replace('http://', '').replace('https://', '')
                conn.execute(f"SET s3_endpoint='{endpoint_host}'")
                conn.execute(f"SET s3_access_key_id='{self.minio_backend.s3_client._request_signer._credentials.access_key}'")
                conn.execute(f"SET s3_secret_access_key='{self.minio_backend.s3_client._request_signer._credentials.secret_key}'")
                conn.execute("SET s3_use_ssl=false")
                conn.execute("SET s3_url_style='path'")
            elif self.ceph_backend:
                endpoint_host = self.ceph_backend.endpoint_url.replace('http://', '').replace('https://', '')
                conn.execute(f"SET s3_endpoint='{endpoint_host}'")
                conn.execute(f"SET s3_access_key_id='{self.ceph_backend._access_key}'")
                conn.execute(f"SET s3_secret_access_key='{self.ceph_backend._secret_key}'")
                conn.execute("SET s3_use_ssl=false")
                conn.execute("SET s3_url_style='path'")
            elif self.s3_backend:
                if self.s3_backend.use_directory_bucket:
                    endpoint = f"s3express-{self.s3_backend.availability_zone}.{self.s3_backend.region}.amazonaws.com"
                    conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{self.s3_backend._access_key}',
                            SECRET '{self.s3_backend._secret_key}',
                            REGION '{self.s3_backend.region}',
                            ENDPOINT '{endpoint}'
                        )
                    """)
                else:
                    conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{self.s3_backend._access_key}',
                            SECRET '{self.s3_backend._secret_key}',
                            REGION '{self.s3_backend.region}'
                        )
                    """)
            elif self.gcs_backend:
                if not self.gcs_backend.configure_duckdb(conn):
                    logger.error("Failed to configure DuckDB for GCS access")

        except Exception as e:
            logger.error(f"Connection configuration failed: {e}")
            raise
    
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
        """Synchronous S3/MinIO configuration for immediate use"""
        try:
            if self.minio_backend:
                # Configure MinIO settings directly
                endpoint_host = self.minio_backend.s3_client._endpoint.host.replace('http://', '').replace('https://', '')
                self.conn.execute(f"SET s3_endpoint='{endpoint_host}'")
                self.conn.execute(f"SET s3_access_key_id='{self.minio_backend.s3_client._request_signer._credentials.access_key}'")
                self.conn.execute(f"SET s3_secret_access_key='{self.minio_backend.s3_client._request_signer._credentials.secret_key}'")
                self.conn.execute("SET s3_use_ssl=false")
                self.conn.execute("SET s3_url_style='path'")
                logger.info("MinIO S3 configuration applied to DuckDB (sync)")
            elif self.ceph_backend:
                # Configure Ceph settings directly
                endpoint_host = self.ceph_backend.endpoint_url.replace('http://', '').replace('https://', '')
                self.conn.execute(f"SET s3_endpoint='{endpoint_host}'")
                self.conn.execute(f"SET s3_access_key_id='{self.ceph_backend._access_key}'")
                self.conn.execute(f"SET s3_secret_access_key='{self.ceph_backend._secret_key}'")
                self.conn.execute("SET s3_use_ssl=false")
                self.conn.execute("SET s3_url_style='path'")
                logger.info("Ceph S3 configuration applied to DuckDB (sync)")
            elif self.s3_backend:
                # Configure S3 using DuckDB SECRET system
                if self.s3_backend.use_directory_bucket:
                    # S3 Express One Zone configuration
                    endpoint = f"s3express-{self.s3_backend.availability_zone}.{self.s3_backend.region}.amazonaws.com"
                    self.conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{self.s3_backend._access_key}',
                            SECRET '{self.s3_backend._secret_key}',
                            REGION '{self.s3_backend.region}',
                            ENDPOINT '{endpoint}'
                        )
                    """)
                    logger.info(f"DuckDB S3 Express SECRET created (endpoint: {endpoint})")
                else:
                    # Standard S3 configuration
                    self.conn.execute(f"""
                        CREATE SECRET (
                            TYPE s3,
                            KEY_ID '{self.s3_backend._access_key}',
                            SECRET '{self.s3_backend._secret_key}',
                            REGION '{self.s3_backend.region}'
                        )
                    """)
                    logger.info("DuckDB S3 SECRET created")
                
                logger.info(f"Directory bucket settings: use_directory_bucket={self.s3_backend.use_directory_bucket}, availability_zone={self.s3_backend.availability_zone}")
                logger.info("AWS S3 configuration applied to DuckDB (sync)")
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
                
                logger.info("GCS configuration applied to DuckDB (sync) - using signed URLs for access")
            
        except Exception as e:
            logger.error(f"Sync GCS configuration failed: {e}")
    
    async def execute_query(self, sql: str, limit: int = None, priority: QueryPriority = QueryPriority.NORMAL) -> Dict[str, Any]:
        """Execute SQL query using DuckDB connection pool

        Args:
            sql: SQL query to execute
            limit: Row limit (deprecated, use LIMIT in SQL)
            priority: Query priority (LOW, NORMAL, HIGH, CRITICAL)

        Returns:
            Dict with success, data, columns, row_count, execution_time_ms, wait_time_ms
        """
        # Convert SQL for S3 paths
        converted_sql = self._convert_sql_to_s3_paths(sql)

        # Check for SHOW TABLES command (uses old connection)
        if self._is_show_tables_query(sql):
            return await self._execute_show_tables_legacy(sql)

        # Use connection pool for query execution
        result = await self.connection_pool.execute_async(
            converted_sql,
            priority=priority,
            timeout=300.0
        )

        return result

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
            
            # Convert data types for JSON serialization
            serialized_data = []
            for row in result:
                serialized_row = []
                for value in row:
                    if hasattr(value, 'isoformat'):  # datetime objects
                        serialized_row.append(value.isoformat())
                    elif isinstance(value, (int, float, str, bool)) or value is None:
                        serialized_row.append(value)
                    else:
                        serialized_row.append(str(value))
                serialized_data.append(serialized_row)
            
            return {
                "success": True,
                "data": serialized_data,
                "columns": columns,
                "row_count": len(result)
            }
            
        except Exception as e:
            raise Exception(f"DuckDB execution failed: {e}")
    
    def _convert_sql_to_s3_paths(self, sql: str) -> str:
        """Convert database.table references to S3 paths"""
        import re

        # First, handle database.table references
        pattern_db_table = r'FROM\s+(\w+)\.(\w+)'

        def replace_db_table(match):
            database = match.group(1)
            table = match.group(2)

            # Find storage connection that matches the database (prefix)
            storage_conn = None
            for conn in self.connection_manager.get_storage_connections():
                if conn.get('prefix', '').rstrip('/') == database:
                    storage_conn = conn
                    break

            if not storage_conn:
                logger.error(f"No storage connection found for database: {database}")
                return match.group(0)  # Return original if no match

            # Build storage path for the table based on backend
            prefix = storage_conn.get('prefix', '').strip()

            if storage_conn.get('backend') == 'gcs':
                # Use native gs:// URLs with DuckDB GCS support
                if self.gcs_backend:
                    bucket = storage_conn.get('bucket', '')
                    if prefix:
                        gs_path = f"gs://{bucket}/{prefix.rstrip('/')}/{table}/**/*.parquet"
                    else:
                        gs_path = f"gs://{bucket}/{table}/**/*.parquet"

                    logger.info(f"Using native GCS path for DuckDB: {gs_path}")
                    return f"FROM read_parquet('{gs_path}', union_by_name=true)"
                else:
                    logger.error("GCS backend not initialized")
                    return match.group(0)
            else:
                # Use S3-compatible path for MinIO/S3/Ceph
                if prefix:
                    s3_path = f"s3://{storage_conn['bucket']}/{prefix.rstrip('/')}/{table}/**/*.parquet"
                else:
                    s3_path = f"s3://{storage_conn['bucket']}/{table}/**/*.parquet"

                return f"FROM read_parquet('{s3_path}', union_by_name=true)"

        converted = re.sub(pattern_db_table, replace_db_table, sql, flags=re.IGNORECASE)

        # Second, handle simple table names (FROM table_name) using default bucket
        # Only match if not already converted (no read_parquet in the match context)
        pattern_simple = r'FROM\s+(?!read_parquet|information_schema|pg_)(\w+)(?!\s*\()'

        def replace_simple_table(match):
            table = match.group(1)

            # Use default historian bucket (hardcoded for now)
            bucket = "historian"
            s3_path = f"s3://{bucket}/{table}/**/*.parquet"
            logger.info(f"Converting simple table reference '{table}' to {s3_path}")
            return f"FROM read_parquet('{s3_path}', union_by_name=true)"

        converted = re.sub(pattern_simple, replace_simple_table, converted, flags=re.IGNORECASE)
        return converted
    
    def _is_show_tables_query(self, sql: str) -> bool:
        """Check if query is a SHOW TABLES command"""
        import re
        # Match SHOW TABLES or SHOW TABLES FROM database
        pattern = r'^\s*SHOW\s+TABLES(?:\s+FROM\s+(\w+))?\s*;?\s*$'
        return bool(re.match(pattern, sql.strip(), re.IGNORECASE))

    def _handle_show_tables_sync(self, sql: str) -> Dict[str, Any]:
        """Handle SHOW TABLES command"""
        import re

        # Extract database name if specified
        pattern = r'^\s*SHOW\s+TABLES(?:\s+FROM\s+(\w+))?\s*;?\s*$'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)

        if not match:
            return {
                "success": False,
                "error": "Invalid SHOW TABLES syntax",
                "data": [],
                "columns": [],
                "row_count": 0
            }

        database = match.group(1)
        logger.info(f"Showing tables for database: {database if database else 'all'}")

        try:
            # Get all storage connections
            storage_conns = self.connection_manager.get_storage_connections() if self.connection_manager else []

            # Find storage backend
            backend = self.minio_backend or self.s3_backend or self.ceph_backend or self.gcs_backend
            if not backend:
                return {
                    "success": True,
                    "data": [],
                    "columns": ["database", "table_name", "storage_path", "file_count", "total_size_mb"],
                    "row_count": 0
                }

            # List all objects
            objects = backend.list_objects()

            # Parse objects to find tables
            tables = {}
            for obj_key in objects:
                parts = obj_key.split('/')

                # Expected format: prefix/table/.../*.parquet or database/table/.../*.parquet
                if len(parts) >= 2:
                    # Check if first part matches a storage connection prefix
                    db_name = None
                    table_name = None

                    for conn in storage_conns:
                        prefix = conn.get('prefix', '').strip().rstrip('/')
                        if prefix and obj_key.startswith(prefix + '/'):
                            db_name = prefix
                            remaining = obj_key[len(prefix)+1:]
                            table_parts = remaining.split('/')
                            if table_parts:
                                table_name = table_parts[0]
                            break

                    # If no prefix matched, use first part as database
                    if not db_name:
                        db_name = parts[0]
                        table_name = parts[1] if len(parts) > 1 else parts[0]

                    # Filter by database if specified
                    if database and db_name != database:
                        continue

                    # Track table info
                    table_key = f"{db_name}.{table_name}"
                    if table_key not in tables:
                        tables[table_key] = {
                            "database": db_name,
                            "table_name": table_name,
                            "file_count": 0,
                            "total_size": 0,
                            "storage_path": f"s3://{backend.bucket}/{db_name}/{table_name}/"
                        }

                    # Count files and size if it's a parquet file
                    if obj_key.endswith('.parquet'):
                        tables[table_key]["file_count"] += 1
                        # Get object size if available
                        try:
                            obj_info = backend.client.stat_object(backend.bucket, obj_key)
                            tables[table_key]["total_size"] += obj_info.size
                        except:
                            pass

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

    async def get_measurements(self) -> List[str]:
        """Get list of available measurements"""
        try:
            backend = self.minio_backend or self.s3_backend or self.ceph_backend
            if not backend:
                return []

            objects = backend.list_objects()
            measurements = set()

            for obj_key in objects:
                parts = obj_key.split('/')
                if len(parts) >= 2:
                    measurement = parts[1]  # Assuming prefix/measurement/...
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

    def get_connection_stats(self) -> List[Dict[str, Any]]:
        """Get per-connection statistics"""
        if hasattr(self, 'connection_pool'):
            return self.connection_pool.get_connection_stats()
        return []

    def close(self):
        """Cleanup"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.close()
        if hasattr(self, 'conn'):
            self.conn.close()
