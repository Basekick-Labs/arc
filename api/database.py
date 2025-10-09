import sqlite3
import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime
from .connection_pool import get_sqlite_pool, initialize_pools
from .config import get_db_path

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_db_path()
        # Initialize connection pools
        initialize_pools(self.db_path)
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database with connections table"""
        try:
            with get_sqlite_pool().get_connection() as conn:
                cursor = conn.cursor()
                
                # Create connections table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS influx_connections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        version TEXT NOT NULL,  -- '1x', '2x', '3x', or 'timescale'
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        database_name TEXT,     -- For 1.x and TimescaleDB
                        username TEXT,          -- For 1.x and TimescaleDB
                        password TEXT,          -- For 1.x and TimescaleDB
                        token TEXT,             -- For 2.x and 3.x
                        org TEXT,               -- For 2.x and 3.x
                        bucket TEXT,            -- For 2.x
                        database TEXT,          -- For 3.x
                        ssl BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create storage connections table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS storage_connections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        backend TEXT NOT NULL,  -- 'minio', 's3', 'ceph', or 'gcs'
                        endpoint TEXT,          -- For MinIO/Ceph
                        access_key TEXT,        -- For S3/MinIO (optional for GCS)
                        secret_key TEXT,        -- For S3/MinIO (optional for GCS)
                        bucket TEXT NOT NULL,
                        database TEXT DEFAULT 'default',
                        region TEXT DEFAULT 'us-east-1',
                        ssl BOOLEAN DEFAULT FALSE,
                        use_directory_bucket BOOLEAN DEFAULT FALSE,  -- For S3 Directory Buckets
                        availability_zone TEXT,                      -- For S3 Directory Buckets
                        project_id TEXT,                            -- For GCS
                        credentials_json TEXT,                      -- For GCS service account JSON
                        credentials_file TEXT,                      -- For GCS service account file path
                        hmac_key_id TEXT,                           -- For GCS HMAC authentication (preferred)
                        hmac_secret TEXT,                           -- For GCS HMAC authentication (preferred)
                        is_active BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Add new columns if they don't exist
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN use_directory_bucket BOOLEAN DEFAULT FALSE')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN availability_zone TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                # Add GCS-specific columns if they don't exist
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN project_id TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN credentials_json TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN credentials_file TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                # Add HMAC key columns for GCS DuckDB support
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN hmac_key_id TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                try:
                    cursor.execute('ALTER TABLE storage_connections ADD COLUMN hmac_secret TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                # Add database column for InfluxDB 3.x if it doesn't exist
                try:
                    cursor.execute('ALTER TABLE influx_connections ADD COLUMN database TEXT')
                except sqlite3.OperationalError:
                    pass  # Column already exists

                # Migrate from 'prefix' to 'database' column
                try:
                    # Check if 'prefix' column exists
                    cursor.execute("PRAGMA table_info(storage_connections)")
                    columns = [row[1] for row in cursor.fetchall()]

                    if 'prefix' in columns and 'database' not in columns:
                        # Add database column and copy values from prefix
                        cursor.execute('ALTER TABLE storage_connections ADD COLUMN database TEXT DEFAULT "default"')
                        # Update database column with prefix values (remove trailing slashes)
                        cursor.execute('''UPDATE storage_connections
                                         SET database = CASE
                                             WHEN prefix = '' OR prefix IS NULL THEN 'default'
                                             ELSE TRIM(prefix, '/')
                                         END''')
                        logger.info("Migrated 'prefix' column to 'database' column in storage_connections")
                    elif 'database' not in columns:
                        # Just add the database column if it doesn't exist
                        cursor.execute('ALTER TABLE storage_connections ADD COLUMN database TEXT DEFAULT "default"')
                except sqlite3.OperationalError as e:
                    logger.warning(f"Storage connections database column migration: {e}")
                    pass  # Column already exists or other error

                conn.commit()
                
            logger.info(f"Database initialized at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def add_influx_connection(self, connection_data: Dict) -> int:
        """Add new InfluxDB connection"""
        try:
            with get_sqlite_pool().get_connection() as conn:
                cursor = conn.cursor()
                
                # Deactivate other connections if this one is active
                if connection_data.get('is_active'):
                    cursor.execute('UPDATE influx_connections SET is_active = FALSE')
                
                cursor.execute('''
                    INSERT INTO influx_connections 
                    (name, version, host, port, database_name, username, password, 
                     token, org, bucket, database, ssl, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    connection_data['name'],
                    connection_data['version'],
                    connection_data['host'],
                    connection_data['port'],
                    connection_data.get('database_name'),
                    connection_data.get('username'),
                    connection_data.get('password'),
                    connection_data.get('token'),
                    connection_data.get('org'),
                    connection_data.get('bucket'),
                    connection_data.get('database'),  # For InfluxDB 3.x
                    connection_data.get('ssl', False),
                    connection_data.get('is_active', False)
                ))
                
                connection_id = cursor.lastrowid
                conn.commit()
                
            logger.info(f"Added InfluxDB connection: {connection_data['name']}")
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to add InfluxDB connection: {e}")
            raise
    
    def add_storage_connection(self, connection_data: Dict) -> int:
        """Add new storage connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Deactivate other connections if this one is active
            if connection_data.get('is_active'):
                cursor.execute('UPDATE storage_connections SET is_active = FALSE')
            
            cursor.execute('''
                INSERT INTO storage_connections
                (name, backend, endpoint, access_key, secret_key, bucket, database, region, ssl,
                 use_directory_bucket, availability_zone, project_id, credentials_json, credentials_file,
                 hmac_key_id, hmac_secret, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                connection_data['name'],
                connection_data['backend'],
                connection_data.get('endpoint'),
                connection_data.get('access_key'),  # Optional for GCS
                connection_data.get('secret_key'),  # Optional for GCS
                connection_data['bucket'],
                connection_data.get('database', 'default'),
                connection_data.get('region', 'us-east-1'),
                connection_data.get('ssl', False),
                connection_data.get('use_directory_bucket', False),
                connection_data.get('availability_zone'),
                connection_data.get('project_id'),  # GCS specific
                connection_data.get('credentials_json'),  # GCS specific
                connection_data.get('credentials_file'),  # GCS specific
                connection_data.get('hmac_key_id'),  # GCS HMAC specific
                connection_data.get('hmac_secret'),  # GCS HMAC specific
                connection_data.get('is_active', False)
            ))
            
            connection_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.info(f"Added storage connection: {connection_data['name']}")
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to add storage connection: {e}")
            raise
    
    def get_influx_connections(self) -> List[Dict]:
        """Get all InfluxDB connections"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM influx_connections ORDER BY created_at DESC')
            connections = [dict(row) for row in cursor.fetchall()]
            
            conn.close()
            return connections
            
        except Exception as e:
            logger.error(f"Failed to get InfluxDB connections: {e}")
            return []
    
    def get_storage_connections(self) -> List[Dict]:
        """Get all storage connections"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM storage_connections ORDER BY created_at DESC')
            connections = [dict(row) for row in cursor.fetchall()]
            
            conn.close()
            return connections
            
        except Exception as e:
            logger.error(f"Failed to get storage connections: {e}")
            return []
    
    def get_storage_connection(self, connection_id: int) -> Optional[Dict]:
        """Get specific storage connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM storage_connections WHERE id = ?', (connection_id,))
            connection = cursor.fetchone()
            
            conn.close()
            return dict(connection) if connection else None
            
        except Exception as e:
            logger.error(f"Failed to get storage connection: {e}")
            return None
    
    def get_active_influx_connection(self) -> Optional[Dict]:
        """Get active InfluxDB connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM influx_connections WHERE is_active = TRUE LIMIT 1')
            connection = cursor.fetchone()
            
            conn.close()
            return dict(connection) if connection else None
            
        except Exception as e:
            logger.error(f"Failed to get active InfluxDB connection: {e}")
            return None
    
    def get_active_storage_connection(self) -> Optional[Dict]:
        """Get active storage connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM storage_connections WHERE is_active = TRUE LIMIT 1')
            connection = cursor.fetchone()
            
            conn.close()
            return dict(connection) if connection else None
            
        except Exception as e:
            logger.error(f"Failed to get active storage connection: {e}")
            return None
    
    def set_active_connection(self, connection_type: str, connection_id: int) -> bool:
        """Set a connection as active"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            table_name = f"{connection_type}_connections"
            
            # Deactivate all connections
            cursor.execute(f'UPDATE {table_name} SET is_active = FALSE')
            
            # Activate selected connection
            cursor.execute(f'UPDATE {table_name} SET is_active = TRUE WHERE id = ?', (connection_id,))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Set {connection_type} connection {connection_id} as active")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set active connection: {e}")
            return False
    
    def delete_connection(self, connection_type: str, connection_id: int) -> bool:
        """Delete a connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            table_name = f"{connection_type}_connections"
            cursor.execute(f'DELETE FROM {table_name} WHERE id = ?', (connection_id,))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Deleted {connection_type} connection {connection_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete connection: {e}")
            return False
    
    def test_influx_connection(self, connection_data: Dict) -> Dict:
        """Test InfluxDB/TimescaleDB connection with timeout"""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Connection test timed out after 5 seconds")
        
        try:
            # Set 5 second timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(5)
            
            if connection_data['version'] == 'timescale':
                # Test TimescaleDB connection
                import asyncio
                from exporter.timescale_exporter import TimescaleExporter
                
                exporter = TimescaleExporter(
                    host=connection_data['host'],
                    port=connection_data['port'],
                    database=connection_data['database_name'],
                    username=connection_data['username'],
                    password=connection_data['password'],
                    ssl=connection_data.get('ssl', False)
                )
                
                # Run async test in sync context
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(exporter.test_connection())
                    loop.run_until_complete(exporter.close())
                finally:
                    loop.close()
                
                signal.alarm(0)
                return result
                
            elif connection_data['version'] == '1x':
                # For reverse proxy scenarios, use HTTP requests instead of InfluxDB client
                import requests
                
                base_url = f"{'https' if connection_data.get('ssl') else 'http'}://{connection_data['host']}:{connection_data['port']}"
                
                # Test ping endpoint
                ping_url = f"{base_url}/ping"
                response = requests.get(ping_url, timeout=3, verify=True if connection_data.get('ssl') else False)
                if response.status_code != 204:
                    raise Exception(f"Ping failed: {response.status_code} {response.text}")
                
                # Try to get databases
                query_url = f"{base_url}/query"
                params = {
                    'q': 'SHOW DATABASES',
                    'u': connection_data.get('username'),
                    'p': connection_data.get('password')
                }
                response = requests.get(query_url, params=params, timeout=3, verify=True if connection_data.get('ssl') else False)
                if response.status_code == 200:
                    result = response.json()
                    databases = []
                    if 'results' in result and result['results']:
                        series = result['results'][0].get('series', [])
                        if series:
                            databases = [row[0] for row in series[0].get('values', [])]
                else:
                    databases = ['Unable to list databases']
                
                signal.alarm(0)  # Cancel timeout
                return {
                    "success": True,
                    "message": f"Connected successfully via reverse proxy. Found {len(databases)} databases.",
                    "databases": databases
                }
                
            elif connection_data['version'] == '2x':
                import requests
                
                base_url = f"{'https' if connection_data.get('ssl') else 'http'}://{connection_data['host']}:{connection_data['port']}"
                
                # Test health endpoint
                health_url = f"{base_url}/health"
                response = requests.get(health_url, timeout=3, verify=True if connection_data.get('ssl') else False)
                if response.status_code != 200:
                    raise Exception(f"Health check failed: {response.status_code} {response.text}")
                
                # Try to get buckets
                buckets_url = f"{base_url}/api/v2/buckets"
                headers = {'Authorization': f'Token {connection_data.get("token")}'}
                response = requests.get(buckets_url, headers=headers, timeout=3, verify=True if connection_data.get('ssl') else False)
                
                buckets = []
                if response.status_code == 200:
                    result = response.json()
                    buckets = [bucket['name'] for bucket in result.get('buckets', [])]
                else:
                    buckets = ['Unable to list buckets']
                
                signal.alarm(0)
                return {
                    "success": True,
                    "message": f"Connected successfully via reverse proxy. Found {len(buckets)} buckets.",
                    "buckets": buckets
                }
                
            else:  # 3x
                import requests
                
                base_url = f"{'https' if connection_data.get('ssl') else 'http'}://{connection_data['host']}:{connection_data['port']}"
                
                # Test health endpoint
                health_url = f"{base_url}/health"
                response = requests.get(health_url, timeout=3, verify=True if connection_data.get('ssl') else False)
                if response.status_code != 200:
                    raise Exception(f"Health check failed: {response.status_code} {response.text}")
                
                signal.alarm(0)
                return {
                    "success": True,
                    "message": f"Connected successfully to InfluxDB 3.x at {connection_data['host']}:{connection_data['port']}",
                    "database": connection_data.get('database', 'N/A')
                }
                
        except TimeoutError as e:
            signal.alarm(0)
            return {
                "success": False,
                "message": "Connection test timed out - check host and port"
            }
        except Exception as e:
            signal.alarm(0)
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}"
            }
    
    def test_storage_connection(self, connection_data: Dict) -> Dict:
        """Test storage connection with timeout"""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Storage test timed out after 5 seconds")
        
        try:
            # Set 5 second timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(5)
            
            import boto3
            from botocore.exceptions import ClientError
            from botocore.config import Config
            
            # Configure with timeout and path-style addressing
            config = Config(
                connect_timeout=3,
                read_timeout=3,
                retries={'max_attempts': 1},
                s3={'addressing_style': 'path'}
            )
            
            if connection_data['backend'] == 'gcs':
                # Google Cloud Storage connection
                from storage.gcs_backend import GCSBackend
                gcs_backend = GCSBackend(
                    bucket=connection_data['bucket'],
                    database=connection_data.get('database', 'default'),
                    project_id=connection_data.get('project_id'),
                    credentials_json=connection_data.get('credentials_json'),
                    credentials_file=connection_data.get('credentials_file'),
                    hmac_key_id=connection_data.get('hmac_key_id'),
                    hmac_secret=connection_data.get('hmac_secret')
                )
                result = gcs_backend.test_connection()
                signal.alarm(0)  # Clear timeout
                return result
            elif connection_data['backend'] in ['minio', 'ceph']:
                # MinIO/Ceph connection
                s3_client = boto3.client(
                    's3',
                    endpoint_url=connection_data.get('endpoint'),
                    aws_access_key_id=connection_data['access_key'],
                    aws_secret_access_key=connection_data['secret_key'],
                    region_name=connection_data.get('region', 'us-east-1'),
                    config=config
                )
            else:
                # AWS S3 connection
                client_kwargs = {
                    'aws_access_key_id': connection_data['access_key'],
                    'aws_secret_access_key': connection_data['secret_key'],
                    'region_name': connection_data.get('region', 'us-east-1'),
                    'config': config
                }
                
                # Use Directory Bucket configuration if enabled
                if connection_data.get('use_directory_bucket'):
                    availability_zone = connection_data.get('availability_zone', 'us-east-1a')
                    region = connection_data.get('region', 'us-east-1')
                    # For S3 Express, use standard S3 endpoint with special config
                    client_kwargs['config'] = Config(
                        connect_timeout=3,
                        read_timeout=3,
                        retries={'max_attempts': 1},
                        s3={
                            'addressing_style': 'path',
                            'use_accelerate_endpoint': False,
                            'use_dualstack_endpoint': False,
                            'use_virtual_host_bucket': False
                        }
                    )
                    logger.info(f"Using S3 Express Directory Bucket configuration for AZ: {availability_zone}")
                else:
                    client_kwargs['config'] = config
                
                s3_client = boto3.client('s3', **client_kwargs)
            
            # Test connection by checking specific bucket access
            bucket_name = connection_data['bucket']
            
            try:
                # Try to list objects in the specific bucket (limited to 1)
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    MaxKeys=1
                )
                
                object_count = response.get('KeyCount', 0)
                
                signal.alarm(0)  # Cancel timeout
                return {
                    "success": True,
                    "message": f"✅ Connected successfully to bucket '{bucket_name}'. Found {object_count} objects (showing max 1).",
                    "bucket_exists": True
                }
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchBucket':
                    signal.alarm(0)
                    return {
                        "success": False,
                        "message": f"❌ Bucket '{bucket_name}' does not exist"
                    }
                else:
                    raise  # Re-raise other errors
            
        except TimeoutError:
            signal.alarm(0)
            return {
                "success": False,
                "message": "Storage connection test timed out - check endpoint and credentials"
            }
        except Exception as e:
            signal.alarm(0)
            return {
                "success": False,
                "message": f"Storage connection failed: {str(e)}"
            }
    
    def update_influx_connection(self, connection_id: int, connection_data: Dict) -> bool:
        """Update existing InfluxDB connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Deactivate other connections if this one is being set active
            if connection_data.get('is_active'):
                cursor.execute('UPDATE influx_connections SET is_active = FALSE')
            
            cursor.execute('''
                UPDATE influx_connections SET
                name = ?, version = ?, host = ?, port = ?, database_name = ?,
                username = ?, password = ?, token = ?, org = ?, bucket = ?,
                database = ?, ssl = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                connection_data['name'],
                connection_data['version'],
                connection_data['host'],
                connection_data['port'],
                connection_data.get('database_name'),
                connection_data.get('username'),
                connection_data.get('password'),
                connection_data.get('token'),
                connection_data.get('org'),
                connection_data.get('bucket'),
                connection_data.get('database'),  # For InfluxDB 3.x
                connection_data.get('ssl', False),
                connection_data.get('is_active', False),
                connection_id
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated InfluxDB connection: {connection_data['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update InfluxDB connection: {e}")
            return False
    
    def update_storage_connection(self, connection_id: int, connection_data: Dict) -> bool:
        """Update existing storage connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Deactivate other connections if this one is being set active
            if connection_data.get('is_active'):
                cursor.execute('UPDATE storage_connections SET is_active = FALSE')
            
            cursor.execute('''
                UPDATE storage_connections SET
                name = ?, backend = ?, endpoint = ?, access_key = ?, secret_key = ?,
                bucket = ?, database = ?, region = ?, ssl = ?, use_directory_bucket = ?,
                availability_zone = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                connection_data['name'],
                connection_data['backend'],
                connection_data.get('endpoint'),
                connection_data['access_key'],
                connection_data['secret_key'],
                connection_data['bucket'],
                connection_data.get('database', 'default'),
                connection_data.get('region', 'us-east-1'),
                connection_data.get('ssl', False),
                connection_data.get('use_directory_bucket', False),
                connection_data.get('availability_zone'),
                connection_data.get('is_active', False),
                connection_id
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated storage connection: {connection_data['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update storage connection: {e}")
            return False

    def get_existing_measurements(self) -> List[str]:
        """Return a list of distinct measurement names referenced by export jobs."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            measurements = set()
            # Export jobs (measurement-type only)
            cursor.execute("SELECT DISTINCT measurement FROM export_jobs WHERE job_type='measurement' AND measurement IS NOT NULL")
            for (m,) in cursor.fetchall():
                if m:
                    measurements.add(m)
            conn.close()
            return sorted(measurements)
        except Exception as e:
            logger.error(f"Failed to get existing measurements: {e}")
            return []

    def count_distinct_measurements(self) -> int:
        """Count distinct measurements across export jobs."""
        return len(self.get_existing_measurements())


class CompactionLock:
    """
    Manages locks for compaction jobs using SQLite.
    Prevents concurrent compaction of the same partition.
    """

    def __init__(self, db_path: str = None):
        """
        Initialize compaction lock manager

        Args:
            db_path: Path to SQLite database (defaults to arc.db from config)
        """
        if db_path is None:
            from api.config import get_db_path
            db_path = get_db_path()

        self.db_path = db_path
        self._init_table()

    def _init_table(self):
        """Initialize compaction_locks table"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compaction_locks (
                    partition_path TEXT PRIMARY KEY,
                    worker_id INTEGER NOT NULL,
                    locked_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL
                )
            ''')

            conn.commit()
            conn.close()

            logger.debug("Compaction locks table initialized")

        except Exception as e:
            logger.error(f"Failed to initialize compaction_locks table: {e}")
            raise

    def acquire_lock(self, partition_path: str, ttl_hours: int = 2) -> bool:
        """
        Try to acquire lock for a partition

        Args:
            partition_path: Partition path (e.g., 'cpu/2025/10/08/14')
            ttl_hours: Lock time-to-live in hours (for crash recovery)

        Returns:
            True if lock acquired, False otherwise
        """
        from datetime import timedelta

        worker_id = os.getpid()
        now = datetime.now()
        expires = now + timedelta(hours=ttl_hours)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Try to insert lock
            cursor.execute('''
                INSERT INTO compaction_locks
                (partition_path, worker_id, locked_at, expires_at)
                VALUES (?, ?, ?, ?)
            ''', (partition_path, worker_id, now, expires))

            conn.commit()
            conn.close()

            logger.debug(f"Acquired lock for {partition_path} (worker {worker_id})")
            return True

        except sqlite3.IntegrityError:
            # Lock already exists, check if expired
            conn.close()
            return self._check_and_steal_expired(partition_path, worker_id, ttl_hours)

        except Exception as e:
            logger.error(f"Failed to acquire lock for {partition_path}: {e}")
            return False

    def _check_and_steal_expired(
        self,
        partition_path: str,
        worker_id: int,
        ttl_hours: int
    ) -> bool:
        """
        Check if existing lock is expired and steal it if so

        Args:
            partition_path: Partition path
            worker_id: Current worker ID
            ttl_hours: TTL for new lock

        Returns:
            True if lock stolen and acquired
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Delete expired locks
            cursor.execute('''
                DELETE FROM compaction_locks
                WHERE partition_path = ?
                AND expires_at < ?
            ''', (partition_path, datetime.now()))

            deleted = cursor.rowcount
            conn.commit()
            conn.close()

            if deleted > 0:
                logger.info(
                    f"Stole expired lock for {partition_path}, retrying acquisition"
                )
                # Expired lock removed, try to acquire again
                return self.acquire_lock(partition_path, ttl_hours)

            # Lock not expired
            logger.debug(f"Lock for {partition_path} is held by another worker")
            return False

        except Exception as e:
            logger.error(f"Failed to check/steal expired lock for {partition_path}: {e}")
            return False

    def release_lock(self, partition_path: str):
        """
        Release lock for a partition

        Args:
            partition_path: Partition path
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                DELETE FROM compaction_locks
                WHERE partition_path = ?
            ''', (partition_path,))

            conn.commit()
            conn.close()

            logger.debug(f"Released lock for {partition_path}")

        except Exception as e:
            logger.error(f"Failed to release lock for {partition_path}: {e}")

    def get_active_locks(self) -> List[Dict[str, Any]]:
        """
        Get all active locks

        Returns:
            List of lock info dicts
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM compaction_locks
                WHERE expires_at > ?
                ORDER BY locked_at DESC
            ''', (datetime.now(),))

            locks = [dict(row) for row in cursor.fetchall()]

            conn.close()
            return locks

        except Exception as e:
            logger.error(f"Failed to get active locks: {e}")
            return []

    def cleanup_expired_locks(self) -> int:
        """
        Cleanup all expired locks

        Returns:
            Number of locks cleaned up
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                DELETE FROM compaction_locks
                WHERE expires_at < ?
            ''', (datetime.now(),))

            deleted = cursor.rowcount
            conn.commit()
            conn.close()

            if deleted > 0:
                logger.info(f"Cleaned up {deleted} expired compaction locks")

            return deleted

        except Exception as e:
            logger.error(f"Failed to cleanup expired locks: {e}")
            return 0

