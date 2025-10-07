import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import asyncio
from croniter import croniter
import threading
import time
from api.config import get_db_path

logger = logging.getLogger(__name__)

class ExportScheduler:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_db_path()
        self.running_jobs = {}
        self.cancelled_jobs = set()
        self.scheduler_thread = None
        self.stop_event = threading.Event()
        self.background_tasks = set()
        self._init_scheduler_tables()
    
    def _init_scheduler_tables(self):
        """Initialize scheduler database tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Export jobs table (updated to support multiple source types)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS export_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    job_type TEXT NOT NULL,  -- 'measurement' or 'database'
                    measurement TEXT,        -- NULL for database-wide
                    source_type TEXT NOT NULL DEFAULT 'influx',  -- 'influx', 'http_json'
                    influx_connection_id INTEGER,  -- Source InfluxDB (optional if using other source)
                    http_json_connection_id INTEGER,  -- Source HTTP JSON (optional)
                    storage_connection_id INTEGER NOT NULL, -- Destination storage
                    cron_schedule TEXT NOT NULL,
                    chunk_size TEXT NOT NULL,  -- '1h', '1d', '1w', etc.
                    overlap_buffer TEXT DEFAULT '5m',
                    initial_export_mode TEXT DEFAULT 'full',  -- 'full', 'from_date', 'chunked', 'retention_policy'
                    initial_start_date TEXT,
                    initial_chunk_duration TEXT DEFAULT '1d',
                    retention_days INTEGER DEFAULT 365,  -- For retention_policy mode
                    export_buffer_days INTEGER DEFAULT 7,  -- Safety buffer for retention
                    max_retries INTEGER DEFAULT 3,
                    retry_delay INTEGER DEFAULT 300,  -- 5 minutes
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (influx_connection_id) REFERENCES influx_connections (id),
                    FOREIGN KEY (http_json_connection_id) REFERENCES http_json_connections (id),
                    FOREIGN KEY (storage_connection_id) REFERENCES storage_connections (id)
                )
            ''')
            
            # Add missing columns if they don't exist
            try:
                cursor.execute('ALTER TABLE export_jobs ADD COLUMN retention_days INTEGER DEFAULT 365')
            except sqlite3.OperationalError:
                pass  # Column already exists

            try:
                cursor.execute('ALTER TABLE export_jobs ADD COLUMN export_buffer_days INTEGER DEFAULT 7')
            except sqlite3.OperationalError:
                pass  # Column already exists

            try:
                cursor.execute('ALTER TABLE export_jobs ADD COLUMN source_type TEXT DEFAULT "influx"')
            except sqlite3.OperationalError:
                pass  # Column already exists

            try:
                cursor.execute('ALTER TABLE export_jobs ADD COLUMN http_json_connection_id INTEGER')
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            # Export state tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS export_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    measurement TEXT NOT NULL,
                    last_export_time TIMESTAMP,
                    total_records_exported INTEGER DEFAULT 0,
                    last_run_status TEXT DEFAULT 'pending',  -- 'pending', 'running', 'completed', 'failed'
                    last_run_start TIMESTAMP,
                    last_run_end TIMESTAMP,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES export_jobs (id),
                    UNIQUE(job_id, measurement)
                )
            ''')
            
            # Job execution history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    measurement TEXT,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL,  -- 'running', 'completed', 'failed'
                    records_exported INTEGER DEFAULT 0,
                    error_message TEXT,
                    execution_duration_seconds INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES export_jobs (id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Scheduler tables initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize scheduler tables: {e}")
            raise
    
    def create_job(self, job_config: Dict) -> int:
        """Create new export job"""
        try:
            # Validate configuration before creating
            self._validate_job_config(job_config)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO export_jobs
                (name, job_type, measurement, source_type, influx_connection_id, http_json_connection_id,
                 storage_connection_id, cron_schedule, chunk_size, overlap_buffer, initial_export_mode,
                 initial_start_date, initial_chunk_duration, max_retries, retry_delay,
                 retention_days, export_buffer_days, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_config['name'],
                job_config['job_type'],
                job_config.get('measurement'),
                job_config.get('source_type', 'influx'),  # Default to influx for backward compatibility
                job_config.get('influx_connection_id'),
                job_config.get('http_json_connection_id'),
                job_config['storage_connection_id'],
                job_config['cron_schedule'],
                job_config['chunk_size'],
                job_config.get('overlap_buffer', '5m'),
                job_config.get('initial_export_mode', 'full'),
                job_config.get('initial_start_date'),
                job_config.get('initial_chunk_duration', '1d'),
                job_config.get('max_retries', 3),
                job_config.get('retry_delay', 300),
                job_config.get('retention_days', 365),
                job_config.get('export_buffer_days', 7),
                job_config.get('is_active', True)
            ))
            
            job_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.info(f"Created export job: {job_config['name']}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to create export job: {e}")
            raise
    
    def get_jobs(self) -> List[Dict]:
        """Get all export jobs"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT j.*, 
                       COUNT(s.id) as measurement_count,
                       MAX(s.last_export_time) as last_export_time,
                       MAX(s.last_run_status) as last_status,
                       ic.name as influx_connection_name,
                       sc.name as storage_connection_name
                FROM export_jobs j
                LEFT JOIN export_state s ON j.id = s.job_id
                LEFT JOIN influx_connections ic ON j.influx_connection_id = ic.id
                LEFT JOIN storage_connections sc ON j.storage_connection_id = sc.id
                GROUP BY j.id
                ORDER BY j.created_at DESC
            ''')
            
            jobs = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get export jobs: {e}")
            return []
    
    def update_job(self, job_id: int, job_config: Dict) -> bool:
        """Update existing export job"""
        try:
            # Validate configuration before updating
            self._validate_job_config(job_config)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE export_jobs SET
                name = ?, job_type = ?, measurement = ?, influx_connection_id = ?,
                storage_connection_id = ?, cron_schedule = ?, chunk_size = ?,
                overlap_buffer = ?, initial_export_mode = ?, initial_start_date = ?,
                initial_chunk_duration = ?, max_retries = ?, retry_delay = ?,
                retention_days = ?, export_buffer_days = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                job_config['name'],
                job_config['job_type'],
                job_config.get('measurement'),
                job_config['influx_connection_id'],
                job_config['storage_connection_id'],
                job_config['cron_schedule'],
                job_config['chunk_size'],
                job_config.get('overlap_buffer', '5m'),
                job_config.get('initial_export_mode', 'full'),
                job_config.get('initial_start_date'),
                job_config.get('initial_chunk_duration', '1d'),
                job_config.get('max_retries', 3),
                job_config.get('retry_delay', 300),
                job_config.get('retention_days', 365),
                job_config.get('export_buffer_days', 7),
                job_config.get('is_active', True),
                job_id
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated export job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update export job: {e}")
            return False

    def _validate_job_config(self, job_config: Dict) -> None:
        """Validate export job configuration and raise ValueError on problems."""
        # Required basics
        job_type = job_config.get('job_type')
        if job_type not in ('measurement', 'database'):
            raise ValueError("job_type must be 'measurement' or 'database'")
        if job_type == 'measurement' and not job_config.get('measurement'):
            raise ValueError("measurement is required for job_type 'measurement'")

        # Durations
        chunk_size = job_config.get('chunk_size', '1d')
        overlap_buffer = job_config.get('overlap_buffer', '5m')
        chunk_td = self._parse_duration(chunk_size)
        overlap_td = self._parse_duration(overlap_buffer)
        if overlap_td <= timedelta(0):
            raise ValueError("overlap_buffer must be > 0")
        if chunk_td <= timedelta(0):
            raise ValueError("chunk_size must be > 0")
        if overlap_td >= chunk_td:
            raise ValueError("overlap_buffer must be less than chunk_size")

        # Cron schedule
        cron_expr = job_config.get('cron_schedule')
        if not cron_expr:
            raise ValueError("cron_schedule is required")
        try:
            _ = croniter(cron_expr, datetime.now())
        except Exception:
            raise ValueError("Invalid cron_schedule expression")

        # Initial export mode specifics
        mode = job_config.get('initial_export_mode', 'full')
        if mode not in ('full', 'from_date', 'chunked', 'retention_policy'):
            raise ValueError("initial_export_mode must be one of: full, from_date, chunked, retention_policy")

        if mode == 'from_date':
            start_str = job_config.get('initial_start_date')
            if not start_str:
                raise ValueError("initial_start_date is required for from_date mode")
            try:
                _ = datetime.fromisoformat(start_str)
            except Exception:
                raise ValueError("initial_start_date must be ISO8601 format")

        if mode == 'retention_policy':
            retention_days = int(job_config.get('retention_days', 365))
            buffer_days = int(job_config.get('export_buffer_days', 7))
            if retention_days <= 0:
                raise ValueError("retention_days must be > 0")
            if buffer_days < 0:
                raise ValueError("export_buffer_days must be >= 0")
            if retention_days <= buffer_days:
                raise ValueError("retention_days must be greater than export_buffer_days")

        # Connection validation
        source_type = job_config.get('source_type', 'influx')
        if source_type == 'http_json':
            if not job_config.get('http_json_connection_id'):
                raise ValueError("http_json_connection_id is required for http_json source_type")
        else:
            if not job_config.get('influx_connection_id'):
                raise ValueError("influx_connection_id is required for non-http_json source_type")

        if not job_config.get('storage_connection_id'):
            raise ValueError("storage_connection_id is required")
    
    def delete_job(self, job_id: int) -> bool:
        """Delete export job and related data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Delete related data
            cursor.execute('DELETE FROM job_executions WHERE job_id = ?', (job_id,))
            cursor.execute('DELETE FROM export_state WHERE job_id = ?', (job_id,))
            cursor.execute('DELETE FROM export_jobs WHERE id = ?', (job_id,))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Deleted export job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete export job: {e}")
            return False
    
    def get_job_executions(self, job_id: int, limit: int = 50) -> List[Dict]:
        """Get job execution history"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM job_executions 
                WHERE job_id = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (job_id, limit))
            
            executions = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return executions
            
        except Exception as e:
            logger.error(f"Failed to get job executions: {e}")
            return []
    
    def start_scheduler(self):
        """Start the background scheduler"""
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            logger.warning("Scheduler already running")
            return
        
        self.stop_event.clear()
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        logger.info("Export scheduler started")
    
    def stop_scheduler(self):
        """Stop the background scheduler"""
        if self.scheduler_thread:
            self.stop_event.set()
            self.scheduler_thread.join(timeout=15)  # Increased timeout to allow task cleanup
            logger.info("Export scheduler stopped")

        # Cancel any pending background tasks
        if self.background_tasks:
            for task in self.background_tasks.copy():
                if not task.done():
                    task.cancel()
            logger.info(f"Cancelled {len(self.background_tasks)} pending background tasks")
    
    def is_running(self):
        """Check if scheduler is running"""
        return (self.scheduler_thread is not None and 
                self.scheduler_thread.is_alive() and 
                not self.stop_event.is_set())
    
    def _scheduler_loop(self):
        """Main scheduler loop with persistent event loop"""
        # Create a persistent event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Configure a larger thread pool for better concurrent performance
        import concurrent.futures
        from functools import partial

        # Use a larger thread pool (20 threads) for HTTP requests
        thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        loop.set_default_executor(thread_pool)

        try:
            while not self.stop_event.is_set():
                try:
                    # Use the persistent loop instead of creating new ones
                    loop.run_until_complete(self._async_check_and_run_jobs())
                    time.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Scheduler loop error: {e}")
                    time.sleep(60)
        finally:
            # Clean up any remaining tasks before closing the loop
            pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
            if pending_tasks:
                logger.info(f"Cleaning up {len(pending_tasks)} pending tasks")
                for task in pending_tasks:
                    task.cancel()
                # Wait for tasks to complete cancellation
                if pending_tasks:
                    loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))

            # Clean up the thread pool
            if 'thread_pool' in locals():
                thread_pool.shutdown(wait=True)
                logger.info("Scheduler thread pool shut down")

            loop.close()

    def _check_and_run_jobs(self):
        """Check for jobs that need to run (deprecated - use persistent loop instead)"""
        try:
            # This method is now unused, keeping for backward compatibility
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._async_check_and_run_jobs())
        except Exception as e:
            logger.error(f"Failed to check jobs: {e}")
    
    async def _async_check_and_run_jobs(self):
        """Async version of job checking"""
        try:
            # Clean up stale jobs first
            stale_count = self.cleanup_stale_jobs(max_runtime_hours=2)
            if stale_count > 0:
                logger.info(f"Cleaned up {stale_count} stale jobs")

            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM export_jobs
                WHERE is_active = TRUE
            ''')

            jobs = cursor.fetchall()
            conn.close()

            current_time = datetime.now()

            for job in jobs:
                job_dict = dict(job)
                job_id = job_dict['id']

                # Skip if job is already running
                if job_id in self.running_jobs:
                    logger.debug(f"Job {job_dict['name']} (ID: {job_id}) is already running, skipping")
                    continue

                if self._should_run_job(job_dict, current_time):
                    logger.info(f"Triggering scheduled job: {job_dict['name']}")
                    # Execute the job asynchronously without blocking the scheduler
                    task = asyncio.create_task(self._run_job_background(job_dict))
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.discard)
                    
        except Exception as e:
            logger.error(f"Failed to check jobs: {e}")
    
    async def _run_job_background(self, job_dict: Dict):
        """Run a job in the background without blocking the scheduler"""
        job_id = job_dict['id']
        try:
            # Set a timeout for job execution (e.g., 2 hours)
            timeout_seconds = 7200  # 2 hours
            success = await asyncio.wait_for(
                self.execute_job_now(job_dict),
                timeout=timeout_seconds
            )
            if success:
                logger.info(f"Scheduled job {job_dict['name']} completed successfully")
            else:
                logger.error(f"Scheduled job {job_dict['name']} failed")
        except asyncio.TimeoutError:
            logger.error(f"Background job {job_dict['name']} timed out after {timeout_seconds} seconds")
            # Clean up on timeout
            self.running_jobs.pop(job_id, None)
        except Exception as e:
            logger.error(f"Background job {job_dict['name']} failed: {e}")
            # Clean up on exception
            self.running_jobs.pop(job_id, None)
    
    def _should_run_job(self, job: Dict, current_time: datetime) -> bool:
        """Check if job should run based on cron schedule"""
        try:
            cron = croniter(job['cron_schedule'], current_time - timedelta(minutes=1))
            next_run = cron.get_next(datetime)
            
            # Job should run if next scheduled time is within the last minute
            return abs((next_run - current_time).total_seconds()) < 60
            
        except Exception as e:
            logger.error(f"Failed to check job schedule: {e}")
            return False
    
    def _parse_duration(self, duration_str: str) -> timedelta:
        """Parse duration string like '1h', '2d', '1w' to timedelta"""
        try:
            if duration_str.endswith('m'):
                return timedelta(minutes=int(duration_str[:-1]))
            elif duration_str.endswith('h'):
                return timedelta(hours=int(duration_str[:-1]))
            elif duration_str.endswith('d'):
                return timedelta(days=int(duration_str[:-1]))
            elif duration_str.endswith('w'):
                return timedelta(weeks=int(duration_str[:-1]))
            else:
                raise ValueError(f"Invalid duration format: {duration_str}")
        except Exception as e:
            logger.error(f"Failed to parse duration {duration_str}: {e}")
            return timedelta(hours=1)  # Default fallback
    
    def cancel_job(self, job_id: int) -> bool:
        """Cancel a running job"""
        try:
            if job_id in self.running_jobs:
                self.cancelled_jobs.add(job_id)
                logger.info(f"Job {job_id} marked for cancellation")
                return True
            else:
                logger.warning(f"Job {job_id} is not currently running")
                return False
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False
    
    def get_running_jobs(self) -> Dict:
        """Get currently running jobs"""
        return dict(self.running_jobs)

    def cleanup_stale_jobs(self, max_runtime_hours: int = 2):
        """Clean up jobs that have been running for too long"""
        from datetime import datetime as dt, timedelta

        current_time = dt.now()
        stale_jobs = []

        for job_id, job_info in self.running_jobs.items():
            runtime = current_time - job_info['start_time']
            if runtime > timedelta(hours=max_runtime_hours):
                stale_jobs.append((job_id, job_info['name'], runtime))

        for job_id, job_name, runtime in stale_jobs:
            logger.warning(f"Cleaning up stale job {job_name} (ID: {job_id}) that has been running for {runtime}")
            self.running_jobs.pop(job_id, None)

        return len(stale_jobs)
    
    async def execute_job_now(self, job: Dict) -> bool:
        """Execute a job immediately"""
        job_id = job['id']
        
        # Check if job was cancelled (only if it's currently running)
        if job_id in self.cancelled_jobs and job_id in self.running_jobs:
            self.cancelled_jobs.discard(job_id)
            logger.info(f"Job {job['name']} was cancelled")
            return False
        
        # Clear any stale cancellation flags for this job
        if job_id in self.cancelled_jobs:
            self.cancelled_jobs.discard(job_id)
        
        # Track running job
        from datetime import datetime as dt
        self.running_jobs[job_id] = {
            'name': job['name'],
            'start_time': dt.now(),
            'status': 'running'
        }
            
        try:
            from api.database import ConnectionManager
            
            # Get connection details
            conn_mgr = ConnectionManager(self.db_path)

            # Get source connection based on source type
            source_type = job.get('source_type', 'influx')
            source_conn = None
            storage_conn = None

            if source_type == 'http_json':
                # Get HTTP JSON connection
                http_json_conn_id = job.get('http_json_connection_id')
                if http_json_conn_id:
                    for conn in conn_mgr.get_http_json_connections():
                        if conn['id'] == http_json_conn_id:
                            source_conn = conn
                            break
            else:
                # Get InfluxDB connection (default for backward compatibility)
                influx_conn_id = job.get('influx_connection_id')
                if influx_conn_id:
                    for conn in conn_mgr.get_influx_connections():
                        if conn['id'] == influx_conn_id:
                            source_conn = conn
                            break

            # Get storage connection
            for conn in conn_mgr.get_storage_connections():
                if conn['id'] == job['storage_connection_id']:
                    storage_conn = conn
                    break

            if not source_conn or not storage_conn:
                logger.error(f"Missing connections for job {job['name']}")
                return False

            # For backward compatibility, alias influx_conn
            influx_conn = source_conn if source_type != 'http_json' else None
            
            # Record job execution start
            execution_id = self._record_job_start(job['id'], job.get('measurement'))
            
            # Update database to show running status
            self._update_job_status(execution_id, 'running')
            
            try:
                # Import and run the actual export based on source type
                if source_type == 'http_json':
                    # Handle HTTP JSON export
                    from exporter.http_json_exporter import HTTPJsonExporter

                    logger.info(f"Creating HTTP JSON exporter for {source_conn['name']}")
                    exporter = HTTPJsonExporter(source_conn)

                elif influx_conn and influx_conn['version'] == 'timescale':
                    from exporter.timescale_exporter import TimescaleExporter
                    exporter = TimescaleExporter(
                        host=influx_conn['host'],
                        port=influx_conn['port'],
                        database=influx_conn.get('database_name', 'historian'),
                        username=influx_conn.get('username'),
                        password=influx_conn.get('password'),
                        ssl=influx_conn.get('ssl', False)
                    )
                elif influx_conn['version'] == '1x':
                    from exporter.influx1x_exporter import InfluxDB1xExporter
                    exporter = InfluxDB1xExporter(
                        host=influx_conn['host'],
                        port=influx_conn['port'],
                        username=influx_conn.get('username'),
                        password=influx_conn.get('password'),
                        database=influx_conn.get('database_name'),
                        ssl=influx_conn.get('ssl', False)
                    )
                else:  # 2x, 3x
                    from exporter.influx2x_exporter import InfluxDB2xExporter
                    exporter = InfluxDB2xExporter(
                        url=f"http://{influx_conn['host']}:{influx_conn['port']}",
                        token=influx_conn.get('token'),
                        org=influx_conn.get('org'),
                        bucket=influx_conn.get('bucket')
                    )
                
                # Setup storage backend
                if storage_conn['backend'] == 'minio':
                    from storage.minio_backend import MinIOBackend
                    storage_backend = MinIOBackend(
                        endpoint_url=storage_conn['endpoint'],
                        access_key=storage_conn['access_key'],
                        secret_key=storage_conn['secret_key'],
                        bucket=storage_conn['bucket'],
                        prefix=storage_conn.get('prefix', '')
                    )
                elif storage_conn['backend'] == 'ceph':
                    from storage.ceph_backend import CephBackend
                    storage_backend = CephBackend(
                        endpoint_url=storage_conn['endpoint'],
                        access_key=storage_conn['access_key'],
                        secret_key=storage_conn['secret_key'],
                        bucket=storage_conn['bucket'],
                        region=storage_conn.get('region', 'us-east-1'),
                        prefix=storage_conn.get('prefix', '')
                    )
                elif storage_conn['backend'] == 'gcs':
                    from storage.gcs_backend import GCSBackend
                    storage_backend = GCSBackend(
                        bucket=storage_conn['bucket'],
                        prefix=storage_conn.get('prefix', ''),
                        project_id=storage_conn.get('project_id'),
                        credentials_json=storage_conn.get('credentials_json'),
                        credentials_file=storage_conn.get('credentials_file'),
                        hmac_key_id=storage_conn.get('hmac_key_id'),
                        hmac_secret=storage_conn.get('hmac_secret')
                    )
                else:
                    from storage.s3_backend import S3Backend
                    storage_backend = S3Backend(
                        bucket=storage_conn['bucket'],
                        region=storage_conn.get('region', 'us-east-1'),
                        prefix=storage_conn.get('prefix', 'historian/'),
                        access_key=storage_conn.get('access_key'),
                        secret_key=storage_conn.get('secret_key'),
                        use_directory_bucket=storage_conn.get('use_directory_bucket', False),
                        availability_zone=storage_conn.get('availability_zone')
                    )
                
                # Import required modules
                from datetime import datetime, timedelta
                import tempfile
                from pathlib import Path
                
                # Execute export
                if job['job_type'] == 'measurement':
                    measurement_name = job['measurement']
                    logger.info(f"Exporting measurement: {measurement_name}")
                    
                    # Create temp directory for export
                    with tempfile.TemporaryDirectory() as temp_dir:
                        temp_path = Path(temp_dir)
                        
                        # Get last export time for incremental export
                        last_export_time = self._get_last_export_time(job['id'], job['measurement'])
                        # Apply overlap buffer to reduce boundary gaps
                        overlap_td = self._parse_duration(job.get('overlap_buffer', '5m'))

                        if job.get('initial_export_mode') == 'retention_policy':
                            # Measurement-level retention policy export (mirror database path)
                            today = datetime.utcnow()
                            retention_days = job.get('retention_days', 365)
                            buffer_days = job.get('export_buffer_days', 7)
                            retention_cutoff = today - timedelta(days=retention_days)
                            end_time = retention_cutoff - timedelta(days=buffer_days)

                            if last_export_time:
                                # Start from last export minus overlap
                                start_time = max(last_export_time - overlap_td, today - timedelta(days=retention_days * 2))
                            elif influx_conn:
                                # First run: from configured start or 2x retention window
                                if job.get('initial_start_date'):
                                    start_time = datetime.fromisoformat(job['initial_start_date'])
                                elif influx_conn:
                                    start_time = today - timedelta(days=retention_days * 2)

                            logger.info(f"Retention policy export (measurement): {retention_days}d retention, {buffer_days}d buffer")
                            logger.info(f"Exporting from {start_time} to {end_time} (data older than {retention_cutoff})")
                        elif job.get('initial_export_mode') == 'from_date' and job.get('initial_start_date') and not last_export_time:
                            # Use configured start date for first run only
                            start_time = datetime.fromisoformat(job['initial_start_date'])
                            end_time = datetime.utcnow()
                            logger.info(f"Initial export from configured date {start_time} to {end_time} UTC")
                        elif last_export_time:
                            # Incremental export - only new data since last export (with overlap)
                            start_time = last_export_time - overlap_td
                            end_time = datetime.utcnow()
                            logger.info(f"Incremental export with overlap {overlap_td}: from {start_time} UTC to {end_time} UTC")
                        elif influx_conn:
                            # First export - get last 24 hours
                            end_time = datetime.utcnow()
                            start_time = end_time - timedelta(days=1)
                            logger.info(f"Initial export from {start_time} UTC to {end_time} UTC")
                        
                        # Skip if no new data to export
                        if start_time >= end_time:
                            logger.info(f"No new data to export for {job['measurement']}")
                            self._record_job_completion(execution_id, 'completed', 0)
                            self._update_last_export_time(job['id'], job['measurement'], end_time)
                            return True
                        
                        # Check for cancellation before single measurement export
                        if job_id in self.cancelled_jobs:
                            logger.info(f"Job {job['name']} cancelled before export")
                            self.cancelled_jobs.discard(job_id)
                            self.running_jobs.pop(job_id, None)
                            self._record_job_completion(execution_id, 'cancelled', 0)
                            self._update_job_status(execution_id, 'cancelled')
                            return False
                        
                        # Export based on source type
                        if source_type == 'http_json':
                            # HTTP JSON export
                            records_exported = await exporter.export_to_parquet(
                                measurement=job.get('measurement', source_conn.get('measurement_field', 'http_json')),
                                start_date=start_time,
                                end_date=end_time,
                                output_path=temp_path,
                                cancellation_check=lambda: job_id in self.cancelled_jobs
                            )
                        elif influx_conn and influx_conn['version'] == 'kafka':
                            # Kafka export - different logic, no chunking
                            await exporter.export_to_parquet(
                                measurement=job['measurement'],  # Topic name
                                start_date=start_time,
                                end_date=end_time,
                                output_path=temp_path,
                                cancellation_check=lambda: job_id in self.cancelled_jobs
                            )
                        elif influx_conn and influx_conn['version'] == 'timescale':
                            # TimescaleDB export
                            await exporter.export_to_parquet(
                                measurement=job['measurement'],
                                start_date=start_time,
                                end_date=end_time,
                                output_path=temp_path,
                                cancellation_check=lambda: job_id in self.cancelled_jobs
                            )
                        elif influx_conn:
                            # InfluxDB export - use chunking
                            chunk_duration = self._parse_duration(job.get('chunk_size', '1d'))
                            chunk_hours = int(chunk_duration.total_seconds() / 3600)
                            
                            if influx_conn['version'] == '2x':
                                await exporter.export_to_parquet(
                                    measurement=job['measurement'],
                                    start_date=start_time,
                                    end_date=end_time,
                                    output_path=temp_path,
                                    chunk_hours=chunk_hours,
                                    cancellation_check=lambda: job_id in self.cancelled_jobs,
                                    bucket=influx_conn.get('bucket')
                                )
                            elif influx_conn:
                                await exporter.export_to_parquet(
                                    measurement=job['measurement'],
                                    start_date=start_time,
                                    end_date=end_time,
                                    output_path=temp_path,
                                    chunk_hours=chunk_hours,
                                    database_name=influx_conn.get('database_name', 'historian')
                                )
                        
                        # Upload to storage backend using new flattened structure
                        uploaded_files = await storage_backend.upload_parquet_files(temp_path, job['measurement'])
                        logger.info(f"Uploaded {uploaded_files} files for {job['measurement']}")
                        
                        # Check if upload was successful
                        if uploaded_files == 0:
                            raise Exception(f"Failed to upload any files for {job['measurement']} - upload returned 0 files")
                        
                        # Count actual records from parquet files
                        records_exported = 0
                        import polars as pl
                        for parquet_file in temp_path.rglob("*.parquet"):
                            try:
                                df = pl.read_parquet(parquet_file)
                                records_exported += df.height
                            except Exception as e:
                                logger.warning(f"Could not count records in {parquet_file}: {e}")
                else:
                    logger.info(f"Full database export - discovering all measurements")
                    
                    # Get all measurements/topics based on source type
                    measurements = []
                    try:
                        if source_type == 'http_json':
                            # For HTTP JSON, use the configured measurement
                            measurements = [job.get('measurement', source_conn.get('measurement_field', 'http_json'))]
                            logger.info(f"HTTP JSON measurement: {measurements}")
                        elif influx_conn and influx_conn['version'] == 'timescale':
                            # For TimescaleDB, get table names that look like time-series tables
                            measurements = ['system_metrics', 'application_metrics', 'business_metrics']  # Default fallback
                            logger.info(f"TimescaleDB measurements (fallback): {measurements}")
                        elif influx_conn and influx_conn['version'] == '1x':
                            import requests
                            base_url = f"{'https' if influx_conn.get('ssl') else 'http'}://{influx_conn['host']}:{influx_conn['port']}"
                            query_url = f"{base_url}/query"
                            params = {
                                'q': 'SHOW MEASUREMENTS',
                                'u': influx_conn.get('username'),
                                'p': influx_conn.get('password'),
                                'db': influx_conn.get('database_name')
                            }
                            logger.info(f"Querying measurements from: {query_url} with db={influx_conn.get('database_name')}")
                            response = requests.get(query_url, params=params, timeout=10, verify=True if influx_conn.get('ssl') else False)
                            logger.info(f"Response status: {response.status_code}, content: {response.text[:200]}")
                            if response.status_code == 200:
                                result = response.json()
                                if 'results' in result and result['results']:
                                    series = result['results'][0].get('series', [])
                                    if series:
                                        measurements = [row[0] for row in series[0].get('values', [])]
                        elif influx_conn:
                            # For InfluxDB 2.x, use Flux to discover measurements
                            try:
                                from influxdb_client import InfluxDBClient
                                client = InfluxDBClient(
                                    url=f"http://{influx_conn['host']}:{influx_conn['port']}",
                                    token=influx_conn.get('token'),
                                    org=influx_conn.get('org')
                                )
                                query_api = client.query_api()
                                
                                flux_query = f'''
                                import "influxdata/influxdb/schema"
                                schema.measurements(bucket: "{influx_conn.get('bucket')}")
                                '''
                                
                                tables = query_api.query(flux_query)
                                measurements = []
                                for table in tables:
                                    for record in table.records:
                                        if record.get_value():
                                            measurements.append(record.get_value())
                                
                                client.close()
                                logger.info(f"Found {len(measurements)} measurements via Flux")
                            except Exception as flux_error:
                                logger.warning(f"Failed to discover measurements via Flux: {flux_error}")
                                measurements = ['cpu', 'mem', 'disk', 'system']  # Fallback
                        
                        logger.info(f"Found {len(measurements)} measurements to export")
                    except Exception as e:
                        logger.error(f"Failed to discover measurements: {e}")
                        measurements = []
                    
                    if not measurements:
                        logger.warning("No measurements found for full database export")
                        self._record_job_completion(execution_id, 'completed', 0)
                        return True
                    
                    # Export each measurement in parallel
                    total_records = 0
                    
                    # Get time range for all measurements
                    last_export_time = self._get_last_export_time(job['id'], 'full_database')
                    
                    if job.get('initial_export_mode') == 'retention_policy':
                        # Retention policy mode: export data older than retention period
                        today = datetime.utcnow()
                        retention_days = job.get('retention_days', 365)
                        buffer_days = job.get('export_buffer_days', 7)
                        
                        retention_cutoff = today - timedelta(days=retention_days)
                        end_time = retention_cutoff - timedelta(days=buffer_days)
                        
                        if last_export_time:
                            start_time = last_export_time
                        elif influx_conn:
                            # First run: start from beginning or configured date
                            if job.get('initial_start_date'):
                                start_time = datetime.fromisoformat(job['initial_start_date'])
                            elif influx_conn:
                                start_time = today - timedelta(days=retention_days * 2)  # Go back 2x retention
                        
                        logger.info(f"Retention policy export: {retention_days}d retention, {buffer_days}d buffer")
                        logger.info(f"Exporting from {start_time} to {end_time} (data older than {retention_cutoff})")
                        
                        # Skip if no data to export
                        if start_time >= end_time:
                            logger.info("No data older than retention policy to export")
                            self._record_job_completion(execution_id, 'completed', 0)
                            return True
                            
                    elif job.get('initial_export_mode') == 'from_date' and job.get('initial_start_date'):
                        # Use configured start date
                        start_time = datetime.fromisoformat(job['initial_start_date'])
                        end_time = datetime.utcnow()
                        logger.info(f"Full database export from configured date {start_time} to {end_time} UTC")
                    elif last_export_time:
                        # Apply overlap buffer in incremental full-database export
                        overlap_td = self._parse_duration(job.get('overlap_buffer', '5m'))
                        start_time = last_export_time - overlap_td
                        end_time = datetime.utcnow()
                        logger.info(f"Full database incremental export with overlap {overlap_td}: from {start_time} UTC to {end_time} UTC")
                    else:
                        end_time = datetime.utcnow()
                        start_time = end_time - timedelta(days=1)
                        logger.info(f"Full database initial export from {start_time} UTC to {end_time} UTC (default 24h)")
                    
                    # Create temp directory for export
                    with tempfile.TemporaryDirectory() as temp_dir:
                        temp_path = Path(temp_dir)
                        
                        # Use actual chunk_size from job config
                        chunk_duration = self._parse_duration(job.get('chunk_size', '1d'))
                        chunk_hours = int(chunk_duration.total_seconds() / 3600)
                        logger.info(f"Using chunk size: {chunk_hours} hours")
                        
                        # Export measurements in parallel (max 3 concurrent)
                        semaphore = asyncio.Semaphore(3)
                        
                        async def export_measurement(measurement):
                            async with semaphore:
                                # Check for cancellation
                                if job_id in self.cancelled_jobs:
                                    return {'measurement': measurement, 'success': False, 'records': 0, 'error': 'cancelled'}

                                try:
                                    # Set a timeout for individual measurement export (15 minutes)
                                    measurement_timeout = 900  # 15 minutes per measurement

                                    async def export_with_timeout():
                                        logger.info(f"Exporting measurement: {measurement} from {start_time} to {end_time}")
                                    
                                    # Export to parquet based on source type
                                    if source_type == 'http_json':
                                        # HTTP JSON export
                                        records_exported = await exporter.export_to_parquet(
                                            measurement=measurement,
                                            start_date=start_time,
                                            end_date=end_time,
                                            output_path=temp_path,
                                            cancellation_check=lambda: job_id in self.cancelled_jobs
                                        )
                                    elif influx_conn and influx_conn['version'] == 'timescale':
                                        # TimescaleDB export
                                        await exporter.export_to_parquet(
                                            measurement=measurement,
                                            start_date=start_time,
                                            end_date=end_time,
                                            output_path=temp_path,
                                            cancellation_check=lambda: job_id in self.cancelled_jobs
                                        )
                                    elif influx_conn and influx_conn['version'] == '2x':
                                        await exporter.export_to_parquet(
                                            measurement=measurement,
                                            start_date=start_time,
                                            end_date=end_time,
                                            output_path=temp_path,
                                            chunk_hours=chunk_hours,
                                            cancellation_check=lambda: job_id in self.cancelled_jobs,
                                            bucket=influx_conn.get('bucket')
                                        )
                                    elif influx_conn:
                                        await exporter.export_to_parquet(
                                            measurement=measurement,
                                            start_date=start_time,
                                            end_date=end_time,
                                            output_path=temp_path,
                                            chunk_hours=chunk_hours,
                                            cancellation_check=lambda: job_id in self.cancelled_jobs,
                                            database_name=influx_conn.get('database_name', 'historian')
                                        )
                                    
                                    # Count records
                                    measurement_records = 0
                                    import polars as pl
                                    for parquet_file in temp_path.rglob("*.parquet"):
                                        if measurement in str(parquet_file):
                                            try:
                                                df = pl.read_parquet(parquet_file)
                                                measurement_records += df.height
                                            except Exception as e:
                                                logger.warning(f"Could not count records in {parquet_file}: {e}")
                                    
                                    # Upload files
                                    uploaded_count = await storage_backend.upload_parquet_files(temp_path, measurement)
                                    logger.info(f"Uploaded {uploaded_count} files for {measurement}")
                                    
                                    if uploaded_count == 0:
                                        raise Exception(f"Failed to upload any files for {measurement}")
                                    
                                    return {'measurement': measurement, 'success': True, 'records': measurement_records, 'uploaded': uploaded_count}
                                    
                                except Exception as e:
                                    logger.error(f"Failed to export {measurement}: {e}")
                                    return {'measurement': measurement, 'success': False, 'records': 0, 'error': str(e)}
                        
                        # Run all measurements in parallel with real-time progress tracking
                        completed_count = 0
                        total_measurements = len(measurements)

                        # Create a shared counter for progress tracking
                        completed_measurements = []

                        async def export_measurement_with_progress(measurement):
                            result = await export_measurement(measurement)
                            # Update progress immediately when measurement completes
                            if isinstance(result, dict) and result.get('success'):
                                completed_measurements.append(measurement)
                                logger.info(f"✅ Measurement {measurement} completed ({len(completed_measurements)}/{total_measurements})")
                                # Run database update in thread pool to avoid blocking event loop
                                loop = asyncio.get_event_loop()
                                await loop.run_in_executor(None, self._update_job_execution_progress, execution_id, len(completed_measurements), total_measurements)
                            return result

                        tasks = [export_measurement_with_progress(measurement) for measurement in measurements]

                        # Update status periodically (less frequently since we have real-time updates)
                        async def track_progress():
                            while len(completed_measurements) < total_measurements:
                                await asyncio.sleep(60)  # Update every 60 seconds for status checks
                                current_count = len(completed_measurements)
                                logger.info(f"Export progress: {current_count}/{total_measurements} measurements completed")

                        progress_task = asyncio.create_task(track_progress())

                        try:
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                        finally:
                            progress_task.cancel()
                            try:
                                await progress_task
                            except asyncio.CancelledError:
                                pass

                        # Process final results
                        for result in results:
                            if isinstance(result, dict) and result.get('success'):
                                completed_count += 1
                            if isinstance(result, dict):
                                if result['success']:
                                    total_records += result['records']
                                    logger.info(f"✅ {result['measurement']}: {result['records']} records")
                                elif influx_conn:
                                    logger.error(f"❌ {result['measurement']}: {result.get('error', 'unknown error')}")
                            elif influx_conn:
                                logger.error(f"Unexpected result: {result}")
                        
                        # Check if job was cancelled during parallel execution
                        if job_id in self.cancelled_jobs:
                            logger.info(f"Job {job['name']} cancelled during parallel execution")
                            self.cancelled_jobs.discard(job_id)
                            self.running_jobs.pop(job_id, None)
                            self._record_job_completion(execution_id, 'cancelled', total_records)
                            self._update_job_status(execution_id, 'cancelled')
                            return False
                    
                    records_exported = total_records
                    
                    # Update last export time for full database
                    self._update_last_export_time(job['id'], 'full_database', end_time)
                
                # Record successful completion and update last export time
                self._record_job_completion(execution_id, 'completed', records_exported)
                measurement_key = job['measurement'] if job['job_type'] == 'measurement' else 'full_database'
                self._update_last_export_time(job['id'], measurement_key, end_time)
                logger.info(f"Job {job['name']} exported {records_exported} records")
                
                # Remove from running jobs and update database
                self.running_jobs.pop(job_id, None)
                self._update_job_status(execution_id, 'completed')
                return True
                
            except Exception as export_error:
                # Record failure
                self._record_job_completion(execution_id, 'failed', 0, str(export_error))
                logger.error(f"Job {job['name']} failed: {export_error}")
                
                # Remove from running jobs and update database
                self.running_jobs.pop(job_id, None)
                self._update_job_status(execution_id, 'failed')
                return False
                
        except Exception as e:
            logger.error(f"Failed to execute job {job['name']}: {e}")
            
            # Remove from running jobs and update database
            self.running_jobs.pop(job_id, None)
            return False
    
    def _record_job_start(self, job_id: int, measurement: str = None) -> int:
        """Record job execution start"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO job_executions 
                (job_id, measurement, start_time, status)
                VALUES (?, ?, CURRENT_TIMESTAMP, 'running')
            ''', (job_id, measurement))
            
            execution_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.info(f"Recorded job start: execution_id={execution_id}, job_id={job_id}, measurement={measurement}")
            return execution_id
            
        except Exception as e:
            logger.error(f"Failed to record job start: {e}")
            return 0
    
    def _record_job_completion(self, execution_id: int, status: str, records_exported: int, error_message: str = None):
        """Record job execution completion"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE job_executions SET
                end_time = CURRENT_TIMESTAMP,
                status = ?,
                records_exported = ?,
                error_message = ?,
                execution_duration_seconds = ROUND(
                    (julianday(CURRENT_TIMESTAMP) - julianday(start_time)) * 86400, 2
                )
                WHERE id = ?
            ''', (status, records_exported, error_message, execution_id))
            
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Recorded job completion: execution_id={execution_id}, status={status}, records={records_exported}, rows_affected={rows_affected}")
            
        except Exception as e:
            logger.error(f"Failed to record job completion: {e}")
    
    def _get_last_export_time(self, job_id: int, measurement: str) -> Optional[datetime]:
        """Get the last export time for a job/measurement"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT last_export_time FROM export_state 
                WHERE job_id = ? AND measurement = ?
            ''', (job_id, measurement))
            
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                return datetime.fromisoformat(result[0])
            return None
            
        except Exception as e:
            logger.error(f"Failed to get last export time: {e}")
            return None
    
    def _update_last_export_time(self, job_id: int, measurement: str, export_time: datetime):
        """Update the last export time for a job/measurement"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert or update export state
            cursor.execute('''
                INSERT OR REPLACE INTO export_state 
                (job_id, measurement, last_export_time, last_run_status, updated_at)
                VALUES (?, ?, ?, 'completed', CURRENT_TIMESTAMP)
            ''', (job_id, measurement, export_time.isoformat()))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated last export time for job {job_id}/{measurement}: {export_time}")
            
        except Exception as e:
            logger.error(f"Failed to update last export time: {e}")
    
    def _update_job_execution_progress(self, execution_id: int, completed: int, total: int):
        """Update job execution progress"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Store progress in error_message field temporarily
            progress_msg = f"Progress: {completed}/{total} measurements completed"

            cursor.execute('''
                UPDATE job_executions
                SET error_message = ?
                WHERE id = ?
            ''', (progress_msg, execution_id))

            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to update job execution progress: {e}")

    def _update_job_status(self, execution_id: int, status: str):
        """Update job execution status in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE job_executions SET
                status = ?
                WHERE id = ?
            ''', (status, execution_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to update job status: {e}")
