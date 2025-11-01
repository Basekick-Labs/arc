"""
Retention Policy API Routes

Provides endpoints for managing and manually triggering retention policies.
Automatic execution is reserved for enterprise edition.
"""

import logging
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Request, Header, Depends
from pydantic import BaseModel, Field

from config_loader import get_config
from api.config import get_db_path
from api.auth import AuthManager

logger = logging.getLogger(__name__)
router = APIRouter()

# Import query_engine from main (will be initialized at startup)
_query_engine = None
_auth_manager = None

def get_query_engine():
    """Get the global query engine instance"""
    global _query_engine
    if _query_engine is None:
        # Import here to avoid circular imports
        from api.main import query_engine
        _query_engine = query_engine
    return _query_engine

def get_auth_manager():
    """Get the global auth manager instance"""
    global _auth_manager
    if _auth_manager is None:
        # Import here to avoid circular imports
        from api.main import auth_manager
        _auth_manager = auth_manager
    return _auth_manager

# Initialize config
config = get_config()


class RetentionPolicyRequest(BaseModel):
    """Request model for creating/updating retention policies"""
    name: str = Field(..., description="Unique name for the retention policy")
    database: str = Field(..., description="Target database name")
    measurement: Optional[str] = Field(None, description="Target measurement (null for database-wide)")
    retention_days: int = Field(..., gt=0, description="Number of days to retain data")
    buffer_days: int = Field(default=7, ge=0, description="Safety buffer days before deletion")
    is_active: bool = Field(default=True, description="Whether policy is active")


class RetentionPolicyResponse(BaseModel):
    """Response model for retention policies"""
    id: int
    name: str
    database: str
    measurement: Optional[str]
    retention_days: int
    buffer_days: int
    is_active: bool
    last_execution_time: Optional[str]
    last_execution_status: Optional[str]
    last_deleted_count: Optional[int]
    created_at: str
    updated_at: str


class ExecuteRetentionRequest(BaseModel):
    """Request model for executing a retention policy"""
    dry_run: bool = Field(default=False, description="If true, only count rows without deleting")
    confirm: bool = Field(default=False, description="Required confirmation for execution")


class ExecuteRetentionResponse(BaseModel):
    """Response model for retention policy execution"""
    policy_id: int
    policy_name: str
    deleted_count: int
    execution_time_ms: float
    dry_run: bool
    cutoff_date: str
    affected_measurements: List[str]


class RetentionPolicyManager:
    """Manager for retention policy database operations"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_db_path()
        self._init_tables()

    def _init_tables(self):
        """Initialize retention policy tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Retention policies table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS retention_policies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        database TEXT NOT NULL,
                        measurement TEXT,
                        retention_days INTEGER NOT NULL,
                        buffer_days INTEGER DEFAULT 7,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Retention policy execution history
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS retention_executions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        policy_id INTEGER NOT NULL,
                        execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT NOT NULL,
                        deleted_count INTEGER DEFAULT 0,
                        cutoff_date TIMESTAMP,
                        execution_duration_ms FLOAT,
                        error_message TEXT,
                        FOREIGN KEY (policy_id) REFERENCES retention_policies (id)
                    )
                ''')

                conn.commit()
                logger.info("Retention policy tables initialized")

        except Exception as e:
            logger.error(f"Failed to initialize retention tables: {e}")
            raise

    def create_policy(self, policy: RetentionPolicyRequest) -> int:
        """Create new retention policy"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    INSERT INTO retention_policies
                    (name, database, measurement, retention_days, buffer_days, is_active)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    policy.name,
                    policy.database,
                    policy.measurement,
                    policy.retention_days,
                    policy.buffer_days,
                    policy.is_active
                ))

                policy_id = cursor.lastrowid
                conn.commit()

                logger.info(f"Created retention policy: {policy.name} (ID: {policy_id})")
                return policy_id

        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint" in str(e):
                raise ValueError(f"Retention policy with name '{policy.name}' already exists")
            raise
        except Exception as e:
            logger.error(f"Failed to create retention policy: {e}")
            raise

    def get_policies(self) -> List[Dict]:
        """Get all retention policies"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT
                        rp.*,
                        re.execution_time as last_execution_time,
                        re.status as last_execution_status,
                        re.deleted_count as last_deleted_count
                    FROM retention_policies rp
                    LEFT JOIN (
                        SELECT policy_id, execution_time, status, deleted_count
                        FROM retention_executions
                        WHERE id IN (
                            SELECT MAX(id)
                            FROM retention_executions
                            GROUP BY policy_id
                        )
                    ) re ON rp.id = re.policy_id
                    ORDER BY rp.created_at DESC
                ''')

                policies = [dict(row) for row in cursor.fetchall()]
                return policies

        except Exception as e:
            logger.error(f"Failed to get retention policies: {e}")
            return []

    def get_policy(self, policy_id: int) -> Optional[Dict]:
        """Get single retention policy by ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT
                        rp.*,
                        re.execution_time as last_execution_time,
                        re.status as last_execution_status,
                        re.deleted_count as last_deleted_count
                    FROM retention_policies rp
                    LEFT JOIN (
                        SELECT policy_id, execution_time, status, deleted_count
                        FROM retention_executions
                        WHERE id IN (
                            SELECT MAX(id)
                            FROM retention_executions
                            GROUP BY policy_id
                        )
                    ) re ON rp.id = re.policy_id
                    WHERE rp.id = ?
                ''', (policy_id,))

                row = cursor.fetchone()

                if row:
                    return dict(row)
                return None

        except Exception as e:
            logger.error(f"Failed to get retention policy {policy_id}: {e}")
            return None

    def update_policy(self, policy_id: int, policy: RetentionPolicyRequest) -> bool:
        """Update existing retention policy"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    UPDATE retention_policies SET
                    name = ?, database = ?, measurement = ?, retention_days = ?,
                    buffer_days = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    policy.name,
                    policy.database,
                    policy.measurement,
                    policy.retention_days,
                    policy.buffer_days,
                    policy.is_active,
                    policy_id
                ))

                rows_affected = cursor.rowcount
                conn.commit()

                if rows_affected == 0:
                    return False

                logger.info(f"Updated retention policy {policy_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to update retention policy: {e}")
            return False

    def delete_policy(self, policy_id: int) -> bool:
        """Delete retention policy"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Delete execution history first
                cursor.execute('DELETE FROM retention_executions WHERE policy_id = ?', (policy_id,))

                # Delete policy
                cursor.execute('DELETE FROM retention_policies WHERE id = ?', (policy_id,))

                rows_affected = cursor.rowcount
                conn.commit()

                if rows_affected == 0:
                    return False

                logger.info(f"Deleted retention policy {policy_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to delete retention policy: {e}")
            return False

    def get_executions(self, policy_id: int, limit: int = 50) -> List[Dict]:
        """Get execution history for a policy"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT * FROM retention_executions
                    WHERE policy_id = ?
                    ORDER BY execution_time DESC
                    LIMIT ?
                ''', (policy_id, limit))

                executions = [dict(row) for row in cursor.fetchall()]
                return executions

        except Exception as e:
            logger.error(f"Failed to get executions for policy {policy_id}: {e}")
            return []

    def record_execution_start(self, policy_id: int, cutoff_date: datetime) -> int:
        """Record retention execution start"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    INSERT INTO retention_executions
                    (policy_id, execution_time, status, cutoff_date)
                    VALUES (?, CURRENT_TIMESTAMP, 'running', ?)
                ''', (policy_id, cutoff_date.isoformat()))

                execution_id = cursor.lastrowid
                conn.commit()

                return execution_id

        except Exception as e:
            logger.error(f"Failed to record execution start: {e}")
            return 0

    def record_execution_complete(
        self,
        execution_id: int,
        status: str,
        deleted_count: int,
        execution_duration_ms: float,
        error_message: str = None
    ):
        """Record retention execution completion"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    UPDATE retention_executions SET
                    status = ?,
                    deleted_count = ?,
                    execution_duration_ms = ?,
                    error_message = ?
                    WHERE id = ?
                ''', (status, deleted_count, execution_duration_ms, error_message, execution_id))

                conn.commit()

        except Exception as e:
            logger.error(f"Failed to record execution complete: {e}")


# Global manager instance
retention_manager = RetentionPolicyManager()


async def _delete_old_parquet_files(
    engine,
    database: str,
    measurement: str,
    cutoff_date: datetime
) -> int:
    """
    Delete Parquet files where ALL rows are older than cutoff_date.

    Returns the number of rows deleted.
    """
    import os
    from pathlib import Path
    import pyarrow.parquet as pq

    # Get storage backend from engine
    storage = engine.local_backend or engine.minio_backend or engine.s3_backend or engine.gcs_backend or engine.ceph_backend
    if not storage:
        raise Exception("No storage backend available")

    deleted_count = 0

    # For local storage, we can directly access files
    if hasattr(storage, 'base_path'):
        # Local filesystem storage
        base_path = Path(storage.base_path) / database / measurement

        if not base_path.exists():
            logger.warning(f"No data directory found for {database}/{measurement}")
            return 0

        # Find all Parquet files
        parquet_files = list(base_path.rglob("*.parquet"))
        logger.info(f"Found {len(parquet_files)} parquet files in {base_path}")

        files_to_delete = []

        for parquet_file in parquet_files:
            try:
                # Read Parquet file metadata to check min/max time
                parquet_meta = pq.read_metadata(parquet_file)

                # Read the actual file to check time column
                table = pq.read_table(parquet_file)

                if 'time' not in table.column_names:
                    logger.warning(f"Skipping {parquet_file} - no 'time' column")
                    continue

                # Get time column
                time_col = table.column('time')

                # Convert to datetime (Arc stores time as microseconds since epoch)
                import pyarrow.compute as pc

                # Get max time in file
                max_time_micros = pc.max(time_col).as_py()

                if max_time_micros is None:
                    continue

                # Convert microseconds to datetime
                max_time = datetime.fromtimestamp(max_time_micros / 1_000_000)

                # If ALL rows in this file are older than cutoff, mark for deletion
                if max_time < cutoff_date:
                    file_row_count = table.num_rows
                    files_to_delete.append((parquet_file, file_row_count))
                    logger.info(f"Marking for deletion: {parquet_file.name} (max_time: {max_time}, rows: {file_row_count})")

            except Exception as e:
                logger.error(f"Error processing {parquet_file}: {e}")
                continue

        # Delete marked files
        for parquet_file, row_count in files_to_delete:
            try:
                logger.info(f"Deleting {parquet_file}")
                os.unlink(parquet_file)
                deleted_count += row_count
            except Exception as e:
                logger.error(f"Failed to delete {parquet_file}: {e}")

        logger.info(f"Deleted {len(files_to_delete)} files, {deleted_count} total rows")

    else:
        # Cloud storage (S3, MinIO, GCS, Ceph)
        # TODO: Implement cloud storage deletion
        raise NotImplementedError(
            "Retention policies for cloud storage (S3/MinIO/GCS/Ceph) not yet implemented. "
            "Currently only local storage is supported."
        )

    return deleted_count


@router.post("/api/v1/retention", response_model=RetentionPolicyResponse)
async def create_retention_policy(request: RetentionPolicyRequest):
    """
    Create a new retention policy.

    Retention policies define how long data should be kept. Manual execution only.

    Example:
        POST /api/v1/retention
        {
            "name": "telegraf_90day_retention",
            "database": "telegraf",
            "measurement": "cpu",
            "retention_days": 90,
            "buffer_days": 7,
            "is_active": true
        }
    """
    # Validate retention days > buffer days
    if request.retention_days <= request.buffer_days:
        raise HTTPException(
            status_code=400,
            detail="retention_days must be greater than buffer_days"
        )

    try:
        policy_id = retention_manager.create_policy(request)
        policy = retention_manager.get_policy(policy_id)

        if not policy:
            raise HTTPException(status_code=500, detail="Failed to retrieve created policy")

        return RetentionPolicyResponse(**policy)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create retention policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/retention", response_model=List[RetentionPolicyResponse])
async def list_retention_policies():
    """
    List all retention policies.

    Returns all configured retention policies with their last execution status.
    """
    policies = retention_manager.get_policies()
    return [RetentionPolicyResponse(**policy) for policy in policies]


@router.get("/api/v1/retention/{policy_id}", response_model=RetentionPolicyResponse)
async def get_retention_policy(policy_id: int):
    """
    Get a specific retention policy by ID.
    """
    policy = retention_manager.get_policy(policy_id)

    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")

    return RetentionPolicyResponse(**policy)


@router.put("/api/v1/retention/{policy_id}", response_model=RetentionPolicyResponse)
async def update_retention_policy(
    policy_id: int,
    request: RetentionPolicyRequest
):
    """
    Update an existing retention policy.
    """
    # Validate retention days > buffer days
    if request.retention_days <= request.buffer_days:
        raise HTTPException(
            status_code=400,
            detail="retention_days must be greater than buffer_days"
        )

    success = retention_manager.update_policy(policy_id, request)

    if not success:
        raise HTTPException(status_code=404, detail="Retention policy not found")

    policy = retention_manager.get_policy(policy_id)
    return RetentionPolicyResponse(**policy)


@router.delete("/api/v1/retention/{policy_id}")
async def delete_retention_policy(policy_id: int):
    """
    Delete a retention policy.
    """
    success = retention_manager.delete_policy(policy_id)

    if not success:
        raise HTTPException(status_code=404, detail="Retention policy not found")

    return {"message": "Retention policy deleted successfully"}


@router.post("/api/v1/retention/{policy_id}/execute", response_model=ExecuteRetentionResponse)
async def execute_retention_policy(
    policy_id: int,
    request: ExecuteRetentionRequest
):
    """
    Manually execute a retention policy.

    This will delete data older than (retention_days - buffer_days) from the current date.

    Example:
        POST /api/v1/retention/1/execute
        {
            "dry_run": false,
            "confirm": true
        }
    """
    start_time = time.perf_counter()

    # Get policy
    policy = retention_manager.get_policy(policy_id)

    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")

    if not policy['is_active']:
        raise HTTPException(status_code=400, detail="Retention policy is not active")

    # Require confirmation for non-dry-run
    if not request.dry_run and not request.confirm:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required for retention policy execution. Set confirm=true"
        )

    try:
        # Calculate cutoff date
        today = datetime.utcnow()
        retention_days = policy['retention_days']
        buffer_days = policy['buffer_days']
        cutoff_date = today - timedelta(days=retention_days + buffer_days)

        logger.info(f"Executing retention policy '{policy['name']}' - deleting data older than {cutoff_date}")

        # Get query engine
        engine = get_query_engine()
        if not engine:
            raise HTTPException(status_code=503, detail="Query engine not initialized")

        # Get measurements to process
        measurements_to_process = []

        if policy['measurement']:
            # Single measurement policy
            measurements_to_process = [policy['measurement']]
        else:
            # Database-wide policy - discover all measurements
            discover_query = f"SHOW TABLES FROM {policy['database']}"
            result = await engine.execute_query(discover_query)

            if result.get('success'):
                data = result.get('data', [])
                columns = result.get('columns', [])

                for row in data:
                    row_dict = dict(zip(columns, row))
                    table_name = row_dict.get('name') or row_dict.get('table_name')
                    if table_name:
                        measurements_to_process.append(table_name)

        logger.info(f"Processing {len(measurements_to_process)} measurements: {measurements_to_process}")

        # Execute deletion for each measurement
        total_deleted = 0
        execution_id = 0

        if not request.dry_run:
            execution_id = retention_manager.record_execution_start(policy_id, cutoff_date)

        for measurement in measurements_to_process:
            where_clause = f"time < '{cutoff_date.isoformat()}'"

            # Count rows that would be deleted
            count_query = f"""
                SELECT COUNT(*) as count
                FROM {policy['database']}.{measurement}
                WHERE {where_clause}
            """

            count_result = await engine.execute_query(count_query)

            if count_result.get('success'):
                data = count_result.get('data', [])
                columns = count_result.get('columns', [])

                if data and columns:
                    row_dict = dict(zip(columns, data[0]))
                    row_count = row_dict.get('count', 0)

                    if row_count > 0:
                        logger.info(f"Measurement {measurement}: {row_count} rows to delete")

                        if not request.dry_run:
                            # Physical deletion: Delete Parquet files older than cutoff date
                            # Arc stores data in partitioned Parquet files by time
                            # We need to identify and delete files where ALL rows are older than cutoff

                            try:
                                deleted_rows = await _delete_old_parquet_files(
                                    engine,
                                    policy['database'],
                                    measurement,
                                    cutoff_date
                                )
                                total_deleted += deleted_rows
                                logger.info(f"Deleted {deleted_rows} rows from {measurement}")
                            except Exception as e:
                                logger.error(f"Failed to delete from {measurement}: {e}")
                        else:
                            total_deleted += row_count

        execution_time = (time.perf_counter() - start_time) * 1000

        # Record execution completion
        if not request.dry_run and execution_id:
            retention_manager.record_execution_complete(
                execution_id,
                'completed',
                total_deleted,
                execution_time
            )

        logger.info(f"Retention policy '{policy['name']}' completed: deleted {total_deleted} rows in {execution_time:.2f}ms")

        return ExecuteRetentionResponse(
            policy_id=policy_id,
            policy_name=policy['name'],
            deleted_count=total_deleted,
            execution_time_ms=execution_time,
            dry_run=request.dry_run,
            cutoff_date=cutoff_date.isoformat(),
            affected_measurements=measurements_to_process
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Retention policy execution failed: {e}")

        if execution_id:
            execution_time = (time.perf_counter() - start_time) * 1000
            retention_manager.record_execution_complete(
                execution_id,
                'failed',
                0,
                execution_time,
                str(e)
            )

        raise HTTPException(status_code=500, detail=f"Retention execution failed: {str(e)}")


@router.get("/api/v1/retention/{policy_id}/executions")
async def get_retention_executions(policy_id: int, limit: int = 50):
    """
    Get execution history for a retention policy.

    Query Parameters:
        limit: Maximum number of executions to return (default: 50)
    """
    executions = retention_manager.get_executions(policy_id, limit)

    return {
        "policy_id": policy_id,
        "executions": executions
    }
