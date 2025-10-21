"""
Delete Operations API Routes

Provides endpoints for deleting data from Arc using tombstone-based logical deletes.
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Request, Header, Depends
from pydantic import BaseModel, Field
import hashlib

from config_loader import get_config

logger = logging.getLogger(__name__)
router = APIRouter()

# Import query_engine from main (will be initialized at startup)
_query_engine = None

def get_query_engine():
    """Get the global query engine instance"""
    global _query_engine
    if _query_engine is None:
        # Import here to avoid circular imports
        from api.main import query_engine
        _query_engine = query_engine
    return _query_engine

# Initialize config
config = get_config()
delete_config = config.get("delete")


class DeleteRequest(BaseModel):
    """Request model for delete operations"""
    database: str = Field(..., description="Target database name")
    measurement: str = Field(..., description="Target measurement (table) name")
    where: str = Field(..., description="SQL WHERE clause for filtering rows to delete")
    dry_run: bool = Field(default=False, description="If true, only count rows without deleting")
    confirm: bool = Field(default=False, description="Required confirmation for large deletes")


class DeleteResponse(BaseModel):
    """Response model for delete operations"""
    deleted_count: int
    execution_time_ms: float
    tombstone_file: Optional[str] = None
    dry_run: bool = False


class DryRunResponse(BaseModel):
    """Response model for dry-run delete operations"""
    would_delete_count: int
    affected_files: List[str]
    estimated_size_mb: float
    execution_time_ms: float


class DeleteAuditEntry(BaseModel):
    """Audit log entry for delete operation"""
    deleted_at: datetime
    deleted_by: str
    database: str
    measurement: str
    where_clause: str
    deleted_count: int
    tombstone_file: Optional[str]
    execution_time_ms: float


def check_delete_enabled():
    """Check if delete operations are enabled"""
    if not delete_config.get("enabled", False):
        raise HTTPException(
            status_code=403,
            detail="Delete operations are disabled. Set delete.enabled=true in arc.conf to enable."
        )


def validate_where_clause(where: str):
    """Validate WHERE clause to prevent accidental full table deletes"""
    if not where or where.strip() == "":
        raise HTTPException(
            status_code=400,
            detail="WHERE clause is required. To delete all data, use WHERE clause '1=1' with confirm=true"
        )

    # Check for dangerous patterns
    dangerous_patterns = ["WHERE 1=1", "WHERE true", "WHERE 1"]
    where_upper = where.upper().strip()

    for pattern in dangerous_patterns:
        if where_upper.startswith(pattern.upper()):
            # This is a full table delete - require confirmation
            return True

    return False


def get_row_hash(row: Dict[str, Any]) -> str:
    """
    Generate a hash for a row to uniquely identify it.
    Uses time + measurement + all tag values.
    """
    # Create stable hash from row data
    hash_parts = [
        str(row.get('time', '')),
        str(row.get('measurement', ''))
    ]

    # Add all non-field columns (tags) sorted by key
    for key in sorted(row.keys()):
        if key not in ['time', 'measurement', 'fields']:
            hash_parts.append(f"{key}={row.get(key, '')}")

    hash_str = '|'.join(hash_parts)
    return hashlib.sha256(hash_str.encode()).hexdigest()


@router.post("/api/v1/delete", response_model=DeleteResponse)
async def delete_data(
    request: DeleteRequest,
    req: Request,
    x_api_key: Optional[str] = Header(None, alias="x-api-key")
):
    """
    Delete data from Arc database using logical deletes with tombstones.

    Requires delete.enabled=true in arc.conf and DELETE permission on token.

    Example:
        POST /api/v1/delete
        {
            "database": "telegraf",
            "measurement": "cpu",
            "where": "host = 'server01' AND time BETWEEN '2024-01-01' AND '2024-01-02'",
            "dry_run": false,
            "confirm": false
        }

    Response:
        {
            "deleted_count": 1500,
            "execution_time_ms": 45.2,
            "tombstone_file": ".deletes/delete_20240101_150000.parquet",
            "dry_run": false
        }
    """
    start_time = time.perf_counter()

    # Check if delete operations are enabled
    check_delete_enabled()

    # Get token from request state (set by auth middleware)
    token_data = getattr(req.state, 'token_data', None)
    if not token_data:
        raise HTTPException(status_code=401, detail="Authentication required")

    # Check for DELETE permission
    permissions = token_data.get('permissions', [])
    has_delete = 'delete' in permissions or 'admin' in permissions

    if not has_delete:
        raise HTTPException(
            status_code=403,
            detail="DELETE permission required. This token does not have delete permissions."
        )

    # Validate WHERE clause
    is_full_table_delete = validate_where_clause(request.where)

    # Check if confirmation is required for full table deletes
    if is_full_table_delete and not request.confirm:
        raise HTTPException(
            status_code=400,
            detail="Full table delete detected. Set confirm=true to proceed."
        )

    try:
        # Get DuckDB engine
        engine = get_query_engine()

        if not engine:
            raise HTTPException(
                status_code=503,
                detail="Query engine not initialized"
            )

        # Build query to count rows that would be deleted
        count_query = f"""
            SELECT COUNT(*) as count
            FROM {request.database}.{request.measurement}
            WHERE {request.where}
        """

        logger.info(f"Delete count query: {count_query}")

        # Execute count query
        count_result = await engine.execute_query(count_query)

        if not count_result.get('success'):
            raise HTTPException(
                status_code=500,
                detail=f"Count query failed: {count_result.get('error', 'Unknown error')}"
            )

        # Data is returned as list of lists, columns separately
        data = count_result.get('data', [])
        columns = count_result.get('columns', [])

        # Convert first row to dict using columns
        if data and columns:
            row_dict = dict(zip(columns, data[0]))
            row_count = row_dict.get('count', 0)
        else:
            row_count = 0

        # Check if exceeds max_rows_per_delete
        max_rows = delete_config.get("max_rows_per_delete", 1000000)
        if row_count > max_rows:
            raise HTTPException(
                status_code=400,
                detail=f"Delete would affect {row_count} rows, exceeding maximum of {max_rows}. "
                       f"Adjust delete.max_rows_per_delete in arc.conf or refine WHERE clause."
            )

        # Check if exceeds confirmation threshold
        threshold = delete_config.get("confirmation_threshold", 10000)
        if row_count > threshold and not request.confirm:
            raise HTTPException(
                status_code=400,
                detail=f"Delete would affect {row_count} rows (threshold: {threshold}). "
                       f"Set confirm=true to proceed."
            )

        # If dry_run, just return the count
        if request.dry_run:
            execution_time = (time.perf_counter() - start_time) * 1000
            return DeleteResponse(
                deleted_count=row_count,
                execution_time_ms=execution_time,
                dry_run=True
            )

        # Get rows to delete (we need to hash them for tombstone)
        select_query = f"""
            SELECT *
            FROM {request.database}.{request.measurement}
            WHERE {request.where}
        """

        select_result = await engine.execute_query(select_query)

        if not select_result.get('success'):
            raise HTTPException(
                status_code=500,
                detail=f"Select query failed: {select_result.get('error', 'Unknown error')}"
            )

        # Convert list of lists to list of dicts
        data_rows = select_result.get('data', [])
        data_columns = select_result.get('columns', [])

        rows_to_delete = []
        for row in data_rows:
            row_dict = dict(zip(data_columns, row))
            rows_to_delete.append(row_dict)

        if not rows_to_delete:
            execution_time = (time.perf_counter() - start_time) * 1000
            return DeleteResponse(
                deleted_count=0,
                execution_time_ms=execution_time,
                dry_run=False
            )

        # Generate tombstone entries
        tombstones = []
        deleted_at = datetime.utcnow()
        deleted_by = token_data.get('name', 'unknown')

        for row in rows_to_delete:
            row_hash = get_row_hash(row)
            tombstone = {
                'time': row.get('time'),
                '_hash': row_hash,
                '_deleted_at': deleted_at,
                '_deleted_by': deleted_by,
                'database': request.database,
                'measurement': request.measurement
            }
            tombstones.append(tombstone)

        # Write tombstone file
        tombstone_file = await write_tombstone_file(
            request.database,
            request.measurement,
            tombstones,
            engine
        )

        # Log audit entry if enabled
        if delete_config.get("audit_enabled", True):
            execution_time = (time.perf_counter() - start_time) * 1000
            await log_delete_audit(
                database=request.database,
                measurement=request.measurement,
                where_clause=request.where,
                deleted_by=deleted_by,
                deleted_count=len(tombstones),
                tombstone_file=tombstone_file,
                execution_time_ms=execution_time
            )

        execution_time = (time.perf_counter() - start_time) * 1000

        logger.info(f"Deleted {len(tombstones)} rows from {request.database}.{request.measurement} "
                   f"by {deleted_by} in {execution_time:.2f}ms")

        return DeleteResponse(
            deleted_count=len(tombstones),
            execution_time_ms=execution_time,
            tombstone_file=tombstone_file,
            dry_run=False
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete operation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


async def write_tombstone_file(
    database: str,
    measurement: str,
    tombstones: List[Dict[str, Any]],
    engine
) -> str:
    """
    Write tombstone entries to a Parquet file in the .deletes subdirectory.

    Returns the relative path to the tombstone file.
    """
    # Get storage backend
    storage = engine.local_backend or engine.minio_backend or engine.s3_backend
    if not storage:
        raise Exception("No storage backend available")

    # Create tombstone file path
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    tombstone_dir = f"{database}/{measurement}/.deletes"
    tombstone_filename = f"delete_{timestamp}.parquet"
    tombstone_path = f"{tombstone_dir}/{tombstone_filename}"

    # Convert tombstones to Arrow table and write as Parquet
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    # Create Arrow table from tombstones
    # Convert datetime objects to timestamps
    for tomb in tombstones:
        if isinstance(tomb.get('_deleted_at'), datetime):
            tomb['_deleted_at'] = int(tomb['_deleted_at'].timestamp() * 1_000_000)  # microseconds
        if isinstance(tomb.get('time'), datetime):
            tomb['time'] = int(tomb['time'].timestamp() * 1_000_000)

    table = pa.Table.from_pylist(tombstones)

    # Write to temporary file
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp_file:
        temp_path = tmp_file.name
        pq.write_table(table, tmp_file, compression='snappy')

    try:
        # Upload to storage using the storage backend's upload_file method
        from pathlib import Path
        # Use full path with database (upload_file expects full relative path)
        relative_path = f"{measurement}/.deletes/{tombstone_filename}"
        success = await storage.upload_file(Path(temp_path), relative_path, database_override=database)

        if not success:
            raise Exception("Failed to upload tombstone file")

        logger.info(f"Wrote {len(tombstones)} tombstones to {tombstone_path}")

        return tombstone_path

    finally:
        # Clean up temp file
        try:
            os.unlink(temp_path)
        except Exception as e:
            logger.warning(f"Failed to delete temp file {temp_path}: {e}")


async def log_delete_audit(
    database: str,
    measurement: str,
    where_clause: str,
    deleted_by: str,
    deleted_count: int,
    tombstone_file: str,
    execution_time_ms: float
):
    """
    Log delete operation to audit log.

    For now, just logs to application log.
    Future: Store in dedicated audit table or file.
    """
    audit_entry = {
        'timestamp': datetime.utcnow().isoformat(),
        'database': database,
        'measurement': measurement,
        'where_clause': where_clause,
        'deleted_by': deleted_by,
        'deleted_count': deleted_count,
        'tombstone_file': tombstone_file,
        'execution_time_ms': execution_time_ms
    }

    logger.info(f"DELETE AUDIT: {audit_entry}")

    # TODO: Write to dedicated audit storage
    # This could be a separate Parquet file or SQLite database


@router.get("/api/v1/delete/audit")
async def get_delete_audit(
    req: Request,
    database: Optional[str] = None,
    measurement: Optional[str] = None,
    limit: int = 100,
    x_api_key: Optional[str] = Header(None, alias="x-api-key")
):
    """
    Get delete operation audit log.

    Returns list of recent delete operations with metadata.

    Query Parameters:
        database: Filter by database name
        measurement: Filter by measurement name
        limit: Maximum number of entries to return (default: 100)
    """
    # Check authentication
    token_data = getattr(req.state, 'token_data', None)
    if not token_data:
        raise HTTPException(status_code=401, detail="Authentication required")

    # TODO: Implement audit log retrieval from storage
    # For now, return empty list

    return {
        "audit_entries": [],
        "message": "Audit log retrieval not yet implemented. Check application logs for delete operations."
    }


@router.get("/api/v1/delete/config")
async def get_delete_config(
    req: Request,
    x_api_key: Optional[str] = Header(None, alias="x-api-key")
):
    """
    Get current delete configuration settings.

    Useful for clients to check limits and thresholds before attempting deletes.
    """
    # Check authentication
    token_data = getattr(req.state, 'token_data', None)
    if not token_data:
        raise HTTPException(status_code=401, detail="Authentication required")

    return {
        "enabled": delete_config.get("enabled", False),
        "confirmation_threshold": delete_config.get("confirmation_threshold", 10000),
        "max_rows_per_delete": delete_config.get("max_rows_per_delete", 1000000),
        "tombstone_retention_days": delete_config.get("tombstone_retention_days", 30),
        "audit_enabled": delete_config.get("audit_enabled", True)
    }
