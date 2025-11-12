"""
Delete Operations API Routes - Rewrite-Based Implementation

Provides endpoints for deleting data from Arc using file rewrite strategy.
This approach rewrites Parquet files without deleted rows instead of using tombstones.

Performance characteristics:
- Write operations: Zero overhead (no hashing)
- Query operations: Zero overhead (no tombstone filtering)
- Delete operations: Expensive (file rewrites) but acceptable for rare operations

Architecture:
- Identify Parquet files containing matching rows
- Read affected files using Arrow
- Filter out rows matching WHERE clause
- Write new files atomically
- Delete old files
"""

import logging
import time
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from config_loader import get_config

logger = logging.getLogger(__name__)
router = APIRouter()

# Import query_engine from main (will be initialized at startup)
_query_engine = None

def get_query_engine():
    """Get the global query engine instance"""
    global _query_engine
    if _query_engine is None:
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
    confirm: bool = Field(default=False, description="Required confirmation for execution")


class DeleteResponse(BaseModel):
    """Response model for delete operations"""
    deleted_count: int
    affected_files: int
    rewritten_files: int
    execution_time_ms: float
    dry_run: bool = False
    files_processed: List[str] = []


class DeleteStats(BaseModel):
    """Statistics for delete operation"""
    total_files_scanned: int
    affected_files: int
    total_rows_before: int
    total_rows_after: int
    deleted_count: int
    estimated_size_before_mb: float
    estimated_size_after_mb: float


def check_delete_enabled():
    """Check if delete operations are enabled"""
    if not delete_config.get("enabled", False):
        raise HTTPException(
            status_code=403,
            detail="Delete operations are disabled. Set delete.enabled=true in arc.conf to enable."
        )


def validate_where_clause(where: str) -> bool:
    """
    Validate WHERE clause to prevent accidental full table deletes.
    Returns True if this is a dangerous full-table delete.
    """
    if not where or where.strip() == "":
        raise HTTPException(
            status_code=400,
            detail="WHERE clause is required. To delete all data, use WHERE clause '1=1' with confirm=true"
        )

    # Check for dangerous patterns
    dangerous_patterns = ["1=1", "true", "1"]
    where_upper = where.upper().strip()

    # Remove "WHERE" prefix if present
    if where_upper.startswith("WHERE "):
        where_upper = where_upper[6:].strip()

    for pattern in dangerous_patterns:
        if where_upper == pattern.upper():
            return True  # Dangerous full table delete

    return False


async def find_affected_files(
    engine,
    database: str,
    measurement: str,
    where_clause: str
) -> List[Tuple[Path, int]]:
    """
    Find Parquet files that contain rows matching the WHERE clause.

    Returns list of (file_path, matching_row_count) tuples.
    """
    # Get storage backend
    storage = engine.local_backend or engine.minio_backend or engine.s3_backend or engine.gcs_backend or engine.ceph_backend
    if not storage:
        raise Exception("No storage backend available")

    affected_files = []

    # For local storage
    if hasattr(storage, 'base_path'):
        base_path = Path(storage.base_path) / database / measurement

        if not base_path.exists():
            logger.warning(f"No data directory found for {database}/{measurement}")
            return []

        # Find all Parquet files
        parquet_files = list(base_path.rglob("*.parquet"))
        logger.info(f"Scanning {len(parquet_files)} parquet files in {base_path}")

        for parquet_file in parquet_files:
            try:
                # Read Parquet file
                table = pq.read_table(parquet_file)

                # Evaluate WHERE clause using DuckDB (most reliable)
                # We'll use DuckDB to filter the table
                matching_count = await count_matching_rows(table, where_clause, engine)

                # Explicitly free table memory
                del table

                if matching_count > 0:
                    affected_files.append((parquet_file, matching_count))
                    logger.info(f"File {parquet_file.name} has {matching_count} matching rows")

            except Exception as e:
                logger.error(f"Error processing {parquet_file}: {e}")
                continue

    else:
        # Cloud storage
        raise NotImplementedError(
            "DELETE operations for cloud storage (S3/MinIO/GCS/Ceph) not yet implemented. "
            "Currently only local storage is supported."
        )

    return affected_files


def validate_where_clause(where_clause: str) -> None:
    """
    Validate WHERE clause for SQL injection by using DuckDB's parser.
    This prevents SQL injection by ensuring the WHERE clause is valid SQL
    without dangerous operations.

    Args:
        where_clause: The WHERE clause to validate

    Raises:
        HTTPException: If the WHERE clause is invalid or dangerous
    """
    import duckdb

    if not where_clause or not where_clause.strip():
        raise HTTPException(status_code=400, detail="WHERE clause cannot be empty")

    # Check for obvious SQL injection patterns
    dangerous_keywords = [
        ';',  # Statement terminator
        '--',  # SQL comment
        '/*',  # Multi-line comment
        'DROP', 'DELETE', 'INSERT', 'UPDATE',  # DML/DDL (not allowed in WHERE)
        'EXEC', 'EXECUTE',  # Command execution
        'xp_', 'sp_',  # SQL Server stored procedures
    ]

    where_upper = where_clause.upper()
    for keyword in dangerous_keywords:
        if keyword in where_upper:
            raise HTTPException(
                status_code=400,
                detail=f"WHERE clause contains forbidden keyword: {keyword}"
            )

    # Use DuckDB's parser to validate the WHERE clause syntax
    # This ensures it's valid SQL before we use it
    try:
        conn = duckdb.connect(':memory:')
        # Try to parse the WHERE clause in a safe context
        conn.execute(f"EXPLAIN SELECT 1 WHERE {where_clause}")
        conn.close()
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid WHERE clause syntax: {str(e)}"
        )


async def count_matching_rows(table: pa.Table, where_clause: str, engine) -> int:
    """
    Count rows in Arrow table that match WHERE clause.
    Uses DuckDB for reliable SQL evaluation.
    """
    import duckdb
    import gc

    # Validate WHERE clause to prevent SQL injection
    validate_where_clause(where_clause)

    # Create temp DuckDB connection
    conn = duckdb.connect(':memory:')

    try:
        # Register Arrow table
        conn.register('temp_table', table)

        # Execute count query (where_clause is validated above)
        query = f"SELECT COUNT(*) as count FROM temp_table WHERE {where_clause}"
        result = conn.execute(query).fetchone()

        count = result[0] if result else 0

        # Unregister table to free memory
        conn.unregister('temp_table')

        return count

    except Exception as e:
        logger.error(f"Error counting matching rows: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid WHERE clause: {str(e)}"
        )
    finally:
        try:
            conn.close()
        except:
            pass
        # Force garbage collection to free Arrow memory
        gc.collect()


async def rewrite_file_without_deleted_rows(
    file_path: Path,
    where_clause: str,
    engine
) -> Tuple[int, int, Path]:
    """
    Rewrite a Parquet file, excluding rows that match the WHERE clause.

    Returns (rows_before, rows_after, new_file_path).
    """
    import duckdb
    import gc

    # Validate WHERE clause to prevent SQL injection
    validate_where_clause(where_clause)

    # Read original file
    table = pq.read_table(file_path)
    rows_before = table.num_rows

    # Create temp DuckDB connection
    conn = duckdb.connect(':memory:')
    temp_file = None

    try:
        # Register Arrow table
        conn.register('temp_table', table)

        # Query to get rows that DON'T match (inverse of WHERE clause)
        # where_clause is validated above
        query = f"SELECT * FROM temp_table WHERE NOT ({where_clause})"
        result = conn.execute(query).fetch_arrow_table()

        rows_after = result.num_rows

        # Unregister table to free memory before writing
        conn.unregister('temp_table')
        del table  # Explicitly delete original table
        gc.collect()  # Force GC before allocating more memory for write

        # Write to temporary file
        temp_dir = file_path.parent / '.tmp'
        temp_dir.mkdir(exist_ok=True)

        temp_file = temp_dir / f"{file_path.stem}_new{file_path.suffix}"

        # Write filtered table to temp file
        pq.write_table(
            result,
            temp_file,
            compression='snappy'
        )

        # Explicitly delete result table and force GC
        del result
        gc.collect()

        logger.info(f"Rewrote {file_path.name}: {rows_before} -> {rows_after} rows")

        return rows_before, rows_after, temp_file

    except Exception as e:
        logger.error(f"Error rewriting file {file_path}: {e}")
        # Clean up temp file if created
        if temp_file and temp_file.exists():
            try:
                os.unlink(temp_file)
            except:
                pass
        raise
    finally:
        try:
            conn.close()
        except:
            pass
        # Force garbage collection to free Arrow/DuckDB memory
        gc.collect()


@router.post("/api/v1/delete", response_model=DeleteResponse)
async def delete_data(request: DeleteRequest, req: Request):
    """
    Delete data from Arc database using file rewrite strategy.

    This operation rewrites Parquet files without the deleted rows.
    It's expensive but preserves zero-overhead writes/queries.

    Requires delete.enabled=true in arc.conf.

    Example:
        POST /api/v1/delete
        {
            "database": "telegraf",
            "measurement": "cpu",
            "where": "host = 'server01' AND time BETWEEN '2024-01-01' AND '2024-01-02'",
            "dry_run": false,
            "confirm": true
        }

    Response:
        {
            "deleted_count": 1500,
            "affected_files": 5,
            "rewritten_files": 5,
            "execution_time_ms": 5432.12,
            "dry_run": false,
            "files_processed": ["cpu_20240101.parquet", ...]
        }
    """
    start_time = time.perf_counter()

    # Check if delete operations are enabled
    check_delete_enabled()

    # Validate WHERE clause
    is_full_table_delete = validate_where_clause(request.where)

    # Check if confirmation is required
    if is_full_table_delete and not request.confirm:
        raise HTTPException(
            status_code=400,
            detail="Full table delete detected (WHERE 1=1). Set confirm=true to proceed."
        )

    if not request.dry_run and not request.confirm:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required for delete operation. Set confirm=true or use dry_run=true to preview."
        )

    try:
        # Get DuckDB engine
        engine = get_query_engine()

        if not engine:
            raise HTTPException(
                status_code=503,
                detail="Query engine not initialized"
            )

        # Find affected files
        logger.info(f"Finding files affected by DELETE WHERE {request.where}")
        affected_files = await find_affected_files(
            engine,
            request.database,
            request.measurement,
            request.where
        )

        if not affected_files:
            execution_time = (time.perf_counter() - start_time) * 1000
            return DeleteResponse(
                deleted_count=0,
                affected_files=0,
                rewritten_files=0,
                execution_time_ms=execution_time,
                dry_run=request.dry_run,
                files_processed=[]
            )

        # Calculate total rows to delete
        total_to_delete = sum(count for _, count in affected_files)

        # Check max_rows_per_delete limit
        max_rows = delete_config.get("max_rows_per_delete", 1000000)
        if total_to_delete > max_rows:
            raise HTTPException(
                status_code=400,
                detail=f"Delete would affect {total_to_delete} rows, exceeding maximum of {max_rows}. "
                       f"Adjust delete.max_rows_per_delete in arc.conf or refine WHERE clause."
            )

        # Check confirmation threshold
        threshold = delete_config.get("confirmation_threshold", 10000)
        if total_to_delete > threshold and not request.confirm:
            raise HTTPException(
                status_code=400,
                detail=f"Delete would affect {total_to_delete} rows (threshold: {threshold}). "
                       f"Set confirm=true to proceed."
            )

        logger.info(f"Found {len(affected_files)} files with {total_to_delete} rows to delete")

        # If dry_run, just return stats
        if request.dry_run:
            execution_time = (time.perf_counter() - start_time) * 1000
            file_names = [f.name for f, _ in affected_files]

            return DeleteResponse(
                deleted_count=total_to_delete,
                affected_files=len(affected_files),
                rewritten_files=0,
                execution_time_ms=execution_time,
                dry_run=True,
                files_processed=file_names
            )

        # Execute actual deletion (rewrite files)
        logger.info(f"Rewriting {len(affected_files)} files to remove {total_to_delete} rows")

        total_deleted = 0
        rewritten_count = 0
        processed_files = []

        import gc

        for file_path, expected_delete_count in affected_files:
            try:
                # Rewrite file without deleted rows
                rows_before, rows_after, temp_file = await rewrite_file_without_deleted_rows(
                    file_path,
                    request.where,
                    engine
                )

                deleted_in_file = rows_before - rows_after
                total_deleted += deleted_in_file

                # If no rows left, delete the entire file
                if rows_after == 0:
                    logger.info(f"All rows deleted from {file_path.name}, removing file")
                    os.unlink(file_path)
                    os.unlink(temp_file)  # Clean up empty temp file
                else:
                    # Atomic replace: rename temp file to original
                    os.replace(temp_file, file_path)

                rewritten_count += 1
                processed_files.append(file_path.name)

                logger.info(f"Processed {file_path.name}: deleted {deleted_in_file} rows")

                # Force garbage collection between files to free memory
                # This is critical for processing many large files
                gc.collect()

            except Exception as e:
                logger.error(f"Failed to rewrite {file_path}: {e}")
                # Force GC even on error to free any partially allocated memory
                gc.collect()
                # Continue processing other files
                continue

        # Clean up temp directory
        try:
            temp_dir = Path(file_path).parent / '.tmp'
            if temp_dir.exists() and not any(temp_dir.iterdir()):
                temp_dir.rmdir()
        except Exception as e:
            logger.warning(f"Failed to clean up temp directory: {e}")

        execution_time = (time.perf_counter() - start_time) * 1000

        logger.info(f"DELETE completed: {total_deleted} rows deleted from {rewritten_count} files in {execution_time:.2f}ms")

        return DeleteResponse(
            deleted_count=total_deleted,
            affected_files=len(affected_files),
            rewritten_files=rewritten_count,
            execution_time_ms=execution_time,
            dry_run=False,
            files_processed=processed_files
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete operation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.get("/api/v1/delete/config")
async def get_delete_config(req: Request):
    """
    Get current delete configuration settings.

    Useful for clients to check limits and thresholds before attempting deletes.
    """
    return {
        "enabled": delete_config.get("enabled", False),
        "confirmation_threshold": delete_config.get("confirmation_threshold", 10000),
        "max_rows_per_delete": delete_config.get("max_rows_per_delete", 1000000),
        "implementation": "rewrite-based",
        "performance_impact": {
            "writes": "zero overhead",
            "queries": "zero overhead",
            "deletes": "expensive (file rewrites)"
        }
    }
