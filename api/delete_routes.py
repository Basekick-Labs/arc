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
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from config_loader import get_config
from api.partition_pruner import PartitionPruner

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

# Initialize partition pruner for optimizing file scans
_partition_pruner = PartitionPruner(enable_statistics_filtering=False)  # Don't need stats for delete

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

    Uses partition pruning when time filters are present in WHERE clause.

    Returns list of (file_path, matching_row_count) tuples.
    """
    # Get storage backend
    storage = engine.local_backend or engine.minio_backend or engine.s3_backend or engine.gcs_backend or engine.ceph_backend
    if not storage:
        raise Exception("No storage backend available")

    affected_files = []

    # Try to extract time range for partition pruning optimization
    time_range = _partition_pruner.extract_time_range(f"SELECT * FROM table WHERE {where_clause}")

    # For local storage
    if hasattr(storage, 'base_path'):
        base_path = Path(storage.base_path) / database / measurement

        if not base_path.exists():
            logger.warning(f"No data directory found for {database}/{measurement}")
            return []

        # Use partition pruning if time range found
        if time_range:
            logger.info(f"Partition pruning: Using time range {time_range[0]} to {time_range[1]}")
            partition_paths = _partition_pruner.generate_partition_paths(
                str(base_path),
                "",  # Already in measurement path
                "",
                time_range
            )

            # Expand glob patterns to individual files
            parquet_files = []
            import glob as glob_module
            for pattern in partition_paths:
                # Adjust pattern since base_path already includes database/measurement
                adjusted_pattern = pattern.replace(f"{base_path}///", str(base_path) + "/")
                adjusted_pattern = adjusted_pattern.replace(f"{base_path}//", str(base_path) + "/")
                files = glob_module.glob(adjusted_pattern)
                parquet_files.extend([Path(f) for f in files])

            logger.info(f"Partition pruning: Narrowed to {len(parquet_files)} files (from potential thousands)")
        else:
            # Fall back to full scan if no time filter
            parquet_files = list(base_path.rglob("*.parquet"))
            logger.info(f"No time filter found, scanning all {len(parquet_files)} parquet files")

        for parquet_file in parquet_files:
            try:
                # Read Parquet file
                table = pq.read_table(parquet_file)

                # Evaluate WHERE clause using DuckDB (most reliable)
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
        # Cloud storage (S3, MinIO, GCS, Ceph)
        logger.info(f"Using cloud storage backend for DELETE operation")

        # Build measurement path
        measurement_path = f"{database}/{measurement}"

        # Use partition pruning if time range found
        if time_range:
            logger.info(f"Partition pruning: Using time range {time_range[0]} to {time_range[1]}")
            partition_paths = _partition_pruner.generate_partition_paths(
                "",  # Cloud storage uses prefix-based paths
                database,
                measurement,
                time_range
            )

            # List files matching partition patterns from cloud storage
            parquet_files = []
            for pattern in partition_paths:
                # Convert local-style glob to cloud prefix pattern
                # Example: default/cpu/2025/11/17/14/*.parquet -> default/cpu/2025/11/17/14/
                prefix = pattern.replace("*.parquet", "").lstrip("/")

                try:
                    if hasattr(storage, 'list_files'):
                        # Async list_files (GCS, S3)
                        files = await storage.list_files(path=prefix, pattern="*.parquet")
                    elif hasattr(storage, 'list_objects'):
                        # MinIO
                        objects = storage.client.list_objects(
                            storage.bucket_name,
                            prefix=prefix,
                            recursive=False
                        )
                        files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
                    else:
                        raise Exception(f"Storage backend does not support file listing")

                    parquet_files.extend(files)
                except Exception as e:
                    logger.warning(f"Failed to list files with prefix {prefix}: {e}")
                    continue

            logger.info(f"Partition pruning: Narrowed to {len(parquet_files)} cloud files")
        else:
            # Fall back to listing all files in measurement
            logger.info(f"No time filter found, listing all files in {measurement_path}")

            try:
                if hasattr(storage, 'list_files'):
                    # Async list_files (GCS, S3)
                    parquet_files = await storage.list_files(path=measurement_path, pattern="*.parquet")
                elif hasattr(storage, 'list_objects'):
                    # MinIO
                    objects = storage.client.list_objects(
                        storage.bucket_name,
                        prefix=f"{measurement_path}/",
                        recursive=True
                    )
                    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
                else:
                    raise Exception(f"Storage backend does not support file listing")

                logger.info(f"Listed {len(parquet_files)} cloud parquet files")
            except Exception as e:
                logger.error(f"Failed to list cloud files: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to list cloud storage files: {str(e)}"
                )

        # Process cloud files
        for file_path in parquet_files:
            try:
                # Build full cloud path
                if hasattr(storage, 'bucket_name'):
                    # MinIO/S3/GCS
                    cloud_url = f"s3://{storage.bucket_name}/{file_path}"
                else:
                    cloud_url = file_path

                # Read Parquet file from cloud
                table = pq.read_table(cloud_url)

                # Evaluate WHERE clause
                matching_count = await count_matching_rows(table, where_clause, engine)

                # Explicitly free table memory
                del table

                if matching_count > 0:
                    # Store cloud path as string (not Path object)
                    affected_files.append((file_path, matching_count))
                    logger.info(f"Cloud file {file_path} has {matching_count} matching rows")

            except Exception as e:
                logger.error(f"Error processing cloud file {file_path}: {e}")
                continue

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
    file_path,  # Can be Path (local) or str (cloud)
    where_clause: str,
    engine,
    storage_backend=None
) -> Tuple[int, int, any]:
    """
    Rewrite a Parquet file, excluding rows that match the WHERE clause.

    Supports both local and cloud storage.

    Returns (rows_before, rows_after, new_file_path).
    """
    import duckdb
    import gc

    # Validate WHERE clause to prevent SQL injection
    validate_where_clause(where_clause)

    is_cloud = not isinstance(file_path, Path)

    # Read original file
    if is_cloud:
        # Cloud storage - build S3/GCS URL
        if hasattr(storage_backend, 'bucket_name'):
            cloud_url = f"s3://{storage_backend.bucket_name}/{file_path}"
        else:
            cloud_url = file_path
        table = pq.read_table(cloud_url)
        logger.info(f"Read cloud file {file_path}")
    else:
        # Local storage
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

        if is_cloud:
            # Cloud storage - write to temp local file, then upload
            import tempfile
            temp_local_file = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')
            temp_file = temp_local_file.name
            temp_local_file.close()

            # Write filtered table to local temp file
            pq.write_table(
                result,
                temp_file,
                compression='snappy'
            )

            logger.info(f"Wrote temp file {temp_file} for cloud upload: {rows_before} -> {rows_after} rows")

        else:
            # Local storage - write to temp directory
            temp_dir = file_path.parent / '.tmp'
            temp_dir.mkdir(exist_ok=True)

            temp_file = temp_dir / f"{file_path.stem}_new{file_path.suffix}"

            # Write filtered table to temp file
            pq.write_table(
                result,
                temp_file,
                compression='snappy'
            )

            logger.info(f"Rewrote local file {file_path.name}: {rows_before} -> {rows_after} rows")

        # Explicitly delete result table and force GC
        del result
        gc.collect()

        return rows_before, rows_after, temp_file

    except Exception as e:
        logger.error(f"Error rewriting file {file_path}: {e}")
        # Clean up temp file if created
        if temp_file:
            try:
                if is_cloud:
                    os.unlink(temp_file)
                elif temp_file.exists():
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
            file_names = [f.name if isinstance(f, Path) else f for f, _ in affected_files]

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

        # Get storage backend for cloud operations
        storage = engine.local_backend or engine.minio_backend or engine.s3_backend or engine.gcs_backend or engine.ceph_backend
        is_cloud_storage = not hasattr(storage, 'base_path')

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
                    engine,
                    storage_backend=storage
                )

                deleted_in_file = rows_before - rows_after
                total_deleted += deleted_in_file

                # Handle file replacement based on storage type
                if is_cloud_storage:
                    # Cloud storage replacement
                    if rows_after == 0:
                        # Delete the cloud file entirely
                        logger.info(f"All rows deleted from cloud file {file_path}, removing")
                        if hasattr(storage, 'delete_file'):
                            if asyncio.iscoroutinefunction(storage.delete_file):
                                await storage.delete_file(file_path)
                            else:
                                storage.delete_file(file_path)
                        # Clean up local temp file
                        os.unlink(temp_file)
                    else:
                        # Upload new file to replace old one
                        logger.info(f"Uploading rewritten file to cloud: {file_path}")

                        # Read temp file and upload
                        with open(temp_file, 'rb') as f:
                            file_data = f.read()

                        if hasattr(storage, 'upload_file'):
                            # GCS/S3 async upload
                            await storage.upload_file(file_path, file_data)
                        elif hasattr(storage, 'put_object'):
                            # MinIO upload
                            from io import BytesIO
                            storage.client.put_object(
                                storage.bucket_name,
                                file_path,
                                BytesIO(file_data),
                                length=len(file_data)
                            )
                        else:
                            raise Exception("Cloud storage backend does not support file upload")

                        # Clean up local temp file
                        os.unlink(temp_file)

                    file_display_name = file_path if isinstance(file_path, str) else file_path.name
                else:
                    # Local storage replacement
                    if rows_after == 0:
                        logger.info(f"All rows deleted from {file_path.name}, removing file")
                        os.unlink(file_path)
                        os.unlink(temp_file)  # Clean up empty temp file
                    else:
                        # Atomic replace: rename temp file to original
                        os.replace(temp_file, file_path)

                    file_display_name = file_path.name

                rewritten_count += 1
                processed_files.append(file_display_name)

                logger.info(f"Processed {file_display_name}: deleted {deleted_in_file} rows")

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
