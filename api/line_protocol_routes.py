"""
InfluxDB Line Protocol Write API

Provides InfluxDB-compatible write endpoints for Telegraf and other clients.
Supports both InfluxDB 1.x and 2.x API formats.
"""

from fastapi import APIRouter, HTTPException, Request, Query, Header
from fastapi.responses import Response
from typing import Optional, List, Dict, Any
import logging
import gzip
import asyncio
import os
from datetime import datetime
from ingest.line_protocol_parser import LineProtocolParser
from ingest.parquet_buffer import ParquetBuffer

logger = logging.getLogger(__name__)

router = APIRouter(tags=["line-protocol"])

# Global buffer instance (initialized on startup)
parquet_buffer: Optional[ParquetBuffer] = None

# Security: Maximum decompressed payload size (default 500MB)
# This prevents gzip bomb DoS attacks (e.g., 100MB gzip -> 10GB decompressed)
MAX_DECOMPRESSED_SIZE_BYTES = int(os.getenv("MAX_DECOMPRESSED_SIZE_MB", "500")) * 1024 * 1024


def merge_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge records with the same measurement, tags, and timestamp.

    This handles cases where Telegraf sends multiple line protocol lines
    for the same metric with different field sets.

    Example:
        system,host=server load1=5.0 1234567890
        system,host=server uptime=12345 1234567890

    Gets merged into one record with both fields.
    """
    # Common tag field names in Telegraf metrics
    KNOWN_TAG_FIELDS = {
        'host', 'environment', 'cpu', 'device', 'fstype', 'mode', 'path',
        'interface', 'database', 'org', 'bucket', 'region', 'datacenter',
        'cluster', 'server', 'service', 'name', 'container', 'pod',
        'namespace', 'deployment', 'node'
    }

    # Group by (measurement, time, tags)
    merged: Dict[tuple, Dict[str, Any]] = {}

    for record in records:
        # Create unique key from measurement, time, and known tags
        measurement = record.get('measurement')
        time = record.get('time')

        # Extract only known tag fields for grouping
        tag_keys = []
        for key, value in sorted(record.items()):
            if key in KNOWN_TAG_FIELDS and isinstance(value, str):
                tag_keys.append((key, value))

        key = (measurement, str(time), tuple(tag_keys))

        if key in merged:
            # Merge all fields from this record into existing
            for k, v in record.items():
                if k not in ('measurement', 'time') and v is not None:
                    # Overwrite: last value wins (shouldn't happen in practice)
                    merged[key][k] = v
        else:
            merged[key] = record.copy()

    return list(merged.values())


def _decompress_gzip_sync(body: bytes) -> bytes:
    """Synchronous gzip decompression (runs in thread pool)"""
    return gzip.decompress(body)


async def decode_body(body: bytes, content_encoding: Optional[str] = None) -> str:
    """
    Decode request body, handling gzip compression if present.

    Telegraf sends gzip-compressed data by default.
    OPTIMIZATION: Gzip decompression runs in thread pool to release GIL.

    Security: Enforces MAX_DECOMPRESSED_SIZE_BYTES limit to prevent gzip bomb DoS attacks.

    Args:
        body: Raw request body bytes
        content_encoding: Content-Encoding header value

    Returns:
        Decoded UTF-8 string

    Raises:
        HTTPException: If payload exceeds size limit or has invalid encoding
    """
    try:
        # Check if body is gzip compressed
        # Gzip magic number: 0x1f 0x8b
        is_gzipped = False
        if len(body) >= 2 and body[0] == 0x1f and body[1] == 0x8b:
            logger.debug("Detected gzip-compressed request body")
            is_gzipped = True
        elif content_encoding and 'gzip' in content_encoding.lower():
            logger.debug("Content-Encoding indicates gzip compression")
            is_gzipped = True

        # OPTIMIZATION: Decompress in thread pool to avoid blocking event loop
        if is_gzipped:
            loop = asyncio.get_event_loop()
            decompressed = await loop.run_in_executor(None, _decompress_gzip_sync, body)

            # SECURITY: Check decompressed size to prevent gzip bomb DoS
            if len(decompressed) > MAX_DECOMPRESSED_SIZE_BYTES:
                size_mb = len(decompressed) / (1024 * 1024)
                max_mb = MAX_DECOMPRESSED_SIZE_BYTES / (1024 * 1024)
                logger.error(
                    f"Decompressed payload too large: {size_mb:.1f}MB exceeds limit of {max_mb:.0f}MB"
                )
                raise HTTPException(
                    status_code=413,
                    detail=f"Decompressed payload size ({size_mb:.1f}MB) exceeds maximum allowed ({max_mb:.0f}MB)"
                )

            body = decompressed
        else:
            # SECURITY: Check raw payload size for non-compressed requests
            if len(body) > MAX_DECOMPRESSED_SIZE_BYTES:
                size_mb = len(body) / (1024 * 1024)
                max_mb = MAX_DECOMPRESSED_SIZE_BYTES / (1024 * 1024)
                logger.error(f"Payload too large: {size_mb:.1f}MB exceeds limit of {max_mb:.0f}MB")
                raise HTTPException(
                    status_code=413,
                    detail=f"Payload size ({size_mb:.1f}MB) exceeds maximum allowed ({max_mb:.0f}MB)"
                )

        # Decode to UTF-8
        return body.decode('utf-8')
    except gzip.BadGzipFile as e:
        logger.error(f"Invalid gzip data: {e}")
        raise HTTPException(status_code=400, detail="Invalid gzip compression")
    except UnicodeDecodeError as e:
        logger.error(f"Failed to decode request body: {e}")
        raise HTTPException(status_code=400, detail="Invalid UTF-8 encoding")


def init_parquet_buffer(storage_backend, config: dict = None):
    """
    Initialize the global Parquet buffer

    Args:
        storage_backend: Storage backend instance
        config: Configuration dict with buffer settings
    """
    global parquet_buffer

    config = config or {}
    parquet_buffer = ParquetBuffer(
        storage_backend=storage_backend,
        max_buffer_size=config.get('max_buffer_size', 10000),
        max_buffer_age_seconds=config.get('max_buffer_age_seconds', 60),
        compression=config.get('compression', 'snappy')
    )

    logger.info("ParquetBuffer initialized for line protocol writes")
    return parquet_buffer


@router.post("/api/v1/write")
async def write_v1(
    request: Request,
    db: Optional[str] = Query(None, description="Database name (used as measurement prefix)"),
    rp: Optional[str] = Query(None, description="Retention policy (ignored for compatibility)"),
    precision: Optional[str] = Query('ns', description="Timestamp precision (ns, u, ms, s)"),
    content_encoding: Optional[str] = Header(None, alias="Content-Encoding"),
    x_arc_database: Optional[str] = Header(None, alias="x-arc-database"),
):
    """
    InfluxDB 1.x compatible write endpoint

    Accepts Line Protocol format in request body (gzip compressed or plain text).

    Example:
        POST /api/v1/write?db=telegraf
        cpu,host=server01 usage_idle=90.5 1609459200000000000
    """
    if not parquet_buffer:
        raise HTTPException(status_code=503, detail="Write service not initialized")

    try:
        # Read and decode body (handles gzip)
        body = await request.body()
        lines = await decode_body(body, content_encoding)

        if not lines.strip():
            raise HTTPException(status_code=400, detail="Empty request body")

        # Parse line protocol
        records = LineProtocolParser.parse_batch(lines)

        if not records:
            raise HTTPException(status_code=400, detail="No valid records in request")

        # Debug: log first parsed record
        if records:
            logger.debug(f"First parsed record - measurement: {records[0].get('measurement')}, tags: {list(records[0].get('tags', {}).keys())[:3]}")

        # Convert to flat format for Parquet
        flat_records = []
        for record in records:
            flat = LineProtocolParser.to_flat_dict(record)

            # Add database name as a tag if provided
            if db:
                flat['database'] = db

            # Inject Arc database if specified via header
            if x_arc_database:
                flat['_database'] = x_arc_database

            flat_records.append(flat)

        # Merge records with same measurement+tags+timestamp
        merged_records = merge_records(flat_records)

        # Write to buffer
        await parquet_buffer.write(merged_records)

        # Return success - InfluxDB 1.x returns 204 No Content with empty body
        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Write error: {e}")
        raise HTTPException(status_code=500, detail=f"Write failed: {str(e)}")


@router.post("/api/v1/write/influxdb")
async def write_influxdb(
    request: Request,
    org: Optional[str] = Query(None, description="Organization name"),
    bucket: str = Query(..., description="Bucket name (required)"),
    precision: Optional[str] = Query('ns', description="Timestamp precision (ns, us, ms, s)"),
    authorization: Optional[str] = Header(None, description="Token for auth (optional)"),
    content_encoding: Optional[str] = Header(None, alias="Content-Encoding"),
    x_arc_database: Optional[str] = Header(None, alias="x-arc-database"),
):
    """
    InfluxDB compatible write endpoint (Line Protocol)

    Accepts Line Protocol format in request body (gzip compressed or plain text).

    Example:
        POST /api/v1/write/influxdb?org=myorg&bucket=mybucket
        Authorization: Token my-token
        cpu,host=server01 usage_idle=90.5 1609459200000000000
    """
    if not parquet_buffer:
        raise HTTPException(status_code=503, detail="Write service not initialized")

    try:
        # Read and decode body (handles gzip)
        body = await request.body()
        lines = await decode_body(body, content_encoding)

        if not lines.strip():
            raise HTTPException(status_code=400, detail="Empty request body")

        # Parse line protocol
        records = LineProtocolParser.parse_batch(lines)

        if not records:
            raise HTTPException(status_code=400, detail="No valid records in request")

        # Convert to flat format for Parquet
        flat_records = []
        for record in records:
            flat = LineProtocolParser.to_flat_dict(record)

            # Add metadata as tags
            if org:
                flat['org'] = org
            if bucket:
                flat['bucket'] = bucket

            # Inject Arc database if specified via header
            if x_arc_database:
                flat['_database'] = x_arc_database

            flat_records.append(flat)

        # Merge records with same measurement+tags+timestamp
        merged_records = merge_records(flat_records)

        # Write to buffer
        await parquet_buffer.write(merged_records)

        # Return success - InfluxDB 2.x returns 204 No Content with empty body
        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Write error: {e}")
        raise HTTPException(status_code=500, detail=f"Write failed: {str(e)}")


@router.post("/api/v1/write/line-protocol")
async def write_simple(
    request: Request,
    content_encoding: Optional[str] = Header(None, alias="Content-Encoding"),
    x_arc_database: Optional[str] = Header(None, alias="x-arc-database"),
):
    """
    Simple write endpoint (no query parameters)

    Accepts Line Protocol format in request body (gzip compressed or plain text).
    Useful for quick testing and simple integrations.

    Example:
        POST /write
        cpu,host=server01 usage_idle=90.5 1609459200000000000
    """
    if not parquet_buffer:
        raise HTTPException(status_code=503, detail="Write service not initialized")

    try:
        # Read and decode body (handles gzip)
        body = await request.body()
        lines = await decode_body(body, content_encoding)

        if not lines.strip():
            raise HTTPException(status_code=400, detail="Empty request body")

        # Parse line protocol
        records = LineProtocolParser.parse_batch(lines)

        if not records:
            raise HTTPException(status_code=400, detail="No valid records in request")

        # Convert to flat format
        flat_records = []
        for record in records:
            flat = LineProtocolParser.to_flat_dict(record)

            # Inject Arc database if specified via header
            if x_arc_database:
                flat['_database'] = x_arc_database

            flat_records.append(flat)

        # Merge records with same measurement+tags+timestamp
        merged_records = merge_records(flat_records)

        # Write to buffer
        await parquet_buffer.write(merged_records)

        # Return success - InfluxDB 1.x returns 204 No Content with empty body
        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Write error: {e}")
        raise HTTPException(status_code=500, detail=f"Write failed: {str(e)}")


@router.get("/api/v1/write/health")
async def write_health():
    """Health check for write endpoint"""
    if not parquet_buffer:
        raise HTTPException(status_code=503, detail="Write service not initialized")

    stats = parquet_buffer.get_stats()

    return {
        "status": "healthy",
        "service": "line_protocol_writer",
        "stats": stats
    }


@router.get("/api/v1/write/stats")
async def write_stats():
    """Get write statistics"""
    if not parquet_buffer:
        raise HTTPException(status_code=503, detail="Write service not initialized")

    stats = parquet_buffer.get_stats()

    return {
        "status": "success",
        "buffer_stats": stats,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/api/v1/write/flush")
async def flush_buffer(measurement: Optional[str] = Query(None, description="Specific measurement to flush")):
    """
    Manually flush buffer to storage

    Useful for testing or forcing immediate persistence.
    """
    if not parquet_buffer:
        raise HTTPException(status_code=503, detail="Write service not initialized")

    try:
        if measurement:
            await parquet_buffer.flush_measurement_sync(measurement)
            return {
                "status": "success",
                "message": f"Flushed buffer for measurement: {measurement}"
            }
        else:
            await parquet_buffer.flush_all()
            return {
                "status": "success",
                "message": "Flushed all buffers"
            }

    except Exception as e:
        logger.error(f"Flush error: {e}")
        raise HTTPException(status_code=500, detail=f"Flush failed: {str(e)}")


# Startup/shutdown handlers
async def start_parquet_buffer():
    """Start the parquet buffer background tasks"""
    if parquet_buffer:
        await parquet_buffer.start()
        logger.info("Line protocol write service started")


async def stop_parquet_buffer():
    """Stop the parquet buffer and flush remaining data"""
    if parquet_buffer:
        await parquet_buffer.stop()
        logger.info("Line protocol write service stopped")
