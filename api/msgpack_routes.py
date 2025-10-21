"""
Arc Binary Protocol API (MessagePack)

High-performance binary write endpoint using MessagePack + Direct Arrow/Parquet.
Expected: 3-5x faster than Line Protocol.
"""

from fastapi import APIRouter, HTTPException, Request, Header
from fastapi.responses import Response
from typing import Optional
import logging
import gzip
from ingest.msgpack_decoder import MessagePackDecoder
from ingest.arrow_writer import ArrowParquetBuffer

logger = logging.getLogger(__name__)

router = APIRouter(tags=["binary-protocol"])

# Global buffer (initialized at startup)
arrow_buffer: Optional[ArrowParquetBuffer] = None
msgpack_decoder = MessagePackDecoder()


def init_arrow_buffer(storage_backend, config: dict = None):
    """
    Initialize the global Arrow Parquet buffer

    Args:
        storage_backend: Storage backend instance
        config: Configuration dict with buffer settings (including WAL config)
    """
    global arrow_buffer

    config = config or {}

    # Get WAL configuration from config
    wal_enabled = config.get('wal_enabled', False)
    wal_config = config.get('wal_config')

    # Prepare WAL config dict if enabled
    wal_init_config = None
    if wal_enabled and wal_config:
        import os
        worker_id = os.getpid()  # Use process ID as worker ID
        wal_init_config = {
            'wal_dir': wal_config.get('dir', './data/wal'),
            'worker_id': worker_id,
            'sync_mode': wal_config.get('sync_mode', 'fdatasync'),
            'max_size_mb': wal_config.get('max_size_mb', 100),
            'max_age_seconds': wal_config.get('max_age_seconds', 3600)
        }

    arrow_buffer = ArrowParquetBuffer(
        storage_backend=storage_backend,
        max_buffer_size=config.get('max_buffer_size', 10000),
        max_buffer_age_seconds=config.get('max_buffer_age_seconds', 60),
        compression=config.get('compression', 'snappy'),
        wal_enabled=wal_enabled,
        wal_config=wal_init_config
    )

    logger.info(
        "ArrowParquetBuffer initialized for MessagePack binary protocol "
        f"(Direct Arrow, zero-copy, WAL={'enabled' if wal_enabled else 'disabled'})"
    )
    return arrow_buffer


async def start_arrow_buffer():
    """Start the Arrow buffer background task"""
    if arrow_buffer:
        await arrow_buffer.start()


async def stop_arrow_buffer():
    """Stop the Arrow buffer and flush remaining data"""
    if arrow_buffer:
        await arrow_buffer.stop()


@router.post("/api/v1/write/msgpack")
async def write_msgpack(
    request: Request,
    content_encoding: Optional[str] = Header(None, alias="Content-Encoding"),
    x_arc_database: Optional[str] = Header(None, alias="x-arc-database"),
):
    """
    Arc Binary Protocol - MessagePack Write Endpoint

    High-performance binary write using MessagePack + Direct Arrow/Parquet.

    Request:
        POST /write/v1/msgpack
        Content-Type: application/msgpack
        Content-Encoding: gzip (optional)
        x-api-key: <token>

        Body: MessagePack binary payload

    Payload Format (columnar - RECOMMENDED for best performance):
        {
            "m": "cpu",              # measurement
            "columns": {             # columnar data (25-35% faster)
                "time": [1633024800000, 1633024801000, 1633024802000],
                "host": ["server01", "server01", "server01"],
                "region": ["us-east", "us-east", "us-east"],
                "usage_idle": [95.0, 94.5, 94.2],
                "usage_user": [3.2, 3.8, 4.1]
            }
        }

    Payload Format (row - LEGACY, slower):
        {
            "m": "cpu",              # measurement
            "t": 1633024800000,      # timestamp (ms)
            "h": "server01",         # host (optional)
            "fields": {              # fields (required)
                "usage_idle": 95.0,
                "usage_user": 3.2
            },
            "tags": {                # tags (optional)
                "region": "us-east"
            }
        }

    Payload Format (batch):
        {
            "batch": [
                {"m": "cpu", "columns": {...}},  # columnar (fast)
                {"m": "mem", "fields": {...}}    # row format (legacy)
            ]
        }

    Returns:
        204 No Content on success
        400 Bad Request if invalid MessagePack
        401 Unauthorized if auth fails
        500 Internal Server Error on processing error
    """
    try:
        # Read binary payload with size check
        payload = await request.body()

        # Validate payload size
        if len(payload) == 0:
            raise HTTPException(status_code=400, detail="Empty payload")

        if len(payload) > 100 * 1024 * 1024:  # 100MB limit
            raise HTTPException(status_code=413, detail="Payload too large (max 100MB)")

        # Decompress if gzip encoded
        if content_encoding and content_encoding.lower() == 'gzip':
            try:
                payload = gzip.decompress(payload)
            except Exception as e:
                logger.error(f"Failed to decompress gzip payload: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid gzip compression: {e}")

        # Decode MessagePack binary to records
        try:
            records = msgpack_decoder.decode(payload)
        except ValueError as e:
            logger.error(f"Invalid MessagePack payload: {e}")
            raise HTTPException(status_code=400, detail=str(e))

        if not records:
            raise HTTPException(status_code=400, detail="No records in payload")

        # Inject database into records if specified via header
        if x_arc_database:
            for record in records:
                record['_database'] = x_arc_database

        # Write to Arrow buffer (Direct Arrow/Parquet, no DataFrame)
        if arrow_buffer is None:
            raise HTTPException(
                status_code=500,
                detail="Arrow buffer not initialized"
            )

        await arrow_buffer.write(records)

        logger.debug(
            f"Received {len(records)} records via MessagePack binary protocol "
            f"(compressed: {content_encoding == 'gzip'})"
        )

        # Return 204 No Content (InfluxDB compatible)
        return Response(status_code=204)

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.error(f"Error processing MessagePack write: {error_msg}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal error: {error_msg}")


@router.get("/api/v1/write/msgpack/stats")
async def msgpack_stats():
    """
    Get MessagePack endpoint statistics

    Returns:
        JSON with decoder and buffer stats
    """
    stats = {
        'decoder': msgpack_decoder.get_stats(),
        'buffer': arrow_buffer.get_stats() if arrow_buffer else None
    }
    return stats


@router.get("/api/v1/write/msgpack/spec")
async def msgpack_spec():
    """
    Get MessagePack binary protocol specification

    Returns:
        JSON with protocol details and examples
    """
    return {
        'version': '2.0',
        'protocol': 'MessagePack',
        'endpoint': '/api/v1/write/msgpack',
        'content_type': 'application/msgpack',
        'compression': 'gzip (optional)',
        'authentication': 'x-api-key header',
        'format': {
            'columnar (RECOMMENDED)': {
                'm': 'measurement (string)',
                'columns': 'dict of column_name: [array of values]',
                'note': '25-35% faster than row format, zero-copy passthrough'
            },
            'row (LEGACY)': {
                'm': 'measurement (string or int)',
                't': 'timestamp (int64 milliseconds)',
                'h': 'host (string or int, optional)',
                'fields': 'dict of field_name: value',
                'tags': 'dict of tag_name: value (optional)'
            },
            'batch': {
                'batch': 'array of measurements (can mix columnar and row)'
            }
        },
        'example_columnar': {
            'm': 'cpu',
            'columns': {
                'time': [1633024800000, 1633024801000, 1633024802000],
                'host': ['server01', 'server01', 'server01'],
                'region': ['us-east', 'us-east', 'us-east'],
                'usage_idle': [95.0, 94.5, 94.2],
                'usage_user': [3.2, 3.8, 4.1],
                'usage_system': [1.8, 1.7, 1.7]
            }
        },
        'example_row': {
            'm': 'cpu',
            't': 1633024800000,
            'h': 'server01',
            'fields': {
                'usage_idle': 95.0,
                'usage_user': 3.2,
                'usage_system': 1.8
            },
            'tags': {
                'region': 'us-east',
                'datacenter': 'aws-1a'
            }
        },
        'performance': {
            'expected_rps_columnar': '2.5M+ (columnar format, zero-copy)',
            'expected_rps_row': '2.1M (row format, with conversion)',
            'columnar_advantage': '25-35% faster (no flattening, no row→column conversion)',
            'parsing_speed': '10-15x faster than text parsing',
            'serialization': 'Direct Arrow (2-3x faster than DataFrame)',
            'payload_size': '50-70% smaller than Line Protocol',
            'wire_efficiency': 'Columnar sends field names once vs per-record'
        }
    }
