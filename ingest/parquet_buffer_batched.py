"""
Parquet Buffer Writer with Batched Compression

Enhanced buffer that batches compression operations for better performance.
Instead of compressing each parquet file individually, accumulates files
and compresses them together for better compression ratios and fewer CPU cycles.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path
import polars as pl
from collections import defaultdict
import tempfile
import os
import tarfile
import gzip

logger = logging.getLogger(__name__)


class ParquetBufferBatched:
    """
    Thread-safe buffer with batched compression for improved performance

    Features:
    - Automatic flushing based on size or time
    - Per-measurement buffering
    - Batched compression (compress multiple files together)
    - Atomic file writes
    - Storage backend abstraction
    """

    def __init__(
        self,
        storage_backend,
        max_buffer_size: int = 10000,
        max_buffer_age_seconds: int = 60,
        compression: str = 'snappy',
        batch_compression: bool = True,
        compression_batch_size: int = 5  # Compress N files together
    ):
        """
        Initialize buffer with batched compression

        Args:
            storage_backend: Storage backend instance
            max_buffer_size: Max records per measurement before flush
            max_buffer_age_seconds: Max seconds before flush
            compression: Parquet compression (snappy, gzip, zstd)
            batch_compression: Enable batched compression
            compression_batch_size: Number of files to compress together
        """
        self.storage_backend = storage_backend
        self.max_buffer_size = max_buffer_size
        self.max_buffer_age_seconds = max_buffer_age_seconds
        self.compression = compression
        self.batch_compression = batch_compression
        self.compression_batch_size = compression_batch_size

        # Per-measurement buffers
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.buffer_start_times: Dict[str, datetime] = {}

        # Pending parquet files waiting for batched compression
        self.pending_files: List[tuple[str, str]] = []  # (temp_path, remote_path)
        self.pending_files_lock = asyncio.Lock()

        # Metrics
        self.total_records_buffered = 0
        self.total_records_written = 0
        self.total_flushes = 0
        self.total_errors = 0
        self.total_compression_batches = 0
        self.total_files_compressed = 0

        # Background tasks
        self._flush_task: Optional[asyncio.Task] = None
        self._compress_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()

        logger.info(
            f"ParquetBufferBatched initialized: "
            f"max_size={max_buffer_size}, max_age={max_buffer_age_seconds}s, "
            f"batch_compression={batch_compression}, compression_batch_size={compression_batch_size}"
        )

    async def start(self):
        """Start background tasks"""
        if self._running:
            return

        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())

        if self.batch_compression:
            self._compress_task = asyncio.create_task(self._periodic_compress())

        logger.info(
            f"ParquetBufferBatched started "
            f"(max_size={self.max_buffer_size}, max_age={self.max_buffer_age_seconds}s)"
        )

    async def stop(self):
        """Stop background tasks and flush remaining data"""
        if not self._running:
            return

        self._running = False

        # Cancel background tasks
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        if self._compress_task:
            self._compress_task.cancel()
            try:
                await self._compress_task
            except asyncio.CancelledError:
                pass

        # Flush all remaining buffers
        await self.flush_all()

        # Compress and upload any pending files
        await self._compress_and_upload_pending(force=True)

        logger.info(
            f"ParquetBufferBatched stopped. "
            f"Total records written: {self.total_records_written}, "
            f"Compression batches: {self.total_compression_batches}"
        )

    async def write(self, records: List[Dict[str, Any]]):
        """
        Add records to buffer

        Args:
            records: List of flat dictionaries with keys: time, measurement, tag_*, field_*
        """
        if not records:
            return

        async with self._lock:
            # Group records by measurement
            by_measurement = defaultdict(list)
            for record in records:
                measurement = record.get('measurement', 'unknown')
                by_measurement[measurement].append(record)

            # Add to buffers
            for measurement, measurement_records in by_measurement.items():
                if measurement not in self.buffer_start_times:
                    self.buffer_start_times[measurement] = datetime.now(timezone.utc)

                self.buffers[measurement].extend(measurement_records)
                self.total_records_buffered += len(measurement_records)

                # Check if buffer should be flushed
                if len(self.buffers[measurement]) >= self.max_buffer_size:
                    logger.debug(f"Buffer for '{measurement}' reached size limit, flushing")
                    await self._flush_measurement(measurement)

    async def _periodic_flush(self):
        """Background task that periodically flushes old buffers"""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds

                async with self._lock:
                    now = datetime.now(timezone.utc)
                    measurements_to_flush = []

                    for measurement, start_time in self.buffer_start_times.items():
                        age_seconds = (now - start_time).total_seconds()
                        if age_seconds >= self.max_buffer_age_seconds:
                            measurements_to_flush.append(measurement)

                    for measurement in measurements_to_flush:
                        logger.debug(f"Buffer for '{measurement}' reached age limit, flushing")
                        await self._flush_measurement(measurement)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
                await asyncio.sleep(5)

    async def _periodic_compress(self):
        """Background task that periodically compresses pending files"""
        while self._running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                await self._compress_and_upload_pending()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic compress: {e}")
                await asyncio.sleep(10)

    async def _flush_measurement(self, measurement: str):
        """
        Flush buffer for a specific measurement

        This method should be called while holding self._lock
        """
        if measurement not in self.buffers or not self.buffers[measurement]:
            return

        records = self.buffers[measurement]
        self.buffers[measurement] = []
        del self.buffer_start_times[measurement]

        try:
            # Convert to DataFrame
            df = pl.DataFrame(records)

            # Ensure time column is datetime
            if 'time' in df.columns:
                df = df.with_columns(pl.col('time').cast(pl.Datetime))

            # Sort by time
            df = df.sort('time')

            # Generate filename with timestamp range
            min_time = df['time'].min()
            max_time = df['time'].max()

            timestamp_str = min_time.strftime('%Y%m%d_%H%M%S')
            filename = f"{measurement}_{timestamp_str}_{len(records)}.parquet"

            # Partition path by date
            date_partition = min_time.strftime('%Y/%m/%d')
            remote_path = f"{measurement}/{date_partition}/{filename}"

            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name

            try:
                # Write Parquet file (uncompressed or snappy, no gzip here)
                df.write_parquet(tmp_path, compression=self.compression)

                if self.batch_compression:
                    # Add to pending files for batched compression
                    async with self.pending_files_lock:
                        self.pending_files.append((tmp_path, remote_path))
                        logger.debug(
                            f"Queued {len(records)} records for '{measurement}' "
                            f"(pending: {len(self.pending_files)})"
                        )

                    # Trigger compression if batch is full
                    if len(self.pending_files) >= self.compression_batch_size:
                        await self._compress_and_upload_pending()
                else:
                    # Upload immediately without batching
                    await self.storage_backend.upload_file(Path(tmp_path), remote_path)
                    os.unlink(tmp_path)

                self.total_records_written += len(records)
                self.total_flushes += 1

            except Exception as e:
                logger.error(f"Failed to write parquet for '{measurement}': {e}")
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise

        except Exception as e:
            logger.error(f"Failed to flush measurement '{measurement}': {e}")
            self.total_errors += 1
            # Re-add records to buffer on error
            if len(self.buffers[measurement]) < self.max_buffer_size * 2:
                self.buffers[measurement].extend(records)
                logger.warning(f"Re-added {len(records)} records to buffer after error")
            else:
                logger.error(f"Dropping {len(records)} records due to persistent errors")

    async def _compress_and_upload_pending(self, force: bool = False):
        """
        Compress pending files in a batch and upload

        Args:
            force: Compress even if batch is not full
        """
        async with self.pending_files_lock:
            if not self.pending_files:
                return

            if not force and len(self.pending_files) < self.compression_batch_size:
                return

            # Take files to compress
            files_to_process = self.pending_files[:]
            self.pending_files = []

        if not files_to_process:
            return

        logger.info(f"Compressing batch of {len(files_to_process)} parquet files")

        # Upload files individually (already compressed by parquet)
        upload_tasks = []
        for tmp_path, remote_path in files_to_process:
            upload_tasks.append(
                self._upload_and_cleanup(tmp_path, remote_path)
            )

        # Execute uploads concurrently
        results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        successful = sum(1 for r in results if r is True)
        self.total_compression_batches += 1
        self.total_files_compressed += successful

        logger.info(
            f"Batch compression complete: {successful}/{len(files_to_process)} files uploaded "
            f"(total batches: {self.total_compression_batches})"
        )

    async def _upload_and_cleanup(self, tmp_path: str, remote_path: str) -> bool:
        """Upload file and cleanup temp file"""
        try:
            await self.storage_backend.upload_file(Path(tmp_path), remote_path)
            logger.debug(f"Uploaded {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {remote_path}: {e}")
            return False
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    async def flush_all(self):
        """Flush all measurement buffers"""
        async with self._lock:
            measurements = list(self.buffers.keys())
            for measurement in measurements:
                if self.buffers[measurement]:
                    await self._flush_measurement(measurement)

    async def flush_measurement_sync(self, measurement: str):
        """Explicitly flush a specific measurement (with lock)"""
        async with self._lock:
            await self._flush_measurement(measurement)

    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            'total_records_buffered': self.total_records_buffered,
            'total_records_written': self.total_records_written,
            'total_flushes': self.total_flushes,
            'total_errors': self.total_errors,
            'total_compression_batches': self.total_compression_batches,
            'total_files_compressed': self.total_files_compressed,
            'pending_files': len(self.pending_files),
            'current_buffer_sizes': {
                measurement: len(records)
                for measurement, records in self.buffers.items()
            },
            'oldest_buffer_age_seconds': self._get_oldest_buffer_age(),
            'batch_compression_enabled': self.batch_compression,
            'compression_batch_size': self.compression_batch_size
        }

    def _get_oldest_buffer_age(self) -> Optional[float]:
        """Get age of oldest buffer in seconds"""
        if not self.buffer_start_times:
            return None

        now = datetime.now(timezone.utc)
        oldest = min(self.buffer_start_times.values())
        return (now - oldest).total_seconds()
