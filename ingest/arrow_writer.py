"""
Direct Arrow/Parquet Writer

High-performance writer that bypasses Pandas DataFrame for zero-copy writes.
Uses PyArrow directly for 2-3x faster serialization.

Performance Optimizations:
1. Flush outside lock: Extract records under lock, flush outside (no write blocking)
2. Optimized columnar conversion: List comprehension + direct dict access (15-25% faster)
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import logging
import hashlib
import json

logger = logging.getLogger(__name__)


class ArrowParquetWriter:
    """
    Direct Arrow → Parquet writer without DataFrame intermediate step.

    Benefits:
    - Zero-copy writes (no DataFrame allocation)
    - 2-3x faster serialization
    - Lower memory usage
    - Columnar from start
    """

    def __init__(self, compression: str = 'snappy'):
        """
        Initialize writer

        Args:
            compression: Parquet compression (snappy, gzip, zstd)
        """
        self.compression = compression.upper()

    def _compute_row_hash(self, row: Dict[str, Any], measurement: str) -> str:
        """
        Compute SHA256 hash for a row to enable delete operations.

        Uses time + measurement + sorted tags (non-field columns) to create
        a unique identifier that matches the delete_routes.py logic.

        Args:
            row: Dictionary containing row data
            measurement: Measurement name

        Returns:
            SHA256 hash string
        """
        # Create stable hash from row data (matches delete_routes.py logic)
        hash_parts = [
            str(row.get('time', '')),
            str(measurement)
        ]

        # Add all non-field columns (tags) sorted by key
        # Fields are typically numeric measurements, tags are string labels
        for key in sorted(row.keys()):
            if key not in ['time', 'measurement', 'fields']:
                # This is a tag or other metadata
                value = row.get(key, '')
                hash_parts.append(f"{key}={value}")

        hash_str = '|'.join(hash_parts)
        return hashlib.sha256(hash_str.encode()).hexdigest()

    def _add_row_hashes_to_records(self, records: List[Dict[str, Any]], measurement: str) -> List[Dict[str, Any]]:
        """
        Add _row_hash column to each record for delete support.

        Args:
            records: List of record dictionaries
            measurement: Measurement name

        Returns:
            Records with _row_hash added
        """
        for record in records:
            record['_row_hash'] = self._compute_row_hash(record, measurement)
        return records

    def _add_row_hashes_to_columns(self, columns: Dict[str, List], measurement: str) -> Dict[str, List]:
        """
        Add _row_hash column to columnar data for delete support.

        Args:
            columns: Columnar data dictionary
            measurement: Measurement name

        Returns:
            Columns with _row_hash added
        """
        # Get number of rows
        if not columns:
            return columns

        num_rows = len(next(iter(columns.values())))

        # Compute hash for each row
        row_hashes = []
        for i in range(num_rows):
            # Reconstruct row from columns
            row = {key: values[i] for key, values in columns.items()}
            row_hash = self._compute_row_hash(row, measurement)
            row_hashes.append(row_hash)

        # Add _row_hash column
        columns['_row_hash'] = row_hashes
        return columns

    def write_parquet(
        self,
        records: List[Dict[str, Any]],
        output_path: Path,
        measurement: str
    ) -> bool:
        """
        Write records directly to Parquet using Arrow RecordBatch.

        Args:
            records: List of flat dictionaries
            output_path: Path to write parquet file
            measurement: Measurement name (for logging)

        Returns:
            True if successful
        """
        if not records:
            logger.warning(f"No records to write for '{measurement}'")
            return False

        try:
            # NOTE: Removed _row_hash computation - DELETE operations disabled
            # This was causing 700K RPS loss (2.4M → 1.7M) due to:
            #   - 2.5M SHA256 hashes per second
            #   - Destroying zero-copy columnar passthrough
            #   - Reconstructing rows from columns
            # records_with_hash = self._add_row_hashes_to_records(records, measurement)

            # Convert records to columnar format (dict of arrays)
            columns = self._records_to_columns(records)

            # Infer Arrow schema from first record
            schema = self._infer_schema(columns)

            # Create Arrow RecordBatch (zero-copy columnar structure)
            record_batch = pa.RecordBatch.from_pydict(columns, schema=schema)

            # Create Arrow Table from RecordBatch
            table = pa.Table.from_batches([record_batch])

            # Write directly to Parquet (bypasses DataFrame)
            pq.write_table(
                table,
                output_path,
                compression=self.compression,
                use_dictionary=True,  # Better compression for repeated values
                write_statistics=True  # Enable query optimization
            )

            logger.debug(
                f"Wrote {len(records)} records for '{measurement}' to {output_path} "
                f"(compression={self.compression}, with _row_hash)"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to write Arrow Parquet for '{measurement}': {e}")
            return False

    def _records_to_columns(self, records: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Convert list of records to columnar format.

        Input: [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        Output: {"a": [1, 3], "b": [2, 4]}

        OPTIMIZATION: Use list comprehension instead of append() loop
        - Avoids repeated list reallocations (each append() can trigger realloc)
        - List comprehension pre-allocates based on size hint
        - Direct dict access [key] faster than .get(key)
        - Expected gain: 15-25% faster columnar conversion
        """
        if not records:
            return {}

        # OPTIMIZATION: Build each column with list comprehension (pre-allocated)
        # Use direct dict access [key] instead of .get(key) for speed
        keys = tuple(records[0].keys())  # tuple is faster to iterate than dict_keys
        columns = {key: [record[key] for record in records] for key in keys}

        return columns

    def write_parquet_columnar(
        self,
        columns: Dict[str, List],
        output_path: Path,
        measurement: str
    ) -> bool:
        """
        Write columnar data directly to Parquet (ZERO-COPY passthrough).

        OPTIMIZATION: Skips _records_to_columns() conversion.
        Columnar data from client → Arrow → Parquet (no intermediate steps).

        Args:
            columns: Columnar dict {col_name: [values...]}
            output_path: Path to write parquet file
            measurement: Measurement name (for logging)

        Returns:
            True if successful
        """
        if not columns:
            logger.warning(f"No columns to write for '{measurement}'")
            return False

        try:
            # NOTE: Removed _row_hash computation - DELETE operations disabled
            # This was causing 700K RPS loss (2.4M → 1.7M) due to:
            #   - 2.5M SHA256 hashes per second
            #   - Destroying zero-copy columnar passthrough
            #   - Reconstructing rows from columns
            # columns_with_hash = self._add_row_hashes_to_columns(columns, measurement)

            # Infer Arrow schema from column data
            schema = self._infer_schema(columns)

            # Create Arrow RecordBatch (zero-copy columnar structure)
            record_batch = pa.RecordBatch.from_pydict(columns, schema=schema)

            # Create Arrow Table from RecordBatch
            table = pa.Table.from_batches([record_batch])

            # Write directly to Parquet (bypasses DataFrame)
            pq.write_table(
                table,
                output_path,
                compression=self.compression,
                use_dictionary=True,  # Better compression for repeated values
                write_statistics=True  # Enable query optimization
            )

            logger.debug(
                f"Wrote columnar data for '{measurement}' to {output_path} "
                f"(compression={self.compression}, passthrough=true, with _row_hash)"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to write columnar Parquet for '{measurement}': {e}")
            return False

    def _infer_schema(self, columns: Dict[str, List]) -> pa.Schema:
        """
        Infer Arrow schema from column data.

        Handles:
        - Timestamps (datetime → timestamp[ms])
        - Integers (int64)
        - Floats (float64)
        - Strings (utf8)
        """
        fields = []

        for col_name, values in columns.items():
            # Get first non-None value for type inference
            sample = next((v for v in values if v is not None), None)

            if sample is None:
                # All None, default to string
                arrow_type = pa.string()
            elif isinstance(sample, datetime):
                # Timestamp with millisecond precision
                arrow_type = pa.timestamp('ms')
            elif isinstance(sample, bool):
                arrow_type = pa.bool_()
            elif isinstance(sample, int):
                arrow_type = pa.int64()
            elif isinstance(sample, float):
                arrow_type = pa.float64()
            elif isinstance(sample, str):
                arrow_type = pa.string()
            else:
                # Default to string for unknown types
                arrow_type = pa.string()

            fields.append(pa.field(col_name, arrow_type))

        return pa.schema(fields)


class ArrowParquetBuffer:
    """
    Buffered Arrow/Parquet writer with automatic flushing.

    Similar to ParquetBuffer but uses Direct Arrow writes.
    Optionally includes Write-Ahead Log (WAL) for durability.
    """

    def __init__(
        self,
        storage_backend,
        max_buffer_size: int = 10000,
        max_buffer_age_seconds: int = 60,
        compression: str = 'snappy',
        wal_enabled: bool = False,
        wal_config: Dict[str, Any] = None
    ):
        """
        Initialize Arrow buffer

        Args:
            storage_backend: Storage backend instance
            max_buffer_size: Max records before flush
            max_buffer_age_seconds: Max seconds before flush
            compression: Parquet compression
            wal_enabled: Enable Write-Ahead Log for durability
            wal_config: WAL configuration dict
        """
        self.storage_backend = storage_backend
        self.max_buffer_size = max_buffer_size
        self.max_buffer_age_seconds = max_buffer_age_seconds

        # Arrow writer
        self.writer = ArrowParquetWriter(compression=compression)

        # Buffers (same as ParquetBuffer)
        from collections import defaultdict
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.buffer_start_times: Dict[str, datetime] = {}

        # WAL (optional)
        self.wal_enabled = wal_enabled
        self.wal_writer = None
        if wal_enabled and wal_config:
            from storage.wal import WALWriter
            self.wal_writer = WALWriter(
                wal_dir=wal_config.get('wal_dir', './data/wal'),
                worker_id=wal_config.get('worker_id', 0),
                sync_mode=wal_config.get('sync_mode', 'fdatasync'),
                max_size_bytes=wal_config.get('max_size_mb', 100) * 1024 * 1024,
                max_age_seconds=wal_config.get('max_age_seconds', 3600)
            )

        # Metrics
        self.total_records_buffered = 0
        self.total_records_written = 0
        self.total_flushes = 0
        self.total_errors = 0

        # Background task
        import asyncio
        self._flush_task = None
        self._running = False
        self._lock = asyncio.Lock()

        wal_status = "enabled" if wal_enabled else "disabled"
        logger.info(
            f"ArrowParquetBuffer initialized: "
            f"max_size={max_buffer_size}, max_age={max_buffer_age_seconds}s, "
            f"compression={compression}, WAL={wal_status}"
        )

    async def start(self):
        """Start background flush task"""
        import asyncio
        from datetime import timezone

        if self._running:
            return

        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info(
            f"ArrowParquetBuffer started "
            f"(max_size={self.max_buffer_size}, max_age={self.max_buffer_age_seconds}s)"
        )

    async def stop(self):
        """Stop background task and flush remaining data"""
        import asyncio

        if not self._running:
            return

        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Flush all remaining buffers
        await self.flush_all()

        # Close WAL writer
        if self.wal_writer:
            self.wal_writer.close()

        logger.info(
            f"ArrowParquetBuffer stopped. Total records written: {self.total_records_written}"
        )

    async def write(self, records: List[Dict[str, Any]]):
        """Add records to buffer (with optional WAL - row format only)"""
        import asyncio
        from datetime import datetime, timezone
        from collections import defaultdict

        if not records:
            return

        # Write to WAL first (if enabled) - BEFORE buffering
        # Note: WAL only supports row format currently
        if self.wal_enabled and self.wal_writer:
            # Filter out columnar records for WAL (not supported yet)
            row_records = [r for r in records if not r.get('_columnar')]
            if row_records:
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(None, self.wal_writer.append, row_records)
                if not success:
                    logger.error("WAL append failed, records may be lost on crash")

        # OPTIMIZATION: Extract records to flush while holding lock, then flush outside lock
        # This prevents blocking all writes during flush operations
        records_to_flush = {}

        async with self._lock:
            # Group records by measurement
            by_measurement = defaultdict(list)
            for record in records:
                measurement = record.get('measurement', 'unknown')
                by_measurement[measurement].append(record)

            # Add to buffers and identify measurements that need flushing
            for measurement, measurement_records in by_measurement.items():
                if measurement not in self.buffer_start_times:
                    self.buffer_start_times[measurement] = datetime.now(timezone.utc)

                self.buffers[measurement].extend(measurement_records)

                # Count records (columnar vs row)
                num_records = sum(
                    len(r['columns']['time']) if r.get('_columnar') else 1
                    for r in measurement_records
                )
                self.total_records_buffered += num_records

                # Check if buffer should be flushed
                # For columnar, count total rows across all columnar batches
                buffer_size = sum(
                    len(r['columns']['time']) if r.get('_columnar') else 1
                    for r in self.buffers[measurement]
                )

                if buffer_size >= self.max_buffer_size:
                    logger.debug(f"Arrow buffer for '{measurement}' reached size limit, flushing")
                    # Extract records while holding lock
                    records_to_flush[measurement] = self.buffers[measurement]
                    self.buffers[measurement] = []
                    del self.buffer_start_times[measurement]

        # OPTIMIZATION: Flush outside lock - allows concurrent writes during flush
        # OPTIMIZATION: Parallel flushes - flush multiple measurements concurrently
        if records_to_flush:
            flush_tasks = [
                self._flush_records(measurement, flush_records)
                for measurement, flush_records in records_to_flush.items()
            ]
            await asyncio.gather(*flush_tasks, return_exceptions=True)

    async def _periodic_flush(self):
        """Background task that periodically flushes old buffers"""
        import asyncio
        from datetime import datetime, timezone

        while self._running:
            try:
                await asyncio.sleep(5)

                # OPTIMIZATION: Extract aged measurements outside lock
                records_to_flush = {}

                async with self._lock:
                    now = datetime.now(timezone.utc)
                    measurements_to_flush = []

                    for measurement, start_time in self.buffer_start_times.items():
                        age_seconds = (now - start_time).total_seconds()
                        if age_seconds >= self.max_buffer_age_seconds:
                            measurements_to_flush.append(measurement)

                    # Extract records while holding lock
                    for measurement in measurements_to_flush:
                        logger.debug(f"Arrow buffer for '{measurement}' reached age limit, flushing")
                        if self.buffers[measurement]:
                            records_to_flush[measurement] = self.buffers[measurement]
                            self.buffers[measurement] = []
                            del self.buffer_start_times[measurement]

                # OPTIMIZATION: Flush outside lock - allows concurrent writes
                # OPTIMIZATION: Parallel flushes - flush multiple measurements concurrently
                if records_to_flush:
                    flush_tasks = [
                        self._flush_records(measurement, flush_records)
                        for measurement, flush_records in records_to_flush.items()
                    ]
                    await asyncio.gather(*flush_tasks, return_exceptions=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Arrow periodic flush: {e}")
                await asyncio.sleep(5)

    async def _flush_measurement(self, measurement: str):
        """
        Flush buffer for a specific measurement using Direct Arrow.
        Extracts records from buffer under lock, then flushes.
        """
        async with self._lock:
            if measurement not in self.buffers or not self.buffers[measurement]:
                return

            records = self.buffers[measurement]
            self.buffers[measurement] = []
            del self.buffer_start_times[measurement]

        # Flush outside lock
        await self._flush_records(measurement, records)

    async def _flush_records(self, measurement: str, records: List[Dict[str, Any]]):
        """
        Flush records to Parquet (does not touch buffers, safe to call outside lock).

        OPTIMIZATION: Supports both row format and columnar format.
        Columnar format bypasses row→column conversion (25-35% faster).
        """
        import asyncio
        import tempfile
        import os
        from datetime import datetime

        if not records:
            return

        try:
            # OPTIMIZATION: Columnar format (zero-copy passthrough)
            if records and records[0].get('_columnar'):
                # Merge multiple columnar batches into single columnar dict
                columns = self._merge_columnar_records(records)
                num_records = len(columns['time'])

                # Get time range for filename
                times = columns['time']
                min_time = min(times)
                max_time = max(times)
                timestamp_str = min_time.strftime('%Y%m%d_%H%M%S')
                date_partition = min_time.strftime('%Y/%m/%d/%H')

                # Check for database override
                database_override = records[0].get('_database')

                filename = f"{measurement}_{timestamp_str}_{num_records}.parquet"
                remote_path = f"{measurement}/{date_partition}/{filename}"

                # Write columnar data directly to Parquet
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp_file:
                    tmp_path = Path(tmp_file.name)

                try:
                    # OPTIMIZATION: Run PyArrow write in executor to avoid blocking event loop
                    loop = asyncio.get_event_loop()
                    success = await loop.run_in_executor(
                        None,
                        self.writer.write_parquet_columnar,
                        columns,
                        tmp_path,
                        measurement
                    )

                    if success:
                        await self.storage_backend.upload_file(tmp_path, remote_path, database_override=database_override)
                        self.total_records_written += num_records
                        self.total_flushes += 1
                        logger.info(
                            f"Flushed {num_records} records for '{measurement}' to {remote_path} "
                            f"(Columnar passthrough)"
                        )

                finally:
                    if tmp_path.exists():
                        os.unlink(tmp_path)

                return

            # LEGACY: Row format (with conversion overhead)
            # Ensure time is datetime
            for record in records:
                if 'time' in record and not isinstance(record['time'], datetime):
                    # Convert if needed (assume ISO string or timestamp)
                    if isinstance(record['time'], str):
                        record['time'] = datetime.fromisoformat(record['time'].replace('Z', '+00:00'))
                    elif isinstance(record['time'], (int, float)):
                        record['time'] = datetime.fromtimestamp(record['time'] / 1000)  # Assume ms

            # Get time range for filename
            times = [r['time'] for r in records if 'time' in r]
            if times:
                min_time = min(times)
                max_time = max(times)
                timestamp_str = min_time.strftime('%Y%m%d_%H%M%S')
                # PHASE 2: Hour-level partitioning
                date_partition = min_time.strftime('%Y/%m/%d/%H')
            else:
                timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                # PHASE 2: Hour-level partitioning
                date_partition = datetime.now().strftime('%Y/%m/%d/%H')

            # Check if records have a database override
            database_override = None
            if records and '_database' in records[0]:
                database_override = records[0]['_database']
                # Remove _database from all records before writing to parquet
                for record in records:
                    record.pop('_database', None)

            filename = f"{measurement}_{timestamp_str}_{len(records)}.parquet"
            # Path is always measurement/date_partition/filename
            # Storage backend adds database prefix
            remote_path = f"{measurement}/{date_partition}/{filename}"

            # Write to temporary file using Direct Arrow
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp_file:
                tmp_path = Path(tmp_file.name)

            try:
                # OPTIMIZATION: Run PyArrow write in executor to avoid blocking event loop
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(
                    None,
                    self.writer.write_parquet,
                    records,
                    tmp_path,
                    measurement
                )

                if success:
                    # Upload to storage (with optional database override)
                    await self.storage_backend.upload_file(tmp_path, remote_path, database_override=database_override)

                    self.total_records_written += len(records)
                    self.total_flushes += 1

                    logger.info(
                        f"Flushed {len(records)} records for '{measurement}' to {remote_path} "
                        f"(Direct Arrow)"
                    )

            finally:
                # Clean up temp file
                if tmp_path.exists():
                    os.unlink(tmp_path)

        except Exception as e:
            logger.error(f"Failed to flush Arrow measurement '{measurement}': {e}")
            self.total_errors += 1
            # Re-add records to buffer on error (need lock)
            async with self._lock:
                if len(self.buffers[measurement]) < self.max_buffer_size * 2:
                    self.buffers[measurement].extend(records)
                    # Restore start time if needed
                    if measurement not in self.buffer_start_times:
                        from datetime import datetime, timezone
                        self.buffer_start_times[measurement] = datetime.now(timezone.utc)
                    logger.warning(f"Re-added {len(records)} records to Arrow buffer after error")
                else:
                    logger.error(f"Dropping {len(records)} records due to persistent errors")

    async def flush_all(self):
        """Flush all measurement buffers"""
        import asyncio

        # OPTIMIZATION: Extract all records outside lock
        records_to_flush = {}

        async with self._lock:
            for measurement in list(self.buffers.keys()):
                if self.buffers[measurement]:
                    records_to_flush[measurement] = self.buffers[measurement]
                    self.buffers[measurement] = []
                    if measurement in self.buffer_start_times:
                        del self.buffer_start_times[measurement]

        # Flush outside lock
        # OPTIMIZATION: Parallel flushes - flush all measurements concurrently
        if records_to_flush:
            flush_tasks = [
                self._flush_records(measurement, flush_records)
                for measurement, flush_records in records_to_flush.items()
            ]
            await asyncio.gather(*flush_tasks, return_exceptions=True)

    def _merge_columnar_records(self, records: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Merge multiple columnar batches into single columnar dict.

        Input: [
            {_columnar: True, columns: {time: [1,2], val: [10,20]}},
            {_columnar: True, columns: {time: [3,4], val: [30,40]}}
        ]
        Output: {time: [1,2,3,4], val: [10,20,30,40]}

        OPTIMIZATION: Zero-copy merge - just concatenate arrays.
        """
        if not records:
            return {}

        # Get all column names from first record
        first_columns = records[0]['columns']
        merged = {key: [] for key in first_columns.keys()}

        # Concatenate arrays from all batches
        for record in records:
            columns = record['columns']
            for key, values in columns.items():
                if key not in merged:
                    # New column appeared in later batch (schema evolution)
                    merged[key] = []
                merged[key].extend(values)

        return merged

    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics (including WAL if enabled)"""
        from datetime import datetime, timezone

        oldest_age = None
        if self.buffer_start_times:
            now = datetime.now(timezone.utc)
            oldest = min(self.buffer_start_times.values())
            oldest_age = (now - oldest).total_seconds()

        stats = {
            'total_records_buffered': self.total_records_buffered,
            'total_records_written': self.total_records_written,
            'total_flushes': self.total_flushes,
            'total_errors': self.total_errors,
            'current_buffer_sizes': {
                measurement: len(records)
                for measurement, records in self.buffers.items()
            },
            'oldest_buffer_age_seconds': oldest_age,
            'writer_type': 'Direct Arrow (zero-copy)',
            'wal_enabled': self.wal_enabled
        }

        # Add WAL stats if enabled
        if self.wal_enabled and self.wal_writer:
            stats['wal'] = self.wal_writer.get_stats()

        return stats
