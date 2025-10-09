"""
Write-Ahead Log (WAL) for Arc Core

Provides durability guarantees by persisting data to disk before acknowledging writes.
Uses binary format with checksums for fast writes and reliable recovery.

Architecture:
- Per-worker WAL files (no lock contention)
- Binary format with MessagePack payload
- Automatic recovery on startup
- Configurable sync modes (fsync, fdatasync, async)

Performance:
- fsync mode:     ~800K records/sec (strict durability)
- fdatasync mode: ~1.2M records/sec (balanced, default)
- async mode:     ~1.7M records/sec (performance-first)
"""

import os
import struct
import hashlib
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import msgpack
import asyncio

logger = logging.getLogger(__name__)

# WAL file format constants
WAL_MAGIC = b'ARCW'           # Magic bytes
WAL_VERSION = 0x0001          # Version 1
WAL_CHECKSUM_CRC32 = 0x01    # CRC32 checksum type

# Entry format:
# [Length: 4 bytes] [Timestamp: 8 bytes] [Checksum: 4 bytes] [Payload: N bytes]
WAL_ENTRY_HEADER_SIZE = 16


class WALWriter:
    """
    Write-Ahead Log writer with configurable durability modes.

    Each worker process has its own WAL file to avoid lock contention.
    """

    def __init__(
        self,
        wal_dir: str,
        worker_id: int,
        sync_mode: str = 'fdatasync',
        max_size_bytes: int = 100 * 1024 * 1024,  # 100MB default
        max_age_seconds: int = 3600  # 1 hour default
    ):
        """
        Initialize WAL writer

        Args:
            wal_dir: Directory for WAL files
            worker_id: Unique worker ID (from Gunicorn worker)
            sync_mode: 'fsync', 'fdatasync', or 'async'
            max_size_bytes: Rotate WAL when it reaches this size
            max_age_seconds: Rotate WAL after this many seconds
        """
        self.wal_dir = Path(wal_dir)
        self.worker_id = worker_id
        self.sync_mode = sync_mode
        self.max_size_bytes = max_size_bytes
        self.max_age_seconds = max_age_seconds

        # Create WAL directory
        self.wal_dir.mkdir(parents=True, exist_ok=True)

        # Current WAL file
        self.current_file: Optional[Path] = None
        self.current_fd: Optional[int] = None
        self.current_size = 0
        self.current_start_time = datetime.now()

        # Metrics
        self.total_entries = 0
        self.total_bytes = 0
        self.total_syncs = 0
        self.total_rotations = 0

        # Initialize first WAL file
        self._rotate_wal()

        logger.info(
            f"WAL writer initialized: worker={worker_id}, mode={sync_mode}, "
            f"max_size={max_size_bytes/1024/1024:.1f}MB, dir={wal_dir}"
        )

    def _rotate_wal(self):
        """Create a new WAL file (rotation)"""
        # Close current file
        if self.current_fd is not None:
            self._sync()
            os.close(self.current_fd)

        # Generate new filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"worker-{self.worker_id}-{timestamp}.wal"
        self.current_file = self.wal_dir / filename

        # Open new file with direct I/O flags for better fsync performance
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if hasattr(os, 'O_DSYNC') and self.sync_mode == 'fdatasync':
            flags |= os.O_DSYNC  # Hardware-level data sync

        self.current_fd = os.open(str(self.current_file), flags, 0o644)
        self.current_size = 0
        self.current_start_time = datetime.now()
        self.total_rotations += 1

        # Write WAL header
        header = struct.pack('>4sHB', WAL_MAGIC, WAL_VERSION, WAL_CHECKSUM_CRC32)
        os.write(self.current_fd, header)
        self.current_size += len(header)

        logger.info(f"WAL rotated: {filename}")

    def append(self, records: List[Dict[str, Any]]) -> bool:
        """
        Append records to WAL

        Args:
            records: List of record dictionaries

        Returns:
            True if successfully written and synced
        """
        try:
            # Serialize records with MessagePack (convert datetime to ISO strings)
            payload = msgpack.packb(records, use_bin_type=True, default=self._serialize_datetime)
            payload_len = len(payload)

            # Calculate checksum (CRC32)
            checksum = self._crc32(payload)

            # Get current timestamp (microseconds since epoch)
            timestamp_us = int(datetime.now().timestamp() * 1_000_000)

            # Build entry: [length][timestamp][checksum][payload]
            entry = struct.pack('>IQI', payload_len, timestamp_us, checksum) + payload

            # Write to WAL file
            bytes_written = os.write(self.current_fd, entry)
            self.current_size += bytes_written

            # Sync based on mode
            self._sync()

            # Update metrics
            self.total_entries += 1
            self.total_bytes += bytes_written
            self.total_syncs += 1

            # Check if rotation needed
            age_seconds = (datetime.now() - self.current_start_time).total_seconds()
            if self.current_size >= self.max_size_bytes or age_seconds >= self.max_age_seconds:
                self._rotate_wal()

            return True

        except Exception as e:
            logger.error(f"WAL append failed: {e}")
            return False

    def _sync(self):
        """Sync WAL file to disk based on sync_mode"""
        if self.sync_mode == 'fsync':
            # Full sync: data + metadata
            os.fsync(self.current_fd)
        elif self.sync_mode == 'fdatasync':
            # Data sync only (faster, metadata async)
            if hasattr(os, 'fdatasync'):
                os.fdatasync(self.current_fd)
            else:
                os.fsync(self.current_fd)  # Fallback on systems without fdatasync
        elif self.sync_mode == 'async':
            # No explicit sync, rely on OS buffer cache
            pass
        else:
            raise ValueError(f"Invalid sync_mode: {self.sync_mode}")

    def _serialize_datetime(self, obj):
        """
        Custom serializer for datetime objects in MessagePack.
        Converts datetime to ISO format string.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def _crc32(self, data: bytes) -> int:
        """Calculate CRC32 checksum"""
        import zlib
        return zlib.crc32(data) & 0xffffffff

    def close(self):
        """Close WAL file"""
        if self.current_fd is not None:
            self._sync()
            os.close(self.current_fd)
            self.current_fd = None
            logger.info(f"WAL closed: {self.current_file}")

    def get_stats(self) -> Dict[str, Any]:
        """Get WAL statistics"""
        age_seconds = (datetime.now() - self.current_start_time).total_seconds()
        return {
            'worker_id': self.worker_id,
            'current_file': str(self.current_file),
            'current_size_mb': self.current_size / 1024 / 1024,
            'current_age_seconds': age_seconds,
            'sync_mode': self.sync_mode,
            'total_entries': self.total_entries,
            'total_bytes': self.total_bytes,
            'total_syncs': self.total_syncs,
            'total_rotations': self.total_rotations
        }


class WALReader:
    """
    WAL file reader for recovery operations.

    Reads WAL files and yields batches of records for replay.
    """

    def __init__(self, wal_file: Path):
        """
        Initialize WAL reader

        Args:
            wal_file: Path to WAL file
        """
        self.wal_file = wal_file
        self.total_entries = 0
        self.total_bytes = 0
        self.corrupted_entries = 0

    def read_all(self) -> List[List[Dict[str, Any]]]:
        """
        Read all entries from WAL file

        Returns:
            List of record batches (each batch is a list of records)
        """
        batches = []

        try:
            with open(self.wal_file, 'rb') as f:
                # Read and verify header
                header = f.read(7)
                if len(header) < 7:
                    logger.warning(f"WAL file too short: {self.wal_file}")
                    return batches

                magic, version, checksum_type = struct.unpack('>4sHB', header)

                if magic != WAL_MAGIC:
                    logger.error(f"Invalid WAL magic: {self.wal_file}")
                    return batches

                if version != WAL_VERSION:
                    logger.warning(f"WAL version mismatch: {version} != {WAL_VERSION}")

                # Read entries
                while True:
                    # Read entry header
                    entry_header = f.read(WAL_ENTRY_HEADER_SIZE)
                    if len(entry_header) < WAL_ENTRY_HEADER_SIZE:
                        break  # End of file

                    payload_len, timestamp_us, checksum = struct.unpack('>IQI', entry_header)

                    # Read payload
                    payload = f.read(payload_len)
                    if len(payload) < payload_len:
                        logger.warning(f"Truncated WAL entry at {f.tell()}")
                        break

                    # Verify checksum
                    computed_checksum = self._crc32(payload)
                    if computed_checksum != checksum:
                        logger.error(
                            f"WAL checksum mismatch at {f.tell()}: "
                            f"expected {checksum}, got {computed_checksum}"
                        )
                        self.corrupted_entries += 1
                        continue  # Skip corrupted entry

                    # Deserialize records
                    try:
                        records = msgpack.unpackb(payload, raw=False)
                        batches.append(records)
                        self.total_entries += 1
                        self.total_bytes += WAL_ENTRY_HEADER_SIZE + payload_len
                    except Exception as e:
                        logger.error(f"WAL deserialization failed: {e}")
                        self.corrupted_entries += 1
                        continue

            logger.info(
                f"WAL read complete: {self.wal_file.name} - "
                f"{self.total_entries} entries, {self.total_bytes} bytes, "
                f"{self.corrupted_entries} corrupted"
            )

        except Exception as e:
            logger.error(f"WAL read failed: {self.wal_file}: {e}")

        return batches

    def _crc32(self, data: bytes) -> int:
        """Calculate CRC32 checksum"""
        import zlib
        return zlib.crc32(data) & 0xffffffff


class WALRecovery:
    """
    WAL recovery manager.

    Scans WAL directory on startup and replays all entries.
    """

    def __init__(self, wal_dir: str):
        """
        Initialize WAL recovery

        Args:
            wal_dir: Directory containing WAL files
        """
        self.wal_dir = Path(wal_dir)

    async def recover(self, replay_callback) -> Dict[str, Any]:
        """
        Recover from WAL files

        Args:
            replay_callback: Async function to replay records: async def replay(records)

        Returns:
            Recovery statistics
        """
        if not self.wal_dir.exists():
            logger.info("No WAL directory found, skipping recovery")
            return {'recovered_files': 0, 'recovered_entries': 0}

        # Find all WAL files
        wal_files = sorted(self.wal_dir.glob('worker-*.wal'))

        if not wal_files:
            logger.info("No WAL files found, skipping recovery")
            return {'recovered_files': 0, 'recovered_entries': 0}

        logger.info(f"WAL recovery started: {len(wal_files)} files")

        total_entries = 0
        total_batches = 0
        corrupted_entries = 0

        for wal_file in wal_files:
            logger.info(f"Recovering WAL: {wal_file.name}")

            reader = WALReader(wal_file)
            batches = reader.read_all()

            # Replay batches
            for batch in batches:
                try:
                    await replay_callback(batch)
                    total_batches += 1
                    total_entries += len(batch)
                except Exception as e:
                    logger.error(f"WAL replay failed for batch: {e}")

            corrupted_entries += reader.corrupted_entries

            # Archive recovered WAL file
            archive_file = wal_file.with_suffix('.wal.recovered')
            wal_file.rename(archive_file)
            logger.info(f"WAL archived: {archive_file.name}")

        logger.info(
            f"WAL recovery complete: {total_batches} batches, {total_entries} entries, "
            f"{corrupted_entries} corrupted"
        )

        return {
            'recovered_files': len(wal_files),
            'recovered_batches': total_batches,
            'recovered_entries': total_entries,
            'corrupted_entries': corrupted_entries
        }

    def cleanup_old_wals(self, max_age_seconds: int = 86400):
        """
        Clean up old recovered WAL files

        Args:
            max_age_seconds: Delete WAL files older than this (default 24 hours)
        """
        if not self.wal_dir.exists():
            return

        now = datetime.now().timestamp()
        deleted = 0

        for wal_file in self.wal_dir.glob('*.wal.recovered'):
            file_age = now - wal_file.stat().st_mtime
            if file_age > max_age_seconds:
                wal_file.unlink()
                deleted += 1

        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old WAL files")
