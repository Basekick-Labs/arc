"""
Compaction lock manager for Arc.
Manages locks for compaction jobs using SQLite to prevent concurrent compaction of the same partition.
"""
import os
import sqlite3
import time
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

class CompactionLock:
    """
    Manages locks for compaction jobs using SQLite.
    Prevents concurrent compaction of the same partition.
    """

    def __init__(self, db_path: str = None):
        """
        Initialize compaction lock manager

        Args:
            db_path: Path to SQLite database (defaults to arc.db from config)
        """
        if db_path is None:
            from api.config import get_db_path
            db_path = get_db_path()

        self.db_path = db_path
        self._init_table()

    def _init_table(self):
        """Initialize compaction_locks table with retry logic for concurrent workers"""
        max_retries = 5
        retry_delay = 0.1  # 100ms

        for attempt in range(max_retries):
            try:
                # Use timeout to handle concurrent access from multiple workers
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS compaction_locks (
                            partition_path TEXT PRIMARY KEY,
                            worker_id INTEGER NOT NULL,
                            locked_at TIMESTAMP NOT NULL,
                            expires_at TIMESTAMP NOT NULL
                        )
                    ''')

                    conn.commit()

                logger.debug("Compaction locks table initialized")
                return

            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.debug(f"Database locked during init, retrying ({attempt + 1}/{max_retries})...")
                    import time
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(f"Failed to initialize compaction_locks table after {attempt + 1} attempts: {e}")
                    raise
            except Exception as e:
                logger.error(f"Failed to initialize compaction_locks table: {e}")
                raise

    def acquire_lock(self, partition_path: str, ttl_hours: int = 2) -> bool:
        """
        Try to acquire lock for a partition

        Args:
            partition_path: Partition path (e.g., 'cpu/2025/10/08/14')
            ttl_hours: Lock time-to-live in hours (for crash recovery)

        Returns:
            True if lock acquired, False otherwise
        """
        from datetime import timedelta

        worker_id = os.getpid()
        now = datetime.now()
        expires = now + timedelta(hours=ttl_hours)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Try to insert lock
                cursor.execute('''
                    INSERT INTO compaction_locks
                    (partition_path, worker_id, locked_at, expires_at)
                    VALUES (?, ?, ?, ?)
                ''', (partition_path, worker_id, now, expires))

                conn.commit()

            logger.debug(f"Acquired lock for {partition_path} (worker {worker_id})")
            return True

        except sqlite3.IntegrityError:
            # Lock already exists, check if expired
            return self._check_and_steal_expired(partition_path, worker_id, ttl_hours)

        except Exception as e:
            logger.error(f"Failed to acquire lock for {partition_path}: {e}")
            return False

    def _check_and_steal_expired(
        self,
        partition_path: str,
        worker_id: int,
        ttl_hours: int
    ) -> bool:
        """
        Check if existing lock is expired and steal it if so

        Args:
            partition_path: Partition path
            worker_id: Current worker ID
            ttl_hours: TTL for new lock

        Returns:
            True if lock stolen and acquired
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Delete expired locks
                cursor.execute('''
                    DELETE FROM compaction_locks
                    WHERE partition_path = ?
                    AND expires_at < ?
                ''', (partition_path, datetime.now()))

                deleted = cursor.rowcount
                conn.commit()

            if deleted > 0:
                logger.info(
                    f"Stole expired lock for {partition_path}, retrying acquisition"
                )
                # Expired lock removed, try to acquire again
                return self.acquire_lock(partition_path, ttl_hours)

            # Lock not expired
            logger.debug(f"Lock for {partition_path} is held by another worker")
            return False

        except Exception as e:
            logger.error(f"Failed to check/steal expired lock for {partition_path}: {e}")
            return False

    def release_lock(self, partition_path: str):
        """
        Release lock for a partition

        Args:
            partition_path: Partition path
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    DELETE FROM compaction_locks
                    WHERE partition_path = ?
                ''', (partition_path,))

                conn.commit()

            logger.debug(f"Released lock for {partition_path}")

        except Exception as e:
            logger.error(f"Failed to release lock for {partition_path}: {e}")

    def get_active_locks(self) -> List[Dict[str, Any]]:
        """
        Get all active locks

        Returns:
            List of lock info dicts
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT * FROM compaction_locks
                    WHERE expires_at > ?
                    ORDER BY locked_at DESC
                ''', (datetime.now(),))

                locks = [dict(row) for row in cursor.fetchall()]

            return locks

        except Exception as e:
            logger.error(f"Failed to get active locks: {e}")
            return []

    def cleanup_expired_locks(self) -> int:
        """
        Cleanup all expired locks

        Returns:
            Number of locks cleaned up
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    DELETE FROM compaction_locks
                    WHERE expires_at < ?
                ''', (datetime.now(),))

                deleted = cursor.rowcount
                conn.commit()

            if deleted > 0:
                logger.info(f"Cleaned up {deleted} expired compaction locks")

            return deleted

        except Exception as e:
            logger.error(f"Failed to cleanup expired locks: {e}")
            return 0

