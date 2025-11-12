import asyncio
import aiofiles
from pathlib import Path
from typing import List, Optional
import logging
import duckdb

logger = logging.getLogger(__name__)

class LocalBackend:
    """Local disk storage backend for maximum write performance testing

    Files are organized by database:
    {base_path}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
    """

    def __init__(self, base_path: str = None, bucket: str = None, database: str = "default"):
        # Support both base_path and bucket for backward compatibility
        path = base_path or bucket
        if not path:
            raise ValueError("Either base_path or bucket must be provided")

        self.base_path = Path(path).resolve()  # Resolve to absolute path
        self.database = database

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Local storage backend initialized at {self.base_path} for database '{self.database}'")

    def _validate_safe_path(self, key: str, database: str) -> Path:
        """
        Validate that the constructed path stays within the base directory.
        This prevents path traversal attacks using ../ or absolute paths.

        Args:
            key: The key/path relative to database directory
            database: The database name

        Returns:
            Resolved absolute path that's safe to use

        Raises:
            ValueError: If path traversal is detected
        """
        # Construct the target path
        target = self.base_path / database / key

        # Resolve to absolute path (resolves .. and symlinks)
        resolved = target.resolve()

        # Check that resolved path is still under base_path
        try:
            resolved.relative_to(self.base_path)
        except ValueError:
            raise ValueError(
                f"Path traversal detected: '{key}' attempts to escape base directory"
            )

        return resolved

    async def upload_file(self, local_path: Path, key: str, database_override: str = None) -> bool:
        """Copy file to local storage location asynchronously

        Files are stored under: {base_path}/{database}/{key}

        Args:
            local_path: Path to the source file
            key: Destination key/path relative to database directory
            database_override: Optional database name to use instead of self.database
        """
        try:
            # Use override database if provided, otherwise use default
            database = database_override or self.database

            # Validate path to prevent traversal attacks
            dest_path = self._validate_safe_path(key, database)
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Read and write file asynchronously
            async with aiofiles.open(local_path, 'rb') as src:
                file_data = await src.read()

            async with aiofiles.open(dest_path, 'wb') as dst:
                await dst.write(file_data)

            logger.info(f"Copied {local_path} to {dest_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to copy {local_path}: {e}")
            return False

    async def upload_parquet_files(self, local_dir: Path, measurement: str) -> int:
        """Upload all parquet files for a measurement with partitioned structure"""
        # Look for partitioned files in measurement subdirectories
        parquet_files = list(local_dir.glob(f"{measurement}/**/*.parquet"))

        # Also look for files with measurement prefix in root directory
        root_files = list(local_dir.glob(f"{measurement}_*.parquet"))
        parquet_files.extend(root_files)

        if not parquet_files:
            logger.warning(f"No parquet files found for {measurement}")
            return 0

        logger.info(f"Copying {len(parquet_files)} files for {measurement}")

        # Copy files with high concurrency
        semaphore = asyncio.Semaphore(50)  # Higher concurrency for local disk

        async def copy_with_semaphore(file_path):
            async with semaphore:
                # Preserve the relative path structure from local_dir
                key = str(file_path.relative_to(local_dir))
                return await self.upload_file(file_path, key)

        # Execute copies concurrently
        tasks = [copy_with_semaphore(file_path) for file_path in parquet_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        logger.info(f"Copied {success_count}/{len(parquet_files)} files to local storage")

        return success_count

    def get_s3_path(self, measurement: str, year: int, month: int, day: int, hour: int = None) -> str:
        """Generate local path (compatible with DuckDB local file reading)

        Path structure: {base_path}/{database}/{measurement}_{year}_{month}_{day}_{hour}.parquet
        """
        if hour is not None:
            return f"{self.base_path}/{self.database}/{measurement}_{year}_{month:02d}_{day:02d}_{hour:02d}.parquet"
        else:
            return f"{self.base_path}/{self.database}/{measurement}_{year}_{month:02d}_*.parquet"

    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> List[str]:
        """List parquet files in the base path with optional prefix filter

        This method follows the same pattern as MinIO/S3 backends:
        1. Searches within the database directory
        2. Applies optional prefix filter within the database
        3. Returns paths relative to the database (not base_path)

        Args:
            prefix: Optional prefix to filter results within database (e.g., "cpu/")
            max_keys: Maximum number of keys to return (for compatibility with S3 backends)

        Returns:
            List of relative paths to parquet files (relative to database, not base_path)
        """
        try:
            objects = []

            # Build search path: base_path/database/prefix
            # This matches MinIO behavior: database prefix is automatically added
            full_prefix = f"{self.database}/{prefix}" if prefix else f"{self.database}/"
            search_path = self.base_path / full_prefix

            if not search_path.exists():
                logger.debug(f"Search path does not exist: {search_path}")
                return []

            # Find all parquet files under the search path
            for file_path in search_path.rglob("*.parquet"):
                # Get relative path from base_path
                relative_to_base = str(file_path.relative_to(self.base_path))

                # Strip database prefix (like MinIO does)
                db_prefix = f"{self.database}/"
                if relative_to_base.startswith(db_prefix):
                    relative_to_db = relative_to_base[len(db_prefix):]
                    objects.append(relative_to_db)

                # Respect max_keys limit
                if len(objects) >= max_keys:
                    break

            logger.debug(f"Found {len(objects)} parquet files in database '{self.database}' with prefix '{prefix}'")
            return objects

        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    def download_file(self, key: str, local_path: str):
        """
        'Download' file from storage to local path.

        For local backend, files are already on the filesystem.
        This method creates a symlink instead of copying to save I/O.

        Args:
            key: Storage key (relative to database)
            local_path: Destination path for the file
        """
        try:
            # Validate source path to prevent traversal attacks
            source_path = self._validate_safe_path(key, self.database)

            if not source_path.exists():
                raise FileNotFoundError(f"Source file not found: {source_path}")

            # Convert local_path to Path object
            dest_path = Path(local_path)

            # If destination already exists and points to source, we're done
            if dest_path.exists():
                if dest_path.resolve() == source_path.resolve():
                    logger.debug(f"File already accessible at: {dest_path}")
                    return
                # Remove existing symlink/file
                dest_path.unlink()

            # Ensure destination directory exists
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Create symlink instead of copying (much faster!)
            dest_path.symlink_to(source_path.resolve())
            logger.debug(f"Created symlink {dest_path} -> {source_path}")

        except Exception as e:
            logger.error(f"Failed to create symlink for {key}: {e}")
            raise

    def delete_file(self, key: str):
        """
        Delete file from local storage

        Args:
            key: Storage key (relative to database)
        """
        try:
            # Build file path: base_path/database/key
            file_path = self.base_path / self.database / key

            if not file_path.exists():
                logger.warning(f"File to delete not found: {file_path}")
                return

            # Delete file
            file_path.unlink()
            logger.debug(f"Deleted file: {file_path}")

        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")
            raise

    async def configure_duckdb_s3(self, duckdb_conn):
        """Configure DuckDB for local file access (no special config needed)"""
        logger.info("Local storage backend - DuckDB will use direct file access")
        # No special configuration needed for local files
        pass
