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

        self.base_path = Path(path)
        self.database = database

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Local storage backend initialized at {self.base_path} for database '{self.database}'")

    async def upload_file(self, local_path: Path, key: str) -> bool:
        """Copy file to local storage location asynchronously

        Files are stored under: {base_path}/{database}/{key}
        """
        try:
            dest_path = self.base_path / self.database / key
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

    def list_objects(self) -> List[str]:
        """List all parquet files in the base path"""
        try:
            objects = []
            for file_path in self.base_path.rglob("*.parquet"):
                relative_path = str(file_path.relative_to(self.base_path))
                objects.append(relative_path)

            logger.info(f"Found {len(objects)} parquet files in local storage")
            return objects

        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    async def configure_duckdb_s3(self, duckdb_conn):
        """Configure DuckDB for local file access (no special config needed)"""
        logger.info("Local storage backend - DuckDB will use direct file access")
        # No special configuration needed for local files
        pass
