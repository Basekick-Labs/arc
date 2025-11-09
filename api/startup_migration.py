"""
Startup migration utility for Arc

Handles migration of SQLite database and data directories to standardized locations.
Ensures compute/storage separation by moving everything to the data folder.

Migration scenarios:
1. Old location (root): ./arc.db → ./data/arc.db
2. New location (data folder): Already correct
3. Docker: /app/arc.db → /app/data/arc.db
4. Brand new instance: Create in data folder directly
"""

import os
import shutil
import logging
from pathlib import Path
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


class StartupMigration:
    """Handles startup migrations for compute/storage separation"""

    def __init__(self, data_dir: str = None):
        """
        Initialize migration handler

        Args:
            data_dir: Base data directory (default: ./data or /app/data for Docker)
        """
        if data_dir is None:
            if os.path.exists("/.dockerenv"):
                data_dir = "/app/data"
            else:
                data_dir = "./data"

        self.data_dir = Path(data_dir)
        self.db_filename = "arc.db"

    def migrate_database(self) -> Tuple[bool, str]:
        """
        Migrate SQLite database to data folder if needed

        Returns:
            Tuple of (migrated, db_path) where:
            - migrated: True if migration was performed
            - db_path: Final database path
        """
        target_path = self.data_dir / self.db_filename
        migrated = False

        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Check if database already in correct location
        if target_path.exists():
            logger.info(f"Database already in correct location: {target_path}")
            return (False, str(target_path))

        # Check for old locations
        old_locations = [
            Path("./arc.db"),  # Root directory
            Path("../arc.db"),  # Parent directory
        ]

        # Add Docker-specific old location
        if os.path.exists("/.dockerenv"):
            old_locations.append(Path("/app/arc.db"))

        # Try to migrate from old locations
        for old_path in old_locations:
            if old_path.exists():
                try:
                    logger.info(f"Migrating database from {old_path} to {target_path}")

                    # Copy (don't move) for safety
                    shutil.copy2(old_path, target_path)

                    # Verify copy succeeded
                    if target_path.exists():
                        target_size = target_path.stat().st_size
                        old_size = old_path.stat().st_size

                        if target_size == old_size:
                            logger.info(
                                f"✅ Database migrated successfully "
                                f"({target_size:,} bytes)"
                            )

                            # Create backup of old location
                            backup_path = old_path.with_suffix(".db.backup")
                            shutil.move(old_path, backup_path)
                            logger.info(f"Old database backed up to: {backup_path}")

                            migrated = True
                            break
                        else:
                            logger.error(
                                f"Migration verification failed: "
                                f"size mismatch ({old_size} != {target_size})"
                            )
                            target_path.unlink()  # Remove incomplete copy
                    else:
                        logger.error("Migration failed: target file not created")

                except Exception as e:
                    logger.error(f"Failed to migrate database from {old_path}: {e}")
                    if target_path.exists():
                        target_path.unlink()  # Clean up partial copy

        # If no migration happened and target doesn't exist, it's a new instance
        if not target_path.exists():
            logger.info(f"New instance: database will be created at {target_path}")

        return (migrated, str(target_path))

    def migrate_data_directories(self) -> dict:
        """
        Ensure data subdirectories are in the correct location

        Returns:
            Dict of directory paths that were migrated
        """
        subdirs = [
            "arc",  # Parquet data storage
            "compaction",  # Compaction temp files
            "wal",  # Write-ahead log
            "duckdb_tmp",  # DuckDB temp directory
            "cq_temp",  # Continuous queries temp
        ]

        migrated_dirs = {}

        for subdir in subdirs:
            target_path = self.data_dir / subdir

            # Ensure target directory exists
            target_path.mkdir(parents=True, exist_ok=True)

            # Check for old location (root directory)
            old_path = Path(f"./{subdir}")

            if old_path.exists() and old_path != target_path:
                try:
                    logger.info(f"Migrating {subdir}/ from {old_path} to {target_path}")

                    # Move contents (not the directory itself)
                    for item in old_path.iterdir():
                        target_item = target_path / item.name
                        if not target_item.exists():
                            shutil.move(str(item), str(target_item))
                            logger.debug(f"  Moved: {item.name}")

                    # Check if old directory is now empty
                    if not any(old_path.iterdir()):
                        old_path.rmdir()
                        logger.info(f"✅ Migrated {subdir}/ successfully")
                        migrated_dirs[subdir] = str(target_path)
                    else:
                        logger.warning(
                            f"Old {subdir}/ directory not empty after migration"
                        )

                except Exception as e:
                    logger.error(f"Failed to migrate {subdir}/: {e}")

        return migrated_dirs

    def cleanup_old_files(self) -> int:
        """
        Clean up known old files that are no longer needed

        Returns:
            Number of files cleaned up
        """
        cleanup_patterns = [
            "*.db-journal",  # SQLite journal files in old location
            "*.db-wal",  # SQLite WAL files in old location
            "*.db.backup",  # Backup files older than 7 days
        ]

        cleaned = 0

        for pattern in cleanup_patterns:
            for old_file in Path(".").glob(pattern):
                try:
                    # For backup files, only delete if older than 7 days
                    if pattern == "*.db.backup":
                        age_days = (
                            (Path.cwd().stat().st_mtime - old_file.stat().st_mtime)
                            / 86400
                        )
                        if age_days < 7:
                            continue

                    logger.info(f"Cleaning up old file: {old_file}")
                    old_file.unlink()
                    cleaned += 1

                except Exception as e:
                    logger.warning(f"Failed to cleanup {old_file}: {e}")

        if cleaned > 0:
            logger.info(f"✅ Cleaned up {cleaned} old file(s)")

        return cleaned

    def run_full_migration(self) -> dict:
        """
        Run complete startup migration

        Returns:
            Dict with migration results
        """
        logger.info("=" * 60)
        logger.info("Starting Arc Startup Migration")
        logger.info("=" * 60)

        results = {
            "database_migrated": False,
            "database_path": None,
            "directories_migrated": {},
            "files_cleaned": 0,
        }

        # 1. Migrate database
        migrated, db_path = self.migrate_database()
        results["database_migrated"] = migrated
        results["database_path"] = db_path

        # 2. Migrate data directories
        migrated_dirs = self.migrate_data_directories()
        results["directories_migrated"] = migrated_dirs

        # 3. Cleanup old files
        cleaned = self.cleanup_old_files()
        results["files_cleaned"] = cleaned

        # Summary
        logger.info("=" * 60)
        logger.info("Migration Complete")
        logger.info("=" * 60)
        logger.info(f"Database: {db_path}")
        logger.info(f"Database migrated: {migrated}")
        logger.info(f"Directories migrated: {len(migrated_dirs)}")
        logger.info(f"Files cleaned: {cleaned}")
        logger.info("=" * 60)

        return results


def run_startup_migration(data_dir: str = None) -> str:
    """
    Convenience function to run startup migration

    Args:
        data_dir: Base data directory (optional)

    Returns:
        Database path after migration
    """
    migration = StartupMigration(data_dir=data_dir)
    results = migration.run_full_migration()
    return results["database_path"]


if __name__ == "__main__":
    # For testing
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    db_path = run_startup_migration()
    print(f"\nFinal database path: {db_path}")
