"""
Configuration helpers for Arc
"""
import os

# Cache for database path (set during startup migration)
_db_path_cache = None


def get_db_path() -> str:
    """
    Get database path from environment or use default.

    Returns path suitable for current environment (Docker or native).
    Path is always in the data folder for compute/storage separation.
    """
    global _db_path_cache

    # Return cached path if available (set during startup migration)
    if _db_path_cache:
        return _db_path_cache

    # Try environment variable first
    db_path = os.getenv("DB_PATH")
    if db_path:
        return db_path

    # Check if running in Docker (presence of /.dockerenv)
    if os.path.exists("/.dockerenv"):
        return "/app/data/arc.db"

    # Native execution - always use data folder
    return "./data/arc.db"


def set_db_path(path: str):
    """
    Set database path (called by startup migration)

    Args:
        path: Database path after migration
    """
    global _db_path_cache
    _db_path_cache = path


def get_log_dir() -> str:
    """
    Get log directory path from environment or use default.
    """
    log_dir = os.getenv("LOG_DIR")
    if log_dir:
        return log_dir

    if os.path.exists("/.dockerenv"):
        return "/app/logs"

    return "./logs"
