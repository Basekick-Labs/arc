"""
Configuration helpers for Arc
"""
import os

def get_db_path() -> str:
    """
    Get database path from environment or use default.

    Returns path suitable for current environment (Docker or native).
    """
    # Try environment variable first
    db_path = os.getenv("DB_PATH")
    if db_path:
        return db_path

    # Check if running in Docker (presence of /.dockerenv)
    if os.path.exists("/.dockerenv"):
        return "/app/data/historian.db"

    # Native execution - use current directory
    return "./data/historian.db"


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
