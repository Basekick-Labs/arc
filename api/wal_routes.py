"""
WAL (Write-Ahead Log) API Endpoints

Provides monitoring and management endpoints for the Write-Ahead Log system.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, List
import logging
from pathlib import Path
import os

logger = logging.getLogger(__name__)

router = APIRouter(tags=["wal"], prefix="/api/v1/wal")


def get_wal_stats_from_buffer() -> Dict[str, Any]:
    """Get WAL statistics from the MessagePack buffer"""
    from api.msgpack_routes import arrow_buffer

    if not arrow_buffer:
        raise HTTPException(status_code=503, detail="Buffer not initialized")

    stats = arrow_buffer.get_stats()

    if not stats.get('wal_enabled', False):
        raise HTTPException(
            status_code=400,
            detail="WAL is not enabled. Set WAL_ENABLED=true in configuration."
        )

    return stats.get('wal', {})


@router.get("/status")
async def get_wal_status():
    """
    Get WAL status and configuration

    Returns:
        - enabled: Whether WAL is enabled
        - configured: WAL configuration details
        - stats: WAL statistics (if enabled)
    """
    from api.msgpack_routes import arrow_buffer

    if not arrow_buffer:
        return {
            "enabled": False,
            "reason": "Buffer not initialized"
        }

    stats = arrow_buffer.get_stats()
    wal_enabled = stats.get('wal_enabled', False)

    response = {
        "enabled": wal_enabled,
        "writer_type": stats.get('writer_type', 'unknown')
    }

    if wal_enabled:
        wal_stats = stats.get('wal', {})
        response["configuration"] = {
            "sync_mode": wal_stats.get('sync_mode', 'unknown'),
            "worker_id": wal_stats.get('worker_id', 0),
            "current_file": wal_stats.get('current_file', 'unknown')
        }
        response["stats"] = wal_stats

    return response


@router.get("/stats")
async def get_wal_stats():
    """
    Get detailed WAL statistics

    Returns:
        - worker_id: Worker process ID
        - current_file: Active WAL file path
        - current_size_mb: Current file size in MB
        - current_age_seconds: Age of current file
        - sync_mode: Sync mode (fsync/fdatasync/async)
        - total_entries: Total entries written
        - total_bytes: Total bytes written
        - total_syncs: Total sync operations
        - total_rotations: Total file rotations
    """
    try:
        return get_wal_stats_from_buffer()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get WAL stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/files")
async def list_wal_files():
    """
    List all WAL files in the WAL directory

    Returns:
        - active: List of active WAL files
        - recovered: List of recovered WAL files (*.wal.recovered)
        - total_size_mb: Total size of all WAL files
    """
    try:
        wal_stats = get_wal_stats_from_buffer()
        wal_dir = Path(wal_stats.get('current_file', '')).parent

        if not wal_dir.exists():
            return {
                "active": [],
                "recovered": [],
                "total_size_mb": 0
            }

        # Find active WAL files
        active_files = []
        for wal_file in sorted(wal_dir.glob('*.wal')):
            stat = wal_file.stat()
            active_files.append({
                "name": wal_file.name,
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified": stat.st_mtime,
                "path": str(wal_file)
            })

        # Find recovered WAL files
        recovered_files = []
        for wal_file in sorted(wal_dir.glob('*.wal.recovered')):
            stat = wal_file.stat()
            recovered_files.append({
                "name": wal_file.name,
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified": stat.st_mtime,
                "path": str(wal_file)
            })

        total_size = sum(f['size_mb'] for f in active_files + recovered_files)

        return {
            "active": active_files,
            "recovered": recovered_files,
            "total_size_mb": round(total_size, 2),
            "wal_directory": str(wal_dir)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list WAL files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cleanup")
async def cleanup_wal_files(max_age_hours: int = 24):
    """
    Clean up old recovered WAL files

    Args:
        max_age_hours: Delete recovered WAL files older than this (default: 24 hours)

    Returns:
        - deleted_count: Number of files deleted
        - freed_mb: Disk space freed in MB
    """
    try:
        wal_stats = get_wal_stats_from_buffer()
        wal_dir = Path(wal_stats.get('current_file', '')).parent

        if not wal_dir.exists():
            return {
                "deleted_count": 0,
                "freed_mb": 0,
                "message": "WAL directory does not exist"
            }

        from datetime import datetime
        import time

        now = time.time()
        max_age_seconds = max_age_hours * 3600
        deleted_count = 0
        freed_bytes = 0

        for wal_file in wal_dir.glob('*.wal.recovered'):
            file_age = now - wal_file.stat().st_mtime
            if file_age > max_age_seconds:
                file_size = wal_file.stat().st_size
                wal_file.unlink()
                deleted_count += 1
                freed_bytes += file_size
                logger.info(f"Deleted old WAL file: {wal_file.name}")

        freed_mb = round(freed_bytes / 1024 / 1024, 2)

        return {
            "deleted_count": deleted_count,
            "freed_mb": freed_mb,
            "max_age_hours": max_age_hours,
            "message": f"Cleaned up {deleted_count} old WAL files, freed {freed_mb} MB"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cleanup WAL files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def wal_health_check():
    """
    Health check for WAL system

    Returns:
        - healthy: Boolean indicating WAL health
        - issues: List of detected issues
        - recommendations: List of recommendations
    """
    try:
        issues = []
        recommendations = []

        # Check if WAL is enabled
        from api.msgpack_routes import arrow_buffer
        if not arrow_buffer:
            return {
                "healthy": False,
                "issues": ["Buffer not initialized"],
                "recommendations": ["Restart Arc Core"]
            }

        stats = arrow_buffer.get_stats()
        wal_enabled = stats.get('wal_enabled', False)

        if not wal_enabled:
            return {
                "healthy": True,
                "wal_enabled": False,
                "message": "WAL is disabled (default behavior)"
            }

        wal_stats = stats.get('wal', {})

        # Check current file size
        current_size_mb = wal_stats.get('current_size_mb', 0)
        if current_size_mb > 90:
            issues.append(f"WAL file near rotation limit: {current_size_mb}MB")
            recommendations.append("Consider reducing WAL_MAX_SIZE_MB or increasing flush frequency")

        # Check file age
        current_age = wal_stats.get('current_age_seconds', 0)
        if current_age > 7200:  # 2 hours
            issues.append(f"WAL file not rotating: age {current_age}s")
            recommendations.append("Check WAL_MAX_AGE_SECONDS configuration")

        # Check disk space
        wal_file = wal_stats.get('current_file', '')
        if wal_file:
            wal_dir = Path(wal_file).parent
            if wal_dir.exists():
                import shutil
                disk_usage = shutil.disk_usage(wal_dir)
                disk_free_pct = (disk_usage.free / disk_usage.total) * 100

                if disk_free_pct < 10:
                    issues.append(f"Low disk space: {disk_free_pct:.1f}% free")
                    recommendations.append("Clean up old WAL files or increase disk space")

        # Check error count
        total_errors = stats.get('total_errors', 0)
        if total_errors > 0:
            issues.append(f"Buffer has {total_errors} errors")
            recommendations.append("Check logs for write failures")

        healthy = len(issues) == 0

        return {
            "healthy": healthy,
            "wal_enabled": True,
            "issues": issues,
            "recommendations": recommendations,
            "stats_summary": {
                "current_size_mb": current_size_mb,
                "current_age_seconds": current_age,
                "total_entries": wal_stats.get('total_entries', 0),
                "total_rotations": wal_stats.get('total_rotations', 0)
            }
        }

    except Exception as e:
        logger.error(f"WAL health check failed: {e}")
        return {
            "healthy": False,
            "issues": [str(e)],
            "recommendations": ["Check Arc Core logs"]
        }


@router.get("/recovery/history")
async def get_recovery_history():
    """
    Get WAL recovery history (from recovered files)

    Returns:
        - recovered_files: List of recovered WAL files
        - last_recovery: Timestamp of last recovery
        - total_recovered: Total number of recovered files
    """
    try:
        wal_stats = get_wal_stats_from_buffer()
        wal_dir = Path(wal_stats.get('current_file', '')).parent

        if not wal_dir.exists():
            return {
                "recovered_files": [],
                "last_recovery": None,
                "total_recovered": 0
            }

        recovered_files = []
        for wal_file in sorted(wal_dir.glob('*.wal.recovered'), key=lambda f: f.stat().st_mtime, reverse=True):
            stat = wal_file.stat()
            recovered_files.append({
                "name": wal_file.name,
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "recovered_at": stat.st_mtime,
                "age_hours": round((os.path.getmtime(wal_file) - stat.st_mtime) / 3600, 2)
            })

        last_recovery = recovered_files[0]['recovered_at'] if recovered_files else None

        return {
            "recovered_files": recovered_files[:10],  # Last 10
            "last_recovery": last_recovery,
            "total_recovered": len(recovered_files)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get recovery history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
