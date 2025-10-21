"""
Compaction API Routes for Arc Core

Provides API endpoints for monitoring and managing compaction jobs.
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/compaction", tags=["compaction"])

# Global references (initialized at startup)
compaction_manager = None
compaction_scheduler = None


def init_compaction(manager, scheduler):
    """Initialize global compaction references"""
    global compaction_manager, compaction_scheduler
    compaction_manager = manager
    compaction_scheduler = scheduler


@router.get("/status")
async def get_compaction_status():
    """
    Get current compaction status

    Returns scheduler status and active jobs
    """
    if not compaction_scheduler:
        raise HTTPException(status_code=503, detail="Compaction not initialized")

    return {
        "scheduler": compaction_scheduler.get_status(),
        "manager": {
            "active_jobs": len(compaction_manager.active_jobs),
            "total_completed": compaction_manager.total_jobs_completed,
            "total_failed": compaction_manager.total_jobs_failed
        }
    }


@router.get("/stats")
async def get_compaction_stats():
    """
    Get detailed compaction statistics

    Returns comprehensive stats about compaction performance
    """
    if not compaction_manager:
        raise HTTPException(status_code=503, detail="Compaction not initialized")

    return compaction_manager.get_stats()


@router.get("/candidates")
async def get_compaction_candidates():
    """
    Get list of partitions eligible for compaction

    Returns list of candidates without triggering compaction
    """
    if not compaction_manager:
        raise HTTPException(status_code=503, detail="Compaction not initialized")

    candidates = await compaction_manager.find_candidates()

    return {
        "count": len(candidates),
        "candidates": candidates
    }


@router.post("/trigger")
async def trigger_compaction():
    """
    Manually trigger compaction cycle

    Runs compaction immediately regardless of schedule
    """
    if not compaction_scheduler:
        raise HTTPException(status_code=503, detail="Compaction not initialized")

    await compaction_scheduler.trigger_now()

    return {
        "message": "Compaction triggered",
        "status": "running"
    }


@router.get("/jobs")
async def get_active_jobs():
    """
    Get currently active compaction jobs

    Returns list of running jobs with progress info
    """
    if not compaction_manager:
        raise HTTPException(status_code=503, detail="Compaction not initialized")

    jobs = [
        job.get_stats()
        for job in compaction_manager.active_jobs.values()
    ]

    return {
        "active_jobs": len(jobs),
        "jobs": jobs
    }


@router.get("/history")
async def get_job_history(limit: int = 10):
    """
    Get recent compaction job history

    Args:
        limit: Number of recent jobs to return (default: 10)

    Returns:
        Recent job history with stats
    """
    if not compaction_manager:
        raise HTTPException(status_code=503, detail="Compaction not initialized")

    history = compaction_manager.job_history[-limit:]

    return {
        "total_jobs": len(compaction_manager.job_history),
        "recent_jobs": history
    }
