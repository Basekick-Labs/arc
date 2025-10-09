"""
Compaction Scheduler for Arc Core

Schedules and runs compaction jobs based on cron-style configuration.
"""

import logging
import asyncio
from datetime import datetime
from typing import Optional
from croniter import croniter

logger = logging.getLogger(__name__)


class CompactionScheduler:
    """
    Schedules compaction jobs using cron-style schedule strings.
    Runs in background and triggers compaction at specified intervals.
    """

    def __init__(
        self,
        compaction_manager,
        schedule: str = "5 * * * *",
        enabled: bool = True
    ):
        """
        Initialize compaction scheduler

        Args:
            compaction_manager: CompactionManager instance
            schedule: Cron schedule string (minute hour day month weekday)
            enabled: Enable automatic scheduling
        """
        self.compaction_manager = compaction_manager
        self.schedule = schedule
        self.enabled = enabled

        # Validate cron schedule
        try:
            croniter(schedule)
        except Exception as e:
            raise ValueError(f"Invalid cron schedule '{schedule}': {e}")

        # Scheduler state
        self._running = False
        self._task: Optional[asyncio.Task] = None

        logger.info(
            f"Compaction scheduler initialized: "
            f"schedule='{schedule}', enabled={enabled}"
        )

    async def start(self):
        """Start the compaction scheduler"""
        if not self.enabled:
            logger.info("Compaction scheduler is disabled")
            return

        if self._running:
            logger.warning("Compaction scheduler already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_scheduler())

        logger.info(f"Compaction scheduler started (schedule: {self.schedule})")

    async def stop(self):
        """Stop the compaction scheduler"""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Compaction scheduler stopped")

    async def _run_scheduler(self):
        """
        Main scheduler loop - waits for next scheduled time and triggers compaction
        """
        cron = croniter(self.schedule, datetime.now())

        while self._running:
            try:
                # Calculate next run time
                next_run = cron.get_next(datetime)
                now = datetime.now()
                wait_seconds = (next_run - now).total_seconds()

                logger.info(
                    f"Next compaction scheduled for {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"({wait_seconds:.0f}s from now)"
                )

                # Wait until next scheduled time
                await asyncio.sleep(wait_seconds)

                if not self._running:
                    break

                # Run compaction cycle
                logger.info(f"Triggering scheduled compaction at {datetime.now()}")
                await self._run_compaction()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in compaction scheduler: {e}", exc_info=True)
                # Wait a bit before retrying
                await asyncio.sleep(60)

    async def _run_compaction(self):
        """
        Run one compaction cycle
        """
        try:
            start_time = datetime.now()

            await self.compaction_manager.run_compaction_cycle()

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"Compaction cycle completed in {duration:.1f}s")

        except Exception as e:
            logger.error(f"Compaction cycle failed: {e}", exc_info=True)

    async def trigger_now(self):
        """
        Trigger compaction immediately (manual trigger)
        """
        logger.info("Manual compaction trigger")
        await self._run_compaction()

    def get_next_run(self) -> datetime:
        """
        Get next scheduled run time

        Returns:
            Next run datetime
        """
        cron = croniter(self.schedule, datetime.now())
        return cron.get_next(datetime)

    def get_status(self) -> dict:
        """
        Get scheduler status

        Returns:
            Status dict
        """
        return {
            'enabled': self.enabled,
            'running': self._running,
            'schedule': self.schedule,
            'next_run': self.get_next_run().isoformat() if self._running else None
        }
