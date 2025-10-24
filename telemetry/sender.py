"""
Telemetry Sender for Arc Time-Series Database

Sends telemetry data to telemetry.basekick.net every 24 hours.
Handles retries, network failures gracefully, and respects user opt-out.
"""

import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from telemetry.collector import TelemetryCollector

logger = logging.getLogger(__name__)


class TelemetrySender:
    """Sends telemetry data periodically"""

    def __init__(
        self,
        collector: TelemetryCollector,
        endpoint: str = "https://telemetry.basekick.net/api/v1/telemetry",
        interval_hours: int = 24,
        enabled: bool = True
    ):
        """
        Initialize telemetry sender

        Args:
            collector: TelemetryCollector instance
            endpoint: URL to send telemetry data to
            interval_hours: Hours between telemetry sends (default: 24)
            enabled: Whether telemetry is enabled
        """
        self.collector = collector
        self.endpoint = endpoint
        self.interval_hours = interval_hours
        self.enabled = enabled
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def send_telemetry(self) -> bool:
        """
        Send telemetry data to the telemetry server

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            logger.info("Telemetry is disabled, skipping send")
            return False

        try:
            telemetry_data = self.collector.collect()

            logger.info(f"Sending telemetry data for instance {telemetry_data['instance_id'][:8]}...")
            logger.debug(f"Telemetry endpoint: {self.endpoint}")
            logger.debug(f"Telemetry data: {telemetry_data}")

            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.endpoint,
                    json=telemetry_data,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": f"Arc/{telemetry_data['arc_version']}"
                    }
                ) as response:
                    if response.status == 200:
                        logger.info(f"Successfully sent telemetry data to {self.endpoint}")
                        return True
                    else:
                        response_text = await response.text()
                        logger.warning(
                            f"Failed to send telemetry: HTTP {response.status} - {response_text}"
                        )
                        return False

        except aiohttp.ClientError as e:
            logger.warning(f"Network error sending telemetry: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending telemetry: {e}", exc_info=True)
            return False

    async def _periodic_sender(self):
        """Background task that sends telemetry periodically"""
        logger.info(
            f"Telemetry sender started (interval: {self.interval_hours}h, endpoint: {self.endpoint})"
        )

        # Send initial telemetry on startup (with a small delay to ensure Arc is fully started)
        await asyncio.sleep(60)  # Wait 1 minute after startup
        await self.send_telemetry()

        # Then send periodically
        interval_seconds = self.interval_hours * 3600
        while not self._stop_event.is_set():
            try:
                # Wait for the interval or until stop is requested
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval_seconds
                )
                # If we get here, stop was requested
                break
            except asyncio.TimeoutError:
                # Timeout means it's time to send telemetry
                await self.send_telemetry()

        logger.info("Telemetry sender stopped")

    def start(self):
        """Start the periodic telemetry sender"""
        if not self.enabled:
            logger.info("Telemetry is disabled, not starting sender")
            return

        if self._task is not None and not self._task.done():
            logger.warning("Telemetry sender is already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._periodic_sender())
        logger.info("Telemetry sender task created")

    async def stop(self):
        """Stop the periodic telemetry sender"""
        if self._task is None or self._task.done():
            logger.info("Telemetry sender is not running")
            return

        logger.info("Stopping telemetry sender...")
        self._stop_event.set()

        try:
            await asyncio.wait_for(self._task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Telemetry sender did not stop gracefully, cancelling")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Telemetry sender stopped")


async def test_telemetry():
    """Test function to send telemetry immediately"""
    collector = TelemetryCollector()
    sender = TelemetrySender(collector, enabled=True)

    print("Collecting telemetry data...")
    print(collector.to_readable_string())

    print("\nSending telemetry data...")
    success = await sender.send_telemetry()

    if success:
        print("✓ Telemetry sent successfully!")
    else:
        print("✗ Failed to send telemetry")


if __name__ == "__main__":
    # Test the sender
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_telemetry())
