"""
Telemetry Collector for Arc Time-Series Database

Collects anonymous system information for telemetry purposes.
This helps the Arc team understand usage patterns and improve the database.

Data collected:
- Instance ID: Random, persistent identifier (not tied to hardware)
- OS: Operating system name and version
- CPU Count: Number of CPU cores
- RAM: Total system memory in GB
- Arc Version: Current version of Arc

All data is sent in readable JSON format for transparency.
Telemetry can be disabled in arc.conf by setting telemetry.enabled = false
"""

import json
import logging
import os
import platform
import psutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class TelemetryCollector:
    """Collects system information for telemetry"""

    def __init__(self, data_dir: str = "./data"):
        """
        Initialize telemetry collector

        Args:
            data_dir: Directory to store instance ID file
        """
        self.data_dir = Path(data_dir)
        self.instance_id_file = self.data_dir / ".instance_id"
        self.instance_id = self._get_or_create_instance_id()

    def _get_or_create_instance_id(self) -> str:
        """
        Get or create a persistent instance ID

        Returns:
            Unique instance identifier (UUID)
        """
        try:
            # Ensure data directory exists
            self.data_dir.mkdir(parents=True, exist_ok=True)

            # Try to read existing instance ID
            if self.instance_id_file.exists():
                instance_id = self.instance_id_file.read_text().strip()
                if instance_id:
                    logger.info(f"Loaded existing instance ID: {instance_id[:8]}...")
                    return instance_id

            # Generate new instance ID
            instance_id = str(uuid.uuid4())
            self.instance_id_file.write_text(instance_id)
            logger.info(f"Generated new instance ID: {instance_id[:8]}...")
            return instance_id

        except Exception as e:
            logger.error(f"Failed to get/create instance ID: {e}")
            # Return a temporary ID if file operations fail
            return str(uuid.uuid4())

    def _get_os_info(self) -> Dict[str, str]:
        """
        Get operating system information

        Returns:
            Dictionary with OS name, version, and architecture
        """
        try:
            return {
                "name": platform.system(),
                "version": platform.release(),
                "architecture": platform.machine(),
                "platform": platform.platform()
            }
        except Exception as e:
            logger.error(f"Failed to collect OS info: {e}")
            return {
                "name": "unknown",
                "version": "unknown",
                "architecture": "unknown",
                "platform": "unknown"
            }

    def _get_cpu_info(self) -> Dict[str, Any]:
        """
        Get CPU information

        Returns:
            Dictionary with CPU count and details
        """
        try:
            return {
                "physical_cores": psutil.cpu_count(logical=False),
                "logical_cores": psutil.cpu_count(logical=True),
                "frequency_mhz": psutil.cpu_freq().max if psutil.cpu_freq() else None
            }
        except Exception as e:
            logger.error(f"Failed to collect CPU info: {e}")
            return {
                "physical_cores": None,
                "logical_cores": None,
                "frequency_mhz": None
            }

    def _get_memory_info(self) -> Dict[str, float]:
        """
        Get memory information

        Returns:
            Dictionary with total RAM in GB
        """
        try:
            mem = psutil.virtual_memory()
            return {
                "total_gb": round(mem.total / (1024 ** 3), 2)
            }
        except Exception as e:
            logger.error(f"Failed to collect memory info: {e}")
            return {
                "total_gb": None
            }

    def _get_arc_version(self) -> str:
        """
        Get Arc version from version file or default

        Returns:
            Arc version string
        """
        try:
            version_file = Path(__file__).parent.parent / "VERSION"
            if version_file.exists():
                return version_file.read_text().strip()
            return "dev"
        except Exception as e:
            logger.error(f"Failed to read version: {e}")
            return "unknown"

    def collect(self) -> Dict[str, Any]:
        """
        Collect all telemetry data

        Returns:
            Dictionary containing all telemetry information
        """
        telemetry_data = {
            "instance_id": self.instance_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "arc_version": self._get_arc_version(),
            "os": self._get_os_info(),
            "cpu": self._get_cpu_info(),
            "memory": self._get_memory_info()
        }

        logger.info(f"Collected telemetry data for instance {self.instance_id[:8]}...")
        return telemetry_data

    def to_json(self) -> str:
        """
        Get telemetry data as JSON string

        Returns:
            JSON string of telemetry data
        """
        return json.dumps(self.collect(), indent=2)

    def to_readable_string(self) -> str:
        """
        Get telemetry data as human-readable string

        Returns:
            Formatted string with telemetry information
        """
        data = self.collect()
        lines = [
            "=== Arc Telemetry Data ===",
            f"Instance ID: {data['instance_id']}",
            f"Timestamp: {data['timestamp']}",
            f"Arc Version: {data['arc_version']}",
            "",
            "Operating System:",
            f"  Name: {data['os']['name']}",
            f"  Version: {data['os']['version']}",
            f"  Architecture: {data['os']['architecture']}",
            f"  Platform: {data['os']['platform']}",
            "",
            "CPU:",
            f"  Physical Cores: {data['cpu']['physical_cores']}",
            f"  Logical Cores: {data['cpu']['logical_cores']}",
            f"  Max Frequency: {data['cpu']['frequency_mhz']} MHz" if data['cpu']['frequency_mhz'] else "  Max Frequency: N/A",
            "",
            "Memory:",
            f"  Total RAM: {data['memory']['total_gb']} GB",
            "=========================="
        ]
        return "\n".join(lines)


if __name__ == "__main__":
    # Test the collector
    collector = TelemetryCollector()
    print(collector.to_readable_string())
    print("\nJSON Format:")
    print(collector.to_json())
