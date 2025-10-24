"""
Arc Telemetry Module

Collects and sends anonymous usage telemetry to help improve Arc.
All data collected is transparent and can be disabled in arc.conf.
"""

from telemetry.collector import TelemetryCollector
from telemetry.sender import TelemetrySender

__all__ = ['TelemetryCollector', 'TelemetrySender']
