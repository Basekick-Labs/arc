"""
Arc Ingestion Module

Handles various data ingestion formats and protocols.
"""

from .line_protocol_parser import LineProtocolParser
from .parquet_buffer import ParquetBuffer

__all__ = ['LineProtocolParser', 'ParquetBuffer']
