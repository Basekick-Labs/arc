"""
MessagePack Binary Protocol Decoder

High-performance decoder for Arc's binary protocol.
10-15x faster than Line Protocol text parsing.
"""

import msgpack
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class MessagePackDecoder:
    """
    Decode MessagePack binary protocol to Arc internal format.

    Supported formats:
    1. Single measurement:  {m: "cpu", t: 1633024800000, h: "server01", fields: {...}}
    2. Batch: {batch: [{m: "cpu", ...}, {m: "mem", ...}]}
    3. Compact: {m: 1, t: 1633024800000, h: 12345, f: [95.0, 3.2]}
    """

    def __init__(self):
        self.total_decoded = 0
        self.total_errors = 0

    def decode(self, data: bytes) -> List[Dict[str, Any]]:
        """
        Decode MessagePack binary data to list of records.

        Args:
            data: MessagePack binary payload

        Returns:
            List of flat dictionaries suitable for Arrow/Parquet
        """
        try:
            # Unpack MessagePack binary
            obj = msgpack.unpackb(data, raw=False)

            # Handle batch vs single
            if isinstance(obj, dict):
                if 'batch' in obj:
                    # Batch format
                    records = self._decode_batch(obj['batch'])
                else:
                    # Single measurement
                    records = [self._decode_single(obj)]
            elif isinstance(obj, list):
                # Array of measurements
                records = [self._decode_single(item) for item in obj]
            else:
                raise ValueError(f"Invalid MessagePack format: {type(obj)}")

            self.total_decoded += len(records)
            return records

        except Exception as e:
            self.total_errors += 1
            logger.error(f"Failed to decode MessagePack: {e}")
            raise ValueError(f"Invalid MessagePack payload: {e}")

    def _decode_single(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decode single measurement to Arc internal format.

        Input formats:
        1. Standard: {m: "cpu", t: 1633024800000, h: "server01", fields: {...}}
        2. Compact:  {m: 1, t: 1633024800000, h: 12345, f: [95.0, 3.2]}
        """
        # Extract measurement
        measurement = obj.get('m')
        if measurement is None:
            raise ValueError("Missing required field 'm' (measurement)")

        # Handle measurement ID (compact format) vs name
        if isinstance(measurement, int):
            measurement = f"measurement_{measurement}"  # TODO: lookup from registry

        # Extract timestamp (milliseconds)
        timestamp_ms = obj.get('t')
        if timestamp_ms is None:
            # Use current time if not provided
            timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Convert to datetime
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

        # Extract host
        host = obj.get('h', 'unknown')
        if isinstance(host, int):
            host = f"host_{host}"  # TODO: lookup from registry

        # Extract fields
        fields = obj.get('fields') or obj.get('f')
        if not fields:
            raise ValueError("Missing required field 'fields' or 'f'")

        # Handle array fields (compact format)
        if isinstance(fields, list):
            # TODO: Use schema to map array positions to field names
            fields = {f"field_{i}": v for i, v in enumerate(fields)}

        # Extract tags (optional)
        tags = obj.get('tags', {})

        # Build flat record for Arrow/Parquet
        record = {
            'measurement': measurement,
            'time': timestamp,
            'host': host
        }

        # Add tags directly (no prefix)
        for tag_key, tag_value in tags.items():
            record[tag_key] = tag_value

        # Add fields directly (no prefix)
        for field_key, field_value in fields.items():
            record[field_key] = field_value

        return record

    def _decode_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Decode batch of measurements."""
        return [self._decode_single(item) for item in batch]

    def get_stats(self) -> Dict[str, Any]:
        """Get decoder statistics."""
        return {
            'total_decoded': self.total_decoded,
            'total_errors': self.total_errors,
            'error_rate': (
                self.total_errors / self.total_decoded
                if self.total_decoded > 0 else 0
            )
        }


def decode_msgpack_payload(data: bytes) -> List[Dict[str, Any]]:
    """
    Convenience function to decode MessagePack payload.

    Args:
        data: MessagePack binary payload

    Returns:
        List of records ready for Arrow/Parquet
    """
    decoder = MessagePackDecoder()
    return decoder.decode(data)
