"""
InfluxDB Line Protocol Parser

Parses InfluxDB Line Protocol format into structured records.

Line Protocol Format:
    measurement[,tag_key=tag_value...] field_key=field_value[,field_key=field_value...] [timestamp]

Examples:
    cpu,host=server01,region=us-west usage_idle=90.5,usage_system=2.1 1609459200000000000
    temperature,sensor=bedroom temp=22.5
    http_requests,method=GET,status=200 count=1i
"""

import re
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class LineProtocolParser:
    """Parser for InfluxDB Line Protocol format"""

    # Note: We don't use regex patterns anymore for comma splitting
    # because they don't handle quoted strings correctly

    @staticmethod
    def parse_line(line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a single line of InfluxDB Line Protocol

        Args:
            line: Line protocol string

        Returns:
            Dictionary with keys: measurement, tags, fields, timestamp
            Returns None if line is invalid or a comment
        """
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith('#'):
            return None

        try:
            # Split into main components, respecting escaped spaces
            # Line Protocol format: measurement[,tags] fields [timestamp]
            # We need to split on unescaped spaces only
            parts = LineProtocolParser._split_line(line)

            if len(parts) < 2:
                logger.warning(f"Invalid line protocol: {line}")
                return None

            # Debug: log the problematic lines
            if len(parts) >= 3 and 'days,' in parts[2]:
                logger.warning(f"DEBUG - Problematic line parts: {parts}")
                logger.warning(f"DEBUG - Original line: {line}")

            # Parse measurement and tags
            measurement_part = parts[0]
            measurement, tags = LineProtocolParser._parse_measurement_tags(measurement_part)

            # Parse fields
            field_part = parts[1]
            fields = LineProtocolParser._parse_fields(field_part)

            if not fields:
                logger.warning(f"No valid fields found in line: {line}")
                return None

            # Parse timestamp (optional)
            timestamp = None
            if len(parts) >= 3:
                timestamp = LineProtocolParser._parse_timestamp(parts[2])

            # Use current time if no timestamp provided
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)

            return {
                'measurement': measurement,
                'tags': tags,
                'fields': fields,
                'timestamp': timestamp
            }

        except Exception as e:
            logger.error(f"Failed to parse line protocol: {line}. Error: {e}")
            return None

    @staticmethod
    def _split_on_comma(text: str) -> List[str]:
        """
        Split on unescaped commas, respecting quoted strings

        Args:
            text: String to split

        Returns:
            List of parts split by unescaped commas outside quotes
        """
        parts = []
        current = []
        i = 0
        in_quotes = False

        while i < len(text):
            if text[i] == '\\' and i + 1 < len(text):
                # Escaped character
                current.append(text[i:i+2])
                i += 2
            elif text[i] == '"':
                # Toggle quote state
                in_quotes = not in_quotes
                current.append(text[i])
                i += 1
            elif text[i] == ',' and not in_quotes:
                # Unescaped comma outside quotes - separator
                if current:
                    parts.append(''.join(current))
                    current = []
                i += 1
            else:
                current.append(text[i])
                i += 1

        # Add final part
        if current:
            parts.append(''.join(current))

        return parts

    @staticmethod
    def _split_line(line: str) -> List[str]:
        """
        Split line protocol into parts, respecting escaped spaces and quoted strings

        Line Protocol format: measurement[,tags] fields [timestamp]
        Parts are separated by unescaped spaces outside of quoted strings.

        Args:
            line: Line protocol string

        Returns:
            List of parts [measurement_tags, fields, timestamp?]
        """
        parts = []
        current = []
        i = 0
        in_quotes = False

        while i < len(line):
            if line[i] == '\\' and i + 1 < len(line):
                # Escaped character - include both backslash and next char
                current.append(line[i:i+2])
                i += 2
            elif line[i] == '"':
                # Toggle quote state
                in_quotes = not in_quotes
                current.append(line[i])
                i += 1
            elif line[i] == ' ' and not in_quotes:
                # Unescaped space outside quotes - part boundary
                if current:
                    parts.append(''.join(current))
                    current = []
                i += 1
            else:
                current.append(line[i])
                i += 1

        # Add final part
        if current:
            parts.append(''.join(current))

        return parts

    @staticmethod
    def _parse_measurement_tags(part: str) -> Tuple[str, Dict[str, str]]:
        """
        Parse measurement name and tags

        Args:
            part: measurement[,tag=value,...]

        Returns:
            Tuple of (measurement_name, tags_dict)
        """
        # Split on unescaped commas (respecting quotes)
        components = LineProtocolParser._split_on_comma(part)

        measurement = LineProtocolParser._unescape(components[0])
        tags = {}

        for component in components[1:]:
            if '=' in component:
                key, value = component.split('=', 1)
                tags[LineProtocolParser._unescape(key)] = LineProtocolParser._unescape(value)

        return measurement, tags

    @staticmethod
    def _parse_fields(part: str) -> Dict[str, Union[float, int, str, bool]]:
        """
        Parse field set

        Args:
            part: field_key=field_value[,field_key=field_value...]

        Returns:
            Dictionary of field name to value
        """
        fields = {}

        # Split on unescaped commas (respecting quotes)
        field_parts = LineProtocolParser._split_on_comma(part)

        for field_part in field_parts:
            if '=' not in field_part:
                continue

            key, value = field_part.split('=', 1)
            key = LineProtocolParser._unescape(key)

            # Parse field value based on type indicators
            parsed_value = LineProtocolParser._parse_field_value(value)

            if parsed_value is not None:
                fields[key] = parsed_value

        return fields

    @staticmethod
    def _parse_field_value(value: str) -> Optional[Union[float, int, str, bool]]:
        """
        Parse field value based on InfluxDB type indicators

        Type indicators:
            - Integer: ends with 'i' (e.g., 123i)
            - Float: numeric without 'i' (e.g., 123.45)
            - String: wrapped in quotes (e.g., "hello")
            - Boolean: t, T, true, TRUE, f, F, false, FALSE
        """
        value = value.strip()

        # Boolean
        if value.lower() in ('t', 'true'):
            return True
        if value.lower() in ('f', 'false'):
            return False

        # String (quoted)
        if value.startswith('"'):
            if value.endswith('"') and len(value) > 1:
                return LineProtocolParser._unescape(value[1:-1])
            else:
                # Malformed quoted string - treat as string anyway
                logger.warning(f"Malformed quoted string: {value}")
                return value.strip('"')

        # Integer (ends with 'i')
        if value.endswith('i'):
            try:
                return int(value[:-1])
            except ValueError:
                logger.warning(f"Invalid integer value: {value}")
                return None

        # Float/number
        try:
            # Try parsing as float
            return float(value)
        except ValueError:
            # If all else fails, treat as string
            logger.warning(f"Treating unparseable value as string: {value}")
            return value

    @staticmethod
    def _parse_timestamp(timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp from line protocol

        Timestamps in line protocol are in nanoseconds since Unix epoch
        """
        try:
            # Convert nanoseconds to seconds
            timestamp_ns = int(timestamp_str)
            timestamp_s = timestamp_ns / 1_000_000_000
            return datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
        except (ValueError, OSError) as e:
            logger.warning(f"Invalid timestamp: {timestamp_str}. Error: {e}")
            return None

    @staticmethod
    def _unescape(s: str) -> str:
        """
        Unescape special characters in line protocol

        Escaped characters: comma, space, equals sign
        """
        return s.replace('\\,', ',').replace('\\ ', ' ').replace('\\=', '=')

    @classmethod
    def parse_batch(cls, lines: str) -> List[Dict[str, Any]]:
        """
        Parse multiple lines of line protocol

        Args:
            lines: Multi-line string of line protocol

        Returns:
            List of parsed records
        """
        records = []

        for line in lines.split('\n'):
            record = cls.parse_line(line)
            if record:
                records.append(record)

        return records

    @staticmethod
    def to_parquet_schema(records: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Infer Parquet schema from parsed records

        Returns:
            Dictionary mapping column names to types
        """
        schema = {
            'time': 'timestamp',
            'measurement': 'string'
        }

        # Collect all tag and field names
        all_tags = set()
        all_fields = set()

        for record in records:
            all_tags.update(record.get('tags', {}).keys())
            all_fields.update(record.get('fields', {}).keys())

        # Tags are always strings (no prefix)
        for tag in all_tags:
            schema[tag] = 'string'

        # Fields can be various types - infer from first occurrence
        for field in all_fields:
            # Handle conflicts: if field name matches a tag, suffix with _value
            field_name = f'{field}_value' if field in all_tags else field

            for record in records:
                if field in record.get('fields', {}):
                    value = record['fields'][field]
                    if isinstance(value, bool):
                        schema[field_name] = 'boolean'
                    elif isinstance(value, int):
                        schema[field_name] = 'int64'
                    elif isinstance(value, float):
                        schema[field_name] = 'double'
                    else:
                        schema[field_name] = 'string'
                    break

        return schema

    @staticmethod
    def to_flat_dict(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert parsed record to flat dictionary for DataFrame/Parquet

        Args:
            record: Parsed line protocol record

        Returns:
            Flat dictionary with tags and fields as columns
        """
        flat = {
            'time': record['timestamp'],
            'measurement': record['measurement']
        }

        # Add tags (no prefix)
        for tag_key, tag_value in record.get('tags', {}).items():
            flat[tag_key] = str(tag_value)

        # Add fields (no prefix, but handle conflicts with tags)
        for field_key, field_value in record.get('fields', {}).items():
            # If field name conflicts with a tag, suffix with _value
            if field_key in record.get('tags', {}):
                flat[f'{field_key}_value'] = field_value
            else:
                flat[field_key] = field_value

        return flat
