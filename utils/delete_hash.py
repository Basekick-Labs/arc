"""
Utilities for computing stable row fingerprints used by delete/tombstone logic.

The goal is to generate a deterministic string (and corresponding hash) that
uniquely identifies a row based on measurement, timestamp, and tag/field values
without requiring heavy per-row reconstruction work in the hot ingest path.
"""

from __future__ import annotations

import zlib
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, Sequence, Optional

DEFAULT_HASH_ALGORITHM = "crc32"

# Keys that should never be part of the fingerprint payload
_EXCLUDED_KEYS = {"measurement", "time", "fields", "_row_hash"}


def _value_to_bytes(value) -> bytes:
    """Convert values to stable byte representations suitable for hashing."""
    if value is None:
        return b""

    if isinstance(value, datetime):
        # Use microsecond epoch for compact canonical representation
        epoch_us = int(value.timestamp() * 1_000_000)
        return str(epoch_us).encode("utf-8")

    if isinstance(value, bool):
        return b"1" if value else b"0"

    if isinstance(value, bytes):
        return value

    if isinstance(value, (int, float)):
        # Avoid locale-specific formatting; repr() keeps precision for floats
        return repr(value).encode("utf-8")

    # Numpy scalars expose .item() to convert to python primitives
    if hasattr(value, "item") and callable(value.item):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, str):
        return value.encode("utf-8")

    # Fallback to string representation for unsupported types
    return str(value).encode("utf-8")


def _digest(payload: str, algorithm: str) -> str:
    algorithm = (algorithm or DEFAULT_HASH_ALGORITHM).lower()
    encoded = payload.encode("utf-8")

    if algorithm == "crc32":
        return f"{zlib.crc32(encoded) & 0xFFFFFFFF:08x}"
    if algorithm == "sha256":
        return hashlib.sha256(encoded).hexdigest()

    raise ValueError(f"Unsupported delete hash algorithm: {algorithm}")


def _iter_identifier_pairs(row: Mapping[str, object]) -> Iterable[tuple[str, object]]:
    """Yield (key, value) pairs for fields that participate in the fingerprint."""
    for key in sorted(row.keys()):
        if key in _EXCLUDED_KEYS or key.startswith("_"):
            continue
        value = row.get(key)
        if isinstance(value, (list, tuple, set, dict)):
            # Complex structures are rare in metric tags; skip to avoid heavy serialization
            continue
        yield key, value


def hash_row_dict(
    row: Mapping[str, object],
    measurement: str,
    algorithm: str = DEFAULT_HASH_ALGORITHM,
) -> str:
    """
    Compute the fingerprint hash for a single row dictionary.

    Args:
        row: Row dictionary (flattened) including columns.
        measurement: Measurement name fallback if row["measurement"] is absent.
        algorithm: Hash algorithm name ("crc32" or "sha256").
    """
    measurement_value = row.get("measurement", measurement)
    algorithm = (algorithm or DEFAULT_HASH_ALGORITHM).lower()

    if algorithm == "crc32":
        return _hash_crc32_row(row, measurement_value)
    if algorithm == "sha256":
        # Lazy import to avoid hashlib overhead when unused
        import hashlib

        digest = hashlib.sha256()
        digest.update(_value_to_bytes(row.get("time")))
        digest.update(b"|")
        digest.update(_value_to_bytes(measurement_value))

        for key, value in _iter_identifier_pairs(row):
            digest.update(b"|")
            digest.update(key.encode("utf-8"))
            digest.update(b"=")
            digest.update(_value_to_bytes(value))

        return digest.hexdigest()

    raise ValueError(f"Unsupported delete hash algorithm: {algorithm}")


def _hash_crc32_row(row: Mapping[str, object], measurement_value: Optional[str]) -> str:
    """CRC32 helper for row dictionaries."""
    checksum = 0
    checksum = zlib.crc32(_value_to_bytes(row.get("time")), checksum)
    checksum = zlib.crc32(_value_to_bytes(measurement_value), checksum)

    for key, value in _iter_identifier_pairs(row):
        checksum = zlib.crc32(b"|", checksum)
        checksum = zlib.crc32(key.encode("utf-8"), checksum)
        checksum = zlib.crc32(b"=", checksum)
        checksum = zlib.crc32(_value_to_bytes(value), checksum)

    return f"{checksum & 0xFFFFFFFF:08x}"


def hash_rows_dicts(
    rows: Iterable[Mapping[str, object]],
    algorithm: str = DEFAULT_HASH_ALGORITHM,
) -> List[str]:
    """Compute hashes for a list of row dictionaries."""
    return [hash_row_dict(row, row.get("measurement"), algorithm) for row in rows]


def hash_columns(
    columns: Mapping[str, Sequence[object]],
    measurement: str,
    algorithm: str = DEFAULT_HASH_ALGORITHM,
) -> List[str]:
    """
    Vectorized hash computation for columnar data represented as dict of lists.

    Args:
        columns: Columnar mapping (column -> sequence of values)
        measurement: Measurement name for this batch
        algorithm: Hash algorithm to use

    Returns:
        List of hex digest strings (length == number of rows)
    """
    if not columns:
        return []

    # Determine row count from first column
    first_col = next(iter(columns.values()))
    row_count = len(first_col)

    algorithm = (algorithm or DEFAULT_HASH_ALGORITHM).lower()

    if algorithm != "crc32":
        # Fall back to per-row path for non-CRC algorithms (only sha256 today)
        return [
            hash_row_dict(
                {key: column[idx] for key, column in columns.items()}, measurement, algorithm
            )
            for idx in range(row_count)
        ]

    time_column = columns.get("time")
    measurement_column = columns.get("measurement")

    identifier_keys = [
        key
        for key in sorted(columns.keys())
        if key not in _EXCLUDED_KEYS
        and not key.startswith("_")
        and key != "measurement"
    ]
    key_prefix_bytes = {key: key.encode("utf-8") + b"=" for key in identifier_keys}

    hashes: List[str] = []
    append_hash = hashes.append
    for idx in range(row_count):
        checksum = 0
        if time_column:
            checksum = zlib.crc32(_value_to_bytes(time_column[idx]), checksum)
        else:
            checksum = zlib.crc32(b"", checksum)

        checksum = zlib.crc32(
            _value_to_bytes(
                measurement_column[idx] if measurement_column else measurement
            ),
            checksum,
        )

        for key in identifier_keys:
            value = columns[key][idx]
            if isinstance(value, (list, tuple, set, dict)):
                # Skip complex types to avoid heavy serialization
                continue
            checksum = zlib.crc32(b"|", checksum)
            checksum = zlib.crc32(key_prefix_bytes[key], checksum)
            checksum = zlib.crc32(_value_to_bytes(value), checksum)

        append_hash(f"{checksum & 0xFFFFFFFF:08x}")

    return hashes
