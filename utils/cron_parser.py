"""
Simple Cron Expression Parser

Minimal, dependency-free cron parser for Arc's scheduling needs.
Replaces abandoned croniter library.

Supports standard cron format: minute hour day month weekday
Example: "5 * * * *" = every hour at 5 minutes past
"""

from datetime import datetime, timedelta
from typing import List, Set
import re


class CronParseError(Exception):
    """Error parsing cron expression"""
    pass


class CronExpression:
    """
    Parse and evaluate cron expressions.

    Supports:
    - Numbers: 5
    - Ranges: 1-5
    - Lists: 1,3,5
    - Steps: */5 or 1-10/2
    - Wildcards: *
    """

    def __init__(self, expression: str):
        """
        Parse cron expression

        Args:
            expression: Cron string (minute hour day month weekday)
                       Example: "5 * * * *" = every hour at 5 minutes past
        """
        self.expression = expression
        parts = expression.strip().split()

        if len(parts) != 5:
            raise CronParseError(
                f"Invalid cron expression '{expression}'. "
                f"Expected 5 fields (minute hour day month weekday), got {len(parts)}"
            )

        try:
            self.minutes = self._parse_field(parts[0], 0, 59)
            self.hours = self._parse_field(parts[1], 0, 23)
            self.days = self._parse_field(parts[2], 1, 31)
            self.months = self._parse_field(parts[3], 1, 12)
            self.weekdays = self._parse_field(parts[4], 0, 6)  # 0=Sunday
        except Exception as e:
            raise CronParseError(f"Invalid cron expression '{expression}': {e}")

    def _parse_field(self, field: str, min_val: int, max_val: int) -> Set[int]:
        """
        Parse a single cron field.

        Supports: numbers, ranges (1-5), lists (1,3,5), steps (*/5, 1-10/2), wildcards (*)
        """
        result = set()

        # Handle wildcard
        if field == '*':
            return set(range(min_val, max_val + 1))

        # Split on commas for lists
        for part in field.split(','):
            # Handle steps (*/5 or 1-10/2)
            if '/' in part:
                range_part, step = part.split('/', 1)
                step = int(step)

                if range_part == '*':
                    start, end = min_val, max_val
                elif '-' in range_part:
                    start_str, end_str = range_part.split('-', 1)
                    start, end = int(start_str), int(end_str)
                else:
                    raise CronParseError(f"Invalid step syntax: {part}")

                result.update(range(start, end + 1, step))

            # Handle ranges (1-5)
            elif '-' in part:
                start_str, end_str = part.split('-', 1)
                start, end = int(start_str), int(end_str)
                if start < min_val or end > max_val:
                    raise CronParseError(
                        f"Range {start}-{end} out of bounds ({min_val}-{max_val})"
                    )
                result.update(range(start, end + 1))

            # Handle single number
            else:
                num = int(part)
                if num < min_val or num > max_val:
                    raise CronParseError(
                        f"Value {num} out of bounds ({min_val}-{max_val})"
                    )
                result.add(num)

        return result

    def next_execution_time(self, from_time: datetime) -> datetime:
        """
        Calculate next execution time from given datetime.

        Args:
            from_time: Calculate next run after this time

        Returns:
            Next execution datetime
        """
        # Start from next minute
        current = from_time.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # Search for next matching time (max 4 years to prevent infinite loop)
        max_iterations = 366 * 24 * 60 * 4  # 4 years of minutes

        for _ in range(max_iterations):
            if self._matches(current):
                return current
            current += timedelta(minutes=1)

        raise RuntimeError(
            f"Could not find next execution time for cron '{self.expression}' "
            f"within 4 years from {from_time}"
        )

    def _matches(self, dt: datetime) -> bool:
        """Check if datetime matches cron expression"""
        return (
            dt.minute in self.minutes and
            dt.hour in self.hours and
            dt.day in self.days and
            dt.month in self.months and
            dt.weekday() in {(d + 1) % 7 for d in self.weekdays}  # Convert Sun=0 to Mon=0
        )

    def __repr__(self):
        return f"CronExpression('{self.expression}')"


def parse_cron(expression: str) -> CronExpression:
    """
    Parse cron expression (convenience function).

    Args:
        expression: Cron string (minute hour day month weekday)

    Returns:
        CronExpression instance

    Raises:
        CronParseError: If expression is invalid
    """
    return CronExpression(expression)
