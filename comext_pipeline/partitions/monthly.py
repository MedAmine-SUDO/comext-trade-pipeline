"""
Monthly partition set for COMEXT data.

Each partition key is a YYYY-MM-DD string (first of month, e.g. "2024-01-01")
as produced by Dagster's MonthlyPartitionsDefinition. Helper functions convert
between this and the YYYYMM format used in COMEXT filenames.
"""

from datetime import date, timedelta

from dagster import MonthlyPartitionsDefinition

# Covers all months from the earliest COMEXT data (2002-01) up to today.
MONTHLY_PARTITIONS: MonthlyPartitionsDefinition = MonthlyPartitionsDefinition(
    start_date="2002-01-01"
)


def partition_key_to_period(partition_key: str) -> str:
    """
    Convert a Dagster partition key ("2024-01-01") to a COMEXT period string ("202401").
    """
    if len(partition_key) != 10 or partition_key[4] != "-" or partition_key[7] != "-":
        raise ValueError(f"Expected partition key in YYYY-MM-DD format, got: {partition_key!r}")
    return partition_key[:4] + partition_key[5:7]


def period_to_partition_key(period: str) -> str:
    """
    Convert a COMEXT period string ("202401") to a Dagster partition key ("2024-01-01").

    Raises ValueError if the period is not exactly 6 characters.
    """
    if len(period) != 6:
        raise ValueError(f"Expected 6-character period string, got: {period!r}")
    return f"{period[:4]}-{period[4:]}-01"


def latest_n_partition_keys(n: int) -> list[str]:
    """
    Return the Dagster partition keys for the most recent ``n`` months
    in ascending chronological order (oldest first).

    Used by the sensor to determine the rolling revision window.
    Returns an empty list if ``n`` is zero or negative.
    """
    if n <= 0:
        return []

    today = date.today()
    keys: list[str] = []

    current = date(today.year, today.month, 1)
    for _ in range(n):
        keys.append(current.strftime("%Y-%m-%d"))
        # Step back one month: subtract a day then reset to the 1st
        current = (current - timedelta(days=1)).replace(day=1)

    return list(reversed(keys))
