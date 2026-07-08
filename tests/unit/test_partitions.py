"""
Unit tests for partitions/monthly.py: partition_key_to_period,
period_to_partition_key, latest_n_partition_keys, and MonthlyPartitionsDefinition.
"""

from datetime import UTC, date
from unittest.mock import patch

import pytest

from comext_pipeline.partitions.monthly import (
    MONTHLY_PARTITIONS,
    latest_n_partition_keys,
    partition_key_to_period,
    period_to_partition_key,
)


class TestPartitionHelpers:
    def test_partition_key_to_period(self):
        assert partition_key_to_period("2024-01-01") == "202401"
        assert partition_key_to_period("2000-12-01") == "200012"
        assert partition_key_to_period("2026-06-01") == "202606"

    def test_period_to_partition_key(self):
        assert period_to_partition_key("202401") == "2024-01-01"
        assert period_to_partition_key("200012") == "2000-12-01"
        assert period_to_partition_key("202606") == "2026-06-01"

    def test_period_to_partition_key_invalid_length(self):
        with pytest.raises(ValueError, match="6-character"):
            period_to_partition_key("20241")
        with pytest.raises(ValueError, match="6-character"):
            period_to_partition_key("20240101")

    def test_latest_n_partition_keys_returns_correct_count(self):
        keys = latest_n_partition_keys(5)
        assert len(keys) == 5

    def test_latest_n_partition_keys_are_months(self):
        with patch(
            "comext_pipeline.partitions.monthly.date",
            wraps=date,
        ) as mock_date:
            mock_date.today.return_value = date(2026, 6, 15)
            keys = latest_n_partition_keys(3)
            assert keys == ["2026-04-01", "2026-05-01", "2026-06-01"]

    def test_latest_n_partition_keys_reversed(self):
        keys = latest_n_partition_keys(3)
        assert keys == sorted(keys), "keys should be in ascending order"

    def test_monthly_partitions_definition(self):
        from datetime import datetime
        assert MONTHLY_PARTITIONS.start == datetime(2002, 1, 1, tzinfo=UTC)
        keys = MONTHLY_PARTITIONS.get_partition_keys()
        assert len(keys) > 0
        last_digit = int(keys[-1][5:7])
        assert 1 <= last_digit <= 12

    def test_partition_key_format(self):
        keys = MONTHLY_PARTITIONS.get_partition_keys()
        assert len(keys) > 0
        key = keys[-1]
        assert len(key) == 10
        assert key[4] == "-"
        assert key[7] == "-"
        assert key.endswith("-01")

    def test_partition_key_to_period_invalid_format(self):
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            partition_key_to_period("2024-1-01")
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            partition_key_to_period("2024/01/01")
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            partition_key_to_period("20240101")

    def test_latest_n_partition_keys_returns_empty_for_non_positive(self):
        assert latest_n_partition_keys(0) == []
        assert latest_n_partition_keys(-1) == []

    def test_roundtrip_conversion(self):
        period = "202401"
        key = period_to_partition_key(period)
        assert partition_key_to_period(key) == period


@pytest.mark.parametrize(
    "period,expected_key",
    [
        ("200001", "2000-01-01"),
        ("202012", "2020-12-01"),
        ("199912", "1999-12-01"),
    ],
)
def test_period_to_partition_key_parametrized(period, expected_key):
    assert period_to_partition_key(period) == expected_key


@pytest.mark.parametrize(
    "key,expected_period",
    [
        ("2000-01-01", "200001"),
        ("2020-12-01", "202012"),
        ("1999-12-01", "199912"),
    ],
)
def test_partition_key_to_period_parametrized(key, expected_period):
    assert partition_key_to_period(key) == expected_period
