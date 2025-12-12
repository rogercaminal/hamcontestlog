# tests/test_config.py
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hamcontestlog.config import _parse_dt


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2024-11-23T00:00:00Z", datetime(2024, 11, 23, 0, 0, 0)),
        ("2024-11-23T00:00:00", datetime(2024, 11, 23, 0, 0, 0)),
        # tz-aware datetime should be converted to UTC and made naive
        (datetime(2024, 11, 23, 1, 0, 0, tzinfo=timezone.utc), datetime(2024, 11, 23, 1, 0, 0)),
    ],
)
def test_parse_dt_naive_utc(value, expected) -> None:
    dt = _parse_dt(value)
    assert isinstance(dt, datetime)
    assert dt.tzinfo is None
    assert dt == expected

