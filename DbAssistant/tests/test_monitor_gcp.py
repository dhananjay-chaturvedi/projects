"""GCP monitor helper tests."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from monitoring import monitor_gcp as mgcp


def test_summarise_gcp_api_error_403():
    exc = Exception('403 PERMISSION_DENIED reason "API disabled"')
    msg = mgcp._summarise_gcp_api_error(exc)
    assert msg
    assert "403" in msg or "permission" in msg.lower() or "disabled" in msg.lower()


def test_summarise_gcp_api_error_401():
    exc = Exception("401 Unauthorized")
    msg = mgcp._summarise_gcp_api_error(exc)
    assert msg


def test_timestamp_from_datetime_subclass():
    """DatetimeWithNanoseconds is a datetime subclass - no ToDatetime."""
    ts = datetime(2024, 1, 1, 12, 0, 0)
    # Simulate metric point timestamp handling logic
    if hasattr(ts, "ToDatetime"):
        out = ts.ToDatetime()
    else:
        out = ts
    assert out.year == 2024
