"""Tests for smart liveness gating helpers on ServerMonitorUI."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class TestLivenessWindow:
    def test_override_positive(self, liveness_ui):
        assert liveness_ui._liveness_window_seconds(5.0, 30) == 30.0

    def test_auto_window(self, liveness_ui):
        assert liveness_ui._liveness_window_seconds(5.0, 0) == 15.0

    def test_should_skip_within_window(self, liveness_ui):
        last = time.time()
        assert liveness_ui._should_skip_liveness(last, 5.0, 0) is True

    def test_should_not_skip_when_never_ok(self, liveness_ui):
        assert liveness_ui._should_skip_liveness(0.0, 5.0, 0) is False

    def test_should_not_skip_when_stale(self, liveness_ui):
        assert liveness_ui._should_skip_liveness(time.time() - 100, 5.0, 0) is False


class TestSecondsUntilExpiry:
    def test_gcp_naive_expiry(self, liveness_ui):
        creds = SimpleNamespace(
            expiry=(datetime.now(timezone.utc) + timedelta(minutes=30)).replace(tzinfo=None),
        )
        monitor = SimpleNamespace(credentials=creds)
        secs = liveness_ui._seconds_until_expiry("GCP", monitor)
        assert secs is not None
        assert 25 * 60 < secs < 35 * 60

    def test_gcp_no_credentials(self, liveness_ui):
        assert liveness_ui._seconds_until_expiry("GCP", SimpleNamespace()) is None

    def test_azure_cached_expires_on(self, liveness_ui):
        monitor = SimpleNamespace(_token_expires_on=time.time() + 600)
        secs = liveness_ui._seconds_until_expiry("AZURE", monitor)
        assert secs is not None
        assert 500 < secs < 700

    def test_azure_fetches_token(self, liveness_ui):
        cred = MagicMock()
        cred.get_token.return_value = SimpleNamespace(expires_on=time.time() + 400)
        monitor = SimpleNamespace(credential=cred)
        secs = liveness_ui._seconds_until_expiry("AZURE", monitor)
        assert secs is not None
        cred.get_token.assert_called_once()

    def test_aws_static_returns_none(self, liveness_ui):
        creds = SimpleNamespace()  # no _expiry_time
        signer = SimpleNamespace(_credentials=creds)
        client = SimpleNamespace(_request_signer=signer)
        monitor = SimpleNamespace(rds=client)
        assert liveness_ui._seconds_until_expiry("AWS", monitor) is None

    def test_aws_refreshable(self, liveness_ui):
        expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        creds = SimpleNamespace(_expiry_time=expiry)
        signer = SimpleNamespace(_credentials=creds)
        client = SimpleNamespace(_request_signer=signer)
        monitor = SimpleNamespace(rds=client)
        secs = liveness_ui._seconds_until_expiry("AWS", monitor)
        assert secs is not None
        assert secs > 3500


class TestCloudShouldRefreshKeepalive:
    def test_needs_refresh_flag(self, liveness_ui):
        liveness_ui._cloud_needs_refresh["x"] = True
        assert liveness_ui._cloud_should_refresh_keepalive("x", {}, None) is True

    def test_consecutive_failures(self, liveness_ui):
        liveness_ui._cloud_consecutive_failures["x"] = 1
        assert liveness_ui._cloud_should_refresh_keepalive("x", {}, None) is True

    def test_near_expiry(self, liveness_ui):
        liveness_ui._cloud_consecutive_failures.clear()
        liveness_ui._cloud_needs_refresh.clear()
        creds = SimpleNamespace(
            expiry=(datetime.now(timezone.utc) + timedelta(minutes=2)).replace(tzinfo=None)
        )
        monitor = SimpleNamespace(credentials=creds)
        assert liveness_ui._cloud_should_refresh_keepalive(
            "x", {"provider": "GCP"}, monitor
        ) is True

    def test_force_refresh_no_last_ok(self, liveness_ui):
        liveness_ui._cloud_consecutive_failures.clear()
        liveness_ui._cloud_needs_refresh.clear()
        assert liveness_ui._cloud_should_refresh_keepalive("x", {}, None) is True

    def test_healthy_skip(self, liveness_ui):
        liveness_ui._cloud_consecutive_failures.clear()
        liveness_ui._cloud_needs_refresh.clear()
        liveness_ui._cloud_last_ok_at["x"] = time.time()
        creds = SimpleNamespace(
            expiry=(datetime.now(timezone.utc) + timedelta(hours=2)).replace(tzinfo=None)
        )
        monitor = SimpleNamespace(credentials=creds)
        assert liveness_ui._cloud_should_refresh_keepalive(
            "x", {"provider": "GCP"}, monitor
        ) is False


class TestClearCloudLivenessState:
    def test_clears_all(self, liveness_ui):
        liveness_ui._cloud_last_ok_at["n"] = time.time()
        liveness_ui._cloud_consecutive_failures["n"] = 2
        liveness_ui._cloud_needs_refresh["n"] = True
        liveness_ui._clear_cloud_liveness_state("n")
        assert "n" not in liveness_ui._cloud_last_ok_at
        assert "n" not in liveness_ui._cloud_consecutive_failures
        assert "n" not in liveness_ui._cloud_needs_refresh
