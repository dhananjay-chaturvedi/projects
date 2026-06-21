"""Tests for Oracle driver shim (oracledb preferred, cx_Oracle fallback)."""

from __future__ import annotations

import importlib.util

import pytest


@pytest.mark.skipif(
    not (importlib.util.find_spec("oracledb") or importlib.util.find_spec("cx_Oracle")),
    reason="No Oracle driver installed",
)
def test_oracle_driver_loads():
    from common.drivers import oracle_driver

    assert oracle_driver.DRIVER_NAME in ("oracledb", "cx_Oracle")
    assert oracle_driver.OracleError is not None


@pytest.mark.skipif(
    not (importlib.util.find_spec("oracledb") or importlib.util.find_spec("cx_Oracle")),
    reason="No Oracle driver installed",
)
def test_con_oracle_exports_driver_name():
    from common.drivers import conOracle

    assert conOracle.ORACLE_DRIVER in ("oracledb", "cx_Oracle")
