"""Tests for the monitoring CLI cloud "add connection" wizard / flag paths.

These cover the non-interactive builder and dispatch logic without touching the
filesystem or the network (a fake service stands in for ``MonitorService``).
The interactive prompt path is exercised separately via a TTY in manual QA.
"""

from __future__ import annotations

import argparse

import pytest

from monitoring import cli as moncli


def _add_args(**over):
    base = dict(
        name="", provider="", json="", field=[], target_kind="",
        auth_mode="", interactive=False, no_test=True,
    )
    base.update(over)
    return argparse.Namespace(**base)


class _FakeSvc:
    def __init__(self):
        self.added = []
        self.tested = []

    def add_cloud_connection(self, name, profile):
        self.added.append((name, dict(profile)))
        return {"ok": True, "message": f"Cloud connection '{name}' saved."}

    def test_cloud_connection(self, name):
        self.tested.append(name)
        return {"ok": True, "message": "healthy"}


# --------------------------------------------------------------------------- #
# _canon_provider
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("aws", "AWS"),
        ("AWS", "AWS"),
        ("azure", "Azure"),
        ("gcp", "GCP"),
        ("other", "Other"),
        ("Custom", "Custom"),  # unknown passes through untouched
        ("", ""),
    ],
)
def test_canon_provider(raw, expected):
    assert moncli._canon_provider(raw) == expected


# --------------------------------------------------------------------------- #
# _build_cloud_profile_noninteractive
# --------------------------------------------------------------------------- #
def test_build_profile_from_fields_sets_defaults_and_casing():
    args = _add_args(
        name="cli-gcp",
        provider="gcp",
        field=["display_name=cli-gcp", "project_id=p", "resource_name=inst",
               "region=us-central1", "sa_key_path=/tmp/k.json"],
    )
    name, profile = moncli._build_cloud_profile_noninteractive(args)
    assert name == "cli-gcp"
    assert profile["provider"] == "GCP"            # canonical casing
    assert profile["auth_mode"] == "keys"          # default
    assert profile["target_kind"] == "cloud_db"    # default
    assert profile["purpose"] == "monitor"
    assert profile["monitoring"] is False
    assert profile["display_name"] == "cli-gcp"
    assert profile["resource_name"] == "inst"


def test_build_profile_name_falls_back_to_display_name():
    args = _add_args(provider="aws", field=["display_name=prod-rds"])
    name, profile = moncli._build_cloud_profile_noninteractive(args)
    assert name == "prod-rds"
    assert profile["display_name"] == "prod-rds"


def test_build_profile_bad_field_raises():
    args = _add_args(provider="aws", field=["not-a-pair"])
    with pytest.raises(ValueError):
        moncli._build_cloud_profile_noninteractive(args)


# --------------------------------------------------------------------------- #
# _cloud_connections_add — dispatch + validation
# --------------------------------------------------------------------------- #
def test_add_noninteractive_happy_path_saves_and_skips_test():
    svc = _FakeSvc()
    args = _add_args(
        name="cli-gcp", provider="gcp",
        field=["display_name=cli-gcp", "project_id=p", "resource_name=inst",
               "region=us", "sa_key_path=/tmp/k.json"],
        no_test=True,
    )
    rc = moncli._cloud_connections_add(args, svc)
    assert rc == 0
    assert len(svc.added) == 1
    saved_name, saved_profile = svc.added[0]
    assert saved_name == "cli-gcp"
    assert saved_profile["provider"] == "GCP"
    assert svc.tested == []  # --no-test honored


def test_add_noninteractive_runs_test_when_enabled():
    svc = _FakeSvc()
    args = _add_args(
        name="rds1", provider="aws",
        field=["display_name=rds1", "region=us-east-1", "resource_name=db1",
               "access_key_id=AKIA..."],
        no_test=False,
    )
    rc = moncli._cloud_connections_add(args, svc)
    assert rc == 0
    assert svc.tested == ["rds1"]


def test_add_noninteractive_validation_failure_does_not_save():
    svc = _FakeSvc()
    # cloud_db target with no resource_name => validation error
    args = _add_args(
        name="bad", provider="gcp",
        field=["display_name=bad", "project_id=p"],
    )
    rc = moncli._cloud_connections_add(args, svc)
    assert rc == 1
    assert svc.added == []


def test_add_noninteractive_requires_name():
    svc = _FakeSvc()
    args = _add_args(provider="aws", field=["region=us-east-1"])
    rc = moncli._cloud_connections_add(args, svc)
    assert rc == 2
    assert svc.added == []
