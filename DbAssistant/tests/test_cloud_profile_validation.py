"""Tests for common.cloud validation."""

from common.cloud.profiles import TARGET_CLOUD_DB, TARGET_CLOUD_SERVICE, TARGET_VM
from common.cloud.schemas import CLOUD_PROVIDER_SCHEMAS, resource_fields_for
from common.cloud.validation import validate_cloud_profile


def test_vm_resource_fields_exclude_db_engine():
    fields = resource_fields_for("AWS", TARGET_VM)
    keys = [f[1] for f in fields]
    assert "db_engine" not in keys
    assert "resource_name" in keys
    assert "host" in keys


def test_cloud_service_resource_fields_exclude_database_name():
    fields = resource_fields_for("Azure", TARGET_CLOUD_SERVICE)
    keys = [f[1] for f in fields]
    assert "database_name" not in keys
    assert "db_service_type" in keys


def test_cloud_db_still_includes_db_fields():
    fields = resource_fields_for("AWS", TARGET_CLOUD_DB)
    keys = [f[1] for f in fields]
    assert "db_engine" in keys


def test_cloud_db_requires_resource_name():
    schema = CLOUD_PROVIDER_SCHEMAS["AWS"]
    data = {
        "display_name": "prod",
        "resource_name": "",
        "auth_mode": "keys",
        "access_key_id": "AKIATEST",
        "secret_access_key": "secret",
    }
    err = validate_cloud_profile(
        data, "AWS", schema, require_db_identifier=True, target_kind=TARGET_CLOUD_DB
    )
    assert err is not None


def test_vm_allows_missing_resource_if_host_set():
    schema = CLOUD_PROVIDER_SCHEMAS["AWS"]
    data = {
        "display_name": "web-vm",
        "resource_name": "",
        "host": "10.0.0.5",
        "auth_mode": "keys",
        "access_key_id": "AKIATEST",
        "secret_access_key": "secret",
    }
    err = validate_cloud_profile(
        data, "AWS", schema, require_db_identifier=False, target_kind=TARGET_VM
    )
    assert err is None


def test_cloud_service_requires_resource_identifier():
    schema = CLOUD_PROVIDER_SCHEMAS["AWS"]
    data = {
        "display_name": "lb-prod",
        "resource_name": "",
        "region": "us-east-1",
        "auth_mode": "keys",
        "access_key_id": "AKIATEST",
        "secret_access_key": "secret",
    }
    err = validate_cloud_profile(
        data,
        "AWS",
        schema,
        require_db_identifier=False,
        target_kind=TARGET_CLOUD_SERVICE,
    )
    assert err is not None
