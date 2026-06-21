"""Validation for cloud connection profiles."""

from __future__ import annotations

from common.cloud.profiles import TARGET_CLOUD_DB, TARGET_CLOUD_SERVICE, TARGET_VM


def validate_cloud_profile(
    data: dict,
    provider: str,
    schema: dict,
    *,
    require_db_identifier: bool,
    target_kind: str = TARGET_CLOUD_DB,
) -> str | None:
    """Return an error message, or None if the profile is valid."""
    if not data.get("display_name"):
        return "Display Name is required."

    resource_name = (data.get("resource_name") or "").strip()
    if require_db_identifier or target_kind == TARGET_CLOUD_DB:
        if not resource_name:
            return "DB / resource identifier is required."
    elif target_kind == TARGET_VM:
        if not resource_name and not data.get("host"):
            return "Provide a VM name/id or host for VM monitoring."
    elif target_kind == TARGET_CLOUD_SERVICE:
        if not resource_name:
            return "Cloud resource identifier is required."

    auth_mode_val = data.get("auth_mode", "keys")
    if auth_mode_val == "env":
        return None

    sso_schema = schema.get("sso_auth", {})
    sso_fields = sso_schema.get("fields", [])
    sso_label = sso_schema.get("tab_label", "SSO / OIDC")

    if auth_mode_val == "sso":
        required_sso = [f for f in sso_fields if f[0].endswith("*")]
        for f in required_sso:
            if not data.get(f[1]):
                return f"'{f[0].rstrip(' *')}' is required for {sso_label} auth."
    else:
        auth_fields = (
            schema["keys_auth"]
            if auth_mode_val == "keys"
            else schema["pwd_auth"]
        )
        tab = (
            "Access Keys / Tokens"
            if auth_mode_val == "keys"
            else "Username / Password"
        )
        if provider == "GCP":
            if auth_mode_val == "keys":
                if not any(data.get(f[1]) for f in auth_fields):
                    return (
                        f"Provide at least one credential in '{tab}' "
                        "(file path, pasted JSON, or OAuth2 token)."
                    )
            elif not data.get("password"):
                return (
                    "'Key File Path or JSON' is required for "
                    "GCP Service-Account password auth."
                )
        else:
            first_key = auth_fields[0][1] if auth_fields else ""
            if first_key and not data.get(first_key):
                return f"The first credential field in '{tab}' is required."
    return None
