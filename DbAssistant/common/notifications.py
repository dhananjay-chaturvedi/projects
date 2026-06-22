"""Notification delivery engine: encrypted secret storage + channel dispatch.

This is generic *delivery* infrastructure shared by the tool. The notification
*settings* themselves are owned by the Monitoring module:

* Non-secret settings (channel toggles, SMTP host/port, recipients, minimum
  severity) live in ``monitoring/monitor_config.ini`` under ``[notifications]``
  and are read through :func:`load_config` (which falls back to safe disabled
  defaults when the Monitoring module is not installed, so core stays
  independently shippable).
* Secrets (Teams webhook URL, SMTP password) are **never** written to the INI.
  They are encrypted at rest with Fernet under ``~/.dbassistant`` via
  :class:`NotificationSecretStore`.
* :func:`dispatch_alert` is the single entry point the daemon / service / UI
  use to deliver an alert to every enabled channel, honouring the configured
  minimum severity.

Back-compat: the legacy ``ALERT_TEAMS_WEBHOOK_URL`` environment variable is
still honoured as a fallback when no encrypted webhook is stored, so existing
``.env`` based setups keep working.
"""

from __future__ import annotations

import os
import smtplib
import ssl
import sys
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Optional

from common import paths
from common.secret_store import (
    atomic_write_json,
    decrypt_value,
    encrypt_value,
    load_or_create_fernet_key,
    safe_read_json,
)

_SEVERITY_ORDER = {"INFO": 10, "WARNING": 20, "CRITICAL": 30}


# --------------------------------------------------------------------------- #
# Secret store
# --------------------------------------------------------------------------- #
class NotificationSecretStore:
    """Tiny encrypted key/value store for notification secrets."""

    _FIELDS = ("teams_webhook_url", "smtp_password")

    def __init__(self, path=None, key_path=None):
        self._path = path or paths.notifications_secrets_path()
        self._key_path = key_path or paths.notifications_key_path()

    def _cipher(self):
        return load_or_create_fernet_key(self._key_path)

    def _read_raw(self) -> dict:
        data = safe_read_json(self._path)
        return data if isinstance(data, dict) else {}

    def get(self, field: str) -> str:
        """Return the decrypted secret, or '' if unset."""
        raw = self._read_raw().get(field, "")
        if not raw:
            return ""
        plain = decrypt_value(self._cipher(), raw)
        return plain if plain is not None else ""

    def has(self, field: str) -> bool:
        return bool(self._read_raw().get(field))

    def set(self, field: str, value: str) -> bool:
        """Store (or clear, when value is empty) an encrypted secret."""
        data = self._read_raw()
        if value:
            token = encrypt_value(self._cipher(), value)
            if token is None:
                return False
            data[field] = token
        else:
            data.pop(field, None)
        return atomic_write_json(self._path, data)

    def status(self) -> dict:
        """Return which secrets are configured (never the values)."""
        raw = self._read_raw()
        return {f: bool(raw.get(f)) for f in self._FIELDS}


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass
class NotificationConfig:
    enabled: bool = False
    min_severity: str = "WARNING"
    teams_enabled: bool = False
    email_enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_use_tls: bool = True
    smtp_username: str = ""
    email_from: str = ""
    email_to: str = ""

    @property
    def recipients(self) -> list[str]:
        return [a.strip() for a in self.email_to.split(",") if a.strip()]


_SEC = "notifications"


def _monitor_config():
    """Return the Monitoring module's config loader, or ``None`` when the
    Monitoring module is not installed (keeps core independently shippable)."""
    try:
        from monitoring import monitor_config
    except Exception:
        return None
    return monitor_config


def load_config() -> NotificationConfig:
    """Read non-secret notification settings from ``monitor_config.ini``.

    Falls back to disabled defaults when the Monitoring module is absent.
    """
    mc = _monitor_config()
    if mc is None:
        return NotificationConfig()
    return NotificationConfig(
        enabled=mc.get_bool(_SEC, "enabled", default=False),
        min_severity=(mc.get(_SEC, "min_severity", default="WARNING") or "WARNING").upper(),
        teams_enabled=mc.get_bool(_SEC, "teams_enabled", default=False),
        email_enabled=mc.get_bool(_SEC, "email_enabled", default=False),
        smtp_host=mc.get(_SEC, "smtp_host", default=""),
        smtp_port=mc.get_int(_SEC, "smtp_port", default=587),
        smtp_use_tls=mc.get_bool(_SEC, "smtp_use_tls", default=True),
        smtp_username=mc.get(_SEC, "smtp_username", default=""),
        email_from=mc.get(_SEC, "email_from", default=""),
        email_to=mc.get(_SEC, "email_to", default=""),
    )


# Editable non-secret notification keys, with type + validation metadata. Used
# by the monitoring CLI / API / settings UI to write to monitor_config.ini.
NOTIFICATION_KEYS: dict[str, dict] = {
    "enabled": {"type": "bool", "label": "Enable alert notifications"},
    "min_severity": {"type": "enum", "label": "Minimum severity to notify",
                     "options": ("INFO", "WARNING", "CRITICAL")},
    "teams_enabled": {"type": "bool", "label": "Send to Microsoft Teams"},
    "email_enabled": {"type": "bool", "label": "Send email alerts"},
    "smtp_host": {"type": "str", "label": "SMTP host"},
    "smtp_port": {"type": "int", "label": "SMTP port", "min": 1, "max": 65535},
    "smtp_use_tls": {"type": "bool", "label": "Use STARTTLS"},
    "smtp_username": {"type": "str", "label": "SMTP username"},
    "email_from": {"type": "str", "label": "From address"},
    "email_to": {"type": "str", "label": "Recipient(s)"},
}

# Secret notification keys (stored encrypted, never in INI).
NOTIFICATION_SECRET_KEYS: dict[str, dict] = {
    "teams_webhook_url": {"label": "Teams webhook URL"},
    "smtp_password": {"label": "SMTP password"},
}


def set_config_value(key: str, value: str) -> dict:
    """Validate + persist one non-secret notification key to monitor_config.ini.

    Returns ``{"ok": bool, "message": str}``. Requires the Monitoring module.
    """
    meta = NOTIFICATION_KEYS.get(key)
    if meta is None:
        return {"ok": False, "message": f"Unknown notification setting '{key}'."}
    mc = _monitor_config()
    if mc is None:
        return {"ok": False,
                "message": "Monitoring module not installed — cannot save."}

    raw = "" if value is None else str(value).strip()
    t = meta["type"]
    if t == "bool":
        low = raw.lower()
        if low not in {"true", "false", "yes", "no", "1", "0", "on", "off"}:
            return {"ok": False, "message": f"{meta['label']} must be true or false."}
        raw = "true" if low in {"true", "yes", "1", "on"} else "false"
    elif t == "enum":
        if raw not in meta.get("options", ()):
            return {"ok": False,
                    "message": f"{meta['label']} must be one of: "
                               f"{', '.join(meta['options'])}."}
    elif t == "int":
        try:
            num = int(raw)
        except ValueError:
            return {"ok": False, "message": f"{meta['label']} must be an integer."}
        if "min" in meta and num < meta["min"]:
            return {"ok": False, "message": f"{meta['label']} must be >= {meta['min']}."}
        if "max" in meta and num > meta["max"]:
            return {"ok": False, "message": f"{meta['label']} must be <= {meta['max']}."}

    try:
        mc.set_value(_SEC, key, raw)
    except Exception as exc:
        return {"ok": False, "message": f"Failed to save {meta['label']}: {exc}"}
    return {"ok": True, "message": f"{meta['label']} saved."}


def status_dict(
    cfg: Optional[NotificationConfig] = None,
    store: Optional["NotificationSecretStore"] = None,
) -> dict:
    """Return the resolved notification config + which secrets are set.

    Shared by the ``notify config`` CLI and the read-only config API so both
    surfaces report the exact same shape. Secret *values* are never included —
    only boolean "configured" flags.
    """
    cfg = cfg or load_config()
    store = store or NotificationSecretStore()
    secrets = store.status()
    return {
        "enabled": cfg.enabled,
        "min_severity": cfg.min_severity,
        "teams_enabled": cfg.teams_enabled,
        "email_enabled": cfg.email_enabled,
        "smtp_host": cfg.smtp_host,
        "smtp_port": cfg.smtp_port,
        "smtp_use_tls": cfg.smtp_use_tls,
        "smtp_username": cfg.smtp_username,
        "email_from": cfg.email_from,
        "email_to": cfg.email_to,
        "teams_webhook_url_set": secrets.get("teams_webhook_url", False),
        "smtp_password_set": secrets.get("smtp_password", False),
    }


def _severity_meets(severity: str, minimum: str) -> bool:
    return _SEVERITY_ORDER.get((severity or "").upper(), 0) >= _SEVERITY_ORDER.get(
        (minimum or "WARNING").upper(), 20
    )


# --------------------------------------------------------------------------- #
# Email delivery
# --------------------------------------------------------------------------- #
def send_email_alert(
    subject: str,
    body: str,
    cfg: Optional[NotificationConfig] = None,
    *,
    store: Optional[NotificationSecretStore] = None,
    timeout: int = 20,
) -> dict:
    """Send a single alert email over SMTP. Never raises."""
    cfg = cfg or load_config()
    store = store or NotificationSecretStore()

    if not cfg.smtp_host:
        return {"ok": False, "channel": "email", "message": "SMTP host not configured."}
    if not cfg.email_from or not cfg.recipients:
        return {"ok": False, "channel": "email",
                "message": "Email from/recipients not configured."}

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.email_from
    msg["To"] = ", ".join(cfg.recipients)
    msg.set_content(body)

    password = store.get("smtp_password")
    try:
        if cfg.smtp_port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port,
                                  timeout=timeout, context=context) as srv:
                if cfg.smtp_username:
                    srv.login(cfg.smtp_username, password)
                srv.send_message(msg)
        else:
            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=timeout) as srv:
                if cfg.smtp_use_tls:
                    srv.starttls(context=ssl.create_default_context())
                if cfg.smtp_username:
                    srv.login(cfg.smtp_username, password)
                srv.send_message(msg)
        return {"ok": True, "channel": "email",
                "message": f"Email alert sent to {len(cfg.recipients)} recipient(s)."}
    except Exception as exc:
        msg = f"Email alert failed: {exc}"
        print(f"ERROR: {msg}", file=sys.stderr)
        return {"ok": False, "channel": "email", "message": msg}


# --------------------------------------------------------------------------- #
# Unified dispatch
# --------------------------------------------------------------------------- #
def dispatch_alert(
    message_body: str,
    *,
    severity: str = "WARNING",
    cfg: Optional[NotificationConfig] = None,
    store: Optional[NotificationSecretStore] = None,
    force: bool = False,
) -> dict:
    """Deliver an alert to every enabled channel, honouring min severity.

    Returns ``{"ok": bool, "delivered": [...], "skipped": str|None,
    "results": [per-channel dicts]}``. Never raises.

    ``force=True`` bypasses the master ``enabled`` switch and the minimum
    severity gate (used by the manual "send test notification" action), but
    still only delivers to channels that are individually toggled on.
    """
    cfg = cfg or load_config()
    store = store or NotificationSecretStore()
    legacy_webhook = os.environ.get("ALERT_TEAMS_WEBHOOK_URL", "").strip()

    if not force:
        if not cfg.enabled:
            # Back-compat: honour a legacy .env Teams webhook even when the new
            # [notifications] section has not been turned on yet.
            if legacy_webhook:
                from monitoring.send_notification import send_alert

                r = send_alert(message_body, webhook_url=legacy_webhook)
                return {"ok": bool(r.get("ok")),
                        "delivered": ["teams"] if r.get("ok") else [],
                        "skipped": None, "results": [r]}
            return {"ok": True, "delivered": [], "skipped": "notifications disabled",
                    "results": []}
        if not _severity_meets(severity, cfg.min_severity):
            return {"ok": True, "delivered": [],
                    "skipped": f"severity {severity} below {cfg.min_severity}",
                    "results": []}

    results: list[dict] = []

    if cfg.teams_enabled:
        from monitoring.send_notification import send_alert

        # Prefer the encrypted webhook; fall back to env for legacy setups.
        webhook = store.get("teams_webhook_url") or legacy_webhook
        results.append(send_alert(message_body, webhook_url=webhook or None))

    if cfg.email_enabled:
        subject = f"[{severity.upper()}] DB Monitoring Alert"
        results.append(send_email_alert(subject, message_body, cfg, store=store))

    # Nothing toggled: for a forced/manual send, try the legacy env webhook so
    # a "test" still does something useful on legacy setups.
    if not results and legacy_webhook:
        from monitoring.send_notification import send_alert

        results.append(send_alert(message_body, webhook_url=legacy_webhook))

    delivered = [r.get("channel", "teams") for r in results if r.get("ok")]
    ok = bool(results) and all(r.get("ok") for r in results)
    skipped = None if results else "no channels enabled"
    return {"ok": ok if results else True, "delivered": delivered,
            "skipped": skipped, "results": results}
