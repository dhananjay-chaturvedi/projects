import os
import sys
import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from common.config_loader import console_debug, get_project_version
from monitoring import monitor_config

load_dotenv()


def setup_logger(name):
    """Return a logger that writes to logs/<name>.log and stdout."""
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(f"logs/{name}.log")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


_DEFAULT_TIMEOUT_SECONDS = 15
_DEFAULT_MAX_ATTEMPTS = 2
_MAX_MESSAGE_CHARS = monitor_config.get_int(
    "notifications", "max_message_chars", default=20_000)
_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}
_NON_RETRYABLE_FAILURES: dict[str, tuple[int | None, str]] = {}


def _bounded_int(value, *, default: int, low: int, high: int) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        return default
    return max(low, min(high, out))


def _truncate_message(message_body: str) -> str:
    text = str(message_body or "")
    if len(text) <= _MAX_MESSAGE_CHARS:
        return text
    omitted = len(text) - _MAX_MESSAGE_CHARS
    return text[:_MAX_MESSAGE_CHARS] + f"\n\n[truncated {omitted} character(s)]"


def _teams_payload(message_body: str) -> bytes:
    title = f"DB Alert: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = _truncate_message(message_body)
    return json.dumps(
        {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": "FF0000",
            "summary": title,
            "sections": [
                {
                    "activityTitle": f"**{title}**",
                    "text": body.replace("\n", "\n\n"),
                }
            ],
        }
    ).encode("utf-8")


def _read_response_body(resp) -> str:
    try:
        return resp.read().decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _urlopen(req, timeout):
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except TypeError as exc:
        # Some tests monkeypatch urlopen with a simple lambda(req). Keep the
        # production timeout while remaining compatible with those callables.
        if "timeout" not in str(exc):
            raise
        return urllib.request.urlopen(req)


def _validate_webhook_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return "ALERT_TEAMS_WEBHOOK_URL must be a valid http(s) URL."
    return None


def _should_retry_status(status: int | None) -> bool:
    return status in _RETRYABLE_STATUSES


def _notification_int(key: str, default: int, *, low: int, high: int) -> int:
    return max(low, min(high, monitor_config.get_int("notifications", key, default=default)))


def send_alert(
    message_body,
    *,
    timeout: int | None = None,
    max_attempts: int | None = None,
    webhook_url: str | None = None,
):
    """Send one alert to the configured Teams webhook.

    The webhook URL is resolved in this order:

    1. the explicit ``webhook_url`` argument (e.g. the encrypted value from the
       notifications secret store),
    2. the ``ALERT_TEAMS_WEBHOOK_URL`` environment variable (legacy ``.env``).

    Returns a structured result and never raises, so callers can decide
    whether a Teams delivery failure should fail their own workflow.
    """
    console_debug(f"--- TRIGGERING ALERT ---\n{message_body}")

    webhook_url = (webhook_url or os.environ.get("ALERT_TEAMS_WEBHOOK_URL", "")).strip()
    if not webhook_url:
        msg = ("No Teams webhook configured (ALERT_TEAMS_WEBHOOK_URL not set) — "
               "alert not sent to Teams.")
        print(f"WARNING: {msg}", file=sys.stderr)
        return {"ok": False, "channel": "teams", "status": None, "message": msg}
    validation_error = _validate_webhook_url(webhook_url)
    if validation_error:
        print(f"ERROR: {validation_error}", file=sys.stderr)
        return {"ok": False, "status": None, "message": validation_error}
    if webhook_url in _NON_RETRYABLE_FAILURES:
        status, message = _NON_RETRYABLE_FAILURES[webhook_url]
        return {"ok": False, "status": status, "message": message}

    timeout_s = _bounded_int(
        timeout if timeout is not None else os.environ.get("ALERT_TEAMS_TIMEOUT_SECONDS"),
        default=_notification_int(
            "teams_timeout_seconds",
            _DEFAULT_TIMEOUT_SECONDS,
            low=1,
            high=120,
        ),
        low=1,
        high=120,
    )
    attempts = _bounded_int(
        max_attempts if max_attempts is not None else os.environ.get("ALERT_TEAMS_MAX_ATTEMPTS"),
        default=_notification_int(
            "teams_max_attempts",
            _DEFAULT_MAX_ATTEMPTS,
            low=1,
            high=5,
        ),
        low=1,
        high=5,
    )
    max_backoff_s = _notification_int(
        "teams_max_backoff_seconds",
        5,
        low=0,
        high=300,
    )

    payload = _teams_payload(message_body)

    last_error = ""
    last_status = None
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json, text/plain, */*",
                "User-Agent": f"DbManagementTool/{get_project_version()}",
            },
            method="POST",
        )
        try:
            with _urlopen(req, timeout=timeout_s) as resp:
                body = _read_response_body(resp)
                status = getattr(resp, "status", None) or getattr(resp, "code", None)
                last_status = status
                if status is not None and 200 <= status < 300:
                    console_debug("Alert sent to MS Teams.")
                    return {
                        "ok": True,
                        "status": status,
                        "message": "Alert sent to MS Teams.",
                        "response": body,
                    }
                last_error = f"Teams webhook returned HTTP {status}: {body}"
                if not _should_retry_status(status):
                    _NON_RETRYABLE_FAILURES[webhook_url] = (status, last_error)
                    print(f"ERROR: {last_error}", file=sys.stderr)
                    return {"ok": False, "status": status, "message": last_error}
        except urllib.error.HTTPError as exc:
            body = _read_response_body(exc)
            last_status = exc.code
            last_error = f"Teams webhook returned HTTP {exc.code}: {body}"
            # Most 4xx statuses are not transient. 408/429 may be.
            if not _should_retry_status(exc.code):
                _NON_RETRYABLE_FAILURES[webhook_url] = (exc.code, last_error)
                print(f"ERROR: {last_error}", file=sys.stderr)
                return {"ok": False, "status": exc.code, "message": last_error}
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            last_error = f"Teams webhook connection failed: {reason}"
        except Exception as exc:
            last_error = str(exc)
        if attempt < attempts and max_backoff_s > 0:
            time.sleep(min(2 ** (attempt - 1), max_backoff_s))

    if last_error:
        if last_error.startswith("Teams webhook returned HTTP"):
            print(f"ERROR: {last_error}", file=sys.stderr)
        else:
            print(f"ERROR: Failed to send Teams alert: {last_error}", file=sys.stderr)
        return {"ok": False, "status": last_status, "message": last_error}

    return {"ok": False, "status": None, "message": "Unknown Teams delivery failure."}
