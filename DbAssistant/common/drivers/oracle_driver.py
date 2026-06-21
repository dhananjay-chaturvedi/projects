"""
Oracle driver shim — prefer python-oracledb, fall back to cx_Oracle.

Compatibility (per Oracle docs):
  - oracledb Thin (default): Oracle Database 12.1+
  - oracledb Thick (init_oracle_client): 11.2+ with Client 19+, older with older clients
  - cx_Oracle: always Thick; 9.2+ depending on Instant Client version

When ``oracle_client_path`` is configured, Thick mode is enabled before connect
so 11g and legacy password verifiers keep working.
"""

from __future__ import annotations

from typing import Any

DRIVER_NAME: str = ""
_oracle: Any = None

try:
    import oracledb as _oracle

    DRIVER_NAME = "oracledb"
except ImportError:
    try:
        import cx_Oracle as _oracle

        DRIVER_NAME = "cx_Oracle"
    except ImportError as exc:
        raise ImportError(
            "Oracle support requires 'oracledb' (recommended) or 'cx_Oracle'. "
            "Install: pip install oracledb"
        ) from exc

OracleError = _oracle.Error
_client_initialized = False
_thick_mode = False


def driver_label() -> str:
    mode = "thick" if _thick_mode else "thin" if DRIVER_NAME == "oracledb" else "thick"
    return f"{DRIVER_NAME} ({mode})"


def init_client(lib_dir: str | None, *, console_print=None) -> None:
    """Initialize Oracle Client libraries when a path is configured."""
    global _client_initialized, _thick_mode

    if _client_initialized:
        return

    if not lib_dir:
        _client_initialized = True
        return

    if DRIVER_NAME == "oracledb":
        _oracle.init_oracle_client(lib_dir=lib_dir)
        _thick_mode = True
        _client_initialized = True
        if console_print:
            console_print(f"Oracle: oracledb thick mode — client from {lib_dir}")
        return

    if not getattr(_oracle, "_client_initialized", False):
        _oracle.init_oracle_client(lib_dir=lib_dir)
        _oracle._client_initialized = True
    _thick_mode = True
    _client_initialized = True
    if console_print:
        console_print(f"Oracle: cx_Oracle — client from {lib_dir}")


def makedsn(host: str, port: int | str, service_name: str) -> str:
    return _oracle.makedsn(host, port, service_name=service_name)


def connect(user: str, password: str, dsn: str, **kwargs):
    return _oracle.connect(user, password, dsn, **kwargs)
