"""
monitoring_utils.py
===================

Sustained-breach counter used by :mod:`monitoring.threshold_checker`.

A rule with ``window = N`` fires only after **N consecutive** breaching
samples have been observed for the same ``(source, path, instance,
metric, severity)`` key.  A single non-breaching sample resets the
counter to ``0`` immediately, so the alerting state never retains stale
history.

State is process-local and held in-memory; it is protected by a single
``threading.Lock`` because long-running monitors poll from multiple
threads (UI, daemon, REST API).

Memory hygiene
--------------
Each call records ``(count, last_seen_monotonic)`` per key.  An
opportunistic mark-and-sweep runs at most once every
``_GC_INTERVAL_SECONDS`` and drops keys older than
``STALE_KEY_TTL_SECONDS``.  Long-running processes that monitor
short-lived resources (containers, autoscaled DB nodes) therefore do
not accumulate dead counters forever.

Tests can call :func:`purge_stale` to run cleanup on demand, or
:func:`reset_all` to wipe state completely.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Tuple

__all__ = [
    "sustained_breach",
    "purge_stale",
    "reset_all",
    "STALE_KEY_TTL_SECONDS",
]

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

# key -> (consecutive_breach_count, last_seen_monotonic)
_store: Dict[str, Tuple[int, float]] = {}
_store_lock = threading.Lock()

# Drop counters that have not been touched for this long (24 h default).
# Picked generously so a flapping resource still gets credit for its
# in-flight counter, but bounded so the dict cannot grow indefinitely.
def _cfg_float(key: str, default: float) -> float:
    try:
        from monitoring import monitor_config
        return monitor_config.get_float("monitoring", key, default=default)
    except Exception:
        return default


STALE_KEY_TTL_SECONDS: float = _cfg_float("sustained_breach_ttl_seconds", 24 * 3600.0)

# Run the opportunistic cleanup at most once per this interval.  This
# keeps the lock-hold time per `sustained_breach` call constant on
# average and avoids walking the dict on every poll.
_GC_INTERVAL_SECONDS: float = _cfg_float("sustained_breach_gc_interval_seconds", 300.0)

# Last monotonic timestamp at which cleanup ran.  Mutable single-element
# list so we can reassign it while holding the lock without rebinding a
# module global.
_last_gc: list[float] = [0.0]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sustained_breach(
    key: str,
    value,
    op: str,
    threshold: float,
    window: int = 3,
) -> bool:
    """Return ``True`` only after ``window`` consecutive breaching samples
    for ``key``.

    Parameters
    ----------
    key:
        Stable identifier for the counter; typically
        ``f"{source}.{path}.{instance}.{metric}.{severity}"``.
    value:
        The latest sample.  Non-numeric or NaN values are treated as a
        missing poll: the counter is left untouched and ``False`` is
        returned.  Outages therefore neither falsely fire nor
        silently reset an in-flight breach.
    op:
        Comparison operator — ``>``, ``>=``, ``<``, ``<=``, ``==`` or ``!=``.
        Other operators evaluate to "not breached" and reset the counter.
    threshold:
        Numeric threshold to compare ``value`` against.
    window:
        Required number of consecutive breaching samples (``>= 1``).

    Semantics
    ---------
    * On a **breaching** sample the counter increments.  If it reaches
      ``window`` the function returns ``True``.
    * On a **non-breaching** numeric sample the counter is reset to
      ``0`` and ``False`` is returned, so the next breach must restart
      the count from scratch.
    * On a **non-numeric / NaN** sample the counter is left as-is and
      ``False`` is returned.
    * The same call also performs an opportunistic GC sweep — see
      :func:`purge_stale`.

    Thread-safety
    -------------
    All state is mutated under ``_store_lock`` so concurrent pollers
    cannot lose increments or read partial counter state.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if v != v:  # NaN
        return False

    breached = (
        (op == ">" and v > threshold)
        or (op == ">=" and v >= threshold)
        or (op == "<" and v < threshold)
        or (op == "<=" and v <= threshold)
        or (op == "==" and v == threshold)
        or (op == "!=" and v != threshold)
    )
    now = time.monotonic()
    window_eff = max(1, int(window))

    with _store_lock:
        # Opportunistic cleanup of long-dormant counters.
        if now - _last_gc[0] > _GC_INTERVAL_SECONDS:
            _purge_locked(now - STALE_KEY_TTL_SECONDS)
            _last_gc[0] = now

        if not breached:
            # A single safe sample fully resets the consecutive-breach
            # counter.  We touch last_seen so the entry stays alive
            # until the TTL elapses without further breaches.
            if key in _store:
                _store[key] = (0, now)
            return False

        count = _store.get(key, (0, now))[0] + 1
        _store[key] = (count, now)
        return count >= window_eff


def purge_stale(
    ttl_seconds: float | None = None,
    *,
    now: float | None = None,
) -> int:
    """Drop counter entries whose ``last_seen`` is older than
    ``ttl_seconds`` (defaults to :data:`STALE_KEY_TTL_SECONDS`).

    Returns the number of keys removed.  Useful for tests and for
    daemons that prefer explicit cleanup cadence over the implicit
    opportunistic sweep.
    """
    ttl = STALE_KEY_TTL_SECONDS if ttl_seconds is None else float(ttl_seconds)
    moment = time.monotonic() if now is None else float(now)
    cutoff = moment - ttl
    with _store_lock:
        return _purge_locked(cutoff)


def reset_all() -> None:
    """Wipe all internal counter state.

    Primarily intended for test isolation.  Production code should
    rely on :func:`purge_stale` or the implicit GC sweep instead.
    """
    with _store_lock:
        _store.clear()
        _last_gc[0] = 0.0


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _purge_locked(cutoff: float) -> int:
    """Remove keys whose ``last_seen`` is strictly less than ``cutoff``.

    Caller must already hold ``_store_lock``.
    """
    stale = [k for k, (_, ts) in _store.items() if ts < cutoff]
    for k in stale:
        _store.pop(k, None)
    return len(stale)
