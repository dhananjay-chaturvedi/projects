"""
monitoring/daemon.py
==================
Background monitoring daemon for DbManagementTool.

Features
--------
- Polls all (or a specified subset of) saved connections on a fixed interval
- Evaluates metrics against monitor_thresholds.ini and fires alerts
- Writes latest metrics to <DBASSISTANT_HOME>/runtime/metrics.json (consumed by the REST API)
- PID file management for start/stop/status
- Structured log output to a log file (or stderr)
- Clean shutdown on SIGTERM / SIGINT
- Can run in foreground mode (for Docker / systemd) or as a Unix double-fork daemon

Usage (via CLI)
---------------
    python dbtool.py daemon start [--foreground]
    python dbtool.py daemon stop
    python dbtool.py daemon status

Usage (directly)
----------------
    from monitoring.daemon import MonitorDaemon
    d = MonitorDaemon(interval=60)
    d.run_foreground()   # blocks; handles SIGTERM/SIGINT
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
import threading
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from common import paths as _paths
from common.secret_store import atomic_write_json
from monitoring.service import make_service


def _default_pid() -> str:
    return str(_paths.daemon_pid_path())


def _default_log() -> str:
    return str(_paths.daemon_log_path())


def _default_metrics() -> str:
    return str(_paths.metrics_snapshot_path())


class MonitorDaemon:
    """
    Monitoring daemon.  Call run_foreground() to block in the current process,
    or start() to daemonise (Unix double-fork).
    """

    def __init__(
        self,
        connections: list[str] | None = None,
        interval:    int | None = None,
        pid_file:    str | None = None,
        log_file:    str | None = None,
        metrics_file: str | None = None,
    ):
        if interval is None:
            from monitoring import monitor_config
            interval = monitor_config.get_int(
                "monitoring", "default_poll_interval", default=30)
        if pid_file is None:
            pid_file = _default_pid()
        if log_file is None:
            log_file = _default_log()
        if metrics_file is None:
            metrics_file = _default_metrics()
        self.connections  = connections   # None = all saved connections
        self.interval     = max(1, int(interval or 1))
        self.pid_file     = Path(pid_file)
        self.log_file     = Path(log_file)
        self.metrics_file = Path(metrics_file)
        self.pid: Optional[int] = None

        self._stop_event  = threading.Event()
        self._svc = None
        self._logger: Optional[logging.Logger] = None
        # Track active breach keys to avoid re-dispatching the same alert
        # every poll for a sustained breach. Cleared when the breach resolves.
        self._active_alert_keys: set[str] = set()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(f"dbtool.daemon.{self.pid_file}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S")
            # File handler
            try:
                self.log_file.parent.mkdir(parents=True, exist_ok=True)
                fh = logging.FileHandler(self.log_file)
                fh.setFormatter(fmt)
                logger.addHandler(fh)
            except Exception:
                pass
            # Stderr handler (always)
            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(fmt)
            logger.addHandler(sh)
        return logger

    # ------------------------------------------------------------------
    # PID file
    # ------------------------------------------------------------------

    def _write_pid(self):
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write_text(self.pid_file, str(os.getpid()))

    @staticmethod
    def _atomic_write_text(path: Path, text: str, *, mode: int = 0o600) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
        )
        try:
            os.fchmod(fd, mode)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass
            os.replace(tmp_name, str(path))
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def _remove_pid(self):
        try:
            self.pid_file.unlink(missing_ok=True)
        except Exception:
            pass

    def _read_pid(self) -> Optional[int]:
        try:
            return int(self.pid_file.read_text().strip())
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_signal(self, signum, frame):
        if self._logger:
            self._logger.info(f"Signal {signum} received — shutting down.")
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Core poll loop
    # ------------------------------------------------------------------

    def _poll_once(self):
        if self.connections:
            names = list(self.connections)
        elif hasattr(self._svc, "list_all_connections"):
            names = [
                c.get("name", "")
                for c in self._svc.list_all_connections()
                if c.get("name") and not c.get("error")
            ]
        else:
            names = [c["name"] for c in self._svc.list_connections()]
        metrics_snapshot: dict = {}

        for name in names:
            try:
                # monitor_any auto-dispatches: db -> get_metrics, cloud ->
                # get_cloud_metrics, monitor (SSH) -> get_remote_os_metrics.
                # This keeps the daemon, CLI and UI on the same code path,
                # so any source that shows up in `monitor-connections list`
                # can be polled here.
                r = self._svc.monitor_any(name)
                if r.get("error"):
                    self._logger.warning(f"[{name}] metrics error: {r['error']}")
                    continue

                metrics_snapshot[name] = {
                    "source":    r.get("source", ""),
                    "sections":  r["sections"],
                    "raw_floats": r.get("raw_floats", {}),
                    "timestamp": r["timestamp"],
                }

                # monitor_any already evaluates source-appropriate alerts
                # (DB threshold + OS overlay for db, OS thresholds for monitor
                # SSH targets, provider-side alerts for cloud).
                alerts = r.get("alerts") or []
                source = r.get("source") or "db"
                # Build the set of currently-active alert keys for this poll
                # (name + severity + message fingerprint).
                current_keys = {
                    f"{name}|{a.get('severity','INFO')}|{a.get('message','')}"
                    for a in alerts
                }
                # Keys that fired last poll but are gone this poll: breach cleared.
                cleared = {k for k in self._active_alert_keys if k.startswith(f"{name}|")} - current_keys
                self._active_alert_keys -= cleared
                for alert in alerts:
                    sev = alert.get("severity", "INFO")
                    msg = alert.get("message", "")
                    alert_key = f"{name}|{sev}|{msg}"
                    # Sub-source: each alert can carry its own (e.g. "os"
                    # alerts surfaced from a DB poll); fall back to the
                    # poll's source so the audit log always has something.
                    asource = alert.get("source") or source
                    self._logger.warning(f"[{name}] {sev}: {msg}")
                    if alert_key in self._active_alert_keys:
                        # Sustained breach already notified — skip dispatch.
                        continue
                    self._active_alert_keys.add(alert_key)
                    try:
                        from common.notifications import dispatch_alert

                        notify_result = dispatch_alert(f"[{sev}] {msg}", severity=sev)
                        if notify_result.get("skipped"):
                            self._logger.debug(
                                "alert not delivered: %s", notify_result["skipped"]
                            )
                        elif not notify_result.get("ok"):
                            errs = "; ".join(
                                r.get("message", "")
                                for r in notify_result.get("results", [])
                                if not r.get("ok")
                            )
                            self._logger.warning("alert delivery failed: %s", errs)
                    except Exception as ae:
                        self._logger.debug(f"dispatch_alert failed: {ae}")
                    try:
                        # Persist to <DBASSISTANT_HOME>/runtime/alerts.jsonl
                        # so ``dbtool alerts list`` / the UI can show
                        # history even after the daemon exits.
                        self._svc.log_alert(
                            sev, msg, source=asource, instance=name
                        )
                    except Exception as le:
                        self._logger.debug(f"log_alert failed: {le}")

                metric_count = sum(len(items) for _, items in r.get("sections", []))
                self._logger.info(f"[{name}] collected {metric_count} metrics, "
                                  f"{len(alerts)} alert(s)")

            except Exception as exc:
                self._logger.error(f"[{name}] poll error: {exc}")

        # Write metrics snapshot to JSON file for REST API consumption
        if metrics_snapshot:
            try:
                atomic_write_json(self.metrics_file, metrics_snapshot)
            except Exception as exc:
                self._logger.warning(f"Could not write metrics file: {exc}")

    # ------------------------------------------------------------------
    # Foreground runner (blocking)
    # ------------------------------------------------------------------

    def run_foreground(self):
        """Run in the current process. Blocks until SIGTERM/SIGINT."""
        self._logger = self._setup_logger()
        self._svc = make_service()

        try:
            signal.signal(signal.SIGTERM, self._handle_signal)
            signal.signal(signal.SIGINT,  self._handle_signal)
        except ValueError:
            # Not running in the main thread (common in tests/embedded mode).
            self._logger.warning("Signal handlers not installed outside main thread.")

        self._write_pid()
        self.pid = os.getpid()
        self._logger.info(f"Daemon started (PID {self.pid}), interval={self.interval}s, "
                          f"metrics={self.metrics_file}")

        try:
            while not self._stop_event.is_set():
                t0 = time.monotonic()
                self._logger.info("Starting poll cycle.")
                try:
                    self._poll_once()
                except Exception as exc:
                    self._logger.error(f"Poll cycle error: {exc}")
                elapsed = time.monotonic() - t0
                self._logger.info(f"Poll cycle done in {elapsed:.1f}s. "
                                  f"Next in {self.interval}s.")
                # Sleep in 1-second slices so SIGTERM is handled promptly
                for _ in range(self.interval):
                    if self._stop_event.is_set():
                        break
                    time.sleep(1)
        finally:
            self._remove_pid()
            if self._svc is not None:
                self._svc.disconnect_all()
            self._logger.info("Daemon stopped.")

    # ------------------------------------------------------------------
    # Background daemon (Unix double-fork)
    # ------------------------------------------------------------------

    def start(self):
        """
        Daemonise via double-fork (Unix only).
        After start() returns, the parent exits and the child runs in background.
        """
        if sys.platform == "win32":
            raise RuntimeError(
                "Background daemon not supported on Windows. Use --foreground."
            )

        # Check if already running
        existing = self._read_pid()
        if existing:
            try:
                os.kill(existing, 0)
                raise RuntimeError(
                    f"Daemon already running (PID {existing}). "
                    f"Run 'dbtool daemon stop' first."
                )
            except OSError:
                pass  # stale PID file

        # First fork
        try:
            pid = os.fork()
            if pid > 0:
                # Parent waits briefly so PID file is written before returning
                time.sleep(0.3)
                self.pid = self._read_pid()
                return
        except OSError as exc:
            raise RuntimeError(f"Fork #1 failed: {exc}")

        # Decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # Second fork
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError:
            sys.exit(1)

        # Redirect standard file descriptors to /dev/null
        sys.stdout.flush()
        sys.stderr.flush()
        devnull = open(os.devnull, "r+")
        os.dup2(devnull.fileno(), sys.stdin.fileno())
        os.dup2(devnull.fileno(), sys.stdout.fileno())
        os.dup2(devnull.fileno(), sys.stderr.fileno())
        devnull.close()

        self.run_foreground()
        sys.exit(0)

    # ------------------------------------------------------------------
    # Static helpers for stop / status
    # ------------------------------------------------------------------

    @staticmethod
    def stop_daemon(pid_file: str | None = None) -> dict:
        """Send SIGTERM to the daemon process. Returns {ok, message}."""
        if pid_file is None:
            pid_file = _default_pid()
        pid_path = Path(pid_file)
        try:
            pid = int(pid_path.read_text().strip())
        except Exception:
            return {"ok": False, "message": f"PID file not found: {pid_file}"}

        try:
            os.kill(pid, signal.SIGTERM)
            # Wait up to 5 seconds for it to stop
            for _ in range(50):
                time.sleep(0.1)
                try:
                    os.kill(pid, 0)
                except OSError:
                    pid_path.unlink(missing_ok=True)
                    return {"ok": True, "message": f"Daemon (PID {pid}) stopped."}
            # Force kill
            os.kill(pid, signal.SIGKILL)
            pid_path.unlink(missing_ok=True)
            return {"ok": True, "message": f"Daemon (PID {pid}) force-killed."}
        except ProcessLookupError:
            pid_path.unlink(missing_ok=True)
            return {"ok": False, "message": f"PID {pid} not found (stale PID file removed)."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    @staticmethod
    def daemon_status(pid_file: str | None = None) -> dict:
        """Check whether the daemon is running. Returns {running, pid, message}."""
        if pid_file is None:
            pid_file = _default_pid()
        pid_path = Path(pid_file)
        try:
            pid = int(pid_path.read_text().strip())
        except Exception:
            return {"running": False, "pid": None, "message": "Daemon is not running."}

        try:
            os.kill(pid, 0)
            return {"running": True, "pid": pid,
                    "message": f"Daemon is running (PID {pid})."}
        except OSError:
            return {"running": False, "pid": None,
                    "message": f"Daemon is NOT running (stale PID {pid})."}
