import os
import signal
import sys
from pathlib import Path

# Resolve PID directory through the central paths module so the location
# follows DBASSISTANT_HOME / runtime_dir overrides. We import lazily so a
# damaged top-level package import doesn't take this script down.
try:
    from common import paths as _paths

    _PID_DIR = _paths.runtime_dir()
except Exception:
    _PID_DIR = Path.home() / ".dbassistant" / "runtime"

MONITORS = {
    "aws": "monitor_aws.py",
    "azure": "monitor_azure.py",
    "gcp": "monitor_gcp.py",
}


def pid_file(name):
    return str(Path(_PID_DIR) / f"{name}.pid")


def stop(name):
    path = pid_file(name)
    if not os.path.exists(path):
        print(f"[{name}] No PID file found at {path} — is it running?")
        return

    with open(path) as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        os.remove(path)
        print(f"[{name}] Sent SIGTERM to PID {pid} — monitor stopped.")
    except ProcessLookupError:
        os.remove(path)
        print(
            f"[{name}] PID {pid} not found — already stopped. Removed stale PID file."
        )
    except PermissionError:
        print(f"[{name}] Permission denied to kill PID {pid}.")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in MONITORS:
        print(f"Usage: python3 stop.py <{'|'.join(MONITORS)}>")
        sys.exit(1)

    stop(sys.argv[1])
