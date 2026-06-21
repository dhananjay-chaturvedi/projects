#!/usr/bin/env bash
# Convenience wrapper — canonical installer is setup/install.sh
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#check if the root directory is not empty
if [ -z "$ROOT" ]; then
    echo "root directory is empty"
    exit 1
fi
echo "root directory found: $ROOT"
#check if the root directory is a directory
if [ ! -d "$ROOT" ]; then
    echo "root directory is not a directory"
    exit 1
fi
echo "root directory is a directory: $ROOT"
#check if the setup directory is not empty
if [ -z "$ROOT/setup" ]; then
    echo "setup directory is empty"
    exit 1
fi
echo "setup directory found: $ROOT/setup"
#check if the setup directory is a directory
if [ ! -d "$ROOT/setup" ]; then
    echo "setup directory is not a directory"
    exit 1
fi
echo "setup directory is a directory: $ROOT/setup"
#check if the setup directory contains install.sh
if [ ! -f "$ROOT/setup/install.sh" ]; then
    echo "install.sh not found in setup directory"
    exit 1
fi
echo "install.sh found in setup directory"
#check if the install.sh is executable
if [ ! -x "$ROOT/setup/install.sh" ]; then
    echo "install.sh is not executable, making it executable"
    chmod +x "$ROOT/setup/install.sh"
fi
echo "running $ROOT/setup/install.sh"
# Belt-and-suspenders: stop a global PIP_USER=1 from redirecting installs to
# the user site-packages instead of the project venv. Only affects this
# installer process tree, not your interactive shell or global Python.
# (The authoritative, venv-only guard lives in setup/install.py.)
export PIP_USER=0
exec "$ROOT/setup/install.sh" "$@"
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "install.sh failed with exit code $exit_code"
    exit $exit_code
fi
exit $exit_code
