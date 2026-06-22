#!/usr/bin/env bash
# =============================================================================
#  install.sh  —  DbManagementTool installer (Linux/macOS bootstrap shim)
#
#  Supported platforms : macOS (Intel + Apple Silicon), Ubuntu/Debian,
#                        Fedora/RHEL/Rocky/AlmaLinux, Arch/Manjaro, openSUSE
#  Usage               : bash setup/install.sh   (or bash install.sh from project root)
#  Options             : --module full|core|migrator|ai|monitor
#                        --no-optional   skip cloud/AI packages
#                        --no-venv       install into current Python only
#                        --python PATH   use a specific Python interpreter
#
#  RESPONSIBILITIES (bootstrap only):
#    • detect OS + package manager
#    • find/validate Python 3.10+ (with install guidance)
#    • install SYSTEM packages pip cannot (tkinter, libffi/openssl, libpq, sshpass)
#    • hand off to setup/install.py, which owns everything portable:
#        venv creation, pip installs, config files, import verification,
#        launcher generation, and the package/import summary.
#
#  The script NEVER aborts on a single system-package failure; it records every
#  problem and prints a remediation guide at the end. It DOES stop early if no
#  Python 3.10+ interpreter is available (nothing else can proceed).
# =============================================================================

# ── colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
fail()    { echo -e "${RED}[FAIL]${RESET}  $*"; }
section() { echo -e "\n${BOLD}━━━  $*  ━━━${RESET}"; }

# ── option parsing ────────────────────────────────────────────────────────────
SKIP_OPTIONAL=false
SKIP_VENV=false
PYTHON_BIN=""
MODULE="full"
_prev=""
for arg in "$@"; do
    case "$arg" in
        --no-optional) SKIP_OPTIONAL=true ;;
        --no-venv)     SKIP_VENV=true ;;
        --module)      _prev="module" ;;
        --module=*)    MODULE="${arg#*=}" ;;
        --python)      _prev="python" ;;
        --python=*)    PYTHON_BIN="${arg#*=}" ;;
        *)
            if [ "$_prev" = "module" ]; then MODULE="$arg"; _prev=""; fi
            if [ "$_prev" = "python" ]; then PYTHON_BIN="$arg"; _prev=""; fi
            ;;
    esac
done

# Project root (install.sh lives in setup/)
SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SETUP_DIR/.." && pwd)"
cd "$ROOT_DIR" || exit 1

# ── issue tracker (system-level only; package/import issues live in install.py) ─
ISSUES=()
WARNINGS=()

add_issue()   { ISSUES+=("$*"); fail "$*"; }
add_warning() { WARNINGS+=("$*"); warn "$*"; }

# ── helper: run a command, return its exit code, never abort ─────────────────
try_cmd() {
    "$@" >/dev/null 2>&1
    return $?
}

# ── helper: run with optional sudo ───────────────────────────────────────────
_SUDO_CACHED=false

ensure_sudo() {
    if $_SUDO_CACHED; then return 0; fi
    echo ""
    warn "Some system packages need elevated privileges."
    if sudo -v 2>/dev/null; then
        _SUDO_CACHED=true
        ok "sudo credentials cached."
        return 0
    else
        add_warning "sudo not available or password rejected — system packages skipped."
        return 1
    fi
}

# =============================================================================
# 1. DETECT OPERATING SYSTEM
# =============================================================================
section "Detecting operating system"

OS_TYPE=""
PKG_MGR=""
PKG_INSTALL=""

case "$(uname -s)" in
    Darwin)
        OS_TYPE="macos"
        info "Detected: macOS $(sw_vers -productVersion 2>/dev/null || uname -r)"
        if command -v brew >/dev/null 2>&1; then
            PKG_MGR="brew"
            PKG_INSTALL="brew install"
            ok "Homebrew found at $(command -v brew)"
        else
            add_warning "Homebrew not found — system package installs will be skipped for macOS."
            info "  To install Homebrew: /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        fi
        ;;
    Linux)
        OS_TYPE="linux"
        if [ -f /etc/os-release ]; then
            . /etc/os-release
            info "Detected: ${PRETTY_NAME:-Linux}"
        else
            info "Detected: Linux (unknown distro)"
        fi
        if command -v apt-get >/dev/null 2>&1; then
            PKG_MGR="apt"; PKG_INSTALL="apt-get install -y"
        elif command -v dnf >/dev/null 2>&1; then
            PKG_MGR="dnf"; PKG_INSTALL="dnf install -y"
        elif command -v yum >/dev/null 2>&1; then
            PKG_MGR="yum"; PKG_INSTALL="yum install -y"
        elif command -v pacman >/dev/null 2>&1; then
            PKG_MGR="pacman"; PKG_INSTALL="pacman -S --noconfirm"
        elif command -v zypper >/dev/null 2>&1; then
            PKG_MGR="zypper"; PKG_INSTALL="zypper install -y"
        else
            add_warning "No recognised package manager found (apt/dnf/yum/pacman/zypper)."
        fi
        [ -n "$PKG_MGR" ] && ok "Package manager: $PKG_MGR"
        ;;
    MINGW*|CYGWIN*|MSYS*)
        OS_TYPE="windows"
        warn "Windows detected. Prefer install.bat; this bash script needs Git Bash or WSL."
        add_warning "Windows: Tkinter, Oracle client, and sshpass require manual installation."
        ;;
    *)
        OS_TYPE="unknown"
        add_warning "Unknown OS: $(uname -s). Proceeding with best-effort checks."
        ;;
esac

# =============================================================================
# 2. FIND PYTHON 3.10+
# =============================================================================
section "Checking Python"

PYTHON=""
MIN_MAJOR=3; MIN_MINOR=10

find_python() {
    for candidate in "$PYTHON_BIN" python3.12 python3.11 python3.10 python3 python; do
        [ -z "$candidate" ] && continue
        if command -v "$candidate" >/dev/null 2>&1; then
            ver=$("$candidate" -c "import sys; print(sys.version_info[:2])" 2>/dev/null)
            major=$(echo "$ver" | tr -d '(),' | awk '{print $1}')
            minor=$(echo "$ver" | tr -d '(),' | awk '{print $2}')
            if [ "$major" -ge "$MIN_MAJOR" ] && [ "$minor" -ge "$MIN_MINOR" ] 2>/dev/null; then
                PYTHON="$candidate"
                return 0
            fi
        fi
    done
    return 1
}

# Print OS-specific, copy-pasteable instructions for installing Python 3.10+
# (3.12 recommended). Called whenever no suitable interpreter is found.
print_python_install_help() {
    echo ""
    echo -e "${BOLD}How to install Python ${MIN_MAJOR}.${MIN_MINOR}+ (3.12 recommended):${RESET}"
    case "$OS_TYPE" in
        macos)
            if [ "$PKG_MGR" = "brew" ]; then
                echo "  • Homebrew (recommended):"
                echo "      brew install python@3.12"
                echo "      # then re-run:  bash install.sh"
            else
                echo "  • Homebrew is not installed. Install it first:"
                echo "      /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                echo "      brew install python@3.12"
            fi
            echo "  • Official installer (.pkg, no Homebrew needed):"
            echo "      https://www.python.org/downloads/macos/"
            echo "  • pyenv (manage multiple versions):"
            echo "      brew install pyenv && pyenv install 3.12 && pyenv local 3.12"
            ;;
        linux)
            case "$PKG_MGR" in
                apt)
                    echo "  • Ubuntu/Debian:"
                    echo "      sudo apt-get update"
                    echo "      sudo apt-get install -y python3.12 python3.12-venv python3.12-dev"
                    echo "      # if 3.12 is not in your repos, add the deadsnakes PPA (Ubuntu):"
                    echo "      sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt-get update"
                    ;;
                dnf|yum)
                    echo "  • Fedora/RHEL/Rocky/Alma:"
                    echo "      sudo $PKG_MGR install -y python3.12 python3.12-devel"
                    ;;
                pacman)
                    echo "  • Arch/Manjaro (ships current Python):"
                    echo "      sudo pacman -S python"
                    ;;
                zypper)
                    echo "  • openSUSE:"
                    echo "      sudo zypper install -y python312 python312-devel"
                    ;;
                *)
                    echo "  • Use your distro's package manager to install python3.10+ (with venv + dev headers)."
                    ;;
            esac
            echo "  • Or build/manage via pyenv:"
            echo "      curl -fsSL https://pyenv.run | bash && pyenv install 3.12 && pyenv local 3.12"
            ;;
        windows)
            echo "  • winget:"
            echo "      winget install -e --id Python.Python.3.12"
            echo "  • Official installer (tick 'Add python.exe to PATH'):"
            echo "      https://www.python.org/downloads/windows/"
            ;;
        *)
            echo "  • Download from: https://www.python.org/downloads/"
            ;;
    esac
    echo ""
    echo "  After installing, re-run:  bash install.sh"
    echo ""
}

if find_python; then
    PY_VER=$("$PYTHON" --version 2>&1)
    ok "$PY_VER found at $(command -v "$PYTHON")"
else
    add_issue "Python $MIN_MAJOR.$MIN_MINOR+ not found."
    warn "Attempting to install Python via system package manager..."
    if [ "$OS_TYPE" = "macos" ] && [ "$PKG_MGR" = "brew" ]; then
        if try_cmd brew install python@3.12; then
            ok "Python 3.12 installed via Homebrew."
            find_python || add_issue "Homebrew Python still not detected on PATH."
        else
            add_issue "Failed to install Python via Homebrew."
        fi
    elif [ "$OS_TYPE" = "linux" ] && [ -n "$PKG_MGR" ]; then
        if ensure_sudo; then
            # Prefer an explicit 3.12 package; fall back to distro default python3.
            if sudo $PKG_INSTALL python3.12 python3.12-venv python3.12-dev 2>/dev/null \
               || sudo $PKG_INSTALL python3 python3-dev python3-pip 2>/dev/null; then
                ok "Python installed via $PKG_MGR."
                find_python || add_issue "Installed Python is still older than $MIN_MAJOR.$MIN_MINOR."
            else
                add_issue "Failed to install Python via $PKG_MGR."
            fi
        fi
    fi

    if [ -z "$PYTHON" ]; then
        print_python_install_help
    fi
fi

if [ -z "$PYTHON" ]; then
    add_issue "No Python $MIN_MAJOR.$MIN_MINOR+ interpreter available — cannot create the venv or install packages."
    echo ""
    echo -e "${RED}${BOLD}✖  Installation cannot continue without Python $MIN_MAJOR.$MIN_MINOR+.${RESET}"
    echo -e "${YELLOW}Install a supported Python using the instructions above, then re-run: bash install.sh${RESET}"
    echo ""
    exit 1
fi

# =============================================================================
# 3. TKINTER  (system package — pip CANNOT install it)
# =============================================================================
section "Checking Tkinter (system package)"

# Minor version of the chosen interpreter (e.g. 12) — used to pick the
# version-matched Tk package (brew python-tk@3.12, apt python3.12-tk, ...).
PY_MINOR="$("$PYTHON" -c 'import sys; print(sys.version_info[1])' 2>/dev/null)"
[ -z "$PY_MINOR" ] && PY_MINOR=""

tkinter_ok() { "$PYTHON" -c "import tkinter" >/dev/null 2>&1; }

# Print every correct way to install Tkinter for the current platform.
print_tkinter_help() {
    echo ""
    echo -e "${BOLD}How to install Tkinter (no pip package exists — it is system/interpreter level):${RESET}"
    case "$OS_TYPE" in
        macos)
            echo "  • Homebrew Python : brew install python-tk@3.${PY_MINOR:-12}   (must match your python minor)"
            echo "                      (generic fallback: brew install python-tk)"
            echo "  • python.org build: reinstall from https://www.python.org/downloads/macos/ (Tk is bundled)"
            echo "  • pyenv           : brew install tcl-tk, then rebuild Python with"
            echo "                      PYTHON_CONFIGURE_OPTS=\"--with-tcltk-includes/-libs=...\" pyenv install 3.${PY_MINOR:-12}"
            echo "  • conda           : conda install tk"
            ;;
        linux)
            echo "  • Debian/Ubuntu   : sudo apt-get install python3.${PY_MINOR:-12}-tk   (or python3-tk)"
            echo "  • Fedora/RHEL     : sudo dnf install python3-tkinter"
            echo "  • Arch/Manjaro    : sudo pacman -S tk"
            echo "  • openSUSE        : sudo zypper install python3-tk"
            echo "  • conda           : conda install tk"
            ;;
        windows)
            echo "  • Re-run the python.org installer and tick 'tcl/tk and IDLE'."
            echo "  • Avoid the Microsoft Store build of Python (Tk support is unreliable)."
            ;;
        *)
            echo "  • Install your platform's Tk/Tkinter package for Python 3.${PY_MINOR:-x}."
            ;;
    esac
    echo ""
}

# Attempt a version-matched install, then any generic fallbacks. Each helper
# returns 0 only if the package manager command itself succeeds.
_brew_install() { try_cmd brew install "$1"; }
_sudo_pkg_install() { sudo $PKG_INSTALL "$@" >/dev/null 2>&1; }

if tkinter_ok; then
    ok "Tkinter is available."
else
    warn "Tkinter not found for $("$PYTHON" --version 2>&1) — attempting install (the GUI needs it)..."

    if [ "$OS_TYPE" = "macos" ]; then
        if [ "$PKG_MGR" = "brew" ]; then
            # Version-matched formula first (python-tk@3.12), then generic.
            if [ -n "$PY_MINOR" ] && _brew_install "python-tk@3.$PY_MINOR"; then
                ok "Installed python-tk@3.$PY_MINOR via Homebrew."
            elif _brew_install "python-tk"; then
                ok "Installed python-tk via Homebrew."
            else
                add_warning "Homebrew could not install a python-tk formula."
            fi
        else
            add_warning "Homebrew not available — cannot auto-install Tkinter on macOS."
        fi

    elif [ "$OS_TYPE" = "linux" ] && [ -n "$PKG_MGR" ]; then
        if ensure_sudo; then
            case "$PKG_MGR" in
                apt)
                    # Prefer the versioned package (deadsnakes/py3.12), fall back to python3-tk.
                    if { [ -n "$PY_MINOR" ] && _sudo_pkg_install "python3.$PY_MINOR-tk"; } \
                       || _sudo_pkg_install "python3-tk"; then
                        ok "Tkinter installed via apt."
                    else
                        add_warning "apt could not install python3-tk."
                    fi
                    ;;
                dnf|yum)
                    if _sudo_pkg_install "python3-tkinter" \
                       || { [ -n "$PY_MINOR" ] && _sudo_pkg_install "python3.$PY_MINOR-tkinter"; }; then
                        ok "Tkinter installed via $PKG_MGR."
                    else
                        add_warning "$PKG_MGR could not install python3-tkinter."
                    fi
                    ;;
                pacman)
                    if _sudo_pkg_install "tk"; then ok "Tkinter (tk) installed via pacman."
                    else add_warning "pacman could not install tk."; fi
                    ;;
                zypper)
                    if _sudo_pkg_install "python3-tk" \
                       || { [ -n "$PY_MINOR" ] && _sudo_pkg_install "python3$PY_MINOR-tk"; }; then
                        ok "Tkinter installed via zypper."
                    else
                        add_warning "zypper could not install python3-tk."
                    fi
                    ;;
            esac
        fi
    elif [ "$OS_TYPE" = "windows" ]; then
        add_warning "On Windows, Tkinter ships with the python.org installer — it cannot be auto-installed here."
    else
        add_warning "No package manager available to install Tkinter."
    fi

    # Re-verify after the attempt; if it still fails, this is a critical issue
    # (the GUI cannot launch) and we print every manual method.
    if tkinter_ok; then
        ok "Tkinter now importable."
    else
        add_issue "Tkinter still not importable for this Python — the GUI will not launch."
        print_tkinter_help
    fi
fi

# =============================================================================
# 4. SYSTEM BUILD LIBRARIES  (C headers some pip wheels need to build)
# =============================================================================
section "Checking system build libraries"

install_sys_pkg() {
    local label="$1"; shift
    if [ "$OS_TYPE" = "macos" ] && [ "$PKG_MGR" = "brew" ]; then
        if try_cmd brew install "$@"; then
            ok "$label installed via Homebrew."
            return 0
        fi
    elif ensure_sudo 2>/dev/null; then
        if sudo $PKG_INSTALL "$@" >/dev/null 2>&1; then
            ok "$label installed."
            return 0
        fi
    fi
    add_warning "$label not installed — some pip packages may fail to build."
    return 1
}

# libffi + openssl (cryptography, paramiko). The CDLL probe checks the system
# lib directly; we don't rely on importing cryptography (not installed yet).
if "$PYTHON" -c "import ctypes; ctypes.CDLL('libffi.so.8' if __import__('sys').platform=='linux' else 'libffi.dylib')" >/dev/null 2>&1; then
    ok "libffi / openssl: available."
else
    warn "libffi/openssl headers may be missing — needed to build cryptography."
    if [ "$OS_TYPE" = "linux" ]; then
        case "$PKG_MGR" in
            apt)     install_sys_pkg "libffi-dev + libssl-dev" libffi-dev libssl-dev build-essential ;;
            dnf|yum) install_sys_pkg "libffi-devel + openssl-devel" libffi-devel openssl-devel gcc ;;
            pacman)  install_sys_pkg "libffi + openssl" libffi openssl ;;
            zypper)  install_sys_pkg "libffi-devel" libffi-devel libopenssl-devel ;;
        esac
    elif [ "$OS_TYPE" = "macos" ] && [ "$PKG_MGR" = "brew" ]; then
        install_sys_pkg "libffi + openssl" libffi openssl
    fi
fi

# libpq (psycopg2 build). psycopg2-binary ships a wheel, but headers help when
# building from source on some distros.
if [ "$OS_TYPE" = "linux" ]; then
    case "$PKG_MGR" in
        apt)     install_sys_pkg "libpq-dev" libpq-dev >/dev/null 2>&1 || true ;;
        dnf|yum) install_sys_pkg "postgresql-devel" postgresql-devel >/dev/null 2>&1 || true ;;
        pacman)  install_sys_pkg "postgresql-libs" postgresql-libs >/dev/null 2>&1 || true ;;
    esac
elif [ "$OS_TYPE" = "macos" ] && [ "$PKG_MGR" = "brew" ]; then
    try_cmd brew install libpq && ok "libpq available." || true
fi

# =============================================================================
# 5. OPTIONAL SYSTEM TOOLS
# =============================================================================
section "Checking optional system tools"

# sshpass — SSH password-based monitoring
if command -v sshpass >/dev/null 2>&1; then
    ok "sshpass: $(sshpass -V 2>&1 | head -1)"
else
    warn "sshpass not found — SSH password-based monitoring disabled."
    if [ "$OS_TYPE" = "macos" ] && [ "$PKG_MGR" = "brew" ]; then
        try_cmd brew install hudochenkov/sshpass/sshpass && ok "sshpass installed via Homebrew." \
            || add_warning "sshpass not installed — SSH password monitoring will not work."
    elif [ "$OS_TYPE" = "linux" ] && [ -n "$PKG_MGR" ] && ensure_sudo 2>/dev/null; then
        sudo $PKG_INSTALL sshpass >/dev/null 2>&1 && ok "sshpass installed." \
            || add_warning "sshpass not installed — SSH password monitoring will not work."
    else
        add_warning "sshpass not installed — SSH password monitoring will not work."
    fi
fi

# AWS / Azure CLI (optional, informational only)
command -v aws >/dev/null 2>&1 && ok "AWS CLI: $(aws --version 2>&1)" \
    || add_warning "AWS CLI not installed (optional). Install: https://aws.amazon.com/cli/"
command -v az >/dev/null 2>&1 && ok "Azure CLI detected." \
    || add_warning "Azure CLI not installed (optional). Install: https://aka.ms/installazurecli"

# Oracle Instant Client cannot be auto-installed (licence). Thin-mode oracledb
# (a pip package) covers most needs; Instant Client is only for thick mode.
info "Oracle thick mode (optional) needs Instant Client: https://www.oracle.com/database/technologies/instant-client.html"

# =============================================================================
# 6. DELEGATE TO setup/install.py  (venv + pip + verify + launchers + summary)
# =============================================================================
section "Installing Python packages (module=$MODULE)"

INSTALL_PY_ARGS=(--root "$ROOT_DIR" --module "$MODULE")
$SKIP_OPTIONAL && INSTALL_PY_ARGS+=(--no-optional)
$SKIP_VENV     && INSTALL_PY_ARGS+=(--skip-venv)

info "Handing off to: $PYTHON setup/install.py ${INSTALL_PY_ARGS[*]}"
if "$PYTHON" "$ROOT_DIR/setup/install.py" "${INSTALL_PY_ARGS[@]}"; then
    ok "Python installer finished successfully."
    INSTALL_PY_OK=true
else
    add_issue "Python installer (setup/install.py) reported errors — see its output above."
    INSTALL_PY_OK=false
fi

# =============================================================================
# 7. SYSTEM-LEVEL SUMMARY  (package/import details are printed by install.py)
# =============================================================================
section "Installation Summary (system prerequisites)"

TOTAL_ISSUES=${#ISSUES[@]}
TOTAL_WARNINGS=${#WARNINGS[@]}

echo ""
if [ $TOTAL_ISSUES -eq 0 ] && [ $TOTAL_WARNINGS -eq 0 ]; then
    echo -e "${GREEN}${BOLD}✔  System prerequisites OK.${RESET}"
else
    if [ $TOTAL_ISSUES -gt 0 ]; then
        echo -e "${RED}${BOLD}✖  $TOTAL_ISSUES system issue(s):${RESET}"
        for i in "${!ISSUES[@]}"; do
            echo -e "   ${RED}$(($i+1)).${RESET} ${ISSUES[$i]}"
        done
    fi
    if [ $TOTAL_WARNINGS -gt 0 ]; then
        echo ""
        echo -e "${YELLOW}${BOLD}⚠  $TOTAL_WARNINGS warning(s):${RESET}"
        for i in "${!WARNINGS[@]}"; do
            echo -e "   ${YELLOW}$(($i+1)).${RESET} ${WARNINGS[$i]}"
        done
    fi
fi

echo ""
echo -e "${BOLD}── System remediation (pip/import details are above, from install.py) ──${RESET}"
echo ""
echo -e "${BOLD}Python ${MIN_MAJOR}.${MIN_MINOR}+ (3.12 recommended):${RESET}"
echo "  macOS  : brew install python@3.12   (or https://www.python.org/downloads/macos/)"
echo "  Ubuntu : sudo apt-get install python3.12 python3.12-venv python3.12-dev"
echo "           (add 'sudo add-apt-repository ppa:deadsnakes/ppa' if 3.12 is missing)"
echo "  Fedora : sudo dnf install python3.12 python3.12-devel"
echo "  Arch   : sudo pacman -S python"
echo "  openSUSE: sudo zypper install python312 python312-devel"
echo ""
echo -e "${BOLD}Tkinter (system package — no pip; match your python minor ${PY_MINOR:-X}):${RESET}"
echo "  macOS/brew    : brew install python-tk@3.${PY_MINOR:-12}  (or python.org build / conda install tk)"
echo "  Ubuntu/Debian : sudo apt-get install python3.${PY_MINOR:-12}-tk  (or python3-tk)"
echo "  Fedora/RHEL   : sudo dnf install python3-tkinter"
echo "  Arch          : sudo pacman -S tk"
echo "  openSUSE      : sudo zypper install python3-tk"
echo "  Windows       : reinstall python.org build with 'tcl/tk and IDLE'"
echo ""
echo -e "${BOLD}cryptography / psycopg2 build deps:${RESET}"
echo "  Ubuntu : sudo apt-get install libffi-dev libssl-dev libpq-dev build-essential"
echo "  Fedora : sudo dnf install libffi-devel openssl-devel postgresql-devel gcc"
echo "  macOS  : brew install libffi openssl libpq"
echo ""
echo -e "${BOLD}sshpass (SSH password monitoring):${RESET}"
echo "  macOS  : brew install hudochenkov/sshpass/sshpass"
echo "  Ubuntu : sudo apt-get install sshpass"
echo "  Fedora : sudo dnf install sshpass"
echo ""

echo -e "${BOLD}── How to run the tool ────────────────────────────────────────────${RESET}"
echo "  Install module     :  bash setup/install.sh --module migrator|ai|monitor|full|core"
echo "  Full GUI (Linux)   :  bash run.sh   (or bash setup/run.sh)"
echo "  Full GUI (Windows) :  run.bat"
echo "  Shell UI (bash menu — no tkinter):"
echo "    Monitoring     :  bash monitoring/run_monitor.sh"
echo "    Data Migration :  bash schema_converter/run_schema_converter.sh"
echo "    AI assistant   :  bash ai_query/run_ai_query_assistant.sh"
echo "  Full module UI   :  python dbtool.py ui --module migrator|ai|monitor"
echo "  Manual (venv)    :  source .venv/bin/activate && python conDbUi.py"
echo ""

if [ $TOTAL_ISSUES -eq 0 ] && [ "${INSTALL_PY_OK:-false}" = "true" ]; then
    echo -e "${GREEN}${BOLD}Ready! Run:  bash run.sh${RESET}"
    exit 0
else
    echo -e "${YELLOW}${BOLD}Resolve the issues above, then re-run: bash setup/install.sh${RESET}"
    exit 1
fi
echo ""
