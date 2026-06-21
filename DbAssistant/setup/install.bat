@echo off
REM ============================================================================
REM  install.bat  --  DbManagementTool installer (Windows bootstrap shim)
REM
REM  Supported : Windows 10 (1809+) and Windows 11
REM  Usage     : double-click, or:  install.bat [options]
REM              or:  setup\install.bat [options]
REM  Options   : --module {full|core|migrator|ai|monitor}   (default: full)
REM              --no-optional   skip cloud / AI packages
REM              --no-venv       install into current Python (no .venv)
REM              --python PATH   full path to python.exe to use
REM
REM  RESPONSIBILITIES (bootstrap only):
REM    * check Windows environment / privileges
REM    * find/validate Python 3.10+ (with install guidance)
REM    * verify Tkinter (ships with the python.org build; cannot pip-install)
REM    * hand off to setup\install.py, which owns everything portable:
REM        venv creation, pip installs, config files, import verification,
REM        launcher generation (run.bat / run_cli.bat / run_api.bat), summary.
REM ============================================================================

setlocal enableextensions enabledelayedexpansion

REM ---- Locate project root (this script lives in setup\ inside the repo) ----
set "SETUP_DIR=%~dp0"
if "%SETUP_DIR:~-1%"=="\" set "SETUP_DIR=%SETUP_DIR:~0,-1%"
for %%I in ("%SETUP_DIR%\..") do set "ROOT_DIR=%%~fI"

cd /d "%ROOT_DIR%"

REM ---- Parse options ---------------------------------------------------------
set "MODULE=full"
set "SKIP_OPTIONAL="
set "SKIP_VENV="
set "PYTHON_BIN="

:parse_args
if "%~1"=="" goto parse_done
if /I "%~1"=="--module" (set "MODULE=%~2" & shift & shift & goto parse_args)
if /I "%~1"=="--no-optional" (set "SKIP_OPTIONAL=1" & shift & goto parse_args)
if /I "%~1"=="--no-venv" (set "SKIP_VENV=1" & shift & goto parse_args)
if /I "%~1"=="--python" (set "PYTHON_BIN=%~2" & shift & shift & goto parse_args)
echo [WARN] Unknown option: %~1 -- ignoring
shift
goto parse_args
:parse_done

set "ISSUES=0"
set "WARNINGS=0"

REM ---- Header ----------------------------------------------------------------
echo.
echo ============================================================================
echo  DbManagementTool Windows Installer (bootstrap)
echo ============================================================================
echo [INFO] Module: %MODULE%
echo [INFO] Root:   %ROOT_DIR%

REM ============================================================================
REM 1. Windows environment + privilege checks
REM ============================================================================
echo.
echo ---- Checking Windows environment ----

net session >nul 2>nul
if errorlevel 1 (
    echo [WARN] Not running as Administrator. Some system installs may need elevation.
    set /A WARNINGS+=1
) else (
    echo [ OK ] Running as Administrator.
)

where winget >nul 2>nul
if errorlevel 1 (
    echo [WARN] winget not found. Install from https://aka.ms/getwinget
    set /A WARNINGS+=1
) else (
    echo [ OK ] winget is available.
)

REM ============================================================================
REM 2. Detect Python (>= 3.10)
REM ============================================================================
echo.
echo ---- Checking Python ----

if not "%PYTHON_BIN%"=="" (
    call :probe_python "%PYTHON_BIN%"
    if errorlevel 1 (
        echo [FAIL] %PYTHON_BIN% is not a usable Python ^>=3.10.
        set "PYTHON_BIN="
    )
)

if "%PYTHON_BIN%"=="" (
    where py >nul 2>nul
    if !ERRORLEVEL! EQU 0 (
        for %%V in (3.12 3.11 3.10) do (
            if "!PYTHON_BIN!"=="" (
                py -%%V -c "import sys" >nul 2>nul
                if !ERRORLEVEL! EQU 0 set "PYTHON_BIN=py -%%V"
            )
        )
    )
)

if "%PYTHON_BIN%"=="" (
    for %%E in (python3 python) do (
        if "!PYTHON_BIN!"=="" (
            where %%E >nul 2>nul
            if !ERRORLEVEL! EQU 0 (
                call :probe_python "%%E"
                if !ERRORLEVEL! EQU 0 set "PYTHON_BIN=%%E"
            )
        )
    )
)

if "%PYTHON_BIN%"=="" (
    echo [FAIL] Python ^>= 3.10 not found.
    echo.
    echo  How to install Python 3.10+ ^(3.12 recommended^):
    echo    * winget : winget install -e --id Python.Python.3.12
    echo    * Official installer ^(tick "Add python.exe to PATH" + "tcl/tk and IDLE"^):
    echo        https://www.python.org/downloads/windows/
    echo    * Avoid the Microsoft Store build ^(it omits Tkinter^).
    echo    After installing, re-run: install.bat
    set /A ISSUES+=1
    goto :summary
)

for /f "delims=" %%V in ('%PYTHON_BIN% --version 2^>^&1') do set "PY_VER=%%V"
echo [ OK ] %PY_VER%  ^(%PYTHON_BIN%^)

REM ============================================================================
REM 3. Tkinter availability (cannot be pip-installed)
REM ============================================================================
echo.
echo ---- Checking Tkinter ----

%PYTHON_BIN% -c "import tkinter" >nul 2>nul
if errorlevel 1 (
    echo [WARN] Tkinter not available -- GUI will not launch.
    echo        Re-install Python from python.org with the "tcl/tk and IDLE" option.
    echo        The Microsoft Store build of Python omits Tkinter -- avoid it.
    set /A WARNINGS+=1
) else (
    echo [ OK ] Tkinter is available.
)

REM ============================================================================
REM 4. Delegate the heavy lifting to setup\install.py
REM    (venv creation, pip installs, config, verification, launcher scripts)
REM ============================================================================
echo.
echo ---- Running setup\install.py --module %MODULE% ----

set "INSTALL_ARGS=--root "%ROOT_DIR%" --module %MODULE%"
if defined SKIP_OPTIONAL set "INSTALL_ARGS=%INSTALL_ARGS% --no-optional"
if defined SKIP_VENV set "INSTALL_ARGS=%INSTALL_ARGS% --skip-venv"

%PYTHON_BIN% "%SETUP_DIR%\install.py" %INSTALL_ARGS%
set "INSTALL_RC=%ERRORLEVEL%"

if not "%INSTALL_RC%"=="0" (
    echo [FAIL] setup\install.py exited %INSTALL_RC%
    set /A ISSUES+=1
) else (
    echo [ OK ] setup\install.py finished successfully.
)

REM ============================================================================
REM 5. Optional tools (informational only)
REM ============================================================================
echo.
echo ---- Optional tools ----

where aws >nul 2>nul
if errorlevel 1 (
    echo [WARN] AWS CLI not found ^(optional^).  winget install Amazon.AWSCLI
    set /A WARNINGS+=1
) else (
    echo [ OK ] AWS CLI detected.
)

where az >nul 2>nul
if errorlevel 1 (
    echo [WARN] Azure CLI not found ^(optional^).  winget install Microsoft.AzureCLI
    set /A WARNINGS+=1
) else (
    echo [ OK ] Azure CLI detected.
)

where ssh >nul 2>nul
if errorlevel 1 (
    echo [WARN] ssh not found -- server OS monitoring over SSH disabled.
    echo        Enable: Settings -^> Apps -^> Optional Features -^> Add "OpenSSH Client".
    set /A WARNINGS+=1
) else (
    echo [ OK ] OpenSSH client detected.
)

REM ============================================================================
REM 6. Summary
REM ============================================================================
:summary
echo.
echo ============================================================================
echo  Installation Summary (system prerequisites)
echo ============================================================================
echo Critical issues : %ISSUES%
echo Warnings        : %WARNINGS%
echo.
echo ---- How to run (launchers generated by setup\install.py) ----
echo   GUI            : run.bat
echo   CLI            : run_cli.bat connections list
echo   REST API       : run_api.bat
echo   Activate venv  : .venv\Scripts\activate.bat
echo.
echo ---- System remediation (pip/import details are above, from install.py) ----
echo   Python missing      : https://www.python.org/downloads/  ^(tick "Add to PATH" + "tcl/tk"^)
echo   Tkinter missing     : re-install python.org Python with the "tcl/tk and IDLE" option
echo   Visual C++ tools    : https://visualstudio.microsoft.com/visual-cpp-build-tools/
echo   Oracle (thick mode) : Instant Client https://www.oracle.com/database/technologies/instant-client.html
echo.

if "%ISSUES%"=="0" (
    echo [ OK ] Ready. Double-click run.bat to launch the GUI.
    set "FINAL_RC=0"
) else (
    echo [FAIL] Fix the %ISSUES% critical issue^(s^) above, then re-run install.bat.
    set "FINAL_RC=1"
)

exit /b %FINAL_RC%


REM ============================================================================
REM Helper: verify a candidate is Python >= 3.10
REM ============================================================================
:probe_python
%~1 -c "import sys;assert sys.version_info >= (3,10), sys.version_info" >nul 2>nul
exit /b %ERRORLEVEL%
