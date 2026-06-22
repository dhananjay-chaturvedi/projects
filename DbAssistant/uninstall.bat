@echo off
REM DbManagementTool - Windows uninstaller launcher.
REM
REM Usage:
REM   uninstall.bat              -- interactive (single PURGE prompt)
REM   uninstall.bat --purge      -- non-interactive, also delete project root
REM   uninstall.bat --no-purge   -- non-interactive, keep project source
REM   uninstall.bat -y           -- same as --no-purge

setlocal enableextensions

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Locate a Python interpreter (prefer py launcher, then python).
set "PY="
where py >nul 2>nul
if %ERRORLEVEL%==0 (
    set "PY=py -3"
) else (
    where python >nul 2>nul
    if %ERRORLEVEL%==0 set "PY=python"
)

if "%PY%"=="" (
    echo DbManagementTool uninstaller requires Python ^>= 3.9.
    echo Install Python 3 from https://www.python.org/downloads/ and re-run this script.
    pause
    exit /b 1
)

%PY% "%SCRIPT_DIR%\setup\uninstall.py" --project-root "%SCRIPT_DIR%" %*
set "RC=%ERRORLEVEL%"

echo.
echo Press any key to close this window . . .
pause >nul
exit /b %RC%
