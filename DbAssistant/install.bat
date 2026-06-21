@echo off
REM Convenience wrapper -- canonical installer is setup\install.bat.
REM
REM Usage:
REM   install.bat                   -- full install (all modules)
REM   install.bat --module ai       -- install only the AI Query module
REM   install.bat --no-optional     -- skip cloud SDKs
REM   install.bat --no-venv         -- install into current Python (no .venv)
REM   install.bat --python C:\path\to\python.exe

setlocal enableextensions

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

call "%ROOT%\setup\install.bat" %*
exit /b %ERRORLEVEL%
