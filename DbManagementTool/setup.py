#!/usr/bin/env python3
"""
Setup script for Database Management Tool
Sets up the project from scratch to ready-to-run state
"""

import sys
import os
import subprocess
import shutil
from pathlib import Path

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    """Print a section header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text:^70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}\n")

def print_success(text):
    """Print success message"""
    print(f"{Colors.OKGREEN}✓ {text}{Colors.ENDC}")

def print_warning(text):
    """Print warning message"""
    print(f"{Colors.WARNING}⚠ {text}{Colors.ENDC}")

def print_error(text):
    """Print error message"""
    print(f"{Colors.FAIL}✗ {text}{Colors.ENDC}")

def print_info(text):
    """Print info message"""
    print(f"{Colors.OKBLUE}ℹ {text}{Colors.ENDC}")

def check_python_version():
    """Check if Python version is 3.8 or higher"""
    print_header("Checking Python Version")

    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")

    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print_error("Python 3.8 or higher is required!")
        print_info("Please upgrade Python: https://www.python.org/downloads/")
        return False

    print_success(f"Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def install_core_dependencies():
    """Install required Python packages"""
    print_header("Installing Core Dependencies")

    core_packages = [
        'cryptography',  # Password encryption
    ]

    print_info("Installing required packages...")
    for package in core_packages:
        try:
            print(f"  Installing {package}...")
            subprocess.run(
                [sys.executable, '-m', 'pip', 'install', package],
                check=True,
                capture_output=True
            )
            print_success(f"{package} installed")
        except subprocess.CalledProcessError as e:
            print_error(f"Failed to install {package}: {e}")
            return False

    return True

def install_database_drivers():
    """Install optional database drivers"""
    print_header("Installing Database Drivers (Optional)")

    db_drivers = {
        'mysql-connector-python': 'MySQL',
        'psycopg2-binary': 'PostgreSQL',
    }

    print_info("Installing optional database drivers...")
    print_info("Note: Oracle and MariaDB drivers may require system packages")

    for package, db_name in db_drivers.items():
        try:
            print(f"  Installing {package} ({db_name})...")
            subprocess.run(
                [sys.executable, '-m', 'pip', 'install', package],
                check=True,
                capture_output=True
            )
            print_success(f"{db_name} driver installed")
        except subprocess.CalledProcessError:
            print_warning(f"{db_name} driver installation failed (optional)")

    # Check for Oracle
    print_info("\nChecking for Oracle Instant Client...")
    try:
        import cx_Oracle
        print_success("Oracle (cx_Oracle) is available")
    except ImportError:
        print_warning("Oracle (cx_Oracle) not installed")
        print_info("  To install: pip install cx_Oracle")
        print_info("  Requires Oracle Instant Client: https://www.oracle.com/database/technologies/instant-client.html")

    # Check for MariaDB
    print_info("\nChecking for MariaDB connector...")
    try:
        import mariadb
        print_success("MariaDB connector is available")
    except ImportError:
        print_warning("MariaDB connector not installed")
        print_info("  To install: pip install mariadb")
        print_info("  May require system packages: brew install mariadb-connector-c (macOS)")

    return True

def install_optional_packages():
    """Install optional packages for enhanced features"""
    print_header("Installing Optional Packages")

    optional_packages = {
        'pandas': 'Data export (CSV, Excel)',
        'openpyxl': 'Excel export support',
    }

    for package, description in optional_packages.items():
        try:
            print(f"  Installing {package} ({description})...")
            subprocess.run(
                [sys.executable, '-m', 'pip', 'install', package],
                check=True,
                capture_output=True
            )
            print_success(f"{package} installed")
        except subprocess.CalledProcessError:
            print_warning(f"{package} installation failed (optional)")

    return True

def check_system_dependencies():
    """Check for system-level dependencies"""
    print_header("Checking System Dependencies")

    # Check for sshpass (for SSH monitoring)
    print_info("Checking for sshpass (SSH monitoring)...")
    if shutil.which('sshpass'):
        print_success("sshpass is installed")
    else:
        print_warning("sshpass not found (optional for SSH monitoring)")
        if sys.platform == "darwin":
            print_info("  To install on macOS: brew install hudochenkov/sshpass/sshpass")
        elif sys.platform.startswith("linux"):
            print_info("  To install on Linux: sudo apt-get install sshpass")

    # Check for Claude CLI (for AI features)
    print_info("\nChecking for Claude CLI (AI features)...")
    if shutil.which('claude'):
        print_success("Claude CLI is installed")
        try:
            result = subprocess.run(['claude', '--version'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                version = result.stdout.strip() or result.stderr.strip()
                print_info(f"  Version: {version}")
        except:
            pass
    else:
        print_warning("Claude CLI not found (optional for AI Query Assistant)")
        print_info("  To install: https://claude.ai/download")
        print_info("  Or if you have Claude Code, the CLI should already be available")

    return True

def create_config_files():
    """Create configuration files if they don't exist"""
    print_header("Setting Up Configuration Files")

    base_dir = Path(__file__).parent

    # Check if config.ini exists, if not copy from example
    config_file = base_dir / 'config.ini'
    config_example = base_dir / 'config.ini.example'

    if not config_file.exists() and config_example.exists():
        print_info("Creating config.ini from config.ini.example...")
        shutil.copy(config_example, config_file)
        print_success("config.ini created")
        print_warning("⚠ Please edit config.ini with your database credentials and paths")
    elif config_file.exists():
        print_success("config.ini already exists")
    else:
        print_warning("No config.ini.example found, skipping config creation")

    # Check if properties.ini exists
    properties_file = base_dir / 'properties.ini'
    properties_example = base_dir / 'properties.ini.example'

    if not properties_file.exists() and properties_example.exists():
        print_info("Creating properties.ini from properties.ini.example...")
        shutil.copy(properties_example, properties_file)
        print_success("properties.ini created")
    elif properties_file.exists():
        print_success("properties.ini already exists")
    else:
        print_warning("No properties.ini.example found, skipping properties creation")

    return True

def create_data_directories():
    """Create necessary data directories"""
    print_header("Creating Data Directories")

    # Create ~/.dbmanager directory
    db_manager_dir = Path.home() / '.dbmanager'

    if not db_manager_dir.exists():
        print_info("Creating ~/.dbmanager directory...")
        db_manager_dir.mkdir(parents=True, exist_ok=True)
        print_success("~/.dbmanager directory created")
    else:
        print_success("~/.dbmanager directory already exists")

    print_info(f"Data directory location: {db_manager_dir}")
    print_info("  This will store:")
    print_info("    - Encrypted connection credentials")
    print_info("    - Server monitoring configurations")
    print_info("    - Encryption keys")

    return True

def verify_installation():
    """Verify the installation by checking imports"""
    print_header("Verifying Installation")

    # Test core imports
    print_info("Testing core imports...")
    try:
        import tkinter
        print_success("tkinter (GUI) available")
    except ImportError:
        print_error("tkinter not available!")
        print_info("  tkinter should be included with Python")
        print_info("  On Linux: sudo apt-get install python3-tk")
        return False

    try:
        from cryptography.fernet import Fernet
        print_success("cryptography (encryption) available")
    except ImportError:
        print_error("cryptography not available!")
        return False

    # Test project imports
    print_info("\nTesting project imports...")
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from config_loader import config, properties
        print_success("config_loader available")

        from connection_manager import ConnectionManager
        print_success("connection_manager available")

        from database_registry import DatabaseRegistry
        print_success("database_registry available")

        print_success("\nAll core components are working!")
        return True
    except ImportError as e:
        print_error(f"Import error: {e}")
        return False

def print_next_steps():
    """Print next steps for the user"""
    print_header("Setup Complete! 🎉")

    print(f"\n{Colors.BOLD}Next Steps:{Colors.ENDC}\n")

    print(f"{Colors.OKGREEN}1. Configure the application:{Colors.ENDC}")
    print(f"   Edit config.ini with your database credentials and paths")
    print(f"   Edit properties.ini to customize UI settings (optional)")

    print(f"\n{Colors.OKGREEN}2. Set up Oracle (if needed):{Colors.ENDC}")
    print(f"   - Download Oracle Instant Client")
    print(f"   - Update oracle_client_path in config.ini")
    print(f"   - Install cx_Oracle: pip install cx_Oracle")

    print(f"\n{Colors.OKGREEN}3. Run the application:{Colors.ENDC}")
    print(f"   {Colors.BOLD}python3 conDbUi.py{Colors.ENDC}")

    print(f"\n{Colors.OKBLUE}Optional Enhancements:{Colors.ENDC}")
    print(f"   - Install Claude CLI for AI Query Assistant")
    print(f"   - Install sshpass for SSH server monitoring")
    print(f"   - Install pandas for enhanced data export")

    print(f"\n{Colors.OKCYAN}Documentation:{Colors.ENDC}")
    print(f"   - README.md - Getting started guide")
    print(f"   - CLAUDE.md - Development documentation")
    print(f"   - CONFIG_MIGRATION_GUIDE.md - Configuration details")

    print(f"\n{Colors.WARNING}Security Note:{Colors.ENDC}")
    print(f"   All database credentials are encrypted and stored in ~/.dbmanager/")
    print(f"   The encryption key is automatically generated on first use")

    print()

def main():
    """Main setup function"""
    print(f"\n{Colors.BOLD}{Colors.HEADER}")
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│                                                                 │")
    print("│          Database Management Tool - Setup Script                │")
    print("│                                                                 │")
    print("└─────────────────────────────────────────────────────────────────┘")
    print(f"{Colors.ENDC}\n")

    print_info("This script will set up the Database Management Tool")
    print_info("It will install dependencies and configure the application\n")

    # Run setup steps
    steps = [
        ("Python Version Check", check_python_version),
        ("Core Dependencies", install_core_dependencies),
        ("Database Drivers", install_database_drivers),
        ("Optional Packages", install_optional_packages),
        ("System Dependencies", check_system_dependencies),
        ("Configuration Files", create_config_files),
        ("Data Directories", create_data_directories),
        ("Installation Verification", verify_installation),
    ]

    failed_steps = []

    for step_name, step_func in steps:
        try:
            if not step_func():
                failed_steps.append(step_name)
        except Exception as e:
            print_error(f"Error in {step_name}: {e}")
            failed_steps.append(step_name)

    # Print summary
    if failed_steps:
        print_header("Setup Completed with Warnings")
        print_warning("The following steps had issues:")
        for step in failed_steps:
            print(f"  - {step}")
        print_info("\nYou can continue, but some features may not work")
    else:
        print_next_steps()

    return 0 if not failed_steps else 1

if __name__ == '__main__':
    sys.exit(main())
