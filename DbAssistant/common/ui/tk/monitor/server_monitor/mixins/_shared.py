"""Shared imports for ServerMonitorUI mixins (runtime lookup in method bodies)."""

import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

from common.cloud.profiles import (
    MONITOR_TARGET_KINDS,
    PURPOSE_CONNECTIONS,
    PURPOSE_MONITOR,
    TARGET_CLOUD_DB,
    TARGET_CLOUD_SERVICE,
    TARGET_VM,
)
from common.cloud.schemas import (
    CLOUD_PROVIDER_SCHEMAS,
    MFA_TYPES,
    RESOURCE_SECTION_TITLES,
    TARGET_KIND_FORM_TITLES,
    resource_fields_for,
)
from common.cloud.validation import validate_cloud_profile
from common.config_loader import config, console_print, properties
from common.database_registry import DatabaseRegistry
from common.tzutil import now as _tz_now
from common.ui.tk import bind_canvas_mousewheel, make_scrollable


def display_time_str(fmt: str = "%H:%M:%S") -> str:
    """Current wall-clock time in the configured display timezone."""
    return _tz_now().strftime(fmt)
from monitoring.cloud_provider_registry import CloudProviderRegistry
from monitoring.db_metric_config import collect_metrics as _collect_db_metrics
from monitoring.send_notification import send_alert
from monitoring.threshold_checker import CRITICAL, INFO, WARNING, ThresholdChecker

__all__ = [
    "CRITICAL",
    "INFO",
    "WARNING",
    "CloudProviderRegistry",
    "DatabaseRegistry",
    "MONITOR_TARGET_KINDS",
    "PURPOSE_CONNECTIONS",
    "PURPOSE_MONITOR",
    "TARGET_CLOUD_DB",
    "TARGET_CLOUD_SERVICE",
    "TARGET_VM",
    "ThresholdChecker",
    "_collect_db_metrics",
    "bind_canvas_mousewheel",
    "make_scrollable",
    "config",
    "console_print",
    "display_time_str",
    "filedialog",
    "messagebox",
    "os",
    "properties",
    "re",
    "resource_fields_for",
    "scrolledtext",
    "send_alert",
    "subprocess",
    "sys",
    "tempfile",
    "threading",
    "time",
    "tk",
    "ttk",
    "validate_cloud_profile",
    "CLOUD_PROVIDER_SCHEMAS",
    "MFA_TYPES",
    "RESOURCE_SECTION_TITLES",
    "TARGET_KIND_FORM_TITLES",
]
