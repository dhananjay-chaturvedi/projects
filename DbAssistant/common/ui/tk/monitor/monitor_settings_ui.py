"""Monitor tab settings — editor for monitoring/monitor_config.ini + notification secrets."""

from __future__ import annotations

from common.ui.tk.module_config_dialog import FieldSpec, open_module_config_dialog
from monitoring import monitor_config

_MONITOR_FIELDS: tuple[FieldSpec, ...] = (
    ("monitoring", "metrics_refresh_interval", "Metrics refresh interval (ms)", "int", ()),
    ("monitoring", "max_graph_data_points", "Max graph data points", "int", ()),
    ("monitoring", "cloud_keepalive_interval", "Cloud keepalive (s)", "int", ()),
    ("monitoring", "db_keepalive_interval", "DB keepalive (s)", "int", ()),
    ("monitoring", "db_keepalive_skip_if_polled_within", "DB keepalive skip if polled (s)", "int", ()),
    ("monitoring", "ssh_keepalive_interval", "SSH keepalive (s)", "int", ()),
    ("monitoring", "db_metric_skip_ping_if_used_within", "DB metric skip ping window (s)", "int", ()),
    ("monitoring", "cloud_health_skip_if_used_within", "Cloud health skip window (s)", "int", ()),
    ("monitoring", "ssh_keepalive_skip_if_used_within", "SSH keepalive skip window (s)", "int", ()),
    ("monitoring", "cloud_force_refresh_interval", "Cloud force refresh (s)", "int", ()),
    ("ssh.connection", "ssh_timeout", "SSH timeout (s)", "int", ()),
    ("ssh.connection", "ssh_test_timeout", "SSH test timeout (s)", "int", ()),
    ("ssh.connection", "ssh_control_persist", "SSH ControlPersist (s)", "int", ()),
    ("ssh.connection", "ssh_os_detection_timeout", "SSH OS detection timeout (s)", "int", ()),
    ("ssh.connection", "ssh_monitoring_timeout", "SSH monitoring timeout (s)", "int", ()),
    ("monitoring.graphs", "metric_graph_width", "Graph width (px)", "int", ()),
    ("monitoring.graphs", "metric_graph_height", "Graph height (px)", "int", ()),
    ("monitoring.limits", "max_data_points", "Max points per graph", "int", ()),
    ("cloud.lookback", "aws_lookback_minutes", "AWS lookback (min)", "int", ()),
    ("cloud.lookback", "azure_lookback_minutes", "Azure lookback (min)", "int", ()),
    ("cloud.lookback", "gcp_lookback_minutes", "GCP lookback (min)", "int", ()),
    ("notifications", "enabled", "Enable notifications", "bool", ()),
    ("notifications", "min_severity", "Minimum severity", "enum", ("INFO", "WARNING", "CRITICAL")),
    ("notifications", "teams_enabled", "Teams enabled", "bool", ()),
    ("notifications", "email_enabled", "Email enabled", "bool", ()),
    ("notifications", "smtp_host", "SMTP host", "str", ()),
    ("notifications", "smtp_port", "SMTP port", "int", ()),
    ("notifications", "smtp_use_tls", "SMTP STARTTLS", "bool", ()),
    ("notifications", "smtp_username", "SMTP username", "str", ()),
    ("notifications", "email_from", "Email from", "str", ()),
    ("notifications", "email_to", "Email to (comma-separated)", "str", ()),
)

_SECRET_FIELDS = (
    ("teams_webhook_url", "Teams webhook URL"),
    ("smtp_password", "SMTP password"),
)


def open_monitor_settings(root, *, on_change=None):
    def _saved():
        monitor_config.reload()
        from common.notifications import load_config
        load_config()
        if on_change:
            on_change()

    open_module_config_dialog(
        root,
        title="Monitor Settings — monitor_config.ini",
        config_module=monitor_config,
        fields=_MONITOR_FIELDS,
        on_saved=_saved,
        secret_fields=_SECRET_FIELDS,
    )
