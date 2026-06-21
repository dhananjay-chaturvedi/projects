"""Cloud / monitor connection profile kinds and metadata."""

from __future__ import annotations

# Saved on cloud connection profiles (cloud_connections.json).
TARGET_CLOUD_DB = "cloud_db"
TARGET_VM = "vm"
TARGET_CLOUD_SERVICE = "cloud_service"

MONITOR_TARGET_KINDS: dict[str, str] = {
    TARGET_CLOUD_DB: "Cloud database (RDS, Azure SQL, Cloud SQL, …)",
    TARGET_VM: "Virtual machine / host",
    TARGET_CLOUD_SERVICE: "Other cloud service (LB, cache, custom API, …)",
}

# UI / wizard purpose
PURPOSE_CONNECTIONS = "connections"  # Connections tab — SQL/objects later
PURPOSE_MONITOR = "monitor"  # Monitoring tab — metrics polling
