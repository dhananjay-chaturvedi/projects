"""
Per-engine capability metadata — drives UI/CLI/API feature availability.

Register capabilities on each DatabaseRegistry entry under the ``capabilities`` key.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DBCapabilities:
    """Feature flags and labels for a database engine."""

    query_language: str = "sql"  # sql | document
    supports_autocommit: bool = True
    supports_transactions: bool = True
    supports_schema_conversion: bool = True
    supports_sql_editor: bool = True
    supports_document_query: bool = False
    supports_multi_statement: bool = True
    supports_ssl: bool = False
    # Allowed ssl_mode combobox values (engine-specific; empty = no SSL UI)
    ssl_mode_options: tuple[str, ...] = ()
    # Optional SSL file fields: ca, cert, key, wallet
    ssl_fields: tuple[str, ...] = ()
    # Registry operation key -> UI button label override
    operation_labels: dict[str, str] = field(default_factory=dict)
    # High-level features to hide in UI menus (schema_conversion, ai_query, …)
    disabled_features: tuple[str, ...] = ()

    def label_for_operation(self, op_key: str, default: str) -> str:
        return self.operation_labels.get(op_key, default)

    def feature_enabled(self, feature: str) -> bool:
        return feature not in self.disabled_features

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_language": self.query_language,
            "supports_autocommit": self.supports_autocommit,
            "supports_transactions": self.supports_transactions,
            "supports_schema_conversion": self.supports_schema_conversion,
            "supports_sql_editor": self.supports_sql_editor,
            "supports_document_query": self.supports_document_query,
            "supports_multi_statement": self.supports_multi_statement,
            "supports_ssl": self.supports_ssl,
            "ssl_mode_options": list(self.ssl_mode_options),
            "ssl_fields": list(self.ssl_fields),
            "operation_labels": dict(self.operation_labels),
            "disabled_features": list(self.disabled_features),
        }


_MYSQL_SSL = ("disable", "require", "verify_ca")
_POSTGRES_SSL = ("disable", "prefer", "require", "verify-ca", "verify-full")
_SQLSERVER_SSL = ("disable", "request", "require")
_ORACLE_SSL = ("disable", "require", "verify_ca")
_SQL_SSL_FIELDS = ("ca", "cert", "key")
_ORACLE_SSL_FIELDS = ("ca", "cert", "key", "wallet")

DEFAULT_SQL_CAPABILITIES = DBCapabilities()

MYSQL_CAPABILITIES = DBCapabilities(
    supports_ssl=True,
    ssl_mode_options=_MYSQL_SSL,
    ssl_fields=_SQL_SSL_FIELDS,
)

MARIADB_CAPABILITIES = DBCapabilities(
    supports_ssl=True,
    ssl_mode_options=_MYSQL_SSL,
    ssl_fields=_SQL_SSL_FIELDS,
)

POSTGRES_CAPABILITIES = DBCapabilities(
    supports_ssl=True,
    ssl_mode_options=_POSTGRES_SSL,
    ssl_fields=_SQL_SSL_FIELDS,
)

SQLSERVER_CAPABILITIES = DBCapabilities(
    supports_ssl=True,
    ssl_mode_options=_SQLSERVER_SSL,
    ssl_fields=("ca",),  # pymssql: encryption mode only; CA via OS/FreeTDS
    operation_labels={"getSchemas": "Schemas"},
)

ORACLE_CAPABILITIES = DBCapabilities(
    supports_ssl=True,
    ssl_mode_options=_ORACLE_SSL,
    ssl_fields=_ORACLE_SSL_FIELDS,
)

MONGODB_CAPABILITIES = DBCapabilities(
    query_language="document",
    supports_autocommit=False,
    supports_transactions=False,
    supports_schema_conversion=False,
    supports_sql_editor=False,
    supports_document_query=True,
    supports_multi_statement=False,
    operation_labels={"getTables": "Collections"},
    disabled_features=("schema_conversion",),
)

DOCUMENTDB_CAPABILITIES = DBCapabilities(
    query_language="document",
    supports_autocommit=False,
    supports_transactions=False,
    supports_schema_conversion=False,
    supports_sql_editor=False,
    supports_document_query=True,
    supports_multi_statement=False,
    operation_labels={"getTables": "Collections"},
    disabled_features=("schema_conversion",),
)
