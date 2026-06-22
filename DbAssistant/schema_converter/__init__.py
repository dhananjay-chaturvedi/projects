"""
Data Migration module (internal package name: ``schema_converter``).

Independently shippable: copy ``common/`` plus this package.

Public surface::

    from schema_converter import SchemaConverter, ConversionValidator
    from schema_converter import SchemaService
"""

from __future__ import annotations

from .converter import (
    DataTypeMapper,
    SchemaConverter,
    DataConverter,
    ConversionValidator,
    SchemaComparer,
    DataComparer,
)
from .service import SchemaService


def __getattr__(name: str):
    if name == "SchemaConverterUI":
        from common.ui.tk.migrator.schema_converter_ui import SchemaConverterUI

        return SchemaConverterUI
    raise AttributeError(f"module 'schema_converter' has no attribute {name!r}")


__all__ = [
    "DataTypeMapper",
    "SchemaConverter",
    "DataConverter",
    "ConversionValidator",
    "SchemaComparer",
    "DataComparer",
    "SchemaService",
    "SchemaConverterUI",
]
