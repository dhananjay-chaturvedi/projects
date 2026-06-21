"""Tests for full schema introspection and DDL generation."""

import pytest


class TestSchemaFullDDL:

    def _sample_schema(self):
        return {
            "table_name": "orders",
            "table_comment": "Order header",
            "table_charset": "utf8mb4",
            "table_collation": "utf8mb4_unicode_ci",
            "columns": [
                {
                    "name": "id",
                    "type": "INT",
                    "nullable": False,
                    "default": None,
                    "auto_increment": True,
                    "comment": "Primary identifier",
                },
                {
                    "name": "status",
                    "type": "ENUM('new','done')",
                    "nullable": False,
                    "default": "'new'",
                    "enum_values": ["new", "done"],
                    "comment": "Order status",
                },
                {
                    "name": "created_at",
                    "type": "DATETIME",
                    "nullable": True,
                    "default": "0000-00-00 00:00:00",
                },
            ],
            "primary_key": ["id"],
            "indexes": [
                {
                    "name": "idx_status",
                    "columns": [{"name": "status", "order": "ASC"}],
                    "unique": False,
                    "type": "BTREE",
                }
            ],
            "unique_constraints": [{"name": "uk_status", "columns": ["status"]}],
            "foreign_keys": [
                {
                    "name": "fk_customer",
                    "columns": ["id"],
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                    "on_delete": "CASCADE",
                    "on_update": "RESTRICT",
                }
            ],
            "check_constraints": [{"name": "chk_id", "expression": "id >= 0"}],
            "partition": None,
            "sequences": [],
            "related_objects": {"views": [], "triggers": [], "procedures": [], "functions": []},
            "conversion_warnings": [],
        }

    def test_generate_mysql_ddl_includes_comments_fk_check_enum(self):
        from schema_converter.schema_full import convert_extended_schema, generate_all_table_ddl

        converted = convert_extended_schema(self._sample_schema(), "MySQL", "MariaDB")
        converted["table_engine"] = "InnoDB"
        ddl = generate_all_table_ddl(converted, "MariaDB")
        combined = "\n".join(ddl)
        assert "COMMENT='Order header'" in combined
        assert "COMMENT 'Primary identifier'" in combined
        assert "ENUM('new','done')" in combined
        assert "DEFAULT '0000-00-00 00:00:00'" in combined
        assert "CONSTRAINT uk_status UNIQUE" in combined
        assert "CONSTRAINT chk_id CHECK" in combined
        assert "FOREIGN KEY" in combined
        assert "CREATE INDEX idx_status" in combined

    def test_convert_extended_schema_preserves_enum_on_mysql_target(self):
        from schema_converter.schema_full import convert_extended_schema

        converted = convert_extended_schema(
            self._sample_schema(), "MySQL", "MariaDB", table_name_map={"customers": "jhcustomers"}
        )
        status_col = next(c for c in converted["columns"] if c["name"] == "status")
        assert "ENUM('new','done')" in status_col["type"]
        assert converted["foreign_keys"][0]["referenced_table"] == "jhcustomers"

    def test_zero_date_null_strategy(self, monkeypatch):
        from schema_converter.schema_full import apply_zero_date_strategy

        monkeypatch.setattr(
            "schema_converter.schema_full.get_zero_date_strategy",
            lambda: "null",
        )
        assert apply_zero_date_strategy("'0000-00-00 00:00:00'") == "NULL"

    def test_schema_converter_generate_all_table_ddl(self):
        from schema_converter import SchemaConverter

        class MockMgr:
            db_type = "MySQL"

        sc = SchemaConverter(MockMgr(), MockMgr())
        converted = sc.convert_schema(self._sample_schema())
        ddl = sc.generate_all_table_ddl(converted)
        assert ddl
        assert ddl[0].startswith("CREATE TABLE")

    def test_mysql_indexes_without_expression_column(self):
        from schema_converter.schema_full import _mysql_indexes

        class FakeCursor:
            def execute(self, sql, params=None):
                self._sql = sql

            def fetchall(self):
                if "information_schema.COLUMNS" in getattr(self, "_sql", ""):
                    return [
                        ("INDEX_NAME",),
                        ("NON_UNIQUE",),
                        ("SEQ_IN_INDEX",),
                        ("COLUMN_NAME",),
                        ("COLLATION",),
                        ("INDEX_TYPE",),
                    ]
                return [
                    ("idx_status", 1, 1, "status", "A", "BTREE", None, None),
                ]

        indexes = _mysql_indexes(FakeCursor(), "db", "orders", ["id"])
        assert len(indexes) == 1
        assert indexes[0]["columns"][0]["name"] == "status"

    def test_collate_not_applied_to_int_columns(self):
        from schema_converter.schema_full import _column_suffix_mysql

        assert _column_suffix_mysql(
            {
                "type": "INT",
                "charset": "utf8mb4",
                "collation": "utf8mb4_general_ci",
            }
        ) == ""

    def test_collate_omitted_when_same_as_table_default(self):
        from schema_converter.schema_full import (
            _column_suffix_mysql,
            _mysql_normalize_column_charset,
        )

        col = {
            "type": "varchar(128)",
            "charset": "utf8mb4",
            "collation": "utf8mb4_general_ci",
        }
        _mysql_normalize_column_charset(col, "utf8mb4", "utf8mb4_general_ci")
        assert _column_suffix_mysql(col) == ""

    def test_collate_kept_when_column_overrides_table(self):
        from schema_converter.schema_full import (
            _column_suffix_mysql,
            _mysql_normalize_column_charset,
        )

        col = {
            "type": "varchar(128)",
            "charset": "utf8mb4",
            "collation": "utf8mb4_bin",
        }
        _mysql_normalize_column_charset(col, "utf8mb4", "utf8mb4_general_ci")
        assert "COLLATE utf8mb4_bin" in _column_suffix_mysql(col)
        assert "CHARACTER SET" not in _column_suffix_mysql(col)

    def test_indexes_for_ddl_skips_unique_constraint_dupes(self):
        from schema_converter.schema_full import _indexes_for_ddl

        schema = {
            "unique_constraints": [{"name": "ACC_SMS_SEND_I01", "columns": ["x"]}],
            "indexes": [
                {"name": "ACC_SMS_SEND_I01", "columns": [{"name": "x"}], "unique": True},
                {"name": "ACC_SMS_SEND_I02", "columns": [{"name": "y"}], "unique": False},
            ],
        }
        out = _indexes_for_ddl(schema, "MariaDB")
        assert len(out) == 1
        assert out[0]["name"] == "ACC_SMS_SEND_I02"
