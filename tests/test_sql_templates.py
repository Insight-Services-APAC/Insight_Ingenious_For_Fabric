from __future__ import annotations

import pathlib
from re import Pattern
import sys
import pytest

sys.path.insert(0, pathlib.Path.cwd())

from ingen_fab.python_libs.python.sql_templates import SQLTemplates  # noqa: E402


class TestSQLTemplatesFabric:
    """Test SQL templates for Fabric dialect."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.templates = SQLTemplates(dialect="fabric")

    def test_check_table_exists(self) -> None:
        """Test check_table_exists template."""
        sql = self.templates.render(
            "check_table_exists",
            schema_name="test_schema",
            table_name="test_table"
        )
        expected = (
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES\n"
            "WHERE TABLE_SCHEMA = 'test_schema' AND TABLE_NAME = 'test_table'"
        )
        assert sql.strip() == expected.strip()

    def test_drop_table(self) -> None:
        """Test drop_table template."""
        sql = self.templates.render(
            "drop_table",
            schema_name="test_schema",
            table_name="test_table"
        )
        expected = "DROP TABLE IF EXISTS test_schema.test_table"
        assert sql.strip() == expected

    def test_create_table_from_values(self) -> None:
        """Test create_table_from_values template."""
        sql = self.templates.render(
            "create_table_from_values",
            schema_name="test_schema",
            table_name="test_table",
            values_clause="(1, 'Alice'), (2, 'Bob')",
            column_names="id, name"
        )
        expected = "SELECT * INTO test_schema.test_table FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS v(id, name)"
        assert sql.strip() == expected    
        
    def test_insert_row(self) -> None:
        """Test insert_row template."""
        sql = self.templates.render(
            "insert_row",
            schema_name="test_schema",
            table_name="test_table",
            row_values="1, 'Alice'"
        )
        expected = "INSERT INTO test_schema.test_table VALUES (1, 'Alice')"
        assert sql.strip() == expected

    def test_list_tables_without_prefix(self) -> None:
        """Test list_tables template without prefix."""
        sql = self.templates.render("list_tables")
        expected = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES"
        assert sql.strip() == expected

    def test_list_tables_with_prefix(self) -> None:
        """Test list_tables template with prefix."""
        sql = self.templates.render("list_tables", prefix="test")
        expected = (
            "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME LIKE 'test%'"
        )
        assert sql.strip() == expected


class TestSQLTemplatesSQLServer:
    """Test SQL templates for SQL Server dialect."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.templates = SQLTemplates(dialect="sqlserver")

    def test_check_table_exists(self) -> None:
        """Test check_table_exists template."""
        sql = self.templates.render(
            "check_table_exists",
            schema_name="test_schema",
            table_name="test_table"
        )
        expected = (
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES\n"
            "WHERE TABLE_SCHEMA = 'test_schema' AND TABLE_NAME = 'test_table'"
        )
        assert sql.strip() == expected.strip()

    def test_drop_table(self) -> None:
        """Test drop_table template."""
        sql = self.templates.render(
            "drop_table",
            schema_name="test_schema",
            table_name="test_table"
        )
        expected = "DROP TABLE IF EXISTS test_schema.test_table"
        assert sql.strip() == expected

    def test_create_table_from_values(self) -> None:
        """Test create_table_from_values template."""
        sql = self.templates.render(
            "create_table_from_values",
            schema_name="test_schema",
            table_name="test_table",
            values_clause="(1, 'Alice'), (2, 'Bob')",
            column_names="id, name"
        )
        expected = "SELECT * INTO test_schema.test_table FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS v(id, name)"
        assert sql.strip() == expected

    def test_insert_row(self) -> None:
        """Test insert_row template."""
        sql = self.templates.render(
            "insert_row",
            schema_name="test_schema",
            table_name="test_table",
            row_values="1, 'Alice'"
        )
        expected = "INSERT INTO test_schema.test_table VALUES (1, 'Alice')"
        assert sql.strip() == expected

    def test_list_tables_without_prefix(self) -> None:
        """Test list_tables template without prefix."""
        sql = self.templates.render("list_tables")
        expected = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES"
        assert sql.strip() == expected

    def test_list_tables_with_prefix(self) -> None:
        """Test list_tables template with prefix."""
        sql = self.templates.render("list_tables", prefix="abc")
        expected = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'abc%'"
        assert sql.strip() == expected


class TestSQLTemplatesErrorHandling:
    """Test error handling in SQL templates."""

    def test_invalid_dialect(self) -> None:
        """Test that invalid dialect raises appropriate error."""        
        with pytest.raises(ValueError, match="Invalid SQL dialect"):
            templates = SQLTemplates(dialect="invalid")
            templates.render("check_table_exists", schema_name="test", table_name="test")

    def test_missing_template(self) -> None:
        """Test that missing template raises appropriate error."""
        templates = SQLTemplates(dialect="fabric")
        with pytest.raises(FileNotFoundError, match="Template not found"):
            templates.render("nonexistent_template")

    def test_missing_required_parameter(self) -> None:
        """Test that missing required parameters raise Jinja2 error."""
        templates = SQLTemplates(dialect="fabric")
        with pytest.raises(Exception):  # Jinja2 will raise UndefinedError
            templates.render("check_table_exists", schema_name="test")  # missing table_name


class TestSQLTemplatesDefaultDialect:
    """Test default dialect behavior."""

    def test_default_dialect_is_fabric(self) -> None:
        """Test that default dialect is fabric."""
        templates = SQLTemplates()  # No dialect specified
        sql = templates.render(
            "check_table_exists",
            schema_name="test_schema",
            table_name="test_table"
        )
        # Should work without error, indicating fabric dialect is used
        assert "INFORMATION_SCHEMA.TABLES" in sql


class TestSQLTemplatesParameterVariations:
    """Test various parameter combinations and edge cases."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.templates = SQLTemplates(dialect="fabric")

    def test_special_characters_in_names(self) -> None:
        """Test templates with special characters in names."""
        sql = self.templates.render(
            "drop_table",
            schema_name="test-schema_123",
            table_name="test.table[name]"
        )
        expected = "DROP TABLE IF EXISTS test-schema_123.test.table[name]"
        assert sql.strip() == expected

    def test_numeric_values_in_insert(self) -> None:
        """Test insert template with various data types."""
        sql = self.templates.render(
            "insert_row",
            schema_name="test_schema",
            table_name="test_table",
            row_values="1, 'text', 123.45, NULL, '2023-01-01'"
        )
        expected = "INSERT INTO test_schema.test_table VALUES (1, 'text', 123.45, NULL, '2023-01-01')"
        assert sql.strip() == expected

    def test_complex_values_clause(self) -> None:
        """Test create_table_from_values with complex values."""
        sql = self.templates.render(
            "create_table_from_values",
            schema_name="test_schema",
            table_name="test_table",
            values_clause="(1, 'John', 25.5, '2023-01-01'), (2, 'Jane', 30.0, '2023-01-02')",
            column_names="id, name, score, date"
        )
        expected = "SELECT * INTO test_schema.test_table FROM (VALUES (1, 'John', 25.5, '2023-01-01'), (2, 'Jane', 30.0, '2023-01-02')) AS v(id, name, score, date)"
        assert sql.strip() == expected

    def test_empty_prefix_behaves_like_no_prefix(self) -> None:
        """Test that empty prefix behaves like no prefix."""
        sql_no_prefix = self.templates.render("list_tables")
        sql_empty_prefix = self.templates.render("list_tables", prefix="")
        # Both should produce the same result (no WHERE clause)
        assert sql_no_prefix.strip() == sql_empty_prefix.strip()
