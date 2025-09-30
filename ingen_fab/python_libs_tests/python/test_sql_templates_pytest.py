from __future__ import annotations

import pytest
from jinja2 import exceptions

from ingen_fab.python_libs.python.sql_templates import SQLTemplates, required_filter


class TestRequiredFilter:
    """Test the required_filter function."""

    def test_required_filter_with_value(self):
        """Test required filter with valid value."""
        result = required_filter("test_value", "test_var")
        assert result == "test_value"

    def test_required_filter_with_none(self):
        """Test required filter with None value."""
        with pytest.raises(
            exceptions.TemplateRuntimeError,
            match="Required parameter 'test_var' was not provided",
        ):
            required_filter(None, "test_var")

    def test_required_filter_with_empty_string(self):
        """Test required filter with empty string."""
        with pytest.raises(
            exceptions.TemplateRuntimeError,
            match="Required parameter 'test_var' was not provided",
        ):
            required_filter("", "test_var")

    def test_required_filter_with_empty_list(self):
        """Test required filter with empty list."""
        with pytest.raises(
            exceptions.TemplateRuntimeError,
            match="Required parameter 'test_var' was not provided",
        ):
            required_filter([], "test_var")

    def test_required_filter_without_var_name(self):
        """Test required filter without variable name."""
        with pytest.raises(
            exceptions.TemplateRuntimeError,
            match="Required parameter 'unknown' was not provided",
        ):
            required_filter(None)

    def test_required_filter_with_zero(self):
        """Test required filter with zero value (should pass)."""
        result = required_filter(0, "test_var")
        assert result == 0

    def test_required_filter_with_false(self):
        """Test required filter with False value (should pass)."""
        result = required_filter(False, "test_var")
        assert result is False


class TestSQLTemplates:
    """Test the SQLTemplates class."""

    def test_init_default_dialect(self):
        """Test initialization with default dialect."""
        sql_templates = SQLTemplates()
        assert sql_templates.dialect == "fabric"
        assert sql_templates.env is not None

    def test_init_custom_dialect(self):
        """Test initialization with custom dialect."""
        sql_templates = SQLTemplates("sql_server")
        assert sql_templates.dialect == "sql_server"

    def test_get_template_exists(self):
        """Test getting an existing template."""
        sql_templates = SQLTemplates("fabric")
        template = sql_templates.get_template("check_schema_exists", "fabric")
        assert template is not None
        assert "INFORMATION_SCHEMA.SCHEMATA" in template
        assert "{{ schema_name | required }}" in template

    def test_get_template_not_exists(self):
        """Test getting a non-existent template."""
        sql_templates = SQLTemplates("fabric")
        with pytest.raises(FileNotFoundError, match="Template nonexistent for dialect fabric not found"):
            sql_templates.get_template("nonexistent", "fabric")

    def test_get_template_wrong_dialect(self):
        """Test getting template with wrong dialect."""
        sql_templates = SQLTemplates("fabric")
        with pytest.raises(
            FileNotFoundError,
            match="Template check_schema_exists for dialect nonexistent not found",
        ):
            sql_templates.get_template("check_schema_exists", "nonexistent")

    def test_render_check_schema_exists(self):
        """Test rendering check_schema_exists template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("check_schema_exists", schema_name="test_schema")
        assert "FROM INFORMATION_SCHEMA.SCHEMATA" in result
        assert "WHERE SCHEMA_NAME = 'test_schema'" in result

    def test_render_check_table_exists(self):
        """Test rendering check_table_exists template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("check_table_exists", schema_name="test_schema", table_name="test_table")
        assert "INFORMATION_SCHEMA.TABLES" in result
        assert "TABLE_SCHEMA = 'test_schema'" in result
        assert "TABLE_NAME = 'test_table'" in result

    def test_render_create_table(self):
        """Test rendering create_table template."""
        sql_templates = SQLTemplates("fabric")
        schema = {"id": "INT", "name": "VARCHAR(255)", "value": "DECIMAL(10,2)"}
        result = sql_templates.render(
            "create_table",
            schema_name="test_schema",
            table_name="test_table",
            schema=schema,
        )
        assert "CREATE TABLE test_schema.test_table" in result
        assert "id INT" in result
        assert "name VARCHAR(255)" in result
        assert "value DECIMAL(10,2)" in result

    def test_render_create_table_with_options(self):
        """Test rendering create_table template with options."""
        sql_templates = SQLTemplates("fabric")
        schema = {"id": "INT", "name": "VARCHAR(255)"}
        options = {"LOCATION": "/path/to/table", "TBLPROPERTIES": "test"}
        result = sql_templates.render(
            "create_table",
            schema_name="test_schema",
            table_name="test_table",
            schema=schema,
            options=options,
        )
        assert "CREATE TABLE test_schema.test_table" in result
        assert "LOCATION = '/path/to/table'" in result
        assert "TBLPROPERTIES = 'test'" in result

    def test_render_drop_table(self):
        """Test rendering drop_table template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("drop_table", schema_name="test_schema", table_name="test_table")
        assert "DROP TABLE IF EXISTS test_schema.test_table" in result

    def test_render_read_table_basic(self):
        """Test rendering read_table template basic."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("read_table", schema_name="test_schema", table_name="test_table")
        assert "SELECT *" in result
        assert "FROM test_schema.test_table" in result

    def test_render_read_table_with_columns(self):
        """Test rendering read_table template with specific columns."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render(
            "read_table",
            schema_name="test_schema",
            table_name="test_table",
            columns=["id", "name"],
        )
        assert "SELECT id, name" in result
        assert "FROM test_schema.test_table" in result

    def test_render_read_table_with_filters(self):
        """Test rendering read_table template with filters."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render(
            "read_table",
            schema_name="test_schema",
            table_name="test_table",
            filters={"status": "active", "type": "user"},
        )
        assert "FROM test_schema.test_table" in result
        assert "WHERE" in result
        assert "status = 'active'" in result
        assert "type = 'user'" in result
        assert "AND" in result

    def test_render_read_table_with_limit(self):
        """Test rendering read_table template with limit."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("read_table", schema_name="test_schema", table_name="test_table", limit=100)
        assert "FROM test_schema.test_table" in result
        assert "LIMIT 100" in result

    def test_render_delete_from_table(self):
        """Test rendering delete_from_table template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render(
            "delete_from_table",
            schema_name="test_schema",
            table_name="test_table",
            filters={"id": "123"},
        )
        assert "DELETE FROM test_schema.test_table" in result
        assert "WHERE" in result
        assert "id = '123'" in result

    def test_render_insert_row(self):
        """Test rendering insert_row template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render(
            "insert_row",
            schema_name="test_schema",
            table_name="test_table",
            row_values="1, 'test', 100.50",
        )
        assert "INSERT INTO test_schema.test_table VALUES (1, 'test', 100.50)" in result

    def test_render_list_tables(self):
        """Test rendering list_tables template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("list_tables")
        assert "SELECT" in result
        assert "TABLE_SCHEMA" in result
        assert "TABLE_NAME" in result
        assert "INFORMATION_SCHEMA.TABLES" in result

    def test_render_list_tables_with_prefix(self):
        """Test rendering list_tables template with prefix."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("list_tables", prefix="test_")
        assert "WHERE" in result
        assert "TABLE_NAME LIKE 'test_%'" in result

    def test_render_get_table_schema(self):
        """Test rendering get_table_schema template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("get_table_schema", schema_name="test_schema", table_name="test_table")
        assert "SELECT COLUMN_NAME, DATA_TYPE" in result
        assert "FROM INFORMATION_SCHEMA.COLUMNS" in result
        assert "TABLE_SCHEMA = 'test_schema'" in result
        assert "TABLE_NAME = 'test_table'" in result

    def test_render_get_table_row_count(self):
        """Test rendering get_table_row_count template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("get_table_row_count", schema_name="test_schema", table_name="test_table")
        assert "SELECT COUNT(*)" in result
        assert "FROM test_schema.test_table" in result

    def test_render_get_table_metadata(self):
        """Test rendering get_table_metadata template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("get_table_metadata", schema_name="test_schema", table_name="test_table")
        assert "SELECT *" in result
        assert "INFORMATION_SCHEMA.TABLES" in result
        assert "TABLE_SCHEMA = 'test_schema'" in result
        assert "TABLE_NAME = 'test_table'" in result

    def test_render_rename_table_fabric(self):
        """Test rendering rename_table template for fabric."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render(
            "rename_table",
            schema_name="test_schema",
            old_table_name="old_table",
            new_table_name="new_table",
        )
        assert "ALTER TABLE test_schema.old_table RENAME TO new_table" in result

    def test_render_rename_table_sql_server(self):
        """Test rendering rename_table template for SQL Server."""
        sql_templates = SQLTemplates("sql_server")
        result = sql_templates.render(
            "rename_table",
            schema_name="test_schema",
            old_table_name="old_table",
            new_table_name="new_table",
        )
        assert "EXEC sp_rename 'test_schema.old_table', 'new_table'" in result

    def test_render_vacuum_table(self):
        """Test rendering vacuum_table template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("vacuum_table", schema_name="test_schema", table_name="test_table")
        assert "-- No-op for SQL warehouses" in result

    def test_render_list_schemas(self):
        """Test rendering list_schemas template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render("list_schemas")
        assert "SELECT SCHEMA_NAME as schema_name" in result
        assert "FROM INFORMATION_SCHEMA.SCHEMATA" in result

    def test_render_create_table_from_values(self):
        """Test rendering create_table_from_values template."""
        sql_templates = SQLTemplates("fabric")
        result = sql_templates.render(
            "create_table_from_values",
            schema_name="test_schema",
            table_name="test_table",
            values_clause="(1, 'test'), (2, 'test2')",
            column_names="id, name",
        )
        assert "SELECT" in result
        assert "INTO test_schema.test_table" in result
        assert "VALUES" in result
        assert "(1, 'test'), (2, 'test2')" in result
        assert "AS v(" in result and "id, name" in result

    def test_render_missing_required_parameter(self):
        """Test rendering template with missing required parameter."""
        sql_templates = SQLTemplates("fabric")
        with pytest.raises(
            exceptions.TemplateRuntimeError,
            match="Required parameter 'unknown' was not provided",
        ):
            sql_templates.render("check_schema_exists")

    def test_render_sql_server_dialect(self):
        """Test rendering with SQL Server dialect."""
        sql_templates = SQLTemplates("sql_server")
        result = sql_templates.render("check_schema_exists", schema_name="test_schema")
        assert "LOWER(SCHEMA_NAME) = LOWER('test_schema')" in result

    def test_templates_list_not_empty(self):
        """Test that TEMPLATES list is not empty."""
        sql_templates = SQLTemplates()
        assert len(sql_templates.TEMPLATES) > 0

    def test_templates_have_required_fields(self):
        """Test that all templates have required fields."""
        sql_templates = SQLTemplates()
        for template in sql_templates.TEMPLATES:
            assert "dialect" in template
            assert "file_name" in template
            assert "file_contents" in template
            assert "full_path" in template

    def test_templates_have_both_dialects(self):
        """Test that templates exist for both dialects."""
        sql_templates = SQLTemplates()
        dialects = set(t["dialect"] for t in sql_templates.TEMPLATES)
        assert "fabric" in dialects
        assert "sql_server" in dialects
