from jinja2 import Environment, exceptions


def required_filter(value, var_name=""):
    """Jinja2 filter: raises an error if value is not provided or is falsy."""
    if value is None or (hasattr(value, "__len__") and len(value) == 0):
        raise exceptions.TemplateRuntimeError(f"Required parameter '{var_name or 'unknown'}' was not provided!")
    return value


class SQLTemplates:
    """Render SQL templates for different dialects."""

    TEMPLATES = [
        {
            "dialect": "fabric",
            "file_name": "get_extract_configuration.sql.jinja",
            "file_contents": "SELECT * \nFROM {{ config_schema | default('config') }}.config_extract_generation\nWHERE extract_name = '{{ extract_name | required(\"extract_name\") }}'\nAND is_active = 1;",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_extract_configuration.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "insert_ddl_log.sql.jinja",
            "file_contents": "INSERT INTO {{ schema_name | required }}.{{ table_name | required }}\n(script_id, script_name, execution_status, update_date)\nVALUES ('{{ script_id | required }}', '{{ script_name | required }}', '{{ execution_status | required }}', '{{ update_date | required }}')",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/insert_ddl_log.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "create_table.sql.jinja",
            "file_contents": "CREATE TABLE {{ schema_name | required }}.{{ table_name | required }} (\n    {%- for col, dtype in schema.items() %}\n        {{ col }} {{ dtype }}{% if not loop.last %}, {% endif %}\n    {%- endfor %}\n)\n{% if options %}\n    {%- for k, v in options.items() %}\n        {{ k }} = '{{ v }}'{% if not loop.last %}, {% endif %}\n    {%- endfor %}\n{% endif %}\n;\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/create_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "get_extract_history.sql.jinja",
            "file_contents": "SELECT TOP {{ limit | default(10) }}\n    run_timestamp,\n    run_status,\n    run_type,\n    rows_extracted,\n    rows_written,\n    files_generated,\n    duration_seconds,\n    error_message\nFROM {{ log_schema | default('log') }}.log_extract_generation\nWHERE extract_name = '{{ extract_name | required }}'\nORDER BY run_timestamp DESC;",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_extract_history.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "get_table_schema.sql.jinja",
            "file_contents": "SELECT COLUMN_NAME, DATA_TYPE\nFROM INFORMATION_SCHEMA.COLUMNS\nWHERE TABLE_SCHEMA = '{{ schema_name | required }}'\n  AND TABLE_NAME = '{{ table_name | required }}'\n;\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_table_schema.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "read_table.sql.jinja",
            "file_contents": "SELECT {% if columns %}{{ columns | join(', ') }}{% else %}*{% endif %}\nFROM {{ schema_name | required }}.{{ table_name | required }}\n{% if filters %}\nWHERE\n    {%- for col, val in filters.items() %}\n        {{ col }} = '{{ val }}'{% if not loop.last %} AND {% endif %}\n    {%- endfor %}\n{% endif %}\n{% if limit %}\nLIMIT {{ limit }}\n{% endif %}\n;\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/read_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "drop_table.sql.jinja",
            "file_contents": "DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/drop_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "get_table_row_count.sql.jinja",
            "file_contents": "SELECT COUNT(*)\nFROM {{ schema_name | required }}.{{ table_name | required }};\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_table_row_count.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "create_schema.sql.jinja",
            "file_contents": "CREATE SCHEMA {{ schema_name | required }};",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/create_schema.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "get_all_active_extracts.sql.jinja",
            "file_contents": "SELECT \n    gen.*,\n    det.*\nFROM {{ config_schema | default('config') }}.config_extract_generation gen\nINNER JOIN {{ config_schema | default('config') }}.config_extract_details det \n    ON gen.extract_name = det.extract_name\nWHERE gen.is_active = 1 \nAND det.is_active = 1\n{% if execution_group %}\nAND gen.execution_group = '{{ execution_group }}'\n{% endif %}\nORDER BY gen.extract_name;",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_all_active_extracts.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "get_table_metadata.sql.jinja",
            "file_contents": "SELECT *\nFROM INFORMATION_SCHEMA.TABLES\nWHERE TABLE_SCHEMA = '{{ schema_name | required }}'\n  AND TABLE_NAME = '{{ table_name | required }}';\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_table_metadata.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "check_table_exists.sql.jinja",
            "file_contents": "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/check_table_exists.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "get_extract_details.sql.jinja",
            "file_contents": "SELECT * \nFROM {{ config_schema | default('config') }}.config_extract_details\nWHERE extract_name = '{{ extract_name | required }}'\nAND is_active = 1;",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/get_extract_details.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "insert_row.sql.jinja",
            "file_contents": "INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/insert_row.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "delete_from_table.sql.jinja",
            "file_contents": "DELETE FROM {{ schema_name | required }}.{{ table_name | required }}\n{% if filters %}\nWHERE\n    {%- for col, val in filters.items() %}\n        {{ col }} = '{{ val }}'{% if not loop.last %} AND {% endif %}\n    {%- endfor %}\n{% endif %}\n;\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/delete_from_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "vacuum_table.sql.jinja",
            "file_contents": "-- No-op for SQL warehouses. This template is required for interface compatibility.\n-- VACUUM is not supported in standard SQL Server/Fabric warehouses.\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/vacuum_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "check_script_executed.sql.jinja",
            "file_contents": "SELECT count(*)\nFROM {{ schema_name | required }}.{{ table_name | required }}\nWHERE script_id = '{{ script_id | required }}'\nAND lower(execution_status) = 'success'",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/check_script_executed.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "create_ddl_log_table.sql.jinja",
            "file_contents": "CREATE TABLE {{ schema_name | required }}.{{ table_name | required }} (\n    script_id VARCHAR(255) NOT NULL,\n    script_name VARCHAR(255) NOT NULL,\n    execution_status VARCHAR(50) NOT NULL,\n    update_date DATETIME2(0) NOT NULL\n)",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/create_ddl_log_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "list_tables.sql.jinja",
            "file_contents": "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/list_tables.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "list_schemas.sql.jinja",
            "file_contents": "SELECT SCHEMA_NAME as schema_name\nFROM INFORMATION_SCHEMA.SCHEMATA;\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/list_schemas.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "rename_table.sql.jinja",
            "file_contents": "EXEC sp_rename '{{ schema_name | required }}.{{ old_table_name | required }}', '{{ new_table_name | required }}';\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/rename_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "check_schema_exists.sql.jinja",
            "file_contents": "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE SCHEMA_NAME = '{{ schema_name | required }}'\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/check_schema_exists.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "create_table_from_values.sql.jinja",
            "file_contents": "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n",
            "full_path": "ingen_fab/python_libs/python/sql_template_factory/fabric/create_table_from_values.sql.jinja",
        },
    ]

    def __init__(self, dialect: str = "fabric"):
        self.dialect = dialect
        # Use a Jinja2 Environment to add custom filters
        self.env = Environment()
        # Register the 'required' filter
        self.env.filters["required"] = lambda value, var_name="": required_filter(value, var_name)

    def get_template(self, template_name: str, dialect: str) -> str:
        """Get the SQL template for the specified dialect."""
        # Always use fabric templates as the source
        template = next(
            (
                t["file_contents"]
                for t in self.TEMPLATES
                if t["file_name"] == f"{template_name}.sql.jinja" and t["dialect"] == "fabric"
            ),
            None,
        )
        if not template:
            raise FileNotFoundError(f"Template {template_name} for dialect fabric not found.")
        return template

    def render(self, template_name: str, **kwargs) -> str:
        """Render a SQL template with the given parameters and translate if needed."""
        # Always get fabric template
        template_str = self.get_template(template_name, "fabric")

        # Render the template with Jinja2
        template = self.env.from_string(template_str)
        params_with_names = {k: v for k, v in kwargs.items()}
        rendered_sql = template.render(**params_with_names)

        # Translate SQL if needed using SQLGlot
        from ingen_fab.python_libs.python.sql_translator import get_sql_translator

        try:
            translator = get_sql_translator()
            translated_sql = translator.translate_sql(rendered_sql)
            return translated_sql
        except Exception as e:
            # Log the error but don't fail - return original SQL
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"SQL translation failed for template {template_name}: {e}")
            logger.warning(f"Returning original SQL: {rendered_sql}")
            return rendered_sql
