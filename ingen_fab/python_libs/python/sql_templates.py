from jinja2 import Template, Environment, exceptions


def required_filter(value, var_name=""):
    """Jinja2 filter: raises an error if value is not provided or is falsy."""
    if value is None or (hasattr(value, "__len__") and len(value) == 0):
        raise exceptions.TemplateRuntimeError(
            f"Required parameter '{var_name or 'unknown'}' was not provided!"
        )
    return value


class SQLTemplates:
    """Render SQL templates for different dialects."""

    TEMPLATES = [
        {
            "dialect": "fabric",
            "file_name": "check_schema_exists.sql.jinja",
            "file_contents": "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE SCHEMA_NAME = '{{ schema_name | required }}'\n",
            "full_path": "./fabric/check_schema_exists.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "check_table_exists.sql.jinja",
            "file_contents": "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n",
            "full_path": "./fabric/check_table_exists.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "create_table_from_values.sql.jinja",
            "file_contents": "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n",
            "full_path": "./fabric/create_table_from_values.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "drop_table.sql.jinja",
            "file_contents": "DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n",
            "full_path": "./fabric/drop_table.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "insert_row.sql.jinja",
            "file_contents": "INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n",
            "full_path": "./fabric/insert_row.sql.jinja",
        },
        {
            "dialect": "fabric",
            "file_name": "list_tables.sql.jinja",
            "file_contents": "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n",
            "full_path": "./fabric/list_tables.sql.jinja",
        },
        {
            "dialect": "sqlserver",
            "file_name": "check_schema_exists.sql.jinja",
            "file_contents": "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE LOWER(SCHEMA_NAME) = LOWER('{{ schema_name | required }}')\n",
            "full_path": "./sqlserver/check_schema_exists.sql.jinja",
        },
        {
            "dialect": "sqlserver",
            "file_name": "check_table_exists.sql.jinja",
            "file_contents": "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n",
            "full_path": "./sqlserver/check_table_exists.sql.jinja",
        },
        {
            "dialect": "sqlserver",
            "file_name": "create_table_from_values.sql.jinja",
            "file_contents": "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n",
            "full_path": "./sqlserver/create_table_from_values.sql.jinja",
        },
        {
            "dialect": "sqlserver",
            "file_name": "drop_table.sql.jinja",
            "file_contents": "DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n",
            "full_path": "./sqlserver/drop_table.sql.jinja",
        },
        {
            "dialect": "sqlserver",
            "file_name": "insert_row.sql.jinja",
            "file_contents": "INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n",
            "full_path": "./sqlserver/insert_row.sql.jinja",
        },
        {
            "dialect": "sqlserver",
            "file_name": "list_tables.sql.jinja",
            "file_contents": "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n",
            "full_path": "./sqlserver/list_tables.sql.jinja",
        },
    ]

    def __init__(self, dialect: str = "fabric"):
        self.dialect = dialect
        # Use a Jinja2 Environment to add custom filters
        self.env = Environment()
        # Register the 'required' filter
        self.env.filters["required"] = lambda value, var_name="": required_filter(
            value, var_name
        )

    def get_template(self, template_name: str, dialect: str) -> str:
        """Get the SQL template for the specified dialect."""
        template = next(
            (
                t["file_contents"]
                for t in self.TEMPLATES
                if t["file_name"] == f"{template_name}.sql.jinja"
                and t["dialect"] == dialect
            ),
            None,
        )
        if not template:
            raise FileNotFoundError(
                f"Template {template_name} for dialect {dialect} not found."
            )
        return template

    def render(self, template_name: str, **kwargs) -> str:
        """Render a SQL template with the given parameters."""
        template_str = self.get_template(template_name, self.dialect)
        # Pass parameter names for error messages
        template = self.env.from_string(template_str)
        # Use kwargs for variable names
        params_with_names = {k: v for k, v in kwargs.items()}
        return template.render(**params_with_names)
