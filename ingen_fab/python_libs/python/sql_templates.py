from jinja2 import Template


class SQLTemplates:
    """Render SQL templates for different dialects."""

    TEMPLATES = {
        "check_table_exists": {
            "fabric": "SELECT TOP 1 * FROM {{ table_name }}",
            "sqlserver": "SELECT TOP 1 * FROM {{ table_name }}",
        },
        "drop_table": {
            "fabric": "DROP TABLE IF EXISTS {{ table_name }}",
            "sqlserver": "IF OBJECT_ID('{{ table_name }}', 'U') IS NOT NULL DROP TABLE {{ table_name }}",
        },
        "create_table_from_values": {
            "fabric": "SELECT * INTO {{ table_name }} FROM (VALUES {{ values_clause }}) AS v({{ column_names }})",
            "sqlserver": "SELECT * INTO {{ table_name }} FROM (VALUES {{ values_clause }}) AS v({{ column_names }})",
        },
        "insert_row": {
            "fabric": "INSERT INTO {{ table_name }} VALUES ({{ row_values }})",
            "sqlserver": "INSERT INTO {{ table_name }} VALUES ({{ row_values }})",
        },
        "list_tables": {
            "fabric": "SHOW TABLES{% if prefix %} LIKE '{{ prefix }}%'{% endif %}",
            "sqlserver": "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES{% if prefix %} WHERE TABLE_NAME LIKE '{{ prefix }}%'{% endif %}",
        },
    }

    def __init__(self, dialect: str = "fabric"):
        self.dialect = dialect

    def render(self, template_name: str, **kwargs) -> str:
        template_str = self.TEMPLATES[template_name][self.dialect]
        return Template(template_str).render(**kwargs)
