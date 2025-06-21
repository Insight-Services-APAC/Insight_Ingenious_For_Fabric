from jinja2 import Template


class SQLTemplates:
    """Render SQL templates for different dialects."""

    TEMPLATES = {
        "check_table_exists": {
            "fabric":   """
                            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = '{{ schema_name }}' AND TABLE_NAME = '{{ table_name }}'
                        """,
            "sqlserver":   """
                            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = '{{ schema_name }}' AND TABLE_NAME = '{{ table_name }}'
                        """,
        },
        "drop_table": {
            "fabric": "DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }}",
            "sqlserver": "DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }}'",
        },
        "create_table_from_values": {
            "fabric": "SELECT * INTO {{ schema_name }}.{{ table_name }} FROM (VALUES {{ values_clause }}) AS v({{ column_names }})",
            "sqlserver": "SELECT * INTO {{ schema_name }}.{{ table_name }} FROM (VALUES {{ values_clause }}) AS v({{ column_names }})",
        },
        "insert_row": {
            "fabric": "INSERT INTO {{ schema_name }}.{{ table_name }} VALUES ({{ row_values }})",
            "sqlserver": "INSERT INTO {{ schema_name }}.{{ table_name }} VALUES ({{ row_values }})",
        },
        "list_tables": {
            "fabric": "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES{% if prefix %} WHERE TABLE_NAME LIKE '{{ prefix }}%'{% endif %}",
            "sqlserver": "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES{% if prefix %} WHERE TABLE_NAME LIKE '{{ prefix }}%'{% endif %}",
        },
    }

    def __init__(self, dialect: str = "fabric"):
        self.dialect = dialect

    def render(self, template_name: str, **kwargs) -> str:
        template_str = self.TEMPLATES[template_name][self.dialect]
        return Template(template_str).render(**kwargs)
