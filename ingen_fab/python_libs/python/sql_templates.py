from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined


class SQLTemplates:
    """Render SQL templates for different dialects."""

    def __init__(self, dialect: str = "fabric") -> None:
        if dialect not in ("fabric", "sqlserver"):
            raise ValueError("Invalid SQL dialect")
        self.dialect = dialect
        self.templates_dir = Path(__file__).parent / "sql_templates" / dialect
        self.jinja_environment = Environment(
            loader=FileSystemLoader(self.templates_dir),
            # undefined=StrictUndefined
        )

    def render(self, template_name: str, **kwargs) -> str:
        """Render a SQL template with the given parameters."""
        template_path = f"{template_name}.sql.jinja"
        try:
            self.jinja_environment.get_template(template_path)
        except Exception as e:
            raise FileNotFoundError(f"Template not found: {template_path}") from e
        
        return self.jinja_environment.get_template(template_path).render(**kwargs)
