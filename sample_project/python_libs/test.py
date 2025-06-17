
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

BASE_DIR = Path.cwd()
TEMPLATES_DIR = BASE_DIR / "notebook_partials"
OUTPUT_DIR = (
    BASE_DIR / "output"
).resolve()

# Jinja2 Environment
env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))


def render_template(template_name, **context):
    """Render a Jinja template with the given context."""
    template = env.get_template(template_name)
    return template.render(**context)


def load_template(template_name):
    """Load a Jinja template."""
    return env.get_template(template_name)


def save_rendered_template(template_name, output_path, **context):
    """Render a Jinja template and save the output to a file."""
    # Create output directory if it doesn't exist
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    rendered_content = render_template(template_name, **context)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_content)


save_rendered_template("parquet_load_utils_spark.py.jinja", OUTPUT_DIR / "parquet_load_utils_spark.py")