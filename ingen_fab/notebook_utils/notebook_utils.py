from pathlib import Path
import uuid
from jinja2 import FileSystemLoader, Template, exceptions
from jinja2.environment import Environment


def required_filter(value, var_name=""):
    """Jinja2 filter: raises an error if value is not provided or is falsy."""
    if value is None or (hasattr(value, "__len__") and len(value) == 0):
        raise exceptions.TemplateRuntimeError(
            f"Required parameter '{var_name or 'unknown'}' was not provided!"
        )
    return value


class NotebookUtils:
    """
    A utility class for handling notebook-related operations, such as rendering templates
    and creating notebooks with platform metadata.
    """

    def __init__(
        self,
        templates_dir: Path = Path(
            "ingen_fab/notebook_utils/templates/platform_testing"
        ),
        output_dir: Path = Path(
            "sample_project/fabric_workspace_items/platform_testing"
        ),
    ):
        # Jinja2 Environment
        env = Environment(loader=FileSystemLoader(str(templates_dir)))
        env.filters["required"] = lambda value, var_name="": required_filter(
            value, var_name
        )
        self.env: Environment = env

    def load_template(self, template_name: str) -> Template:
        """
        Load a Jinja2 template by name.

        Args:
            template_name: The name of the template file to load

        Returns:
            A Jinja2 Template object
        """
        return self.env.get_template(template_name)

    def create_notebook_with_platform(
        self,
        notebook_name: str,
        rendered_content: str,
        output_dir: Path = None,
    ) -> Path:
        """
        Create a notebook with its .platform file.

        Args:
            notebook_name: The name of the notebook (without extension)
            rendered_content: The rendered notebook content
            output_dir: The directory where the notebook should be created

        Returns:
            Path to the created notebook directory
        """
        if output_dir is None:
            output_dir = self.output_dir

        # Create output path
        output_path = output_dir / f"{notebook_name}.Notebook"
        output_path.mkdir(parents=True, exist_ok=True)

        # Write notebook content
        notebook_file = output_path / "notebook-content.py"
        with notebook_file.open("w", encoding="utf-8") as f:
            f.write(rendered_content)

        # Create .platform file
        platform_template = self.load_template("platform_file_template.json.jinja")
        platform_metadata = platform_template.render(
            notebook_name=f"{notebook_name}", guid=uuid.uuid4()
        )

        platform_path = output_path / ".platform"
        # Only create .platform if it does not exist
        if not platform_path.exists():
            with platform_path.open("w", encoding="utf-8") as f:
                f.write(platform_metadata)

        return output_path
