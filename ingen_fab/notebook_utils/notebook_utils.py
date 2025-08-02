import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from jinja2 import FileSystemLoader, Template, exceptions
from jinja2.environment import Environment
from rich.console import Console
from rich.panel import Panel

try:
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object
except ImportError:
    get_configs_as_object = None

from ingen_fab.python_libs.common.utils.path_utils import PathUtils


def required_filter(value, var_name=""):
    """Jinja2 filter: raises an error if value is not provided or is falsy."""
    if value is None or (hasattr(value, "__len__") and len(value) == 0):
        raise exceptions.TemplateRuntimeError(
            f"Required parameter '{var_name or 'unknown'}' was not provided!"
        )
    return value


class NotebookUtils:
    """
    Enhanced base class for handling notebook-related operations, including template rendering,
    notebook creation with platform metadata, and configuration management.
    
    This class serves as a foundation for specialized notebook generators like DDL generators
    and flat file ingestion compilers.
    """

    def __init__(
        self,
        templates_dir: Union[str, Path, List[Union[str, Path]]] = None,
        output_dir: Union[str, Path] = None,
        fabric_workspace_repo_dir: Union[str, Path] = None,
        console: Optional[Console] = None,
        enable_console: bool = True,
    ):
        """
        Initialize NotebookUtils with flexible template and output directory configuration.
        
        Args:
            templates_dir: Template directory path(s). Can be single path or list of paths for multi-path loading.
            output_dir: Output directory for generated notebooks
            fabric_workspace_repo_dir: Root directory of the fabric workspace repository
            console: Optional Rich Console instance
            enable_console: Whether to enable Rich console output
        """
        # Set up console
        self.console = console if console is not None else Console() if enable_console else None
        
        # Set up directories
        self._setup_directories(templates_dir, output_dir, fabric_workspace_repo_dir)
        
        # Set up Jinja2 environment with multi-path support
        self._setup_jinja_environment()
        
        if self.console:
            self._print_initialization_info()

    def _setup_directories(self, templates_dir, output_dir, fabric_workspace_repo_dir):
        """Set up directory paths with intelligent defaults."""
        # Set fabric workspace repo directory
        if fabric_workspace_repo_dir is None:
            fabric_workspace_repo_dir = PathUtils.get_workspace_repo_dir()
        self.fabric_workspace_repo_dir = Path(fabric_workspace_repo_dir).resolve()
        
        # Set output directory
        if output_dir is None:
            output_dir = self.fabric_workspace_repo_dir / "fabric_workspace_items"
        self.output_dir = Path(output_dir).resolve()
        
        # Set templates directory with multi-path support
        if templates_dir is None:
            # Default to common notebook templates
            templates_dir = Path(__file__).parent / "templates"
        
        if isinstance(templates_dir, (str, Path)):
            self.templates_dirs = [Path(templates_dir).resolve()]
        else:
            self.templates_dirs = [Path(td).resolve() for td in templates_dir]

    def _setup_jinja_environment(self):
        """Set up Jinja2 environment with multi-path template loading."""
        template_search_paths = [str(td) for td in self.templates_dirs]
        
        self.env = Environment(
            loader=FileSystemLoader(template_search_paths),
            autoescape=False
        )
        self.env.filters["required"] = lambda value, var_name="": required_filter(value, var_name)

    def _print_initialization_info(self):
        """Print initialization information to console."""
        if self.console:
            self.console.print(f"[bold blue]Fabric Workspace Dir:[/bold blue] {self.fabric_workspace_repo_dir}")
            self.console.print(f"[bold blue]Output Directory:[/bold blue] {self.output_dir}")
            self.console.print("[bold blue]Template Directories:[/bold blue]")
            for i, td in enumerate(self.templates_dirs, 1):
                self.console.print(f"  {i}. {td}")

    def add_template_directory(self, template_dir: Union[str, Path]) -> None:
        """Add an additional template directory to the search path."""
        new_dir = Path(template_dir).resolve()
        if new_dir not in self.templates_dirs:
            self.templates_dirs.append(new_dir)
            # Recreate Jinja environment with updated paths
            self._setup_jinja_environment()

    def load_config_variables(self) -> Dict[str, Any]:
        """Load configuration variables from the config system."""
        try:
            if get_configs_as_object is None:
                if self.console:
                    self.console.print("[yellow]Warning: config_utils not available[/yellow]")
                return {}
                
            configs = get_configs_as_object()
            return {
                "varlib": {
                    "config_workspace_id": getattr(configs, "config_workspace_id", ""),
                    "config_workspace_name": getattr(configs, "config_workspace_name", ""),
                    "config_lakehouse_workspace_id": getattr(configs, "config_lakehouse_workspace_id", ""),
                    "config_lakehouse_id": getattr(configs, "config_lakehouse_id", "")
                }
            }
        except Exception as e:
            if self.console:
                self.console.print(f"[yellow]Warning: Could not load config variables: {e}[/yellow]")
            return {"varlib": {}}

    def load_template(self, template_name: str) -> Template:
        """
        Load a Jinja2 template by name.

        Args:
            template_name: The name of the template file to load

        Returns:
            A Jinja2 Template object
        """
        return self.env.get_template(template_name)

    def render_template(self, template_name: str, **template_vars) -> str:
        """
        Load and render a template with the given variables.
        
        Args:
            template_name: The name of the template file to load
            **template_vars: Template variables to pass to the renderer
            
        Returns:
            Rendered template content as string
        """
        template = self.load_template(template_name)
        return template.render(**template_vars)

    def create_platform_metadata(
        self,
        notebook_name: str,
        display_name: str = None,
        description: str = None,
        notebook_type: str = "Notebook",
        logical_id: str = None,
    ) -> Dict[str, Any]:
        """
        Create platform metadata dictionary for a notebook.
        
        Args:
            notebook_name: The notebook name
            display_name: Display name (defaults to notebook_name)
            description: Optional description
            notebook_type: Type of notebook (default: "Notebook")
            logical_id: Logical ID (generates UUID if not provided)
            
        Returns:
            Platform metadata dictionary
        """
        return {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": notebook_type,
                "displayName": display_name or notebook_name,
                **({"description": description} if description else {})
            },
            "config": {
                "version": "2.0",
                "logicalId": logical_id or str(uuid.uuid4())
            }
        }

    def create_notebook_with_platform(
        self,
        notebook_name: str,
        rendered_content: str,
        output_dir: Path = None,
        display_name: str = None,
        description: str = None,
        platform_template: str = None,
    ) -> Path:
        """
        Create a notebook with its .platform file.

        Args:
            notebook_name: The name of the notebook (without extension)
            rendered_content: The rendered notebook content
            output_dir: The directory where the notebook should be created
            display_name: Display name for the notebook
            description: Optional description for the notebook
            platform_template: Optional platform template name (uses default if None)

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
        platform_path = output_path / ".platform"
        if not platform_path.exists():
            if platform_template:
                # Use template if provided
                try:
                    platform_metadata = self.render_template(
                        platform_template,
                        notebook_name=display_name or notebook_name,
                        guid=uuid.uuid4()
                    )
                except Exception:
                    # Fallback to programmatic generation
                    platform_metadata = json.dumps(
                        self.create_platform_metadata(
                            notebook_name, display_name, description
                        ), 
                        indent=2
                    )
            else:
                # Use programmatic generation
                platform_metadata = json.dumps(
                    self.create_platform_metadata(
                        notebook_name, display_name, description
                    ), 
                    indent=2
                )
            
            with platform_path.open("w", encoding="utf-8") as f:
                f.write(platform_metadata)

        if self.console:
            self.console.print(f"[green]✓ Notebook created: {notebook_file}[/green]")

        return output_path

    def copy_files(self, source_files: List[tuple], base_source_dir: Path, base_output_dir: Path) -> List[Path]:
        """
        Copy multiple files from source to destination with rename support.
        
        Args:
            source_files: List of (source_filename, target_filename) tuples
            base_source_dir: Base directory containing source files
            base_output_dir: Base directory for output files
            
        Returns:
            List of successfully copied file paths
        """
        copied_files = []
        base_output_dir.mkdir(parents=True, exist_ok=True)
        
        for source_file, target_file in source_files:
            source_path = base_source_dir / source_file
            target_path = base_output_dir / target_file
            
            if source_path.exists():
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with source_path.open("r", encoding="utf-8") as f:
                    content = f.read()
                with target_path.open("w", encoding="utf-8") as f:
                    f.write(content)
                copied_files.append(target_path)
                
                if self.console:
                    self.console.print(f"[green]✓ File copied: {target_path}[/green]")
            elif self.console:
                self.console.print(f"[yellow]Warning: Source file not found: {source_path}[/yellow]")
        
        return copied_files

    def generate_guid(self, input_string: str) -> str:
        """Generate a deterministic GUID by hashing the input string."""
        import hashlib
        return hashlib.sha256(str(input_string).encode("utf-8")).hexdigest()[:12]

    def print_success_panel(self, title: str, message: str, border_style: str = "green") -> None:
        """Print a success panel using Rich console."""
        if self.console:
            self.console.print(
                Panel.fit(
                    f"[bold green]{message}[/bold green]",
                    title=f"[bold]{title}[/bold]",
                    border_style=border_style
                )
            )

    def print_header_panel(self, title: str, border_style: str = "cyan") -> None:
        """Print a header panel using Rich console."""
        if self.console:
            self.console.print(
                Panel.fit(
                    f"[bold cyan]{title}[/bold cyan]",
                    border_style=border_style
                )
            )
