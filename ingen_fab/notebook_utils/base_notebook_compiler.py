"""
Base Notebook Compiler

This module provides a base class for notebook compilers that extends NotebookUtils
with additional functionality common to various notebook generation tasks.
"""

from pathlib import Path
from typing import Any, Dict, List, Union

from .notebook_utils import NotebookUtils


class BaseNotebookCompiler(NotebookUtils):
    """
    Base class for notebook compilers that extends NotebookUtils with additional
    functionality common to various notebook generation tasks like DDL generation
    and flat file ingestion.
    """

    def __init__(
        self,
        templates_dir: Union[str, Path, List[Union[str, Path]]] = None,
        output_dir: Union[str, Path] = None,
        fabric_workspace_repo_dir: Union[str, Path] = None,
        package_name: str = None,
        **kwargs
    ):
        """
        Initialize BaseNotebookCompiler.
        
        Args:
            templates_dir: Template directory path(s)
            output_dir: Output directory for generated notebooks
            fabric_workspace_repo_dir: Root directory of the fabric workspace repository
            package_name: Name of the package/module (used for logging and organization)
            **kwargs: Additional arguments passed to NotebookUtils
        """
        # Add unified templates to search path
        unified_templates_dir = Path(__file__).parent.parent / "templates"
        
        if templates_dir is None:
            templates_dir = [unified_templates_dir]
        elif isinstance(templates_dir, (str, Path)):
            templates_dir = [Path(templates_dir), unified_templates_dir]
        else:
            templates_dir = list(templates_dir) + [unified_templates_dir]
        
        super().__init__(
            templates_dir=templates_dir,
            output_dir=output_dir,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            **kwargs
        )
        
        self.package_name = package_name or "notebook_compiler"

    def compile_notebook_from_template(
        self, 
        template_name: str, 
        output_notebook_name: str,
        template_vars: Dict[str, Any] = None,
        display_name: str = None,
        description: str = None,
        output_subdir: str = None
    ) -> Path:
        """
        Compile a notebook from a template with optional configuration injection.
        
        Args:
            template_name: Name of the template file
            output_notebook_name: Name for the output notebook
            template_vars: Variables to pass to the template
            display_name: Display name for the notebook
            description: Description for the notebook
            output_subdir: Optional subdirectory within output_dir
            
        Returns:
            Path to the created notebook
        """
        if template_vars is None:
            template_vars = {}
        
        # Load and merge configuration variables
        config_vars = self.load_config_variables()
        template_vars.update(config_vars)
        
        # Render template
        rendered_content = self.render_template(template_name, **template_vars)
        
        # Determine output directory
        output_dir = self.output_dir
        if output_subdir:
            output_dir = output_dir / output_subdir
        
        # Create notebook
        return self.create_notebook_with_platform(
            notebook_name=output_notebook_name,
            rendered_content=rendered_content,
            output_dir=output_dir,
            display_name=display_name,
            description=description
        )

    def compile_ddl_scripts(
        self, 
        ddl_source_dir: Path, 
        ddl_output_base: Path,
        script_mappings: Dict[str, List[tuple]]
    ) -> Dict[str, List[Path]]:
        """
        Compile DDL scripts by copying files according to mappings.
        
        Args:
            ddl_source_dir: Source directory containing DDL scripts
            ddl_output_base: Base output directory for DDL scripts
            script_mappings: Dict mapping target directories to list of (source, target) file tuples
            
        Returns:
            Dict mapping target directories to lists of successfully copied files
        """
        results = {}
        
        for target_subdir, file_mappings in script_mappings.items():
            target_dir = ddl_output_base / target_subdir
            copied_files = self.copy_files(file_mappings, ddl_source_dir, target_dir)
            results[target_subdir] = copied_files
        
        return results

    def compile_all_with_results(
        self, 
        compile_functions: List[tuple],
        header_title: str = None
    ) -> Dict[str, Any]:
        """
        Execute multiple compilation functions and return consolidated results.
        
        Args:
            compile_functions: List of (function, args, kwargs) tuples to execute
            header_title: Optional title for header panel
            
        Returns:
            Dictionary with compilation results and success/error tracking
        """
        if header_title:
            self.print_header_panel(header_title)
        
        results = {
            "success": True,
            "errors": [],
            "compiled_items": {}
        }
        
        try:
            for func_info in compile_functions:
                if len(func_info) == 2:
                    func, args = func_info
                    kwargs = {}
                elif len(func_info) == 3:
                    func, args, kwargs = func_info
                else:
                    func = func_info[0]
                    args = []
                    kwargs = {}
                
                # Execute function
                if isinstance(args, dict):
                    # If args is actually kwargs
                    result = func(**args)
                elif isinstance(args, (list, tuple)):
                    result = func(*args, **kwargs)
                else:
                    result = func(args, **kwargs)
                
                # Store result with function name as key
                func_name = getattr(func, '__name__', str(func))
                results["compiled_items"][func_name] = result
        
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            if self.console:
                self.console.print(f"[red]Error during compilation: {e}[/red]")
            raise
        
        return results

    def get_sorted_directories(self, parent_dir: Path) -> List[Path]:
        """
        Get sorted directories based on numeric prefix convention (e.g., '1_name', '2_name').
        Alerts if any directory names don't match the expected convention.
        """
        directories = []
        errors = []

        for path in parent_dir.iterdir():
            if path.is_dir():
                try:
                    # Check if name matches pattern: number_name
                    parts = path.name.split("_", 1)
                    if len(parts) < 2:
                        errors.append(
                            f"Directory '{path.name}' doesn't match expected pattern 'number_name'"
                        )
                        continue

                    # Try to parse the numeric prefix
                    numeric_prefix = int(parts[0])
                    directories.append((numeric_prefix, path))
                except ValueError:
                    errors.append(
                        f"Directory '{path.name}' doesn't have a valid numeric prefix"
                    )
                except Exception as e:
                    errors.append(
                        f"Error processing directory '{path.name}': {str(e)}"
                    )

        if errors and self.console:
            self.console.print(
                "\n[bold yellow]⚠️  WARNING: The following directories don't "
                "match the expected naming convention:[/bold yellow]"
            )
            for error in errors:
                self.console.print(f"  [red]✗[/red] {error}")
            self.console.print()

        # Sort by numeric prefix and return just the paths
        sorted_dirs = sorted(directories, key=lambda x: x[0])
        return [path for _, path in sorted_dirs]

    def get_sorted_files(self, parent_dir: Path, extensions: List[str] = None) -> List[Path]:
        """
        Get sorted files based on numeric prefix convention (e.g., '1_name.py', '2_name.sql').
        Alerts if any file names don't match the expected convention.

        Args:
            parent_dir: The directory to scan for files
            extensions: List of file extensions to include (e.g., ['.py', '.sql']). If None, includes all files.
        """
        files = []
        errors = []

        for path in parent_dir.iterdir():
            if path.is_file():
                # Check if we should filter by extension
                if extensions and path.suffix not in extensions:
                    continue

                try:
                    # Check if name matches pattern: number_name.extension
                    name_without_ext = path.stem
                    parts = name_without_ext.split("_", 1)
                    if len(parts) < 2:
                        errors.append(
                            f"File '{path.name}' doesn't match expected pattern 'number_name.extension'"
                        )
                        continue

                    # Try to parse the numeric prefix
                    numeric_prefix = int(parts[0])
                    files.append((numeric_prefix, path))
                except ValueError:
                    errors.append(
                        f"File '{path.name}' doesn't have a valid numeric prefix"
                    )
                except Exception as e:
                    errors.append(
                        f"Error processing file '{path.name}': {str(e)}"
                    )

        if errors and self.console:
            self.console.print(
                "\n[bold yellow]⚠️  WARNING: The following files don't "
                "match the expected naming convention:[/bold yellow]"
            )
            for error in errors:
                self.console.print(f"  [red]✗[/red] {error}")
            self.console.print()

        # Sort by numeric prefix and return just the paths
        sorted_files = sorted(files, key=lambda x: x[0])
        return [path for _, path in sorted_files]