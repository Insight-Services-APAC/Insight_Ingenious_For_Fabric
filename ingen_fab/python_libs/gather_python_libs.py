import ast
import re
import traceback
from pathlib import Path
from typing import List, Set


class GatherPythonLibs:
    def __init__(self, console, include_jinja_raw_tags: bool = True):
        """ """
        self.console = console
        self.include_jinja_raw_tags = include_jinja_raw_tags

    def analyze_dependencies(self, script_content: str) -> List[str]:
        """
        Analyze script content to extract imported Python libraries.

        Args:
            script_content: The content of the Python script to analyze.

        Returns:
            A list of unique Python libraries imported in the script.
        """
        tree = ast.parse(script_content)
        imports = set()

        # Extract imported libraries from import statements
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module)

        # Filter out built-in libraries and keep only external libraries
        external_imports = [lib for lib in imports if not self.is_builtin_library(lib)]
        return list(external_imports)

    def is_builtin_library(self, library_name: str) -> bool:
        """
        Check if a library is a built-in Python library.

        Args:
            library_name: The name of the library to check.

        Returns:
            True if the library is a built-in library, False otherwise.
        """
        # List of standard Python libraries (this can be extended)
        builtin_libraries = [
            "sys",
            "os",
            "json",
            "re",
            "time",
            "datetime",
            "math",
            "random",
            "string",
            "ast",
            "uuid",
            "hashlib",
            "pathlib",
        ]
        return library_name in builtin_libraries

    def inject_libraries(self, libraries: List[str], target_file: Path):
        """
        Inject library installation commands into the target file.

        Args:
            libraries: A list of libraries to inject.
            target_file: The target file to inject the libraries into.
        """
        # Read the existing content of the target file
        with target_file.open("r", encoding="utf-8") as f:
            content = f.read()

        # Inject the libraries at the beginning of the file
        for library in libraries:
            # Skip if the library is already imported
            if re.search(rf"^\s*import\s+{library}\b", content, re.MULTILINE):
                continue
            if re.search(rf"^\s*from\s+{library}\s+import\b", content, re.MULTILINE):
                continue

            # Add import statement at the top
            content = f"import {library}\n" + content

        # Write the modified content back to the target file
        with target_file.open("w", encoding="utf-8") as f:
            f.write(content)

    def analyze_file_dependencies(self, file_path: Path) -> Set[str]:
        """
        Analyze a Python file to extract its relative import dependencies.
        Returns a set of module names that this file depends on.
        """
        dependencies = set()

        try:
            with file_path.open("r", encoding="utf-8") as f:
                content = f.read()

            # Parse the AST to find imports
            tree = ast.parse(content)

            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    # Handle relative imports like "from .module_name import ..."
                    if node.module and node.level > 0:
                        # Remove leading dots for relative imports
                        module_name = node.module
                        dependencies.add(module_name)
                elif isinstance(node, ast.Import):
                    # Handle direct imports like "import .module_name"
                    for alias in node.names:
                        if alias.name.startswith("."):
                            # Remove leading dot
                            module_name = alias.name[1:]
                            dependencies.add(module_name)

            # Also check for comment-based dependency hints
            # Look for patterns like: # { "depends_on": "module_name" }
            comment_pattern = r'#\s*{\s*"depends_on"\s*:\s*"([^"]+)"\s*}'
            matches = re.findall(comment_pattern, content)
            for match in matches:
                dependencies.add(match)

        except Exception as e:
            self.console.print(
                f"[yellow]Warning: Could not analyze {file_path.name}: {e}\n"
                f"Stack trace:\n{traceback.format_exc()}[/yellow]"
            )

        return dependencies

    def topological_sort_files(self, python_files: List[Path]) -> List[Path]:
        """
        Sort Python files based on their dependencies using topological sort.
        Files with no dependencies come first, followed by files that depend on them.
        """
        # Create mapping from filename (without extension) to Path object
        file_map = {f.stem: f for f in python_files}

        # Build dependency graph
        dependencies = {}
        for file_path in python_files:
            deps = self.analyze_file_dependencies(file_path)
            # Only keep dependencies that exist in our file set
            valid_deps = {dep for dep in deps if dep in file_map}
            dependencies[file_path.stem] = valid_deps

        # Perform topological sort
        sorted_files = []
        visited = set()
        temp_visited = set()

        def visit(filename: str):
            if filename in temp_visited:
                # Circular dependency - handle gracefully
                self.console.print(
                    f"[yellow]Warning: Circular dependency detected involving "
                    f"{filename}[/yellow]"
                )
                return
            if filename in visited:
                return

            temp_visited.add(filename)

            # Visit dependencies first
            for dep in dependencies.get(filename, set()):
                if dep in file_map:  # Only visit if the dependency file exists
                    visit(dep)

            temp_visited.remove(filename)
            visited.add(filename)
            if filename in file_map:
                sorted_files.append(file_map[filename])

        # Visit all files
        for filename in file_map.keys():
            if filename not in visited:
                visit(filename)

        return sorted_files

    def gather_files(self, python_libs_path: Path, libs_to_include: List[str]) -> []:
        """
        Analyze python_libs files, sort by dependencies, and inject into lib.py.jinja.
        """
        # Try multiple possible paths for python_libs
        possible_paths = [Path(__file__).resolve().parent / python_libs_path]

        python_libs_path = None
        for path in possible_paths:
            if path.exists():
                python_libs_path = path
                break

        if python_libs_path is None:
            self.console.print(
                f"[yellow]Warning: Python libs path not found in any of: "
                f"{[str(p) for p in possible_paths]}[/yellow]"
            )
            return
        # Get all Python files
        if libs_to_include:
            # Filter files based on libs_to_include list
            python_files = [
                f
                for f in python_libs_path.iterdir()
                if f.is_file()
                and f.suffix == ".py"
                and not f.name.startswith("__")
                and f.stem in libs_to_include
            ]
        else:
            # Include all files if libs_to_include is empty
            python_files = [
                f
                for f in python_libs_path.iterdir()
                if f.is_file() and f.suffix == ".py" and not f.name.startswith("__")
            ]

        if not python_files:
            self.console.print(
                f"[yellow]Warning: No Python files found in {python_libs_path}[/yellow]"
            )
            return

        self.console.print(
            f"[blue]Found {len(python_files)} Python library files[/blue]"
        )

        # Sort files by dependencies
        sorted_files = self.topological_sort_files(python_files)

        # Display dependency order
        self.console.print("\n[bold]Dependency-sorted file order:[/bold]")
        for i, file_path in enumerate(sorted_files, 1):
            deps = self.analyze_file_dependencies(file_path)
            valid_deps = {
                dep for dep in deps if any(f.stem == dep for f in python_files)
            }
            dep_str = f" (depends on: {', '.join(valid_deps)})" if valid_deps else ""
            self.console.print(f"  {i}. {file_path.name}{dep_str}")

        # Read and combine file contents
        combined_content = []
        if self.include_jinja_raw_tags:
            combined_content.append("{% raw %}")
        combined_content.append("# Auto-generated library code from python_libs")
        combined_content.append("# Files are ordered based on dependency analysis\n")

        for file_path in sorted_files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    content = f.read()

                # Remove relative imports since we're combining files
                lines = content.split("\n")
                filtered_lines = []

                for line in lines:  # Skip relative import lines
                    if line.strip().startswith("from .") or line.strip().startswith(
                        "import ."
                    ):
                        continue
                    filtered_lines.append(line)

                # Add file section
                combined_content.append(f"\n# === {file_path.name} ===")
                combined_content.append("\n".join(filtered_lines))

            except Exception as e:
                self.console.print(f"[red]Error reading {file_path.name}: {e}[/red]")

        if self.include_jinja_raw_tags:
            combined_content.append("{% endraw %}")

        return combined_content
