from __future__ import annotations

from pathlib import Path

from jinja2 import Template
from ingen_fab.python_libs.gather_python_libs import GatherPythonLibs
from ingen_fab.notebook_utils.notebook_utils import NotebookUtils
from ingen_fab.cli_utils.console_styles import ConsoleStyles
from rich.console import Console


class GeneratePlatformTests:
    def inject_python_libs_into_template(self) -> None:
        """
        Analyze python_libs files, sort by dependencies, and inject into lib.py.jinja.
        """
        self.console: Console = Console()  # Initialize console for logging
        ConsoleStyles.print_success(
            self.console, "Starting injection of python libs into template."
        )
        # ToDo: Implement logic to include only some libs
        lib_path = Path("ingen_fab/python_libs/")
        templates_dir = Path("ingen_fab/notebook_utils/templates/platform_testing")
        tests_path = Path("ingen_fab/python_libs_tests")
        output_dir: Path = Path(
            "sample_project/fabric_workspace_items/platform_testing"
        )
        notebook_utils = NotebookUtils(templates_dir=templates_dir)

        ConsoleStyles.print_dim(
            self.console, f"Looking for language directories in: {tests_path}"
        )
        language_dirs = [
            subdir
            for subdir in tests_path.iterdir()
            if subdir.is_dir() and subdir.name != "__pycache__"
        ]
        ConsoleStyles.print_dim(
            self.console,
            f"Found language directories: {[d.name for d in language_dirs]}",
        )

        for lang_dir in language_dirs:
            lang_dir: Path = Path(lib_path / lang_dir.name)
            ConsoleStyles.print_warning(
                self.console, f"Processing language directory: {lang_dir}"
            )
            libs_to_include: list[str] = None

            gpl = GatherPythonLibs(console=self.console, include_jinja_raw_tags=False)

            ConsoleStyles.print_dim(
                self.console, f"Gathering files for {lang_dir.name}"
            )
            library_bundle = gpl.gather_files(lang_dir.name, libs_to_include)
            # Loop through python_libs_tests folder and append to test_scripts
            test_scripts: list[str] = []
            test_scripts_dir = tests_path / lang_dir.name
            ConsoleStyles.print_dim(
                self.console, f"Searching for test scripts in: {test_scripts_dir}"
            )
            found_files = (
                list(test_scripts_dir.glob("*.py")) if test_scripts_dir.exists() else []
            )
            ConsoleStyles.print_dim(
                self.console,
                f"Found test script files: {[str(f) for f in found_files]}",
            )
            if not found_files:
                ConsoleStyles.print_error(
                    self.console,
                    f"No test scripts found in {test_scripts_dir}. Aborting.",
                )
                raise FileNotFoundError(f"No test scripts found in {test_scripts_dir}")
            for test_file in found_files:
                ConsoleStyles.print_dim(
                    self.console, f"Adding test script: {test_file}"
                )
                with test_file.open("r", encoding="utf-8") as f:
                    test_scripts.append(f.read())
            # Write to lib.py.jinja template
            test_template_name = f"notebook-content_template-{lang_dir.name}.py.jinja"
            ConsoleStyles.print_dim(
                self.console, f"Loading template from: {test_template_name}"
            )
            nb_template: Template = notebook_utils.load_template(test_template_name)
            nb_content = nb_template.render(
                library_bundle="\n".join(library_bundle),
                test_scripts="\n\n".join(test_scripts),
            )
            ConsoleStyles.print_success(
                self.console, f"Creating notebook for {lang_dir.name} in {output_dir}"
            )
            notebook_utils.create_notebook_with_platform(
                notebook_name=f"{lang_dir.name}_platform_test",
                rendered_content=nb_content,
                output_dir=output_dir,
            )
        ConsoleStyles.print_success(
            self.console, "Completed injection of python libs into templates."
        )



def main() -> None:
    generator = GeneratePlatformTests()
    generator.inject_python_libs_into_template()


if __name__ == "__main__":
    main()
