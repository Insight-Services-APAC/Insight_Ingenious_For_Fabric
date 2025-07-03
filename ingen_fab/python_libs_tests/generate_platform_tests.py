from __future__ import annotations

from pathlib import Path
import logging

from jinja2 import Template
from ingen_fab.python_libs.gather_python_libs import GatherPythonLibs
from ingen_fab.notebook_utils.notebook_utils import NotebookUtils
from rich.console import Console
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class GeneratePlatformTests:

    def inject_python_libs_into_template(self) -> None:
        """
        Analyze python_libs files, sort by dependencies, and inject into lib.py.jinja.
        """
        self.console: Console = Console()  # Initialize console for loggin
        logger.info("Starting injection of python libs into template.")
        
        # ToDo: Implement logic to inculde only some libs

        # List subdirs in notebook_utils/python_libs and populate a list
        lib_path = Path("ingen_fab/python_libs/")                
        templates_dir = Path("ingen_fab/notebook_utils/templates/platform_testing")
        tests_path = Path("ingen_fab/python_libs_tests")
        output_dir: Path = Path("sample_project/fabric_workspace_items/platform_testing")        
        notebook_utils = NotebookUtils(templates_dir=templates_dir)

        logger.debug(f"Looking for language directories in: {tests_path}")
        language_dirs = [subdir for subdir in tests_path.iterdir() if subdir.is_dir() and subdir.name != "__pycache__"]


        logger.info(f"Found language directories: {[d.name for d in language_dirs]}")

        for lang_dir in language_dirs:
            lang_dir: Path = Path(lib_path / lang_dir.name)  
            logger.info(f"Processing language directory: {lang_dir}")
            libs_to_include: list[str] = None

            gpl = GatherPythonLibs(
                console=self.console, include_jinja_raw_tags=False
            )  # Replace with actual console instance

            logger.debug(f"Gathering files for {lang_dir.name}")
            library_bundle = gpl.gather_files(
                lang_dir.name, libs_to_include
            )  # Call the method to gather and process files
                        

            # Write to lib.py.jinja template
            test_template_name = f"notebook-content_template-{lang_dir.name}.py.jinja"
            logger.debug(f"Loading template from: {test_template_name}")

            nb_template: Template = notebook_utils.load_template(test_template_name)
            nb_content = nb_template.render(
                library_bundle="\n".join(library_bundle),
                test_scripts="adadx"
            )

            # Loop through python_libs_tests folder and append to test_scripts
            test_scripts: list[str] = []
            test_scripts_dir = tests_path / lang_dir.name
            logger.debug(f"Looking for test scripts in: {test_scripts_dir}")
            if test_scripts_dir.exists():
                for test_file in test_scripts_dir.glob("*.py"):
                    logger.info(f"Adding test script: {test_file}")
                    with test_file.open("r", encoding="utf-8") as f:
                        test_scripts.append(f.read())

            logger.debug(f"Creating notebook for {lang_dir.name} in {output_dir}")
            notebook_utils.create_notebook_with_platform(
                notebook_name=f"{lang_dir.name}_platform_test",
                rendered_content=nb_content,
                output_dir=output_dir
            )
        
        logger.info("Completed injection of python libs into templates.")

if __name__ == "__main__":
    generator = GeneratePlatformTests()
    generator.inject_python_libs_into_template()