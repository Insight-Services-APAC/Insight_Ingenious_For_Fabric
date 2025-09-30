import os
from pathlib import Path
from typing import List

from jinja2 import Template


def list_templates(directory: str) -> List[dict[str, str]]:
    """
    Recursively iterates through the subfolders of the given directory
    and creates a list of template file paths.

    Args:
        directory (str): The root directory to start searching.

    Returns:
        List[dict[str, str]]: A list of dictionaries containing template metadata.
    """
    templates = list()
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql.jinja"):  # Assuming templates are SQL files
                file_path = os.path.join(root, file)
                folder_name = os.path.basename(root)

                # Read file contents
                with open(file_path, "r", encoding="utf-8") as f:
                    file_contents = f.read()

                templates.append(
                    {
                        "dialect": folder_name,
                        "file_name": file,
                        "file_contents": file_contents,
                        "full_path": file_path,
                    }
                )
    return templates


if __name__ == "__main__":
    script_dir = Path("ingen_fab/python_libs/python/sql_template_factory")
    project_root = Path(".")
    templates_directory = script_dir
    print(f"Scanning directory: {templates_directory}")

    templates_list = list_templates(templates_directory)
    print(f"Found {len(templates_list)} template files")

    # Print each template found
    for template in templates_list:
        print(f"  - {template['dialect']}/{template['file_name']}")

    # Output file should be relative to the project root
    output_file = os.path.join(script_dir, "../sql_templates.py")
    print(f"\nGenerating {output_file}...")

    module_template_path = os.path.join(script_dir, "module_template.py.jinja")
    with open(module_template_path, "r", encoding="utf-8") as template_file:
        template_content = template_file.read()

    with open(output_file, "w", encoding="utf-8") as f:
        # render the final templates using jinja2
        final_content = Template(template_content).render(Templates=repr(templates_list) + "\n")
        f.write(final_content)

    print(f"\nSuccessfully generated {output_file}")
