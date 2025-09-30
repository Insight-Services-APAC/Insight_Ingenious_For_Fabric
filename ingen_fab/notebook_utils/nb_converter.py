import json

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook


class FabricNotebookConverter:
    @staticmethod
    def convert_fabric_py_to_ipynb(input_path: str, output_path: str):
        with open(input_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        nb = new_notebook()
        cells = []
        current_cell_lines = []
        current_cell_type = None

        def flush_cell():
            nonlocal current_cell_lines, current_cell_type
            if not current_cell_lines:
                return
            if current_cell_type == "code":
                cells.append(new_code_cell(source="".join(current_cell_lines)))  # join with no extra newline
            elif current_cell_type == "markdown":
                # Remove `# ` from start of each line
                stripped = [line[2:] if line.startswith("# ") else line for line in current_cell_lines]
                cells.append(new_markdown_cell(source="\n".join(stripped)))
            current_cell_lines = []

        for line in lines:
            line_strip = line.strip()

            if line_strip.startswith("# MARKDOWN"):
                flush_cell()
                current_cell_type = "markdown"
            elif line_strip.startswith("# PARAMETERS CELL") or line_strip.startswith("# CELL"):
                flush_cell()
                current_cell_type = "code"
            elif line_strip.startswith("# META") or line_strip.startswith("# METADATA"):
                continue  # skip meta lines
            elif current_cell_type:
                current_cell_lines.append(line)  # preserve newline structure

        flush_cell()
        nb.cells = cells

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(nb, f, indent=2)

    @staticmethod
    def convert_ipynb_to_fabric_py(input_path: str, output_path: str):
        with open(input_path, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        lines = []
        lines.append("# Fabric notebook source\n")
        lines.append("\n# METADATA ********************\n")
        lines.append("# META {\n")
        lines.append('# META   "kernel_info": {\n')
        lines.append('# META     "name": "synapse_pyspark"\n')
        lines.append("# META   }\n")
        lines.append("# META }\n")

        for cell in nb.cells:
            if cell.cell_type == "markdown":
                lines.append("\n# MARKDOWN ********************\n")
                for line in cell.source.splitlines():
                    lines.append(f"# {line}")
            elif cell.cell_type == "code":
                lines.append("\n# CELL ********************\n")
                lines.append(cell.source)

        lines.append("\n# METADATA ********************\n")
        lines.append("# META {\n")
        lines.append('# META   "language": "python",\n')
        lines.append('# META   "language_group": "synapse_pyspark"\n')
        lines.append("# META }\n")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))


# FabricNotebookConverter.convert_fabric_py_to_ipynb(
#    "./fabric_workspace_items/extract/extract_from_synapse.Notebook/notebook-content.py", "converted_notebook.ipynb"
# )
