# DDL script templates

This folder contains Jinja templates and helpers for building DDL notebooks for Fabric.
The main entry point is `notebook_generator.py` which compiles notebooks from a set of configuration folders.

## NotebookGenerator

`NotebookGenerator` scans the `ddl_scripts` directory of a workspace repository.
Each entity (lakehouse or warehouse) has its own folder containing numbered `.py` or `.sql` files.
The generator sorts these files by their numeric prefix and renders them into a `notebook-content.py` file using templates from `_templates`.

It can produce notebooks directly in `fabric_workspace_items/ddl_scripts` or to a local output directory.
For each entity an orchestrator notebook is also generated and a master orchestrator can run all entities in sequence.

## Template layout

The `_templates` directory provides base Jinja templates used during generation.
There are subfolders for `lakehouse` and `warehouse`, each containing:

- `notebook_content.py.jinja` – wraps the rendered cells to create a notebook.
- `nb_cell_python.py.jinja` and `nb_cell_sql.py.jinja` – templates for Python and SQL cells.
- `orchestrator_notebook.py.jinja` – orchestrator that runs notebooks for a single entity.
- `orchestrator_notebook_all_lakehouses.py.jinja` – orchestrator that runs all entities.

You can customise these templates to fit your own coding standards or add additional cells.
