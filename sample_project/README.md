# Sample Fabric Workspace

This folder demonstrates the layout expected by the CLI. It includes
configuration files for DDL generation along with example notebooks and
helper scripts.

```
sample_project/
├── db_projects              # Example SQL projects for warehouses
├── ddl_scripts              # Jinja templates and generation config
├── diagrams                 # Architecture diagrams (draw.io format)
├── fabric_workspace_items   # Generated notebooks and pipelines
├── notebook_partials        # Reusable notebook code blocks
└── python_libs              # Helper utilities used by the samples
```

Use these files as a reference when building your own Fabric workspace
repository. The project is intentionally small but covers the key
folders that the CLI expects when compiling notebooks.
