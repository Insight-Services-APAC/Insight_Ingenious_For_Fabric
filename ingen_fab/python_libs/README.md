# Python libraries

The `python_libs` package provides helper modules used by the CLI and example notebooks.
It is split into two subpackages:

- **`python`** – modules that work with standard CPython or the Fabric runtime.
- **`pyspark`** – PySpark implementations intended for use inside Spark notebooks.

## `python` modules

- `config_utils` – Load workspace configuration from a table and return a `FabricConfig` dataclass.
- `ddl_utils` – Execute DDL scripts exactly once and log their execution.
- `lakehouse_utils` – Minimal helpers for interacting with Lakehouse storage.
- `promotion_utils` – Wrapper around `fabric_cicd` for publishing items between workspaces.
- `sql_templates` – Jinja-based SQL statement templates used by other modules.
- `warehouse_utils` – Connect to Fabric or SQL Server warehouses and run queries.

## `pyspark` modules

These modules mirror the Python versions but operate on Spark `DataFrame` objects and Delta tables.
They implement similar APIs for configuration management, DDL execution and Lakehouse operations.
