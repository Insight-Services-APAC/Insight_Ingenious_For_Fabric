# Python libraries

The `python_libs` folder contains python and pyspark libraries that will be injected into Fabric notebooks to provide helper utilities and re-usable code logic..

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

## Pre-Deployment Development and Testing
When developing these libraries, you can run tests using `pytest`. There is a separate folder for tests located at `ingen_fab/python_libs_tests`.

Test Examples:

```bash
 pytest ./ingen_fab/python_libs_tests/pyspark/ddl_utils/ddl_utils_pytest.py -v
 pytest ./ingen_fab/python_libs_tests/pyspark/lakehouse_utils/test_lakehouse_utils_pytest.py -v

 ```
