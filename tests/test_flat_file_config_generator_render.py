"""The flat-file config generator compiled for a warehouse target runs in a Python
kernel and uses the CPython lakehouse_utils; the constructor call it emits must match
that class's signature (it used to pass a `spark` argument the class does not take)."""

import ast
import inspect

import pytest

from ingen_fab.packages.flat_file_ingestion.flat_file_ingestion import (
    FlatFileIngestionCompiler,
)
from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils


@pytest.fixture
def warehouse_generator_source(tmp_path) -> str:
    compiler = FlatFileIngestionCompiler(fabric_workspace_repo_dir=str(tmp_path))
    out = compiler.compile_config_generator_notebook(target_datastore="warehouse")
    out = out if out.is_dir() else out.parent
    content = next(out.rglob("notebook-content.py"))
    return content.read_text(encoding="utf-8")


def _lakehouse_utils_calls(source: str):
    """Keyword names of every `lakehouse_utils(...)` call in the rendered notebook.

    The notebook is a Fabric notebook-content.py (cells separated by comment
    markers), which is still one parseable Python module.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "lakehouse_utils"
        ):
            yield [kw.arg for kw in node.keywords]


def test_warehouse_generator_constructs_the_cpython_class_correctly(
    warehouse_generator_source,
):
    calls = list(_lakehouse_utils_calls(warehouse_generator_source))
    assert calls, "the notebook should instantiate lakehouse_utils"

    accepted = set(inspect.signature(lakehouse_utils.__init__).parameters) - {"self"}
    for keywords in calls:
        assert set(keywords) <= accepted, (
            f"lakehouse_utils called with {keywords}; the CPython class accepts {sorted(accepted)}"
        )
    assert "spark" not in {kw for call in calls for kw in call}


def test_warehouse_generator_loads_the_cpython_library(warehouse_generator_source):
    assert (
        "ingen_fab/python_libs/python/lakehouse_utils.py" in warehouse_generator_source
    )
