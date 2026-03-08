"""Pytest fixtures for export framework tests.

Sets up mock Fabric dependencies so tests can run locally.
"""

import sys
import types
from unittest.mock import MagicMock

import pytest


# ============================================================================
# Mock Fabric Libraries (before any imports)
# ============================================================================

def _setup_fabric_mocks():
    """Set up mock modules for Fabric-specific libraries."""

    # Mock com.microsoft.spark.fabric
    com = types.ModuleType("com")
    microsoft = types.ModuleType("microsoft")
    spark = types.ModuleType("spark")
    fabric = types.ModuleType("fabric")

    class MockConstants:
        WorkspaceId = "WorkspaceId"
        DatabaseName = "DatabaseName"

    fabric.Constants = MockConstants
    spark.fabric = fabric
    microsoft.spark = spark
    com.microsoft = microsoft

    sys.modules.setdefault("com", com)
    sys.modules.setdefault("com.microsoft", microsoft)
    sys.modules.setdefault("com.microsoft.spark", spark)
    sys.modules.setdefault("com.microsoft.spark.fabric", fabric)
    sys.modules.setdefault("com.microsoft.spark.fabric.Constants", MockConstants)

    # Mock sempy.fabric
    sempy = types.ModuleType("sempy")
    sempy_fabric = types.ModuleType("fabric")
    sempy_fabric.resolve_workspace_id = MagicMock(return_value="mock-workspace-id")
    sempy.fabric = sempy_fabric

    sys.modules.setdefault("sempy", sempy)
    sys.modules.setdefault("sempy.fabric", sempy_fabric)

    # Ensure notebookutils has fs methods for mount/unmount
    if "notebookutils" in sys.modules:
        notebookutils = sys.modules["notebookutils"]
        if not hasattr(notebookutils, "fs"):
            fs = types.SimpleNamespace(
                mount=MagicMock(),
                unmount=MagicMock(),
                getMountPath=MagicMock(return_value="/mnt/test"),
                mounts=MagicMock(return_value=[]),
            )
            notebookutils.fs = fs


# Run setup before any test imports
_setup_fabric_mocks()


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession for testing."""
    spark = MagicMock()

    # Mock read chain for synapsesql
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.synapsesql.return_value = MagicMock()  # Returns mock DataFrame
    spark.read = mock_reader

    return spark


@pytest.fixture
def mock_dataframe():
    """Create a mock PySpark DataFrame."""
    df = MagicMock()
    df.count.return_value = 100
    df.select.return_value = df
    df.filter.return_value = df
    df.coalesce.return_value = df
    df.withColumn.return_value = df
    df.drop.return_value = df
    df.write = MagicMock()
    return df


@pytest.fixture
def sample_export_config():
    """Create a sample ExportConfig for testing."""
    from ingen_fab.python_libs.pyspark.export.common.config import (
        ExportConfig,
        ExportSourceConfig,
        FileFormatParams,
    )

    source_config = ExportSourceConfig(
        source_type="lakehouse",
        source_workspace="test-workspace",
        source_datastore="test-lakehouse",
        source_table="test_table",
    )

    return ExportConfig(
        export_group_name="test_group",
        export_name="test_export",
        is_active=True,
        execution_group=1,
        source_config=source_config,
        target_workspace="target-workspace",
        target_lakehouse="target-lakehouse",
        target_path="exports/test/",
        file_format_params=FileFormatParams(file_format="csv"),
    )


@pytest.fixture
def sample_incremental_config(sample_export_config):
    """Create a sample incremental ExportConfig."""
    sample_export_config.extract_type = "incremental"
    sample_export_config.incremental_column = "updated_at"
    return sample_export_config
