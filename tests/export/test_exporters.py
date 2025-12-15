"""Tests for exporters."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from ingen_fab.python_libs.pyspark.export.common.config import (
    ExportConfig,
    ExportSourceConfig,
    FileFormatParams,
)
from ingen_fab.python_libs.pyspark.export.exporters.base_exporter import (
    BaseExporter,
    get_registered_exporters,
)
from ingen_fab.python_libs.pyspark.export.exporters.lakehouse_exporter import (
    LakehouseExporter,
)
from ingen_fab.python_libs.pyspark.export.exporters.warehouse_exporter import (
    WarehouseExporter,
)


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def lakehouse_source_config():
    """Create lakehouse source config."""
    return ExportSourceConfig(
        source_type="lakehouse",
        source_workspace="test-workspace",
        source_datastore="test-lakehouse",
        source_table="test_table",
    )


@pytest.fixture
def warehouse_source_config():
    """Create warehouse source config."""
    return ExportSourceConfig(
        source_type="warehouse",
        source_workspace="test-workspace",
        source_datastore="test-warehouse",
        source_schema="dbo",
        source_table="test_table",
    )


@pytest.fixture
def lakehouse_export_config(lakehouse_source_config):
    """Create lakehouse export config."""
    return ExportConfig(
        export_group_name="test_group",
        export_name="test_export",
        is_active=True,
        execution_group=1,
        source_config=lakehouse_source_config,
        target_workspace="target-workspace",
        target_lakehouse="target-lakehouse",
        target_path="exports/",
        file_format_params=FileFormatParams(file_format="csv"),
    )


@pytest.fixture
def warehouse_export_config(warehouse_source_config):
    """Create warehouse export config."""
    return ExportConfig(
        export_group_name="test_group",
        export_name="test_export",
        is_active=True,
        execution_group=1,
        source_config=warehouse_source_config,
        target_workspace="target-workspace",
        target_lakehouse="target-lakehouse",
        target_path="exports/",
        file_format_params=FileFormatParams(file_format="csv"),
    )


# ============================================================================
# BaseExporter Tests
# ============================================================================


class TestBaseExporterRegistry:
    """Tests for exporter registry."""

    def test_lakehouse_exporter_registered(self):
        """Test LakehouseExporter is registered."""
        registry = get_registered_exporters()
        assert "lakehouse" in registry
        assert registry["lakehouse"] == LakehouseExporter

    def test_warehouse_exporter_registered(self):
        """Test WarehouseExporter is registered."""
        registry = get_registered_exporters()
        assert "warehouse" in registry
        assert registry["warehouse"] == WarehouseExporter

    def test_create_lakehouse_exporter(self, lakehouse_export_config, mock_spark):
        """Test factory creates LakehouseExporter."""
        exporter = BaseExporter.create(lakehouse_export_config, mock_spark)
        assert isinstance(exporter, LakehouseExporter)

    def test_create_warehouse_exporter(self, warehouse_export_config, mock_spark):
        """Test factory creates WarehouseExporter."""
        exporter = BaseExporter.create(warehouse_export_config, mock_spark)
        assert isinstance(exporter, WarehouseExporter)

    def test_create_unknown_type_raises(self, mock_spark):
        """Test factory raises for unknown source type."""
        # Create config with invalid source type (bypass validation)
        source = ExportSourceConfig.__new__(ExportSourceConfig)
        source.source_type = "unknown"
        source.source_workspace = "ws"
        source.source_datastore = "ds"
        source.source_table = "table"
        source.source_schema = None
        source.source_query = None
        source.source_columns = None

        config = ExportConfig.__new__(ExportConfig)
        config.source_config = source

        with pytest.raises(ValueError, match="No exporter registered"):
            BaseExporter.create(config, mock_spark)


class TestGetSelectColumns:
    """Tests for _get_select_columns method."""

    def test_no_columns_returns_star(self, lakehouse_export_config, mock_spark):
        """Test no source_columns returns *."""
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)
        assert exporter._get_select_columns() == "*"

    def test_columns_returns_list(self, lakehouse_export_config, mock_spark):
        """Test source_columns returns comma-separated list."""
        lakehouse_export_config.source_config.source_columns = ["col1", "col2", "col3"]
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)
        assert exporter._get_select_columns() == "col1, col2, col3"

    def test_incremental_column_auto_included(self, lakehouse_export_config, mock_spark):
        """Test incremental_column is auto-added when not in list."""
        lakehouse_export_config.source_config.source_columns = ["col1", "col2"]
        lakehouse_export_config.extract_type = "incremental"
        lakehouse_export_config.incremental_column = "updated_at"

        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)
        result = exporter._get_select_columns()
        assert "updated_at" in result
        assert result == "col1, col2, updated_at"

    def test_incremental_column_not_duplicated(self, lakehouse_export_config, mock_spark):
        """Test incremental_column not duplicated if already in list."""
        lakehouse_export_config.source_config.source_columns = ["col1", "updated_at"]
        lakehouse_export_config.extract_type = "incremental"
        lakehouse_export_config.incremental_column = "updated_at"

        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)
        result = exporter._get_select_columns()
        assert result == "col1, updated_at"
        # Should not appear twice
        assert result.count("updated_at") == 1


# ============================================================================
# LakehouseExporter Tests
# ============================================================================


class TestLakehouseExporterQueryBuilding:
    """Tests for LakehouseExporter query building."""

    def test_build_table_query_simple(self, lakehouse_export_config, mock_spark):
        """Test simple table query without filters."""
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)
        query = exporter._build_table_query(None, None, None)
        assert query == "SELECT * FROM test-lakehouse.dbo.test_table"

    def test_build_table_query_with_schema(self, lakehouse_export_config, mock_spark):
        """Test table query respects custom schema."""
        lakehouse_export_config.source_config.source_schema = "custom"
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)
        query = exporter._build_table_query(None, None, None)
        assert "custom.test_table" in query

    def test_build_table_query_incremental(self, lakehouse_export_config, mock_spark):
        """Test incremental query adds WHERE clause."""
        lakehouse_export_config.extract_type = "incremental"
        lakehouse_export_config.incremental_column = "updated_at"
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)

        query = exporter._build_table_query("2025-01-01 00:00:00", None, None)
        assert "WHERE updated_at > '2025-01-01 00:00:00'" in query

    def test_build_table_query_period(self, lakehouse_export_config, mock_spark):
        """Test period query adds BETWEEN clause."""
        lakehouse_export_config.extract_type = "period"
        lakehouse_export_config.period_filter_column = "order_date"
        lakehouse_export_config.period_date_query = "SELECT start, end FROM dates"
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)

        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)
        query = exporter._build_table_query(None, start, end)
        assert "WHERE order_date BETWEEN" in query

    def test_build_table_query_with_columns(self, lakehouse_export_config, mock_spark):
        """Test query uses specified columns."""
        lakehouse_export_config.source_config.source_columns = ["id", "name", "value"]
        exporter = LakehouseExporter(lakehouse_export_config, mock_spark)

        query = exporter._build_table_query(None, None, None)
        assert query.startswith("SELECT id, name, value FROM")


# ============================================================================
# WarehouseExporter Tests
# ============================================================================


class TestWarehouseExporterQueryBuilding:
    """Tests for WarehouseExporter query building."""

    def test_build_table_query_simple(self, warehouse_export_config, mock_spark):
        """Test simple table query."""
        exporter = WarehouseExporter(warehouse_export_config, mock_spark)
        query = exporter._build_table_query(None, None, None)
        assert query == "SELECT * FROM test-warehouse.dbo.test_table"

    def test_build_table_query_custom_schema(self, warehouse_export_config, mock_spark):
        """Test table query with custom schema."""
        warehouse_export_config.source_config.source_schema = "sales"
        exporter = WarehouseExporter(warehouse_export_config, mock_spark)
        query = exporter._build_table_query(None, None, None)
        assert "test-warehouse.sales.test_table" in query

    def test_build_table_query_incremental(self, warehouse_export_config, mock_spark):
        """Test incremental query adds WHERE clause."""
        warehouse_export_config.extract_type = "incremental"
        warehouse_export_config.incremental_column = "modified_date"
        exporter = WarehouseExporter(warehouse_export_config, mock_spark)

        query = exporter._build_table_query("2025-01-01", None, None)
        assert "WHERE modified_date > '2025-01-01'" in query

    def test_build_table_query_period(self, warehouse_export_config, mock_spark):
        """Test period query adds BETWEEN clause."""
        warehouse_export_config.extract_type = "period"
        warehouse_export_config.period_filter_column = "transaction_date"
        warehouse_export_config.period_date_query = "SELECT start, end FROM dates"
        exporter = WarehouseExporter(warehouse_export_config, mock_spark)

        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)
        query = exporter._build_table_query(None, start, end)
        assert "WHERE transaction_date BETWEEN" in query
