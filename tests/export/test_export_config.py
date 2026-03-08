"""Tests for export configuration validation."""

import pytest

from ingen_fab.python_libs.pyspark.export.common.config import (
    ExportConfig,
    ExportSourceConfig,
    FileFormatParams,
)


class TestFileFormatParams:
    """Tests for FileFormatParams validation."""

    def test_valid_csv_format(self):
        """Test valid CSV format creation."""
        params = FileFormatParams(file_format="csv")
        assert params.file_format == "csv"
        assert params.compression is None

    def test_valid_parquet_format(self):
        """Test valid Parquet format creation."""
        params = FileFormatParams(file_format="parquet")
        assert params.file_format == "parquet"

    def test_invalid_file_format(self):
        """Test that invalid file format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported file_format"):
            FileFormatParams(file_format="invalid")

    def test_valid_compression(self):
        """Test valid compression setting."""
        params = FileFormatParams(file_format="csv", compression="gzip")
        assert params.compression == "gzip"

    def test_invalid_compression(self):
        """Test that invalid compression raises ValueError."""
        with pytest.raises(ValueError, match="Invalid compression"):
            FileFormatParams(file_format="csv", compression="invalid")

    def test_compression_level_without_compression(self):
        """Test that compression_level without compression raises ValueError."""
        with pytest.raises(ValueError, match="compression_level requires compression"):
            FileFormatParams(file_format="csv", compression_level=5)

    def test_valid_compression_level(self):
        """Test valid compression level."""
        params = FileFormatParams(file_format="csv", compression="gzip", compression_level=5)
        assert params.compression_level == 5

    def test_invalid_compression_level_too_high(self):
        """Test that compression level out of range raises ValueError."""
        with pytest.raises(ValueError, match="invalid for 'gzip'"):
            FileFormatParams(file_format="csv", compression="gzip", compression_level=15)

    def test_compression_level_unsupported_type(self):
        """Test that compression level for unsupported type raises ValueError."""
        with pytest.raises(ValueError, match="not supported for compression type"):
            FileFormatParams(file_format="csv", compression="snappy", compression_level=5)

    def test_to_spark_options(self):
        """Test Spark options conversion."""
        params = FileFormatParams(
            file_format="csv",
            file_format_options={"header": "true", "sep": ","},
            compression="gzip",
        )
        options = params.to_spark_options()
        assert options["header"] == "true"
        assert options["sep"] == ","
        assert options["compression"] == "gzip"


class TestExportSourceConfig:
    """Tests for ExportSourceConfig validation."""

    def test_valid_lakehouse_source(self):
        """Test valid lakehouse source config."""
        config = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        assert config.source_type == "lakehouse"

    def test_valid_warehouse_source(self):
        """Test valid warehouse source config."""
        config = ExportSourceConfig(
            source_type="warehouse",
            source_workspace="ws",
            source_datastore="wh",
            source_schema="dbo",
            source_table="table",
        )
        assert config.source_type == "warehouse"

    def test_invalid_source_type(self):
        """Test that invalid source type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid source_type"):
            ExportSourceConfig(
                source_type="invalid",
                source_workspace="ws",
                source_datastore="ds",
                source_table="table",
            )

    def test_warehouse_requires_schema(self):
        """Test that warehouse source requires schema."""
        with pytest.raises(ValueError, match="source_schema is required"):
            ExportSourceConfig(
                source_type="warehouse",
                source_workspace="ws",
                source_datastore="wh",
                source_table="table",
            )

    def test_table_or_query_required(self):
        """Test that either source_table or source_query is required."""
        with pytest.raises(ValueError, match="Must specify either"):
            ExportSourceConfig(
                source_type="lakehouse",
                source_workspace="ws",
                source_datastore="lh",
            )

    def test_table_and_query_mutually_exclusive(self):
        """Test that source_table and source_query are mutually exclusive."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            ExportSourceConfig(
                source_type="lakehouse",
                source_workspace="ws",
                source_datastore="lh",
                source_table="table",
                source_query="SELECT * FROM table",
            )

    def test_query_and_columns_mutually_exclusive(self):
        """Test that source_query and source_columns are mutually exclusive."""
        with pytest.raises(ValueError, match="Cannot specify both source_query and source_columns"):
            ExportSourceConfig(
                source_type="lakehouse",
                source_workspace="ws",
                source_datastore="lh",
                source_query="SELECT * FROM table",
                source_columns=["col1", "col2"],
            )


class TestExportConfig:
    """Tests for ExportConfig validation."""

    def test_valid_full_config(self):
        """Test valid full export config."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        config = ExportConfig(
            export_group_name="group",
            export_name="export",
            is_active=True,
            execution_group=1,
            source_config=source,
            target_workspace="target_ws",
            target_lakehouse="target_lh",
            target_path="exports/",
            file_format_params=FileFormatParams(file_format="csv"),
        )
        assert config.export_name == "export"

    def test_incremental_requires_column(self):
        """Test that incremental extract type requires incremental_column."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="incremental_column is required"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=1,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
                extract_type="incremental",
            )

    def test_period_requires_query(self):
        """Test that period extract type requires period_date_query."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="period_date_query is required"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=1,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
                extract_type="period",
            )

    def test_period_with_table_requires_filter_column(self):
        """Test that period exports with source_table require period_filter_column."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="period_filter_column"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=1,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
                extract_type="period",
                period_date_query="SELECT start, end FROM dates",
            )

    def test_invalid_extract_type(self):
        """Test that invalid extract_type raises ValueError."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="Invalid extract_type"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=1,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
                extract_type="invalid",
            )

    def test_invalid_execution_group(self):
        """Test that execution_group <= 0 raises ValueError."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="execution_group must be > 0"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=0,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
            )

    def test_invalid_filename_placeholder(self):
        """Test that invalid filename placeholder raises ValueError."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="Invalid placeholder"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=1,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
                target_filename_pattern="file_{invalid_placeholder}.csv",
            )

    def test_valid_filename_pattern(self):
        """Test valid filename pattern with placeholders."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        config = ExportConfig(
            export_group_name="group",
            export_name="export",
            is_active=True,
            execution_group=1,
            source_config=source,
            target_workspace="target_ws",
            target_lakehouse="target_lh",
            target_path="exports/",
            file_format_params=FileFormatParams(file_format="csv"),
            target_filename_pattern="{export_name}_{run_date:%Y%m%d}.csv",
        )
        assert config.target_filename_pattern == "{export_name}_{run_date:%Y%m%d}.csv"

    def test_compressed_filename_requires_compression(self):
        """Test that compressed_filename_pattern requires compression."""
        source = ExportSourceConfig(
            source_type="lakehouse",
            source_workspace="ws",
            source_datastore="lh",
            source_table="table",
        )
        with pytest.raises(ValueError, match="compressed_filename_pattern requires compression"):
            ExportConfig(
                export_group_name="group",
                export_name="export",
                is_active=True,
                execution_group=1,
                source_config=source,
                target_workspace="target_ws",
                target_lakehouse="target_lh",
                target_path="exports/",
                file_format_params=FileFormatParams(file_format="csv"),
                compressed_filename_pattern="file.zip",
            )

    def test_from_row(self):
        """Test creating config from dictionary."""
        row = {
            "export_group_name": "group",
            "export_name": "export",
            "is_active": True,
            "execution_group": 1,
            "source_type": "lakehouse",
            "source_workspace": "ws",
            "source_datastore": "lh",
            "source_table": "table",
            "target_workspace": "target_ws",
            "target_lakehouse": "target_lh",
            "target_path": "exports/",
            "file_format": "csv",
        }
        config = ExportConfig.from_row(row)
        assert config.export_name == "export"
        assert config.source_config.source_table == "table"
