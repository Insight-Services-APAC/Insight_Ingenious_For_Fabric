"""Tests for ExportOrchestrator."""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from ingen_fab.python_libs.pyspark.export.common.config import (
    ExportConfig,
    ExportSourceConfig,
    FileFormatParams,
)
from ingen_fab.python_libs.pyspark.export.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.export.export_orchestrator import ExportOrchestrator


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def mock_export_logger():
    """Create mock ExportLogger."""
    logger = MagicMock()
    logger.get_failed_export_keys.return_value = set()
    logger.get_watermark.return_value = None
    return logger


@pytest.fixture
def orchestrator(mock_spark, mock_export_logger):
    """Create ExportOrchestrator with mocks."""
    return ExportOrchestrator(
        spark=mock_spark,
        export_logger=mock_export_logger,
        max_concurrency=4,
    )


@pytest.fixture
def basic_export_config():
    """Create basic export config."""
    source = ExportSourceConfig(
        source_type="lakehouse",
        source_workspace="source-ws",
        source_datastore="source-lh",
        source_table="test_table",
    )
    return ExportConfig(
        export_group_name="test_group",
        export_name="test_export",
        is_active=True,
        execution_group=1,
        source_config=source,
        target_workspace="target-ws",
        target_lakehouse="target-lh",
        target_path="exports/",
        file_format_params=FileFormatParams(file_format="csv"),
    )


# ============================================================================
# Unit Tests (no Fabric dependencies)
# ============================================================================


class TestGetUniqueTargetLakehouses:
    """Tests for _get_unique_target_lakehouses."""

    def test_single_config(self, orchestrator, basic_export_config):
        """Test single config returns single target."""
        targets = orchestrator._get_unique_target_lakehouses([basic_export_config])
        assert targets == {("target-ws", "target-lh")}

    def test_multiple_same_target(self, orchestrator, basic_export_config):
        """Test multiple configs with same target returns single entry."""
        config2 = ExportConfig(
            export_group_name="test_group",
            export_name="test_export_2",
            is_active=True,
            execution_group=1,
            source_config=basic_export_config.source_config,
            target_workspace="target-ws",  # Same
            target_lakehouse="target-lh",  # Same
            target_path="exports2/",
            file_format_params=FileFormatParams(file_format="csv"),
        )
        targets = orchestrator._get_unique_target_lakehouses([basic_export_config, config2])
        assert len(targets) == 1

    def test_multiple_different_targets(self, orchestrator, basic_export_config):
        """Test multiple configs with different targets."""
        config2 = ExportConfig(
            export_group_name="test_group",
            export_name="test_export_2",
            is_active=True,
            execution_group=1,
            source_config=basic_export_config.source_config,
            target_workspace="other-ws",
            target_lakehouse="other-lh",
            target_path="exports2/",
            file_format_params=FileFormatParams(file_format="csv"),
        )
        targets = orchestrator._get_unique_target_lakehouses([basic_export_config, config2])
        assert len(targets) == 2
        assert ("target-ws", "target-lh") in targets
        assert ("other-ws", "other-lh") in targets


class TestToDatetime:
    """Tests for _to_datetime conversion."""

    def test_datetime_passthrough(self, orchestrator):
        """Test datetime passes through unchanged."""
        dt = datetime(2025, 12, 12, 14, 30, 0)
        result = orchestrator._to_datetime(dt, "test")
        assert result == dt

    def test_date_converted_to_datetime(self, orchestrator):
        """Test date is converted to datetime at midnight."""
        d = date(2025, 12, 12)
        result = orchestrator._to_datetime(d, "test")
        assert isinstance(result, datetime)
        assert result.date() == d
        assert result.hour == 0
        assert result.minute == 0

    def test_invalid_type_raises(self, orchestrator):
        """Test invalid type raises ValueError."""
        with pytest.raises(ValueError, match="must return date/datetime"):
            orchestrator._to_datetime("2025-01-01", "start_date")

        with pytest.raises(ValueError, match="must return date/datetime"):
            orchestrator._to_datetime(12345, "start_date")


class TestGetOutputDataframe:
    """Tests for _get_output_dataframe."""

    def test_no_columns_returns_original(self, orchestrator, basic_export_config, mock_dataframe):
        """Test no source_columns returns original DataFrame."""
        result = orchestrator._get_output_dataframe(mock_dataframe, basic_export_config)
        # Should not call select
        mock_dataframe.select.assert_not_called()

    def test_columns_returns_selected(self, orchestrator, basic_export_config, mock_dataframe):
        """Test source_columns causes select."""
        basic_export_config.source_config.source_columns = ["col1", "col2"]
        orchestrator._get_output_dataframe(mock_dataframe, basic_export_config)
        mock_dataframe.select.assert_called_once_with("col1", "col2")


class TestProcessExportsEmpty:
    """Tests for process_exports with empty configs."""

    def test_empty_configs_returns_success(self, orchestrator):
        """Test empty config list returns success with zero counts."""
        result = orchestrator.process_exports([])
        assert result["success"] is True
        assert result["total_exports"] == 0
        assert result["successful"] == 0
        assert result["failed"] == 0


class TestRetryMode:
    """Tests for retry mode in process_exports."""

    def test_retry_filters_to_failed(self, orchestrator, mock_export_logger, basic_export_config):
        """Test retry mode filters to only failed exports."""
        # Set up mock to return specific failed keys
        mock_export_logger.get_failed_export_keys.return_value = {
            ("test_group", "failed_export")
        }

        # Create config that won't match failed keys
        result = orchestrator.process_exports(
            configs=[basic_export_config],
            is_retry=True,
        )

        # Should have filtered out the config (no match)
        assert result["total_exports"] == 0


# ============================================================================
# Integration Tests (mocking Fabric at boundaries)
# ============================================================================


class TestProcessSingleExport:
    """Tests for process_single_export with mocked Fabric."""

    @patch("ingen_fab.python_libs.pyspark.export.export_orchestrator.BaseExporter")
    def test_failed_export_error_handling(
        self,
        mock_exporter_class,
        orchestrator,
        basic_export_config,
    ):
        """Test error handling in export flow."""
        # Set up exporter to raise
        mock_exporter = MagicMock()
        mock_exporter.read_source.side_effect = Exception("Connection failed")
        mock_exporter_class.create.return_value = mock_exporter

        result = orchestrator.process_single_export(
            config=basic_export_config,
            execution_id="test-exec-id",
            run_date=datetime(2025, 12, 12),
            mount_path="/mnt/test",
        )

        assert result.status == ExecutionStatus.ERROR
        assert "Connection failed" in result.error_message

    @patch("ingen_fab.python_libs.pyspark.export.export_orchestrator.BaseExporter")
    def test_incremental_export_uses_watermark(
        self,
        mock_exporter_class,
        orchestrator,
        mock_export_logger,
        basic_export_config,
        mock_dataframe,
    ):
        """Test incremental export fetches and uses watermark."""
        # Configure for incremental
        basic_export_config.extract_type = "incremental"
        basic_export_config.incremental_column = "updated_at"

        # Set up watermark
        mock_export_logger.get_watermark.return_value = "2025-01-01 00:00:00"

        # Set up exporter
        mock_exporter = MagicMock()
        mock_exporter.read_source.return_value = mock_dataframe
        mock_exporter_class.create.return_value = mock_exporter

        with patch(
            "ingen_fab.python_libs.pyspark.export.export_orchestrator.ExportFileWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer_class.return_value = mock_writer

            mock_write_result = MagicMock()
            mock_write_result.success = True
            mock_write_result.rows_written = 50
            mock_write_result.bytes_written = 2500
            mock_write_result.file_paths = ["/path/to/file.csv"]
            mock_write_result.trigger_file_path = None
            mock_writer.write.return_value = mock_write_result

            result = orchestrator.process_single_export(
                config=basic_export_config,
                execution_id="test-exec-id",
                run_date=datetime(2025, 12, 12),
                mount_path="/mnt/test",
            )

        # Verify watermark was fetched
        mock_export_logger.get_watermark.assert_called_once_with(
            "test_group", "test_export"
        )

        # Verify exporter received watermark
        mock_exporter.read_source.assert_called_once()
        call_kwargs = mock_exporter.read_source.call_args[1]
        assert call_kwargs["watermark_value"] == "2025-01-01 00:00:00"

        # Verify result includes watermark
        assert result.watermark_value == "2025-01-01 00:00:00"
