"""Unit tests for FileSystemExtractor.

Uses pytest fixtures for shared test objects and @patch for mocking
utility functions (standard Python testing pattern).

Testing approach:
- FilesystemConnection objects are injected with mock fs clients (proper DI)
- Utility functions (glob, move_file, etc.) are patched since they're external dependencies
"""

import pytest
from unittest.mock import Mock, patch

from ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor import (
    FileSystemExtractor,
)
from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    ResourceConfig,
    SourceConfig,
    FileFormatParams,
    FileSystemExtractionParams,
)
from ingen_fab.python_libs.common.fsspec_utils import FilesystemConnection
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_extraction_logger():
    """Mock extraction logger."""
    logger = Mock()
    logger.check_file_already_extracted.return_value = False
    return logger


@pytest.fixture
def mock_source_conn():
    """Mock source filesystem connection."""
    return FilesystemConnection(
        fs=Mock(),
        base_url="abfss://source_ws@onelake.dfs.fabric.microsoft.com/source_lh.Lakehouse"
    )


@pytest.fixture
def mock_dest_conn():
    """Mock destination filesystem connection."""
    return FilesystemConnection(
        fs=Mock(),
        base_url="abfss://dest_ws@onelake.dfs.fabric.microsoft.com/dest_lh.Lakehouse"
    )


@pytest.fixture
def sample_config():
    """Sample resource configuration for testing."""
    return ResourceConfig(
        resource_name="test_resource",
        source_name="test_source",
        source_config=SourceConfig(
            source_type="filesystem",
            source_connection_params={
                "workspace_name": "test_workspace",
                "lakehouse_name": "test_lakehouse",
            },
        ),
        source_extraction_params=FileSystemExtractionParams(
            inbound_path="Files/inbound/test/",
            discovery_pattern="*.csv",
        ),
        extract_path="Files/raw/test/",
        extract_file_format_params=FileFormatParams(
            file_format="csv",
            format_options={"file_delimiter": ","}
        ),
        extract_storage_workspace="test_storage_ws",
        extract_storage_lakehouse="test_storage_lh",
        extract_error_path="Files/error/test/",
        target_write_mode="append",  # Avoid merge validation for extraction tests
    )


# ============================================================================
# TESTS
# ============================================================================


class TestFileSystemExtractor:
    """Unit tests for FileSystemExtractor."""

    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.glob"
    )
    def test_extract_yields_no_data_when_no_files(
        self, mock_glob, sample_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Test extract() yields result when no files found."""
        mock_glob.return_value = []

        extractor = FileSystemExtractor(
            resource_config=sample_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        assert len(results) == 1
        assert results[0].file_count == 0
        mock_glob.assert_called_once()

    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.cleanup_empty_directories"
    )
    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.move_file"
    )
    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.glob"
    )
    def test_extract_moves_files_to_raw(
        self,
        mock_glob,
        mock_move,
        mock_cleanup,
        sample_config,
        mock_extraction_logger,
        mock_source_conn,
        mock_dest_conn,
    ):
        """Test extract() moves files and yields success."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1000,
            )
        ]
        mock_move.return_value = True

        extractor = FileSystemExtractor(
            resource_config=sample_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        assert len(results) == 1
        assert results[0].status.value == "success"
        assert results[0].file_count == 1
        mock_move.assert_called_once()

    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.cleanup_empty_directories"
    )
    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.move_file"
    )
    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.glob"
    )
    def test_extract_includes_batch_id_in_path(
        self,
        mock_glob,
        mock_move,
        mock_cleanup,
        sample_config,
        mock_extraction_logger,
        mock_source_conn,
        mock_dest_conn,
    ):
        """Test extract() includes batch_id folder in destination path."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1000,
            )
        ]
        mock_move.return_value = True

        extractor = FileSystemExtractor(
            resource_config=sample_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        assert len(results) == 1
        result = results[0]

        # Verify batch_id appears in extracted file path
        assert len(result.extract_file_paths) == 1
        dest_path = result.extract_file_paths[0]
        assert "batch_id=" in dest_path

        # Verify extraction_id matches batch_id in path
        extraction_id = result.extraction_id
        assert f"batch_id={extraction_id}" in dest_path

    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.glob"
    )
    def test_extract_skips_duplicate_files(
        self, mock_glob, sample_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Test extract() skips files already extracted."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1000,
            )
        ]
        mock_extraction_logger.check_file_already_extracted.return_value = True

        extractor = FileSystemExtractor(
            resource_config=sample_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        assert len(results) == 1
        assert results[0].status.value == "warning"
        assert "Duplicate" in results[0].error_message

    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.move_file"
    )
    @patch(
        "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor.glob"
    )
    def test_extract_handles_move_failure(
        self, mock_glob, mock_move, sample_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Test extract() handles move operation failure."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1000,
            )
        ]
        mock_move.return_value = False  # Move fails

        extractor = FileSystemExtractor(
            resource_config=sample_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        assert len(results) == 1
        assert results[0].status.value == "error"
