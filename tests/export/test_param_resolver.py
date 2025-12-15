"""Tests for parameter resolution."""

from datetime import datetime

import pytest

from ingen_fab.python_libs.pyspark.export.common.param_resolver import (
    Params,
    resolve_params,
)


class TestParams:
    """Tests for Params dataclass."""

    def test_default_values(self):
        """Test default Params has all None values."""
        params = Params()
        assert params.run_date is None
        assert params.export_name is None
        assert params.part is None

    def test_with_values(self):
        """Test Params with provided values."""
        dt = datetime(2025, 12, 12, 14, 30, 0)
        params = Params(run_date=dt, export_name="test", part=1)
        assert params.run_date == dt
        assert params.export_name == "test"
        assert params.part == 1


class TestResolveParams:
    """Tests for resolve_params function."""

    def test_no_placeholders(self):
        """Test pattern with no placeholders."""
        result = resolve_params("simple_filename.csv", Params())
        assert result == "simple_filename.csv"

    def test_export_name_placeholder(self):
        """Test {export_name} placeholder."""
        params = Params(export_name="my_export")
        result = resolve_params("{export_name}.csv", params)
        assert result == "my_export.csv"

    def test_run_id_placeholder_truncated(self):
        """Test {run_id} is truncated to 8 characters."""
        params = Params(run_id="12345678-9abc-def0-1234-567890abcdef")
        result = resolve_params("file_{run_id}.csv", params)
        assert result == "file_12345678.csv"

    def test_part_placeholder(self):
        """Test {part} placeholder."""
        params = Params(part=5)
        result = resolve_params("file_part{part}.csv", params)
        assert result == "file_part5.csv"

    def test_watermark_placeholder(self):
        """Test {watermark} placeholder."""
        params = Params(watermark="2025-01-01T00:00:00")
        result = resolve_params("file_{watermark}.csv", params)
        assert result == "file_2025-01-01T00:00:00.csv"

    def test_run_date_default_format(self):
        """Test {run_date} without format spec uses ISO datetime."""
        params = Params(run_date=datetime(2025, 12, 12, 14, 30, 22))
        result = resolve_params("{run_date}", params)
        assert result == "2025-12-12 14:30:22"

    def test_run_date_with_format(self):
        """Test {run_date:format} uses custom format."""
        params = Params(run_date=datetime(2025, 12, 12, 14, 30, 22))
        result = resolve_params("{run_date:%Y%m%d}", params)
        assert result == "20251212"

    def test_run_date_date_only_format(self):
        """Test {run_date:%Y-%m-%d} for date only."""
        params = Params(run_date=datetime(2025, 12, 12, 14, 30, 22))
        result = resolve_params("{run_date:%Y-%m-%d}", params)
        assert result == "2025-12-12"

    def test_process_date_defaults_to_now(self):
        """Test {process_date} defaults to current time when not provided."""
        params = Params()  # No process_date
        result = resolve_params("{process_date:%Y}", params)
        # Should resolve to current year
        assert result == str(datetime.now().year)

    def test_period_dates(self):
        """Test period_start_date and period_end_date placeholders."""
        params = Params(
            period_start_date=datetime(2025, 1, 1),
            period_end_date=datetime(2025, 1, 31),
        )
        result = resolve_params(
            "data_{period_start_date:%Y%m%d}_to_{period_end_date:%Y%m%d}.csv",
            params,
        )
        assert result == "data_20250101_to_20250131.csv"

    def test_multiple_placeholders(self):
        """Test pattern with multiple different placeholders."""
        params = Params(
            export_name="sales",
            run_date=datetime(2025, 12, 12),
            part=3,
        )
        result = resolve_params("{export_name}_{run_date:%Y%m%d}_part{part}.csv", params)
        assert result == "sales_20251212_part3.csv"

    def test_missing_placeholder_unchanged(self):
        """Test unresolved placeholders remain unchanged."""
        params = Params()  # No export_name set
        result = resolve_params("{export_name}.csv", params)
        assert result == "{export_name}.csv"

    def test_unknown_placeholder_unchanged(self):
        """Test unknown placeholders remain unchanged."""
        params = Params(export_name="test")
        result = resolve_params("{unknown_field}.csv", params)
        assert result == "{unknown_field}.csv"

    def test_complex_filename_pattern(self):
        """Test realistic complex filename pattern."""
        params = Params(
            export_name="customer_data",
            run_date=datetime(2025, 12, 12, 9, 0, 0),
            run_id="abc12345-6789-0def-ghij-klmnopqrstuv",
        )
        result = resolve_params(
            "{export_name}_{run_date:%Y%m%d}_{run_id}.csv",
            params,
        )
        assert result == "customer_data_20251212_abc12345.csv"
