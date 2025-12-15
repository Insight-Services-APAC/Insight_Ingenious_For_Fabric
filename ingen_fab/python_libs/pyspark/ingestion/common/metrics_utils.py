"""Utilities for working with ProcessingMetrics in the ingestion framework."""

from typing import List

from ingen_fab.python_libs.pyspark.ingestion.common.results import ProcessingMetrics


class ProcessingMetricsUtils:
    """Utilities for working with processing metrics"""

    @staticmethod
    def calculate_performance_metrics(
        metrics: ProcessingMetrics, write_mode: str = "append"
    ) -> ProcessingMetrics:
        """Calculate derived performance metrics"""
        # Calculate total duration if not set
        if metrics.total_duration_ms == 0:
            metrics.total_duration_ms = (
                metrics.read_duration_ms + metrics.write_duration_ms
            )

        # Set row count reconciliation status based on write mode
        if metrics.source_row_count > 0 and metrics.target_row_count_after >= 0:
            if write_mode.lower() == "overwrite":
                # For overwrite mode, source rows should equal final target rows
                if metrics.source_row_count == metrics.target_row_count_after:
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            elif write_mode.lower() == "append":
                # For append mode, source rows should equal the difference
                if metrics.source_row_count == (
                    metrics.target_row_count_after - metrics.target_row_count_before
                ):
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            elif write_mode.lower() == "merge":
                # For merge mode, check that all source rows were either inserted or updated
                if metrics.source_row_count == (
                    metrics.records_inserted + metrics.records_updated
                ):
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            else:
                # Unknown write mode
                metrics.row_count_reconciliation_status = "not_verified"
        else:
            metrics.row_count_reconciliation_status = "not_verified"

        return metrics

    @staticmethod
    def merge_metrics(
        metrics_list: List[ProcessingMetrics], write_mode: str = "append"
    ) -> ProcessingMetrics:
        """Merge multiple metrics into a single aggregated metric"""
        if not metrics_list:
            return ProcessingMetrics()

        merged = ProcessingMetrics()

        # Sum up numerical values
        merged.read_duration_ms = sum(m.read_duration_ms for m in metrics_list)
        merged.write_duration_ms = sum(m.write_duration_ms for m in metrics_list)
        merged.total_duration_ms = sum(m.total_duration_ms for m in metrics_list)
        merged.records_processed = sum(m.records_processed for m in metrics_list)
        merged.records_inserted = sum(m.records_inserted for m in metrics_list)
        merged.records_updated = sum(m.records_updated for m in metrics_list)
        merged.records_deleted = sum(m.records_deleted for m in metrics_list)
        merged.source_row_count = sum(m.source_row_count for m in metrics_list)
        merged.target_row_count_before = sum(
            m.target_row_count_before for m in metrics_list
        )
        merged.target_row_count_after = sum(
            m.target_row_count_after for m in metrics_list
        )

        # Preserve completed_at timestamp (use the latest one)
        completed_timestamps = [m.completed_at for m in metrics_list if m.completed_at]
        if completed_timestamps:
            merged.completed_at = max(completed_timestamps)

        # Calculate performance metrics
        return ProcessingMetricsUtils.calculate_performance_metrics(merged, write_mode)
