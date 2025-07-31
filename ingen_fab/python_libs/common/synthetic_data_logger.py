"""
Enhanced Synthetic Data Generation Logging

This module provides comprehensive logging capabilities for synthetic data generation
with date column tracking, data correlation information, and detailed generation metrics.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col, min as spark_min, max as spark_max, count, isnan, isnull
    from pyspark.sql.types import DateType, TimestampType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


@dataclass
class DateColumnInfo:
    """Information about a date column in generated data."""
    column_name: str
    data_type: str
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    null_count: int = 0
    total_count: int = 0
    is_primary_date: bool = False
    date_format: str = "YYYY-MM-DD"
    
    @property
    def completeness_percentage(self) -> float:
        """Calculate completeness percentage for the date column."""
        if self.total_count == 0:
            return 0.0
        return ((self.total_count - self.null_count) / self.total_count) * 100


@dataclass
class TableGenerationMetrics:
    """Comprehensive metrics for table generation."""
    table_name: str
    generation_date: str
    generation_timestamp: str
    file_path: str
    rows_generated: int
    generation_duration_seconds: float
    table_type: str  # "snapshot" or "incremental"
    
    # Date column information
    date_columns: List[DateColumnInfo]
    primary_date_column: Optional[str] = None
    
    # Data quality metrics
    total_size_mb: float = 0.0
    compression_ratio: float = 0.0
    
    # Business logic metrics
    seasonal_multiplier_applied: float = 1.0
    growth_rate_applied: float = 0.0
    base_rows_configured: int = 0
    
    # File and storage information
    file_format: str = "parquet"
    partition_columns: List[str] = None
    
    def __post_init__(self):
        if self.partition_columns is None:
            self.partition_columns = []
    
    @property
    def generation_rate_rows_per_second(self) -> float:
        """Calculate generation rate in rows per second."""
        if self.generation_duration_seconds <= 0:
            return 0.0
        return self.rows_generated / self.generation_duration_seconds
    
    @property
    def generation_rate_mb_per_second(self) -> float:
        """Calculate generation rate in MB per second."""
        if self.generation_duration_seconds <= 0:
            return 0.0
        return self.total_size_mb / self.generation_duration_seconds
    
    def get_date_correlation_info(self) -> str:
        """Generate human-readable correlation information."""
        if not self.date_columns:
            return f"File {Path(self.file_path).name} contains {self.rows_generated:,} records with no date columns identified"
        
        primary_col = next((col for col in self.date_columns if col.is_primary_date), self.date_columns[0])
        
        correlation_parts = [
            f"File {Path(self.file_path).name}",
            f"contains {self.rows_generated:,} records"
        ]
        
        if primary_col.min_date and primary_col.max_date:
            if primary_col.min_date == primary_col.max_date:
                correlation_parts.append(f"with {primary_col.column_name} = {primary_col.min_date}")
            else:
                correlation_parts.append(f"with {primary_col.column_name} from {primary_col.min_date} to {primary_col.max_date}")
        
        return " ".join(correlation_parts)


@dataclass
class DatasetGenerationSummary:
    """Summary of entire dataset generation."""
    dataset_id: str
    generation_date: str
    generation_timestamp: str
    total_tables: int
    total_rows: int
    total_size_mb: float
    total_duration_seconds: float
    
    table_metrics: List[TableGenerationMetrics]
    
    # Configuration information
    runtime_config_applied: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.runtime_config_applied is None:
            self.runtime_config_applied = {}
    
    @property
    def overall_generation_rate(self) -> float:
        """Calculate overall generation rate in rows per second."""
        if self.total_duration_seconds <= 0:
            return 0.0
        return self.total_rows / self.total_duration_seconds
    
    def get_table_by_name(self, table_name: str) -> Optional[TableGenerationMetrics]:
        """Get metrics for a specific table."""
        return next((metrics for metrics in self.table_metrics if metrics.table_name == table_name), None)


class SyntheticDataLogger:
    """Enhanced logger for synthetic data generation with detailed tracking."""
    
    def __init__(self, 
                 logger_name: str = __name__,
                 log_level: int = logging.INFO,
                 enable_console_output: bool = True,
                 enable_file_logging: bool = False,
                 log_file_path: str = None):
        
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        
        # Clear existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Console handler
        if enable_console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
        
        # File handler
        if enable_file_logging and log_file_path:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(log_level)
            
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
        
        # Storage for persistent logging
        self._generation_logs = []
        self._persistent_storage_enabled = False
        self._persistent_storage_callback = None
    
    def enable_persistent_storage(self, storage_callback=None):
        """Enable persistent storage of generation logs."""
        self._persistent_storage_enabled = True
        self._persistent_storage_callback = storage_callback
    
    def analyze_dataframe_date_columns(self, 
                                     df: Union[DataFrame, 'pd.DataFrame'], 
                                     table_name: str,
                                     expected_date_columns: List[str] = None) -> List[DateColumnInfo]:
        """Analyze DataFrame to identify and characterize date columns."""
        date_columns = []
        
        if PYSPARK_AVAILABLE and hasattr(df, 'schema'):
            return self._analyze_pyspark_dataframe(df, table_name, expected_date_columns)
        elif PANDAS_AVAILABLE and hasattr(df, 'dtypes'):
            return self._analyze_pandas_dataframe(df, table_name, expected_date_columns)
        else:
            self.logger.warning(f"Unable to analyze date columns for {table_name} - unsupported DataFrame type")
            return []
    
    def _analyze_pyspark_dataframe(self, 
                                 df: DataFrame, 
                                 table_name: str,
                                 expected_date_columns: List[str] = None) -> List[DateColumnInfo]:
        """Analyze PySpark DataFrame for date columns."""
        date_columns = []
        
        # Identify date/timestamp columns from schema
        date_column_names = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (DateType, TimestampType)):
                date_column_names.append(field.name)
        
        # Add expected date columns that might be strings but contain dates
        if expected_date_columns:
            for col_name in expected_date_columns:
                if col_name not in date_column_names and col_name in df.columns:
                    date_column_names.append(col_name)
        
        # Analyze each date column
        for col_name in date_column_names:
            try:
                # Get basic statistics
                stats = df.agg(
                    spark_min(col(col_name)).alias("min_date"),
                    spark_max(col(col_name)).alias("max_date"),
                    count(col(col_name)).alias("non_null_count")
                ).collect()[0]
                
                total_count = df.count()
                non_null_count = stats["non_null_count"]
                null_count = total_count - non_null_count
                
                # Convert dates to strings for storage
                min_date_str = str(stats["min_date"]) if stats["min_date"] else None
                max_date_str = str(stats["max_date"]) if stats["max_date"] else None
                
                # Determine data type
                field_type = next((f.dataType for f in df.schema.fields if f.name == col_name), None)
                data_type = str(field_type) if field_type else "unknown"
                
                date_col_info = DateColumnInfo(
                    column_name=col_name,
                    data_type=data_type,
                    min_date=min_date_str,
                    max_date=max_date_str,
                    null_count=null_count,
                    total_count=total_count,
                    is_primary_date=(col_name in (expected_date_columns or []) and 
                                   expected_date_columns.index(col_name) == 0)
                )
                
                date_columns.append(date_col_info)
                
            except Exception as e:
                self.logger.warning(f"Error analyzing date column {col_name} in {table_name}: {e}")
        
        return date_columns
    
    def _analyze_pandas_dataframe(self, 
                                df: 'pd.DataFrame', 
                                table_name: str,
                                expected_date_columns: List[str] = None) -> List[DateColumnInfo]:
        """Analyze Pandas DataFrame for date columns."""
        date_columns = []
        
        # Identify date columns
        date_column_names = []
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_datetime64_any_dtype(dtype):
                date_column_names.append(col_name)
        
        # Add expected date columns
        if expected_date_columns:
            for col_name in expected_date_columns:
                if col_name not in date_column_names and col_name in df.columns:
                    date_column_names.append(col_name)
        
        # Analyze each date column
        for col_name in date_column_names:
            try:
                col_data = df[col_name]
                
                # Convert to datetime if needed
                if not pd.api.types.is_datetime64_any_dtype(col_data):
                    try:
                        col_data = pd.to_datetime(col_data, errors='coerce')
                    except:
                        continue
                
                min_date = col_data.min()
                max_date = col_data.max()
                null_count = col_data.isnull().sum()
                total_count = len(col_data)
                
                date_col_info = DateColumnInfo(
                    column_name=col_name,
                    data_type=str(df[col_name].dtype),
                    min_date=min_date.strftime("%Y-%m-%d") if pd.notna(min_date) else None,
                    max_date=max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else None,
                    null_count=int(null_count),
                    total_count=total_count,
                    is_primary_date=(col_name in (expected_date_columns or []) and 
                                   expected_date_columns.index(col_name) == 0)
                )
                
                date_columns.append(date_col_info)
                
            except Exception as e:
                self.logger.warning(f"Error analyzing date column {col_name} in {table_name}: {e}")
        
        return date_columns
    
    def log_table_generation_start(self, 
                                 table_name: str, 
                                 table_type: str,
                                 generation_date: date,
                                 target_rows: int,
                                 config_applied: Dict[str, Any] = None):
        """Log the start of table generation."""
        self.logger.info(f"🚀 Starting generation for {table_name}")
        self.logger.info(f"   📅 Generation date: {generation_date}")
        self.logger.info(f"   📊 Table type: {table_type}")
        self.logger.info(f"   🎯 Target rows: {target_rows:,}")
        
        if config_applied:
            self.logger.debug(f"   ⚙️ Configuration: {json.dumps(config_applied, indent=2)}")
    
    def log_table_generation_complete(self, 
                                    table_metrics: TableGenerationMetrics) -> str:
        """
        Log the completion of table generation with detailed metrics.
        
        Returns:
            Correlation information string
        """
        # Log basic completion info
        self.logger.info(f"✅ Completed generation for {table_metrics.table_name}")
        self.logger.info(f"   📈 Rows generated: {table_metrics.rows_generated:,}")
        self.logger.info(f"   ⏱️ Duration: {table_metrics.generation_duration_seconds:.2f} seconds")
        self.logger.info(f"   🚀 Rate: {table_metrics.generation_rate_rows_per_second:.0f} rows/second")
        self.logger.info(f"   📁 File: {table_metrics.file_path}")
        
        # Log date column information
        if table_metrics.date_columns:
            self.logger.info(f"   📅 Date columns identified: {len(table_metrics.date_columns)}")
            
            for date_col in table_metrics.date_columns:
                primary_indicator = " (PRIMARY)" if date_col.is_primary_date else ""
                completeness = f"{date_col.completeness_percentage:.1f}%"
                
                self.logger.info(f"     • {date_col.column_name}{primary_indicator}: {completeness} complete")
                
                if date_col.min_date and date_col.max_date:
                    if date_col.min_date == date_col.max_date:
                        self.logger.info(f"       Date value: {date_col.min_date}")
                    else:
                        self.logger.info(f"       Date range: {date_col.min_date} to {date_col.max_date}")
        else:
            self.logger.info(f"   📅 No date columns identified")
        
        # Log correlation information
        correlation_info = table_metrics.get_date_correlation_info()
        self.logger.info(f"   🔗 {correlation_info}")
        
        # Store for persistent logging
        if self._persistent_storage_enabled:
            self._generation_logs.append(table_metrics)
            if self._persistent_storage_callback:
                try:
                    self._persistent_storage_callback(table_metrics)
                except Exception as e:
                    self.logger.warning(f"Failed to persist log entry: {e}")
        
        return correlation_info
    
    def log_dataset_generation_summary(self, summary: DatasetGenerationSummary):
        """Log a complete dataset generation summary."""
        self.logger.info(f"🎉 Dataset Generation Complete: {summary.dataset_id}")
        self.logger.info(f"   📅 Generation date: {summary.generation_date}")
        self.logger.info(f"   📊 Total tables: {summary.total_tables}")
        self.logger.info(f"   📈 Total rows: {summary.total_rows:,}")
        self.logger.info(f"   💾 Total size: {summary.total_size_mb:.2f} MB")
        self.logger.info(f"   ⏱️ Total duration: {summary.total_duration_seconds:.2f} seconds")
        self.logger.info(f"   🚀 Overall rate: {summary.overall_generation_rate:.0f} rows/second")
        
        # Log per-table breakdown
        self.logger.info(f"   📋 Table breakdown:")
        for table_metrics in summary.table_metrics:
            primary_date_col = next((col for col in table_metrics.date_columns if col.is_primary_date), None)
            
            if primary_date_col and primary_date_col.min_date:
                date_info = f" ({primary_date_col.min_date})" if primary_date_col.min_date == primary_date_col.max_date else f" ({primary_date_col.min_date} to {primary_date_col.max_date})"
            else:
                date_info = ""
            
            self.logger.info(f"     • {table_metrics.table_name}: {table_metrics.rows_generated:,} rows{date_info}")
    
    def create_table_metrics(self,
                           table_name: str,
                           generation_date: date,
                           file_path: str,
                           rows_generated: int,
                           duration_seconds: float,
                           table_type: str,
                           dataframe: Union[DataFrame, 'pd.DataFrame'] = None,
                           expected_date_columns: List[str] = None,
                           **kwargs) -> TableGenerationMetrics:
        """Create comprehensive table generation metrics."""
        
        # Analyze date columns if DataFrame provided
        date_columns = []
        if dataframe is not None:
            date_columns = self.analyze_dataframe_date_columns(
                dataframe, table_name, expected_date_columns
            )
        
        # Determine primary date column
        primary_date_column = None
        if date_columns:
            primary_col = next((col for col in date_columns if col.is_primary_date), None)
            if primary_col:
                primary_date_column = primary_col.column_name
            elif expected_date_columns:
                primary_date_column = expected_date_columns[0]
        
        metrics = TableGenerationMetrics(
            table_name=table_name,
            generation_date=generation_date.isoformat(),
            generation_timestamp=datetime.now().isoformat(),
            file_path=file_path,
            rows_generated=rows_generated,
            generation_duration_seconds=duration_seconds,
            table_type=table_type,
            date_columns=date_columns,
            primary_date_column=primary_date_column,
            **kwargs
        )
        
        return metrics
    
    def export_generation_logs(self, 
                             output_path: str,
                             format: Literal["json", "csv"] = "json"):
        """Export all generation logs to file."""
        try:
            if format == "json":
                with open(output_path, 'w') as f:
                    json.dump([asdict(log) for log in self._generation_logs], f, indent=2, default=str)
            elif format == "csv" and PANDAS_AVAILABLE:
                # Flatten the data for CSV export
                flattened_data = []
                for log in self._generation_logs:
                    base_data = {
                        "table_name": log.table_name,
                        "generation_date": log.generation_date,
                        "rows_generated": log.rows_generated,
                        "duration_seconds": log.generation_duration_seconds,
                        "file_path": log.file_path,
                        "table_type": log.table_type
                    }
                    
                    # Add date column info
                    for i, date_col in enumerate(log.date_columns):
                        base_data[f"date_col_{i}_name"] = date_col.column_name
                        base_data[f"date_col_{i}_min"] = date_col.min_date
                        base_data[f"date_col_{i}_max"] = date_col.max_date
                    
                    flattened_data.append(base_data)
                
                df = pd.DataFrame(flattened_data)
                df.to_csv(output_path, index=False)
            
            self.logger.info(f"Exported {len(self._generation_logs)} log entries to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export logs: {e}")
    
    def get_generation_statistics(self) -> Dict[str, Any]:
        """Get aggregated statistics from all generation logs."""
        if not self._generation_logs:
            return {}
        
        total_rows = sum(log.rows_generated for log in self._generation_logs)
        total_duration = sum(log.generation_duration_seconds for log in self._generation_logs)
        total_tables = len(self._generation_logs)
        
        table_types = {}
        for log in self._generation_logs:
            table_types[log.table_type] = table_types.get(log.table_type, 0) + 1
        
        return {
            "total_tables_generated": total_tables,
            "total_rows_generated": total_rows,
            "total_duration_seconds": total_duration,
            "average_rows_per_table": total_rows / total_tables if total_tables > 0 else 0,
            "overall_generation_rate": total_rows / total_duration if total_duration > 0 else 0,
            "table_types_breakdown": table_types,
            "generation_dates": list(set(log.generation_date for log in self._generation_logs))
        }


class DataCorrelationTracker:
    """Tracks correlations between generated data and file names/dates."""
    
    def __init__(self, logger: SyntheticDataLogger = None):
        self.logger = logger or SyntheticDataLogger()
        self._correlations = {}
    
    def track_correlation(self, 
                        file_path: str,
                        table_name: str,
                        generation_date: date,
                        data_date_range: Tuple[Optional[date], Optional[date]],
                        row_count: int):
        """Track correlation between file and data dates."""
        correlation_key = f"{table_name}_{generation_date.isoformat()}"
        
        self._correlations[correlation_key] = {
            "file_path": file_path,
            "table_name": table_name,
            "generation_date": generation_date.isoformat(),
            "data_min_date": data_date_range[0].isoformat() if data_date_range[0] else None,
            "data_max_date": data_date_range[1].isoformat() if data_date_range[1] else None,
            "row_count": row_count,
            "correlation_timestamp": datetime.now().isoformat()
        }
    
    def get_correlations_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Get all correlations for a specific date."""
        target_str = target_date.isoformat()
        return [
            corr for corr in self._correlations.values()
            if corr["generation_date"] == target_str
        ]
    
    def get_correlations_for_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all correlations for a specific table."""
        return [
            corr for corr in self._correlations.values()
            if corr["table_name"] == table_name
        ]
    
    def export_correlations(self, output_path: str):
        """Export correlation tracking data."""
        try:
            with open(output_path, 'w') as f:
                json.dump(list(self._correlations.values()), f, indent=2)
            
            self.logger.logger.info(f"Exported {len(self._correlations)} correlations to {output_path}")
            
        except Exception as e:
            self.logger.logger.error(f"Failed to export correlations: {e}")