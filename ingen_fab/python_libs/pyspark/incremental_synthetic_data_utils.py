"""
Incremental Synthetic Data Utils - Enhanced version for time-based generation

This module extends the existing synthetic data utilities to support incremental
data generation with state management, date-based partitioning, and different
table types (snapshot vs incremental).

Enhanced with:
- Flexible configuration system
- Advanced file path patterns
- Comprehensive logging and data tracking
- Runtime parameter support
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union, Literal

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        col, lit, when, expr, concat, date_add, datediff,
        rand, randn, round as spark_round, abs as spark_abs
    )
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

try:
    import pandas as pd
    from faker import Faker
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Import enhanced utilities
try:
    from ingen_fab.python_libs.common.synthetic_data_config import DatasetConfiguration, TableGenerationConfig, ConfigurationManager
    from ingen_fab.python_libs.common.file_path_utils import DateBasedFilePathGenerator, FilePathManager
    from ingen_fab.python_libs.common.synthetic_data_logger import SyntheticDataLogger, TableGenerationMetrics, DatasetGenerationSummary
    ENHANCED_UTILS_AVAILABLE = True
except ImportError:
    ENHANCED_UTILS_AVAILABLE = False


class IncrementalSyntheticDataGenerator:
    """
    Enhanced synthetic data generator that supports incremental data generation
    with date-based partitioning and state management.
    """
    
    def __init__(
        self,
        lakehouse_utils_instance=None,
        seed: Optional[int] = None,
        state_table_name: str = "synthetic_data_state",
        enhanced_logging: bool = True,
        file_path_generator: DateBasedFilePathGenerator = None
    ):
        self.lakehouse_utils = lakehouse_utils_instance
        self.seed = seed
        self.state_table_name = state_table_name
        
        # Initialize enhanced utilities if available
        if ENHANCED_UTILS_AVAILABLE and enhanced_logging:
            self.enhanced_logger = SyntheticDataLogger(
                logger_name=f"{__name__}.{self.__class__.__name__}",
                enable_console_output=True
            )
            self.logger = self.enhanced_logger.logger  # Use the internal logger for standard logging
            self.enhanced_logging = True
        else:
            self.enhanced_logger = None
            self.logger = logging.getLogger(__name__)
            self.enhanced_logging = False
        
        # Initialize file path generation
        self.file_path_generator = file_path_generator or (
            DateBasedFilePathGenerator() if ENHANCED_UTILS_AVAILABLE else None
        )
        self.file_path_manager = FilePathManager(self.file_path_generator) if ENHANCED_UTILS_AVAILABLE else None
        
        # Initialize state management
        self._state_cache = {}
        
        # Initialize base generator for reusing existing methods
        if lakehouse_utils_instance:
            from .synthetic_data_utils import PySparkSyntheticDataGenerator
            self.base_generator = PySparkSyntheticDataGenerator(
                lakehouse_utils_instance=lakehouse_utils_instance,
                seed=seed
            )
        else:
            self.base_generator = None
        
        if seed:
            if PYSPARK_AVAILABLE and lakehouse_utils_instance:
                self.lakehouse_utils.set_spark_config("spark.sql.shuffle.partitions", "200")
    
    def generate_dataset_from_config(
        self,
        dataset_config: Union[DatasetConfiguration, Dict[str, Any]],
        generation_date: Union[str, date],
        runtime_overrides: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate dataset using enhanced configuration system.
        
        Args:
            dataset_config: Enhanced dataset configuration or legacy dict
            generation_date: Date to generate data for
            runtime_overrides: Runtime parameter overrides
            
        Returns:
            Enhanced generation results with metrics
        """
        if not ENHANCED_UTILS_AVAILABLE:
            # Fall back to legacy method
            return self.generate_incremental_dataset(
                dataset_config if isinstance(dataset_config, dict) else dataset_config.to_dict(),
                generation_date
            )
        
        # Handle configuration
        if isinstance(dataset_config, dict):
            config = DatasetConfiguration.from_dict(dataset_config)
        else:
            config = dataset_config
        
        # Apply runtime overrides
        if runtime_overrides:
            config.apply_runtime_overrides(runtime_overrides)
        
        # Parse generation date
        if isinstance(generation_date, str):
            generation_date = datetime.strptime(generation_date, "%Y-%m-%d").date()
        
        # Get output settings
        output_settings = config.output_settings
        path_format = output_settings.get("path_format", "nested")
        output_mode = output_settings.get("output_mode", "parquet")
        
        # Start overall timing
        overall_start_time = time.time()
        
        # Initialize results
        results = {
            "dataset_id": config.dataset_id,
            "generation_date": generation_date.isoformat(),
            "generated_tables": {},
            "file_paths": {},
            "total_rows": 0,
            "table_metrics": [],
            "dataset_summary": None
        }
        
        # Load or initialize state
        if runtime_overrides and runtime_overrides.get("ignore_state", False):
            # When ignoring state, start with empty state to force regeneration
            state = {"tables": {}, "last_generation": None}
        else:
            state = self._load_dataset_state(config.dataset_id, generation_date)
        
        # Generate each table
        for table_name, table_config in config.table_configs.items():
            start_time = time.time()
            
            # Enhanced logging for table start
            if self.enhanced_logging:
                target_rows = table_config.calculate_target_rows(
                    generation_date,
                    current_size=state.get("tables", {}).get(table_name, {}).get("current_size"),
                    seasonal_multipliers=config.incremental_config.get("seasonal_multipliers", {})
                )
                
                self.enhanced_logger.log_table_generation_start(
                    table_name=table_name,
                    table_type=table_config.table_type,
                    generation_date=generation_date,
                    target_rows=target_rows,
                    config_applied={
                        "base_rows": table_config.base_rows if table_config.table_type == "snapshot" else table_config.base_rows_per_day,
                        "seasonal_enabled": table_config.seasonal_enabled,
                        "growth_enabled": table_config.growth_enabled
                    }
                )
            
            # Check if we need to generate this table
            if not self._should_generate_table(table_name, table_config.__dict__, generation_date, state):
                if self.enhanced_logging:
                    self.logger.info(f"⏩ Skipping {table_name} - not due for generation on {generation_date}")
                continue
            
            # Generate the table
            if table_config.table_type == "snapshot":
                df_result = self._generate_snapshot_table_enhanced(
                    table_name, table_config, generation_date, state, config.incremental_config
                )
            else:  # incremental
                df_result = self._generate_incremental_table_enhanced(
                    table_name, table_config, generation_date, state, config.incremental_config
                )
            
            if df_result is not None:
                # Generate file path using enhanced system
                file_path = self._generate_enhanced_file_path(
                    table_name, config.dataset_id, generation_date, 
                    path_format, output_mode, output_settings
                )
                
                # Save the data
                self._save_table_data_enhanced(
                    df_result, table_name, file_path, output_mode
                )
                
                # Calculate metrics
                duration = time.time() - start_time
                row_count = df_result.count() if hasattr(df_result, 'count') else len(df_result)
                
                # Create enhanced metrics
                if self.enhanced_logging:
                    table_metrics = self.enhanced_logger.create_table_metrics(
                        table_name=table_name,
                        generation_date=generation_date,
                        file_path=file_path,
                        rows_generated=row_count,
                        duration_seconds=duration,
                        table_type=table_config.table_type,
                        dataframe=df_result,
                        expected_date_columns=table_config.date_columns,
                        seasonal_multiplier_applied=1.0,  # Would be calculated based on actual multipliers applied
                        base_rows_configured=table_config.base_rows if table_config.table_type == "snapshot" else table_config.base_rows_per_day
                    )
                    
                    # Log completion with enhanced details
                    correlation_info = self.enhanced_logger.log_table_generation_complete(table_metrics)
                    results["table_metrics"].append(table_metrics)
                
                # Update standard results
                results["generated_tables"][table_name] = {
                    "rows": row_count,
                    "type": table_config.table_type,
                    "file_path": file_path,
                    "duration_seconds": duration
                }
                results["file_paths"][table_name] = file_path
                results["total_rows"] += row_count
                
                # Update state
                self._update_table_state(table_name, table_config.__dict__, generation_date, state, row_count)
        
        # Save updated state
        self._save_dataset_state(config.dataset_id, generation_date, state)
        
        # Create dataset summary
        if self.enhanced_logging and results["table_metrics"]:
            overall_duration = time.time() - overall_start_time
            total_size_mb = sum(getattr(tm, 'total_size_mb', 0.0) for tm in results["table_metrics"])
            
            dataset_summary = DatasetGenerationSummary(
                dataset_id=config.dataset_id,
                generation_date=generation_date.isoformat(),
                generation_timestamp=datetime.now().isoformat(),
                total_tables=len(results["generated_tables"]),
                total_rows=results["total_rows"],
                total_size_mb=total_size_mb,
                total_duration_seconds=overall_duration,
                table_metrics=results["table_metrics"],
                runtime_config_applied=runtime_overrides or {}
            )
            
            self.enhanced_logger.log_dataset_generation_summary(dataset_summary)
            results["dataset_summary"] = dataset_summary
        
        return results
    
    def generate_incremental_dataset(
        self,
        dataset_config: Dict[str, Any],
        generation_date: Union[str, date],
        path_format: Literal["nested", "flat"] = "nested",
        output_mode: str = "parquet",
        ignore_state: bool = False
    ) -> Dict[str, Any]:
        """
        Generate incremental synthetic data for a specific date.
        
        Args:
            dataset_config: Dataset configuration with table configs
            generation_date: Date to generate data for
            path_format: Path format ("nested" for /YYYY/MM/DD/ or "flat" for YYYYMMDD_)
            output_mode: Output mode ("parquet" or "table")
            ignore_state: Whether to ignore existing state and regenerate all files
        
        Returns:
            Dictionary with generation results
        """
        if isinstance(generation_date, str):
            generation_date = datetime.strptime(generation_date, "%Y-%m-%d").date()
        
        dataset_id = dataset_config["dataset_id"]
        table_configs = dataset_config.get("table_configs", {})
        incremental_config = dataset_config.get("incremental_config", {})
        
        results = {
            "dataset_id": dataset_id,
            "generation_date": generation_date.isoformat(),
            "generated_tables": {},
            "file_paths": {},
            "total_rows": 0
        }
        
        # Load or initialize state
        if ignore_state:
            # When ignoring state, start with empty state to force regeneration
            state = {"tables": {}, "last_generation": None}
        else:
            state = self._load_dataset_state(dataset_id, generation_date)
        
        # Generate each table based on its configuration
        for table_name, table_config in table_configs.items():
            self.logger.info(f"Generating {table_name} for {generation_date}")
            
            table_type = table_config.get("type", "incremental")
            frequency = table_config.get("frequency", "daily")
            
            # Check if we need to generate this table for this date
            if not self._should_generate_table(table_name, table_config, generation_date, state):
                self.logger.info(f"Skipping {table_name} - not due for generation on {generation_date}")
                continue
            
            if table_type == "snapshot":
                df_result = self._generate_snapshot_table(
                    table_name, table_config, generation_date, state, incremental_config
                )
            else:  # incremental
                df_result = self._generate_incremental_table(
                    table_name, table_config, generation_date, state, incremental_config
                )
            
            if df_result is not None:
                # Save the data
                file_path = self._save_table_data(
                    df_result, table_name, dataset_id, generation_date, 
                    path_format, output_mode
                )
                
                # Update results
                results["generated_tables"][table_name] = {
                    "rows": df_result.count() if PYSPARK_AVAILABLE else len(df_result),
                    "type": table_type,
                    "file_path": file_path
                }
                results["file_paths"][table_name] = file_path
                results["total_rows"] += results["generated_tables"][table_name]["rows"]
                
                # Update state
                actual_size = results["generated_tables"][table_name]["rows"]
                self._update_table_state(table_name, table_config, generation_date, state, actual_size)
        
        # Save updated state
        self._save_dataset_state(dataset_id, generation_date, state)
        
        return results
    
    def generate_incremental_dataset_series(
        self,
        dataset_config: Dict[str, Any],
        start_date: Union[str, date],
        end_date: Union[str, date],
        path_format: Literal["nested", "flat"] = "nested",
        output_mode: str = "parquet",
        batch_size: int = 30,
        ignore_state: bool = False
    ) -> Dict[str, Any]:
        """
        Generate incremental synthetic data for a series of dates.
        
        Args:
            dataset_config: Dataset configuration
            start_date: Start date for generation
            end_date: End date for generation
            path_format: Path format
            output_mode: Output mode
            batch_size: Number of days to process at once
            ignore_state: Whether to ignore existing state and regenerate all files
        
        Returns:
            Aggregated results from all dates
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        total_results = {
            "dataset_id": dataset_config["dataset_id"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_days": (end_date - start_date).days + 1,
            "generated_tables": {},
            "total_rows": 0,
            "daily_results": []
        }
        
        current_date = start_date
        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=batch_size - 1), end_date)
            
            self.logger.info(f"Processing batch: {current_date} to {batch_end}")
            
            # Process each day in the batch
            batch_date = current_date
            while batch_date <= batch_end:
                daily_result = self.generate_incremental_dataset(
                    dataset_config, batch_date, path_format, output_mode, ignore_state
                )
                
                total_results["daily_results"].append(daily_result)
                total_results["total_rows"] += daily_result["total_rows"]
                
                # Aggregate table statistics
                for table_name, table_result in daily_result["generated_tables"].items():
                    if table_name not in total_results["generated_tables"]:
                        total_results["generated_tables"][table_name] = {
                            "total_rows": 0,
                            "generation_count": 0
                        }
                    
                    total_results["generated_tables"][table_name]["total_rows"] += table_result["rows"]
                    total_results["generated_tables"][table_name]["generation_count"] += 1
                
                batch_date += timedelta(days=1)
            
            current_date = batch_end + timedelta(days=1)
        
        return total_results
    
    def _should_generate_table(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        generation_date: date,
        state: Dict[str, Any]
    ) -> bool:
        """Determine if a table should be generated for the given date."""
        frequency = table_config.get("frequency", "daily")
        table_type = table_config.get("type", "incremental")
        
        # Incremental tables are always generated
        if table_type == "incremental":
            return True
        
        # Check snapshot frequency
        last_generated = state.get("tables", {}).get(table_name, {}).get("last_generated")
        if last_generated is None:
            return True  # First time generation
        
        last_date = datetime.strptime(last_generated, "%Y-%m-%d").date()
        days_since_last = (generation_date - last_date).days
        
        if frequency == "daily":
            return days_since_last >= 1
        elif frequency == "weekly":
            return days_since_last >= 7
        elif frequency == "monthly":
            return days_since_last >= 30
        elif frequency == "quarterly":
            return days_since_last >= 90
        elif frequency == "once":
            return False  # Only generate once
        
        return True
    
    def _generate_snapshot_table(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        generation_date: date,
        state: Dict[str, Any],
        incremental_config: Dict[str, Any]
    ):
        """Generate a snapshot table with growth and churn applied."""
        base_rows = table_config.get("base_rows", 10000)
        growth_enabled = table_config.get("growth_enabled", False)
        churn_enabled = table_config.get("churn_enabled", False)
        
        # Calculate current size based on growth/churn
        current_size = self._calculate_current_snapshot_size(
            table_name, table_config, generation_date, state, base_rows
        )
        
        # Generate the table data
        if table_name in ["customers", "dim_customer"]:
            return self._generate_customers_snapshot(current_size, generation_date)
        elif table_name in ["products", "dim_product"]:
            return self._generate_products_snapshot(current_size, generation_date)
        elif table_name in ["stores", "dim_store"]:
            return self._generate_stores_snapshot(current_size, generation_date)
        elif table_name == "dim_date":
            return self._generate_date_dimension(generation_date)
        else:
            return self._generate_generic_snapshot(table_name, current_size, generation_date)
    
    def _generate_incremental_table(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        generation_date: date,
        state: Dict[str, Any],
        incremental_config: Dict[str, Any]
    ):
        """Generate incremental table data for a specific date."""
        base_rows_per_day = table_config.get("base_rows_per_day", 1000)
        seasonal_enabled = table_config.get("seasonal_multipliers_enabled", False)
        
        # Apply seasonal multipliers
        actual_rows = self._apply_seasonal_multipliers(
            base_rows_per_day, generation_date, seasonal_enabled, incremental_config
        )
        
        # Generate the data
        if table_name in ["orders"]:
            return self._generate_orders_incremental(actual_rows, generation_date, state)
        elif table_name in ["order_items"]:
            return self._generate_order_items_incremental(actual_rows, generation_date, state)
        elif table_name in ["fact_sales"]:
            return self._generate_sales_fact_incremental(actual_rows, generation_date, state)
        elif table_name in ["fact_inventory"]:
            return self._generate_inventory_fact_incremental(actual_rows, generation_date, state)
        else:
            return self._generate_generic_incremental(table_name, actual_rows, generation_date)
    
    def _calculate_current_snapshot_size(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        generation_date: date,
        state: Dict[str, Any],
        base_rows: int
    ) -> int:
        """Calculate current snapshot table size considering growth and churn."""
        growth_enabled = table_config.get("growth_enabled", False)
        churn_enabled = table_config.get("churn_enabled", False)
        
        if not growth_enabled and not churn_enabled:
            return base_rows
        
        # Get the last size from state
        table_state = state.get("tables", {}).get(table_name, {})
        last_size = table_state.get("current_size", base_rows)
        last_generated = table_state.get("last_generated")
        
        if last_generated is None:
            return base_rows
        
        last_date = datetime.strptime(last_generated, "%Y-%m-%d").date()
        days_elapsed = (generation_date - last_date).days
        
        if days_elapsed <= 0:
            return last_size
        
        # Apply growth and churn
        current_size = last_size
        frequency = table_config.get("frequency", "daily")
        
        if frequency == "daily":
            daily_growth_rate = table_config.get("daily_growth_rate", 0.001)
            daily_churn_rate = table_config.get("daily_churn_rate", 0.0005)
            
            for _ in range(days_elapsed):
                if growth_enabled:
                    growth = int(current_size * daily_growth_rate)
                    current_size += growth
                
                if churn_enabled:
                    churn = int(current_size * daily_churn_rate)
                    current_size = max(current_size - churn, base_rows // 2)
        
        elif frequency == "weekly" and days_elapsed >= 7:
            weeks_elapsed = days_elapsed // 7
            weekly_growth_rate = table_config.get("weekly_growth_rate", 0.007)
            
            if growth_enabled:
                growth = int(current_size * weekly_growth_rate * weeks_elapsed)
                current_size += growth
        
        return max(current_size, 100)  # Minimum size
    
    def _generate_enhanced_file_path(self,
                                   table_name: str,
                                   dataset_id: str,
                                   generation_date: date,
                                   path_format: str,
                                   output_mode: str,
                                   output_settings: Dict[str, Any]) -> str:
        """Generate file path using enhanced path generator."""
        if not self.file_path_generator:
            # Fall back to legacy method
            return self._generate_file_path(table_name, dataset_id, generation_date, path_format, output_mode)
        
        base_path = f"synthetic_data/{dataset_id}"
        file_extension = output_settings.get("file_extension", "parquet")
        
        # Handle custom pattern
        custom_pattern = output_settings.get("custom_path_pattern")
        if path_format == "custom" and custom_pattern:
            return self.file_path_generator.generate_path(
                pattern="custom",
                base_path=base_path,
                table_name=table_name,
                generation_date=generation_date,
                custom_pattern=custom_pattern,
                file_extension=file_extension
            )
        else:
            return self.file_path_generator.generate_path(
                pattern=path_format,
                base_path=base_path,
                table_name=table_name,
                generation_date=generation_date,
                file_extension=file_extension
            )
    
    def _save_table_data_enhanced(self,
                                data,
                                table_name: str,
                                file_path: str,
                                output_mode: str):
        """Save table data using enhanced file management."""
        if self.file_path_manager:
            # Ensure directory exists
            self.file_path_manager.ensure_directory_exists(file_path)
            
            # Validate path
            is_valid, issues = self.file_path_manager.validate_file_path(file_path)
            if not is_valid and self.enhanced_logging:
                self.logger.warning(f"File path validation issues: {issues}")
        
        # Determine the actual save parameters from the file path
        if "/" in file_path:
            path_parts = file_path.rsplit("/", 1)
            directory = path_parts[0]
            filename = path_parts[1]
        else:
            directory = ""
            filename = file_path
        
        # Save using existing method
        if output_mode == "parquet":
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                # PySpark DataFrame
                self.lakehouse_utils.write_file(
                    data, file_path, "parquet", {"mode": "overwrite"}
                )
            elif PANDAS_AVAILABLE and hasattr(data, 'to_parquet'):
                # Pandas DataFrame - need full path
                full_path = f"{self.lakehouse_utils.lakehouse_files_uri()}{file_path}.parquet"
                data.to_parquet(full_path, index=False)
        elif output_mode == "csv":
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                # PySpark DataFrame - save as CSV
                self.lakehouse_utils.write_file(
                    data, file_path, "csv", {"mode": "overwrite", "header": "true"}
                )
            elif PANDAS_AVAILABLE and hasattr(data, 'to_csv'):
                # Pandas DataFrame - need full path
                full_path = f"{self.lakehouse_utils.lakehouse_files_uri()}{file_path}.csv"
                data.to_csv(full_path, index=False)
        else:
            # Table mode - extract table name from file path
            table_name_from_path = filename.replace(".parquet", "").replace(".csv", "")
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                self.lakehouse_utils.write_to_table(data, table_name_from_path, mode="overwrite")
        
        return file_path
    
    def _generate_snapshot_table_enhanced(self,
                                        table_name: str,
                                        table_config: TableGenerationConfig,
                                        generation_date: date,
                                        state: Dict[str, Any],
                                        incremental_config: Dict[str, Any]):
        """Generate snapshot table using enhanced configuration."""
        current_size = table_config.calculate_target_rows(
            generation_date, 
            current_size=state.get("tables", {}).get(table_name, {}).get("current_size"),
            seasonal_multipliers=incremental_config.get("seasonal_multipliers", {})
        )
        
        # Use existing generation methods but with enhanced configuration
        return self._generate_snapshot_table(
            table_name, 
            table_config.__dict__, 
            generation_date, 
            state, 
            incremental_config
        )
    
    def _generate_incremental_table_enhanced(self,
                                           table_name: str,
                                           table_config: TableGenerationConfig,
                                           generation_date: date,
                                           state: Dict[str, Any],
                                           incremental_config: Dict[str, Any]):
        """Generate incremental table using enhanced configuration."""
        target_rows = table_config.calculate_target_rows(
            generation_date,
            seasonal_multipliers=incremental_config.get("seasonal_multipliers", {})
        )
        
        # Create a temporary dict config for compatibility with existing methods
        temp_config = table_config.__dict__.copy()
        temp_config["base_rows_per_day"] = target_rows
        
        return self._generate_incremental_table(
            table_name,
            temp_config,
            generation_date,
            state,
            incremental_config
        )
    
    def _apply_seasonal_multipliers(
        self,
        base_rows: int,
        generation_date: date,
        seasonal_enabled: bool,
        incremental_config: Dict[str, Any]
    ) -> int:
        """Apply seasonal multipliers to the base row count."""
        if not seasonal_enabled:
            return base_rows
        
        multiplier = 1.0
        
        # Day of week multiplier
        seasonal_multipliers = incremental_config.get("seasonal_multipliers", {})
        day_name = generation_date.strftime("%A").lower()
        day_multiplier = seasonal_multipliers.get(day_name, 1.0)
        multiplier *= day_multiplier
        
        # Holiday multiplier (simplified - just check if it's a weekend)
        if generation_date.weekday() >= 5:  # Saturday or Sunday
            weekend_multiplier = incremental_config.get("weekend_multiplier", 1.2)
            multiplier *= weekend_multiplier
        
        return int(base_rows * multiplier)
    
    def _generate_file_path(
        self,
        table_name: str,
        dataset_id: str,
        generation_date: date,
        path_format: str,
        output_mode: str
    ) -> str:
        """Generate file path based on format preference."""
        base_path = f"synthetic_data/{dataset_id}"
        
        if path_format == "nested":
            # Files/Dataset/YYYY/MM/DD/table_name.parquet
            date_path = generation_date.strftime("%Y/%m/%d")
            return f"{base_path}/{date_path}/{table_name}"
        else:
            # Files/Dataset/YYYYMMDD_table_name.parquet
            date_str = generation_date.strftime("%Y%m%d")
            return f"{base_path}/{date_str}_{table_name}"
    
    def _save_table_data(
        self,
        data,
        table_name: str,
        dataset_id: str,
        generation_date: date,
        path_format: str,
        output_mode: str
    ) -> str:
        """Save table data to the appropriate location."""
        file_path = self._generate_file_path(
            table_name, dataset_id, generation_date, path_format, output_mode
        )
        
        if output_mode == "parquet":
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                # PySpark DataFrame
                self.lakehouse_utils.write_file(
                    data, file_path, "parquet", {"mode": "overwrite"}
                )
            elif PANDAS_AVAILABLE and hasattr(data, 'to_parquet'):
                # Pandas DataFrame
                full_path = f"{self.lakehouse_utils.lakehouse_files_uri()}{file_path}.parquet"
                data.to_parquet(full_path, index=False)
        elif output_mode == "csv":
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                # PySpark DataFrame - save as CSV
                self.lakehouse_utils.write_file(
                    data, file_path, "csv", {"mode": "overwrite", "header": "true"}
                )
            elif PANDAS_AVAILABLE and hasattr(data, 'to_csv'):
                # Pandas DataFrame
                full_path = f"{self.lakehouse_utils.lakehouse_files_uri()}{file_path}.csv"
                data.to_csv(full_path, index=False)
        else:
            # Table mode
            table_name_clean = f"{dataset_id}_{table_name}_{generation_date.strftime('%Y%m%d')}"
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                self.lakehouse_utils.write_to_table(data, table_name_clean, mode="overwrite")
            else:
                # For pandas, we'd need to convert to PySpark or use other methods
                raise NotImplementedError("Table mode not implemented for pandas DataFrames")
        
        return file_path
    
    def _load_dataset_state(self, dataset_id: str, generation_date: date) -> Dict[str, Any]:
        """Load or initialize dataset state."""
        if dataset_id in self._state_cache:
            return self._state_cache[dataset_id].copy()
        
        # Try to load from persistent state table
        persistent_state = self._load_state_from_table(dataset_id)
        if persistent_state:
            self._state_cache[dataset_id] = persistent_state
            return persistent_state.copy()
        
        # Initialize new state
        new_state = {
            "dataset_id": dataset_id,
            "created_date": generation_date.isoformat(),
            "last_updated": generation_date.isoformat(),
            "tables": {}
        }
        
        self._state_cache[dataset_id] = new_state
        return new_state.copy()
    
    def _save_dataset_state(self, dataset_id: str, generation_date: date, state: Dict[str, Any]):
        """Save dataset state to persistent storage."""
        state["last_updated"] = generation_date.isoformat()
        self._state_cache[dataset_id] = state
        
        # Save to persistent state table
        try:
            self._ensure_state_table_exists()
            self._upsert_state_record(dataset_id, state)
            self.logger.info(f"State saved for dataset {dataset_id}")
        except Exception as e:
            self.logger.warning(f"Failed to save state for {dataset_id}: {e}")
            raise e
            # Continue execution - state is still cached in memory
        
    def _update_table_state(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        generation_date: date,
        state: Dict[str, Any],
        actual_size: int = None
    ):
        """Update state for a specific table."""
        if "tables" not in state:
            state["tables"] = {}
        
        if table_name not in state["tables"]:
            state["tables"][table_name] = {}
        
        table_state = state["tables"][table_name]
        table_state["last_generated"] = generation_date.isoformat()
        table_state["generation_count"] = table_state.get("generation_count", 0) + 1
        
        # Update size for snapshot tables
        if table_config.get("type") == "snapshot" and actual_size is not None:
            table_state["current_size"] = actual_size
            self.logger.debug(f"Updated {table_name} size to {actual_size}")
    
    # Placeholder methods for specific table generation
    # These would be implemented based on the existing synthetic_data_utils patterns
    
    def _generate_customers_snapshot(self, num_rows: int, generation_date: date):
        """Generate customers snapshot data."""
        if not self.base_generator:
            raise RuntimeError("Base generator not initialized")
        
        # Use base generator with date-based seed modification
        date_seed_modifier = int(generation_date.strftime("%Y%m%d"))
        original_seed = self.base_generator.seed
        
        if original_seed:
            modified_seed = (original_seed + date_seed_modifier) % 2147483647
            self.base_generator.seed = modified_seed
        
        df = self.base_generator.generate_customers_table(num_rows)
        
        # Add generation metadata
        if PYSPARK_AVAILABLE:
            df = df.withColumn("snapshot_date", lit(generation_date.isoformat()))
            df = df.withColumn("is_active", 
                when((col("customer_id") % 100) < 85, True).otherwise(False))
        
        # Restore original seed
        if original_seed:
            self.base_generator.seed = original_seed
            
        return df
    
    def _generate_products_snapshot(self, num_rows: int, generation_date: date):
        """Generate products snapshot data."""
        if not self.base_generator:
            raise RuntimeError("Base generator not initialized")
        
        # Use base generator with date-based seed modification
        date_seed_modifier = int(generation_date.strftime("%Y%m%d"))
        original_seed = self.base_generator.seed
        
        if original_seed:
            modified_seed = (original_seed + date_seed_modifier) % 2147483647
            self.base_generator.seed = modified_seed
        
        df = self.base_generator.generate_products_table(num_rows)
        
        # Add generation metadata
        if PYSPARK_AVAILABLE:
            df = df.withColumn("snapshot_date", lit(generation_date.isoformat()))
            df = df.withColumn("is_active", 
                when((col("product_id") % 100) < 90, True).otherwise(False))
        
        # Restore original seed
        if original_seed:
            self.base_generator.seed = original_seed
            
        return df
    
    def _generate_stores_snapshot(self, num_rows: int, generation_date: date):
        """Generate stores snapshot data."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark not available or lakehouse utils not initialized")
        
        # Generate stores using similar pattern to customers/products
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, max(1, num_rows // 10000)).toDF("store_id")
        
        stores_df = base_df.select(
            col("store_id"),
            concat(lit("Store #"), col("store_id").cast("string")).alias("store_name"),
            expr(
                "CASE WHEN store_id % 5 = 0 THEN 'Mall' "
                + "WHEN store_id % 5 = 1 THEN 'Strip Center' "
                + "WHEN store_id % 5 = 2 THEN 'Downtown' "
                + "WHEN store_id % 5 = 3 THEN 'Suburban' "
                + "ELSE 'Outlet' END"
            ).alias("store_type"),
            expr(
                "CASE WHEN store_id % 10 < 2 THEN 'Large' "
                + "WHEN store_id % 10 < 7 THEN 'Medium' "
                + "ELSE 'Small' END"
            ).alias("store_size"),
            lit(generation_date.isoformat()).alias("snapshot_date"),
            lit(True).alias("is_active")
        )
        
        return stores_df
    
    def _generate_date_dimension(self, generation_date: date):
        """Generate date dimension."""
        if not self.base_generator:
            raise RuntimeError("Base generator not initialized")
        
        # Generate a comprehensive date dimension
        return self.base_generator.generate_date_dimension(generation_date)
    
    def _generate_generic_snapshot(self, table_name: str, num_rows: int, generation_date: date):
        """Generate generic snapshot table."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark not available or lakehouse utils not initialized")
        
        # Generate generic table with basic structure
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, max(1, num_rows // 10000)).toDF("id")
        
        generic_df = base_df.select(
            col("id"),
            concat(lit(table_name), lit("_"), col("id").cast("string")).alias("name"),
            expr("CASE WHEN id % 2 = 0 THEN 'Type A' ELSE 'Type B' END").alias("type"),
            lit(generation_date.isoformat()).alias("snapshot_date"),
            (col("id") % 1000).alias("value")
        )
        
        return generic_df
    
    def _generate_orders_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental orders data."""
        if not self.base_generator:
            raise RuntimeError("Base generator not initialized")
        
        # Get customer count from state or use default
        customer_count = state.get("tables", {}).get("customers", {}).get("current_size", num_rows // 10)
        
        # Use base generator to create orders
        df = self.base_generator.generate_orders_table(num_rows, customer_count)
        
        # Add incremental-specific columns
        if PYSPARK_AVAILABLE:
            df = df.withColumn("order_date", lit(generation_date.isoformat()))
            from pyspark.sql.functions import from_unixtime, unix_timestamp
            base_timestamp = unix_timestamp(lit(f"{generation_date.isoformat()} 00:00:00"))
            df = df.withColumn("created_timestamp", 
                from_unixtime(base_timestamp + (col("order_id") % 86400)))
        
        return df
    
    def _generate_order_items_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental order items data."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark not available or lakehouse utils not initialized")
        
        # Generate order items based on estimated order count
        estimated_orders = num_rows // 2  # Assume ~2 items per order on average
        product_count = state.get("tables", {}).get("products", {}).get("current_size", 1000)
        
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, max(1, num_rows // 10000)).toDF("item_id")
        
        order_items_df = base_df.select(
            col("item_id"),
            (col("item_id") % estimated_orders + 1).alias("order_id"),
            (col("item_id") % product_count + 1).alias("product_id"),
            (col("item_id") % 5 + 1).alias("quantity"),
            spark_round((rand() * 100 + 10), 2).alias("unit_price"),
            lit(generation_date.isoformat()).alias("order_date")
        )
        
        # Add calculated total price
        order_items_df = order_items_df.withColumn(
            "total_price", 
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        return order_items_df
    
    def _generate_sales_fact_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental sales fact data."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark not available or lakehouse utils not initialized")
        
        customer_count = state.get("tables", {}).get("dim_customer", {}).get("current_size", 100000)
        product_count = state.get("tables", {}).get("dim_product", {}).get("current_size", 10000)
        store_count = state.get("tables", {}).get("dim_store", {}).get("current_size", 100)
        
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, max(1, num_rows // 50000)).toDF("sale_id")
        
        sales_df = base_df.select(
            col("sale_id"),
            (col("sale_id") % customer_count + 1).alias("customer_id"),
            (col("sale_id") % product_count + 1).alias("product_id"),
            (col("sale_id") % store_count + 1).alias("store_id"),
            lit(generation_date.isoformat()).alias("sale_date"),
            (col("sale_id") % 10 + 1).alias("quantity"),
            spark_round((rand() * 500 + 10), 2).alias("unit_price"),
            spark_round((rand() * 50), 2).alias("discount_amount")
        )
        
        # Add calculated columns
        sales_df = sales_df.withColumn(
            "gross_amount", 
            spark_round(col("quantity") * col("unit_price"), 2)
        ).withColumn(
            "net_amount",
            spark_round(col("gross_amount") - col("discount_amount"), 2)
        )
        
        return sales_df
    
    def _generate_inventory_fact_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental inventory fact data."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark not available or lakehouse utils not initialized")
        
        product_count = state.get("tables", {}).get("dim_product", {}).get("current_size", 10000)
        store_count = state.get("tables", {}).get("dim_store", {}).get("current_size", 100)
        
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, max(1, num_rows // 50000)).toDF("record_id")
        
        inventory_df = base_df.select(
            col("record_id"),
            (col("record_id") % product_count + 1).alias("product_id"),
            (col("record_id") % store_count + 1).alias("store_id"),
            lit(generation_date.isoformat()).alias("inventory_date"),
            (col("record_id") % 1000 + 50).alias("quantity_on_hand"),
            (col("record_id") % 100 + 10).alias("reorder_level"),
            spark_round((rand() * 50 + 5), 2).alias("unit_cost")
        )
        
        return inventory_df
    
    def _generate_generic_incremental(self, table_name: str, num_rows: int, generation_date: date):
        """Generate generic incremental table data."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark not available or lakehouse utils not initialized")
        
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, max(1, num_rows // 10000)).toDF("id")
        
        generic_df = base_df.select(
            col("id"),
            concat(lit(table_name), lit("_"), col("id").cast("string")).alias("name"),
            expr("CASE WHEN id % 3 = 0 THEN 'Active' WHEN id % 3 = 1 THEN 'Pending' ELSE 'Inactive' END").alias("status"),
            lit(generation_date.isoformat()).alias("created_date"),
            (rand() * 100).alias("value"),
            expr("CASE WHEN id % 2 = 0 THEN 'Category A' ELSE 'Category B' END").alias("category")
        )
        
        return generic_df
    
    # State management helper methods
    
    def _ensure_state_table_exists(self):
        """Ensure the state table exists, create if necessary."""
        if not self.lakehouse_utils:
            raise RuntimeError("Lakehouse utils not available for state management")
        
        try:
            # Try to read the table to see if it exists
            self.lakehouse_utils.read_table(self.state_table_name)
            self.logger.debug(f"State table {self.state_table_name} already exists")
        except Exception:
            # Table doesn't exist, create it
            self._create_state_table()
    
    def _create_state_table(self):
        """Create the state management table."""
        if not PYSPARK_AVAILABLE:
            raise RuntimeError("PySpark not available for state table creation")
        
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        # Define the state table schema
        state_schema = StructType([
            StructField("dataset_id", StringType(), False),
            StructField("state_json", StringType(), False),
            StructField("created_date", TimestampType(), False),
            StructField("last_updated", TimestampType(), False)
        ])
        
        # Create empty DataFrame with the schema
        empty_df = self.lakehouse_utils.spark.createDataFrame([], state_schema)
        
        # Write as table to create it
        self.lakehouse_utils.write_to_table(
            empty_df, 
            self.state_table_name, 
            mode="overwrite"
        )
        
        self.logger.info(f"Created state table: {self.state_table_name}")
    
    def _upsert_state_record(self, dataset_id: str, state: Dict[str, Any]):
        """Insert or update state record for a dataset."""
        if not PYSPARK_AVAILABLE or not self.lakehouse_utils:
            raise RuntimeError("PySpark/lakehouse utils not available")
        
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        from pyspark.sql.functions import current_timestamp
        import json
        
        # Convert state to JSON string
        state_json = json.dumps(state, default=str, sort_keys=True)
        
        # Create DataFrame with new state record
        schema = StructType([
            StructField("dataset_id", StringType(), False),
            StructField("state_json", StringType(), False),
            StructField("created_date", TimestampType(), False),
            StructField("last_updated", TimestampType(), False)
        ])
        
        # Get creation date from state or use current time
        created_date = state.get("created_date")
        if isinstance(created_date, str):
            from pyspark.sql.functions import to_timestamp
            created_ts = to_timestamp(lit(created_date), "yyyy-MM-dd")
        else:
            created_ts = current_timestamp()
        
        # Create DataFrame with proper timestamp values
        from pyspark.sql.functions import current_timestamp
        new_record_data = [(dataset_id, state_json)]
        new_record = self.lakehouse_utils.spark.createDataFrame(
            new_record_data, 
            ["dataset_id", "state_json"]
        ).select(
            col("dataset_id"),
            col("state_json"),
            created_ts.alias("created_date"),
            current_timestamp().alias("last_updated")
        )
        
        try:
            # Try to read existing table
            existing_df = self.lakehouse_utils.read_table(self.state_table_name)
            
            # Remove existing record for this dataset_id
            filtered_df = existing_df.filter(col("dataset_id") != dataset_id)
            
            # Union with new record
            updated_df = filtered_df.union(new_record)
            
            # Write back to table
            self.lakehouse_utils.write_to_table(
                updated_df,
                self.state_table_name,
                mode="overwrite"
            )
            
        except Exception as e:
            # If table read fails, just write the new record
            self.logger.warning(f"Could not read existing state table, creating new: {e}")
            self.lakehouse_utils.write_to_table(
                new_record,
                self.state_table_name,
                mode="overwrite"
            )
    
    def _load_state_from_table(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Load state from persistent table."""
        try:
            if not self.lakehouse_utils:
                return None
                
            state_df = self.lakehouse_utils.read_table(self.state_table_name)
            state_records = state_df.filter(
                col("dataset_id") == dataset_id
            ).collect()
            
            if state_records:
                latest_record = state_records[0]
                state = json.loads(latest_record["state_json"])
                self.logger.info(f"Loaded persistent state for {dataset_id}")
                return state
                
        except Exception as e:
            self.logger.debug(f"Could not load state from table for {dataset_id}: {e}")
            
        return None
