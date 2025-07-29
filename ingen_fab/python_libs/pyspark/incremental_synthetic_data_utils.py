"""
Incremental Synthetic Data Utils - Enhanced version for time-based generation

This module extends the existing synthetic data utilities to support incremental
data generation with state management, date-based partitioning, and different
table types (snapshot vs incremental).
"""

from __future__ import annotations

import json
import logging
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


class IncrementalSyntheticDataGenerator:
    """
    Enhanced synthetic data generator that supports incremental data generation
    with date-based partitioning and state management.
    """
    
    def __init__(
        self,
        lakehouse_utils_instance=None,
        seed: Optional[int] = None,
        state_table_name: str = "synthetic_data_state"
    ):
        self.lakehouse_utils = lakehouse_utils_instance
        self.seed = seed
        self.state_table_name = state_table_name
        self.logger = logging.getLogger(__name__)
        
        # Initialize state management
        self._state_cache = {}
        
        if seed:
            if PYSPARK_AVAILABLE and lakehouse_utils_instance:
                self.lakehouse_utils.set_spark_config("spark.sql.shuffle.partitions", "200")
    
    def generate_incremental_dataset(
        self,
        dataset_config: Dict[str, Any],
        generation_date: Union[str, date],
        path_format: Literal["nested", "flat"] = "nested",
        output_mode: str = "parquet"
    ) -> Dict[str, Any]:
        """
        Generate incremental synthetic data for a specific date.
        
        Args:
            dataset_config: Dataset configuration with table configs
            generation_date: Date to generate data for
            path_format: Path format ("nested" for /YYYY/MM/DD/ or "flat" for YYYYMMDD_)
            output_mode: Output mode ("parquet" or "table")
        
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
                self._update_table_state(table_name, table_config, generation_date, state)
        
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
        batch_size: int = 30
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
                    dataset_config, batch_date, path_format, output_mode
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
        else:
            # Table mode
            table_name = f"{dataset_id}_{table_name}_{generation_date.strftime('%Y%m%d')}"
            if PYSPARK_AVAILABLE and hasattr(data, 'write'):
                self.lakehouse_utils.write_to_table(data, table_name, mode="overwrite")
            else:
                # For pandas, we'd need to convert to PySpark or use other methods
                raise NotImplementedError("Table mode not implemented for pandas DataFrames")
        
        return file_path
    
    def _load_dataset_state(self, dataset_id: str, generation_date: date) -> Dict[str, Any]:
        """Load or initialize dataset state."""
        if dataset_id in self._state_cache:
            return self._state_cache[dataset_id].copy()
        
        # Try to load from state table
        try:
            if self.lakehouse_utils and hasattr(self.lakehouse_utils, 'read_table'):
                state_df = self.lakehouse_utils.read_table(self.state_table_name)
                state_records = state_df.filter(
                    col("dataset_id") == dataset_id
                ).collect()
                
                if state_records:
                    latest_record = state_records[0]
                    state = json.loads(latest_record["state_json"])
                    self._state_cache[dataset_id] = state
                    return state.copy()
        except Exception as e:
            self.logger.warning(f"Could not load state for {dataset_id}: {e}")
        
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
        """Save dataset state."""
        state["last_updated"] = generation_date.isoformat()
        self._state_cache[dataset_id] = state
        
        # In a real implementation, we would save this to the state table
        # For now, we just cache it in memory
        
    def _update_table_state(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        generation_date: date,
        state: Dict[str, Any]
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
        if table_config.get("type") == "snapshot":
            # This would be set during generation
            pass
    
    # Placeholder methods for specific table generation
    # These would be implemented based on the existing synthetic_data_utils patterns
    
    def _generate_customers_snapshot(self, num_rows: int, generation_date: date):
        """Generate customers snapshot data."""
        # Implementation would go here using existing patterns
        pass
    
    def _generate_products_snapshot(self, num_rows: int, generation_date: date):
        """Generate products snapshot data."""
        pass
    
    def _generate_stores_snapshot(self, num_rows: int, generation_date: date):
        """Generate stores snapshot data."""
        pass
    
    def _generate_date_dimension(self, generation_date: date):
        """Generate date dimension."""
        pass
    
    def _generate_generic_snapshot(self, table_name: str, num_rows: int, generation_date: date):
        """Generate generic snapshot table."""
        pass
    
    def _generate_orders_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental orders data."""
        pass
    
    def _generate_order_items_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental order items data."""
        pass
    
    def _generate_sales_fact_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental sales fact data."""
        pass
    
    def _generate_inventory_fact_incremental(self, num_rows: int, generation_date: date, state: Dict[str, Any]):
        """Generate incremental inventory fact data."""
        pass
    
    def _generate_generic_incremental(self, table_name: str, num_rows: int, generation_date: date):
        """Generate generic incremental table data."""
        pass
