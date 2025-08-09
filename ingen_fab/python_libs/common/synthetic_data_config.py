"""
Enhanced Synthetic Data Configuration Management

This module provides flexible configuration management for synthetic data generation
with runtime parameter support and dynamic scaling capabilities.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Literal, Optional


@dataclass
class TableGenerationConfig:
    """Configuration for individual table generation."""

    table_name: str
    table_type: Literal["snapshot", "incremental"] = "incremental"
    frequency: Literal["daily", "weekly", "monthly", "quarterly", "once"] = "daily"
    base_rows: int = 10000
    base_rows_per_day: int = 1000
    growth_enabled: bool = False
    churn_enabled: bool = False
    seasonal_enabled: bool = True
    daily_growth_rate: float = 0.001
    daily_churn_rate: float = 0.0005
    weekly_growth_rate: float = 0.007
    monthly_growth_rate: float = 0.02
    quarterly_growth_rate: float = 0.05
    weekend_multiplier: float = 1.0
    holiday_multiplier: float = 1.0
    custom_multipliers: Dict[str, float] = field(default_factory=dict)
    date_columns: List[str] = field(default_factory=list)
    primary_date_column: str = None

    def calculate_target_rows(
        self,
        generation_date: date,
        current_size: int = None,
        seasonal_multipliers: Dict[str, float] = None,
    ) -> int:
        """Calculate target rows for a specific generation date."""
        if self.table_type == "snapshot":
            return self._calculate_snapshot_rows(generation_date, current_size)
        else:
            return self._calculate_incremental_rows(
                generation_date, seasonal_multipliers
            )

    def _calculate_snapshot_rows(
        self, generation_date: date, current_size: int = None
    ) -> int:
        """Calculate snapshot table rows with growth/churn."""
        if current_size is None:
            current_size = self.base_rows

        if not self.growth_enabled and not self.churn_enabled:
            return current_size

        # Apply growth and churn based on frequency
        if self.frequency == "daily":
            if self.growth_enabled:
                growth = int(current_size * self.daily_growth_rate)
                current_size += growth
            if self.churn_enabled:
                churn = int(current_size * self.daily_churn_rate)
                current_size = max(current_size - churn, self.base_rows // 2)

        return max(current_size, 100)

    def _calculate_incremental_rows(
        self, generation_date: date, seasonal_multipliers: Dict[str, float] = None
    ) -> int:
        """Calculate incremental table rows with seasonal adjustments."""
        base_rows = self.base_rows_per_day

        if not self.seasonal_enabled:
            return base_rows

        multiplier = 1.0

        # Apply day-of-week multiplier
        if seasonal_multipliers:
            day_name = generation_date.strftime("%A").lower()
            day_multiplier = seasonal_multipliers.get(day_name, 1.0)
            multiplier *= day_multiplier

        # Apply weekend multiplier
        if generation_date.weekday() >= 5:  # Saturday or Sunday
            multiplier *= self.weekend_multiplier

        # Apply custom multipliers
        date_key = generation_date.strftime("%Y-%m-%d")
        if date_key in self.custom_multipliers:
            multiplier *= self.custom_multipliers[date_key]

        return int(base_rows * multiplier)


@dataclass
class DatasetConfiguration:
    """Enhanced dataset configuration with runtime flexibility."""

    dataset_id: str
    dataset_name: str
    dataset_type: Literal["transactional", "analytical", "custom"] = "transactional"
    schema_pattern: Literal["oltp", "star_schema", "custom"] = "oltp"
    domain: str = "retail"
    description: str = ""

    # Table configurations
    table_configs: Dict[str, TableGenerationConfig] = field(default_factory=dict)

    # Global settings
    incremental_config: Dict[str, Any] = field(default_factory=dict)
    output_settings: Dict[str, Any] = field(default_factory=dict)

    # Runtime overrides
    runtime_overrides: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize default configurations after object creation."""
        if not self.table_configs:
            self.table_configs = self._get_default_table_configs()

        if not self.incremental_config:
            self.incremental_config = self._get_default_incremental_config()

        if not self.output_settings:
            self.output_settings = self._get_default_output_settings()

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "DatasetConfiguration":
        """Create configuration from dictionary."""
        config = cls(
            dataset_id=config_dict.get("dataset_id", "custom_dataset"),
            dataset_name=config_dict.get("dataset_name", "Custom Dataset"),
            dataset_type=config_dict.get("dataset_type", "transactional"),
            schema_pattern=config_dict.get("schema_pattern", "oltp"),
            domain=config_dict.get("domain", "retail"),
            description=config_dict.get("description", ""),
        )

        # Load table configurations
        table_configs_dict = config_dict.get("table_configs", {})
        for table_name, table_config in table_configs_dict.items():
            config.table_configs[table_name] = TableGenerationConfig(
                table_name=table_name, **table_config
            )

        config.incremental_config = config_dict.get("incremental_config", {})
        config.output_settings = config_dict.get("output_settings", {})
        config.runtime_overrides = config_dict.get("runtime_overrides", {})

        return config

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        table_configs_dict = {}
        for table_name, table_config in self.table_configs.items():
            table_configs_dict[table_name] = {
                "table_type": table_config.table_type,
                "frequency": table_config.frequency,
                "base_rows": table_config.base_rows,
                "base_rows_per_day": table_config.base_rows_per_day,
                "growth_enabled": table_config.growth_enabled,
                "churn_enabled": table_config.churn_enabled,
                "seasonal_enabled": table_config.seasonal_enabled,
                "daily_growth_rate": table_config.daily_growth_rate,
                "daily_churn_rate": table_config.daily_churn_rate,
                "weekend_multiplier": table_config.weekend_multiplier,
                "holiday_multiplier": table_config.holiday_multiplier,
                "date_columns": table_config.date_columns,
                "primary_date_column": table_config.primary_date_column,
            }

        return {
            "dataset_id": self.dataset_id,
            "dataset_name": self.dataset_name,
            "dataset_type": self.dataset_type,
            "schema_pattern": self.schema_pattern,
            "domain": self.domain,
            "description": self.description,
            "table_configs": table_configs_dict,
            "incremental_config": self.incremental_config,
            "output_settings": self.output_settings,
            "runtime_overrides": self.runtime_overrides,
        }

    def get_table_config(self, table_name: str) -> Optional[TableGenerationConfig]:
        """Get configuration for a specific table."""
        return self.table_configs.get(table_name)

    def apply_runtime_overrides(self, overrides: Dict[str, Any]):
        """Apply runtime overrides to the configuration."""
        self.runtime_overrides.update(overrides)

        # Apply table-specific overrides
        table_overrides = overrides.get("table_configs", {})
        for table_name, table_override in table_overrides.items():
            if table_name in self.table_configs:
                table_config = self.table_configs[table_name]
                for key, value in table_override.items():
                    if hasattr(table_config, key):
                        setattr(table_config, key, value)

        # Apply global overrides
        incremental_overrides = overrides.get("incremental_config", {})
        self.incremental_config.update(incremental_overrides)

        output_overrides = overrides.get("output_settings", {})
        self.output_settings.update(output_overrides)

    def calculate_total_rows_for_date(self, generation_date: date) -> int:
        """Calculate total rows to be generated for a specific date."""
        total_rows = 0
        seasonal_multipliers = self.incremental_config.get("seasonal_multipliers", {})

        for table_config in self.table_configs.values():
            rows = table_config.calculate_target_rows(
                generation_date, seasonal_multipliers=seasonal_multipliers
            )
            total_rows += rows

        return total_rows

    def _get_default_table_configs(self) -> Dict[str, TableGenerationConfig]:
        """Get default table configurations based on schema pattern."""
        if self.schema_pattern == "oltp":
            return self._get_oltp_table_configs()
        elif self.schema_pattern == "star_schema":
            return self._get_star_schema_table_configs()
        else:
            return self._get_custom_table_configs()

    def _get_oltp_table_configs(self) -> Dict[str, TableGenerationConfig]:
        """Get OLTP table configurations."""
        return {
            "customers": TableGenerationConfig(
                table_name="customers",
                table_type="snapshot",
                frequency="daily",
                base_rows=50000,
                growth_enabled=True,
                churn_enabled=True,
                daily_growth_rate=0.002,
                daily_churn_rate=0.001,
                date_columns=["registration_date", "last_login_date"],
                primary_date_column="registration_date",
            ),
            "products": TableGenerationConfig(
                table_name="products",
                table_type="snapshot",
                frequency="weekly",
                base_rows=5000,
                growth_enabled=True,
                churn_enabled=False,
                weekly_growth_rate=0.01,
                date_columns=["created_date", "last_updated_date"],
                primary_date_column="created_date",
            ),
            "orders": TableGenerationConfig(
                table_name="orders",
                table_type="incremental",
                frequency="daily",
                base_rows_per_day=100000,
                seasonal_enabled=True,
                weekend_multiplier=1.4,
                holiday_multiplier=2.2,
                date_columns=["order_date", "shipped_date", "delivered_date"],
                primary_date_column="order_date",
            ),
            "order_items": TableGenerationConfig(
                table_name="order_items",
                table_type="incremental",
                frequency="daily",
                base_rows_per_day=240000,  # ~2.4 items per order
                seasonal_enabled=True,
                weekend_multiplier=1.4,
                date_columns=["order_date"],
                primary_date_column="order_date",
            ),
        }

    def _get_star_schema_table_configs(self) -> Dict[str, TableGenerationConfig]:
        """Get Star Schema table configurations."""
        return {
            "dim_customer": TableGenerationConfig(
                table_name="dim_customer",
                table_type="snapshot",
                frequency="weekly",
                base_rows=500000,
                growth_enabled=True,
                weekly_growth_rate=0.005,
                date_columns=["effective_date", "expiry_date"],
                primary_date_column="effective_date",
            ),
            "dim_product": TableGenerationConfig(
                table_name="dim_product",
                table_type="snapshot",
                frequency="monthly",
                base_rows=100000,
                growth_enabled=True,
                monthly_growth_rate=0.02,
                date_columns=["effective_date", "expiry_date"],
                primary_date_column="effective_date",
            ),
            "dim_store": TableGenerationConfig(
                table_name="dim_store",
                table_type="snapshot",
                frequency="quarterly",
                base_rows=1000,
                growth_enabled=True,
                quarterly_growth_rate=0.05,
                date_columns=["effective_date"],
                primary_date_column="effective_date",
            ),
            "dim_date": TableGenerationConfig(
                table_name="dim_date",
                table_type="snapshot",
                frequency="once",
                base_rows=3653,  # 10 years
                date_columns=["full_date"],
                primary_date_column="full_date",
            ),
            "fact_sales": TableGenerationConfig(
                table_name="fact_sales",
                table_type="incremental",
                frequency="daily",
                base_rows_per_day=1000000,
                seasonal_enabled=True,
                weekend_multiplier=1.3,
                holiday_multiplier=2.5,
                date_columns=["sale_date", "transaction_timestamp"],
                primary_date_column="sale_date",
            ),
        }

    def _get_custom_table_configs(self) -> Dict[str, TableGenerationConfig]:
        """Get custom table configurations."""
        return {
            "main_table": TableGenerationConfig(
                table_name="main_table",
                table_type="incremental",
                frequency="daily",
                base_rows_per_day=10000,
                seasonal_enabled=True,
                date_columns=["created_date"],
                primary_date_column="created_date",
            )
        }

    def _get_default_incremental_config(self) -> Dict[str, Any]:
        """Get default incremental configuration."""
        return {
            "snapshot_frequency": "daily",
            "state_table_name": "synthetic_data_state",
            "enable_data_drift": True,
            "drift_percentage": 0.05,
            "enable_seasonal_patterns": True,
            "seasonal_multipliers": {
                "monday": 0.8,
                "tuesday": 0.9,
                "wednesday": 1.0,
                "thursday": 1.1,
                "friday": 1.3,
                "saturday": 1.2,
                "sunday": 0.7,
            },
            "growth_rate": 0.001,
            "churn_rate": 0.0005,
            "weekend_multiplier": 1.2,
            "holiday_multiplier": 2.0,
            "holiday_dates": [],  # Custom holiday dates
            "seed_value": None,
        }

    def _get_default_output_settings(self) -> Dict[str, Any]:
        """Get default output settings."""
        return {
            "output_mode": "parquet",  # "parquet" or "table"
            "path_format": "nested",  # "nested", "flat", or "custom"
            "custom_path_pattern": None,
            "partition_by": None,  # Column to partition by
            "file_compression": "snappy",
            "include_metadata": True,
            "validate_output": True,
        }


class ConfigurationManager:
    """Manager for synthetic data configurations."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._predefined_configs = {}
        self._load_predefined_configs()

    def _load_predefined_configs(self):
        """Load predefined dataset configurations."""
        self._predefined_configs = {
            "retail_oltp_enhanced": DatasetConfiguration(
                dataset_id="retail_oltp_enhanced",
                dataset_name="Enhanced Retail OLTP",
                dataset_type="transactional",
                schema_pattern="oltp",
                domain="retail",
                description="Enhanced retail transactional system with flexible parameters",
            ),
            "retail_star_enhanced": DatasetConfiguration(
                dataset_id="retail_star_enhanced",
                dataset_name="Enhanced Retail Star Schema",
                dataset_type="analytical",
                schema_pattern="star_schema",
                domain="retail",
                description="Enhanced retail star schema with configurable dimensions",
            ),
            "ecommerce_full_stack": DatasetConfiguration(
                dataset_id="ecommerce_full_stack",
                dataset_name="E-commerce Full Stack",
                dataset_type="transactional",
                schema_pattern="oltp",
                domain="ecommerce",
                description="Complete e-commerce dataset with web events and transactions",
            ),
        }

    def get_predefined_config(self, dataset_id: str) -> Optional[DatasetConfiguration]:
        """Get a predefined configuration."""
        return self._predefined_configs.get(dataset_id)

    def get_available_configs(self) -> List[str]:
        """Get list of available predefined configurations."""
        return list(self._predefined_configs.keys())

    def create_config_from_template(
        self, template_id: str, dataset_id: str, overrides: Dict[str, Any] = None
    ) -> DatasetConfiguration:
        """Create a new configuration based on a template."""
        template = self.get_predefined_config(template_id)
        if not template:
            raise ValueError(f"Template '{template_id}' not found")

        # Deep copy the template
        config_dict = template.to_dict()
        config_dict["dataset_id"] = dataset_id
        config_dict["dataset_name"] = f"Custom {dataset_id}"

        # Apply overrides
        if overrides:
            config_dict.update(overrides)

        return DatasetConfiguration.from_dict(config_dict)

    def validate_configuration(self, config: DatasetConfiguration) -> List[str]:
        """Validate a configuration and return any issues."""
        issues = []

        if not config.dataset_id:
            issues.append("Dataset ID is required")

        if not config.table_configs:
            issues.append("At least one table configuration is required")

        for table_name, table_config in config.table_configs.items():
            if table_config.table_type == "snapshot" and table_config.base_rows <= 0:
                issues.append(
                    f"Table '{table_name}': base_rows must be positive for snapshot tables"
                )

            if (
                table_config.table_type == "incremental"
                and table_config.base_rows_per_day <= 0
            ):
                issues.append(
                    f"Table '{table_name}': base_rows_per_day must be positive for incremental tables"
                )

        return issues

    def merge_configurations(
        self, base_config: DatasetConfiguration, override_config: Dict[str, Any]
    ) -> DatasetConfiguration:
        """Merge runtime overrides into a base configuration."""
        merged_config = deepcopy(base_config)
        merged_config.apply_runtime_overrides(override_config)
        return merged_config
