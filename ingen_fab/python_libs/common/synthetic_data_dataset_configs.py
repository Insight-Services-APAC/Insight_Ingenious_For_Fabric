"""
Centralized dataset configurations for synthetic data generation.

This module consolidates all predefined dataset configurations to eliminate duplication
and provide a single source of truth for dataset definitions.
"""

from typing import Dict, Any, Optional


class DatasetConfigurationRepository:
    """Repository for all predefined dataset configurations."""
    
    # Base configurations for different dataset types
    BASE_CONFIGS = {
        "retail_oltp": {
            "dataset_type": "transactional",
            "schema_pattern": "oltp",
            "domain": "retail",
            "tables": ["customers", "products", "orders", "order_items"],
            "relationships": "normalized"
        },
        "retail_star": {
            "dataset_type": "analytical",
            "schema_pattern": "star_schema",
            "domain": "retail",
            "fact_tables": ["fact_sales"],
            "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store"]
        },
        "finance_oltp": {
            "dataset_type": "transactional",
            "schema_pattern": "oltp",
            "domain": "finance",
            "tables": ["customers", "accounts", "transactions", "account_types"],
            "compliance": "pci_dss"
        },
        "ecommerce_star": {
            "dataset_type": "analytical",
            "schema_pattern": "star_schema",
            "domain": "ecommerce",
            "fact_tables": ["fact_web_events", "fact_orders"],
            "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date"]
        },
        "healthcare_oltp": {
            "dataset_type": "transactional",
            "schema_pattern": "oltp",
            "domain": "healthcare",
            "tables": ["patients", "providers", "appointments", "diagnoses", "prescriptions"],
            "compliance": "hipaa"
        }
    }
    
    # Incremental configuration defaults
    DEFAULT_INCREMENTAL_CONFIG = {
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
            "sunday": 0.7
        },
        "growth_rate": 0.001,
        "churn_rate": 0.0005
    }
    
    # Table configurations by schema pattern
    TABLE_CONFIGS_BY_PATTERN = {
        "oltp": {
            "customers": {
                "type": "snapshot",
                "frequency": "daily",
                "growth_enabled": True,
                "churn_enabled": True,
                "base_rows": 10000,
                "daily_growth_rate": 0.002,
                "daily_churn_rate": 0.001
            },
            "products": {
                "type": "snapshot",
                "frequency": "weekly",
                "growth_enabled": True,
                "churn_enabled": False,
                "base_rows": 1000,
                "weekly_growth_rate": 0.01
            },
            "orders": {
                "type": "incremental",
                "frequency": "daily",
                "base_rows_per_day": 10000,
                "seasonal_multipliers_enabled": True,
                "weekend_multiplier": 1.5,
                "holiday_multiplier": 2.0
            },
            "order_items": {
                "type": "incremental",
                "frequency": "daily",
                "base_rows_per_day": 25000,
                "seasonal_multipliers_enabled": True,
                "weekend_multiplier": 1.5
            },
            "transactions": {
                "type": "incremental",
                "frequency": "daily",
                "base_rows_per_day": 50000,
                "seasonal_multipliers_enabled": True,
                "weekend_multiplier": 0.3
            }
        },
        "star_schema": {
            "dim_customer": {
                "type": "snapshot",
                "frequency": "weekly",
                "growth_enabled": True,
                "base_rows": 100000,
                "weekly_growth_rate": 0.005
            },
            "dim_product": {
                "type": "snapshot",
                "frequency": "monthly",
                "growth_enabled": True,
                "base_rows": 10000,
                "monthly_growth_rate": 0.02
            },
            "dim_store": {
                "type": "snapshot",
                "frequency": "quarterly",
                "growth_enabled": True,
                "base_rows": 1000,
                "quarterly_growth_rate": 0.05
            },
            "dim_date": {
                "type": "snapshot",
                "frequency": "once",
                "base_rows": 3653
            },
            "fact_sales": {
                "type": "incremental",
                "frequency": "daily",
                "base_rows_per_day": 100000,
                "seasonal_multipliers_enabled": True,
                "weekend_multiplier": 1.3,
                "holiday_multiplier": 2.5
            },
            "fact_web_events": {
                "type": "incremental",
                "frequency": "daily",
                "base_rows_per_day": 500000,
                "seasonal_multipliers_enabled": True,
                "weekend_multiplier": 0.8,
                "holiday_multiplier": 2.0
            }
        }
    }
    
    @classmethod
    def get_predefined_dataset(cls, dataset_id: str) -> Dict[str, Any]:
        """Get a predefined dataset configuration by ID."""
        configs = cls._get_all_predefined_datasets()
        if dataset_id not in configs:
            raise ValueError(f"Unknown dataset_id: {dataset_id}. Available: {list(configs.keys())}")
        return configs[dataset_id]
    
    @classmethod
    def _get_all_predefined_datasets(cls) -> Dict[str, Dict[str, Any]]:
        """Get all predefined dataset configurations."""
        return {
            # Retail OLTP variants
            "retail_oltp_small": {
                **cls.BASE_CONFIGS["retail_oltp"],
                "dataset_id": "retail_oltp_small",
                "dataset_name": "Retail OLTP - Small",
                "description": "Small retail transactional system with customers, orders, products",
                "scale": "small",
                "target_rows": 10000
            },
            "retail_oltp_large": {
                **cls.BASE_CONFIGS["retail_oltp"],
                "dataset_id": "retail_oltp_large",
                "dataset_name": "Retail OLTP - Large",
                "description": "Large-scale retail transactional system",
                "scale": "large",
                "target_rows": 10000000,
                "partitioning": "date"
            },
            
            # Retail Star Schema variants
            "retail_star_small": {
                **cls.BASE_CONFIGS["retail_star"],
                "dataset_id": "retail_star_small",
                "dataset_name": "Retail Star Schema - Small",
                "description": "Small retail data warehouse with fact_sales and dimensions",
                "scale": "small",
                "target_rows": 100000
            },
            "retail_star_large": {
                **cls.BASE_CONFIGS["retail_star"],
                "dataset_id": "retail_star_large",
                "dataset_name": "Retail Star Schema - Large",
                "description": "Large retail data warehouse with multiple fact tables",
                "scale": "large",
                "target_rows": 100000000,
                "fact_tables": ["fact_sales", "fact_inventory"],
                "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"]
            },
            
            # Finance OLTP
            "finance_oltp_small": {
                **cls.BASE_CONFIGS["finance_oltp"],
                "dataset_id": "finance_oltp_small",
                "dataset_name": "Financial OLTP - Small",
                "description": "Small financial system with accounts, transactions, customers",
                "scale": "small",
                "target_rows": 50000
            },
            
            # E-commerce Star Schema
            "ecommerce_star_small": {
                **cls.BASE_CONFIGS["ecommerce_star"],
                "dataset_id": "ecommerce_star_small",
                "dataset_name": "E-commerce Star Schema - Small",
                "description": "E-commerce analytics with web events and sales",
                "scale": "small",
                "target_rows": 500000
            },
            
            # Healthcare OLTP
            "healthcare_oltp_small": {
                **cls.BASE_CONFIGS["healthcare_oltp"],
                "dataset_id": "healthcare_oltp_small",
                "dataset_name": "Healthcare OLTP - Small",
                "description": "Healthcare system with patients, appointments, prescriptions",
                "scale": "small",
                "target_rows": 100000
            }
        }
    
    @classmethod
    def get_incremental_config(cls, dataset_id: str, custom_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get incremental configuration for a dataset."""
        base_dataset = cls.get_predefined_dataset(dataset_id)
        incremental_config = cls.DEFAULT_INCREMENTAL_CONFIG.copy()
        
        # Apply domain-specific adjustments
        if base_dataset.get("domain") == "finance":
            incremental_config["seasonal_multipliers"] = {
                "monday": 1.2,
                "tuesday": 1.3,
                "wednesday": 1.3,
                "thursday": 1.2,
                "friday": 1.1,
                "saturday": 0.3,
                "sunday": 0.1
            }
        elif base_dataset.get("domain") == "healthcare":
            incremental_config["seasonal_multipliers"] = {
                "monday": 1.3,
                "tuesday": 1.2,
                "wednesday": 1.2,
                "thursday": 1.2,
                "friday": 1.1,
                "saturday": 0.4,
                "sunday": 0.2
            }
        elif base_dataset.get("domain") == "ecommerce":
            incremental_config["seasonal_multipliers"] = {
                "monday": 0.85,
                "tuesday": 0.90,
                "wednesday": 0.95,
                "thursday": 1.05,
                "friday": 1.40,
                "saturday": 1.30,
                "sunday": 0.75
            }
            incremental_config["holiday_multipliers"] = {
                "black_friday": 3.5,
                "cyber_monday": 3.0,
                "christmas": 2.5
            }
        
        # Apply custom configuration if provided
        if custom_config:
            incremental_config.update(custom_config)
        
        return incremental_config
    
    @classmethod
    def get_table_configs(cls, dataset_id: str, scale_factor: float = 1.0) -> Dict[str, Dict[str, Any]]:
        """Get table configurations for a dataset."""
        base_dataset = cls.get_predefined_dataset(dataset_id)
        schema_pattern = base_dataset.get("schema_pattern", "oltp")
        
        # Get base table configs for the pattern
        table_configs = {}
        pattern_configs = cls.TABLE_CONFIGS_BY_PATTERN.get(schema_pattern, {})
        
        # Add configurations for all tables in the dataset
        if "tables" in base_dataset:
            for table in base_dataset["tables"]:
                if table in pattern_configs:
                    config = pattern_configs[table].copy()
                    # Apply scale factor
                    if "base_rows" in config:
                        config["base_rows"] = int(config["base_rows"] * scale_factor)
                    if "base_rows_per_day" in config:
                        config["base_rows_per_day"] = int(config["base_rows_per_day"] * scale_factor)
                    table_configs[table] = config
        
        # Add dimension tables for star schema
        if "dimensions" in base_dataset:
            for dim in base_dataset["dimensions"]:
                if dim in pattern_configs:
                    config = pattern_configs[dim].copy()
                    if "base_rows" in config:
                        config["base_rows"] = int(config["base_rows"] * scale_factor)
                    table_configs[dim] = config
        
        # Add fact tables for star schema
        if "fact_tables" in base_dataset:
            for fact in base_dataset["fact_tables"]:
                if fact in pattern_configs:
                    config = pattern_configs[fact].copy()
                    if "base_rows_per_day" in config:
                        config["base_rows_per_day"] = int(config["base_rows_per_day"] * scale_factor)
                    table_configs[fact] = config
        
        return table_configs
    
    @classmethod
    def list_available_datasets(cls) -> Dict[str, str]:
        """List all available dataset IDs with descriptions."""
        configs = cls._get_all_predefined_datasets()
        return {
            dataset_id: config.get("description", "No description")
            for dataset_id, config in configs.items()
        }
    
    @classmethod
    def create_custom_config(
        cls,
        dataset_id: str,
        dataset_name: str,
        base_template: str,
        customizations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a custom dataset configuration based on a template."""
        # Start with base template
        base_config = cls.get_predefined_dataset(base_template).copy()
        
        # Update with custom values
        base_config.update({
            "dataset_id": dataset_id,
            "dataset_name": dataset_name,
            **customizations
        })
        
        return base_config