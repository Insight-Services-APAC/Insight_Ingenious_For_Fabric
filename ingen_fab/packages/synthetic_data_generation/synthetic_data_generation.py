"""
Synthetic Data Generation Package Compiler

This module provides the main compiler for the synthetic data generation package,
extending BaseNotebookCompiler to generate notebooks for creating synthetic datasets.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

from ...notebook_utils.base_notebook_compiler import BaseNotebookCompiler


class SyntheticDataGenerationCompiler(BaseNotebookCompiler):
    """Compiler for synthetic data generation notebooks and configurations."""
    
    def __init__(self, fabric_workspace_repo_dir: str = None, fabric_environment: str = None, **kwargs):
        """Initialize the synthetic data generation compiler."""
        # Set package-specific defaults
        package_templates_dir = Path(__file__).parent / "templates"
        
        # Store fabric_environment for later use but don't pass to parent
        self.fabric_environment = fabric_environment
        
        super().__init__(
            templates_dir=package_templates_dir,
            package_name="synthetic_data_generation",
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            **kwargs
        )
    
    def compile_synthetic_data_generation_notebook(
        self,
        dataset_config: Dict[str, Any],
        target_environment: str = "lakehouse",  # "lakehouse" or "warehouse"
        generation_mode: str = "auto",  # "python", "pyspark", or "auto"
        output_subdir: str = None
    ) -> Path:
        """
        Compile a synthetic data generation notebook based on dataset configuration.
        
        Args:
            dataset_config: Configuration dictionary for the dataset
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            output_subdir: Optional subdirectory for output
            
        Returns:
            Path to the generated notebook
        """
        # Auto-select generation mode based on target rows
        if generation_mode == "auto":
            target_rows = dataset_config.get("target_rows", 10000)
            generation_mode = "pyspark" if target_rows > 1000000 else "python"
        
        # Override generation mode for warehouse (always use python)
        if target_environment == "warehouse":
            generation_mode = "python"
        
        # Select template based on generation mode and environment
        if generation_mode == "pyspark":
            template_name = "synthetic_data_lakehouse_notebook.py.jinja"
            language_group = "synapse_pyspark"
        else:
            # Use warehouse template for python mode regardless of environment
            # This ensures proper library imports
            template_name = "synthetic_data_warehouse_notebook.py.jinja"
            language_group = "python"
        
        target_datastore_config_prefix = "config"
        if target_environment == "warehouse":
            target_datastore_config_prefix = "config_wh"

        # Prepare template variables
        template_vars = {
            "target_lakehouse_config_prefix": target_datastore_config_prefix,
            "dataset_config": dataset_config,
            "generation_mode": generation_mode,
            "target_environment": target_environment,
            "language_group": language_group,
            "dataset_id": dataset_config.get("dataset_id", "custom_dataset"),
            "target_rows": dataset_config.get("target_rows", 10000),
            "chunk_size": dataset_config.get("chunk_size", 1000000),
            "seed_value": dataset_config.get("seed_value"),
            "parallel_workers": dataset_config.get("parallel_workers", 1)
        }
        
        # Generate notebook name
        dataset_id = dataset_config.get("dataset_id", "custom")
        scale = "large" if template_vars["target_rows"] > 1000000 else "small"
        notebook_name = f"synthetic_data_{dataset_id}_{scale}_{generation_mode}"
        
        # Compile notebook
        # Set default output_subdir if not provided
        if output_subdir is None:
            output_subdir = "synthetic_data_generation"
            
        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=f"Synthetic Data Generation - {dataset_config.get('dataset_name', dataset_id)}",
            description=f"Generate synthetic data for {dataset_id} using {generation_mode}",
            output_subdir=output_subdir
        )
    
    def compile_predefined_dataset_notebook(
        self,
        dataset_id: str,
        target_rows: int,
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        seed_value: Optional[int] = None,
        output_subdir: str = None
    ) -> Path:
        """
        Compile a notebook for a predefined dataset configuration.
        
        Args:
            dataset_id: ID of the predefined dataset
            target_rows: Number of rows to generate
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            seed_value: Optional seed for reproducible generation
            output_subdir: Optional subdirectory for output
            
        Returns:
            Path to the generated notebook
        """
        # Load predefined dataset configurations
        predefined_configs = self._get_predefined_dataset_configs()
        
        if dataset_id not in predefined_configs:
            raise ValueError(f"Unknown dataset_id: {dataset_id}. Available: {list(predefined_configs.keys())}")
        
        # Create dataset configuration
        base_config = predefined_configs[dataset_id]
        dataset_config = {
            **base_config,
            "target_rows": target_rows,
            "seed_value": seed_value,
            "chunk_size": min(target_rows, 1000000) if target_rows > 1000000 else target_rows
        }
        
        return self.compile_synthetic_data_generation_notebook(
            dataset_config=dataset_config,
            target_environment=target_environment,
            generation_mode=generation_mode,
            output_subdir=output_subdir
        )
    
    def compile_ddl_scripts(self, target_environment: str = "warehouse") -> Dict[str, List[Path]]:
        """
        Compile DDL scripts for synthetic data generation configuration tables.
        
        Args:
            target_environment: Target environment ("warehouse" or "lakehouse")
            
        Returns:
            Dictionary mapping target directories to lists of copied files
        """
        ddl_source_dir = Path(__file__).parent / "ddl_scripts" / target_environment
        ddl_output_base = self.output_dir / "ddl_scripts" / target_environment
        
        if target_environment == "warehouse":
            # Copy SQL files in order
            script_mappings = {
                "": [  # Root ddl_scripts directory
                    ("001_config_synthetic_data_datasets.sql", "001_config_synthetic_data_datasets.sql"),
                    ("002_config_synthetic_data_generation_jobs.sql", "002_config_synthetic_data_generation_jobs.sql"),
                    ("003_log_synthetic_data_generation.sql", "003_log_synthetic_data_generation.sql"),
                    ("004_sample_dataset_configurations.sql", "004_sample_dataset_configurations.sql")
                ]
            }
        else:
            # Lakehouse DDL scripts (Python files)
            script_mappings = {
                "": [
                    ("001_config_synthetic_data_datasets.py", "001_config_synthetic_data_datasets.py"),
                    ("002_config_synthetic_data_generation_jobs.py", "002_config_synthetic_data_generation_jobs.py"),
                    ("003_log_synthetic_data_generation.py", "003_log_synthetic_data_generation.py"),
                    ("004_sample_dataset_configurations.py", "004_sample_dataset_configurations.py")
                ]
            }
        
        return super().compile_ddl_scripts(ddl_source_dir, ddl_output_base, script_mappings)
    
    def compile_all_synthetic_data_notebooks(
        self,
        datasets: List[Dict[str, Any]] = None,
        target_environment: str = "lakehouse"
    ) -> Dict[str, Any]:
        """
        Compile notebooks for multiple synthetic datasets.
        
        Args:
            datasets: List of dataset configurations. If None, uses predefined datasets.
            target_environment: Target environment ("lakehouse" or "warehouse")
            
        Returns:
            Dictionary with compilation results
        """
        if datasets is None:
            # Use sample predefined datasets
            predefined = self._get_predefined_dataset_configs()
            datasets = [
                {**config, "target_rows": 10000 if "small" in config["dataset_id"] else 1000000}
                for config in predefined.values()
                if "small" in config["dataset_id"]  # Only compile small samples by default
            ]
        
        compile_functions = []
        
        # Add DDL compilation
        compile_functions.append((
            self.compile_ddl_scripts,
            [target_environment],
            {}
        ))
        
        # Add notebook compilations
        for dataset_config in datasets:
            compile_functions.append((
                self.compile_synthetic_data_generation_notebook,
                [dataset_config, target_environment],
                {"output_subdir": f"synthetic_data_generation/{dataset_config['dataset_id']}"}
            ))
        
        return self.compile_all_with_results(
            compile_functions,
            header_title=f"🎲 Compiling Synthetic Data Generation Package for {target_environment.title()}"
        )
    
    def _get_predefined_dataset_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get predefined dataset configurations."""
        return {
            "retail_oltp_small": {
                "dataset_id": "retail_oltp_small",
                "dataset_name": "Retail OLTP - Small",
                "dataset_type": "transactional",
                "schema_pattern": "oltp",
                "domain": "retail",
                "description": "Small retail transactional system with customers, orders, products",
                "tables": ["customers", "products", "orders", "order_items"],
                "relationships": "normalized"
            },
            "retail_oltp_large": {
                "dataset_id": "retail_oltp_large",
                "dataset_name": "Retail OLTP - Large",
                "dataset_type": "transactional",
                "schema_pattern": "oltp",
                "domain": "retail",
                "description": "Large-scale retail transactional system",
                "tables": ["customers", "products", "orders", "order_items"],
                "relationships": "normalized",
                "partitioning": "date"
            },
            "retail_star_small": {
                "dataset_id": "retail_star_small",
                "dataset_name": "Retail Star Schema - Small",
                "dataset_type": "analytical",
                "schema_pattern": "star_schema",
                "domain": "retail",
                "description": "Small retail data warehouse with fact_sales and dimensions",
                "fact_tables": ["fact_sales"],
                "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store"]
            },
            "retail_star_large": {
                "dataset_id": "retail_star_large",
                "dataset_name": "Retail Star Schema - Large",
                "dataset_type": "analytical",
                "schema_pattern": "star_schema",
                "domain": "retail",
                "description": "Large retail data warehouse with multiple fact tables",
                "fact_tables": ["fact_sales", "fact_inventory"],
                "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"]
            },
            "finance_oltp_small": {
                "dataset_id": "finance_oltp_small",
                "dataset_name": "Financial OLTP - Small",
                "dataset_type": "transactional",
                "schema_pattern": "oltp",
                "domain": "finance",
                "description": "Small financial system with accounts, transactions, customers",
                "tables": ["customers", "accounts", "transactions", "account_types"],
                "compliance": "pci_dss"
            },
            "ecommerce_star_small": {
                "dataset_id": "ecommerce_star_small",
                "dataset_name": "E-commerce Star Schema - Small",
                "dataset_type": "analytical",
                "schema_pattern": "star_schema",
                "domain": "ecommerce",
                "description": "E-commerce analytics with web events and sales",
                "fact_tables": ["fact_web_events", "fact_orders"],
                "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date"]
            }
        }