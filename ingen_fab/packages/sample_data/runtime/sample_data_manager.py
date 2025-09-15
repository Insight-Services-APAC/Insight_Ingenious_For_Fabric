from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .dataset_loader import DatasetLoader
from .dataset_registry import DatasetRegistry
from .relational_datasets import RelationalDatasetRegistry

logger = logging.getLogger(__name__)


class SampleDataManager:
    """Manager for loading and managing sample datasets in Fabric lakehouse."""
    
    def __init__(self, lakehouse_utils_instance: Any):
        """Initialize the sample data manager.
        
        Args:
            lakehouse_utils_instance: Instance of lakehouse_utils for data persistence
        """
        self.loader = DatasetLoader(lakehouse_utils_instance)
        self.registry = DatasetRegistry()
        self.relational_registry = RelationalDatasetRegistry()
        self.lakehouse_utils = lakehouse_utils_instance
        
    def load_dataset(self, name: str, save_to_table: bool = True) -> pd.DataFrame:
        """Load a dataset from the registry.
        
        Args:
            name: Name of the dataset to load
            save_to_table: Whether to save to lakehouse table
            
        Returns:
            DataFrame containing the dataset
        """
        dataset_info = self.registry.get_dataset(name)
        
        logger.info(f"Loading dataset: {name}")
        logger.info(f"Description: {dataset_info.description}")
        logger.info(f"Source: {dataset_info.source}")
        
        read_options = dataset_info.read_options or {}
        
        df = self.loader.load_dataset(
            dataset_name=name,
            url=dataset_info.url,
            file_type=dataset_info.file_type,
            save_to_table=save_to_table,
            **read_options
        )
        
        # Apply column names if specified and not already present
        if dataset_info.columns and len(dataset_info.columns) == len(df.columns):
            if list(df.columns) != dataset_info.columns:
                df.columns = dataset_info.columns
                
                # Re-save with proper column names if saved to table
                if save_to_table:
                    self.loader.save_to_lakehouse(df, name)
        
        return df
    
    def load_multiple_datasets(
        self, 
        names: List[str], 
        save_to_tables: bool = True
    ) -> dict[str, pd.DataFrame]:
        """Load multiple datasets from the registry.
        
        Args:
            names: List of dataset names to load
            save_to_tables: Whether to save to lakehouse tables
            
        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        results = {}
        
        for name in names:
            try:
                df = self.load_dataset(name, save_to_table=save_to_tables)
                results[name] = df
                logger.info(f"Successfully loaded {name}: {len(df)} rows")
            except Exception as e:
                logger.error(f"Failed to load {name}: {e}")
                results[name] = None
                
        return results
    
    def load_all_from_source(
        self, 
        source: str, 
        save_to_tables: bool = True
    ) -> dict[str, pd.DataFrame]:
        """Load all datasets from a specific source.
        
        Args:
            source: Source name to load datasets from
            save_to_tables: Whether to save to lakehouse tables
            
        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        dataset_names = self.registry.get_datasets_by_source(source)
        
        if not dataset_names:
            logger.warning(f"No datasets found for source: {source}")
            return {}
            
        logger.info(f"Loading {len(dataset_names)} datasets from {source}")
        return self.load_multiple_datasets(dataset_names, save_to_tables)
    
    def list_available_datasets(self) -> pd.DataFrame:
        """Get a DataFrame of all available datasets with their information.
        
        Returns:
            DataFrame with dataset information
        """
        info_table = self.registry.get_dataset_info_table()
        return pd.DataFrame(info_table)
    
    def check_dataset_exists(self, name: str) -> bool:
        """Check if a dataset already exists in the lakehouse.
        
        Args:
            name: Name of the dataset/table
            
        Returns:
            True if the dataset exists in lakehouse
        """
        return self.lakehouse_utils.check_if_table_exists(name)
    
    def refresh_dataset(self, name: str) -> pd.DataFrame:
        """Refresh a dataset by re-downloading and overwriting.
        
        Args:
            name: Name of the dataset to refresh
            
        Returns:
            DataFrame containing the refreshed dataset
        """
        logger.info(f"Refreshing dataset: {name}")
        return self.load_dataset(name, save_to_table=True)
    
    def load_custom_dataset(
        self,
        name: str,
        url: str,
        file_type: str = "csv",
        description: Optional[str] = None,
        save_to_table: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """Load a custom dataset not in the registry.
        
        Args:
            name: Name for the dataset
            url: URL to download from
            file_type: Type of file (csv or json)
            description: Optional description
            save_to_table: Whether to save to lakehouse
            **kwargs: Additional arguments for reading the file
            
        Returns:
            DataFrame containing the dataset
        """
        logger.info(f"Loading custom dataset: {name}")
        if description:
            logger.info(f"Description: {description}")
            
        return self.loader.load_dataset(
            dataset_name=name,
            url=url,
            file_type=file_type,
            save_to_table=save_to_table,
            **kwargs
        )
    
    def load_relational_dataset(
        self,
        name: str,
        prefix: Optional[str] = None,
        save_to_tables: bool = True,
        validate_relationships: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """Load a relational dataset with multiple related tables.
        
        Args:
            name: Name of the relational dataset
            prefix: Optional prefix for table names
            save_to_tables: Whether to save to lakehouse tables
            validate_relationships: Whether to validate foreign key relationships
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        dataset_info = self.relational_registry.get_dataset(name)
        
        logger.info(f"Loading relational dataset: {name}")
        logger.info(f"Description: {dataset_info.description}")
        logger.info(f"Source: {dataset_info.source}")
        logger.info(f"Tables: {len(dataset_info.tables)}")
        
        return self.loader.load_relational_dataset(
            dataset_info=dataset_info,
            prefix=prefix,
            save_to_tables=save_to_tables,
            validate_relationships=validate_relationships
        )
    
    def list_relational_datasets(self) -> pd.DataFrame:
        """Get a DataFrame of all available relational datasets with their information.
        
        Returns:
            DataFrame with relational dataset information
        """
        info_table = self.relational_registry.get_dataset_info_table()
        return pd.DataFrame(info_table)
    
    def get_relational_schema(self, name: str) -> pd.DataFrame:
        """Get schema information for a relational dataset.
        
        Args:
            name: Name of the relational dataset
            
        Returns:
            DataFrame with table and relationship information
        """
        dataset_info = self.relational_registry.get_dataset(name)
        
        schema_data = []
        for table_name, table_info in dataset_info.tables.items():
            schema_data.append({
                "table": table_name,
                "description": table_info.description,
                "primary_key": table_info.primary_key,
                "foreign_keys": ", ".join([f"{col}->{ref}" for col, ref in table_info.foreign_keys.items()]) if table_info.foreign_keys else None
            })
        
        return pd.DataFrame(schema_data)