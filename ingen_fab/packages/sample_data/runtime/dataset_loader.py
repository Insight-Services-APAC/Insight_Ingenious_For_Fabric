from __future__ import annotations

import io
import logging
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class DatasetLoader:
    """Base class for loading sample datasets from public repositories."""

    def __init__(self, lakehouse_utils_instance: Any):
        """Initialize the dataset loader with a lakehouse_utils instance.
        
        Args:
            lakehouse_utils_instance: Instance of lakehouse_utils for data persistence
        """
        self.lakehouse_utils = lakehouse_utils_instance
        
    def download_csv(self, url: str, **kwargs) -> pd.DataFrame:
        """Download a CSV file from a URL and return as DataFrame.
        
        Args:
            url: URL to download CSV from
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            DataFrame containing the CSV data
        """
        logger.info(f"Downloading CSV from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, **kwargs)
        logger.info(f"Downloaded {len(df)} rows")
        return df
    
    def download_json(self, url: str, **kwargs) -> pd.DataFrame:
        """Download a JSON file from a URL and return as DataFrame.
        
        Args:
            url: URL to download JSON from
            **kwargs: Additional arguments to pass to pd.json_normalize
            
        Returns:
            DataFrame containing the JSON data
        """
        logger.info(f"Downloading JSON from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        json_data = response.json()
        
        # Handle nested JSON with json_normalize
        if isinstance(json_data, list):
            df = pd.json_normalize(json_data, **kwargs)
        elif isinstance(json_data, dict):
            # If dict has a data key, use that
            if 'data' in json_data:
                df = pd.json_normalize(json_data['data'], **kwargs)
            else:
                df = pd.json_normalize([json_data], **kwargs)
        else:
            df = pd.DataFrame([json_data])
            
        logger.info(f"Downloaded {len(df)} rows")
        return df
    
    def save_to_lakehouse(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        mode: str = "overwrite",
        options: Optional[dict[str, str]] = None
    ) -> None:
        """Save a DataFrame to the lakehouse.
        
        Args:
            df: DataFrame to save
            table_name: Name of the table to save to
            mode: Write mode (overwrite, append, etc.)
            options: Additional options for writing
        """
        try:
            logger.info(f"Saving {len(df)} rows to table: {table_name}")
            
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.lakehouse_utils.spark.createDataFrame(df)
            
            self.lakehouse_utils.write_to_table(
                df=spark_df,
                table_name=table_name,
                mode=mode,
                options=options
            )
            logger.info(f"Successfully saved to {table_name}")
        except Exception as e:
            logger.error(f"Failed to save DataFrame to table {table_name}: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            raise
    
    def load_dataset(
        self, 
        dataset_name: str, 
        url: str, 
        file_type: str = "csv",
        save_to_table: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """Load a dataset from a URL and optionally save to lakehouse.
        
        Args:
            dataset_name: Name for the dataset (used as table name)
            url: URL to download from
            file_type: Type of file (csv or json)
            save_to_table: Whether to save to lakehouse
            **kwargs: Additional arguments for reading the file
            
        Returns:
            DataFrame containing the dataset
        """
        if file_type.lower() == "csv":
            df = self.download_csv(url, **kwargs)
        elif file_type.lower() == "json":
            df = self.download_json(url, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        if save_to_table:
            self.save_to_lakehouse(df, dataset_name)
            
        return df
    
    def load_relational_dataset(
        self,
        dataset_info: Any,  # RelationalDatasetInfo
        prefix: Optional[str] = None,
        save_to_tables: bool = True,
        validate_relationships: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """Load a relational dataset with multiple related tables.
        
        Args:
            dataset_info: RelationalDatasetInfo object with table definitions
            prefix: Optional prefix for table names
            save_to_tables: Whether to save to lakehouse tables
            validate_relationships: Whether to validate foreign key relationships
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        loaded_tables = {}
        failed_tables = []
        
        logger.info(f"Loading relational dataset: {dataset_info.name}")
        logger.info(f"Total tables to load: {len(dataset_info.tables)}")
        
        # Load tables in dependency order
        for table_name in dataset_info.load_order:
            if table_name not in dataset_info.tables:
                logger.warning(f"Table {table_name} in load_order but not in tables definition")
                continue
                
            table_info = dataset_info.tables[table_name]
            actual_table_name = f"{prefix}_{table_name}" if prefix else table_name
            
            try:
                logger.info(f"Loading table: {table_name} ({table_info.description})")
                
                # Download the CSV
                read_options = table_info.read_options or {}
                df = self.download_csv(table_info.url, **read_options)
                
                # Apply column names if specified
                if table_info.columns:
                    df.columns = table_info.columns
                
                # Store in dictionary
                loaded_tables[table_name] = df
                
                # Validate relationships if requested
                if validate_relationships and table_info.foreign_keys:
                    for fk_column, ref_table in table_info.foreign_keys.items():
                        if ref_table in loaded_tables:
                            if fk_column in df.columns:
                                # Check if foreign key values exist in referenced table
                                ref_df = loaded_tables[ref_table]
                                if table_info.primary_key and table_info.primary_key in ref_df.columns:
                                    invalid_fks = ~df[fk_column].isin(ref_df[table_info.primary_key])
                                    if invalid_fks.any():
                                        logger.warning(
                                            f"Found {invalid_fks.sum()} invalid foreign keys in "
                                            f"{table_name}.{fk_column} -> {ref_table}"
                                        )
                
                # Save to lakehouse if requested
                if save_to_tables:
                    self.save_to_lakehouse(df, actual_table_name)
                    
                logger.info(f"Successfully loaded {table_name}: {len(df)} rows")
                
            except Exception as e:
                logger.error(f"Failed to load table {table_name}: {e}")
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
                failed_tables.append(table_name)
                loaded_tables[table_name] = None
        
        # Summary
        successful = len([t for t in loaded_tables.values() if t is not None])
        logger.info(f"Loaded {successful}/{len(dataset_info.tables)} tables successfully")
        
        if failed_tables:
            logger.warning(f"Failed tables: {', '.join(failed_tables)}")
        
        return loaded_tables