"""Lakehouse persistence implementation using Delta tables."""

import json
from datetime import date, datetime
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..core.enums.profile_types import ScanLevel
from ..core.models.metadata import (
    ScanProgress,
    SchemaMetadata,
    TableMetadata,
)
from ..core.models.profile_models import DatasetProfile
from .base_persistence import BasePersistence


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime and date objects."""
    
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


class LakehousePersistence(BasePersistence):
    """Lakehouse persistence implementation using Delta tables and lakehouse_utils."""
    
    def __init__(self, lakehouse, spark: SparkSession, table_prefix: str = "tiered_profile"):
        """
        Initialize lakehouse persistence.
        
        Args:
            lakehouse: lakehouse_utils instance for storage
            spark: SparkSession for creating DataFrames  
            table_prefix: Prefix for profile result tables
        """
        super().__init__(table_prefix)
        self.lakehouse = lakehouse
        self.spark = spark
        
        # Initialize tables if they don't exist
        self._initialize_tables()
    
    def _initialize_tables(self) -> None:
        """Create Delta tables if they don't exist."""
        
        # Create metadata table if not exists
        if not self.lakehouse.check_if_table_exists(self.metadata_table):
            metadata_schema = StructType([
                StructField("table_name", StringType(), False),
                StructField("table_path", StringType(), False),
                StructField("table_format", StringType(), False),
                StructField("row_count", LongType(), True),
                StructField("size_bytes", LongType(), True),
                StructField("num_files", LongType(), True),
                StructField("created_time", StringType(), True),
                StructField("modified_time", StringType(), True),
                StructField("partition_columns", StringType(), True),
                StructField("properties", StringType(), True),
                StructField("scan_timestamp", TimestampType(), False),
            ])
            df = self.spark.createDataFrame([], metadata_schema)
            self.lakehouse.write_to_table(df, self.metadata_table, mode="overwrite")
            self.logger.info(f"Created table: {self.metadata_table}")
        
        # Create schema table if not exists
        if not self.lakehouse.check_if_table_exists(self.schema_table):
            schema_schema = StructType([
                StructField("table_name", StringType(), False),
                StructField("column_count", IntegerType(), False),
                StructField("columns", StringType(), False),
                StructField("primary_key_candidates", StringType(), False),
                StructField("foreign_key_candidates", StringType(), False),
                StructField("scan_timestamp", TimestampType(), False),
            ])
            df = self.spark.createDataFrame([], schema_schema)
            self.lakehouse.write_to_table(df, self.schema_table, mode="overwrite")
            self.logger.info(f"Created table: {self.schema_table}")
        
        # Create progress table if not exists
        if not self.lakehouse.check_if_table_exists(self.progress_table):
            progress_schema = StructType([
                StructField("table_name", StringType(), False),
                StructField("level_1_completed", TimestampType(), True),
                StructField("level_1_duration_ms", LongType(), True),
                StructField("level_2_completed", TimestampType(), True),
                StructField("level_2_duration_ms", LongType(), True),
                StructField("level_3_completed", TimestampType(), True),
                StructField("level_3_duration_ms", LongType(), True),
                StructField("level_4_completed", TimestampType(), True),
                StructField("level_4_duration_ms", LongType(), True),
                StructField("last_error", StringType(), True),
                StructField("last_error_time", TimestampType(), True),
                StructField("last_updated", TimestampType(), False),
            ])
            df = self.spark.createDataFrame([], progress_schema)
            self.lakehouse.write_to_table(df, self.progress_table, mode="overwrite")
            self.logger.info(f"Created table: {self.progress_table}")
    
    def save_table_metadata(self, metadata: TableMetadata) -> None:
        """Save table metadata to lakehouse."""
        metadata_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("table_path", StringType(), False),
            StructField("table_format", StringType(), False),
            StructField("row_count", LongType(), True),
            StructField("size_bytes", LongType(), True),
            StructField("num_files", LongType(), True),
            StructField("created_time", StringType(), True),
            StructField("modified_time", StringType(), True),
            StructField("partition_columns", StringType(), True),
            StructField("properties", StringType(), True),
            StructField("scan_timestamp", TimestampType(), False),
        ])
        
        # Convert metadata to Row data with proper types
        row_data = [
            metadata.table_name,
            metadata.table_path,
            metadata.table_format,
            metadata.row_count,
            metadata.size_bytes,
            metadata.num_files,
            metadata.created_time,
            metadata.modified_time,
            json.dumps(metadata.partition_columns) if metadata.partition_columns else None,
            json.dumps(metadata.properties) if metadata.properties else None,
            datetime.now()
        ]
        
        # Create DataFrame with single row and upsert
        df = self.spark.createDataFrame([row_data], schema=metadata_schema)
        
        # Use merge/upsert if table has data, otherwise just write
        if self.lakehouse.check_if_table_exists(self.metadata_table):
            try:
                existing_df = self.lakehouse.read_table(self.metadata_table)
                if existing_df.count() > 0:
                    # Remove existing entry for this table
                    filtered_df = existing_df.filter(f"table_name != '{metadata.table_name}'")
                    # Union with new data
                    combined_df = filtered_df.unionByName(df)
                    self.lakehouse.write_to_table(combined_df, self.metadata_table, mode="overwrite")
                else:
                    self.lakehouse.write_to_table(df, self.metadata_table, mode="overwrite")
            except Exception as e:
                self.logger.warning(f"Error during upsert, using overwrite: {e}")
                self.lakehouse.write_to_table(df, self.metadata_table, mode="append")
        else:
            self.lakehouse.write_to_table(df, self.metadata_table, mode="overwrite")
    
    def load_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Load table metadata for a specific table."""
        try:
            df = self.lakehouse.read_table(self.metadata_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
            if result:
                row = result[0].asDict()
                return self._deserialize_metadata(row)
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading table metadata for {table_name}: {e}")
            return None
    
    def save_schema_metadata(self, metadata: SchemaMetadata) -> None:
        """Save schema metadata to lakehouse."""
        schema_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("column_count", IntegerType(), False),
            StructField("columns", StringType(), False),
            StructField("primary_key_candidates", StringType(), False),
            StructField("foreign_key_candidates", StringType(), False),
            StructField("scan_timestamp", TimestampType(), False),
        ])
        
        # Convert to serializable format
        row_data = [
            metadata.table_name,
            metadata.column_count,
            json.dumps(metadata.columns if metadata.columns and isinstance(metadata.columns[0], dict) else [col.to_dict() for col in (metadata.column_metadata or [])]),
            json.dumps(metadata.primary_key_candidates or []),
            json.dumps(metadata.foreign_key_candidates or []),
            datetime.now()
        ]
        
        df = self.spark.createDataFrame([row_data], schema=schema_schema)
        
        # Handle upsert similar to metadata
        if self.lakehouse.check_if_table_exists(self.schema_table):
            try:
                existing_df = self.lakehouse.read_table(self.schema_table)
                if existing_df.count() > 0:
                    filtered_df = existing_df.filter(f"table_name != '{metadata.table_name}'")
                    combined_df = filtered_df.unionByName(df)
                    self.lakehouse.write_to_table(combined_df, self.schema_table, mode="overwrite")
                else:
                    self.lakehouse.write_to_table(df, self.schema_table, mode="overwrite")
            except Exception as e:
                self.logger.warning(f"Error during schema upsert: {e}")
                self.lakehouse.write_to_table(df, self.schema_table, mode="append")
        else:
            self.lakehouse.write_to_table(df, self.schema_table, mode="overwrite")
    
    def load_schema_metadata(self, table_name: str) -> Optional[SchemaMetadata]:
        """Load schema metadata for a specific table."""
        try:
            df = self.lakehouse.read_table(self.schema_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
            if result:
                row = result[0].asDict()
                return self._deserialize_schema_metadata(row)
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading schema metadata for {table_name}: {e}")
            return None
    
    def save_profile(self, profile: DatasetProfile) -> None:
        """Save profile results from Level 3/4 scans."""
        # Create profiles table schema if it doesn't exist
        if not self.lakehouse.check_if_table_exists(self.profile_table):
            profile_schema = StructType([
                StructField("table_name", StringType(), False),
                StructField("profile_data", StringType(), False),
                StructField("scan_timestamp", TimestampType(), False),
            ])
            df = self.spark.createDataFrame([], profile_schema)
            self.lakehouse.write_to_table(df, self.profile_table, mode="overwrite")
        
        # Serialize profile to JSON
        profile_json = json.dumps(self._serialize_profile(profile), cls=DateTimeEncoder)
        
        row_data = [
            profile.dataset_name,
            profile_json,
            datetime.now()
        ]
        
        profile_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("profile_data", StringType(), False),
            StructField("scan_timestamp", TimestampType(), False),
        ])
        
        df = self.spark.createDataFrame([row_data], schema=profile_schema)
        
        # Handle upsert with schema compatibility check
        try:
            existing_df = self.lakehouse.read_table(self.profile_table)
            existing_columns = set(existing_df.columns)
            new_columns = set(df.columns)
            
            # Check if schemas are compatible
            if existing_columns == new_columns:
                # Schemas match, can do normal upsert
                if existing_df.count() > 0:
                    filtered_df = existing_df.filter(f"table_name != '{profile.dataset_name}'")
                    combined_df = filtered_df.unionByName(df)
                    self.lakehouse.write_to_table(combined_df, self.profile_table, mode="overwrite")
                else:
                    self.lakehouse.write_to_table(df, self.profile_table, mode="overwrite")
            else:
                # Schema mismatch - recreate table with new schema
                self.logger.info(f"Schema mismatch detected. Recreating profile table with new schema.")
                self.logger.info(f"Old columns: {existing_columns}")
                self.logger.info(f"New columns: {new_columns}")
                self.lakehouse.write_to_table(df, self.profile_table, mode="overwrite")
        except Exception as e:
            self.logger.warning(f"Error during profile upsert: {e}")
            # Fallback: overwrite the table entirely
            self.lakehouse.write_to_table(df, self.profile_table, mode="overwrite")
    
    def load_profile(self, table_name: str, level: Optional[ScanLevel] = None) -> Optional[DatasetProfile]:
        """Load profile for a specific table and optional scan level."""
        try:
            if not self.lakehouse.check_if_table_exists(self.profile_table):
                return None
                
            df = self.lakehouse.read_table(self.profile_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
            if result:
                row = result[0].asDict()
                profile_data = json.loads(row["profile_data"])
                return self._deserialize_profile(profile_data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading profile for {table_name}: {e}")
            return None
    
    def save_progress(self, progress: ScanProgress) -> None:
        """Save scan progress for a table."""
        progress_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("level_1_completed", TimestampType(), True),
            StructField("level_1_duration_ms", LongType(), True),
            StructField("level_2_completed", TimestampType(), True),
            StructField("level_2_duration_ms", LongType(), True),
            StructField("level_3_completed", TimestampType(), True),
            StructField("level_3_duration_ms", LongType(), True),
            StructField("level_4_completed", TimestampType(), True),
            StructField("level_4_duration_ms", LongType(), True),
            StructField("last_error", StringType(), True),
            StructField("last_error_time", TimestampType(), True),
            StructField("last_updated", TimestampType(), False),
        ])
        
        row_data = [
            progress.table_name,
            progress.level_1_completed,
            progress.level_1_duration_ms,
            progress.level_2_completed,
            progress.level_2_duration_ms,
            progress.level_3_completed,
            progress.level_3_duration_ms,
            progress.level_4_completed,
            progress.level_4_duration_ms,
            progress.last_error,
            progress.last_error_time,
            datetime.now()
        ]
        
        df = self.spark.createDataFrame([row_data], schema=progress_schema)
        
        # Handle upsert
        try:
            existing_df = self.lakehouse.read_table(self.progress_table)
            if existing_df.count() > 0:
                filtered_df = existing_df.filter(f"table_name != '{progress.table_name}'")
                combined_df = filtered_df.unionByName(df)
                self.lakehouse.write_to_table(combined_df, self.progress_table, mode="overwrite")
            else:
                self.lakehouse.write_to_table(df, self.progress_table, mode="overwrite")
        except Exception as e:
            self.logger.warning(f"Error during progress upsert: {e}")
            self.lakehouse.write_to_table(df, self.progress_table, mode="append")
    
    def load_progress(self, table_name: str) -> Optional[ScanProgress]:
        """Load scan progress for a table."""
        try:
            df = self.lakehouse.read_table(self.progress_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
            if result:
                row = result[0].asDict()
                return self._deserialize_progress(row)
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading progress for {table_name}: {e}")
            return None
    
    def list_completed_tables(self, level: ScanLevel) -> List[str]:
        """List all tables that have completed a specific scan level."""
        try:
            df = self.lakehouse.read_table(self.progress_table)
            column_name = self._get_scan_level_column(level)
            
            # Filter tables where the scan level is completed (not null)
            completed_df = df.filter(f"{column_name} IS NOT NULL")
            result = completed_df.select("table_name").collect()
            
            return [row["table_name"] for row in result]
            
        except Exception as e:
            self.logger.error(f"Error listing completed tables for {level}: {e}")
            return []
    
    def clear_table_data(self, table_name: str) -> None:
        """Clear all persisted data for a specific table."""
        tables_to_clear = [
            self.metadata_table,
            self.schema_table, 
            self.profile_table,
            self.progress_table
        ]
        
        for table in tables_to_clear:
            try:
                if self.lakehouse.check_if_table_exists(table):
                    df = self.lakehouse.read_table(table)
                    filtered_df = df.filter(f"table_name != '{table_name}'")
                    self.lakehouse.write_to_table(filtered_df, table, mode="overwrite")
                    self.logger.info(f"Cleared data for {table_name} from {table}")
            except Exception as e:
                self.logger.error(f"Error clearing data from {table}: {e}")