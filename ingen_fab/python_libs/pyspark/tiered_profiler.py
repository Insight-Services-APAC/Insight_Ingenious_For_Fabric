"""
Tiered Data Profiler for PySpark

This module provides a tiered profiling system with 4 scan levels:
1. Table Discovery - Fast metadata scan to discover and document delta tables
2. Column Discovery - Metadata scan to capture column names, numbers and types
3. Column Profiling - Single-pass detailed column profile information
4. Advanced Profiling - Multi-pass analysis including relationships and patterns

Each scan level builds upon the previous ones, allowing for progressive enhancement
of data profiles while maintaining the ability to restart from any level.
"""

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField
from delta.tables import DeltaTable

from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
from ingen_fab.python_libs.interfaces.data_profiling_interface import (
    ColumnProfile,
    DatasetProfile,
    ProfileType,
    SemanticType,
    ValueStatistics,
    NamingPattern,
)


class ScanLevel(Enum):
    """Enumeration of scan levels for tiered profiling."""
    LEVEL_1_DISCOVERY = "level_1_discovery"  # Table discovery only
    LEVEL_2_SCHEMA = "level_2_schema"  # Column metadata
    LEVEL_3_PROFILE = "level_3_profile"  # Single-pass profiling
    LEVEL_4_ADVANCED = "level_4_advanced"  # Multi-pass advanced


@dataclass
class TableMetadata:
    """Metadata for a discovered table."""
    table_name: str
    table_path: str
    table_format: str = "delta"
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    num_files: Optional[int] = None
    created_time: Optional[str] = None
    modified_time: Optional[str] = None
    partition_columns: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return asdict(self)


@dataclass
class SchemaMetadata:
    """Schema metadata for a table."""
    table_name: str
    column_count: int
    columns: List[Dict[str, str]] = field(default_factory=list)  # [{name, type, nullable}]
    primary_key_candidates: List[str] = field(default_factory=list)
    foreign_key_candidates: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return asdict(self)


@dataclass
class ScanProgress:
    """Track scan progress for a table."""
    table_name: str
    level_1_completed: Optional[datetime] = None
    level_1_duration_ms: Optional[int] = None
    level_2_completed: Optional[datetime] = None
    level_2_duration_ms: Optional[int] = None
    level_3_completed: Optional[datetime] = None
    level_3_duration_ms: Optional[int] = None
    level_4_completed: Optional[datetime] = None
    level_4_duration_ms: Optional[int] = None
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        result = {}
        for key, value in asdict(self).items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScanProgress':
        """Create from dictionary."""
        for key in ['level_1_completed', 'level_2_completed', 
                   'level_3_completed', 'level_4_completed',
                   'last_error_time']:
            if key in data and data[key]:
                data[key] = datetime.fromisoformat(data[key])
        return cls(**data)


class ProfilePersistence:
    """Handle persistence of profiling results and progress using lakehouse_utils."""
    
    def __init__(self, lakehouse: lakehouse_utils, spark: SparkSession, table_prefix: str = "tiered_profile"):
        """
        Initialize persistence layer using lakehouse storage.
        
        Args:
            lakehouse: lakehouse_utils instance for storage
            spark: SparkSession for creating DataFrames
            table_prefix: Prefix for profile result tables
        """
        self.lakehouse = lakehouse
        self.spark = spark
        self.table_prefix = table_prefix
        
        # Table names for different scan levels
        self.metadata_table = f"{table_prefix}_metadata"
        self.schema_table = f"{table_prefix}_schemas"
        self.profile_table = f"{table_prefix}_profiles"
        self.progress_table = f"{table_prefix}_progress"
        
        # Initialize tables if they don't exist
        self._initialize_tables()
    
    def _initialize_tables(self) -> None:
        """Create Delta tables if they don't exist."""
        from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
        
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
            print(f"  ✅ Created table: {self.metadata_table}")
        
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
            print(f"  ✅ Created table: {self.schema_table}")
        
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
            print(f"  ✅ Created table: {self.progress_table}")
    
    def save_table_metadata(self, metadata: TableMetadata) -> None:
        """Save table metadata to lakehouse."""
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        
        # Define the schema explicitly
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
        
        # Create DataFrame with explicit schema
        df = self.spark.createDataFrame([row_data], metadata_schema)
        
        # Merge with existing data (upsert)
        try:
            existing_df = self.lakehouse.read_table(self.metadata_table)
            merged_df = existing_df.filter(F.col("table_name") != metadata.table_name).union(df)
            self.lakehouse.write_to_table(merged_df, self.metadata_table, mode="overwrite")
        except Exception:
            # If table is empty or has issues, just write the new row
            self.lakehouse.write_to_table(df, self.metadata_table, mode="append")
    
    def load_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Load table metadata from lakehouse."""
        try:
            df = self.lakehouse.read_table(self.metadata_table)
            result = df.filter(F.col("table_name") == table_name).collect()
            
            if result:
                row = result[0]
                return TableMetadata(
                    table_name=row.table_name,
                    table_path=row.table_path,
                    table_format=row.table_format,
                    row_count=row.row_count,
                    size_bytes=row.size_bytes,
                    num_files=row.num_files,
                    created_time=row.created_time,
                    modified_time=row.modified_time,
                    partition_columns=json.loads(row.partition_columns) if row.partition_columns else [],
                    properties=json.loads(row.properties) if row.properties else {}
                )
        except Exception as e:
            print(f"  ⚠️  Could not load metadata for {table_name}: {e}")
        return None
    
    def save_schema_metadata(self, schema: SchemaMetadata) -> None:
        """Save schema metadata to lakehouse."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
        
        # Define schema explicitly
        schema_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("column_count", IntegerType(), False),
            StructField("columns", StringType(), False),
            StructField("primary_key_candidates", StringType(), False),
            StructField("foreign_key_candidates", StringType(), False),
            StructField("scan_timestamp", TimestampType(), False),
        ])
        
        # Convert schema to row data
        row_data = [
            schema.table_name,
            schema.column_count,
            json.dumps(schema.columns),
            json.dumps(schema.primary_key_candidates),
            json.dumps(schema.foreign_key_candidates),
            datetime.now()
        ]
        
        # Create DataFrame with explicit schema
        df = self.spark.createDataFrame([row_data], schema_schema)
        
        # Merge with existing data (upsert)
        try:
            existing_df = self.lakehouse.read_table(self.schema_table)
            merged_df = existing_df.filter(F.col("table_name") != schema.table_name).union(df)
            self.lakehouse.write_to_table(merged_df, self.schema_table, mode="overwrite")
        except Exception:
            # If table is empty or has issues, just write the new row
            self.lakehouse.write_to_table(df, self.schema_table, mode="append")
    
    def load_schema_metadata(self, table_name: str) -> Optional[SchemaMetadata]:
        """Load schema metadata from lakehouse."""
        try:
            df = self.lakehouse.read_table(self.schema_table)
            result = df.filter(F.col("table_name") == table_name).collect()
            
            if result:
                row = result[0]
                return SchemaMetadata(
                    table_name=row.table_name,
                    column_count=row.column_count,
                    columns=json.loads(row.columns),
                    primary_key_candidates=json.loads(row.primary_key_candidates),
                    foreign_key_candidates=json.loads(row.foreign_key_candidates)
                )
        except Exception as e:
            print(f"  ⚠️  Could not load schema for {table_name}: {e}")
        return None
    
    def save_progress(self, progress: ScanProgress) -> None:
        """Save scan progress to lakehouse."""
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        
        # Define schema explicitly
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
        
        # Convert progress to row data
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
        
        # Create DataFrame with explicit schema
        df = self.spark.createDataFrame([row_data], progress_schema)
        
        # Merge with existing data (upsert)
        try:
            existing_df = self.lakehouse.read_table(self.progress_table)
            merged_df = existing_df.filter(F.col("table_name") != progress.table_name).union(df)
            self.lakehouse.write_to_table(merged_df, self.progress_table, mode="overwrite")
        except Exception:
            # If table is empty or has issues, just write the new row
            self.lakehouse.write_to_table(df, self.progress_table, mode="append")
    
    def load_progress(self, table_name: str) -> Optional[ScanProgress]:
        """Load scan progress from lakehouse."""
        try:
            df = self.lakehouse.read_table(self.progress_table)
            result = df.filter(F.col("table_name") == table_name).collect()
            
            if result:
                row = result[0]
                return ScanProgress(
                    table_name=row.table_name,
                    level_1_completed=row.level_1_completed,
                    level_1_duration_ms=row.level_1_duration_ms,
                    level_2_completed=row.level_2_completed,
                    level_2_duration_ms=row.level_2_duration_ms,
                    level_3_completed=row.level_3_completed,
                    level_3_duration_ms=row.level_3_duration_ms,
                    level_4_completed=row.level_4_completed,
                    level_4_duration_ms=row.level_4_duration_ms,
                    last_error=row.last_error,
                    last_error_time=row.last_error_time
                )
        except Exception as e:
            print(f"  ⚠️  Could not load progress for {table_name}: {e}")
        return None
    
    def list_completed_tables(self, scan_level: ScanLevel) -> List[str]:
        """List tables that have completed a specific scan level."""
        completed = []
        try:
            df = self.lakehouse.read_table(self.progress_table)
            
            if scan_level == ScanLevel.LEVEL_1_DISCOVERY:
                completed_df = df.filter(F.col("level_1_completed").isNotNull())
            elif scan_level == ScanLevel.LEVEL_2_SCHEMA:
                completed_df = df.filter(F.col("level_2_completed").isNotNull())
            elif scan_level == ScanLevel.LEVEL_3_PROFILE:
                completed_df = df.filter(F.col("level_3_completed").isNotNull())
            elif scan_level == ScanLevel.LEVEL_4_ADVANCED:
                completed_df = df.filter(F.col("level_4_completed").isNotNull())
            else:
                return completed
            
            completed = [row.table_name for row in completed_df.select("table_name").collect()]
        except Exception as e:
            print(f"  ⚠️  Could not list completed tables: {e}")
        
        return completed


class TieredProfiler:
    """
    Tiered data profiler with 4 scan levels for progressive profiling.
    
    This profiler allows for incremental discovery and profiling of lakehouse
    tables with the ability to restart from any level.
    """
    
    def __init__(
        self,
        lakehouse: Optional[lakehouse_utils] = None,
        table_prefix: str = "tiered_profile",
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the tiered profiler.
        
        Args:
            lakehouse: Optional lakehouse_utils instance
            table_prefix: Prefix for profile result tables in lakehouse
            spark: Optional SparkSession (will be created from lakehouse if not provided)
        """
        if lakehouse:
            self.lakehouse = lakehouse
            self.spark = lakehouse.get_connection
        elif spark:
            self.spark = spark
            # Create a minimal lakehouse_utils for local testing
            import ingen_fab.python_libs.common.config_utils as cu
            configs = cu.get_configs_as_object()
            self.lakehouse = lakehouse_utils(
                target_workspace_id=configs.config_workspace_id,
                target_lakehouse_id=configs.config_lakehouse_id,
                spark=spark
            )
        else:
            # Create a local Spark session and lakehouse_utils
            from delta import configure_spark_with_delta_pip
            builder = (
                SparkSession.builder.appName("TieredProfiler")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", 
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            
            import ingen_fab.python_libs.common.config_utils as cu
            configs = cu.get_configs_as_object()
            self.lakehouse = lakehouse_utils(
                target_workspace_id=configs.config_workspace_id,
                target_lakehouse_id=configs.config_lakehouse_id,
                spark=self.spark
            )
        
        # Initialize persistence with lakehouse
        self.persistence = ProfilePersistence(self.lakehouse, self.spark, table_prefix)
    
    def scan_level_1_discovery(
        self,
        table_paths: Optional[List[str]] = None,
        resume: bool = True
    ) -> List[TableMetadata]:
        """
        Level 1 Scan: Fast discovery of delta tables (metadata only).
        
        This scan discovers tables and captures basic metadata without
        reading any data.
        
        Args:
            table_paths: Optional list of specific table paths to scan
            resume: Whether to skip already scanned tables
            
        Returns:
            List of TableMetadata objects for discovered tables
        """
        print("\n" + "="*60)
        print("🔍 LEVEL 1 SCAN: Table Discovery (Metadata Only)")
        print("="*60)
        
        discovered_tables = []
        
        # Discover tables
        if table_paths:
            tables_to_scan = table_paths
        else:
            tables_to_scan = self._discover_delta_tables()
        
        # Filter out already scanned tables if resuming
        if resume:
            completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_1_DISCOVERY)
            tables_to_scan = [t for t in tables_to_scan if self._extract_table_name(t) not in completed]
            if completed:
                print(f"ℹ️  Skipping {len(completed)} already scanned tables")
        
        print(f"📊 Found {len(tables_to_scan)} tables to scan")
        
        for table_path in tables_to_scan:
            table_name = self._extract_table_name(table_path)
            print(f"\n  Scanning table: {table_name}")
            
            start_time = time.time()
            progress = self.persistence.load_progress(table_name) or ScanProgress(table_name=table_name)
            
            try:
                # Get Delta table metadata without reading data
                metadata = self._extract_table_metadata(table_path, table_name)
                
                # Save metadata
                self.persistence.save_table_metadata(metadata)
                discovered_tables.append(metadata)
                
                # Update progress
                duration_ms = int((time.time() - start_time) * 1000)
                progress.level_1_completed = datetime.now()
                progress.level_1_duration_ms = duration_ms
                self.persistence.save_progress(progress)
                
                print(f"    ✅ Completed in {duration_ms}ms")
                if metadata.row_count:
                    print(f"    📈 Rows: {metadata.row_count:,}")
                if metadata.num_files:
                    print(f"    📁 Files: {metadata.num_files}")
                
            except Exception as e:
                import traceback
                error_details = f"{str(e)}\nFile: {__file__}\nLine: {traceback.extract_tb(e.__traceback__)[-1].lineno}\n{traceback.format_exc()}"
                print(f"    ❌ Error in Level 1 scan for table '{table_name}': {str(e)}")
                print(f"    📍 Location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
                print(f"    🔍 Full traceback: {traceback.format_exc()}")
                progress.last_error = error_details
                progress.last_error_time = datetime.now()
                self.persistence.save_progress(progress)
        
        print(f"\n✨ Level 1 scan complete. Discovered {len(discovered_tables)} tables")
        return discovered_tables
    
    def _discover_delta_tables(self) -> List[str]:
        """Discover all Delta tables in the lakehouse using lakehouse_utils."""
        try:
            # Use lakehouse_utils list_tables method - much simpler!
            table_names = self.lakehouse.list_tables()
            
            # Convert table names to full paths for consistency
            tables_uri = self.lakehouse.lakehouse_tables_uri()
            tables = [f"{tables_uri}{table_name}" for table_name in table_names]
            
            return tables
        except Exception as e:
            print(f"  ⚠️  Could not discover tables using lakehouse_utils: {e}")
            return []
    
    def _extract_table_name(self, table_path: str) -> str:
        """Extract table name from path."""
        if "/" in table_path:
            return table_path.rstrip("/").split("/")[-1]
        return table_path
    
    def _extract_table_metadata(self, table_path: str, table_name: str) -> TableMetadata:
        """Extract metadata for a Delta table without reading data."""
        metadata = TableMetadata(
            table_name=table_name,
            table_path=table_path
        )
        
        try:
            # Check if table exists using lakehouse_utils
            if not self.lakehouse.check_if_table_exists(table_name):
                print(f"    ⚠️  Table {table_name} does not exist in lakehouse")
                return metadata
            
            # Get Delta table details directly
            if "file://" in table_path or "/" in table_path:
                delta_table = DeltaTable.forPath(self.spark, self.lakehouse.lakehouse_tables_uri() + table_name)
            else:
                delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Get table details using Delta API
            details = delta_table.detail().collect()[0]
            # Helper for tolerant attribute access with warning
            def attr_with_warning(obj, attr, default=None):
                if not hasattr(obj, attr) or getattr(obj, attr) is None:
                    print(f"    ⚠️  Warning: attribute '{attr}' missing in Delta table details for {table_name}")
                    return default
                return getattr(obj, attr)

            metadata.num_files = attr_with_warning(details, 'numFiles')
            metadata.size_bytes = attr_with_warning(details, 'sizeInBytes')
            metadata.created_time = str(attr_with_warning(details, 'createdAt', ''))
            metadata.modified_time = str(attr_with_warning(details, 'lastModified', ''))

            # Get partition columns if any
            partition_cols = attr_with_warning(details, 'partitionColumns', [])
            if partition_cols:
                metadata.partition_columns = partition_cols
            
            # Try to get row count from table metadata (faster than querying data)
            try:
                # Use DESCRIBE DETAIL which doesn't read all data
                describe_result = self.lakehouse.execute_query(f"DESCRIBE DETAIL `{table_name}`")
                stats = describe_result.collect()[0]
                if hasattr(stats, 'numRecords') and stats.numRecords is not None:
                    metadata.row_count = stats.numRecords
            except Exception:
                # Fallback: try to get approximate count without full table scan
                try:
                    # This is still relatively fast for Delta tables
                    count_result = self.lakehouse.execute_query(f"SELECT COUNT(*) as row_count FROM `{table_name}` LIMIT 1")
                    metadata.row_count = count_result.collect()[0].row_count
                except Exception:
                    # If all else fails, leave row_count as None
                    pass
            
            # Get table properties
            try:
                properties = delta_table.detail().select("properties").collect()[0][0]
                if properties:
                    metadata.properties = dict(properties)
            except Exception:
                pass
                
        except Exception as e:
            import traceback
            print(f"    ⚠️  Could not extract full metadata for '{table_name}': {e}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
            print(f"    🔍 Traceback: {traceback.format_exc()}")
        
        return metadata
    
    def scan_level_2_schema(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True
    ) -> List[SchemaMetadata]:
        """
        Level 2 Scan: Column metadata extraction.
        
        This scan captures column names, types, and counts without reading data.
        
        Args:
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables
            
        Returns:
            List of SchemaMetadata objects
        """
        print("\n" + "="*60)
        print("📋 LEVEL 2 SCAN: Schema Discovery (Column Metadata)")
        print("="*60)
        
        schemas = []
        
        # Get tables to scan
        if table_names:
            tables_to_scan = table_names
        else:
            # Get all tables that completed Level 1
            level_1_completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_1_DISCOVERY)
            tables_to_scan = level_1_completed
        
        # Filter out already scanned tables if resuming
        if resume:
            completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_2_SCHEMA)
            tables_to_scan = [t for t in tables_to_scan if t not in completed]
            if completed:
                print(f"ℹ️  Skipping {len(completed)} already scanned tables")
        
        print(f"📊 Processing {len(tables_to_scan)} tables")
        
        for table_name in tables_to_scan:
            print(f"\n  Processing schema for: {table_name}")
            
            start_time = time.time()
            progress = self.persistence.load_progress(table_name) or ScanProgress(table_name=table_name)
            
            try:
                # Load table metadata
                table_metadata = self.persistence.load_table_metadata(table_name)
                if not table_metadata:
                    print(f"    ⚠️  No Level 1 metadata found, skipping")
                    continue
                
                # Extract schema
                schema = self._extract_schema_metadata(table_metadata.table_path, table_name)
                
                # Save schema
                self.persistence.save_schema_metadata(schema)
                schemas.append(schema)
                
                # Update progress
                duration_ms = int((time.time() - start_time) * 1000)
                progress.level_2_completed = datetime.now()
                progress.level_2_duration_ms = duration_ms
                self.persistence.save_progress(progress)
                
                print(f"    ✅ Completed in {duration_ms}ms")
                print(f"    📊 Columns: {schema.column_count}")
                if schema.primary_key_candidates:
                    print(f"    🔑 Potential PKs: {', '.join(schema.primary_key_candidates[:3])}")
                
            except Exception as e:
                import traceback
                error_details = f"{str(e)}\nFile: {__file__}\nLine: {traceback.extract_tb(e.__traceback__)[-1].lineno}\n{traceback.format_exc()}"
                print(f"    ❌ Error in Level 2 scan for table '{table_name}': {str(e)}")
                print(f"    📍 Location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
                print(f"    🔍 Full traceback: {traceback.format_exc()}")
                progress.last_error = error_details
                progress.last_error_time = datetime.now()
                self.persistence.save_progress(progress)
        
        print(f"\n✨ Level 2 scan complete. Processed {len(schemas)} schemas")
        return schemas
    
    def _extract_schema_metadata(self, table_path: str, table_name: str) -> SchemaMetadata:
        """Extract schema metadata using lakehouse_utils."""
        try:
            # Use lakehouse_utils get_table_schema method - much simpler!
            schema_dict = self.lakehouse.get_table_schema(table_name)
            
            schema = SchemaMetadata(
                table_name=table_name,
                column_count=len(schema_dict)
            )
            
            # Convert schema dict to column info format
            for col_name, col_type in schema_dict.items():
                col_info = {
                    "name": col_name,
                    "type": col_type,
                    "nullable": True  # lakehouse_utils doesn't provide nullability info
                }
                schema.columns.append(col_info)
                
                # Identify potential keys based on naming
                col_lower = col_name.lower()
                if any(pattern in col_lower for pattern in ['_id', 'id_', '_key', 'key_', '_pk']):
                    if col_lower == 'id' or col_lower.endswith('_id'):
                        schema.primary_key_candidates.append(col_name)
                    else:
                        schema.foreign_key_candidates.append(col_name)
            
            return schema
            
        except Exception as e:
            import traceback
            print(f"    ⚠️  Could not extract schema using lakehouse_utils: {e}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
            print(f"    🔍 Traceback: {traceback.format_exc()}")
            # Fallback to direct DataFrame approach
            return self._extract_schema_metadata_fallback(table_path, table_name)
    
    def _extract_schema_metadata_fallback(self, table_path: str, table_name: str) -> SchemaMetadata:
        """Fallback method for schema extraction using direct DataFrame access."""
        # Get DataFrame schema directly
        if "file://" in table_path or "/" in table_path:
            df = self.spark.read.format("delta").load(table_path)
        else:
            df = self.spark.table(table_path)
        
        schema = SchemaMetadata(
            table_name=table_name,
            column_count=len(df.columns)
        )
        
        # Extract column information
        for field_obj in df.schema.fields:
            col_info = {
                "name": field_obj.name,
                "type": str(field_obj.dataType),
                "nullable": field_obj.nullable
            }
            schema.columns.append(col_info)
            
            # Identify potential keys based on naming
            col_lower = field_obj.name.lower()
            if any(pattern in col_lower for pattern in ['_id', 'id_', '_key', 'key_', '_pk']):
                if col_lower == 'id' or col_lower.endswith('_id'):
                    schema.primary_key_candidates.append(field_obj.name)
                else:
                    schema.foreign_key_candidates.append(field_obj.name)
        
        return schema
    
    def scan_level_3_profile(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        sample_size: Optional[int] = None
    ) -> List[DatasetProfile]:
        """
        Level 3 Scan: Efficient single-pass profiling.
        
        This scan performs efficient profiling of tables and columns in a single
        pass, collecting only statistics that can be computed efficiently without
        expensive operations (no percentiles, no top values, no entropy).
        
        Uses approximate distinct count (HyperLogLog) for memory efficiency.
        
        Args:
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables  
            sample_size: Optional number of rows to sample (ignored - L3 processes full datasets efficiently)
            
        Returns:
            List of DatasetProfile objects with efficient statistics
        """
        print("\n" + "="*60)
        print("📊 LEVEL 3 SCAN: Efficient Single-Pass Profiling")
        print("="*60)
        
        profiles = []
        
        # Get tables to scan
        if table_names:
            tables_to_scan = table_names
        else:
            # Get all tables that completed Level 2
            level_2_completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_2_SCHEMA)
            tables_to_scan = level_2_completed
        
        # Filter out already scanned tables if resuming
        if resume:
            completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_3_PROFILE)
            tables_to_scan = [t for t in tables_to_scan if t not in completed]
            if completed:
                print(f"ℹ️  Skipping {len(completed)} already scanned tables")
        
        print(f"📊 Processing {len(tables_to_scan)} tables")
        
        for table_name in tables_to_scan:
            print(f"\n  Profiling table: {table_name}")
            
            start_time = time.time()
            progress = self.persistence.load_progress(table_name) or ScanProgress(table_name=table_name)
            
            try:
                # Load previous scan results
                table_metadata = self.persistence.load_table_metadata(table_name)
                schema_metadata = self.persistence.load_schema_metadata(table_name)
                
                if not table_metadata or not schema_metadata:
                    print("    ⚠️  Missing Level 1 or 2 metadata, skipping")
                    continue
                
                # Perform comprehensive profiling
                dataset_profile = self._perform_single_pass_profile(
                    table_name,
                    table_metadata,
                    schema_metadata,
                    sample_size
                )
                
                # Save profile results
                self._save_profile_results(dataset_profile)
                profiles.append(dataset_profile)
                
                # Update progress
                duration_ms = int((time.time() - start_time) * 1000)
                progress.level_3_completed = datetime.now()
                progress.level_3_duration_ms = duration_ms
                self.persistence.save_progress(progress)
                
                print(f"    ✅ Completed in {duration_ms}ms")
                print(f"    📊 Profiled {len(dataset_profile.column_profiles)} columns")
                print(f"    📈 Row count: {dataset_profile.row_count:,}")
                
            except Exception as e:
                import traceback
                error_details = f"{str(e)}\nFile: {__file__}\nLine: {traceback.extract_tb(e.__traceback__)[-1].lineno}\n{traceback.format_exc()}"
                print(f"    ❌ Error in Level 3 scan for table '{table_name}': {str(e)}")
                print(f"    📍 Location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
                print(f"    🔍 Full traceback: {traceback.format_exc()}")
                progress.last_error = error_details
                progress.last_error_time = datetime.now()
                self.persistence.save_progress(progress)
        
        print(f"\n✨ Level 3 scan complete. Profiled {len(profiles)} tables")
        return profiles
    
    def get_explorer(self):
        """Get a ProfileExplorer instance for easy exploration of results."""
        from ingen_fab.python_libs.pyspark.profile_explorer import ProfileExplorer
        # Extract table prefix from the persistence metadata table name
        table_prefix = self.persistence.metadata_table.replace("_metadata", "")
        return ProfileExplorer(self.lakehouse, table_prefix)
    
    def _perform_single_pass_profile(
        self,
        table_name: str,
        table_metadata: TableMetadata,
        schema_metadata: SchemaMetadata,
        sample_size: Optional[int] = None
    ) -> DatasetProfile:
        """
        Perform comprehensive single-pass profiling of a table.
        
        This method collects as many statistics as possible in a single table scan,
        optimizing for performance while gathering comprehensive information.
        """
        # Read the table
        df = self.lakehouse.read_table(table_name)
        
        # L3 is now optimized to not require sampling - use full dataset for accurate statistics
        actual_row_count = table_metadata.row_count or df.count()
        print(f"    ⚙️  Processing full dataset: {actual_row_count:,} rows (no sampling needed for L3)")
        
        # Initialize profile
        dataset_profile = DatasetProfile(
            dataset_name=table_name,
            row_count=actual_row_count,
            column_count=schema_metadata.column_count,
            column_profiles=[],
            profile_timestamp=datetime.now().isoformat()
        )
        
        # Prepare aggregation expressions for all columns in a single pass
        agg_exprs = []
        column_info = {}
        
        for col_dict in schema_metadata.columns:
            col_name = col_dict["name"]
            col_type = col_dict["type"]
            
            # Track column info
            column_info[col_name] = {
                "name": col_name,
                "type": col_type,
                "nullable": col_dict.get("nullable", True)
            }
            
            # Basic statistics for all columns - only efficient operations
            agg_exprs.extend([
                F.count(F.col(col_name)).alias(f"{col_name}__count"),
                F.approx_count_distinct(F.col(col_name)).alias(f"{col_name}__approx_distinct"),
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"{col_name}__nulls")
            ])
            
            # Type-specific aggregations - only efficient operations
            if self._is_numeric_type(col_type):
                agg_exprs.extend([
                    F.min(col_name).alias(f"{col_name}__min"),
                    F.max(col_name).alias(f"{col_name}__max"),
                    F.mean(col_name).alias(f"{col_name}__mean"),
                    F.stddev(col_name).alias(f"{col_name}__stddev"),
                    F.sum(F.col(col_name)).alias(f"{col_name}__sum")
                ])
            elif self._is_string_type(col_type):
                agg_exprs.extend([
                    F.min(F.length(col_name)).alias(f"{col_name}__min_length"),
                    F.max(F.length(col_name)).alias(f"{col_name}__max_length"),
                    F.avg(F.length(col_name)).alias(f"{col_name}__avg_length"),
                    F.first(col_name, ignorenulls=True).alias(f"{col_name}__sample")
                ])
            elif self._is_timestamp_type(col_type):
                agg_exprs.extend([
                    F.min(col_name).alias(f"{col_name}__min"),
                    F.max(col_name).alias(f"{col_name}__max")
                ])
            elif self._is_boolean_type(col_type):
                agg_exprs.extend([
                    F.sum(F.when(F.col(col_name).eqNullSafe(True), 1).otherwise(0)).alias(f"{col_name}__true_count"),
                    F.sum(F.when(F.col(col_name).eqNullSafe(False), 1).otherwise(0)).alias(f"{col_name}__false_count")
                ])
        
        # Execute all aggregations in a single pass
        print(f"    ⚙️  Computing statistics for {len(schema_metadata.columns)} columns...")
        stats_row = df.agg(*agg_exprs).collect()[0]
        stats_dict = stats_row.asDict()
        
        # Process results for each column
        for col_dict in schema_metadata.columns:
            col_name = col_dict["name"]
            col_type = col_dict["type"]
            
            # Extract statistics for this column
            count = stats_dict.get(f"{col_name}__count", 0)
            distinct = stats_dict.get(f"{col_name}__approx_distinct", 0)
            nulls = stats_dict.get(f"{col_name}__nulls", 0)
            
            # Create column profile with safe calculations
            col_profile = ColumnProfile(
                column_name=col_name,
                data_type=col_type,
                null_count=nulls if nulls is not None else 0,
                null_percentage=(nulls / actual_row_count * 100) if actual_row_count > 0 and nulls is not None else 0,
                distinct_count=distinct if distinct is not None else 0,
                distinct_percentage=(distinct / actual_row_count * 100) if actual_row_count > 0 and distinct is not None else 0,
                completeness=((actual_row_count - nulls) / actual_row_count) if actual_row_count > 0 and nulls is not None else 0,
                uniqueness=(distinct / count) if count > 0 and distinct is not None else 0
            )
            
            # Add type-specific statistics - only efficient ones
            if self._is_numeric_type(col_type):
                col_profile.min_value = stats_dict.get(f"{col_name}__min")
                col_profile.max_value = stats_dict.get(f"{col_name}__max")
                col_profile.mean_value = stats_dict.get(f"{col_name}__mean")
                col_profile.std_dev = stats_dict.get(f"{col_name}__stddev")
                
                # Basic numeric statistics (no expensive operations)
                col_profile.value_statistics = ValueStatistics(
                    selectivity=distinct / actual_row_count if actual_row_count > 0 and distinct is not None else 0,
                    is_unique_key=(distinct >= actual_row_count * 0.95 and nulls == 0) if distinct is not None and nulls is not None else False,  # Approximate check
                    is_constant=(distinct == 1) if distinct is not None else False,
                    numeric_distribution={
                        "sum": stats_dict.get(f"{col_name}__sum"),
                        "range": (stats_dict.get(f"{col_name}__max", 0) - stats_dict.get(f"{col_name}__min", 0)) if stats_dict.get(f"{col_name}__max") is not None and stats_dict.get(f"{col_name}__min") is not None else None
                    }
                )
            elif self._is_string_type(col_type):
                col_profile.value_statistics = ValueStatistics(
                    selectivity=distinct / actual_row_count if actual_row_count > 0 and distinct is not None else 0,
                    is_unique_key=(distinct >= actual_row_count * 0.95 and nulls == 0) if distinct is not None and nulls is not None else False,  # Approximate check
                    is_constant=(distinct == 1) if distinct is not None else False,
                    value_length_stats={
                        "min": stats_dict.get(f"{col_name}__min_length"),
                        "max": stats_dict.get(f"{col_name}__max_length"),
                        "avg": stats_dict.get(f"{col_name}__avg_length")
                    },
                    sample_values=[stats_dict.get(f"{col_name}__sample")] if stats_dict.get(f"{col_name}__sample") else []
                )
            elif self._is_timestamp_type(col_type):
                col_profile.min_value = stats_dict.get(f"{col_name}__min")
                col_profile.max_value = stats_dict.get(f"{col_name}__max")
                col_profile.value_statistics = ValueStatistics(
                    selectivity=distinct / actual_row_count if actual_row_count > 0 and distinct is not None else 0,
                    is_unique_key=(distinct >= actual_row_count * 0.95 and nulls == 0) if distinct is not None and nulls is not None else False,  # Approximate check
                    numeric_distribution={
                        "date_range_days": (stats_dict.get(f"{col_name}__max") - stats_dict.get(f"{col_name}__min")).days if stats_dict.get(f"{col_name}__max") and stats_dict.get(f"{col_name}__min") else None
                    }
                )
            elif self._is_boolean_type(col_type):
                true_count = stats_dict.get(f"{col_name}__true_count", 0) or 0
                false_count = stats_dict.get(f"{col_name}__false_count", 0) or 0
                total_bool_count = true_count + false_count
                col_profile.value_distribution = {
                    True: true_count,
                    False: false_count
                }
                col_profile.value_statistics = ValueStatistics(
                    dominant_value=True if true_count > false_count else False,
                    dominant_value_ratio=max(true_count, false_count) / total_bool_count if total_bool_count > 0 else 0
                )
            
            # Analyze naming patterns
            col_profile.naming_pattern = self._analyze_naming_pattern(col_name)
            
            # Determine semantic type based on name and statistics
            col_profile.semantic_type = self._determine_semantic_type(
                col_name, col_type, col_profile, col_profile.naming_pattern
            )
            
            # Skip entropy calculation and value distributions for L3 - these require additional scans
            # These will be moved to L4 (Advanced Analytics)
            
            dataset_profile.column_profiles.append(col_profile)
        
        print(f"    ✅ Single-pass statistics complete - no additional table scans needed")
        return dataset_profile
    
    def _is_numeric_type(self, type_str: str) -> bool:
        """Check if a type string represents a numeric type."""
        numeric_types = ['int', 'long', 'float', 'double', 'decimal', 'bigint', 'smallint', 'tinyint']
        return any(t in type_str.lower() for t in numeric_types)
    
    def _is_string_type(self, type_str: str) -> bool:
        """Check if a type string represents a string type."""
        return 'string' in type_str.lower() or 'varchar' in type_str.lower() or 'char' in type_str.lower()
    
    def _is_timestamp_type(self, type_str: str) -> bool:
        """Check if a type string represents a timestamp/date type."""
        return 'timestamp' in type_str.lower() or 'date' in type_str.lower()
    
    def _is_boolean_type(self, type_str: str) -> bool:
        """Check if a type string represents a boolean type."""
        return 'boolean' in type_str.lower() or 'bool' in type_str.lower()
    
    def _analyze_naming_pattern(self, column_name: str) -> NamingPattern:
        """Analyze column naming patterns to identify potential relationships."""
        pattern = NamingPattern()
        col_lower = column_name.lower()
        
        # Check for ID patterns
        id_patterns = ['_id', 'id_', '_key', 'key_', '_pk', '_code', 'code_']
        pattern.is_id_column = any(p in col_lower for p in id_patterns) or col_lower == 'id'
        
        # Check for foreign key patterns
        fk_patterns = ['_id', 'id_', '_fk', 'fk_', '_ref', 'ref_']
        pattern.is_foreign_key = any(p in col_lower for p in fk_patterns) and col_lower != 'id'
        
        # Check for timestamp patterns
        ts_patterns = ['_date', 'date_', '_time', 'time_', '_ts', 'ts_', 'created', 'modified', 'updated']
        pattern.is_timestamp = any(p in col_lower for p in ts_patterns)
        
        # Check for status/flag patterns
        status_patterns = ['status', 'flag', 'is_', 'has_', 'can_', '_ind', 'ind_']
        pattern.is_status_flag = any(p in col_lower for p in status_patterns)
        
        # Check for measurement patterns
        measure_patterns = ['amount', 'count', 'qty', 'quantity', 'total', 'sum', 'avg', 'min', 'max', 'price', 'cost', 'rate']
        pattern.is_measurement = any(p in col_lower for p in measure_patterns)
        
        # Collect detected patterns
        for patterns_list, pattern_type in [
            (id_patterns, "ID"), (fk_patterns, "FK"), (ts_patterns, "Timestamp"),
            (status_patterns, "Status"), (measure_patterns, "Measure")
        ]:
            for p in patterns_list:
                if p in col_lower:
                    pattern.detected_patterns.append(f"{pattern_type}: {p}")
        
        # Calculate confidence based on matches
        match_count = sum([
            pattern.is_id_column, pattern.is_foreign_key, pattern.is_timestamp,
            pattern.is_status_flag, pattern.is_measurement
        ])
        pattern.naming_confidence = min(match_count * 0.3, 1.0)
        
        return pattern
    
    def _determine_semantic_type(
        self,
        column_name: str,
        data_type: str,
        profile: ColumnProfile,
        naming_pattern: NamingPattern
    ) -> SemanticType:
        """Determine the semantic type of a column based on its profile."""
        # Check for identifiers
        if profile.value_statistics and profile.value_statistics.is_unique_key:
            return SemanticType.IDENTIFIER
        
        # Check for foreign keys
        if naming_pattern.is_foreign_key:
            return SemanticType.FOREIGN_KEY
        
        # Check for timestamps
        if naming_pattern.is_timestamp or self._is_timestamp_type(data_type):
            return SemanticType.TIMESTAMP
        
        # Check for status/flags
        if naming_pattern.is_status_flag or (profile.distinct_count is not None and profile.distinct_count <= 10 and profile.distinct_count > 0):
            return SemanticType.STATUS
        
        # Check for measures
        if naming_pattern.is_measurement and self._is_numeric_type(data_type):
            return SemanticType.MEASURE
        
        # Check for dimensions
        if profile.distinct_count is not None and profile.distinct_percentage is not None:
            if profile.distinct_percentage < 50 and not self._is_numeric_type(data_type):
                return SemanticType.DIMENSION
        
        # Check for descriptions
        if self._is_string_type(data_type) and profile.value_statistics:
            length_stats = profile.value_statistics.value_length_stats
            if length_stats:
                avg_length = length_stats.get("avg", 0)
                if avg_length is not None and avg_length > 50:
                    return SemanticType.DESCRIPTION
        
        return SemanticType.UNKNOWN
    
    def _calculate_entropy(self, df: DataFrame, column_name: str, distinct_count: int) -> float:
        """Calculate Shannon entropy for a column."""
        try:
            # Get value frequencies
            freq_df = df.groupBy(column_name).count().withColumnRenamed("count", "freq")
            total = df.count()
            
            # Calculate entropy
            entropy_df = freq_df.withColumn(
                "p", F.col("freq") / F.lit(total)
            ).withColumn(
                "entropy_contrib", -F.col("p") * F.log2(F.col("p"))
            )
            
            entropy = entropy_df.agg(F.sum("entropy_contrib")).collect()[0][0]
            return float(entropy) if entropy else 0.0
        except Exception as entropy_error:
            import traceback
            print(f"    ⚠️  Could not calculate entropy for column '{column_name}': {entropy_error}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(entropy_error.__traceback__)[-1].lineno}")
            return 0.0
    
    def _calculate_entropy_efficient(self, df: DataFrame, column_name: str, distinct_count: int, total_rows: int) -> float:
        """Calculate entropy more efficiently for large datasets using approximation."""
        try:
            # For large datasets, use a more memory-efficient approach
            # Limit the groupBy operation and use approximation
            freq_df = (df.groupBy(column_name)
                      .count()
                      .withColumnRenamed("count", "freq")
                      .limit(distinct_count + 10))  # Small buffer for safety
            
            # Calculate entropy using the frequencies
            entropy_df = freq_df.withColumn(
                "p", F.col("freq") / F.lit(total_rows)
            ).withColumn(
                "entropy_contrib", -F.col("p") * F.log2(F.col("p"))
            )
            
            # Use collect() on the limited result set
            entropy_values = entropy_df.select("entropy_contrib").collect()
            entropy = sum(row.entropy_contrib for row in entropy_values if row.entropy_contrib is not None)
            
            return float(entropy) if entropy else 0.0
        except Exception as entropy_error:
            import traceback
            print(f"    ⚠️  Could not calculate efficient entropy for column '{column_name}': {entropy_error}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(entropy_error.__traceback__)[-1].lineno}")
            return 0.0
    
    def _save_profile_results(self, profile: DatasetProfile) -> None:
        """Save Level 3 profile results to lakehouse."""
        from pyspark.sql.types import StringType, LongType, TimestampType
        
        # Create profile table if it doesn't exist
        if not self.lakehouse.check_if_table_exists(self.persistence.profile_table):
            profile_schema = StructType([
                StructField("table_name", StringType(), False),
                StructField("row_count", LongType(), False),
                StructField("column_count", LongType(), False),
                StructField("profile_data", StringType(), False),  # JSON serialized profile
                StructField("scan_timestamp", TimestampType(), False),
            ])
            df = self.spark.createDataFrame([], profile_schema)
            self.lakehouse.write_to_table(df, self.persistence.profile_table, mode="overwrite")
            print(f"  ✅ Created table: {self.persistence.profile_table}")
        
        # Serialize profile to JSON
        profile_dict = {
            "table_name": profile.dataset_name,
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "profile_timestamp": profile.profile_timestamp,
            "column_profiles": []
        }
        
        for col_profile in profile.column_profiles:
            # Helper function to safely convert values to JSON-serializable format
            def safe_json_value(val):
                if val is None:
                    return None
                elif hasattr(val, 'isoformat'):  # datetime/date objects
                    return val.isoformat()
                elif isinstance(val, (bool, int, float, str)):
                    return val
                else:
                    return str(val)
            
            col_dict = {
                "column_name": col_profile.column_name,
                "data_type": col_profile.data_type,
                "null_count": col_profile.null_count,
                "null_percentage": col_profile.null_percentage,
                "distinct_count": col_profile.distinct_count,
                "distinct_percentage": col_profile.distinct_percentage,
                "completeness": col_profile.completeness,
                "uniqueness": col_profile.uniqueness,
                "min_value": safe_json_value(col_profile.min_value),
                "max_value": safe_json_value(col_profile.max_value),
                "mean_value": col_profile.mean_value,
                "median_value": col_profile.median_value,
                "std_dev": col_profile.std_dev,
                "entropy": col_profile.entropy,
                "semantic_type": col_profile.semantic_type.value if col_profile.semantic_type else None,
                "top_distinct_values": [safe_json_value(v) for v in col_profile.top_distinct_values[:10]] if col_profile.top_distinct_values else None
            }
            
            # Add value statistics if present
            if col_profile.value_statistics:
                col_dict["value_statistics"] = {
                    "selectivity": col_profile.value_statistics.selectivity,
                    "is_unique_key": col_profile.value_statistics.is_unique_key,
                    "is_constant": col_profile.value_statistics.is_constant,
                    "dominant_value": safe_json_value(col_profile.value_statistics.dominant_value),
                    "dominant_value_ratio": col_profile.value_statistics.dominant_value_ratio
                }
                if col_profile.value_statistics.numeric_distribution:
                    col_dict["value_statistics"]["numeric_distribution"] = col_profile.value_statistics.numeric_distribution
                if col_profile.value_statistics.value_length_stats:
                    col_dict["value_statistics"]["value_length_stats"] = col_profile.value_statistics.value_length_stats
            
            # Add value distribution if present (also needs safe conversion)
            if hasattr(col_profile, 'value_distribution') and col_profile.value_distribution:
                col_dict["value_distribution"] = {
                    safe_json_value(k): v for k, v in col_profile.value_distribution.items()
                }
            
            # Add naming pattern if present
            if col_profile.naming_pattern:
                col_dict["naming_pattern"] = {
                    "is_id_column": col_profile.naming_pattern.is_id_column,
                    "is_foreign_key": col_profile.naming_pattern.is_foreign_key,
                    "is_timestamp": col_profile.naming_pattern.is_timestamp,
                    "is_status_flag": col_profile.naming_pattern.is_status_flag,
                    "is_measurement": col_profile.naming_pattern.is_measurement,
                    "confidence": col_profile.naming_pattern.naming_confidence
                }
            
            profile_dict["column_profiles"].append(col_dict)
        
        # Create DataFrame row
        row_data = [
            profile.dataset_name,
            profile.row_count,
            profile.column_count,
            json.dumps(profile_dict),
            datetime.now()
        ]
        
        # Create DataFrame with explicit schema
        profile_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("row_count", LongType(), False),
            StructField("column_count", LongType(), False),
            StructField("profile_data", StringType(), False),
            StructField("scan_timestamp", TimestampType(), False),
        ])
        
        df = self.spark.createDataFrame([row_data], profile_schema)
        
        # Merge with existing data (upsert)
        try:
            existing_df = self.lakehouse.read_table(self.persistence.profile_table)
            merged_df = existing_df.filter(F.col("table_name") != profile.dataset_name).union(df)
            self.lakehouse.write_to_table(merged_df, self.persistence.profile_table, mode="overwrite")
        except Exception as save_error:
            import traceback
            print(f"    ⚠️  Could not merge profile results, attempting append: {save_error}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(save_error.__traceback__)[-1].lineno}")
            try:
                self.lakehouse.write_to_table(df, self.persistence.profile_table, mode="append")
            except Exception as append_error:
                print(f"    ❌ Failed to append profile results: {append_error}")
                print(f"    📍 Append error location: {__file__}:{traceback.extract_tb(append_error.__traceback__)[-1].lineno}")
                print(f"    🔍 Full traceback: {traceback.format_exc()}")
                raise
    
    def get_scan_summary(self) -> Dict[str, Any]:
        """Get summary of all scan progress from lakehouse."""
        summary = {
            "total_tables": 0,
            "level_1_completed": 0,
            "level_2_completed": 0,
            "level_3_completed": 0,
            "level_4_completed": 0,
            "tables_with_errors": 0,
            "scan_details": []
        }
        
        try:
            # Read progress table from lakehouse
            progress_df = self.lakehouse.read_table(self.persistence.progress_table)
            all_progress = progress_df.collect()
            
            for row in all_progress:
                summary["total_tables"] += 1
                
                if row.level_1_completed:
                    summary["level_1_completed"] += 1
                if row.level_2_completed:
                    summary["level_2_completed"] += 1
                if row.level_3_completed:
                    summary["level_3_completed"] += 1
                if row.level_4_completed:
                    summary["level_4_completed"] += 1
                if row.last_error:
                    summary["tables_with_errors"] += 1
                
                summary["scan_details"].append({
                    "table": row.table_name,
                    "level_1": row.level_1_completed.isoformat() if row.level_1_completed else None,
                    "level_2": row.level_2_completed.isoformat() if row.level_2_completed else None,
                    "level_3": row.level_3_completed.isoformat() if row.level_3_completed else None,
                    "level_4": row.level_4_completed.isoformat() if row.level_4_completed else None,
                    "last_error": row.last_error
                })
        except Exception as e:
            print(f"  ⚠️  Could not read scan summary: {e}")
        
        return summary
    
    def get_scan_results_df(self, scan_level: str = "metadata") -> Optional[DataFrame]:
        """
        Get scan results as a DataFrame for easy querying.
        
        Args:
            scan_level: Which results to retrieve ("metadata", "schema", "progress")
            
        Returns:
            DataFrame with scan results or None if error
        """
        try:
            if scan_level == "metadata":
                return self.lakehouse.read_table(self.persistence.metadata_table)
            elif scan_level == "schema":
                return self.lakehouse.read_table(self.persistence.schema_table)
            elif scan_level == "progress":
                return self.lakehouse.read_table(self.persistence.progress_table)
            else:
                print(f"  ❌ Invalid scan level: {scan_level}")
                return None
        except Exception as e:
            print(f"  ⚠️  Could not read {scan_level} results: {e}")
            return None