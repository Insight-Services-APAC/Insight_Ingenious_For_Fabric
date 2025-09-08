"""Enhanced lakehouse persistence implementation with normalized data model for Power BI reporting."""

import json
from datetime import datetime, date
from typing import Any, Dict, List, Optional
import uuid
import hashlib
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..core.enums.profile_types import ScanLevel, SemanticType, ProfileGrain
from ..core.models.metadata import SchemaMetadata, ScanProgress, TableMetadata
from ..core.models.profile_models import ColumnProfile, DatasetProfile
from .base_persistence import BasePersistence
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime and date objects."""
    
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def safe_numeric_convert(value: Any) -> Any:
    """Safely convert numeric values to Python types compatible with Spark."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    # Preserve boolean values (bool is a subclass of int) without coercion
    if isinstance(value, bool):
        return value
    # Convert ints explicitly to float so Spark infers DoubleType uniformly
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        return value
    # Try to convert string representations
    if isinstance(value, str):
        try:
            # Try decimal first, then float
            return float(Decimal(value))
        except (ValueError, TypeError):
            return value
    return value


class EnhancedLakehousePersistence(BasePersistence):
    """
    Enhanced lakehouse persistence using a star schema optimized for Power BI reporting.
    
    Data Model Structure:
    
    DIMENSION TABLES (Lookups):
    - dim_tables: Table information and metadata
    - dim_columns: Column metadata and properties
    - dim_scan_levels: Scan level definitions
    - dim_semantic_types: Semantic type definitions  
    - dim_data_types: Data type categories
    
    FACT TABLES (Measurements):
    - fact_table_profiles: Table-level profiling results
    - fact_column_profiles: Column-level profiling statistics
    - fact_data_quality: Data quality metrics and scores
    - fact_relationships: Column relationships and correlations
    - fact_scan_progress: Scan execution tracking
    
    BRIDGE TABLES (Many-to-Many):
    - bridge_column_values: Top values per column (normalized)
    - bridge_percentiles: Percentile values per column
    - bridge_anomalies: Detected anomalies
    - bridge_business_rules: Business rules per column
    """
    
    def __init__(self, lakehouse: lakehouse_utils, spark: SparkSession, table_prefix: str = "tiered_profile", profile_grain: ProfileGrain = ProfileGrain.DAILY):
        """Initialize enhanced persistence."""
        super().__init__(table_prefix)
        
        self.lakehouse = lakehouse
        self.spark = spark
        self.profile_grain = profile_grain
        
        # Define all table names
        self.dim_tables_table = f"{table_prefix}_dim_tables"
        self.dim_columns_table = f"{table_prefix}_dim_columns"  
        self.dim_scan_levels_table = f"{table_prefix}_dim_scan_levels"
        self.dim_semantic_types_table = f"{table_prefix}_dim_semantic_types"
        self.dim_data_types_table = f"{table_prefix}_dim_data_types"
        
        self.fact_table_profiles_table = f"{table_prefix}_fact_table_profiles"
        self.fact_column_profiles_table = f"{table_prefix}_fact_column_profiles"
        self.fact_data_quality_table = f"{table_prefix}_fact_data_quality"
        self.fact_relationships_table = f"{table_prefix}_fact_relationships"
        self.fact_scan_progress_table = f"{table_prefix}_fact_scan_progress"
        
        self.bridge_column_values_table = f"{table_prefix}_bridge_column_values"
        self.bridge_percentiles_table = f"{table_prefix}_bridge_percentiles"
        self.bridge_anomalies_table = f"{table_prefix}_bridge_anomalies"
        self.bridge_business_rules_table = f"{table_prefix}_bridge_business_rules"
        
        # Initialize dimension tables
        self._initialize_dimension_tables()
    
    def _get_profile_date_key(self) -> str:
        """Get the date key for profile grain control."""
        if self.profile_grain == ProfileGrain.DAILY:
            return datetime.now().strftime("%Y-%m-%d")
        else:  # CONTINUOUS
            return datetime.now().isoformat()
    
    def _generate_table_key(self, table_name: str, profile_date_key: str) -> str:
        """Generate deterministic surrogate key for table-level records."""
        composite_key = f"{table_name}#{profile_date_key}"
        return hashlib.md5(composite_key.encode()).hexdigest()
    
    def _generate_column_key(self, table_name: str, column_name: str, profile_date_key: str) -> str:
        """Generate deterministic surrogate key for column-level records."""
        composite_key = f"{table_name}#{column_name}#{profile_date_key}"
        return hashlib.md5(composite_key.encode()).hexdigest()
    
    def _generate_profile_run_key(self, table_name: str, profile_date_key: str, profile_id: str) -> str:
        """Generate deterministic surrogate key for profile run records."""
        composite_key = f"{table_name}#{profile_date_key}#{profile_id}"
        return hashlib.md5(composite_key.encode()).hexdigest()
    
    def _initialize_dimension_tables(self):
        """Initialize dimension tables with reference data."""
        # Initialize scan levels dimension
        self._initialize_scan_levels_dim()
        
        # Initialize semantic types dimension
        self._initialize_semantic_types_dim()
        
        # Initialize data types dimension
        self._initialize_data_types_dim()
    
    def _initialize_scan_levels_dim(self):
        """Initialize scan levels dimension table."""
        scan_levels_data = [
            (1, "LEVEL_1_DISCOVERY", "Table Discovery", "Fast discovery of Delta tables and basic metadata collection"),
            (2, "LEVEL_2_SCHEMA", "Schema Discovery", "Column metadata extraction and key identification"),
            (3, "LEVEL_3_PROFILE", "Statistical Profiling", "Single-pass detailed column profile information"),
            (4, "LEVEL_4_ADVANCED", "Advanced Analysis", "Multi-pass analysis including relationships and patterns")
        ]
        
        schema = StructType([
            StructField("scan_level_id", IntegerType(), False),
            StructField("scan_level_code", StringType(), False),
            StructField("scan_level_name", StringType(), False),
            StructField("scan_level_description", StringType(), False)
        ])
        
        if not self.lakehouse.check_if_table_exists(self.dim_scan_levels_table):
            df = self.spark.createDataFrame(scan_levels_data, schema=schema)
            self.lakehouse.write_to_table(df, self.dim_scan_levels_table, mode="overwrite")
    
    def _initialize_semantic_types_dim(self):
        """Initialize semantic types dimension table."""
        semantic_types_data = [
            ("UNKNOWN", "Unknown", "No semantic type identified"),
            ("IDENTIFIER", "Identifier", "Primary or unique identifier column"),
            ("FOREIGN_KEY", "Foreign Key", "References another table's identifier"),
            ("MEASURE", "Measure", "Numeric measurement or metric"),
            ("DIMENSION", "Dimension", "Categorical or descriptive attribute"),
            ("TIMESTAMP", "Timestamp", "Date or datetime value"),
            ("TEXT", "Text", "Free-form text content"),
            ("BOOLEAN", "Boolean", "True/false or yes/no value"),
            ("CATEGORY", "Category", "Fixed set of categorical values"),
            ("EMAIL", "Email", "Email address"),
            ("PHONE", "Phone", "Phone number"),
            ("URL", "URL", "Web address"),
            ("ADDRESS", "Address", "Physical address"),
            ("NAME", "Name", "Person or entity name"),
            ("CODE", "Code", "Coded value or classification")
        ]
        
        schema = StructType([
            StructField("semantic_type_code", StringType(), False),
            StructField("semantic_type_name", StringType(), False),
            StructField("semantic_type_description", StringType(), False)
        ])
        
        if not self.lakehouse.check_if_table_exists(self.dim_semantic_types_table):
            df = self.spark.createDataFrame(semantic_types_data, schema=schema)
            self.lakehouse.write_to_table(df, self.dim_semantic_types_table, mode="overwrite")
    
    def _initialize_data_types_dim(self):
        """Initialize data types dimension table."""
        data_types_data = [
            ("string", "STRING", "Text/String", "Character and text data"),
            ("int", "INTEGER", "Integer", "Whole number values"),
            ("long", "LONG", "Long Integer", "Large whole number values"),
            ("float", "FLOAT", "Float", "Single precision decimal numbers"),
            ("double", "DOUBLE", "Double", "Double precision decimal numbers"),
            ("decimal", "DECIMAL", "Decimal", "Precise decimal numbers"),
            ("boolean", "BOOLEAN", "Boolean", "True/false values"),
            ("timestamp", "TIMESTAMP", "Timestamp", "Date and time values"),
            ("date", "DATE", "Date", "Date only values"),
            ("binary", "BINARY", "Binary", "Binary data"),
            ("array", "ARRAY", "Array", "Array/list structures"),
            ("map", "MAP", "Map", "Key-value map structures"),
            ("struct", "STRUCT", "Struct", "Complex nested structures")
        ]
        
        schema = StructType([
            StructField("data_type_raw", StringType(), False),
            StructField("data_type_code", StringType(), False),
            StructField("data_type_name", StringType(), False),
            StructField("data_type_description", StringType(), False)
        ])
        
        if not self.lakehouse.check_if_table_exists(self.dim_data_types_table):
            df = self.spark.createDataFrame(data_types_data, schema=schema)
            self.lakehouse.write_to_table(df, self.dim_data_types_table, mode="overwrite")
    
    def save_table_metadata(self, metadata: TableMetadata) -> None:
        """Save table metadata to dimension table."""
        # Generate unique table ID
        table_id = str(uuid.uuid4())
        
        row_data = [
            table_id,
            metadata.table_name,
            metadata.table_path,
            metadata.table_format,
            metadata.row_count,
            metadata.size_bytes,
            metadata.num_files,
            metadata.created_time,
            metadata.modified_time,
            json.dumps(metadata.partition_columns) if metadata.partition_columns else None,
            json.dumps(metadata.properties, cls=DateTimeEncoder) if metadata.properties else None,
            datetime.now()
        ]
        
        schema = StructType([
            StructField("table_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("table_path", StringType(), False),
            StructField("table_format", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("size_bytes", LongType(), True),
            StructField("num_files", IntegerType(), True),
            StructField("created_time", StringType(), True),
            StructField("modified_time", StringType(), True),
            StructField("partition_columns", StringType(), True),
            StructField("properties", StringType(), True),
            StructField("last_updated", TimestampType(), False)
        ])
        
        df = self.spark.createDataFrame([row_data], schema=schema)
        
        # Upsert logic
        if self.lakehouse.check_if_table_exists(self.dim_tables_table):
            existing_df = self.lakehouse.read_table(self.dim_tables_table)
            filtered_df = existing_df.filter(f"table_name != '{metadata.table_name}'")
            combined_df = filtered_df.unionByName(df)
            self.lakehouse.write_to_table(combined_df, self.dim_tables_table, mode="overwrite")
        else:
            self.lakehouse.write_to_table(df, self.dim_tables_table, mode="overwrite")
    
    def save_schema_metadata(self, metadata: SchemaMetadata) -> None:
        """Save schema metadata to dimension table."""
        if not metadata.column_metadata:
            return
            
        # Get profile date key for surrogate key generation
        profile_date_key = self._get_profile_date_key()
        
        rows = []
        for col_meta in metadata.column_metadata:
            column_id = str(uuid.uuid4())
            # Generate deterministic column key
            column_key = self._generate_column_key(metadata.table_name, col_meta.name, profile_date_key)
            
            row_data = [
                column_id,
                column_key,  # Add surrogate key
                metadata.table_name,
                col_meta.name,
                col_meta.data_type,
                col_meta.nullable,
                col_meta.description,
                col_meta.is_partition,
                col_meta.is_primary_key,
                col_meta.is_foreign_key,
                col_meta.foreign_key_reference,
                col_meta.default_value,
                json.dumps(col_meta.check_constraints) if col_meta.check_constraints else None,
                json.dumps(col_meta.properties, cls=DateTimeEncoder) if col_meta.properties else None,
                datetime.now()
            ]
            rows.append(row_data)
        
        schema = StructType([
            StructField("column_id", StringType(), False),
            StructField("column_key", StringType(), False),  # Add surrogate key field
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("data_type", StringType(), False),
            StructField("nullable", BooleanType(), False),
            StructField("description", StringType(), True),
            StructField("is_partition", BooleanType(), False),
            StructField("is_primary_key", BooleanType(), False),
            StructField("is_foreign_key", BooleanType(), False),
            StructField("foreign_key_reference", StringType(), True),
            StructField("default_value", StringType(), True),
            StructField("check_constraints", StringType(), True),
            StructField("properties", StringType(), True),
            StructField("last_updated", TimestampType(), False)
        ])
        
        df = self.spark.createDataFrame(rows, schema=schema)
        
        # Upsert logic
        if self.lakehouse.check_if_table_exists(self.dim_columns_table):
            existing_df = self.lakehouse.read_table(self.dim_columns_table)
            filtered_df = existing_df.filter(f"table_name != '{metadata.table_name}'")
            combined_df = filtered_df.unionByName(df)
            self.lakehouse.write_to_table(combined_df, self.dim_columns_table, mode="overwrite")
        else:
            self.lakehouse.write_to_table(df, self.dim_columns_table, mode="overwrite")
    
    def save_profile(self, profile: DatasetProfile) -> None:
        """Save complete profile using normalized fact tables with grain control."""
        profile_id = str(uuid.uuid4())
        
        # Handle daily grain by removing existing same-day data first
        if self.profile_grain == ProfileGrain.DAILY:
            self._remove_existing_daily_profile(profile.dataset_name)
        
        # Save table-level facts
        self._save_table_profile_facts(profile, profile_id)
        
        # Save column-level facts
        self._save_column_profile_facts(profile, profile_id)
        
        # Save data quality facts
        self._save_data_quality_facts(profile, profile_id)
        
        # Save relationship facts
        self._save_relationship_facts(profile, profile_id)
        
        # Save bridge table data
        self._save_column_values_bridge(profile, profile_id)
        self._save_percentiles_bridge(profile, profile_id)
        self._save_anomalies_bridge(profile, profile_id)
        self._save_business_rules_bridge(profile, profile_id)
    
    def _remove_existing_daily_profile(self, table_name: str) -> None:
        """Remove existing profile data for the same table and date (daily grain only)."""
        profile_date_key = self._get_profile_date_key()
        
        # List of all fact and bridge tables to clean
        tables_to_clean = [
            self.fact_table_profiles_table,
            self.fact_column_profiles_table,
            self.fact_data_quality_table,
            self.fact_relationships_table,
            self.bridge_column_values_table,
            self.bridge_percentiles_table,
            self.bridge_anomalies_table,
            self.bridge_business_rules_table
        ]
        
        for table_name_to_clean in tables_to_clean:
            try:
                if self.lakehouse.check_if_table_exists(table_name_to_clean):
                    existing_df = self.lakehouse.read_table(table_name_to_clean)
                    
                    # Filter condition depends on table structure
                    if "profile_date_key" in [field.name for field in existing_df.schema.fields]:
                        # Tables with profile_date_key
                        filtered_df = existing_df.filter(
                            f"NOT (table_name = '{table_name}' AND profile_date_key = '{profile_date_key}')"
                        )
                    else:
                        # Bridge tables - need to find related profile_id first
                        # For now, we'll filter by table_name and today's created_timestamp
                        today_start = datetime.now().strftime("%Y-%m-%d") + " 00:00:00"
                        today_end = datetime.now().strftime("%Y-%m-%d") + " 23:59:59"
                        filtered_df = existing_df.filter(
                            f"NOT (table_name = '{table_name}' AND created_timestamp >= '{today_start}' AND created_timestamp <= '{today_end}')"
                        )
                    
                    if filtered_df.count() < existing_df.count():  # Only rewrite if data was removed
                        self.lakehouse.write_to_table(filtered_df, table_name_to_clean, mode="overwrite")
            except Exception as e:
                self.logger.warning(f"Could not clean table {table_name_to_clean}: {e}")
    
    def _save_table_profile_facts(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save table-level profile facts with surrogate keys."""
        profile_date_key = self._get_profile_date_key()
        table_key = self._generate_table_key(profile.dataset_name, profile_date_key)
        profile_run_key = self._generate_profile_run_key(profile.dataset_name, profile_date_key, profile_id)
        
        row_data = [
            profile_id,
            table_key,  # Surrogate key
            profile_run_key,  # Profile run surrogate key
            profile.dataset_name,
            profile.row_count,
            profile.column_count,
            profile.profile_timestamp,
            safe_numeric_convert(profile.data_quality_score),
            profile.null_count,
            profile.duplicate_count,
            json.dumps(profile.statistics, cls=DateTimeEncoder) if profile.statistics else None,
            json.dumps(profile.recommendations) if profile.recommendations else None,
            json.dumps(profile.semantic_summary, cls=DateTimeEncoder) if profile.semantic_summary else None,
            json.dumps(profile.business_glossary) if profile.business_glossary else None,
            profile_date_key,
            datetime.now()
        ]
        
        schema = StructType([
            StructField("profile_id", StringType(), False),
            StructField("table_key", StringType(), False),  # Surrogate key
            StructField("profile_run_key", StringType(), False),  # Profile run surrogate key
            StructField("table_name", StringType(), False),
            StructField("row_count", LongType(), False),
            StructField("column_count", IntegerType(), False),
            StructField("profile_timestamp", StringType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("null_count", LongType(), True),
            StructField("duplicate_count", LongType(), True),
            StructField("statistics", StringType(), True),
            StructField("recommendations", StringType(), True),
            StructField("semantic_summary", StringType(), True),
            StructField("business_glossary", StringType(), True),
            StructField("profile_date_key", StringType(), False),
            StructField("created_timestamp", TimestampType(), False)
        ])
        
        df = self.spark.createDataFrame([row_data], schema=schema)
        
        if self.lakehouse.check_if_table_exists(self.fact_table_profiles_table):
            self.lakehouse.write_to_table(df, self.fact_table_profiles_table, mode="append")
        else:
            self.lakehouse.write_to_table(df, self.fact_table_profiles_table, mode="overwrite")
    
    def _save_column_profile_facts(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save column-level profile facts with surrogate keys."""
        profile_date_key = self._get_profile_date_key()
        table_key = self._generate_table_key(profile.dataset_name, profile_date_key)
        rows = []
        
        for col_profile in profile.column_profiles:
            column_profile_id = str(uuid.uuid4())
            column_key = self._generate_column_key(profile.dataset_name, col_profile.column_name, profile_date_key)
            
            row_data = [
                column_profile_id,
                profile_id,
                table_key,  # Foreign key to fact_table_profiles
                column_key,  # Surrogate key for this column
                profile.dataset_name,
                col_profile.column_name,
                col_profile.data_type,
                col_profile.semantic_type.value if col_profile.semantic_type else None,
                col_profile.null_count,
                safe_numeric_convert(col_profile.null_percentage),
                col_profile.distinct_count,
                safe_numeric_convert(col_profile.distinct_percentage),
                str(col_profile.min_value) if col_profile.min_value is not None else None,
                str(col_profile.max_value) if col_profile.max_value is not None else None,
                safe_numeric_convert(col_profile.mean_value),
                safe_numeric_convert(col_profile.median_value),
                safe_numeric_convert(col_profile.std_dev),
                safe_numeric_convert(col_profile.completeness),
                safe_numeric_convert(col_profile.uniqueness),
                safe_numeric_convert(col_profile.entropy),
                # Naming pattern fields (flattened)
                col_profile.naming_pattern.is_id_column if col_profile.naming_pattern else None,
                col_profile.naming_pattern.is_foreign_key if col_profile.naming_pattern else None,
                col_profile.naming_pattern.is_timestamp if col_profile.naming_pattern else None,
                safe_numeric_convert(col_profile.naming_pattern.naming_confidence) if col_profile.naming_pattern else None,
                # Value statistics fields (flattened)
                col_profile.value_statistics.is_unique_key if col_profile.value_statistics else None,
                col_profile.value_statistics.is_constant if col_profile.value_statistics else None,
                str(col_profile.value_statistics.sample_values[0]) if col_profile.value_statistics and col_profile.value_statistics.sample_values else None,
                datetime.now()
            ]
            rows.append(row_data)
        
        schema = StructType([
            StructField("column_profile_id", StringType(), False),
            StructField("profile_id", StringType(), False),
            StructField("table_key", StringType(), False),  # Foreign key to fact_table_profiles
            StructField("column_key", StringType(), False),  # Surrogate key for this column
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("data_type", StringType(), False),
            StructField("semantic_type", StringType(), True),
            StructField("null_count", LongType(), False),
            StructField("null_percentage", DoubleType(), False),
            StructField("distinct_count", LongType(), False),
            StructField("distinct_percentage", DoubleType(), False),
            StructField("min_value", StringType(), True),
            StructField("max_value", StringType(), True),
            StructField("mean_value", DoubleType(), True),
            StructField("median_value", DoubleType(), True),
            StructField("std_dev", DoubleType(), True),
            StructField("completeness", DoubleType(), True),
            StructField("uniqueness", DoubleType(), True),
            StructField("entropy", DoubleType(), True),
            StructField("is_id_column", BooleanType(), True),
            StructField("is_foreign_key", BooleanType(), True),
            StructField("is_timestamp", BooleanType(), True),
            StructField("naming_confidence", DoubleType(), True),
            StructField("is_unique_key", BooleanType(), True),
            StructField("is_constant", BooleanType(), True),
            StructField("sample_value", StringType(), True),
            StructField("created_timestamp", TimestampType(), False)
        ])
        
        df = self.spark.createDataFrame(rows, schema=schema)
        
        if self.lakehouse.check_if_table_exists(self.fact_column_profiles_table):
            self.lakehouse.write_to_table(df, self.fact_column_profiles_table, mode="append")
        else:
            self.lakehouse.write_to_table(df, self.fact_column_profiles_table, mode="overwrite")
    
    def _save_data_quality_facts(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save data quality facts."""
        rows = []
        
        # Table-level data quality
        if profile.data_quality_issues:
            for i, issue in enumerate(profile.data_quality_issues):
                row_data = [
                    str(uuid.uuid4()),
                    profile_id,
                    profile.dataset_name,
                    None,  # column_name
                    "TABLE",  # scope
                    issue.get("type", "UNKNOWN"),
                    issue.get("severity", "MEDIUM"),
                    issue.get("description", ""),
                    issue.get("recommendation", ""),
                    datetime.now()
                ]
                rows.append(row_data)
        
        # Column-level data quality issues
        for col_profile in profile.column_profiles:
            if col_profile.business_rules:
                for rule in col_profile.business_rules:
                    row_data = [
                        str(uuid.uuid4()),
                        profile_id,
                        profile.dataset_name,
                        col_profile.column_name,
                        "COLUMN",  # scope
                        rule.rule_type if hasattr(rule, 'rule_type') else "BUSINESS_RULE",
                        rule.severity if hasattr(rule, 'severity') else "MEDIUM",
                        rule.description if hasattr(rule, 'description') else "",
                        rule.recommendation if hasattr(rule, 'recommendation') else "",
                        datetime.now()
                    ]
                    rows.append(row_data)
        
        if rows:
            schema = StructType([
                StructField("quality_issue_id", StringType(), False),
                StructField("profile_id", StringType(), False),
                StructField("table_name", StringType(), False),
                StructField("column_name", StringType(), True),
                StructField("scope", StringType(), False),
                StructField("issue_type", StringType(), False),
                StructField("severity", StringType(), False),
                StructField("description", StringType(), True),
                StructField("recommendation", StringType(), True),
                StructField("created_timestamp", TimestampType(), False)
            ])
            
            df = self.spark.createDataFrame(rows, schema=schema)
            
            if self.lakehouse.check_if_table_exists(self.fact_data_quality_table):
                self.lakehouse.write_to_table(df, self.fact_data_quality_table, mode="append")
            else:
                self.lakehouse.write_to_table(df, self.fact_data_quality_table, mode="overwrite")
    
    def _save_relationship_facts(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save relationship facts with surrogate keys."""
        rows = []
        profile_date_key = self._get_profile_date_key()
        
        # Table-level correlations
        if profile.correlations:
            for correlation_key, correlation_value in profile.correlations.items():
                if isinstance(correlation_value, dict):
                    for col2, corr_val in correlation_value.items():
                        # Generate surrogate keys for both columns
                        source_column_key = self._generate_column_key(profile.dataset_name, correlation_key, profile_date_key)
                        target_column_key = self._generate_column_key(profile.dataset_name, col2, profile_date_key)
                        
                        row_data = [
                            str(uuid.uuid4()),
                            profile_id,
                            source_column_key,  # Add source column surrogate key
                            target_column_key,  # Add target column surrogate key
                            profile.dataset_name,
                            correlation_key,
                            col2,
                            "CORRELATION",
                            float(corr_val),
                            datetime.now()
                        ]
                        rows.append(row_data)
                else:
                    # Handle simple correlation format "col1:col2" -> value
                    if ":" in correlation_key:
                        col1, col2 = correlation_key.split(":", 1)
                        # Generate surrogate keys for both columns
                        source_column_key = self._generate_column_key(profile.dataset_name, col1, profile_date_key)
                        target_column_key = self._generate_column_key(profile.dataset_name, col2, profile_date_key)
                        
                        row_data = [
                            str(uuid.uuid4()),
                            profile_id,
                            source_column_key,  # Add source column surrogate key
                            target_column_key,  # Add target column surrogate key
                            profile.dataset_name,
                            col1,
                            col2,
                            "CORRELATION",
                            safe_numeric_convert(correlation_value),
                            datetime.now()
                        ]
                        rows.append(row_data)
        
        # Column-level relationships
        for col_profile in profile.column_profiles:
            if col_profile.relationships:
                for rel in col_profile.relationships:
                    # Generate surrogate keys
                    source_column_key = self._generate_column_key(profile.dataset_name, col_profile.column_name, profile_date_key)
                    target_column_key = None
                    if hasattr(rel, 'target_column') and rel.target_column:
                        # Assuming target might be in format "table.column" or just "column"
                        if "." in rel.target_column:
                            target_table, target_col = rel.target_column.rsplit(".", 1)
                            target_column_key = self._generate_column_key(target_table, target_col, profile_date_key)
                        else:
                            # Same table relationship
                            target_column_key = self._generate_column_key(profile.dataset_name, rel.target_column, profile_date_key)
                    
                    row_data = [
                        str(uuid.uuid4()),
                        profile_id,
                        source_column_key,  # Add source column surrogate key
                        target_column_key,  # Add target column surrogate key (nullable)
                        profile.dataset_name,
                        col_profile.column_name,
                        rel.target_column if hasattr(rel, 'target_column') else None,
                        rel.relationship_type.value if hasattr(rel, 'relationship_type') else "UNKNOWN",
                        safe_numeric_convert(rel.confidence) if hasattr(rel, 'confidence') else None,
                        datetime.now()
                    ]
                    rows.append(row_data)
        
        if rows:
            schema = StructType([
                StructField("relationship_id", StringType(), False),
                StructField("profile_id", StringType(), False),
                StructField("source_column_key", StringType(), False),  # Add source column surrogate key
                StructField("target_column_key", StringType(), True),   # Add target column surrogate key
                StructField("table_name", StringType(), False),
                StructField("source_column", StringType(), False),
                StructField("target_column", StringType(), True),
                StructField("relationship_type", StringType(), False),
                StructField("strength", DoubleType(), True),
                StructField("created_timestamp", TimestampType(), False)
            ])
            
            df = self.spark.createDataFrame(rows, schema=schema)
            
            if self.lakehouse.check_if_table_exists(self.fact_relationships_table):
                self.lakehouse.write_to_table(df, self.fact_relationships_table, mode="append")
            else:
                self.lakehouse.write_to_table(df, self.fact_relationships_table, mode="overwrite")
    
    def _save_column_values_bridge(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save top column values to bridge table with surrogate keys."""
        profile_date_key = self._get_profile_date_key()
        rows = []
        
        for col_profile in profile.column_profiles:
            column_key = self._generate_column_key(profile.dataset_name, col_profile.column_name, profile_date_key)
            
            # Save value distribution
            if col_profile.value_distribution:
                for value, count in col_profile.value_distribution.items():
                    row_data = [
                        str(uuid.uuid4()),
                        profile_id,
                        column_key,  # Surrogate key reference
                        profile.dataset_name,
                        col_profile.column_name,
                        str(value),
                        count,
                        "DISTRIBUTION",
                        datetime.now()
                    ]
                    rows.append(row_data)
            
            # Save top distinct values
            if col_profile.top_distinct_values:
                for i, value in enumerate(col_profile.top_distinct_values):
                    row_data = [
                        str(uuid.uuid4()),
                        profile_id,
                        column_key,  # Surrogate key reference
                        profile.dataset_name,
                        col_profile.column_name,
                        str(value),
                        None,  # count not available for top values
                        "TOP_VALUE",
                        datetime.now()
                    ]
                    rows.append(row_data)
        
        if rows:
            schema = StructType([
                StructField("column_value_id", StringType(), False),
                StructField("profile_id", StringType(), False),
                StructField("column_key", StringType(), False),  # Surrogate key reference
                StructField("table_name", StringType(), False),
                StructField("column_name", StringType(), False),
                StructField("value", StringType(), False),
                StructField("count", LongType(), True),
                StructField("value_type", StringType(), False),
                StructField("created_timestamp", TimestampType(), False)
            ])
            
            df = self.spark.createDataFrame(rows, schema=schema)
            
            if self.lakehouse.check_if_table_exists(self.bridge_column_values_table):
                self.lakehouse.write_to_table(df, self.bridge_column_values_table, mode="append")
            else:
                self.lakehouse.write_to_table(df, self.bridge_column_values_table, mode="overwrite")
    
    def _save_percentiles_bridge(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save percentiles to bridge table with surrogate keys."""
        profile_date_key = self._get_profile_date_key()
        rows = []
        
        for col_profile in profile.column_profiles:
            if col_profile.percentiles:
                column_key = self._generate_column_key(profile.dataset_name, col_profile.column_name, profile_date_key)
                for percentile, value in col_profile.percentiles.items():
                    row_data = [
                        str(uuid.uuid4()),
                        profile_id,
                        column_key,  # Surrogate key reference
                        profile.dataset_name,
                        col_profile.column_name,
                        int(percentile),
                        safe_numeric_convert(value),
                        datetime.now()
                    ]
                    rows.append(row_data)
        
        if rows:
            schema = StructType([
                StructField("percentile_id", StringType(), False),
                StructField("profile_id", StringType(), False),
                StructField("column_key", StringType(), False),  # Surrogate key reference
                StructField("table_name", StringType(), False),
                StructField("column_name", StringType(), False),
                StructField("percentile", IntegerType(), False),
                StructField("value", DoubleType(), True),
                StructField("created_timestamp", TimestampType(), False)
            ])
            
            df = self.spark.createDataFrame(rows, schema=schema)
            
            if self.lakehouse.check_if_table_exists(self.bridge_percentiles_table):
                self.lakehouse.write_to_table(df, self.bridge_percentiles_table, mode="append")
            else:
                self.lakehouse.write_to_table(df, self.bridge_percentiles_table, mode="overwrite")
    
    def _save_anomalies_bridge(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save anomalies to bridge table with surrogate keys."""
        if not profile.anomalies:
            return
        
        profile_date_key = self._get_profile_date_key()
        rows = []
        for anomaly in profile.anomalies:
            # Generate column_key if this is a column-level anomaly
            column_name = anomaly.get("column_name")
            column_key = None
            if column_name:
                column_key = self._generate_column_key(profile.dataset_name, column_name, profile_date_key)
            
            row_data = [
                str(uuid.uuid4()),
                profile_id,
                column_key,  # Surrogate key reference (can be null for table-level anomalies)
                profile.dataset_name,
                column_name,
                anomaly.get("anomaly_type", "UNKNOWN"),
                anomaly.get("severity", "MEDIUM"),
                anomaly.get("description", ""),
                anomaly.get("value"),
                safe_numeric_convert(anomaly.get("confidence", 0.0)),
                datetime.now()
            ]
            rows.append(row_data)
        
        schema = StructType([
            StructField("anomaly_id", StringType(), False),
            StructField("profile_id", StringType(), False),
            StructField("column_key", StringType(), True),  # Surrogate key reference (nullable for table-level anomalies)
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), True),
            StructField("anomaly_type", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("description", StringType(), True),
            StructField("value", StringType(), True),
            StructField("confidence", DoubleType(), False),
            StructField("created_timestamp", TimestampType(), False)
        ])
        
        df = self.spark.createDataFrame(rows, schema=schema)
        
        if self.lakehouse.check_if_table_exists(self.bridge_anomalies_table):
            self.lakehouse.write_to_table(df, self.bridge_anomalies_table, mode="append")
        else:
            self.lakehouse.write_to_table(df, self.bridge_anomalies_table, mode="overwrite")
    
    def _save_business_rules_bridge(self, profile: DatasetProfile, profile_id: str) -> None:
        """Save business rules to bridge table with surrogate keys."""
        profile_date_key = self._get_profile_date_key()
        rows = []
        
        for col_profile in profile.column_profiles:
            if col_profile.business_rules:
                column_key = self._generate_column_key(profile.dataset_name, col_profile.column_name, profile_date_key)
                for rule in col_profile.business_rules:
                    row_data = [
                        str(uuid.uuid4()),
                        profile_id,
                        column_key,  # Surrogate key reference
                        profile.dataset_name,
                        col_profile.column_name,
                        rule.rule_type if hasattr(rule, 'rule_type') else "BUSINESS_RULE",
                        rule.rule_expression if hasattr(rule, 'rule_expression') else "",
                        rule.description if hasattr(rule, 'description') else "",
                        rule.severity if hasattr(rule, 'severity') else "MEDIUM",
                        safe_numeric_convert(rule.confidence if hasattr(rule, 'confidence') else 1.0),
                        datetime.now()
                    ]
                    rows.append(row_data)
        
        if rows:
            schema = StructType([
                StructField("business_rule_id", StringType(), False),
                StructField("profile_id", StringType(), False),
                StructField("column_key", StringType(), False),  # Surrogate key reference
                StructField("table_name", StringType(), False),
                StructField("column_name", StringType(), False),
                StructField("rule_type", StringType(), False),
                StructField("rule_expression", StringType(), True),
                StructField("description", StringType(), True),
                StructField("severity", StringType(), False),
                StructField("confidence", DoubleType(), False),
                StructField("created_timestamp", TimestampType(), False)
            ])
            
            df = self.spark.createDataFrame(rows, schema=schema)
            
            if self.lakehouse.check_if_table_exists(self.bridge_business_rules_table):
                self.lakehouse.write_to_table(df, self.bridge_business_rules_table, mode="append")
            else:
                self.lakehouse.write_to_table(df, self.bridge_business_rules_table, mode="overwrite")
    
    def save_progress(self, progress: ScanProgress) -> None:
        """Save scan progress to fact table."""
        row_data = [
            str(uuid.uuid4()),
            progress.table_name,
            progress.level_1_completed.isoformat() if progress.level_1_completed else None,
            progress.level_2_completed.isoformat() if progress.level_2_completed else None,
            progress.level_3_completed.isoformat() if progress.level_3_completed else None,
            progress.level_4_completed.isoformat() if progress.level_4_completed else None,
            progress.level_1_duration_ms,
            progress.level_2_duration_ms,
            progress.level_3_duration_ms,
            progress.level_4_duration_ms,
            progress.last_error,
            progress.last_error_time.isoformat() if progress.last_error_time else None,
            datetime.now()
        ]
        
        schema = StructType([
            StructField("progress_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("level_1_completed", StringType(), True),
            StructField("level_2_completed", StringType(), True),
            StructField("level_3_completed", StringType(), True),
            StructField("level_4_completed", StringType(), True),
            StructField("level_1_duration_ms", IntegerType(), True),
            StructField("level_2_duration_ms", IntegerType(), True),
            StructField("level_3_duration_ms", IntegerType(), True),
            StructField("level_4_duration_ms", IntegerType(), True),
            StructField("last_error", StringType(), True),
            StructField("last_error_time", StringType(), True),
            StructField("created_timestamp", TimestampType(), False)
        ])
        
        df = self.spark.createDataFrame([row_data], schema=schema)
        
        # Upsert logic
        if self.lakehouse.check_if_table_exists(self.fact_scan_progress_table):
            existing_df = self.lakehouse.read_table(self.fact_scan_progress_table)
            filtered_df = existing_df.filter(f"table_name != '{progress.table_name}'")
            combined_df = filtered_df.unionByName(df)
            self.lakehouse.write_to_table(combined_df, self.fact_scan_progress_table, mode="overwrite")
        else:
            self.lakehouse.write_to_table(df, self.fact_scan_progress_table, mode="overwrite")
    
    def load_progress(self, table_name: str) -> Optional[ScanProgress]:
        """Load scan progress from fact table."""
        try:
            if not self.lakehouse.check_if_table_exists(self.fact_scan_progress_table):
                return None
                
            df = self.lakehouse.read_table(self.fact_scan_progress_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
            if result:
                row = result[0]
                return ScanProgress(
                    table_name=row.table_name,
                    level_1_completed=datetime.fromisoformat(row.level_1_completed) if row.level_1_completed else None,
                    level_2_completed=datetime.fromisoformat(row.level_2_completed) if row.level_2_completed else None,
                    level_3_completed=datetime.fromisoformat(row.level_3_completed) if row.level_3_completed else None,
                    level_4_completed=datetime.fromisoformat(row.level_4_completed) if row.level_4_completed else None,
                    level_1_duration_ms=row.level_1_duration_ms,
                    level_2_duration_ms=row.level_2_duration_ms,
                    level_3_duration_ms=row.level_3_duration_ms,
                    level_4_duration_ms=row.level_4_duration_ms,
                    last_error=row.last_error,
                    last_error_time=datetime.fromisoformat(row.last_error_time) if row.last_error_time else None
                )
        except Exception as e:
            print(f"Error loading progress for {table_name}: {e}")
            
        return None
    
    def load_profile(self, table_name: str, level: Optional[ScanLevel] = None) -> Optional[DatasetProfile]:
        """Load profile from fact and dimension tables by reconstructing DatasetProfile."""
        try:
            print(f"Loading profile for table: {table_name} at level: {level}")
            # Load table-level facts
            if not self.lakehouse.check_if_table_exists(self.fact_table_profiles_table):
                return None
                
            table_facts_df = self.lakehouse.read_table(self.fact_table_profiles_table)
            print(f"Table facts columns: {table_facts_df.columns}")
            print(f"Table facts count: {table_facts_df.count()}") 

            table_rows = table_facts_df.filter(f"table_name = '{table_name}'").collect()
            
            if not table_rows:
                print(f"No profile table_rows found for table: {table_name}")
                return None
                
            # Get the most recent profile
            table_row = sorted(table_rows, key=lambda x: x.created_timestamp, reverse=True)[0]
            
            # Load column-level facts
            if not self.lakehouse.check_if_table_exists(self.fact_column_profiles_table):
                print(f"Column profiles table does not exist: {self.fact_column_profiles_table}")
                return None
                
            column_facts_df = self.lakehouse.read_table(self.fact_column_profiles_table)
            column_rows = column_facts_df.filter(f"profile_id = '{table_row.profile_id}'").collect()
            
            # Reconstruct column profiles
            column_profiles = []
            for col_row in column_rows:
                # Generate column key for bridge data lookup using stored timestamp
                # Parse timestamp if it's a string
                if isinstance(table_row.profile_timestamp, str):
                    from datetime import datetime
                    timestamp = datetime.fromisoformat(table_row.profile_timestamp)
                else:
                    timestamp = table_row.profile_timestamp
                    
                if self.profile_grain == ProfileGrain.DAILY:
                    profile_date_key = timestamp.strftime("%Y-%m-%d")
                else:  # CONTINUOUS
                    profile_date_key = timestamp.isoformat()
                column_key = self._generate_column_key(table_row.table_name, col_row.column_name, profile_date_key)
                
                # Load bridge data for this column
                top_values = self._load_column_top_values(column_key)
                percentiles = self._load_column_percentiles(column_key)
                
                # Create ColumnProfile
                col_profile = ColumnProfile(
                    column_name=col_row.column_name,
                    data_type=col_row.data_type,
                    null_count=col_row.null_count,
                    null_percentage=col_row.null_percentage,
                    distinct_count=col_row.distinct_count,
                    distinct_percentage=col_row.distinct_percentage,
                    min_value=col_row.min_value,
                    max_value=col_row.max_value,
                    mean_value=col_row.mean_value,
                    median_value=col_row.median_value,
                    std_dev=col_row.std_dev,
                    completeness=col_row.completeness,
                    uniqueness=col_row.uniqueness,
                    entropy=col_row.entropy,
                    top_distinct_values=top_values,
                    percentiles=percentiles,
                    semantic_type=SemanticType(col_row.semantic_type) if col_row.semantic_type else None
                )
                column_profiles.append(col_profile)
            
            # Reconstruct DatasetProfile
            profile = DatasetProfile(
                dataset_name=table_row.table_name,
                row_count=table_row.row_count,
                column_count=table_row.column_count,
                profile_timestamp=table_row.profile_timestamp,
                column_profiles=column_profiles,
                data_quality_score=table_row.data_quality_score,
                null_count=table_row.null_count,
                duplicate_count=table_row.duplicate_count,
                statistics=json.loads(table_row.statistics) if table_row.statistics else None,
                recommendations=json.loads(table_row.recommendations) if table_row.recommendations else None,
                semantic_summary=json.loads(table_row.semantic_summary) if table_row.semantic_summary else {},
                business_glossary=json.loads(table_row.business_glossary) if table_row.business_glossary else {}
            )
            print(f"Loaded profile for table: {table_name} with {len(column_profiles)} columns")
            return profile
            
        except Exception as e:
            print(f"Error loading profile for {table_name}: {e}")
            self.logger.error(f"Error loading profile for {table_name}: {e}")
            return None
    
    def _load_column_top_values(self, column_key: str) -> Optional[List[Any]]:
        """Load top distinct values for a column from bridge table using surrogate key."""
        try:
            if not self.lakehouse.check_if_table_exists(self.bridge_column_values_table):
                return None
                
            df = self.lakehouse.read_table(self.bridge_column_values_table)
            rows = df.filter(f"column_key = '{column_key}' AND value_type = 'TOP_VALUE'") \
                     .limit(10) \
                     .collect()
            
            return [row.value for row in rows] if rows else None
            
        except Exception:
            return None
    
    def _load_column_percentiles(self, column_key: str) -> Optional[Dict[int, float]]:
        """Load percentiles for a column from bridge table using surrogate key."""
        try:
            if not self.lakehouse.check_if_table_exists(self.bridge_percentiles_table):
                return None
                
            df = self.lakehouse.read_table(self.bridge_percentiles_table)
            rows = df.filter(f"column_key = '{column_key}'").collect()
            
            if rows:
                return {row.percentile: row.value for row in rows}
            return None
            
        except Exception:
            return None
    
    def list_completed_tables(self, level: ScanLevel) -> List[str]:
        """List tables that completed a specific scan level."""
        level_columns = {
            ScanLevel.LEVEL_1_DISCOVERY: "level_1_completed",
            ScanLevel.LEVEL_2_SCHEMA: "level_2_completed", 
            ScanLevel.LEVEL_3_PROFILE: "level_3_completed",
            ScanLevel.LEVEL_4_ADVANCED: "level_4_completed"
        }
        
        try:
            if not self.lakehouse.check_if_table_exists(self.fact_scan_progress_table):
                return []
                
            df = self.lakehouse.read_table(self.fact_scan_progress_table)
            column_name = level_columns[level]
            completed_df = df.filter(f"{column_name} IS NOT NULL")
            return [row.table_name for row in completed_df.collect()]
            
        except Exception as e:
            print(f"Error listing completed tables: {e}")
            return []
    
    def load_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Load table metadata from dimension table."""
        try:
            if not self.lakehouse.check_if_table_exists(self.dim_tables_table):
                return None
                
            df = self.lakehouse.read_table(self.dim_tables_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
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
            print(f"Error loading table metadata for {table_name}: {e}")
            
        return None
    
    def load_schema_metadata(self, table_name: str) -> Optional[SchemaMetadata]:
        """Load schema metadata from dimension table."""
        try:
            if not self.lakehouse.check_if_table_exists(self.dim_columns_table):
                return None
                
            df = self.lakehouse.read_table(self.dim_columns_table)
            result = df.filter(f"table_name = '{table_name}'").collect()
            
            if result:
                from ..core.models.metadata import ColumnMetadata
                
                column_metadata = []
                columns_dict = []
                
                for row in result:
                    col_meta = ColumnMetadata(
                        name=row.column_name,
                        data_type=row.data_type,
                        nullable=row.nullable,
                        description=row.description,
                        is_partition=row.is_partition,
                        is_primary_key=row.is_primary_key,
                        is_foreign_key=row.is_foreign_key,
                        foreign_key_reference=row.foreign_key_reference,
                        default_value=row.default_value,
                        check_constraints=json.loads(row.check_constraints) if row.check_constraints else [],
                        properties=json.loads(row.properties) if row.properties else {}
                    )
                    column_metadata.append(col_meta)
                    
                    # Backward compatibility dict format
                    columns_dict.append({
                        "name": row.column_name,
                        "type": row.data_type,
                        "nullable": str(row.nullable)
                    })
                
                return SchemaMetadata(
                    table_name=table_name,
                    column_count=len(column_metadata),
                    columns=columns_dict,
                    column_metadata=column_metadata,
                    primary_key_candidates=[],  # Would need separate analysis
                    foreign_key_candidates=[]   # Would need separate analysis
                )
        except Exception as e:
            print(f"Error loading schema metadata for {table_name}: {e}")
            
        return None
    
    def clear_table_data(self, table_name: str) -> None:
        """Clear all data for a specific table from all tables."""
        tables_to_clear = [
            self.dim_tables_table,
            self.dim_columns_table,
            self.fact_table_profiles_table,
            self.fact_column_profiles_table,
            self.fact_data_quality_table,
            self.fact_relationships_table,
            self.fact_scan_progress_table,
            self.bridge_column_values_table,
            self.bridge_percentiles_table,
            self.bridge_anomalies_table,
            self.bridge_business_rules_table
        ]
        
        for table in tables_to_clear:
            try:
                if self.lakehouse.check_if_table_exists(table):
                    df = self.lakehouse.read_table(table)
                    filtered_df = df.filter(f"table_name != '{table_name}'")
                    self.lakehouse.write_to_table(filtered_df, table, mode="overwrite")
            except Exception as e:
                print(f"Warning: Could not clear data from {table}: {e}")

    def drop_tables(self) -> None:
        """Drop all profiling related tables."""
        tables_to_drop = [
            self.dim_tables_table,
            self.dim_columns_table,
            self.fact_table_profiles_table,
            self.fact_column_profiles_table,
            self.fact_data_quality_table,
            self.fact_relationships_table,
            self.fact_scan_progress_table,
            self.bridge_column_values_table,
            self.bridge_percentiles_table,
            self.bridge_anomalies_table,
            self.bridge_business_rules_table
        ]
        
        for table in tables_to_drop:
            try:
                if self.lakehouse.check_if_table_exists(table):
                    self.lakehouse.drop_table(table)
            except Exception as e:
                print(f"Warning: Could not drop table {table}: {e}")
    
    def list_all_profiles(self) -> List[Dict[str, Any]]:
        """List all available profiles with basic metadata."""
        try:
            if not self.lakehouse.check_if_table_exists(self.fact_table_profiles_table):
                self.logger.info(f"Profile table {self.fact_table_profiles_table} does not exist yet")
                return []
            
            df = self.lakehouse.read_table(self.fact_table_profiles_table)
            profiles = df.select(
                "table_name", 
                "profile_timestamp", 
                "row_count", 
                "column_count", 
                "data_quality_score"
            ).collect()
            
            result = []
            for row in profiles:
                try:
                    # Convert profile_timestamp string to datetime for formatting
                    profile_timestamp = datetime.fromisoformat(row.profile_timestamp.replace('Z', '+00:00')) if row.profile_timestamp else None
                    
                    result.append({
                        "table_name": row.table_name,
                        "scan_timestamp": profile_timestamp,
                        "row_count": row.row_count or 0,
                        "column_count": row.column_count or 0,
                        "quality_score": row.data_quality_score
                    })
                except Exception as e:
                    self.logger.warning(f"Could not parse profile data for {row.table_name}: {e}")
                    result.append({
                        "table_name": row.table_name,
                        "scan_timestamp": None,
                        "row_count": row.row_count or 0,
                        "column_count": row.column_count or 0,
                        "quality_score": row.data_quality_score
                    })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error listing profiles: {e}")
            return []