"""Level 3 Profile Scanner - Efficient single-pass profiling."""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingen_fab.packages.data_profiling.runtime.core.enums.profile_types import (
    ScanLevel,
    SemanticType,
)
from ingen_fab.packages.data_profiling.runtime.core.interfaces.persistence_interface import (
    PersistenceInterface,
)
from ingen_fab.packages.data_profiling.runtime.core.models.metadata import (
    ScanProgress,
    SchemaMetadata,
    TableMetadata,
)
from ingen_fab.packages.data_profiling.runtime.core.models.profile_models import (
    ColumnProfile,
    DatasetProfile,
    NamingPattern,
    ValueStatistics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

from .base_scan_level import BaseScanLevel


class Level3ProfileScanner(BaseScanLevel):
    """
    Level 3 Scanner: Efficient single-pass profiling.
    
    This scan performs efficient profiling of tables and columns in a single
    pass, collecting only statistics that can be computed efficiently without
    expensive operations (no percentiles, no top values, no entropy).
    
    Uses approximate distinct count (HyperLogLog) for memory efficiency.
    """
    
    @property
    def scan_level(self) -> ScanLevel:
        """Return the ScanLevel enum for this scan level."""
        return ScanLevel.LEVEL_3_PROFILE
    
    @property
    def scan_name(self) -> str:
        """Return human-readable name for this scan level."""
        return "LEVEL 3 SCAN: Single-Pass Statistical Profiling"
    
    @property
    def scan_description(self) -> str:
        """Return description of what this scan level does."""
        return "Statistical analysis, data quality metrics, and semantic type classification"
    
    def execute(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        sample_size: Optional[int] = None,
        **kwargs
    ) -> List[DatasetProfile]:
        """
        Execute Level 3 profiling scan.
        
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
                
                # Save profile results via centralized persistence
                self.persistence.save_profile(dataset_profile)
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
        
        # Use column_metadata if available (new format), otherwise columns (backward compatibility)
        columns_to_iterate = schema_metadata.column_metadata if schema_metadata.column_metadata else schema_metadata.columns
        
        for col_obj in columns_to_iterate:
            if hasattr(col_obj, 'name'):
                # New format: ColumnMetadata object
                col_name = col_obj.name
                col_type = col_obj.data_type
                nullable = col_obj.nullable
            else:
                # Old format: dict
                col_name = col_obj["name"]
                col_type = col_obj["type"]
                nullable = col_obj.get("nullable", True)
            
            # Track column info
            column_info[col_name] = {
                "name": col_name,
                "type": col_type,
                "nullable": nullable
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
        for col_obj in columns_to_iterate:
            if hasattr(col_obj, 'name'):
                # New format: ColumnMetadata object
                col_name = col_obj.name
                col_type = col_obj.data_type
            else:
                # Old format: dict
                col_name = col_obj["name"]
                col_type = col_obj["type"]
            
            # Extract statistics for this column
            count = stats_dict.get(f"{col_name}__count", 0)
            distinct = stats_dict.get(f"{col_name}__approx_distinct", 0)
            nulls = stats_dict.get(f"{col_name}__nulls", 0)
            
            # Create column profile with safe calculations
            col_profile = ColumnProfile(
                column_name=col_name,
                data_type=col_type,
                null_count=int(nulls) if nulls is not None else 0,
                null_percentage=(float(nulls) / actual_row_count * 100) if actual_row_count > 0 and nulls is not None else 0.0,
                distinct_count=int(distinct) if distinct is not None else 0,
                distinct_percentage=(float(distinct) / actual_row_count * 100) if actual_row_count > 0 and distinct is not None else 0.0,
                completeness=((actual_row_count - nulls) / actual_row_count) if actual_row_count > 0 and nulls is not None else 0.0,
                uniqueness=(distinct / count) if count and count > 0 and distinct is not None else 0.0
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
                        "min": stats_dict.get(f"{col_name}__min"),
                        "max": stats_dict.get(f"{col_name}__max"),
                        "mean": stats_dict.get(f"{col_name}__mean"),
                        "stddev": stats_dict.get(f"{col_name}__stddev"),
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
        
        print("    ✅ Single-pass statistics complete - no additional table scans needed")
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
    
    def get_scan_statistics(self) -> Dict[str, Any]:
        """Get statistics for Level 3 scan."""
        try:
            completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_3_PROFILE)
            return {
                "completed_tables": len(completed),
                "scan_level": "LEVEL_3_PROFILE"
            }
        except Exception as e:
            return {"error": str(e)}