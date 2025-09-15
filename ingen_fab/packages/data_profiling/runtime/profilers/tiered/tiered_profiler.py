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
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingen_fab.packages.data_profiling.runtime.core.enums.profile_types import (
    ProfileType,
    RelationshipType,
    ScanLevel,
    SemanticType,
)
from ingen_fab.packages.data_profiling.runtime.core.interfaces import (
    DataProfilingInterface,
)
from ingen_fab.packages.data_profiling.runtime.core.models import (
    ColumnProfile,
    DatasetProfile,
    SchemaMetadata,
    TableMetadata,
)
from ingen_fab.packages.data_profiling.runtime.core.models.relationships import (
    ColumnRelationship,
)
from ingen_fab.packages.data_profiling.runtime.core.models.statistics import (
    NamingPattern,
    ValueStatistics,
)
from ingen_fab.packages.data_profiling.runtime.persistence import PersistenceFactory
from ingen_fab.packages.data_profiling.runtime.profilers.tiered.scan_levels import (
    ScanCoordinator,
)
from ingen_fab.packages.data_profiling.runtime.utils.table_discovery import (
    TableDiscoveryService,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

"""Using core model definitions for TableMetadata, SchemaMetadata, ScanProgress.
Duplicate local dataclass definitions removed to avoid shadowing and divergence."""


"""Inline persistence implementation removed. Using centralized persistence layer."""


class TieredProfiler(DataProfilingInterface):
    """
    Tiered data profiler with 4 scan levels for progressive profiling.
    
    This profiler allows for incremental discovery and profiling of lakehouse
    tables with the ability to restart from any level.
    
    Implements DataProfilingInterface for compatibility with the registry system.
    """
    
    def __init__(
        self,
        target_lakehouse: lakehouse_utils,
        config_lakehouse: lakehouse_utils,
        table_prefix: str = "tiered_profile",
        spark: Optional[SparkSession] = None,
        exclude_views: bool = True,
        force_rescan: bool = True,
        profile_grain: str = "daily"
    ):
        """
        Initialize the tiered profiler.
        
        Args:
            target_lakehouse: lakehouse_utils instance
            config_lakehouse: lakehouse_utils instance for configuration
            table_prefix: Prefix for profile result tables in lakehouse
            spark: Optional SparkSession (will be created from lakehouse if not provided)
            exclude_views: Whether to exclude views from profiling (default: True)
            force_rescan: Whether to force rescan or resume from previous state
            profile_grain: Profile versioning control ("daily" or "continuous")
        """
        if force_rescan:
            self.resume = False
        else:
            self.resume = True

        self.target_lakehouse = target_lakehouse
        self.config_lakehouse = config_lakehouse    
 
        # Store configuration
        self.exclude_views = exclude_views
        self.profile_grain = profile_grain
        
        # Initialize persistence with enhanced lakehouse using factory
        self.persistence = PersistenceFactory.create_persistence(
            "enhanced_lakehouse",
            lakehouse=self.config_lakehouse,
            spark=self.config_lakehouse.spark,
            table_prefix=table_prefix,
            profile_grain=profile_grain
        )
        
        # Initialize table discovery service
        self.table_discovery = TableDiscoveryService(
            lakehouse=self.target_lakehouse,
            persistence=self.persistence,
            config_lakehouse=self.config_lakehouse  # Use same lakehouse for config by default
        )
        
        # Initialize scan coordinator
        self.scan_coordinator = ScanCoordinator(
            lakehouse=self.target_lakehouse,
            spark=self.target_lakehouse.spark,
            persistence=self.persistence,
            exclude_views=self.exclude_views
        )
    
    def scan_level_1_discovery(
        self,
        table_paths: Optional[List[str]] = None,
    ) -> List[TableMetadata]:
        """
        Level 1 Scan: Fast discovery of delta tables (metadata only).
        
        Delegates to ScanCoordinator for modular execution.
        
        Args:
            table_paths: Optional list of specific table paths to scan
            
        Returns:
            List of TableMetadata objects for discovered tables
        """
        return self.scan_coordinator.execute_scan_level(
            ScanLevel.LEVEL_1_DISCOVERY,
            table_names=table_paths,
            resume=self.resume
        )
    
    def scan_level_2_schema(
        self,
        table_names: Optional[List[str]] = None,
    ) -> List[SchemaMetadata]:
        """
        Level 2 Scan: Column metadata extraction.
        
        Delegates to ScanCoordinator for modular execution.
        
        Args:
            table_names: Optional list of specific tables to scan
            
        Returns:
            List of SchemaMetadata objects
        """
        return self.scan_coordinator.execute_scan_level(
            ScanLevel.LEVEL_2_SCHEMA,
            table_names=table_names,
            resume=self.resume
        )
    
    def scan_level_3_profile(
        self,
        table_names: Optional[List[str]] = None,
        sample_size: Optional[int] = None
    ) -> List[DatasetProfile]:
        """Delegates to ScanCoordinator for modular execution."""
        return self.scan_coordinator.execute_scan_level(
            ScanLevel.LEVEL_3_PROFILE,
            table_names=table_names,
            resume=self.resume,
            sample_size=sample_size
        )
    
    def scan_level_4_advanced(
        self,
        table_names: Optional[List[str]] = None
    ) -> List[DatasetProfile]:
        """Delegates to ScanCoordinator for modular execution."""
        return self.scan_coordinator.execute_scan_level(
            ScanLevel.LEVEL_4_ADVANCED,
            table_names=table_names,
            resume=self.resume
        )
    
    def get_explorer(self):
        """Get a ProfileExplorer instance for easy exploration of results."""
        # Lazy import to avoid hard dependency if explorer not available
        try:
            from ingen_fab.packages.data_profiling.runtime.utils.profile_explorer import (
                ProfileExplorer,
            )
            table_prefix = getattr(self.persistence, 'metadata_table', 'tiered_profile_metadata').replace("_metadata", "")
            return ProfileExplorer(self.target_lakehouse, table_prefix)
        except Exception:
            return None
    
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
            progress_df = self.config_lakehouse.read_table(self.persistence.progress_table)
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
    
    def profile_dataset(
        self,
        dataset: Any,
        profile_type: ProfileType = ProfileType.BASIC,
        columns: Optional[List[str]] = None,
        sample_size: Optional[float] = None,
    ) -> DatasetProfile:
        """
        Profile a dataset using the tiered profiling approach.
        
        This method provides compatibility with DataProfilingInterface.
        Maps ProfileType to appropriate scan levels.
        
        Args:
            dataset: Dataset to profile (table name or DataFrame)
            profile_type: Type of profiling to perform
            columns: Optional list of specific columns to profile
            sample_size: Optional sampling fraction (0-1) for Level 3+
            
        Returns:
            DatasetProfile with profiling results
        """
        # Convert dataset to table name if it's a string
        if isinstance(dataset, str):
            table_name = dataset
        else:
            # For DataFrames, create a temporary table
            temp_table_name = f"_temp_profile_{hashlib.md5(str(id(dataset)).encode()).hexdigest()[:8]}"
            dataset.createOrReplaceTempView(temp_table_name)
            table_name = temp_table_name
        
        # Map ProfileType to scan levels
        if profile_type == ProfileType.BASIC:
            # Run Level 1 and 2 only
            self.scan_level_1_discovery([table_name])
            self.scan_level_2_schema([table_name])
            
            # Convert to DatasetProfile
            table_metadata = self.persistence.load_table_metadata(table_name)
            schema_metadata = self.persistence.load_schema_metadata(table_name)
            
            if not table_metadata or not schema_metadata:
                raise ValueError(f"Failed to profile table {table_name}")
            
            # Build basic profile
            column_profiles = []
            for col_dict in schema_metadata.columns:
                col_profile = ColumnProfile(
                    column_name=col_dict["name"],
                    data_type=col_dict["type"],
                    null_count=0,  # Not available in basic scan
                    null_percentage=0.0,
                    distinct_count=0,  # Not available in basic scan
                    distinct_percentage=0.0,
                )
                column_profiles.append(col_profile)
            
            return DatasetProfile(
                dataset_name=table_name,
                row_count=table_metadata.row_count or 0,
                column_count=schema_metadata.column_count,
                column_profiles=column_profiles,
                profile_timestamp=datetime.now().isoformat(),
            )
        
        elif profile_type in [ProfileType.STATISTICAL, ProfileType.DATA_QUALITY, ProfileType.FULL]:
            # Run Level 1, 2, and 3
            self.scan_level_1_discovery([table_name])
            self.scan_level_2_schema([table_name])
            profiles = self.scan_level_3_profile([table_name])
            self.scan_level_4_advanced([table_name])
            
            if profiles:
                return profiles[0]
            else:
                raise ValueError(f"Failed to profile table {table_name}")
        
        elif profile_type in [ProfileType.CORRELATION, ProfileType.RELATIONSHIP]:
            # Run all levels including Level 4
            self.scan_level_1_discovery([table_name])
            self.scan_level_2_schema([table_name])
            self.scan_level_3_profile([table_name])
            advanced_profiles = self.scan_level_4_advanced([table_name])
            
            if advanced_profiles:
                return advanced_profiles[0]
            else:
                # Fall back to Level 3 results
                profiles = self.scan_level_3_profile([table_name])
                if profiles:
                    return profiles[0]
                else:
                    raise ValueError(f"Failed to profile table {table_name}")
        
        else:
            raise ValueError(f"Unsupported profile type: {profile_type}")
    
    def profile_column(
        self, 
        df: DataFrame, 
        column_name: str, 
        profile_type: ProfileType = ProfileType.BASIC
    ) -> ColumnProfile:
        """
        Profile a single column.
        
        This method provides compatibility with DataProfilingInterface.
        
        Args:
            df: DataFrame containing the column
            column_name: Name of the column to profile
            profile_type: Type of profiling to perform
            
        Returns:
            ColumnProfile for the specified column
        """
        # Select the column to profile
        column_df = df.select(column_name)
        
        # Get basic column information
        column_type = str(column_df.schema[column_name].dataType)
        
        # Create basic column profile
        column_profile = ColumnProfile(
            column_name=column_name,
            data_type=column_type,
            null_count=0,
            null_percentage=0.0,
            distinct_count=0,
            distinct_percentage=0.0
        )
        
        if profile_type in [ProfileType.RELATIONSHIP]:
            try:
                total_count = column_df.count()
                null_count = column_df.filter(F.col(column_name).isNull()).count()
                column_profile.null_count = null_count
                column_profile.null_percentage = (null_count / total_count * 100) if total_count > 0 else 0.0
                column_profile.distinct_count = column_df.select(column_name).distinct().count()
                column_profile.distinct_percentage = (column_profile.distinct_count / total_count * 100) if total_count > 0 else 0.0
            except Exception as e:
                print(f"Warning: Could not compute statistics for column {column_name}: {e}")
        
        return column_profile
    
    def compare_profiles(self, profile1: DatasetProfile, profile2: DatasetProfile) -> Dict[str, Any]:
        """
        Compare two dataset profiles and return differences.
        
        Args:
            profile1: First profile to compare
            profile2: Second profile to compare
            
        Returns:
            Dictionary containing comparison results
        """
        comparison = {
            "profile1_timestamp": profile1.profile_timestamp,
            "profile2_timestamp": profile2.profile_timestamp,
            "row_count_change": profile2.row_count - profile1.row_count,
            "row_count_change_pct": ((profile2.row_count - profile1.row_count) / profile1.row_count * 100) if profile1.row_count > 0 else 0,
            "column_count_change": profile2.column_count - profile1.column_count,
            "added_columns": [],
            "removed_columns": [],
            "changed_columns": [],
            "quality_score_change": None,
        }
        
        # Compare column profiles
        cols1 = {cp.column_name: cp for cp in profile1.column_profiles}
        cols2 = {cp.column_name: cp for cp in profile2.column_profiles}
        
        comparison["added_columns"] = [name for name in cols2.keys() if name not in cols1]
        comparison["removed_columns"] = [name for name in cols1.keys() if name not in cols2]
        
        # Compare quality scores if available
        if profile1.data_quality_score is not None and profile2.data_quality_score is not None:
            comparison["quality_score_change"] = profile2.data_quality_score - profile1.data_quality_score
        
        # Check for column-level changes
        for col_name in cols1.keys() & cols2.keys():  # Common columns
            col1, col2 = cols1[col_name], cols2[col_name]
            
            # Check for significant changes
            if (abs((col2.null_percentage or 0) - (col1.null_percentage or 0)) > 5 or
                abs((col2.distinct_percentage or 0) - (col1.distinct_percentage or 0)) > 10):
                comparison["changed_columns"].append({
                    "column_name": col_name,
                    "null_percentage_change": (col2.null_percentage or 0) - (col1.null_percentage or 0),
                    "distinct_percentage_change": (col2.distinct_percentage or 0) - (col1.distinct_percentage or 0),
                })
        
        return comparison
    
    def generate_quality_report(self, profile: DatasetProfile, output_format: str = "html") -> str:
        """
        Generate a quality report from a dataset profile.
        
        Args:
            profile: Dataset profile to generate report from
            output_format: Report format ('yaml', 'html', 'markdown', 'json')
            
        Returns:
            String containing the formatted report
        """
        if output_format.lower() == "yaml":
            return self._generate_yaml_report(profile)
        elif output_format.lower() == "json":
            return self._generate_json_report(profile)
        elif output_format.lower() == "markdown":
            return self._generate_markdown_report(profile)
        elif output_format.lower() == "html":
            return self._generate_html_report(profile)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
    
    def suggest_data_quality_rules(self, profile: DatasetProfile) -> List[Dict[str, Any]]:
        """
        Suggest data quality rules based on a dataset profile.
        
        Args:
            profile: Dataset profile to analyze
            
        Returns:
            List of suggested quality rules
        """
        rules = []
        
        for col_profile in profile.column_profiles:
            col_name = col_profile.column_name
            
            # Completeness rules
            if col_profile.null_percentage is not None and col_profile.null_percentage < 5:
                rules.append({
                    "type": "completeness",
                    "column": col_name,
                    "rule": f"Column '{col_name}' should be at least 95% complete",
                    "threshold": 0.95,
                    "current_value": (100 - col_profile.null_percentage) / 100,
                    "confidence": "high" if col_profile.null_percentage < 1 else "medium"
                })
            
            # Uniqueness rules for potential keys
            if (col_profile.uniqueness is not None and col_profile.uniqueness > 0.95 and
                hasattr(col_profile, 'naming_pattern') and col_profile.naming_pattern and 
                hasattr(col_profile.naming_pattern, 'is_id_column') and col_profile.naming_pattern.is_id_column):
                rules.append({
                    "type": "uniqueness",
                    "column": col_name,
                    "rule": f"Column '{col_name}' should be unique (appears to be an ID column)",
                    "threshold": 1.0,
                    "current_value": col_profile.uniqueness,
                    "confidence": "high" if col_profile.uniqueness > 0.99 else "medium"
                })
            
            # Value range rules for numeric columns
            if (col_profile.min_value is not None and col_profile.max_value is not None and
                self._is_numeric_type(col_profile.data_type)):
                rules.append({
                    "type": "value_range",
                    "column": col_name,
                    "rule": f"Column '{col_name}' values should be between {col_profile.min_value} and {col_profile.max_value}",
                    "min_value": col_profile.min_value,
                    "max_value": col_profile.max_value,
                    "confidence": "medium"
                })
            
            # Categorical value rules
            if (col_profile.distinct_count is not None and col_profile.distinct_count <= 20 and
                hasattr(col_profile, 'top_distinct_values') and col_profile.top_distinct_values):
                rules.append({
                    "type": "allowed_values",
                    "column": col_name,
                    "rule": f"Column '{col_name}' should contain only specific categorical values",
                    "allowed_values": col_profile.top_distinct_values[:10],
                    "confidence": "medium" if col_profile.distinct_count <= 10 else "low"
                })
        
        return rules
    
    def validate_against_rules(self, dataset: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a dataset against quality rules.
        
        Args:
            dataset: Dataset to validate (DataFrame or table name)
            rules: List of validation rules
            
        Returns:
            Dictionary containing validation results
        """
        # Convert dataset to DataFrame if needed
        if isinstance(dataset, str):
            df = self.lakehouse.read_table(dataset)
            dataset_name = dataset
        else:
            df = dataset
            dataset_name = "dataframe"
        
        validation_results = {
            "dataset": dataset_name,
            "validation_timestamp": datetime.now().isoformat(),
            "total_rules": len(rules),
            "passed_rules": 0,
            "failed_rules": 0,
            "violations": [],
            "success_rate": 0.0
        }
        
        for rule in rules:
            rule_type = rule.get("type", "unknown")
            column = rule.get("column")
            
            try:
                if rule_type == "completeness" and column:
                    # Check completeness
                    total_rows = df.count()
                    non_null_rows = df.filter(F.col(column).isNotNull()).count()
                    completeness = non_null_rows / total_rows if total_rows > 0 else 0
                    threshold = rule.get("threshold", 0.95)
                    
                    if completeness >= threshold:
                        validation_results["passed_rules"] += 1
                    else:
                        validation_results["failed_rules"] += 1
                        validation_results["violations"].append({
                            "rule": rule.get("rule", f"Completeness check for {column}"),
                            "type": rule_type,
                            "column": column,
                            "expected": threshold,
                            "actual": completeness,
                            "severity": "high" if completeness < threshold * 0.8 else "medium"
                        })
                
                elif rule_type == "uniqueness" and column:
                    # Check uniqueness
                    total_rows = df.count()
                    distinct_rows = df.select(column).distinct().count()
                    uniqueness = distinct_rows / total_rows if total_rows > 0 else 0
                    threshold = rule.get("threshold", 1.0)
                    
                    if uniqueness >= threshold:
                        validation_results["passed_rules"] += 1
                    else:
                        validation_results["failed_rules"] += 1
                        validation_results["violations"].append({
                            "rule": rule.get("rule", f"Uniqueness check for {column}"),
                            "type": rule_type,
                            "column": column,
                            "expected": threshold,
                            "actual": uniqueness,
                            "severity": "high"
                        })
                
                elif rule_type == "value_range" and column:
                    # Check value range
                    min_val = rule.get("min_value")
                    max_val = rule.get("max_value")
                    
                    if min_val is not None and max_val is not None:
                        out_of_range = df.filter(
                            (F.col(column) < min_val) | (F.col(column) > max_val)
                        ).count()
                        
                        if out_of_range == 0:
                            validation_results["passed_rules"] += 1
                        else:
                            validation_results["failed_rules"] += 1
                            validation_results["violations"].append({
                                "rule": rule.get("rule", f"Value range check for {column}"),
                                "type": rule_type,
                                "column": column,
                                "violation_count": out_of_range,
                                "severity": "medium"
                            })
                
                elif rule_type == "allowed_values" and column:
                    # Check allowed values
                    allowed = rule.get("allowed_values", [])
                    if allowed:
                        invalid_values = df.filter(~F.col(column).isin(allowed)).count()
                        
                        if invalid_values == 0:
                            validation_results["passed_rules"] += 1
                        else:
                            validation_results["failed_rules"] += 1
                            validation_results["violations"].append({
                                "rule": rule.get("rule", f"Allowed values check for {column}"),
                                "type": rule_type,
                                "column": column,
                                "violation_count": invalid_values,
                                "severity": "medium"
                            })
                else:
                    # Unknown rule type - skip
                    validation_results["passed_rules"] += 1
                    
            except Exception as e:
                validation_results["failed_rules"] += 1
                validation_results["violations"].append({
                    "rule": rule.get("rule", "Unknown rule"),
                    "type": rule_type,
                    "column": column,
                    "error": str(e),
                    "severity": "high"
                })
        
        # Calculate success rate
        total_rules = validation_results["total_rules"]
        if total_rules > 0:
            validation_results["success_rate"] = (validation_results["passed_rules"] / total_rules) * 100
        
        return validation_results
    
    def _is_numeric_type(self, type_str: str) -> bool:
        """Check if a type string represents a numeric type."""
        numeric_types = ['int', 'long', 'float', 'double', 'decimal', 'bigint', 'smallint', 'tinyint']
        return any(t in type_str.lower() for t in numeric_types)
    
    def _generate_yaml_report(self, profile: DatasetProfile) -> str:
        """Generate a YAML format report."""
        import yaml
        
        # Convert profile to dictionary for YAML serialization
        def safe_convert(obj):
            if hasattr(obj, '__dict__'):
                if hasattr(obj, '__dataclass_fields__'):  # Dataclass
                    return {k: safe_convert(v) for k, v in obj.__dict__.items()}
                else:
                    return str(obj)
            elif hasattr(obj, 'value'):  # Enum
                return obj.value
            elif isinstance(obj, (datetime, date)):
                return obj.isoformat()
            elif isinstance(obj, list):
                return [safe_convert(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: safe_convert(v) for k, v in obj.items()}
            else:
                return obj
        
        profile_dict = safe_convert(profile)
        return yaml.dump(profile_dict, default_flow_style=False, sort_keys=False)
    
    def _generate_json_report(self, profile: DatasetProfile) -> str:
        """Generate a JSON format report."""
        def json_serializer(obj):
            if hasattr(obj, '__dict__'):
                if hasattr(obj, '__dataclass_fields__'):
                    return obj.__dict__
                else:
                    return str(obj)
            elif hasattr(obj, 'value'):
                return obj.value
            elif isinstance(obj, (datetime, date)):
                return obj.isoformat()
            else:
                return str(obj)
        
        return json.dumps(profile.__dict__, indent=2, default=json_serializer)
    
    def _generate_markdown_report(self, profile: DatasetProfile) -> str:
        """Generate a Markdown format report."""
        report = f"""# Data Profile Report

**Dataset:** {profile.dataset_name}
**Timestamp:** {profile.profile_timestamp}
**Rows:** {profile.row_count:,}
**Columns:** {profile.column_count}
"""
        
        if profile.data_quality_score is not None:
            report += f"**Quality Score:** {profile.data_quality_score:.2%}\n"
        
        report += "\n## Column Profiles\n\n"
        
        for col in profile.column_profiles:
            report += f"### {col.column_name}\n"
            report += f"- **Type:** {col.data_type}\n"
            report += f"- **Null Count:** {col.null_count:,} ({col.null_percentage:.1f}%)\n"
            report += f"- **Distinct Count:** {col.distinct_count:,} ({col.distinct_percentage:.1f}%)\n"
            
            if col.min_value is not None:
                report += f"- **Min Value:** {col.min_value}\n"
            if col.max_value is not None:
                report += f"- **Max Value:** {col.max_value}\n"
            if col.mean_value is not None:
                report += f"- **Mean Value:** {col.mean_value:.2f}\n"
            
            report += "\n"
        
        return report
    
    def _generate_html_report(self, profile: DatasetProfile) -> str:
        """Generate an HTML format report."""
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Profile Report - {profile.dataset_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
        .column {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .metric {{ margin: 5px 0; }}
        .quality-good {{ color: green; }}
        .quality-warning {{ color: orange; }}
        .quality-poor {{ color: red; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Profile Report</h1>
        <p><strong>Dataset:</strong> {profile.dataset_name}</p>
        <p><strong>Timestamp:</strong> {profile.profile_timestamp}</p>
        <p><strong>Rows:</strong> {profile.row_count:,}</p>
        <p><strong>Columns:</strong> {profile.column_count}</p>
"""
        
        if profile.data_quality_score is not None:
            quality_class = ("quality-good" if profile.data_quality_score > 0.9 
                           else "quality-warning" if profile.data_quality_score > 0.7 
                           else "quality-poor")
            html += f'        <p><strong>Quality Score:</strong> <span class="{quality_class}">{profile.data_quality_score:.2%}</span></p>\n'
        
        html += """    </div>
    
    <h2>Column Profiles</h2>
"""
        
        for col in profile.column_profiles:
            html += f"""    <div class="column">
        <h3>{col.column_name}</h3>
        <div class="metric"><strong>Type:</strong> {col.data_type}</div>
        <div class="metric"><strong>Null Count:</strong> {col.null_count:,} ({col.null_percentage:.1f}%)</div>
        <div class="metric"><strong>Distinct Count:</strong> {col.distinct_count:,} ({col.distinct_percentage:.1f}%)</div>
"""
            
            if col.min_value is not None:
                html += f'        <div class="metric"><strong>Min Value:</strong> {col.min_value}</div>\n'
            if col.max_value is not None:
                html += f'        <div class="metric"><strong>Max Value:</strong> {col.max_value}</div>\n'
            if col.mean_value is not None:
                html += f'        <div class="metric"><strong>Mean Value:</strong> {col.mean_value:.2f}</div>\n'
            
            html += "    </div>\n"
        
        html += """</body>
</html>"""
        
        return html

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
                metadata_table = getattr(self.persistence, 'metadata_table', 'tiered_profile_metadata')
                return self.lakehouse.read_table(metadata_table)
            elif scan_level == "schema":
                schema_table = getattr(self.persistence, 'schema_table', 'tiered_profile_schemas')
                return self.lakehouse.read_table(schema_table)
            elif scan_level == "progress":
                progress_table = getattr(self.persistence, 'progress_table', 'tiered_profile_progress')
                return self.lakehouse.read_table(progress_table)
            else:
                print(f"  ❌ Invalid scan level: {scan_level}")
                return None
        except Exception as e:
            print(f"  ⚠️  Could not read {scan_level} results: {e}")
            return None
        

