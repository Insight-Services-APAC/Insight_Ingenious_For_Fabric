"""PySpark implementation of data profiling using native Spark functions."""

from typing import Any, Dict, List, Optional
from datetime import datetime
import json
from ingen_fab.python_libs.interfaces.data_profiling_interface import (
    DataProfilingInterface,
    DatasetProfile,
    ColumnProfile,
    ProfileType
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
import ingen_fab.python_libs.common.config_utils as cu


class DataProfilingPySpark(DataProfilingInterface):
    """PySpark implementation of data profiling interface."""
    
    def __init__(self, spark: Optional[SparkSession] = None, lakehouse: Optional[lakehouse_utils] = None):
        """Initialize the PySpark data profiler."""
        self.spark = spark or SparkSession.builder.getOrCreate()
        
        # Initialize lakehouse utils if not provided
        if lakehouse:
            self.lakehouse = lakehouse
        else:
            # Try to get configs and create lakehouse_utils
            try:
                configs = cu.get_configs_as_object()
                self.lakehouse = lakehouse_utils(
                    target_workspace_id=configs.config_workspace_id,
                    target_lakehouse_id=configs.config_lakehouse_id
                )
            except Exception:
                # If configs not available, lakehouse operations won't work
                self.lakehouse = None
    
    def profile_dataset(
        self,
        dataset: Any,
        profile_type: ProfileType = ProfileType.BASIC,
        columns: Optional[List[str]] = None,
        sample_size: Optional[float] = None
    ) -> DatasetProfile:
        """Profile a dataset using PySpark."""
        # Convert dataset to DataFrame if needed
        df = self._get_dataframe(dataset)
        
        # Apply sampling if specified
        if sample_size and 0 < sample_size < 1:
            df = df.sample(fraction=sample_size, seed=42)
        
        # Filter columns if specified
        if columns:
            df = df.select(*columns)
        
        # Get basic statistics
        row_count = df.count()
        column_count = len(df.columns)
        
        # Profile each column
        column_profiles = []
        for col_name in df.columns:
            col_profile = self.profile_column(df, col_name, profile_type)
            column_profiles.append(col_profile)
        
        # Calculate overall null count
        total_null_count = sum(col_profile.null_count for col_profile in column_profiles)
        
        # Calculate duplicate count
        duplicate_count = df.count() - df.distinct().count()
        
        # Create dataset profile
        profile = DatasetProfile(
            dataset_name=self._get_dataset_name(dataset),
            row_count=row_count,
            column_count=column_count,
            profile_timestamp=datetime.now().isoformat(),
            column_profiles=column_profiles,
            null_count=total_null_count,
            duplicate_count=duplicate_count
        )
        
        # Add advanced profiling based on type
        if profile_type in [ProfileType.CORRELATION, ProfileType.FULL]:
            profile.correlations = self._calculate_correlations(df)
        
        if profile_type in [ProfileType.DATA_QUALITY, ProfileType.FULL]:
            profile.data_quality_score = self._calculate_quality_score(profile)
            profile.anomalies = self._detect_anomalies(df)
            profile.recommendations = self._generate_recommendations(profile)
            profile.data_quality_issues = self._detect_data_quality_issues(df, column_profiles)
        
        # Add statistics for statistical profiling
        if profile_type in [ProfileType.STATISTICAL, ProfileType.FULL]:
            profile.statistics = self._calculate_dataset_statistics(df, column_profiles)
        
        return profile
    
    def profile_column(
        self,
        dataset: Any,
        column_name: str,
        profile_type: ProfileType = ProfileType.BASIC
    ) -> ColumnProfile:
        """Profile a single column using PySpark."""
        df = self._get_dataframe(dataset)
        
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in dataset")
        
        col_df = df.select(column_name)
        total_count = df.count()
        
        # Handle empty tables
        if total_count == 0:
            # Return minimal profile for empty table
            return ColumnProfile(
                column_name=column_name,
                data_type=str(df.schema[column_name].dataType),
                null_count=0,
                null_percentage=0.0,
                distinct_count=0,
                distinct_percentage=0.0,
                completeness=0.0,
                uniqueness=0.0
            )
        
        # Basic statistics
        null_count = col_df.filter(F.col(column_name).isNull()).count()
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        
        distinct_count = col_df.select(column_name).distinct().count()
        distinct_percentage = (distinct_count / total_count * 100) if total_count > 0 else 0
        
        # Create base profile
        profile = ColumnProfile(
            column_name=column_name,
            data_type=str(df.schema[column_name].dataType),
            null_count=null_count,
            null_percentage=null_percentage,
            distinct_count=distinct_count,
            distinct_percentage=distinct_percentage,
            completeness=1 - (null_count / total_count) if total_count > 0 else 0,
            uniqueness=distinct_count / total_count if total_count > 0 else 0
        )
        
        # Add type-specific statistics
        col_type = df.schema[column_name].dataType
        
        if isinstance(col_type, NumericType):
            # Use stddev_samp which returns null for single values instead of causing divide by zero
            stats_result = col_df.select(
                F.min(column_name).alias("min"),
                F.max(column_name).alias("max"),
                F.mean(column_name).alias("mean"),
                F.coalesce(F.stddev_samp(column_name), F.lit(0)).alias("stddev")
            ).collect()
            
            if stats_result:
                stats = stats_result[0]
                profile.min_value = stats["min"]
                profile.max_value = stats["max"]
                profile.mean_value = stats["mean"]
                profile.std_dev = stats["stddev"]
                
                if profile_type in [ProfileType.STATISTICAL, ProfileType.FULL]:
                    # Calculate median (approximation for large datasets)
                    quantiles = col_df.approxQuantile(column_name, [0.5], 0.01)
                    if quantiles:
                        profile.median_value = quantiles[0]
        
        elif isinstance(col_type, StringType):
            # Get min/max string lengths
            length_stats_result = col_df.select(
                F.min(F.length(column_name)).alias("min_length"),
                F.max(F.length(column_name)).alias("max_length"),
                F.coalesce(F.avg(F.length(column_name)), F.lit(0)).alias("avg_length")
            ).collect()
            
            if length_stats_result:
                length_stats = length_stats_result[0]
                profile.min_value = length_stats["min_length"]
                profile.max_value = length_stats["max_length"]
                profile.mean_value = length_stats["avg_length"]
        
        elif isinstance(col_type, (DateType, TimestampType)):
            date_stats_result = col_df.select(
                F.min(column_name).alias("min"),
                F.max(column_name).alias("max")
            ).collect()
            
            if date_stats_result:
                date_stats = date_stats_result[0]
                profile.min_value = date_stats["min"]
                profile.max_value = date_stats["max"]
        
        # Add value distribution for categorical columns
        if profile_type in [ProfileType.DISTRIBUTION, ProfileType.FULL]:
            if distinct_count <= 100:  # Only for low-cardinality columns
                value_counts = col_df.groupBy(column_name).count().collect()
                profile.value_distribution = {
                    row[column_name]: row["count"] for row in value_counts
                }
        
        return profile
    
    def compare_profiles(
        self,
        profile1: DatasetProfile,
        profile2: DatasetProfile
    ) -> Dict[str, Any]:
        """Compare two dataset profiles."""
        comparison = {
            "row_count_change": profile2.row_count - profile1.row_count,
            "row_count_change_pct": ((profile2.row_count - profile1.row_count) / profile1.row_count * 100) 
                                    if profile1.row_count > 0 else 0,
            "column_count_change": profile2.column_count - profile1.column_count,
            "column_changes": [],
            "quality_score_change": None
        }
        
        # Compare columns
        profile1_cols = {cp.column_name: cp for cp in profile1.column_profiles}
        profile2_cols = {cp.column_name: cp for cp in profile2.column_profiles}
        
        # Find added/removed columns
        added_columns = set(profile2_cols.keys()) - set(profile1_cols.keys())
        removed_columns = set(profile1_cols.keys()) - set(profile2_cols.keys())
        common_columns = set(profile1_cols.keys()) & set(profile2_cols.keys())
        
        comparison["added_columns"] = list(added_columns)
        comparison["removed_columns"] = list(removed_columns)
        
        # Compare common columns
        for col in common_columns:
            col1 = profile1_cols[col]
            col2 = profile2_cols[col]
            
            col_change = {
                "column": col,
                "null_change": col2.null_count - col1.null_count,
                "distinct_change": col2.distinct_count - col1.distinct_count,
                "completeness_change": col2.completeness - col1.completeness if col2.completeness and col1.completeness else None
            }
            
            if col1.mean_value is not None and col2.mean_value is not None:
                col_change["mean_drift"] = col2.mean_value - col1.mean_value
            
            comparison["column_changes"].append(col_change)
        
        # Compare quality scores if available
        if profile1.data_quality_score and profile2.data_quality_score:
            comparison["quality_score_change"] = profile2.data_quality_score - profile1.data_quality_score
        
        return comparison
    
    def generate_quality_report(
        self,
        profile: DatasetProfile,
        output_format: str = "html"
    ) -> str:
        """Generate a formatted quality report."""
        if output_format == "json":
            return self._generate_json_report(profile)
        elif output_format == "markdown":
            return self._generate_markdown_report(profile)
        elif output_format == "html":
            return self._generate_html_report(profile)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def suggest_data_quality_rules(
        self,
        profile: DatasetProfile
    ) -> List[Dict[str, Any]]:
        """Suggest data quality rules based on profiling."""
        rules = []
        
        for col_profile in profile.column_profiles:
            # Completeness rule for columns with high completeness
            if col_profile.completeness and col_profile.completeness > 0.95:
                rules.append({
                    "type": "completeness",
                    "column": col_profile.column_name,
                    "threshold": 0.95,
                    "description": f"Column '{col_profile.column_name}' should have at least 95% completeness"
                })
            
            # Uniqueness rule for columns with high uniqueness
            if col_profile.uniqueness and col_profile.uniqueness > 0.95:
                rules.append({
                    "type": "uniqueness",
                    "column": col_profile.column_name,
                    "threshold": 0.95,
                    "description": f"Column '{col_profile.column_name}' should have at least 95% unique values"
                })
            
            # Range rules for numeric columns
            if col_profile.min_value is not None and col_profile.max_value is not None:
                if "int" in col_profile.data_type.lower() or "float" in col_profile.data_type.lower():
                    rules.append({
                        "type": "range",
                        "column": col_profile.column_name,
                        "min": col_profile.min_value,
                        "max": col_profile.max_value,
                        "description": f"Column '{col_profile.column_name}' values should be between {col_profile.min_value} and {col_profile.max_value}"
                    })
        
        return rules
    
    def validate_against_rules(
        self,
        dataset: Any,
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate dataset against quality rules."""
        df = self._get_dataframe(dataset)
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "total_rules": len(rules),
            "passed_rules": 0,
            "failed_rules": 0,
            "violations": []
        }
        
        for rule in rules:
            if rule["type"] == "completeness":
                col = rule["column"]
                threshold = rule["threshold"]
                total_count = df.count()
                if total_count > 0:
                    actual_completeness = 1 - (df.filter(F.col(col).isNull()).count() / total_count)
                else:
                    actual_completeness = 0
                
                if actual_completeness < threshold:
                    validation_results["failed_rules"] += 1
                    validation_results["violations"].append({
                        "rule": rule["description"],
                        "expected": threshold,
                        "actual": actual_completeness
                    })
                else:
                    validation_results["passed_rules"] += 1
            
            elif rule["type"] == "uniqueness":
                col = rule["column"]
                threshold = rule["threshold"]
                total_count = df.count()
                if total_count > 0:
                    actual_uniqueness = df.select(col).distinct().count() / total_count
                else:
                    actual_uniqueness = 0
                
                if actual_uniqueness < threshold:
                    validation_results["failed_rules"] += 1
                    validation_results["violations"].append({
                        "rule": rule["description"],
                        "expected": threshold,
                        "actual": actual_uniqueness
                    })
                else:
                    validation_results["passed_rules"] += 1
            
            elif rule["type"] == "range":
                col = rule["column"]
                min_val = rule["min"]
                max_val = rule["max"]
                violations = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
                
                if violations > 0:
                    validation_results["failed_rules"] += 1
                    validation_results["violations"].append({
                        "rule": rule["description"],
                        "violation_count": violations
                    })
                else:
                    validation_results["passed_rules"] += 1
        
        validation_results["success_rate"] = (
            validation_results["passed_rules"] / validation_results["total_rules"] * 100
        ) if validation_results["total_rules"] > 0 else 0
        
        return validation_results
    
    # Helper methods
    def _get_dataframe(self, dataset: Any) -> DataFrame:
        """Convert various input types to DataFrame using lakehouse_utils when possible."""
        if isinstance(dataset, DataFrame):
            return dataset
        elif isinstance(dataset, str):
            # Check if it's a file path or table name
            if "." in dataset and dataset.split(".")[-1] in ["parquet", "delta", "csv", "json"]:
                # File path - use spark directly for file access
                file_format = dataset.split(".")[-1]
                if file_format == "delta":
                    return self.spark.read.format("delta").load(dataset)
                else:
                    return self.spark.read.format(file_format).load(dataset)
            else:
                # Table name - use lakehouse_utils if available
                if self.lakehouse:
                    try:
                        # Try to read using lakehouse_utils
                        return self.lakehouse.read_table(table_name=dataset)
                    except Exception:
                        # Fall back to spark.table if lakehouse read fails
                        return self.spark.table(dataset)
                else:
                    # No lakehouse available, use spark directly
                    return self.spark.table(dataset)
        else:
            raise ValueError(f"Unsupported dataset type: {type(dataset)}")
    
    def _get_dataset_name(self, dataset: Any) -> str:
        """Extract dataset name from various input types."""
        if isinstance(dataset, str):
            return dataset.split("/")[-1].split(".")[0]
        elif isinstance(dataset, DataFrame):
            return "dataframe"
        else:
            return "unknown"
    
    def _calculate_correlations(self, df: DataFrame) -> Dict[str, Dict[str, float]]:
        """Calculate correlations between numeric columns."""
        numeric_cols = [
            field.name for field in df.schema.fields 
            if isinstance(field.dataType, NumericType)
        ]
        
        correlations = {}
        for col1 in numeric_cols:
            correlations[col1] = {}
            for col2 in numeric_cols:
                if col1 != col2:
                    corr = df.stat.corr(col1, col2)
                    correlations[col1][col2] = corr if corr is not None else 0
        
        return correlations
    
    def _calculate_quality_score(self, profile: DatasetProfile) -> float:
        """Calculate overall data quality score."""
        scores = []
        
        for col_profile in profile.column_profiles:
            # Completeness contributes to quality
            if col_profile.completeness is not None:
                scores.append(col_profile.completeness)
            
            # Low null percentage is good
            scores.append(1 - (col_profile.null_percentage / 100))
        
        return sum(scores) / len(scores) if scores else 0
    
    def _detect_anomalies(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Detect potential anomalies in the data."""
        anomalies = []
        
        # Check for columns with all nulls
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count == df.count():
                anomalies.append({
                    "type": "all_nulls",
                    "column": col,
                    "severity": "high"
                })
        
        # Check for duplicate rows
        duplicate_count = df.count() - df.distinct().count()
        if duplicate_count > 0:
            anomalies.append({
                "type": "duplicate_rows",
                "count": duplicate_count,
                "severity": "medium"
            })
        
        return anomalies
    
    def _generate_recommendations(self, profile: DatasetProfile) -> List[str]:
        """Generate recommendations based on profiling results."""
        recommendations = []
        
        for col_profile in profile.column_profiles:
            # Recommend handling high null percentages
            if col_profile.null_percentage > 50:
                recommendations.append(
                    f"Consider removing or imputing column '{col_profile.column_name}' "
                    f"with {col_profile.null_percentage:.1f}% null values"
                )
            
            # Recommend indexing for high cardinality columns
            if col_profile.uniqueness and col_profile.uniqueness > 0.8:
                recommendations.append(
                    f"Consider indexing column '{col_profile.column_name}' "
                    f"with {col_profile.uniqueness:.1%} uniqueness"
                )
        
        # Check for anomalies
        if profile.anomalies:
            for anomaly in profile.anomalies:
                if anomaly["type"] == "duplicate_rows":
                    recommendations.append(
                        f"Remove {anomaly['count']} duplicate rows to improve data quality"
                    )
        
        return recommendations
    
    def _generate_json_report(self, profile: DatasetProfile) -> str:
        """Generate JSON format report."""
        report_dict = {
            "dataset_name": profile.dataset_name,
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "profile_timestamp": profile.profile_timestamp,
            "data_quality_score": profile.data_quality_score,
            "columns": []
        }
        
        for col_profile in profile.column_profiles:
            col_dict = {
                "name": col_profile.column_name,
                "type": col_profile.data_type,
                "null_percentage": col_profile.null_percentage,
                "distinct_count": col_profile.distinct_count,
                "completeness": col_profile.completeness
            }
            if col_profile.min_value is not None:
                col_dict["min"] = col_profile.min_value
            if col_profile.max_value is not None:
                col_dict["max"] = col_profile.max_value
            if col_profile.mean_value is not None:
                col_dict["mean"] = col_profile.mean_value
            
            report_dict["columns"].append(col_dict)
        
        if profile.recommendations:
            report_dict["recommendations"] = profile.recommendations
        
        return json.dumps(report_dict, indent=2, default=str)
    
    def _generate_markdown_report(self, profile: DatasetProfile) -> str:
        """Generate Markdown format report."""
        lines = [
            f"# Data Profile Report: {profile.dataset_name}",
            f"\n**Generated:** {profile.profile_timestamp}",
            f"\n## Dataset Overview",
            f"- **Total Rows:** {profile.row_count:,}",
            f"- **Total Columns:** {profile.column_count}",
        ]
        
        if profile.data_quality_score:
            lines.append(f"- **Data Quality Score:** {profile.data_quality_score:.2%}")
        
        lines.append("\n## Column Profiles\n")
        lines.append("| Column | Type | Nulls (%) | Distinct | Completeness |")
        lines.append("|--------|------|-----------|----------|--------------|")
        
        for col in profile.column_profiles:
            lines.append(
                f"| {col.column_name} | {col.data_type} | "
                f"{col.null_percentage:.1f}% | {col.distinct_count:,} | "
                f"{col.completeness:.2%} |"
            )
        
        if profile.recommendations:
            lines.append("\n## Recommendations\n")
            for rec in profile.recommendations:
                lines.append(f"- {rec}")
        
        return "\n".join(lines)
    
    def _generate_html_report(self, profile: DatasetProfile) -> str:
        """Generate HTML format report."""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Profile Report: {profile.dataset_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .metric {{ margin: 10px 0; }}
                .recommendation {{ background-color: #fffbcc; padding: 10px; margin: 5px 0; }}
            </style>
        </head>
        <body>
            <h1>Data Profile Report: {profile.dataset_name}</h1>
            <p><strong>Generated:</strong> {profile.profile_timestamp}</p>
            
            <h2>Dataset Overview</h2>
            <div class="metric">Total Rows: {profile.row_count:,}</div>
            <div class="metric">Total Columns: {profile.column_count}</div>
        """
        
        if profile.data_quality_score:
            html += f'<div class="metric">Data Quality Score: {profile.data_quality_score:.2%}</div>'
        
        html += """
            <h2>Column Profiles</h2>
            <table>
                <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nulls (%)</th>
                    <th>Distinct</th>
                    <th>Completeness</th>
                </tr>
        """
        
        for col in profile.column_profiles:
            html += f"""
                <tr>
                    <td>{col.column_name}</td>
                    <td>{col.data_type}</td>
                    <td>{col.null_percentage:.1f}%</td>
                    <td>{col.distinct_count:,}</td>
                    <td>{col.completeness:.2%}</td>
                </tr>
            """
        
        html += "</table>"
        
        if profile.recommendations:
            html += "<h2>Recommendations</h2>"
            for rec in profile.recommendations:
                html += f'<div class="recommendation">{rec}</div>'
        
        html += "</body></html>"
        
        return html
    
    def _detect_data_quality_issues(self, df: DataFrame, column_profiles: List[ColumnProfile]) -> List[Dict[str, Any]]:
        """Detect data quality issues in the dataset."""
        issues = []
        
        for col_profile in column_profiles:
            # High null percentage
            if col_profile.null_percentage > 50:
                issues.append({
                    "type": "high_null_percentage",
                    "column": col_profile.column_name,
                    "value": col_profile.null_percentage,
                    "severity": "high" if col_profile.null_percentage > 75 else "medium",
                    "description": f"Column '{col_profile.column_name}' has {col_profile.null_percentage:.1f}% null values"
                })
            
            # Low uniqueness for potential ID columns
            if "id" in col_profile.column_name.lower() and col_profile.uniqueness and col_profile.uniqueness < 0.95:
                issues.append({
                    "type": "low_uniqueness_id",
                    "column": col_profile.column_name,
                    "value": col_profile.uniqueness,
                    "severity": "high",
                    "description": f"ID column '{col_profile.column_name}' has only {col_profile.uniqueness:.1%} unique values"
                })
            
            # All values are the same (no variance)
            if col_profile.distinct_count == 1:
                issues.append({
                    "type": "no_variance",
                    "column": col_profile.column_name,
                    "value": col_profile.distinct_count,
                    "severity": "medium",
                    "description": f"Column '{col_profile.column_name}' has no variance (all values are the same)"
                })
        
        return issues
    
    def _calculate_dataset_statistics(self, df: DataFrame, column_profiles: List[ColumnProfile]) -> Dict[str, Any]:
        """Calculate overall dataset statistics."""
        numeric_columns = [
            col_profile.column_name for col_profile in column_profiles
            if "int" in col_profile.data_type.lower() or "float" in col_profile.data_type.lower() or "double" in col_profile.data_type.lower()
        ]
        
        string_columns = [
            col_profile.column_name for col_profile in column_profiles
            if "string" in col_profile.data_type.lower()
        ]
        
        statistics = {
            "numeric_columns": len(numeric_columns),
            "string_columns": len(string_columns),
            "total_columns": len(column_profiles),
            "completeness_avg": sum(col.completeness or 0 for col in column_profiles) / len(column_profiles) if column_profiles else 0,
            "uniqueness_avg": sum(col.uniqueness or 0 for col in column_profiles) / len(column_profiles) if column_profiles else 0
        }
        
        # Add column-level statistics summary
        if numeric_columns:
            statistics["numeric_column_names"] = numeric_columns
        
        if string_columns:
            statistics["string_column_names"] = string_columns
            
            # Calculate average string lengths
            avg_lengths = []
            for col_name in string_columns:
                col_profile = next((cp for cp in column_profiles if cp.column_name == col_name), None)
                if col_profile and col_profile.mean_value:
                    avg_lengths.append(col_profile.mean_value)
            
            if avg_lengths:
                statistics["avg_string_length"] = sum(avg_lengths) / len(avg_lengths)
        
        return statistics