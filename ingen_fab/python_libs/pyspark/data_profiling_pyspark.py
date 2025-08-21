"""PySpark implementation of data profiling using native Spark functions."""

from typing import Any, Dict, List, Optional
from datetime import datetime, date
import json
import yaml
import hashlib
import random
from ingen_fab.python_libs.interfaces.data_profiling_interface import (
    DataProfilingInterface,
    DatasetProfile,
    ColumnProfile,
    ProfileType,
    SemanticType,
    ValueFormat,
    NamingPattern,
    ValuePattern,
    BusinessRule,
    ColumnRelationship,
    EntityRelationshipGraph,
    ValueStatistics,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
import ingen_fab.python_libs.common.config_utils as cu
from ingen_fab.python_libs.common.relationship_discovery import (
    ColumnNamingAnalyzer,
    ValueFormatDetector,
    SemanticTypeClassifier,
    BusinessRuleDetector,
    ReferentialIntegrityAnalyzer,
    JoinRecommendationEngine,
)


class DataProfilingPySpark(DataProfilingInterface):
    """PySpark implementation of data profiling interface."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        lakehouse: Optional[lakehouse_utils] = None,
    ):
        """Initialize the PySpark data profiler."""
        self.spark = spark or SparkSession.builder.getOrCreate()

        # Initialize relationship discovery components
        self.naming_analyzer = ColumnNamingAnalyzer()
        self.format_detector = ValueFormatDetector()
        self.semantic_classifier = SemanticTypeClassifier()
        self.rule_detector = BusinessRuleDetector()
        self.integrity_analyzer = ReferentialIntegrityAnalyzer()
        self.join_recommender = JoinRecommendationEngine()

        # Initialize lakehouse utils if not provided
        if lakehouse:
            self.lakehouse = lakehouse
        else:
            # Try to get configs and create lakehouse_utils
            try:
                configs = cu.get_configs_as_object()
                self.lakehouse = lakehouse_utils(
                    target_workspace_id=configs.config_workspace_id,
                    target_lakehouse_id=configs.config_lakehouse_id,
                )
            except Exception:
                # If configs not available, lakehouse operations won't work
                self.lakehouse = None

    def profile_dataset(
        self,
        dataset: Any,
        profile_type: ProfileType = ProfileType.BASIC,
        columns: Optional[List[str]] = None,
        sample_size: Optional[float] = None,
    ) -> DatasetProfile:
        """Profile a dataset using PySpark."""
        import time
        start_time = time.time()
        
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

        # Calculate overall null count (rows with at least one null value)
        null_conditions = [F.col(col_name).isNull() for col_name in df.columns]
        any_null_condition = null_conditions[0]
        for condition in null_conditions[1:]:
            any_null_condition = any_null_condition | condition
        total_null_count = df.filter(any_null_condition).count()

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
            duplicate_count=duplicate_count,
        )

        # Add advanced profiling based on type
        if profile_type in [ProfileType.CORRELATION, ProfileType.FULL]:
            profile.correlations = self._calculate_correlations(df)

        if profile_type in [ProfileType.DATA_QUALITY, ProfileType.FULL]:
            profile.data_quality_score = self._calculate_quality_score(profile)
            profile.anomalies = self._detect_anomalies(df)
            profile.recommendations = self._generate_recommendations(profile)
            profile.data_quality_issues = self._detect_data_quality_issues(
                df, column_profiles
            )

        # Add statistics for statistical profiling
        if profile_type in [ProfileType.STATISTICAL, ProfileType.FULL]:
            profile.statistics = self._calculate_dataset_statistics(df, column_profiles)

        # Add performance metrics
        end_time = time.time()
        duration = end_time - start_time
        
        # Add timing info to statistics
        if not profile.statistics:
            profile.statistics = {}
        profile.statistics["profiling_duration_seconds"] = round(duration, 2)
        profile.statistics["profiling_timestamp"] = start_time
        profile.statistics["sample_rate_used"] = sample_size if sample_size else 1.0
        
        print(f"⏱️  Profiling completed in {duration:.1f} seconds")
        if sample_size:
            print(f"🎯 Used {sample_size:.1%} sample for performance")

        return profile

    def profile_column(
        self,
        dataset: Any,
        column_name: str,
        profile_type: ProfileType = ProfileType.BASIC,
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
                uniqueness=0.0,
            )

        # Basic statistics
        null_count = col_df.filter(F.col(column_name).isNull()).count()
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0

        distinct_count = col_df.select(column_name).distinct().count()
        distinct_percentage = (
            (distinct_count / total_count * 100) if total_count > 0 else 0
        )

        # Create base profile
        profile = ColumnProfile(
            column_name=column_name,
            data_type=str(df.schema[column_name].dataType),
            null_count=null_count,
            null_percentage=null_percentage,
            distinct_count=distinct_count,
            distinct_percentage=distinct_percentage,
            completeness=1 - (null_count / total_count) if total_count > 0 else 0,
            uniqueness=distinct_count / total_count if total_count > 0 else 0,
        )

        # Add type-specific statistics
        col_type = df.schema[column_name].dataType

        if isinstance(col_type, NumericType):
            # Use stddev_samp which returns null for single values instead of causing divide by zero
            stats_result = col_df.select(
                F.min(column_name).alias("min"),
                F.max(column_name).alias("max"),
                F.mean(column_name).alias("mean"),
                F.coalesce(F.stddev_samp(column_name), F.lit(0)).alias("stddev"),
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
                F.coalesce(F.avg(F.length(column_name)), F.lit(0)).alias("avg_length"),
            ).collect()

            if length_stats_result:
                length_stats = length_stats_result[0]
                profile.min_value = length_stats["min_length"]
                profile.max_value = length_stats["max_length"]
                profile.mean_value = length_stats["avg_length"]

        elif isinstance(col_type, (DateType, TimestampType)):
            date_stats_result = col_df.select(
                F.min(column_name).alias("min"), F.max(column_name).alias("max")
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

        # Add top 100 distinct values for all columns
        if profile_type in [
            ProfileType.BASIC,
            ProfileType.STATISTICAL,
            ProfileType.DATA_QUALITY,
            ProfileType.DISTRIBUTION,
            ProfileType.FULL,
        ]:
            try:
                # Get top 100 distinct values ordered by frequency
                top_values_df = (
                    col_df.groupBy(column_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(100)
                )
                top_values = [row[column_name] for row in top_values_df.collect()]
                profile.top_distinct_values = top_values
            except Exception:
                # If there's an error collecting top values, set to empty list
                profile.top_distinct_values = []

        # Add enhanced value statistics for deeper analysis
        if profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL]:
            profile.value_statistics = self._calculate_enhanced_value_statistics(
                df, column_name, total_count, profile
            )

        # Add relationship discovery analysis
        if profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL]:
            profile = self._enhance_profile_with_relationships(
                profile, df, column_name, total_count
            )

        return profile

    def _enhance_profile_with_relationships(
        self, 
        profile: ColumnProfile, 
        df: DataFrame, 
        column_name: str, 
        total_count: int
    ) -> ColumnProfile:
        """Enhance column profile with relationship discovery analysis."""
        
        # Analyze column naming patterns
        profile.naming_pattern = self.naming_analyzer.analyze_column_name(column_name)
        
        # Analyze value patterns (use sample for performance)
        if profile.top_distinct_values:
            sample_values = profile.top_distinct_values[:20]  # Use top values as sample
        else:
            # Get a sample of values
            sample_values = [
                row[column_name] for row in 
                df.select(column_name).sample(False, min(0.1, 1000 / total_count), 42).limit(20).collect()
            ]
        
        profile.value_pattern = self.format_detector.detect_format(sample_values)
        
        # Classify semantic type
        profile.semantic_type = self.semantic_classifier.classify_column(
            column_name=column_name,
            data_type=profile.data_type,
            null_percentage=profile.null_percentage,
            distinct_percentage=profile.distinct_percentage,
            uniqueness=profile.uniqueness or 0.0,
            naming_pattern=profile.naming_pattern,
            value_pattern=profile.value_pattern,
            sample_values=sample_values
        )
        
        # Detect business rules
        distinct_values = profile.top_distinct_values[:20] if profile.top_distinct_values else []
        profile.business_rules = self.rule_detector.detect_rules(
            column_name=column_name,
            data_type=profile.data_type,
            min_value=profile.min_value,
            max_value=profile.max_value,
            distinct_values=distinct_values,
            null_count=profile.null_count,
            total_count=total_count,
            semantic_type=profile.semantic_type or SemanticType.UNKNOWN
        )
        
        return profile

    def _calculate_enhanced_value_statistics(
        self, 
        df: DataFrame, 
        column_name: str, 
        total_count: int, 
        profile: ColumnProfile
    ) -> ValueStatistics:
        """Calculate enhanced value statistics for relationship discovery."""
        
        try:
            col_df = df.select(column_name)
            
            # Calculate selectivity
            selectivity = profile.distinct_count / total_count if total_count > 0 else 0
            
            # Check if unique key
            is_unique_key = profile.uniqueness == 1.0 and profile.null_count == 0
            
            # Check if constant
            is_constant = profile.distinct_count <= 1
            
            # Get value hash signature for quick comparison
            value_hash_signature = None
            if profile.top_distinct_values:
                # Create hash of sorted distinct values (limited to top 100)
                sorted_values = sorted([str(v) for v in profile.top_distinct_values if v is not None])
                hash_input = "|".join(sorted_values)
                value_hash_signature = hashlib.md5(hash_input.encode()).hexdigest()[:16]
            
            # Get dominant value and its ratio
            dominant_value = None
            dominant_value_ratio = 0.0
            if profile.value_distribution:
                dominant_value = max(profile.value_distribution.keys(), 
                                   key=profile.value_distribution.get)
                dominant_value_ratio = profile.value_distribution[dominant_value] / total_count
            elif profile.top_distinct_values:
                # Use first value from top distinct values as dominant
                dominant_value = profile.top_distinct_values[0]
                # Calculate ratio by counting occurrences
                dominant_count = col_df.filter(F.col(column_name) == dominant_value).count()
                dominant_value_ratio = dominant_count / total_count if total_count > 0 else 0
            
            # Calculate value count distribution (cardinality analysis)
            value_count_distribution = None
            if profile.value_distribution:
                # Group by count frequencies
                count_freq = {}
                for value, count in profile.value_distribution.items():
                    count_freq[count] = count_freq.get(count, 0) + 1
                value_count_distribution = count_freq
            
            # Calculate value length stats for strings
            value_length_stats = None
            if isinstance(df.schema[column_name].dataType, StringType):
                length_stats = col_df.select(
                    F.min(F.length(F.col(column_name))).alias("min_length"),
                    F.max(F.length(F.col(column_name))).alias("max_length"),
                    F.avg(F.length(F.col(column_name))).alias("avg_length")
                ).collect()
                
                if length_stats:
                    stats = length_stats[0]
                    value_length_stats = {
                        "min_length": stats["min_length"] or 0,
                        "max_length": stats["max_length"] or 0,
                        "avg_length": round(stats["avg_length"] or 0, 2)
                    }
            
            # Calculate numeric distribution (quartiles)
            numeric_distribution = None
            if isinstance(df.schema[column_name].dataType, NumericType) and profile.std_dev is not None:
                try:
                    quartiles = col_df.select(
                        F.expr(f"percentile_approx({column_name}, 0.25)").alias("q1"),
                        F.expr(f"percentile_approx({column_name}, 0.5)").alias("q2"),
                        F.expr(f"percentile_approx({column_name}, 0.75)").alias("q3")
                    ).collect()
                    
                    if quartiles:
                        q = quartiles[0]
                        numeric_distribution = {
                            "q1": q["q1"],
                            "q2": q["q2"], 
                            "q3": q["q3"],
                            "iqr": (q["q3"] - q["q1"]) if q["q3"] and q["q1"] else None
                        }
                except Exception:
                    # Fallback to None if quartile calculation fails
                    numeric_distribution = None
            
            # Get sample values for overlap testing (random sample)
            sample_values = None
            if total_count > 0:
                sample_size = min(1000, total_count, profile.distinct_count)
                try:
                    # Get a random sample of non-null values
                    sample_df = col_df.filter(F.col(column_name).isNotNull()).sample(
                        fraction=sample_size/total_count if total_count > sample_size else 1.0
                    ).limit(sample_size)
                    
                    sample_rows = sample_df.collect()
                    sample_values = [row[column_name] for row in sample_rows]
                    
                    # Ensure we have unique values in sample
                    sample_values = list(set(sample_values))[:sample_size]
                    
                except Exception:
                    # Fallback to top distinct values if sampling fails
                    sample_values = profile.top_distinct_values[:1000] if profile.top_distinct_values else None
            
            return ValueStatistics(
                value_hash_signature=value_hash_signature,
                value_count_distribution=value_count_distribution,
                selectivity=selectivity,
                is_unique_key=is_unique_key,
                is_constant=is_constant,
                dominant_value=dominant_value,
                dominant_value_ratio=dominant_value_ratio,
                value_length_stats=value_length_stats,
                numeric_distribution=numeric_distribution,
                sample_values=sample_values
            )
            
        except Exception as e:
            # Return minimal stats if calculation fails
            return ValueStatistics(
                selectivity=profile.distinct_count / total_count if total_count > 0 else 0,
                is_unique_key=(profile.uniqueness == 1.0 and profile.null_count == 0),
                is_constant=(profile.distinct_count <= 1)
            )

    def compare_profiles(
        self, profile1: DatasetProfile, profile2: DatasetProfile
    ) -> Dict[str, Any]:
        """Compare two dataset profiles."""
        comparison = {
            "row_count_change": profile2.row_count - profile1.row_count,
            "row_count_change_pct": (
                (profile2.row_count - profile1.row_count) / profile1.row_count * 100
            )
            if profile1.row_count > 0
            else 0,
            "column_count_change": profile2.column_count - profile1.column_count,
            "column_changes": [],
            "quality_score_change": None,
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
                "completeness_change": col2.completeness - col1.completeness
                if col2.completeness and col1.completeness
                else None,
            }

            if col1.mean_value is not None and col2.mean_value is not None:
                col_change["mean_drift"] = col2.mean_value - col1.mean_value

            comparison["column_changes"].append(col_change)

        # Compare quality scores if available
        if profile1.data_quality_score and profile2.data_quality_score:
            comparison["quality_score_change"] = (
                profile2.data_quality_score - profile1.data_quality_score
            )

        return comparison

    def generate_quality_report(
        self, profile: DatasetProfile, output_format: str = "yaml"
    ) -> str:
        """Generate a formatted quality report."""
        if output_format == "json":
            return self._generate_json_report(profile)
        elif output_format == "markdown":
            return self._generate_markdown_report(profile)
        elif output_format == "html":
            return self._generate_html_report(profile)
        elif output_format == "yaml":
            return self._generate_yaml_report(profile)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    def suggest_data_quality_rules(
        self, profile: DatasetProfile
    ) -> List[Dict[str, Any]]:
        """Suggest data quality rules based on profiling."""
        rules = []

        for col_profile in profile.column_profiles:
            # Completeness rule for columns with high completeness
            if col_profile.completeness and col_profile.completeness > 0.95:
                rules.append(
                    {
                        "type": "completeness",
                        "column": col_profile.column_name,
                        "threshold": 0.95,
                        "description": f"Column '{col_profile.column_name}' should have at least 95% completeness",
                    }
                )

            # Uniqueness rule for columns with high uniqueness
            if col_profile.uniqueness and col_profile.uniqueness > 0.95:
                rules.append(
                    {
                        "type": "uniqueness",
                        "column": col_profile.column_name,
                        "threshold": 0.95,
                        "description": f"Column '{col_profile.column_name}' should have at least 95% unique values",
                    }
                )

            # Range rules for numeric columns
            if col_profile.min_value is not None and col_profile.max_value is not None:
                if (
                    "int" in col_profile.data_type.lower()
                    or "float" in col_profile.data_type.lower()
                ):
                    rules.append(
                        {
                            "type": "range",
                            "column": col_profile.column_name,
                            "min": col_profile.min_value,
                            "max": col_profile.max_value,
                            "description": f"Column '{col_profile.column_name}' values should be between {col_profile.min_value} and {col_profile.max_value}",
                        }
                    )

        return rules

    def validate_against_rules(
        self, dataset: Any, rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate dataset against quality rules."""
        df = self._get_dataframe(dataset)
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "total_rules": len(rules),
            "passed_rules": 0,
            "failed_rules": 0,
            "violations": [],
        }

        for rule in rules:
            if rule["type"] == "completeness":
                col = rule["column"]
                threshold = rule["threshold"]
                total_count = df.count()
                if total_count > 0:
                    actual_completeness = 1 - (
                        df.filter(F.col(col).isNull()).count() / total_count
                    )
                else:
                    actual_completeness = 0

                if actual_completeness < threshold:
                    validation_results["failed_rules"] += 1
                    validation_results["violations"].append(
                        {
                            "rule": rule["description"],
                            "expected": threshold,
                            "actual": actual_completeness,
                        }
                    )
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
                    validation_results["violations"].append(
                        {
                            "rule": rule["description"],
                            "expected": threshold,
                            "actual": actual_uniqueness,
                        }
                    )
                else:
                    validation_results["passed_rules"] += 1

            elif rule["type"] == "range":
                col = rule["column"]
                min_val = rule["min"]
                max_val = rule["max"]
                violations = df.filter(
                    (F.col(col) < min_val) | (F.col(col) > max_val)
                ).count()

                if violations > 0:
                    validation_results["failed_rules"] += 1
                    validation_results["violations"].append(
                        {"rule": rule["description"], "violation_count": violations}
                    )
                else:
                    validation_results["passed_rules"] += 1

        validation_results["success_rate"] = (
            (
                validation_results["passed_rules"]
                / validation_results["total_rules"]
                * 100
            )
            if validation_results["total_rules"] > 0
            else 0
        )

        return validation_results

    # Helper methods
    def _get_dataframe(self, dataset: Any) -> DataFrame:
        """Convert various input types to DataFrame using lakehouse_utils when possible."""
        if isinstance(dataset, DataFrame):
            return dataset
        elif isinstance(dataset, str):
            # Check if it's a file path or table name
            if "." in dataset and dataset.split(".")[-1] in [
                "parquet",
                "delta",
                "csv",
                "json",
            ]:
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
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, NumericType)
        ]

        correlations = {}
        for col1 in numeric_cols:
            correlations[col1] = {}
            for col2 in numeric_cols:
                if col1 != col2:
                    # Check for variance in both columns
                    distinct1 = df.select(col1).distinct().count()
                    distinct2 = df.select(col2).distinct().count()
                    if distinct1 > 1 and distinct2 > 1:
                        try:
                            corr = df.stat.corr(col1, col2)
                        except Exception:
                            corr = None
                        correlations[col1][col2] = corr if corr is not None else 0
                    else:
                        correlations[col1][col2] = 0

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
                anomalies.append(
                    {"type": "all_nulls", "column": col, "severity": "high"}
                )

        # Check for duplicate rows
        duplicate_count = df.count() - df.distinct().count()
        if duplicate_count > 0:
            anomalies.append(
                {
                    "type": "duplicate_rows",
                    "count": duplicate_count,
                    "severity": "medium",
                }
            )

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
            "columns": [],
        }

        for col_profile in profile.column_profiles:
            col_dict = {
                "name": col_profile.column_name,
                "type": col_profile.data_type,
                "null_percentage": col_profile.null_percentage,
                "distinct_count": col_profile.distinct_count,
                "completeness": col_profile.completeness,
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

    def _generate_yaml_report(self, profile: DatasetProfile) -> str:
        """Generate YAML format report with top distinct values."""

        def serialize_value(value):
            """Convert values to YAML-serializable format."""
            if value is None:
                return None
            elif isinstance(value, (datetime, date)):
                return value.isoformat()
            elif hasattr(value, "__dict__"):
                return str(value)
            else:
                return value

        report_dict = {
            "dataset_profile": {
                "dataset_name": profile.dataset_name,
                "metadata": {
                    "profile_timestamp": profile.profile_timestamp,
                    "row_count": profile.row_count,
                    "column_count": profile.column_count,
                    "data_quality_score": profile.data_quality_score,
                },
                "summary_statistics": {
                    "null_count": profile.null_count,
                    "duplicate_count": profile.duplicate_count,
                },
                "columns": [],
            }
        }

        # Add column profiles
        for col_profile in profile.column_profiles:
            col_dict = {
                "name": col_profile.column_name,
                "data_type": col_profile.data_type,
                "basic_stats": {
                    "null_count": col_profile.null_count,
                    "null_percentage": round(col_profile.null_percentage, 2),
                    "distinct_count": col_profile.distinct_count,
                    "distinct_percentage": round(col_profile.distinct_percentage, 2),
                    "completeness": round(col_profile.completeness, 4)
                    if col_profile.completeness
                    else None,
                    "uniqueness": round(col_profile.uniqueness, 4)
                    if col_profile.uniqueness
                    else None,
                },
            }

            # Add value statistics if available
            value_stats = {}
            if col_profile.min_value is not None:
                value_stats["min"] = serialize_value(col_profile.min_value)
            if col_profile.max_value is not None:
                value_stats["max"] = serialize_value(col_profile.max_value)
            if col_profile.mean_value is not None:
                value_stats["mean"] = round(col_profile.mean_value, 4)
            if col_profile.median_value is not None:
                value_stats["median"] = round(col_profile.median_value, 4)
            if col_profile.std_dev is not None:
                value_stats["std_dev"] = round(col_profile.std_dev, 4)

            if value_stats:
                col_dict["value_statistics"] = value_stats

            # Add top distinct values
            if col_profile.top_distinct_values:
                col_dict["top_distinct_values"] = [
                    serialize_value(val) for val in col_profile.top_distinct_values
                ]

            # Add value distribution for low-cardinality columns
            if col_profile.value_distribution:
                col_dict["value_distribution"] = {
                    serialize_value(k): v
                    for k, v in col_profile.value_distribution.items()
                }

            # Add relationship discovery information
            if col_profile.semantic_type:
                col_dict["semantic_type"] = col_profile.semantic_type.value

            if col_profile.naming_pattern:
                col_dict["naming_analysis"] = {
                    "is_identifier": col_profile.naming_pattern.is_id_column,
                    "is_foreign_key": col_profile.naming_pattern.is_foreign_key,
                    "is_timestamp": col_profile.naming_pattern.is_timestamp,
                    "is_status_flag": col_profile.naming_pattern.is_status_flag,
                    "is_measurement": col_profile.naming_pattern.is_measurement,
                    "detected_patterns": col_profile.naming_pattern.detected_patterns,
                    "confidence": round(col_profile.naming_pattern.naming_confidence, 3)
                }

            if col_profile.value_pattern and col_profile.value_pattern.detected_format != ValueFormat.UNKNOWN:
                col_dict["value_format"] = {
                    "detected_format": col_profile.value_pattern.detected_format.value,
                    "confidence": round(col_profile.value_pattern.format_confidence, 3),
                    "sample_values": col_profile.value_pattern.sample_values[:5]
                }

            if col_profile.business_rules:
                col_dict["business_rules"] = [
                    {
                        "rule_type": rule.rule_type,
                        "description": rule.rule_description,
                        "confidence": round(rule.confidence, 3),
                        "expression": rule.rule_expression
                    }
                    for rule in col_profile.business_rules
                ]

            if col_profile.relationships:
                col_dict["relationships"] = [
                    {
                        "target_table": rel.target_table,
                        "target_column": rel.target_column,
                        "relationship_type": rel.relationship_type.value,
                        "confidence": round(rel.confidence_score, 3),
                        "overlap_percentage": round(rel.overlap_percentage, 3),
                        "referential_integrity": round(rel.referential_integrity_score, 3),
                        "suggested_join": rel.suggested_join_condition
                    }
                    for rel in col_profile.relationships
                ]

            report_dict["dataset_profile"]["columns"].append(col_dict)

        # Add additional profile information
        if profile.statistics:
            report_dict["dataset_profile"]["advanced_statistics"] = profile.statistics

        if profile.data_quality_issues:
            report_dict["dataset_profile"]["data_quality_issues"] = (
                profile.data_quality_issues
            )

        if profile.recommendations:
            report_dict["dataset_profile"]["recommendations"] = profile.recommendations

        if profile.anomalies:
            report_dict["dataset_profile"]["anomalies"] = profile.anomalies

        if profile.correlations:
            report_dict["dataset_profile"]["correlations"] = profile.correlations

        # Add entity relationship information
        if profile.entity_relationships:
            report_dict["dataset_profile"]["entity_relationships"] = {
                "entities": profile.entity_relationships.entities,
                "join_recommendations": profile.entity_relationships.join_recommendations,
                "suggested_primary_keys": profile.entity_relationships.suggested_primary_keys,
                "table_relationships": [
                    {
                        "table1": rel.table1,
                        "table2": rel.table2,
                        "relationship_type": rel.relationship_type.value,
                        "confidence": round(rel.confidence_score, 3),
                        "join_conditions": rel.suggested_join_conditions,
                        "common_columns": rel.common_columns
                    }
                    for rel in profile.entity_relationships.relationships
                ]
            }

        if profile.semantic_summary:
            report_dict["dataset_profile"]["semantic_summary"] = profile.semantic_summary

        if profile.business_glossary:
            report_dict["dataset_profile"]["business_glossary"] = profile.business_glossary

        return yaml.dump(
            report_dict, default_flow_style=False, sort_keys=False, allow_unicode=True
        )

    def _detect_data_quality_issues(
        self, df: DataFrame, column_profiles: List[ColumnProfile]
    ) -> List[Dict[str, Any]]:
        """Detect data quality issues in the dataset."""
        issues = []

        for col_profile in column_profiles:
            # High null percentage
            if col_profile.null_percentage > 50:
                issues.append(
                    {
                        "type": "high_null_percentage",
                        "column": col_profile.column_name,
                        "value": col_profile.null_percentage,
                        "severity": "high"
                        if col_profile.null_percentage > 75
                        else "medium",
                        "description": f"Column '{col_profile.column_name}' has {col_profile.null_percentage:.1f}% null values",
                    }
                )

            # Low uniqueness for potential ID columns
            if (
                "id" in col_profile.column_name.lower()
                and col_profile.uniqueness
                and col_profile.uniqueness < 0.95
            ):
                issues.append(
                    {
                        "type": "low_uniqueness_id",
                        "column": col_profile.column_name,
                        "value": col_profile.uniqueness,
                        "severity": "high",
                        "description": f"ID column '{col_profile.column_name}' has only {col_profile.uniqueness:.1%} unique values",
                    }
                )

            # All values are the same (no variance)
            if col_profile.distinct_count == 1:
                issues.append(
                    {
                        "type": "no_variance",
                        "column": col_profile.column_name,
                        "value": col_profile.distinct_count,
                        "severity": "medium",
                        "description": f"Column '{col_profile.column_name}' has no variance (all values are the same)",
                    }
                )

        return issues

    def _calculate_dataset_statistics(
        self, df: DataFrame, column_profiles: List[ColumnProfile]
    ) -> Dict[str, Any]:
        """Calculate overall dataset statistics."""
        numeric_columns = [
            col_profile.column_name
            for col_profile in column_profiles
            if "int" in col_profile.data_type.lower()
            or "float" in col_profile.data_type.lower()
            or "double" in col_profile.data_type.lower()
        ]

        string_columns = [
            col_profile.column_name
            for col_profile in column_profiles
            if "string" in col_profile.data_type.lower()
        ]

        statistics = {
            "numeric_columns": len(numeric_columns),
            "string_columns": len(string_columns),
            "total_columns": len(column_profiles),
            "completeness_avg": sum(col.completeness or 0 for col in column_profiles)
            / len(column_profiles)
            if column_profiles
            else 0,
            "uniqueness_avg": sum(col.uniqueness or 0 for col in column_profiles)
            / len(column_profiles)
            if column_profiles
            else 0,
        }

        # Add column-level statistics summary
        if numeric_columns:
            statistics["numeric_column_names"] = numeric_columns

        if string_columns:
            statistics["string_column_names"] = string_columns

            # Calculate average string lengths
            avg_lengths = []
            for col_name in string_columns:
                col_profile = next(
                    (cp for cp in column_profiles if cp.column_name == col_name), None
                )
                if col_profile and col_profile.mean_value:
                    avg_lengths.append(col_profile.mean_value)

            if avg_lengths:
                statistics["avg_string_length"] = sum(avg_lengths) / len(avg_lengths)

        return statistics
