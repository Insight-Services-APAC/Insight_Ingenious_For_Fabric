"""Statistical calculation utilities for data profiling."""

import math
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array_contains,
    avg,
    col,
    count,
    count_distinct,
    isnan,
    isnull,
    kurtosis,
    length,
    percentile_approx,
    regexp_extract,
    regexp_replace,
    size,
    skewness,
    split,
    stddev,
    variance,
    when,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType


class StatisticalCalculations:
    """Core statistical calculations for data profiling."""
    
    @staticmethod
    def calculate_basic_statistics(df: DataFrame, column: str) -> Dict[str, Any]:
        """
        Calculate basic statistics for a numeric column.
        
        Args:
            df: Spark DataFrame
            column: Column name
            
        Returns:
            Dictionary with basic statistics
        """
        # Get data type for the column
        column_type = dict(df.dtypes)[column]
        
        stats = {
            "count": None,
            "null_count": None,
            "distinct_count": None,
            "mean": None,
            "median": None,
            "std_dev": None,
            "variance": None,
            "min": None,
            "max": None,
            "skewness": None,
            "kurtosis": None
        }
        
        try:
            # Basic counts
            total_count = df.count()
            null_count = df.filter(col(column).isNull()).count()
            distinct_count = df.select(column).distinct().count()
            
            stats.update({
                "count": total_count,
                "null_count": null_count,
                "distinct_count": distinct_count,
                "non_null_count": total_count - null_count,
                "null_percentage": (null_count / total_count * 100) if total_count > 0 else 0
            })
            
            # For numeric columns, calculate advanced statistics
            if any(numeric_type in column_type for numeric_type in ['int', 'long', 'float', 'double', 'decimal']):
                non_null_df = df.filter(col(column).isNotNull())
                
                if non_null_df.count() > 0:
                    # Calculate statistics using Spark functions
                    agg_result = non_null_df.agg(
                        avg(col(column)).alias("mean"),
                        spark_min(col(column)).alias("min"),
                        spark_max(col(column)).alias("max"),
                        stddev(col(column)).alias("std_dev"),
                        variance(col(column)).alias("variance"),
                        skewness(col(column)).alias("skewness"),
                        kurtosis(col(column)).alias("kurtosis")
                    ).collect()[0]
                    
                    # Calculate median (50th percentile)
                    median_result = non_null_df.agg(
                        percentile_approx(col(column), 0.5).alias("median")
                    ).collect()[0]
                    
                    stats.update({
                        "mean": agg_result["mean"],
                        "min": agg_result["min"],
                        "max": agg_result["max"],
                        "std_dev": agg_result["std_dev"],
                        "variance": agg_result["variance"],
                        "skewness": agg_result["skewness"],
                        "kurtosis": agg_result["kurtosis"],
                        "median": median_result["median"]
                    })
        
        except Exception as e:
            stats["error"] = str(e)
        
        return stats
    
    @staticmethod
    def calculate_percentiles(df: DataFrame, column: str, percentiles: List[float] = None) -> Dict[str, float]:
        """
        Calculate percentiles for a numeric column.
        
        Args:
            df: Spark DataFrame
            column: Column name
            percentiles: List of percentile values (0-1)
            
        Returns:
            Dictionary mapping percentile to value
        """
        if percentiles is None:
            percentiles = [0.05, 0.25, 0.5, 0.75, 0.95]
        
        result = {}
        
        try:
            non_null_df = df.filter(col(column).isNotNull())
            
            if non_null_df.count() > 0:
                # Calculate multiple percentiles in a single aggregation
                percentile_exprs = [
                    percentile_approx(col(column), p).alias(f"p{int(p*100)}")
                    for p in percentiles
                ]
                
                agg_result = non_null_df.agg(*percentile_exprs).collect()[0]
                
                for i, p in enumerate(percentiles):
                    result[f"p{int(p*100)}"] = agg_result[f"p{int(p*100)}"]
        
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    @staticmethod
    def calculate_string_statistics(df: DataFrame, column: str) -> Dict[str, Any]:
        """
        Calculate statistics specific to string columns.
        
        Args:
            df: Spark DataFrame
            column: Column name
            
        Returns:
            Dictionary with string-specific statistics
        """
        stats = {
            "min_length": None,
            "max_length": None,
            "avg_length": None,
            "empty_count": None,
            "whitespace_only_count": None,
            "pattern_analysis": {}
        }
        
        try:
            non_null_df = df.filter(col(column).isNotNull())
            
            if non_null_df.count() > 0:
                # Length statistics
                length_stats = non_null_df.agg(
                    spark_min(length(col(column))).alias("min_length"),
                    spark_max(length(col(column))).alias("max_length"),
                    avg(length(col(column))).alias("avg_length")
                ).collect()[0]
                
                # Empty and whitespace counts
                empty_count = df.filter(col(column) == "").count()
                whitespace_count = df.filter(
                    regexp_replace(col(column), r"\s", "") == ""
                ).count()
                
                stats.update({
                    "min_length": length_stats["min_length"],
                    "max_length": length_stats["max_length"],
                    "avg_length": length_stats["avg_length"],
                    "empty_count": empty_count,
                    "whitespace_only_count": whitespace_count
                })
                
                # Pattern analysis
                stats["pattern_analysis"] = StatisticalCalculations._analyze_string_patterns(df, column)
        
        except Exception as e:
            stats["error"] = str(e)
        
        return stats
    
    @staticmethod
    def _analyze_string_patterns(df: DataFrame, column: str) -> Dict[str, int]:
        """Analyze common patterns in string data."""
        patterns = {
            "numeric_only": r"^\d+$",
            "alphanumeric": r"^[a-zA-Z0-9]+$",
            "email_like": r"^[^@]+@[^@]+\.[^@]+$",
            "phone_like": r"^[\+]?[\d\s\-\(\)]+$",
            "date_like": r"^\d{4}-\d{2}-\d{2}$",
            "uuid_like": r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
        }
        
        pattern_counts = {}
        
        try:
            for pattern_name, pattern_regex in patterns.items():
                count = df.filter(
                    regexp_extract(col(column), pattern_regex, 0) != ""
                ).count()
                pattern_counts[pattern_name] = count
        
        except Exception as e:
            pattern_counts["error"] = str(e)
        
        return pattern_counts
    
    @staticmethod
    def calculate_data_quality_metrics(df: DataFrame, column: str) -> Dict[str, Any]:
        """
        Calculate data quality metrics for a column.
        
        Args:
            df: Spark DataFrame
            column: Column name
            
        Returns:
            Dictionary with data quality metrics
        """
        metrics = {
            "completeness": 0.0,
            "uniqueness": 0.0,
            "validity": 0.0,
            "consistency": 0.0
        }
        
        try:
            total_count = df.count()
            
            if total_count > 0:
                # Completeness (non-null percentage)
                null_count = df.filter(col(column).isNull()).count()
                metrics["completeness"] = ((total_count - null_count) / total_count) * 100
                
                # Uniqueness (distinct values percentage)
                distinct_count = df.select(column).distinct().count()
                metrics["uniqueness"] = (distinct_count / total_count) * 100
                
                # Validity (non-empty, non-whitespace for strings)
                column_type = dict(df.dtypes)[column]
                if 'string' in column_type.lower():
                    valid_count = df.filter(
                        (col(column).isNotNull()) & 
                        (col(column) != "") & 
                        (regexp_replace(col(column), r"\s", "") != "")
                    ).count()
                    metrics["validity"] = (valid_count / total_count) * 100
                else:
                    # For non-string types, validity is same as completeness
                    metrics["validity"] = metrics["completeness"]
                
                # Consistency (placeholder - could be enhanced with domain-specific rules)
                metrics["consistency"] = metrics["validity"]  # Simplified for now
        
        except Exception as e:
            metrics["error"] = str(e)
        
        return metrics


class ProfilerCalculations:
    """High-level calculations for profiling operations."""
    
    @staticmethod
    def calculate_correlation_matrix(df: DataFrame, numeric_columns: List[str]) -> Dict[str, Dict[str, float]]:
        """
        Calculate correlation matrix for numeric columns.
        
        Args:
            df: Spark DataFrame
            numeric_columns: List of numeric column names
            
        Returns:
            Nested dictionary representing correlation matrix
        """
        correlation_matrix = {}
        
        try:
            if len(numeric_columns) < 2:
                return correlation_matrix
            
            # Use Spark's built-in correlation calculation
            from pyspark.ml.stat import Correlation
            from pyspark.ml.feature import VectorAssembler
            
            # Prepare data
            assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
            vector_df = assembler.transform(df.na.drop())
            
            # Calculate correlation matrix
            correlation_result = Correlation.corr(vector_df, "features").head()
            correlation_array = correlation_result[0].toArray()
            
            # Convert to nested dictionary
            for i, col1 in enumerate(numeric_columns):
                correlation_matrix[col1] = {}
                for j, col2 in enumerate(numeric_columns):
                    correlation_matrix[col1][col2] = float(correlation_array[i][j])
        
        except Exception as e:
            correlation_matrix["error"] = str(e)
        
        return correlation_matrix
    
    @staticmethod
    def detect_outliers_iqr(df: DataFrame, column: str, multiplier: float = 1.5) -> Dict[str, Any]:
        """
        Detect outliers using IQR method.
        
        Args:
            df: Spark DataFrame
            column: Column name
            multiplier: IQR multiplier for outlier detection
            
        Returns:
            Dictionary with outlier information
        """
        result = {
            "outlier_count": 0,
            "outlier_percentage": 0.0,
            "lower_bound": None,
            "upper_bound": None,
            "q1": None,
            "q3": None
        }
        
        try:
            non_null_df = df.filter(col(column).isNotNull())
            total_count = non_null_df.count()
            
            if total_count > 0:
                # Calculate Q1 and Q3
                percentiles = non_null_df.agg(
                    percentile_approx(col(column), 0.25).alias("q1"),
                    percentile_approx(col(column), 0.75).alias("q3")
                ).collect()[0]
                
                q1 = percentiles["q1"]
                q3 = percentiles["q3"]
                iqr = q3 - q1
                
                lower_bound = q1 - (multiplier * iqr)
                upper_bound = q3 + (multiplier * iqr)
                
                # Count outliers
                outlier_count = non_null_df.filter(
                    (col(column) < lower_bound) | (col(column) > upper_bound)
                ).count()
                
                result.update({
                    "outlier_count": outlier_count,
                    "outlier_percentage": (outlier_count / total_count) * 100,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr
                })
        
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    @staticmethod
    def calculate_column_profile_score(stats: Dict[str, Any]) -> float:
        """
        Calculate an overall profile quality score for a column.
        
        Args:
            stats: Dictionary containing column statistics
            
        Returns:
            Score between 0 and 100
        """
        score = 0.0
        max_score = 100.0
        
        try:
            # Completeness component (40 points)
            if "null_percentage" in stats:
                completeness = 100 - stats["null_percentage"]
                score += (completeness / 100) * 40
            
            # Uniqueness component (30 points)
            if "distinct_count" in stats and "count" in stats and stats["count"] > 0:
                uniqueness = (stats["distinct_count"] / stats["count"]) * 100
                # Cap uniqueness contribution for very high cardinality
                uniqueness_capped = min(uniqueness, 100)
                score += (uniqueness_capped / 100) * 30
            
            # Data quality component (30 points)
            if "validity" in stats:
                score += (stats["validity"] / 100) * 30
            elif "error" not in stats:
                # If no specific validity metric but no errors, give partial credit
                score += 20
        
        except Exception:
            # If calculation fails, return a minimal score
            score = 10.0
        
        return min(score, max_score)


class AggregationUtils:
    """Utilities for complex aggregations in profiling."""
    
    @staticmethod
    def calculate_frequency_distribution(
        df: DataFrame, 
        column: str, 
        top_n: int = 10,
        include_null: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Calculate frequency distribution for a column.
        
        Args:
            df: Spark DataFrame
            column: Column name
            top_n: Number of top values to return
            include_null: Whether to include null values
            
        Returns:
            List of dictionaries with value and frequency
        """
        try:
            if include_null:
                freq_df = df.groupBy(column).count().orderBy(col("count").desc())
            else:
                freq_df = df.filter(col(column).isNotNull()).groupBy(column).count().orderBy(col("count").desc())
            
            # Get top N values
            top_values = freq_df.limit(top_n).collect()
            
            # Convert to list of dictionaries
            result = []
            total_count = df.count()
            
            for row in top_values:
                result.append({
                    "value": row[column],
                    "count": row["count"],
                    "percentage": (row["count"] / total_count) * 100 if total_count > 0 else 0
                })
            
            return result
            
        except Exception as e:
            return [{"error": str(e)}]
    
    @staticmethod
    def calculate_histogram_bins(
        df: DataFrame, 
        column: str, 
        num_bins: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Calculate histogram bins for a numeric column.
        
        Args:
            df: Spark DataFrame
            column: Column name
            num_bins: Number of bins
            
        Returns:
            List of dictionaries with bin information
        """
        try:
            non_null_df = df.filter(col(column).isNotNull())
            
            if non_null_df.count() == 0:
                return []
            
            # Get min and max values
            min_max = non_null_df.agg(
                spark_min(col(column)).alias("min_val"),
                spark_max(col(column)).alias("max_val")
            ).collect()[0]
            
            min_val = min_max["min_val"]
            max_val = min_max["max_val"]
            
            if min_val == max_val:
                return [{"bin_start": min_val, "bin_end": max_val, "count": non_null_df.count()}]
            
            # Calculate bin width
            bin_width = (max_val - min_val) / num_bins
            bins = []
            
            for i in range(num_bins):
                bin_start = min_val + (i * bin_width)
                bin_end = min_val + ((i + 1) * bin_width)
                
                if i == num_bins - 1:
                    # Last bin includes the maximum value
                    count = non_null_df.filter(
                        (col(column) >= bin_start) & (col(column) <= bin_end)
                    ).count()
                else:
                    count = non_null_df.filter(
                        (col(column) >= bin_start) & (col(column) < bin_end)
                    ).count()
                
                bins.append({
                    "bin_start": bin_start,
                    "bin_end": bin_end,
                    "count": count,
                    "bin_center": (bin_start + bin_end) / 2
                })
            
            return bins
            
        except Exception as e:
            return [{"error": str(e)}]