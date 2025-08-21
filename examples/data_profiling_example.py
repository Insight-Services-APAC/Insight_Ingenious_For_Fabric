#!/usr/bin/env python3
"""
Example demonstrating data profiling capabilities.

This example shows how to:
1. Profile a dataset
2. Generate quality reports
3. Suggest and validate data quality rules
4. Compare dataset profiles for drift detection
"""

from pyspark.sql import SparkSession
from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark
from ingen_fab.python_libs.interfaces.data_profiling_interface import ProfileType
import json


def main():
    """Run data profiling examples."""
    
    # Initialize Spark session
    print("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("DataProfilingExample") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Create sample dataset
        print("\nCreating sample dataset...")
        data = [
            (1, "Alice", 25, "Engineering", 75000.0, "2023-01-15"),
            (2, "Bob", 30, "Marketing", 65000.0, "2022-06-20"),
            (3, "Charlie", 35, "Engineering", 85000.0, "2021-03-10"),
            (4, "Diana", 28, "Sales", 70000.0, "2023-02-28"),
            (5, "Eve", 32, "Marketing", 72000.0, "2022-11-15"),
            (6, None, 29, "Engineering", 78000.0, "2023-01-01"),
            (7, "Frank", None, "Sales", 68000.0, "2022-08-12"),
            (8, "Grace", 31, None, 76000.0, "2021-12-05"),
            (9, "Henry", 27, "Engineering", None, "2023-03-20"),
            (10, "Iris", 33, "Marketing", 71000.0, None),
        ]
        
        columns = ["id", "name", "age", "department", "salary", "hire_date"]
        df = spark.createDataFrame(data, columns)
        
        print(f"Dataset created with {df.count()} rows and {len(df.columns)} columns")
        df.show()
        
        # Initialize profiler
        print("\nInitializing data profiler...")
        profiler = DataProfilingPySpark(spark)
        
        # Example 1: Basic profiling
        print("\n" + "="*60)
        print("EXAMPLE 1: Basic Dataset Profiling")
        print("="*60)
        
        basic_profile = profiler.profile_dataset(df, ProfileType.BASIC)
        
        print(f"\nDataset: {basic_profile.dataset_name}")
        print(f"Rows: {basic_profile.row_count}")
        print(f"Columns: {basic_profile.column_count}")
        print(f"Profiled at: {basic_profile.profile_timestamp}")
        
        print("\nColumn Profiles:")
        for col in basic_profile.column_profiles:
            print(f"\n  {col.column_name}:")
            print(f"    Type: {col.data_type}")
            print(f"    Nulls: {col.null_count} ({col.null_percentage:.1f}%)")
            print(f"    Distinct: {col.distinct_count}")
            print(f"    Completeness: {col.completeness:.2%}")
            if col.min_value is not None:
                print(f"    Min: {col.min_value}")
            if col.max_value is not None:
                print(f"    Max: {col.max_value}")
            if col.mean_value is not None:
                print(f"    Mean: {col.mean_value:.2f}")
        
        # Example 2: Full profiling with quality analysis
        print("\n" + "="*60)
        print("EXAMPLE 2: Full Profiling with Quality Analysis")
        print("="*60)
        
        full_profile = profiler.profile_dataset(df, ProfileType.FULL)
        
        if full_profile.data_quality_score:
            print(f"\nData Quality Score: {full_profile.data_quality_score:.2%}")
        
        if full_profile.anomalies:
            print("\nDetected Anomalies:")
            for anomaly in full_profile.anomalies:
                print(f"  - {anomaly}")
        
        if full_profile.recommendations:
            print("\nRecommendations:")
            for rec in full_profile.recommendations:
                print(f"  - {rec}")
        
        # Example 3: Generate quality reports
        print("\n" + "="*60)
        print("EXAMPLE 3: Generate Quality Reports")
        print("="*60)
        
        # JSON report
        json_report = profiler.generate_quality_report(full_profile, "json")
        print("\nJSON Report (first 500 chars):")
        print(json_report[:500] + "..." if len(json_report) > 500 else json_report)
        
        # Markdown report
        markdown_report = profiler.generate_quality_report(full_profile, "markdown")
        print("\nMarkdown Report:")
        print(markdown_report)
        
        # Example 4: Suggest and validate rules
        print("\n" + "="*60)
        print("EXAMPLE 4: Suggest and Validate Data Quality Rules")
        print("="*60)
        
        # Suggest rules
        suggested_rules = profiler.suggest_data_quality_rules(full_profile)
        print(f"\nSuggested {len(suggested_rules)} data quality rules:")
        for i, rule in enumerate(suggested_rules, 1):
            print(f"\n  Rule {i}: {rule['type']}")
            print(f"    {rule['description']}")
        
        # Validate against rules
        if suggested_rules:
            print("\nValidating dataset against suggested rules...")
            validation_results = profiler.validate_against_rules(df, suggested_rules)
            
            print(f"\nValidation Results:")
            print(f"  Total Rules: {validation_results['total_rules']}")
            print(f"  Passed: {validation_results['passed_rules']}")
            print(f"  Failed: {validation_results['failed_rules']}")
            print(f"  Success Rate: {validation_results['success_rate']:.1f}%")
            
            if validation_results['violations']:
                print("\n  Violations:")
                for violation in validation_results['violations']:
                    print(f"    - {violation}")
        
        # Example 5: Profile comparison (simulate drift)
        print("\n" + "="*60)
        print("EXAMPLE 5: Dataset Comparison for Drift Detection")
        print("="*60)
        
        # Create a modified dataset (simulate data drift)
        print("\nCreating modified dataset to simulate drift...")
        new_data = [
            (11, "Jack", 26, "Engineering", 77000.0, "2023-04-01"),
            (12, "Kate", 34, "HR", 69000.0, "2023-04-15"),  # New department
            (13, "Liam", 29, "Engineering", 79000.0, "2023-05-01"),
            (14, None, None, "Sales", 66000.0, "2023-05-15"),  # More nulls
            (15, None, 31, "Marketing", 73000.0, "2023-06-01"),
        ]
        
        df2 = df.union(spark.createDataFrame(new_data, columns))
        
        print(f"Modified dataset has {df2.count()} rows")
        
        # Profile the modified dataset
        profile2 = profiler.profile_dataset(df2, ProfileType.BASIC)
        
        # Compare profiles
        comparison = profiler.compare_profiles(basic_profile, profile2)
        
        print("\nProfile Comparison Results:")
        print(f"  Row count change: {comparison['row_count_change']:+} ({comparison['row_count_change_pct']:+.1f}%)")
        print(f"  Column count change: {comparison['column_count_change']:+}")
        
        if comparison.get('added_columns'):
            print(f"  Added columns: {comparison['added_columns']}")
        if comparison.get('removed_columns'):
            print(f"  Removed columns: {comparison['removed_columns']}")
        
        print("\n  Column-level changes:")
        for change in comparison['column_changes'][:5]:  # Show first 5
            print(f"    {change['column']}:")
            print(f"      Null change: {change['null_change']:+}")
            print(f"      Distinct change: {change['distinct_change']:+}")
            if change.get('mean_drift') is not None:
                print(f"      Mean drift: {change['mean_drift']:+.2f}")
        
        print("\n" + "="*60)
        print("Data profiling examples completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\nError during profiling: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()