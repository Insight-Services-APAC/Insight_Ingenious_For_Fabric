"""Template for data profiling notebooks in Fabric."""

PROFILING_NOTEBOOK_TEMPLATE = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Data Profiling Report
# MAGIC 
# MAGIC This notebook profiles the dataset: **{{ dataset_name }}**
# MAGIC 
# MAGIC Generated: {{ timestamp }}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
from ingen_fab.packages.data_profiling.libs.interfaces.data_profiling_interface import ProfileType
import json
from datetime import datetime

# Initialize profiler
spark = SparkSession.builder.getOrCreate()
profiler = TieredProfiler(spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Dataset

# COMMAND ----------

# Load the dataset
dataset_path = "{{ dataset_path }}"
df = spark.read.format("{{ file_format }}").load(dataset_path)

print(f"Dataset loaded: {df.count():,} rows, {len(df.columns)} columns")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Profile

# COMMAND ----------

# Generate basic profile
basic_profile = profiler.profile_dataset(
    df,
    profile_type=ProfileType.BASIC
)

# Display summary
print(f"Dataset: {basic_profile.dataset_name}")
print(f"Rows: {basic_profile.row_count:,}")
print(f"Columns: {basic_profile.column_count}")
print(f"Profile Time: {basic_profile.profile_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Statistics

# COMMAND ----------

# Display column profiles
column_data = []
for col in basic_profile.column_profiles:
    column_data.append({
        "Column": col.column_name,
        "Type": col.data_type,
        "Nulls (%)": f"{col.null_percentage:.1f}%",
        "Distinct": col.distinct_count,
        "Completeness": f"{col.completeness:.2%}",
        "Min": col.min_value,
        "Max": col.max_value,
        "Mean": col.mean_value
    })

import pandas as pd
column_df = pd.DataFrame(column_data)
display(column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Analysis

# COMMAND ----------

# Generate full profile with quality metrics
full_profile = profiler.profile_dataset(
    df,
    profile_type=ProfileType.FULL
)

# Display quality score
if full_profile.data_quality_score:
    print(f"Overall Data Quality Score: {full_profile.data_quality_score:.2%}")

# Display anomalies
if full_profile.anomalies:
    print("\nDetected Anomalies:")
    for anomaly in full_profile.anomalies:
        print(f"  - {anomaly['type']}: {anomaly.get('column', 'N/A')} (Severity: {anomaly['severity']})")

# Display recommendations
if full_profile.recommendations:
    print("\nRecommendations:")
    for rec in full_profile.recommendations:
        print(f"  - {rec}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Distribution Analysis

# COMMAND ----------

# Analyze value distributions for categorical columns
for col_profile in full_profile.column_profiles:
    if col_profile.value_distribution and len(col_profile.value_distribution) <= 20:
        print(f"\nValue distribution for {col_profile.column_name}:")
        for value, count in sorted(col_profile.value_distribution.items(), 
                                   key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {value}: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlation Analysis

# COMMAND ----------

# Display correlations for numeric columns
if full_profile.correlations:
    import pandas as pd
    corr_df = pd.DataFrame(full_profile.correlations)
    
    # Display correlation matrix
    print("Correlation Matrix:")
    display(corr_df)
    
    # Find highly correlated pairs
    high_corr = []
    for col1 in full_profile.correlations:
        for col2, corr_value in full_profile.correlations[col1].items():
            if col1 < col2 and abs(corr_value) > 0.7:
                high_corr.append((col1, col2, corr_value))
    
    if high_corr:
        print("\nHighly Correlated Columns (|correlation| > 0.7):")
        for col1, col2, corr in high_corr:
            print(f"  {col1} <-> {col2}: {corr:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suggested Data Quality Rules

# COMMAND ----------

# Generate suggested quality rules
suggested_rules = profiler.suggest_data_quality_rules(full_profile)

print(f"Suggested {len(suggested_rules)} data quality rules:\n")
for rule in suggested_rules:
    print(f"Rule Type: {rule['type']}")
    print(f"  Column: {rule.get('column', 'N/A')}")
    print(f"  Description: {rule['description']}")
    if 'threshold' in rule:
        print(f"  Threshold: {rule['threshold']}")
    if 'min' in rule and 'max' in rule:
        print(f"  Range: [{rule['min']}, {rule['max']}]")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results

# COMMAND ----------

# Export profile to JSON
profile_json = profiler.generate_quality_report(full_profile, "json")

# Save to file (optional)
output_path = f"/tmp/profile_{basic_profile.dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(output_path, 'w') as f:
    f.write(profile_json)

print(f"Profile exported to: {output_path}")

# Display first 1000 chars of JSON
print("\nProfile JSON (preview):")
print(profile_json[:1000] + "..." if len(profile_json) > 1000 else profile_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Profile completed successfully. Key findings:
# MAGIC - **Rows:** {{ "{:,}".format(row_count) }}
# MAGIC - **Columns:** {{ column_count }}
# MAGIC - **Quality Score:** {{ "{:.1%}".format(quality_score) if quality_score else "N/A" }}
# MAGIC - **Issues Found:** {{ anomaly_count }}
# MAGIC - **Recommendations:** {{ recommendation_count }}
'''