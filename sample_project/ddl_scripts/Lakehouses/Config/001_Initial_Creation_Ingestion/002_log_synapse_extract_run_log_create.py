# Log table for synapse extract run log - Lakehouse version

# Define schema for the table
schema = StructType([
    StructField("master_execution_id", StringType(), True),
    StructField("execution_id", StringType(), True),
    StructField("pipeline_job_id", StringType(), True),
    StructField("execution_group", IntegerType(), True),
    StructField("master_execution_parameters", StringType(), True),
    StructField("trigger_type", StringType(), True),
    StructField("config_synapse_connection_name", StringType(), True),
    StructField("source_schema_name", StringType(), True),
    StructField("source_table_name", StringType(), True),
    StructField("extract_mode", StringType(), True),
    StructField("extract_start_dt", DateType(), True),
    StructField("extract_end_dt", DateType(), True),
    StructField("partition_clause", StringType(), True),
    StructField("output_path", StringType(), True),
    StructField("extract_file_name", StringType(), True),
    StructField("external_table", StringType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("duration_sec", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("error_messages", StringType(), True),
    StructField("end_timestamp_int", LongType(), True)
])

target_lakehouse.create_table(
    table_name="log_synapse_extract_run_log",
    schema=schema,
    mode="overwrite",
    partition_by=["master_execution_id"],
    options={
        "parquet.vorder.default": "true",
        "overwriteSchema": "true"
    }
)