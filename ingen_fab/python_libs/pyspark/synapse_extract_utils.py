"""
PySpark implementation of Synapse Extract utilities.

This module provides the PySpark-specific implementation for Synapse Extraction 
utilities including enhanced logging, retry mechanisms, and SQL template generation.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import numpy as np
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface
from ingen_fab.python_libs.interfaces.synapse_extract_utils_interface import (
    SynapseExtractUtilsInterface,
)

logger = logging.getLogger(__name__)

# Generic type for with_retry return typing
T = TypeVar("T")


class SynapseExtractUtils(SynapseExtractUtilsInterface):
    """PySpark implementation of Synapse Extract utilities."""

    # CETAS SQL Template used by the Synapse package to facilitate data extraction from Synapse objects.
    CETAS_TEMPLATE = (
        """
    IF NOT EXISTS (
        SELECT * FROM sys.external_file_formats WHERE name = 'parquet'
    )
    CREATE EXTERNAL FILE FORMAT [parquet] WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

    IF NOT EXISTS (
        SELECT * FROM sys.external_data_sources WHERE name = '@DataSourceName'
    )
    CREATE EXTERNAL DATA SOURCE [@DataSourceName] WITH (
        LOCATION = '@DataSourceLocation',
        TYPE = HADOOP
    );

    IF OBJECT_ID('exports.@ExternalTableName', 'U') IS NOT NULL
    DROP EXTERNAL TABLE exports.@ExternalTableName;

    CREATE EXTERNAL TABLE exports.@ExternalTableName WITH (
        LOCATION = '@LocationPath',
        DATA_SOURCE = [@DataSourceName],
        FILE_FORMAT = parquet
    ) AS
    @SelectStatement;
        """
    )

    LOG_SCHEMA = StructType([
        StructField("master_execution_id", StringType(), True),
        StructField("execution_id", StringType(), True),
        StructField("pipeline_job_id", StringType(), True),
        StructField("execution_group", IntegerType(), True),
        StructField("master_execution_parameters", StringType(), True),
        StructField("trigger_type", StringType(), True),
        StructField("config_synapse_connection_name", StringType(), True),
        StructField("synapse_datasource_name", StringType(), True),
        StructField("synapse_datasource_location", StringType(), True),
        StructField("source_schema_name", StringType(), True),
        StructField("source_table_name", StringType(), True),
        StructField("extract_mode", StringType(), True),
        StructField("extract_start_dt", DateType(), True),
        StructField("extract_end_dt", DateType(), True),
        StructField("partition_clause", StringType(), True),
        StructField("custom_select_sql", StringType(), True),
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

    # Status and validation constants
    TERMINAL_STATUSES = {"Completed", "Failed", "Cancelled", "Deduped"}
    VALID_STATUSES = {"Queued", "Claimed", "Running", "Completed", "Failed", "Cancelled", "Deduped"}
    REQUIRED_FIELDS = {
        "master_execution_id",
        "execution_id",
        "source_schema_name",
        "source_table_name",
        "extract_mode",
        "status",
    }

    def __init__(self, lakehouse: DataStoreInterface):
        """Initialise the extract utils with a data store."""
        self.lakehouse = lakehouse
        self.log_table_uri = f"{lakehouse.lakehouse_tables_uri()}synapse_extract_run_log"


    def bulk_insert_queued_extracts(
        self,
        extract_records: List[Dict[str, Any]],
        max_retries: int = 5
    ) -> Dict[str, str]:
        """Bulk insert extraction records with validation and retry logic."""
        if not extract_records:
            logger.warning("No extraction records provided for bulk insert")
            return {}
        
        def _perform_bulk_insert() -> Dict[str, str]:
            try:
                spark = SparkSession.getActiveSession()
                if not spark:
                    raise RuntimeError("No active Spark session found")
                
                # Parse string dates to Date before insert
                normalised_records: List[Dict[str, Any]] = []
                for rec in extract_records:
                    tmp = dict(rec)
                    for k in ("extract_start_dt", "extract_end_dt"):
                        v = tmp.get(k)
                        if isinstance(v, str) and v:
                            parsed = self._parse_ymd(v)
                            if parsed is not None:
                                tmp[k] = parsed
                    normalised_records.append(tmp)

                # Create DataFrame from records
                df = spark.createDataFrame(normalised_records, schema=self.LOG_SCHEMA)
                
                # Write to Delta table
                df.write.format("delta").mode("append").partitionBy("master_execution_id").save(self.log_table_uri)
                
                # Validate insertion using composite key (master_execution_id, execution_id)
                inserted_keys = [
                    {
                        "master_execution_id": record["master_execution_id"],
                        "execution_id": record["execution_id"],
                    }
                    for record in normalised_records
                ]

                keys_schema = StructType([
                    StructField("master_execution_id", StringType(), True),
                    StructField("execution_id", StringType(), True),
                ])
                keys_df = spark.createDataFrame(inserted_keys, schema=keys_schema)

                # Read back and inner-join on the composite key to verify all rows exist
                verification_df = (
                    spark.read.format("delta").load(self.log_table_uri)
                    .select("master_execution_id", "execution_id")
                    .join(keys_df, ["master_execution_id", "execution_id"], "inner")
                )

                actual_count = verification_df.count()
                expected_count = len(extract_records)
                
                if actual_count != expected_count:
                    raise RuntimeError(
                        f"Insertion validation failed: expected {expected_count} records, "
                        f"found {actual_count} records"
                    )
                
                logger.info(f"Successfully bulk inserted {len(extract_records)} extraction records")
                # Build mapping of external_table -> execution_id for orchestrator usage
                mapping = {r["external_table"]: r["execution_id"] for r in extract_records}
                return mapping
                
            except Exception as e:
                logger.error(f"Error in bulk insert operation: {e}")
                raise
        
        return self.with_retry(_perform_bulk_insert, max_retries=max_retries)

    def update_log_record(
        self,
        master_execution_id: str,
        execution_id: str,
        updates: Dict[str, Any],
        max_retries: int = 5
    ) -> bool:
        """Update log record with merge operations and retry logic.

        Composite key enforcement:
        - All merges are performed using (master_execution_id AND execution_id)
        """

        def _perform_update() -> bool:
            try:
                spark = SparkSession.getActiveSession()
                if not spark:
                    raise RuntimeError("No active Spark session found")

                # Enforce presence of master_execution_id (as parameter)
                if not master_execution_id or not str(master_execution_id).strip():
                    raise ValueError(
                        "update_log_record requires 'master_execution_id' parameter to merge by composite key"
                    )

                # Add end_timestamp_int for terminal statuses
                if updates.get("status") in self.TERMINAL_STATUSES:
                    if "end_timestamp" not in updates:
                        updates["end_timestamp"] = datetime.utcnow()
                    # Format end_timestamp_int as yyyymmddHHMMSSsss (notebook-aligned)
                    if isinstance(updates["end_timestamp"], datetime):
                        dt = updates["end_timestamp"]
                        updates["end_timestamp_int"] = int(
                            f"{dt.strftime('%Y%m%d%H%M%S')}{int(dt.microsecond/1000):03d}"
                        )

                # Create update DataFrame
                # Ensure master_execution_id is included in the row
                update_payload = {"master_execution_id": str(master_execution_id), "execution_id": execution_id}
                # Avoid duplicate keys from updates
                for k, v in updates.items():
                    if k not in ("master_execution_id", "execution_id"):
                        update_payload[k] = v
                update_data = [update_payload]

                # Build schema dynamically based on updates
                update_fields = [
                    StructField("master_execution_id", StringType(), True),
                    StructField("execution_id", StringType(), True),
                ]
                for key, value in update_payload.items():
                    if key in {"master_execution_id", "execution_id"}:
                        continue
                    if key == "execution_group":
                        update_fields.append(StructField(key, IntegerType(), True))
                    elif key == "duration_sec":
                        update_fields.append(StructField(key, DoubleType(), True))
                    elif key in ["start_timestamp", "end_timestamp"]:
                        update_fields.append(StructField(key, TimestampType(), True))
                    elif key == "end_timestamp_int":
                        update_fields.append(StructField(key, LongType(), True))
                    elif key in ["extract_start_dt", "extract_end_dt"]:
                        update_fields.append(StructField(key, DateType(), True))
                    else:
                        update_fields.append(StructField(key, StringType(), True))

                update_schema = StructType(update_fields)
                update_df = spark.createDataFrame(update_data, schema=update_schema)

                # Perform merge operation
                delta_log_table = DeltaTable.forPath(spark, self.log_table_uri)

                # Composite key merge condition (enforced)
                merge_condition = (
                    "target.master_execution_id = source.master_execution_id AND "
                    "target.execution_id = source.execution_id"
                )

                # Build update expression
                update_expr = {}
                for field in update_df.columns:
                    if field not in {"master_execution_id", "execution_id"}:
                        update_expr[field] = f"source.{field}"

                delta_log_table.alias("target").merge(
                    update_df.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(set=update_expr).execute()
                
                logger.debug(
                    f"Successfully updated log record for master_execution_id={master_execution_id}, execution_id={execution_id}"
                )
                return True
                
            except Exception as e:
                if self.is_concurrent_write_error(e):
                    logger.warning(f"Concurrent write error during update, will retry: {e}")
                    raise
                else:
                    logger.error(f"Non-retryable error during update: {e}")
                    raise
        
        return self.with_retry(_perform_update, max_retries=max_retries)

    def get_queued_extracts(
        self,
        master_execution_id: str,
        execution_group: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get queued extracts for processing."""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session found")
            
            # Read from Delta table
            df = self._log_df(spark)
            
            # Filter by master execution ID and status
            filtered_df = df.filter(
                (col("master_execution_id") == master_execution_id) &
                (col("status") == "Queued")
            )
            
            # Filter by execution group if specified
            if execution_group is not None:
                filtered_df = filtered_df.filter(col("execution_group") == execution_group)
            
            # Apply limit if specified
            if limit is not None:
                filtered_df = filtered_df.limit(limit)
            
            # Convert to list of dictionaries
            return [row.asDict() for row in filtered_df.collect()]
            
        except Exception as e:
            logger.error(f"Error getting queued extracts: {e}")
            return []

    def get_failed_extracts(
        self,
        master_execution_id: Optional[str] = None,
        status_filter: Optional[List[str]] = None,
        hours_back: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get failed extracts for retry processing."""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session found")
            
            # Read from Delta table
            df = self._log_df(spark)
            
            # Filter by status
            statuses = status_filter if status_filter else ["Failed"]
            filtered_df = df.filter(col("status").isin(statuses))
            
            # Filter by master execution ID if specified
            if master_execution_id:
                filtered_df = filtered_df.filter(col("master_execution_id") == master_execution_id)
            
            # Optional time window filter
            if hours_back is not None:
                cutoff_dt = datetime.utcnow() - timedelta(hours=hours_back)
                filtered_df = filtered_df.filter(col("end_timestamp") >= cutoff_dt)
            
            return [row.asDict() for row in filtered_df.collect()]
            
        except Exception as e:
            logger.error(f"Error getting failed extracts: {e}")
            return []

    def create_extraction_record(
        self,
        work_item: Dict[str, Any],
        master_execution_id: str,
    ) -> Dict[str, Any]:
        """Create an extraction record from a work item and configuration."""
        execution_id = str(uuid.uuid4())
        
        # Build path components
        path_components = self.build_path_components(
            source_schema_name=work_item["source_schema_name"],
            source_table_name=work_item["source_table_name"],
            extract_mode=work_item.get("extract_mode", "snapshot"),
            extract_start_dt=work_item.get("extract_start_dt"),
            extract_end_dt=work_item.get("extract_end_dt"),
            export_base_dir=work_item.get("export_base_dir"),
        )
        
        # Build partition clause
        partition_clause = self.build_partition_clause(
            extract_mode=work_item.get("extract_mode", "snapshot"),
            single_date_filter=work_item.get("single_date_filter"),
            date_range_filter=work_item.get("date_range_filter"),
            extract_start_dt=work_item.get("extract_start_dt"),
            extract_end_dt=work_item.get("extract_end_dt")
        )
        
        # TODO(auditing): If adding datasource fields to the log table, also
        # include them in the record persisted to Delta (extend LOG_SCHEMA/DDL).
        return {
            "master_execution_id": master_execution_id,
            "execution_id": execution_id,
            "pipeline_job_id": None,
            "execution_group": work_item.get("execution_group", 1),
            "master_execution_parameters": json.dumps(work_item.get("master_execution_parameters") or {}),
            "trigger_type": work_item.get("trigger_type", "Manual"),
            "config_synapse_connection_name": work_item.get("synapse_connection_name"),
            # Datasource details should come from WorkItem (varlib-derived) or per-item override
            "synapse_datasource_name": work_item.get("synapse_datasource_name"),
            "synapse_datasource_location": work_item.get("synapse_datasource_location"),
            "source_schema_name": work_item["source_schema_name"],
            "source_table_name": work_item["source_table_name"],
            "extract_mode": work_item.get("extract_mode", "snapshot"),
            "extract_start_dt": work_item.get("extract_start_dt"),
            "extract_end_dt": work_item.get("extract_end_dt"),
            "partition_clause": partition_clause,
            "custom_select_sql": work_item.get("custom_select_sql"),
            "output_path": path_components["output_path"],
            "extract_file_name": path_components["file_name"],
            "external_table": path_components.get("external_table"),
            "start_timestamp": datetime.utcnow(),
            "end_timestamp": None,
            "duration_sec": None,
            "status": "Queued",
            "error_messages": None,
            "end_timestamp_int": None
        }

    def validate_extraction_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate an extraction record against the expected schema."""
        errors = []
        
        # Required fields
        for field in self.REQUIRED_FIELDS:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate extract_mode
        if record.get("extract_mode") not in ["incremental", "snapshot"]:
            errors.append("extract_mode must be 'incremental' or 'snapshot'")
        
        # Validate status
        if record.get("status") not in self.VALID_STATUSES:
            errors.append(f"status must be one of: {', '.join(sorted(self.VALID_STATUSES))}")
        
        # Validate incremental mode requirements
        if record.get("extract_mode") == "incremental":
            if not record.get("extract_start_dt") and not record.get("extract_end_dt"):
                errors.append("incremental mode requires extract_start_dt or extract_end_dt")
        
        return len(errors) == 0, errors

    def build_path_components(
        self,
        source_schema_name: str,
        source_table_name: str,
        extract_mode: str,
        export_base_dir: Optional[str] = None,
        extract_start_dt: Optional[str] = None,
        extract_end_dt: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build path components and deterministic external table name."""
        schema_table = f"{source_schema_name}_{source_table_name}"
        # Default to 'exports' if not provided
        base_export_dir = (export_base_dir or "exports").strip() or "exports"

        if extract_mode == "incremental":
            # Prefer end date for naming; fallback to start date; else today (UTC)
            date_str = (extract_end_dt or extract_start_dt or "").strip()
            d = self._parse_ymd(date_str) or datetime.utcnow().date()
            yyyy = f"{d.year:04d}"
            mm = f"{d.month:02d}"
            dd = f"{d.day:02d}"
            output_path = f"{base_export_dir}/incremental/{schema_table}/{yyyy}/{mm}/{dd}"
            external_table = f"{schema_table}_incremental_{yyyy}_{mm}_{dd}"
            file_name = schema_table
        else:
            output_path = f"{base_export_dir}/snapshot/{schema_table}"
            external_table = f"{schema_table}"
            file_name = schema_table

        return {"output_path": output_path, "file_name": file_name, "external_table": external_table}

    def build_partition_clause(
        self,
        extract_mode: str,
        single_date_filter: Optional[str] = None,
        date_range_filter: Optional[str] = None,
        extract_start_dt: Optional[str] = None,
        extract_end_dt: Optional[str] = None
    ) -> str:
        """Build SQL WHERE clause for partitioning."""
        
        if extract_mode == "snapshot":
            return ""  # No partition clause for snapshot
        
        if extract_mode == "incremental":
            if date_range_filter and extract_start_dt and extract_end_dt:
                return self._create_sql_script(
                    template=date_range_filter,
                    replacements={
                        "start_date": extract_start_dt,
                        "end_date": extract_end_dt
                    }
                )
            elif single_date_filter and extract_start_dt:
                return self._create_sql_script(
                    template=single_date_filter,
                    replacements={"date": extract_start_dt}
                )
        
        return ""

    def get_log_schema(self) -> Dict[str, str]:
        """Get the schema definition for the extraction log table."""
        return {field.name: str(field.dataType) for field in self.LOG_SCHEMA.fields}

    def prepare_extract_payloads(
        self,
        work_items: List[Dict[str, Any]],
        master_execution_id: str,
    ) -> List[Dict[str, Any]]:
        """Prepare extraction payloads for batch processing."""
        payloads = []
        
        for work_item in work_items:
            # Create extraction record
            extraction_record = self.create_extraction_record(
                work_item, master_execution_id
            )
            
            # Validate record
            is_valid, errors = self.validate_extraction_record(extraction_record)
            if not is_valid:
                logger.error(f"Invalid extraction record for {work_item}: {errors}")
                continue
            
            # Attach pipeline parameters for downstream use
            pipeline_params = self.build_pipeline_parameters_from_record(extraction_record)
            extraction_record.update(pipeline_params)

            payloads.append(extraction_record)
        
        return payloads

    def with_retry(
        self,
        operation: Callable[[], T],
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True
    ) -> T:
        """Execute an operation with exponential backoff retry logic."""
        
        for attempt in range(max_retries + 1):
            try:
                return operation()
            except Exception as e:
                if attempt == max_retries:
                    logger.error(f"Operation failed after {max_retries} retries: {e}")
                    raise
                
                if not self.is_concurrent_write_error(e):
                    logger.error(f"Non-retryable error: {e}")
                    raise
                
                # Calculate delay with exponential backoff
                delay = min(base_delay * (2 ** attempt), max_delay)
                
                # Add jitter if requested
                if jitter:
                    jitter_amount = delay * 0.1 * np.random.uniform(-1, 1)
                    delay += jitter_amount
                
                logger.warning(f"Retryable error on attempt {attempt + 1}/{max_retries}, "
                             f"waiting {delay:.2f}s before retry: {e}")
                time.sleep(delay)

        raise RuntimeError("with_retry exited without returning or raising")

    def is_concurrent_write_error(self, error: Exception) -> bool:
        """Check if an error is a Delta Lake concurrent write error."""
        error_message = str(error).lower()
        concurrent_write_indicators = [
            "concurrent update",
            "concurrent transaction",
            "conflicting commit",
            "delta table",
            "concurrent modification"
        ]
        
        return any(indicator in error_message for indicator in concurrent_write_indicators)


    def _create_sql_script(self, template: str, replacements: Dict[str, str]) -> str:
        """Substitute values into SQL templates."""
        script = template
        for key, value in replacements.items():
            placeholder = f"@{key}"
            script = script.replace(placeholder, value)
        return script

    def _parse_ymd(self, date_str: Optional[str]):
        """Parse 'YYYY-MM-DD' to date, or return None if invalid/empty."""
        if not date_str:
            return None
        s = date_str.strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    def _log_df(self, spark: SparkSession):
        """Load the Delta log table as a DataFrame."""
        return spark.read.format("delta").load(self.log_table_uri)

    def build_cetas_script(
        self,
        *,
        source_schema_name: str,
        source_table_name: str,
        external_table_name: str,
        location_path: str,
        datasource_name: str,
        datasource_location: str,
        custom_select_sql: Optional[str] = None,
        partition_clause: str = "",
    ) -> str:
        """Construct CETAS script content from components."""
        clause = (partition_clause or "").strip()
        if clause and not clause.upper().startswith("WHERE"):
            clause = f"WHERE {clause}"

        # Build SELECT with placeholders filled
        base_select = (
            custom_select_sql
            or "SELECT * FROM [@TableSchema].[@TableName] @PartitionClause"
        ).strip()
        select_statement = self._create_sql_script(
            base_select,
            {
                "TableSchema": source_schema_name,
                "TableName": source_table_name,
                "PartitionClause": clause,
            },
        )

        # Fill CETAS template
        return self._create_sql_script(
            self.CETAS_TEMPLATE,
            {
                "DataSourceName": datasource_name.strip(),
                "DataSourceLocation": datasource_location.strip(),
                "ExternalTableName": external_table_name,
                "LocationPath": location_path,
                "SelectStatement": select_statement,
            },
        )

    def build_pipeline_parameters_from_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Build pipeline parameters dict from a prepared extraction record (wrapped for Fabric)."""
        # Resolve datasource info with robust fallbacks and validation
        ds_name, ds_location = self._resolve_datasource_info(record)

        script = self.build_cetas_script(
            source_schema_name=record["source_schema_name"],
            source_table_name=record["source_table_name"],
            external_table_name=(record.get("external_table") or ""),
            location_path=record["output_path"],
            datasource_name=ds_name,
            datasource_location=ds_location,
            custom_select_sql=record.get("custom_select_sql"),
            partition_clause=record.get("partition_clause") or "",
        )
        params = {
            "script_content": script,
            "table_schema_and_name": (record.get("external_table") or ""),
            "parquet_file_path": record["output_path"],
        }
        return {"executionData": {"parameters": params}}

    def _resolve_datasource_info(self, record: Dict[str, Any]) -> Tuple[str, str]:
        """Resolve datasource name/location from record or config table.

        Priority:
        1) Use values present on the record (preferred via variable library)
        2) Exact match in synapse_extract_objects for the same source table

        If neither source provides both fields, raises a ValueError (do not
        fall back to an arbitrary global row), to avoid silently selecting an
        incorrect datasource.
        """
        name = (record.get("synapse_datasource_name") or "").strip()
        loc = (record.get("synapse_datasource_location") or "").strip()

        if name and loc:
            return name, loc

        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session found")

            table_uri = f"{self.lakehouse.lakehouse_tables_uri()}synapse_extract_objects"
            df = spark.read.format("delta").load(table_uri)

            # Guard for legacy tables without the new columns
            required_cols = {"synapse_datasource_name", "synapse_datasource_location"}
            if not required_cols.issubset(set(c.lower() for c in df.columns)):  # case-insensitive check
                raise ValueError(
                    "synapse_extract_objects table is missing columns 'synapse_datasource_name' "
                    "and 'synapse_datasource_location'. Run the updated DDL scripts to migrate "
                    "the configuration schema, or repopulate sample data via the package DDL."
                )

            # Try exact match for the source table
            src_schema = record.get("source_schema_name")
            src_table = record.get("source_table_name")
            if src_schema and src_table:
                match = (
                    df.filter(col("source_schema_name") == src_schema)
                      .filter(col("source_table_name") == src_table)
                      .limit(1)
                      .collect()
                )
                if match:
                    row = match[0]
                    cand_name = (getattr(row, "synapse_datasource_name", None) or "").strip()
                    cand_loc = (getattr(row, "synapse_datasource_location", None) or "").strip()
                    if cand_name and cand_loc:
                        return cand_name, cand_loc

        except Exception as e:
            # Convert any unexpected error into a clear validation error for the caller
            raise ValueError(
                f"Failed to resolve Synapse external data source info: {e}"
            ) from e

        # If we get here, no values could be determined
        raise ValueError(
            "Missing synapse_datasource_name/synapse_datasource_location. Provide via variable "
            "library on the record or populate the matching row in synapse_extract_objects."
        )
