import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

from ingen_fab.python_libs.python.error_categorization import (
    error_categorizer,
)

# Import pipeline utils and error categorization
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils
from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils

logger = logging.getLogger(__name__)

# Constants for polling
POLL_INTERVAL = 30
FAST_RETRY_SEC = 3
TRANSIENT_HTTP = {429, 500, 503, 504, 408}


def timestamp_now() -> int:
    """Get current timestamp as integer."""
    return int(datetime.now().timestamp() * 1000)


async def random_delay_before_logging():
    """Add random delay to reduce write contention."""
    delay = np.random.random() * 0.5  # 0-0.5 seconds
    await asyncio.sleep(delay)


class SynapseExtractUtils:
    """Utility class for Synapse data extraction operations."""

    # Schema definition that matches the original file
    LOG_SCHEMA = pa.schema(
        [
            ("master_execution_id", pa.string()),
            ("execution_id", pa.string()),
            ("pipeline_job_id", pa.string()),
            ("execution_group", pa.int32()),
            ("master_execution_parameters", pa.string()),
            ("trigger_type", pa.string()),
            ("config_synapse_connection_name", pa.string()),
            ("source_schema_name", pa.string()),
            ("source_table_name", pa.string()),
            ("extract_mode", pa.string()),
            ("extract_start_dt", pa.date32()),
            ("extract_end_dt", pa.date32()),
            ("partition_clause", pa.string()),
            ("output_path", pa.string()),
            ("extract_file_name", pa.string()),
            ("external_table", pa.string()),
            ("start_timestamp", pa.timestamp("ms")),
            ("end_timestamp", pa.timestamp("ms")),
            ("duration_sec", pa.float64()),
            ("status", pa.string()),
            ("error_messages", pa.string()),
            ("end_timestamp_int", pa.int64()),
        ]
    )

    def __init__(
        self,
        datasource_name: str,
        datasource_location: str,
        workspace_id: str,
        lakehouse_id: str,
    ):
        """
        Initialise the Synapse extract utilities.

        Args:
            datasource_name: Name of the Synapse datasource
            datasource_location: Location of the Synapse datasource
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
        """
        self.datasource_name = datasource_name
        self.datasource_location = datasource_location

        # Initialize warehouse utils for metadata and logging
        self.warehouse_utils = warehouse_utils(
            target_workspace_id=workspace_id,
            target_warehouse_id="config_wh",  # Using config warehouse for metadata
        )

        # Table names for metadata and logging
        self.log_table_name = "synapse_extract_run_log"
        self.config_table_name = "synapse_extract_objects"
        self.schema_name = "dbo"

    def get_extract_sql_template(self) -> str:
        """Get the SQL template for CETAS extraction."""
        return f"""
    IF NOT EXISTS (
        SELECT
            *
        FROM
            sys.external_file_formats 
        WHERE
            name = 'parquet'
    )
    CREATE EXTERNAL FILE FORMAT [parquet] WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

    IF NOT EXISTS (
        SELECT
            *
        FROM
            sys.external_data_sources
        WHERE
            name = '{self.datasource_name}'
    )
    CREATE EXTERNAL DATA SOURCE [{self.datasource_name.strip()}] WITH (
        LOCATION = '{self.datasource_location.strip()}',
        TYPE = HADOOP
    );

    IF OBJECT_ID('exports.@ExternalTableName', 'U') IS NOT NULL
    DROP EXTERNAL TABLE exports.@ExternalTableName;

    CREATE EXTERNAL TABLE exports.@ExternalTableName WITH (
        LOCATION = '@LocationPath', 
        DATA_SOURCE = [{self.datasource_name.strip()}],
        FILE_FORMAT = parquet 
    ) AS
    SELECT
        *
    FROM
        [@TableSchema].[@TableName]
    @PartitionClause;
    """

    def get_pipeline_utils(self) -> PipelineUtils:
        """Get an instance of PipelineUtils for pipeline operations."""
        return PipelineUtils()

    async def trigger_pipeline_extraction(
        self,
        workspace_id: str,
        pipeline_id: str,
        payload: Dict[str, Any],
        table_name: str,
    ) -> str:
        """Trigger a pipeline extraction using PipelineUtils.

        Args:
            workspace_id: Fabric workspace ID
            pipeline_id: Pipeline ID to trigger
            payload: Pipeline execution payload
            table_name: Table name for logging

        Returns:
            Job ID of the triggered pipeline
        """
        pipeline_utils = self.get_pipeline_utils()
        return await pipeline_utils.trigger_pipeline(
            workspace_id=workspace_id, pipeline_id=pipeline_id, payload=payload
        )

    async def check_pipeline_status(
        self, workspace_id: str, pipeline_id: str, job_id: str, table_name: str
    ) -> tuple[Optional[str], str]:
        """Check pipeline status using PipelineUtils.

        Args:
            workspace_id: Fabric workspace ID
            pipeline_id: Pipeline ID
            job_id: Job ID to check
            table_name: Table name for logging

        Returns:
            Tuple of (status, error_message)
        """
        pipeline_utils = self.get_pipeline_utils()
        return await pipeline_utils.check_pipeline(
            table_name=table_name,
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
        )

    async def poll_pipeline_to_completion(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_id: str,
        table_name: str,
        poll_interval: int = 30,
    ) -> str:
        """Poll pipeline until completion using PipelineUtils with advanced polling logic.

        Args:
            workspace_id: Fabric workspace ID
            pipeline_id: Pipeline ID
            job_id: Job ID to poll
            table_name: Table name for logging
            poll_interval: Seconds between status checks (ignored, uses constants)

        Returns:
            Final pipeline status
        """
        pipeline_utils = self.get_pipeline_utils()

        # Construct job URL for polling
        job_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"

        # Use the advanced polling logic from PipelineUtils
        return await pipeline_utils.poll_job(job_url, table_name)

    def _with_retry(self, fn, max_retries=5, initial_delay=0.5):
        """
        Execute a function with exponential backoff retry logic using sophisticated error categorization.

        Args:
            fn: Function to execute
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds before first retry

        Returns:
            Result of the function if successful

        Raises:
            Exception: Re-raises the last exception after all retries are exhausted
        """
        for attempt in range(1, max_retries + 1):
            try:
                return fn()
            except Exception as exc:
                # Categorize the error
                category, severity, is_retryable = error_categorizer.categorize_exception(exc)

                # Log error with appropriate context
                error_categorizer.log_error(
                    exc,
                    category,
                    severity,
                    context={"attempt": attempt, "max_retries": max_retries},
                )

                # If last attempt or non-retryable, re-raise
                if attempt == max_retries or not is_retryable:
                    raise

                # Calculate delay based on error category
                delay = error_categorizer.get_retry_delay(category, attempt, initial_delay)

                # Add jitter to prevent thundering herd
                jitter = 0.8 + (0.4 * np.random.random())
                delay_with_jitter = delay * jitter

                logger.info(
                    f"Retrying in {delay_with_jitter:.2f}s (category: {category.value}, severity: {severity.value})"
                )
                time.sleep(delay_with_jitter)

    def bulk_insert_queued_extracts(
        self,
        extraction_payloads: List[Dict],
        master_execution_id: str,
        master_execution_parameters: Dict = None,
        trigger_type: str = "Manual",
    ) -> Dict[str, str]:
        """
        Bulk insert all extraction payloads into log table with 'Queued' status.
        This ensures all planned extractions are logged, even if never started.

        Args:
            extraction_payloads: List of extraction payload dictionaries
            master_execution_id: ID linking all extractions in this batch
            master_execution_parameters: Optional parameters to record with the execution
            trigger_type: Type of trigger that initiated the extraction

        Returns:
            Dictionary mapping table names to execution IDs for status updates

        Raises:
            Exception: If bulk insert fails after retries
        """
        logger.info(f"Pre-logging {len(extraction_payloads)} extractions as 'Queued'...")

        records = []
        # Create timestamp with millisecond precision, no timezone
        ts_now = datetime.utcnow().replace(microsecond=0)

        for item in extraction_payloads:
            # Convert string dates to date objects if needed
            extract_start = None
            extract_end = None
            if item.get("extract_start"):
                extract_start = (
                    datetime.strptime(item["extract_start"], "%Y-%m-%d").date()
                    if isinstance(item["extract_start"], str)
                    else item["extract_start"]
                )
            if item.get("extract_end"):
                extract_end = (
                    datetime.strptime(item["extract_end"], "%Y-%m-%d").date()
                    if isinstance(item["extract_end"], str)
                    else item["extract_end"]
                )

            record = {
                "master_execution_id": master_execution_id,
                "execution_id": str(uuid.uuid4()),
                "pipeline_job_id": None,
                "execution_group": item.get("execution_group"),
                "master_execution_parameters": json.dumps(master_execution_parameters)
                if master_execution_parameters
                else None,
                "trigger_type": trigger_type,
                "config_synapse_connection_name": self.datasource_name,
                "source_schema_name": item["source_schema"],
                "source_table_name": item["source_table"],
                "extract_mode": item["extract_mode"],
                "extract_start_dt": extract_start,
                "extract_end_dt": extract_end,
                "partition_clause": item.get("partition_clause"),
                "output_path": item.get("output_path"),
                "extract_file_name": item.get("extract_file_name"),
                "external_table": item.get("external_table"),
                "start_timestamp": ts_now,
                "end_timestamp": None,
                "duration_sec": None,
                "status": "Queued",
                "error_messages": None,
                "end_timestamp_int": None,
            }
            records.append(record)

        # Create DataFrame for bulk insert
        df = pd.DataFrame(records)

        try:
            # Use warehouse utils to write to the log table
            self.warehouse_utils.write_to_table(
                df=df,
                table_name=self.log_table_name,
                schema_name=self.schema_name,
                mode="append",
            )
            logger.info(f"Successfully queued {len(extraction_payloads)} extractions in log table")
            # Return mapping using external table name as key (guaranteed unique)
            return {r["external_table"]: r["execution_id"] for r in records}
        except Exception as exc:
            logger.error(f"Failed to bulk insert log records: {exc}")
            raise

    def update_log_record(
        self,
        status: str,
        master_execution_id: str,
        execution_id: str,
        error: Optional[str] = None,
        duration_sec: Optional[float] = None,
        pipeline_job_id: Optional[str] = None,
        output_path: Optional[str] = None,
        extract_file_name: Optional[str] = None,
        external_table: Optional[str] = None,
    ) -> None:
        """
        Update a log record in the extraction run log table using Delta Lake merge operations.

        Args:
            status: Current status of the extraction ('Queued', 'Claimed', 'Running', 'Completed', 'Failed', etc.)
            master_execution_id: ID linking all extractions in this batch
            execution_id: Unique ID for this specific extraction
            error: Optional error message if the extraction failed
            duration_sec: Optional duration of the extraction in seconds
            pipeline_job_id: Optional ID of the pipeline job executing the extraction
            output_path: Optional path where extracted data is stored
            extract_file_name: Optional base name for the extracted files
            external_table: Optional name of the external table created

        Raises:
            Exception: If update fails after retries
        """

        # Create timestamp with millisecond precision, no timezone
        current_time = datetime.utcnow().replace(microsecond=0)

        updates = {
            "master_execution_id": master_execution_id,
            "execution_id": execution_id,
            "status": status,
            "error_messages": error,
            "end_timestamp": None,
            "end_timestamp_int": None,
        }

        if status in {"Completed", "Failed", "Cancelled", "Deduped"}:
            updates["end_timestamp"] = current_time
            updates["end_timestamp_int"] = int(current_time.strftime("%Y%m%d%H%M%S%f")[:-3])

        if duration_sec is not None:
            updates["duration_sec"] = duration_sec
        if pipeline_job_id:
            updates["pipeline_job_id"] = pipeline_job_id
        if output_path:
            updates["output_path"] = output_path
        if extract_file_name:
            updates["extract_file_name"] = extract_file_name
        if external_table:
            updates["external_table"] = external_table

        # Build the update SQL statement
        set_clauses = []
        params = []

        # Always update these fields
        set_clauses.append("status = ?")
        params.append(status)

        if error:
            set_clauses.append("error_messages = ?")
            params.append(error)

        if status in {"Completed", "Failed", "Cancelled", "Deduped"}:
            set_clauses.append("end_timestamp = ?")
            params.append(current_time)
            set_clauses.append("end_timestamp_int = ?")
            params.append(int(current_time.strftime("%Y%m%d%H%M%S%f")[:-3]))

        if duration_sec is not None:
            set_clauses.append("duration_sec = ?")
            params.append(duration_sec)

        if pipeline_job_id:
            set_clauses.append("pipeline_job_id = ?")
            params.append(pipeline_job_id)

        if output_path:
            set_clauses.append("output_path = ?")
            params.append(output_path)

        if extract_file_name:
            set_clauses.append("extract_file_name = ?")
            params.append(extract_file_name)

        if external_table:
            set_clauses.append("external_table = ?")
            params.append(external_table)

        # Build and execute the update query
        sql = f"""
        UPDATE {self.schema_name}.{self.log_table_name}
        SET {", ".join(set_clauses)}
        WHERE execution_id = ? AND master_execution_id = ?
        """
        params.extend([execution_id, master_execution_id])

        try:
            self.warehouse_utils.execute_query(query=sql, params=params)
            logger.debug(f"Updated log record: execution_id={execution_id}, status={status}")
        except Exception as exc:
            logger.error(f"Failed to update log record: {exc}")
            raise

    def extract_exists(self, master_execution_id: str, rec: Dict) -> bool:
        """
        Check if an extraction with the same parameters already exists.

        This helps avoid duplicate extractions for the same table and date range.

        Args:
            master_execution_id: ID linking all extractions in this batch
            rec: Dictionary containing extraction parameters to check

        Returns:
            bool: True if a matching extraction exists, False otherwise
        """
        try:
            # Build SQL query to check existence
            sql = f"""
            SELECT COUNT(*) as cnt
            FROM {self.schema_name}.{self.log_table_name}
            WHERE master_execution_id = ?
              AND source_schema_name = ?
              AND source_table_name = ?
              AND extract_start_dt = ?
              AND extract_end_dt = ?
            """

            params = [
                master_execution_id,
                rec["source_schema"],
                rec["source_table"],
                rec.get("extract_start"),
                rec.get("extract_end"),
            ]

            result = self.warehouse_utils.execute_query(query=sql, params=params)

            # Check if any matching record exists
            return result[0]["cnt"] > 0 if result else False

        except Exception as e:
            logger.warning(f"Error checking extraction existence: {e}")
            return False

    # Note: The insert_log_record method has been removed as we now use bulk_insert_queued_extracts
    # and update_log_record for all logging operations through warehouse_utils


def build_path_components(item: Dict[str, Any]) -> Dict[str, str]:
    """
    Build path components and naming details for the extraction process.

    This function constructs the appropriate file paths, external table names, and
    extraction file names based on the extraction mode (incremental or snapshot).

    For incremental extractions:
    - Creates date-partitioned paths in format: exports/incremental/{schema}_{table}/YYYY/MM/DD/
    - Generates unique external table names with date suffix: {schema}_{table}_incremental_YYYY_MM_DD

    For snapshot extractions:
    - Creates simple paths in format: exports/snapshot/{schema}_{table}/
    - Generates simple external table names: {schema}_{table}

    Args:
        item: Dictionary containing extraction configuration with keys:
            - source_schema: Schema name of the source table
            - source_table: Name of the source table
            - extract_mode: "incremental" or "snapshot"
            - extract_start: Start date for incremental extractions (format: "YYYY-MM-DD")
            - extract_end: End date for incremental extractions (format: "YYYY-MM-DD")

    Returns:
        Dictionary with keys:
            - full_path: Complete output path for extracted data
            - external_table_name: Name for the external table (without "exports." prefix)
            - extract_file_name: Base filename for the extracted files
    """
    # Get the load tier from extract_mode
    load_tier = item["extract_mode"]

    # Extract date components if incremental load
    if load_tier == "incremental":
        extract_start = datetime.strptime(item.get("extract_start"), "%Y-%m-%d")  # noqa: F841
        extract_end = datetime.strptime(item.get("extract_end"), "%Y-%m-%d")
        run_year = extract_end.year
        run_month = extract_end.month
        run_day = extract_end.day
    else:
        # Defaults for snapshot, not used in path construction
        run_year, run_month, run_day = None, None, None

    # Build base path for output
    base_path = f"exports/{load_tier}/{item['source_schema']}_{item['source_table']}"

    # For incremental loads, include date in path and table name
    if load_tier == "incremental":
        date_path = f"{run_year:04d}/{run_month:02d}/{run_day:02d}"
        full_path = f"{base_path}/{date_path}"

        # External table gets a tier + date suffix so names stay unique
        date_suffix = f"{run_year:04d}_{run_month:02d}_{run_day:02d}"
        external_table_name = f"{item['source_schema']}_{item['source_table']}_{load_tier}_{date_suffix}"
    else:
        # For snapshot loads, use base path and set table name (without suffix)
        full_path = base_path
        external_table_name = f"{item['source_schema']}_{item['source_table']}"

    extract_file_name = f"{item['source_schema']}_{item['source_table']}"

    return {
        "full_path": full_path,
        "external_table_name": external_table_name,
        "extract_file_name": extract_file_name,
    }


def build_partition_clause(item: Dict[str, Any]) -> str:
    """
    Build the SQL partition clause (WHERE clause) for the CETAS extraction.

    This function generates the appropriate SQL filtering clause based on the extraction mode
    and date parameters. For snapshot extractions, no partition clause is needed. For incremental
    extractions, it creates date-based filters using either the provided custom filters or default
    ones based on DATE_SK.

    The function handles two main cases:
    1. Single date extraction: Uses the table's single_date_filter pattern or a default filter
    2. Date range extraction: Uses the table's date_range_filter pattern or a default range filter

    Args:
        item: Dictionary containing extraction configuration with keys:
            - extract_mode: "incremental" or "snapshot"
            - extract_start: Start date for incremental extractions (format: "YYYY-MM-DD")
            - extract_end: End date for incremental extractions (format: "YYYY-MM-DD")
            - single_date_filter: Optional template for single date filtering (e.g., "WHERE DATE_SK = @date")
            - date_range_filter: Optional template for date range filtering (e.g., "WHERE DATE_SK BETWEEN @start_date AND @end_date")
            - mode: Optional extraction mode identifier ("Daily" or "Historical")

    Returns:
        str: SQL WHERE clause for filtering the data, or empty string for snapshot extractions
             For example: "WHERE DATE_SK = 20230101" or "WHERE DATE_SK between 20230101 and 20230131"
    """
    load_tier = item["extract_mode"]

    # If not incremental, no partition clause needed
    if load_tier != "incremental":
        return ""

    # Parse dates for incremental loads
    extract_start = datetime.strptime(item.get("extract_start"), "%Y-%m-%d")
    extract_end = datetime.strptime(item.get("extract_end"), "%Y-%m-%d")
    start_date_sql = extract_start.strftime("%Y%m%d")
    end_date_sql = extract_end.strftime("%Y%m%d")

    # Determine extraction mode and create appropriate filter
    mode = item.get("mode", "Daily")  # noqa: F841

    if start_date_sql == end_date_sql:
        # Same date - use single date filter
        if item.get("single_date_filter"):
            return item["single_date_filter"].replace("@date", end_date_sql)
        else:
            # Default single date filter if not provided
            return f"WHERE DATE_SK = {end_date_sql}"
    else:
        # Date range - use date range filter
        if item.get("date_range_filter"):
            return item["date_range_filter"].replace("@start_date", start_date_sql).replace("@end_date", end_date_sql)
        else:
            # Default date range filter if not provided
            return f"WHERE DATE_SK between {start_date_sql} and {end_date_sql}"


def create_sql_script(
    sql_template: str,
    schema_name: str,
    table_name: str,
    external_table_name: str,
    partition_clause: str,
    location_path: str,
) -> str:
    """
    Create the SQL script by replacing placeholders in template.

    Args:
        sql_template: SQL template string with placeholders
        schema_name: Source schema name
        table_name: Source table name
        external_table_name: Name for the external table
        partition_clause: SQL WHERE clause for partitioning
        location_path: Output path for the extracted data

    Returns:
        Complete SQL script with all placeholders replaced
    """
    return (
        sql_template.replace("@TableSchema", schema_name)
        .replace("@TableName", table_name)
        .replace("@ExternalTableName", external_table_name)
        .replace("@PartitionClause", partition_clause)
        .replace("@LocationPath", location_path)
    )


def enrich_work_item(
    item: Dict[str, Any], path_info: Dict[str, str], partition_clause: str, script: str
) -> Dict[str, Any]:
    """
    Add computed fields to the work item for extraction processing.

    This function takes a basic work item and enriches it with all the computed fields
    needed for pipeline execution, including paths, SQL scripts, and pipeline payloads.

    Args:
        item: Original work item dictionary
        path_info: Dictionary with path components from build_path_components()
        partition_clause: Generated partition clause from build_partition_clause()
        script: Generated SQL script from create_sql_script()

    Returns:
        Enriched work item with additional fields for pipeline execution
    """
    enriched_item = item.copy()
    enriched_item.update(
        {
            "output_path": path_info["full_path"],
            "extract_file_name": path_info["extract_file_name"],
            "external_table_name": path_info["external_table_name"],
            "external_table": f"exports.{path_info['external_table_name']}",
            "cetas_sql": script,
            "partition_clause": partition_clause,
            "pipeline_payload": {
                "executionData": {
                    "parameters": {
                        "script_content": script,
                        "table_schema_and_name": path_info["external_table_name"],
                        "parquet_file_path": path_info["full_path"],
                    }
                }
            },
        }
    )

    return enriched_item


def prepare_extract_payloads(work_items: List[Dict[str, Any]], sql_template: str) -> List[Dict[str, Any]]:
    """
    Prepare the CETAS SQL statements and pipeline payloads for each extraction work item.

    This function transforms the basic work items into complete extraction payloads by:
    1. Generating appropriate output paths and naming conventions based on extraction mode
    2. Building SQL WHERE clauses for data filtering (partition clauses)
    3. Creating the complete CETAS SQL scripts by substituting values into the template
    4. Enriching each work item with additional metadata needed for execution

    Args:
        work_items: List of work item dictionaries, each containing at minimum:
            - source_schema: Schema name of the source table
            - source_table: Name of the source table
            - extract_mode: "incremental" or "snapshot"
            - extract_start: Start date for incremental extractions (format: "YYYY-MM-DD")
            - extract_end: End date for incremental extractions (format: "YYYY-MM-DD")
            - single_date_filter: Optional template for single date filtering
            - date_range_filter: Optional template for date range filtering

        sql_template: SQL template for CETAS extraction with placeholders for:
            - @TableSchema: Schema name
            - @TableName: Table name
            - @ExternalTableName: External table name
            - @PartitionClause: SQL WHERE clause
            - @LocationPath: Output file path

    Returns:
        List of enriched work items with added fields:
            - output_path: Path where extracted files will be stored
            - extract_file_name: Base filename for extraction
            - external_table_name: Name for the external table (without schema)
            - external_table: Full name for the external table (with schema)
            - cetas_sql: Complete SQL script for the extraction
            - partition_clause: Generated SQL WHERE clause
            - pipeline_payload: Complete payload for pipeline execution
    """
    logger.info(f"Preparing extraction payloads for {len(work_items)} tables")

    extraction_payloads = []

    for item in work_items:
        # Get path components and naming details
        path_info = build_path_components(item)

        # Build appropriate partition clause
        partition_clause = build_partition_clause(item)

        # Create the SQL script by replacing placeholders in template
        script = create_sql_script(
            sql_template,
            item["source_schema"],
            item["source_table"],
            path_info["external_table_name"],
            partition_clause,
            path_info["full_path"],
        )

        # Add fields to the work item
        enriched_item = enrich_work_item(item, path_info, partition_clause, script)
        extraction_payloads.append(enriched_item)

    logger.info(f"Successfully prepared {len(extraction_payloads)} extraction payloads")
    return extraction_payloads
