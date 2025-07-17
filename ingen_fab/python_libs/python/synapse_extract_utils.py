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
from deltalake import write_deltalake

# Import pipeline utils
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils

logger = logging.getLogger(__name__)


def timestamp_now() -> int:
    """Get current timestamp as integer."""
    return int(datetime.now().timestamp() * 1000)


async def random_delay_before_logging():
    """Add random delay to reduce write contention."""
    delay = np.random.random() * 0.5  # 0-0.5 seconds
    await asyncio.sleep(delay)


class SynapseExtractUtils: 
    """Utility class for Synapse data extraction operations."""

    def __init__(self, datasource_name: str, datasource_location: str, workspace_id: str, lakehouse_id: str):
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
        base_lh_table_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"
        self.log_table_uri = f"{base_lh_table_path}/log_synapse_extracts"
        self.config_table_uri = f"{base_lh_table_path}/config_synapse_extracts"

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
        table_name: str
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
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            payload=payload
        )
    
    async def check_pipeline_status(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_id: str,
        table_name: str
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
            job_id=job_id
        )
    
    async def poll_pipeline_to_completion(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_id: str,
        table_name: str,
        poll_interval: int = 30
    ) -> str:
        """Poll pipeline until completion using PipelineUtils.
        
        Args:
            workspace_id: Fabric workspace ID
            pipeline_id: Pipeline ID
            job_id: Job ID to poll
            table_name: Table name for logging
            poll_interval: Seconds between status checks
            
        Returns:
            Final pipeline status
        """
        pipeline_utils = self.get_pipeline_utils()
        
        while True:
            status, error = await pipeline_utils.check_pipeline(
                table_name=table_name,
                workspace_id=workspace_id,
                pipeline_id=pipeline_id,
                job_id=job_id
            )
            
            if status is None:
                # Transient error, continue polling
                await asyncio.sleep(poll_interval)
                continue
            
            # Check for terminal states
            if status in {"Completed", "Failed", "Cancelled", "Deduped", "Error"}:
                return status
            
            # Still running, wait and check again
            await asyncio.sleep(poll_interval)
    
    async def insert_log_record(
        self,
        execution_id: str,
        cfg_synapse_connection_name: str,
        source_schema_name: str,
        source_table_name: str,
        extract_file_name: str,
        partition_clause: str,
        status: str,
        error_messages: str,
        start_date: Optional[int] = None, 
        finish_date: Optional[int] = None, 
        update_date: Optional[int] = None,
        output_path: Optional[str] = None,
        master_execution_id: Optional[str] = None
    ) -> None:
        """
        Insert a log record into the log_synapse_extracts table with retry mechanism.
        
        Args:
            execution_id: Unique ID for this execution
            cfg_synapse_connection_name: Name of the Synapse connection
            source_schema_name: Schema containing the source table
            source_table_name: Name of the source table
            extract_file_name: Name of the extract file
            partition_clause: SQL clause for partitioning
            status: Current status of the extraction
            error_messages: Any error messages
            start_date: Start timestamp
            finish_date: Finish timestamp
            update_date: Update timestamp
            output_path: Path to the output file
            master_execution_id: ID that links related executions together
        """
        # Default the dates only if they are not provided
        start_ts = start_date if start_date is not None else timestamp_now()
        finish_ts = finish_date if finish_date is not None else timestamp_now()
        update_ts = update_date if update_date is not None else timestamp_now()

        # Step 1: Prepare schema
        schema = pa.schema([
            ("execution_id", pa.string()),
            ("cfg_synapse_connection_name", pa.string()),
            ("source_schema_name", pa.string()),
            ("source_table_name", pa.string()),
            ("extract_file_name", pa.string()),
            ("partition_clause", pa.string()),
            ("status", pa.string()),
            ("error_messages", pa.string()),
            ("start_date", pa.int64()),
            ("finish_date", pa.int64()),
            ("update_date", pa.int64()),
            ("output_path", pa.string()),
            ("master_execution_id", pa.string())
        ])

        # Step 2: Create DataFrame
        df = pd.DataFrame([{
            "execution_id": execution_id,
            "cfg_synapse_connection_name": cfg_synapse_connection_name,
            "source_schema_name": source_schema_name,
            "source_table_name": source_table_name,
            "extract_file_name": extract_file_name,
            "partition_clause": partition_clause,
            "status": status,
            "error_messages": error_messages,
            "start_date": start_ts,
            "finish_date": finish_ts,
            "update_date": update_ts,
            "output_path": output_path,
            "master_execution_id": master_execution_id
        }])

        # Step 3: Convert to Arrow Table
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Add a small random delay before writing to reduce contention with parallel writes
        await random_delay_before_logging()

        # Step 4: Write to Delta (append) with retry mechanism
        max_retries = 5
        retry_delay_base = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                write_deltalake(self.log_table_uri, table, mode="append", schema=schema)
                # If successful, break out of retry loop
                return
            except Exception as e:
                error_msg = str(e)
                # Check if it's a timeout error
                if "OperationTimedOut" in error_msg or "timeout" in error_msg.lower() or attempt < max_retries - 1:
                    # Calculate exponential backoff with jitter
                    delay = retry_delay_base * (2 ** attempt) * (0.5 + np.random.random())
                    logger.warning(f"Delta write attempt {attempt+1}/{max_retries} failed for {source_schema_name}.{source_table_name} with error: {error_msg[:100]}...")
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    # On last attempt, or if not a timeout error, re-raise
                    logger.error(f"Failed to write log record for {source_schema_name}.{source_table_name} after {attempt+1} attempts: {error_msg}")
                    raise


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
        extract_start = datetime.strptime(item.get('extract_start'), "%Y-%m-%d")
        extract_end = datetime.strptime(item.get('extract_end'), "%Y-%m-%d")
        run_year = extract_end.year
        run_month = extract_end.month
        run_day = extract_end.day
    else:
        # Defaults for snapshot, not used in path construction
        run_year, run_month, run_day = None, None, None
    
    # Build base path for output
    base_path = (
        f"exports/{load_tier}/" 
        f"{item['source_schema']}_{item['source_table']}"
    )
    
    # For incremental loads, include date in path and table name
    if load_tier == "incremental":
        date_path = f"{run_year:04d}/{run_month:02d}/{run_day:02d}"
        full_path = f"{base_path}/{date_path}"
        
        # External table gets a tier + date suffix so names stay unique
        date_suffix = f"{run_year:04d}_{run_month:02d}_{run_day:02d}"
        external_table_name = (
            f"{item['source_schema']}_{item['source_table']}"
            f"_{load_tier}_{date_suffix}"
        )
    else:
        # For snapshot loads, use base path and set table name (without suffix)
        full_path = base_path
        external_table_name = f"{item['source_schema']}_{item['source_table']}"
    
    extract_file_name = f"{item['source_schema']}_{item['source_table']}"

    return {
        "full_path": full_path,
        "external_table_name": external_table_name,
        "extract_file_name": extract_file_name
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
    extract_start = datetime.strptime(item.get('extract_start'), "%Y-%m-%d")
    extract_end = datetime.strptime(item.get('extract_end'), "%Y-%m-%d")
    start_date_sql = extract_start.strftime("%Y%m%d")
    end_date_sql = extract_end.strftime("%Y%m%d")
    
    # Determine extraction mode and create appropriate filter
    mode = item.get("mode", "Daily")
    
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
    location_path: str
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
        sql_template
        .replace("@TableSchema", schema_name)
        .replace("@TableName", table_name)
        .replace("@ExternalTableName", external_table_name)
        .replace("@PartitionClause", partition_clause)
        .replace("@LocationPath", location_path)
    )


def enrich_work_item(item: Dict[str, Any], path_info: Dict[str, str], partition_clause: str, script: str) -> Dict[str, Any]:
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
    enriched_item.update({
        "output_path": path_info["full_path"],
        "extract_file_name": path_info["extract_file_name"],
        "external_table_name": path_info["external_table_name"],
        "external_table": f'exports.{path_info["external_table_name"]}',
        "cetas_sql": script,
        "partition_clause": partition_clause,
        "pipeline_payload": {
            "executionData": {
                "parameters": {
                    "script_content": script,
                    "table_schema_and_name": path_info["external_table_name"],
                    "parquet_file_path": path_info["full_path"]
                }
            }
        }
    })
    
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
            path_info["full_path"]
        )
        
        # Add fields to the work item
        enriched_item = enrich_work_item(item, path_info, partition_clause, script)
        extraction_payloads.append(enriched_item)
    
    logger.info(f"Successfully prepared {len(extraction_payloads)} extraction payloads")
    return extraction_payloads