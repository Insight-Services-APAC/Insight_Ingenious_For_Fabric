#!/usr/bin/env python
# coding: utf-8

# ## extract_from_synapse_v2
# 
# New notebook

# ## Install libraries

# In[55]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -U semantic-link
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -U rich
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -U pydantic


# In[56]:


notebookutils.session.restartPython()


# ## Import libraries

# In[57]:


import json
import time
import sempy.fabric as fabric
from rich.progress import Progress
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
import asyncio
import uuid
from pydantic import BaseModel
from typing import List
from typing import Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import pyarrow as pa
import pandas as pd
from datetime import datetime
import numpy as np
from typing import List, Dict, Optional, Any
import pprint


# In[58]:


from deltalake import write_deltalake, DeltaTable


# ## Define parameters

# In[59]:


current_date = datetime.now()

run_year = current_date.year
run_month = current_date.month
run_day = current_date.day

execution_group = None
max_parallel_pipelines = 3
poll_interval_seconds = 30

debug = False  # For production deployments set to false
config_workspace_id = "2e464f52-4667-4cd9-9bc9-ed8887658567"
config_lakehouse_id = "34b88168-9d0e-468a-81b1-35054a98e9b8"
fabric_environment = "development"

include_snapshot_tables = True  # Parameter to control whether to include snapshot tables
master_execution_id = None      # Will be set by orchestrator or generated if None


# In[60]:


# run_year = 2021
# run_month = 1
# run_day = 2


# In[61]:


print(f"Run Date: {run_year}-{run_month:02d}-{run_day:02d}")
print(f"Execution Group: {execution_group if execution_group is not None else 'All'}")
print(f"Max Parallel Pipelines: {max_parallel_pipelines}")
print(f"Include Snapshot Tables: {include_snapshot_tables}")


# ## Utilities

# ### Lakehouse utils

# In[62]:


class LakehouseUtils:
    """Utility class for working with Fabric Lakehouse."""
    
    def __init__(self, workspace_id: str, lakehouse_id: str):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
    
    def lakehouse_tables_uri(self) -> str:
        """Get the URI for the Tables folder in the Lakehouse."""
        return f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}/Tables/"
    
    def check_if_table_exists(self, table_path: str) -> bool:
        """Check if a Delta table exists at the specified path."""
        try:
            _ = DeltaTable(table_path)
            return True
        except Exception:
            return False


# ### Config utils

# In[63]:


@dataclass
class FabricConfig:
    """Configuration model for Fabric environment settings."""
    fabric_environment: str
    config_workspace_id: str
    config_lakehouse_id: str
    edw_workspace_id: str
    edw_warehouse_id: str
    edw_warehouse_name: str        
    edw_lakehouse_id: str
    edw_lakehouse_name: str
    legacy_synapse_connection_name: str
    synapse_export_shortcut_path_in_onelake: str
    d365_workspace_id: str
    d365_lakehouse_id: str
    d365_shortcuts_workspace_id: str
    d365_shortcuts_lakehouse_id: str
    full_reset: bool
    update_date: datetime = datetime.now()

    def get_attribute(self, attr_name: str) -> Any:
        """Get attribute value by string name with error handling."""
        if hasattr(self, attr_name):
            return getattr(self, attr_name)
        else:
            raise AttributeError(f"FabricConfig has no attribute '{attr_name}'")


class ConfigUtils:
    """Utility class for working with configuration tables."""
    
    def __init__(self, lakehouse: LakehouseUtils):
        self.lakehouse = lakehouse
        self.fabric_environments_table_uri = f"{lakehouse.lakehouse_tables_uri()}config_fabric_environments"

    @staticmethod
    def config_schema() -> pa.Schema:
        """Get the schema for the config_fabric_environments table."""
        return pa.schema([
            ("fabric_environment", pa.string()),
            ("config_workspace_id", pa.string()),
            ("config_lakehouse_id", pa.string()),
            ("edw_workspace_id", pa.string()),
            ("edw_warehouse_id", pa.string()),
            ("edw_warehouse_name", pa.string()),
            ("edw_lakehouse_id", pa.string()),
            ("edw_lakehouse_name", pa.string()),
            ("legacy_synapse_connection_name", pa.string()),
            ("synapse_export_shortcut_path_in_onelake", pa.string()),
            ("d365_workspace_id", pa.string()),
            ("d365_lakehouse_id", pa.string()),
            ("d365_shortcuts_workspace_id", pa.string()),
            ("d365_shortcuts_lakehouse_id", pa.string()),
            ("full_reset", pa.bool_()),
            ("update_date", pa.timestamp('ms')),
        ])

    def get_configs_as_dict(self, fabric_environment: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific environment as a dictionary."""
        table = DeltaTable(self.fabric_environments_table_uri).to_pyarrow_table().to_pandas()
        row = table[table["fabric_environment"] == fabric_environment]
        return row.iloc[0].to_dict() if not row.empty else None

    def get_configs_as_object(self, fabric_environment: str) -> Optional[FabricConfig]:
        """Get configuration for a specific environment as a FabricConfig object."""
        row_dict = self.get_configs_as_dict(fabric_environment)
        return FabricConfig(**row_dict) if row_dict else None


# ### Pipeline utils

# In[64]:


async def trigger_pipeline(
    client: fabric.FabricRestClient,
    workspace_id: str,
    pipeline_id: str,
    payload: Dict[str, Any]
) -> str:
    """
    Trigger a Fabric pipeline job via REST API with robust retry logic.
    
    Args:
        client: Authenticated Fabric REST client
        workspace_id: Fabric workspace ID
        pipeline_id: ID of the pipeline to run
        payload: Parameters to pass to the pipeline
        
    Returns:
        The job ID of the triggered pipeline
    """
    max_retries = 5
    retry_delay = 2  # Initial delay in seconds
    backoff_factor = 1.5  # Exponential backoff multiplier
    
    for attempt in range(1, max_retries + 1):
        try:
            trigger_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
            
            response = client.post(trigger_url, json=payload)
            
            # Check for successful response (202 Accepted)
            if response.status_code == 202:
                # Extract job ID from Location header
                response_location = response.headers.get('Location', '')
                job_id = response_location.rstrip("/").split("/")[-1]    
                print(f"✅ Pipeline triggered successfully. Job ID: {job_id}")
                return job_id
            
            # Handle specific error conditions
            elif response.status_code >= 500 or response.status_code in [429, 408]:
                # Server errors (5xx) or rate limiting (429) or timeout (408) are likely transient
                error_msg = f"Transient error (HTTP {response.status_code}): {response.text[:100]}"
                print(f"⚠️ Attempt {attempt}/{max_retries}: {error_msg}")
            else:
                # Client errors (4xx) other than rate limits are likely permanent
                error_msg = f"Client error (HTTP {response.status_code}): {response.text[:100]}"
                if attempt == max_retries:
                    print(f"❌ Failed after {max_retries} attempts: {error_msg}")
                    raise Exception(f"Failed to trigger pipeline: {response.status_code}\n{response.text}")
                else:
                    print(f"⚠️ Attempt {attempt}/{max_retries}: {error_msg}")
        
        except Exception as e:
            # Handle connection or other exceptions
            error_msg = str(e)
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                # Network-related errors are likely transient
                print(f"⚠️ Attempt {attempt}/{max_retries}: Network error: {error_msg}")
            else:
                # Re-raise non-network exceptions on the last attempt
                if attempt == max_retries:
                    print(f"❌ Failed after {max_retries} attempts due to unexpected error: {error_msg}")
                    raise
                else:
                    print(f"⚠️ Attempt {attempt}/{max_retries}: Unexpected error: {error_msg}")
        
        # Don't sleep on the last attempt
        if attempt < max_retries:
            # Calculate sleep time with exponential backoff and a bit of randomization
            sleep_time = retry_delay * (backoff_factor ** (attempt - 1))
            # Add jitter (±20%) to avoid thundering herd problem
            jitter = 0.8 + (0.4 * np.random.random())
            sleep_time = sleep_time * jitter
            
            print(f"🕒 Retrying in {sleep_time:.2f} seconds...")
            await asyncio.sleep(sleep_time)
    
    # This should only be reached if we exhaust all retries on a non-4xx error
    raise Exception(f"❌ Failed to trigger pipeline after {max_retries} attempts")


async def check_pipeline(
    client: fabric.FabricRestClient,
    table_name: str,
    workspace_id: str,
    pipeline_id: str,
    job_id: str
) -> tuple[Optional[str], str]:
    """
    Check the status of a pipeline job with enhanced error handling.
    
    Args:
        client: Authenticated Fabric REST client
        table_name: Name of the table being processed for display
        workspace_id: Fabric workspace ID
        pipeline_id: ID of the pipeline being checked
        job_id: The job ID to check
        
    Returns:
        Tuple of (status, error_message)
    """
    status_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
    
    try:
        response = client.get(status_url)
        
        # Handle HTTP error status codes
        if response.status_code >= 400:
            if response.status_code == 404:
                # Job not found - could be a temporary issue or job is still initializing
                print(f"[INFO] Pipeline {job_id} for {table_name}: Job not found (404) - may be initializing")
                return None, f"Job not found (404): {job_id}"
            elif response.status_code >= 500 or response.status_code in [429, 408]:
                # Server-side error or rate limiting - likely temporary
                print(f"[INFO] Pipeline {job_id} for {table_name}: Server error ({response.status_code})")
                return None, f"Server error ({response.status_code}): {response.text[:100]}"
            else:
                # Other client errors (4xx)
                print(f"[ERROR] Pipeline {job_id} for {table_name}: API error ({response.status_code})")
                return "Error", f"API error ({response.status_code}): {response.text[:100]}"
        
        # Parse the JSON response
        try:
            data = response.json()
        except Exception as e:
            # Invalid JSON in response
            print(f"[WARNING] Pipeline {job_id} for {table_name}: Invalid response format")
            return None, f"Invalid response format: {str(e)}"
        
        status = data.get("status")

        # Handle specific failure states with more context
        if status == "Failed" and "failureReason" in data:
            fr = data["failureReason"]
            msg = fr.get("message", "")
            error_code = fr.get("errorCode", "")
            
            # Check for specific transient errors
            if error_code == "RequestExecutionFailed" and "NotFound" in msg:
                print(f"[INFO] Pipeline {job_id} for {table_name}: Transient check-failure, retrying: {error_code}")
                return None, msg
            
            # Resource constraints may be temporary
            if any(keyword in msg.lower() for keyword in ["quota", "capacity", "throttl", "timeout"]):
                print(f"[INFO] Pipeline {job_id} for {table_name}: Resource constraint issue: {error_code}")
                return None, msg
            
            # Print failure with details
            print(f"Pipeline {job_id} for {table_name}: ❌ {status} - {error_code}: {msg[:100]}")
            return status, msg

        # Print status update with appropriate icon
        status_icons = {
            "Completed": "✅",
            "Failed": "❌",
            "Running": "⏳",
            "NotStarted": "⏳",
            "Pending": "⌛",
            "Queued": "🔄"
        }
        icon = status_icons.get(status, "⏳")
        print(f"Pipeline {job_id} for {table_name}: {icon} {status}")

        return status, ""
    
    except Exception as e:
        error_msg = str(e)
        
        # Categorize exceptions
        if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
            # Network-related issues are transient
            print(f"[WARNING] Pipeline {job_id} for {table_name}: Network error: {error_msg[:100]}")
            return None, f"Network error: {error_msg}"
        elif "not valid for Guid" in error_msg:
            # Invalid GUID format - this is likely a client error
            print(f"[ERROR] Pipeline {job_id} for {table_name}: Invalid job ID format")
            return "Error", f"Invalid job ID format: {error_msg}"
        else:
            # Unexpected exceptions
            print(f"[ERROR] Failed to check pipeline status for {table_name}: {error_msg[:100]}")
            return None, error_msg


# ### Synapse Extract utils

# In[65]:


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
                    print(f"Delta write attempt {attempt+1}/{max_retries} failed for {source_schema_name}.{source_table_name} with error: {error_msg[:100]}...")
                    print(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    # On last attempt, or if not a timeout error, re-raise
                    print(f"Failed to write log record for {source_schema_name}.{source_table_name} after {attempt+1} attempts: {error_msg}")
                    raise


# ### Helper functions

# In[66]:


def timestamp_now() -> int:
    """Get current UTC timestamp in YYYYMMDDHHMMSSmmm format as integer."""
    return int(datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3])


# In[ ]:


async def random_delay_before_logging() -> None:
    """
    Adds a short random delay before logging operations.
    This helps reduce contention when multiple parallel operations
    try to write to the same Delta table at the same time.
    """
    # Random delay between 0.1 and 1.5 seconds
    delay = 0.1 + np.random.random() * 1.4
    await asyncio.sleep(delay)


# ## Data Models

# In[67]:


class DataPipelineJob(BaseModel):
    """Model for tracking pipeline job execution."""
    job_id: str
    status: str


class ExtractConfigDetails(BaseModel):
    """Model for Synapse extraction configuration."""
    # Configuration fields
    synapse_connection_name: str
    source_schema_name: str
    source_table_name: str
    partition_clause: str
    execution_group: int
    active_yn: str
    pipeline_id: str
    synapse_datasource_name: str
    synapse_datasource_location: str
    
    # Runtime fields
    data_pipeline_payload: Optional[Dict[str, Any]] = None
    job: Optional[DataPipelineJob] = None
    execution_id: Optional[str] = None
    master_execution_id: Optional[str] = None
    start_date: Optional[int] = None
    finish_date: Optional[int] = None
    update_date: Optional[int] = None
    output_path: Optional[str] = None
    status_check_retry_count: Optional[int] = 0

    def get_attribute(self, attr_name: str) -> Any:
        """Get attribute value by string name with error handling."""
        if hasattr(self, attr_name):
            return getattr(self, attr_name)
        else:
            raise AttributeError(f"ExtractConfigDetails has no attribute '{attr_name}'")
            
    @property
    def is_active(self) -> bool:
        """Check if this configuration is active."""
        return self.active_yn == "Y"


# In[68]:


def prepare_extract_payloads(
    tables: List[ExtractConfigDetails],
    sql_template: str,
    run_year: int,
    run_month: int,
    run_day: int
) -> None:
    """
    Prepare the extraction payloads for each table.
    
    Args:
        tables: List of tables to prepare payloads for
        sql_template: SQL template for CETAS extraction
        run_year: Year for partitioning
        run_month: Month for partitioning
        run_day: Day for partitioning
    """
    print(f"Preparing extraction payloads for {len(tables)} tables...")
    
    for table in tables:
        # Determine load tier based on presence of partition clause
        load_tier = "incremental" if table.partition_clause else "snapshot"

        # Build base path for output
        base_path = (
            # f"exports_v2/{load_tier}/"
            f"exports/{load_tier}/"
            f"{table.source_schema_name}_{table.source_table_name}"
        )

        # For incremental loads, include date in path and table name
        if load_tier == "incremental":
            date_path = f"{run_year:04d}/{run_month:02d}/{run_day:02d}"
            full_path = f"{base_path}/{date_path}"
            
            # External table gets a tier + date suffix so names stay unique
            date_suffix = f"_{run_year:04d}_{run_month:02d}_{run_day:02d}"
            external_table_name = (
                f"{table.source_schema_name}_{table.source_table_name}"
                f"_{load_tier}{date_suffix}"
            )
        else:
            # For snapshot loads, use base path and simpler table name
            full_path = base_path
            external_table_name = f"{table.source_schema_name}_{table.source_table_name}"

        # Store output path in the table config
        table.output_path = full_path

        # Create the SQL script by replacing placeholders in template
        # Pre-calculate arithmetic expressions for date parameters
        year_month = run_year * 100 + run_month
        full_date = run_year * 10000 + run_month * 100 + run_day
        
        # Replace partition clause with expressions pre-calculated
        partition_clause = table.partition_clause
        partition_clause = partition_clause.replace("@year*100 + @month", str(year_month))
        partition_clause = partition_clause.replace("@year*10000 + @month*100 + @day", str(full_date))

        # Create the SQL script by replacing placeholders in template
        script = (
            sql_template
            .replace("@TableSchema", table.source_schema_name)
            .replace("@TableName", table.source_table_name)
            .replace("@ExternalTableName", external_table_name)
            .replace("@PartitionClause", partition_clause)
            .replace("@year", str(run_year))
            .replace("@month", str(run_month))
            .replace("@day", str(run_day))
            .replace("@LocationPath", table.output_path)
        )

        # Create the pipeline payload
        table.data_pipeline_payload = {
            "executionData": {
                "parameters": {
                    "script_content": script,
                    "table_schema_and_name": (
                        f"{table.source_schema_name}_{table.source_table_name}"
                    ),
                    "parquet_file_path": str(table.output_path)
                }
            }
        }
    
    print(f"✅ Prepared {len(tables)} extraction payloads")


# In[ ]:


async def trigger_and_monitor_pipelines(
    client: fabric.FabricRestClient,
    synapse_utils: SynapseExtractUtils,
    workspace_id: str,
    pipeline_id: str,
    tables: List[ExtractConfigDetails],
    run_year: int,
    run_month: int,
    run_day: int,
    max_parallel: int = 3,
    poll_interval: int = 10,
    master_execution_id: Optional[str] = None
) -> None:
    """
    Trigger and monitor pipelines for extraction.
    
    Args:
        client: Authenticated Fabric REST client
        synapse_utils: Synapse extraction utilities
        workspace_id: Fabric workspace ID
        pipeline_id: ID of the pipeline to run
        tables: List of tables to extract
        run_year: Year for partitioning
        run_month: Month for partitioning
        run_day: Day for partitioning
        max_parallel: Maximum number of parallel pipelines
        poll_interval: Interval for polling pipeline status (seconds)
        master_execution_id: ID that links related executions together
    """
    console = Console()
    terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
    
    # Use provided master_execution_id or generate a new one
    if master_execution_id is None:
        master_execution_id = str(uuid.uuid4())
        print(f"Generated new master execution ID: {master_execution_id}")
    else:
        print(f"Using provided master execution ID: {master_execution_id}")
    
    # Separate snapshot tables (no partition clause) and incremental tables (with partition clause)
    snapshot_tables = [table for table in tables if not table.partition_clause]
    incremental_tables = [table for table in tables if table.partition_clause]
    
    print(f"\n{'=' * 80}")
    print(f"STARTING EXTRACTION PROCESS")
    print(f"Master execution ID: {master_execution_id}")
    print(f"Total tables to process: {len(tables)}")
    print(f"  - Snapshot tables: {len(snapshot_tables)}")
    print(f"  - Incremental tables: {len(incremental_tables)}")
    print(f"Maximum parallel pipelines: {max_parallel}")
    print(f"{'=' * 80}\n")
    
    # Process snapshot tables first, all in one batch to avoid conflicts
    if snapshot_tables:
        print(f"\n{'=' * 80}")
        print(f"PROCESSING SNAPSHOT TABLES")
        print(f"These will be processed first to avoid parallel write conflicts")
        print(f"{'=' * 80}\n")
        
        # We'll use the same function but limit concurrency for snapshot tables to prevent conflicts
        # For snapshots, we'll process them sequentially to avoid any overlap
        await process_table_batch(
            client=client,
            synapse_utils=synapse_utils,
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            tables=snapshot_tables,
            run_year=run_year,
            run_month=run_month,
            run_day=run_day,
            max_parallel=1,  # Force sequential processing for snapshots
            poll_interval=poll_interval,
            batch_type="Snapshot",
            master_execution_id=master_execution_id
        )
    
    # Now process incremental tables with the requested parallelism
    if incremental_tables:
        print(f"\n{'=' * 80}")
        print(f"PROCESSING INCREMENTAL TABLES")
        print(f"These can be processed in parallel as they write to date-partitioned locations")
        print(f"{'=' * 80}\n")
        
        await process_table_batch(
            client=client,
            synapse_utils=synapse_utils,
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            tables=incremental_tables,
            run_year=run_year,
            run_month=run_month,
            run_day=run_day,
            max_parallel=max_parallel,
            poll_interval=poll_interval,
            batch_type="Incremental",
            master_execution_id=master_execution_id
        )
    
    # Print overall completion message
    print(f"\n{'=' * 80}")
    print(f"EXTRACTION PROCESS COMPLETED")
    print(f"Master execution ID: {master_execution_id}")
    print(f"Total tables processed: {len(tables)}")
    print(f"{'=' * 80}\n")
    
    # Generate summary table
    summary = Table(title="Pipeline Jobs Status Summary")
    summary.add_column("Job ID", style="bold")
    summary.add_column("Table Name", style="bold")
    summary.add_column("Type", style="bold")
    summary.add_column("Status", style="bold")
    summary.add_column("Execution Time (sec)", style="bold")
    
    for table in tables:
        # Calculate execution time if applicable
        exec_time = "N/A"
        if table.job and table.start_date is not None:
            finish_time = table.finish_date if table.finish_date is not None else timestamp_now()
            # Convert from YYYYMMDDHHMMSSmmm format to seconds
            start_str = str(table.start_date)
            finish_str = str(finish_time)
            
            # Parse the timestamps and calculate the difference
            try:
                start_datetime = datetime.strptime(start_str, '%Y%m%d%H%M%S%f')
                finish_datetime = datetime.strptime(finish_str, '%Y%m%d%H%M%S%f')
                exec_time = f"{(finish_datetime - start_datetime).total_seconds():.1f}"
            except ValueError:
                exec_time = "Error"
        
        table_type = "Incremental" if table.partition_clause else "Snapshot"
        
        if table.job:
            summary.add_row(
                str(table.job.job_id), 
                f"{table.source_schema_name}.{table.source_table_name}", 
                table_type,
                table.job.status,
                exec_time
            )
        else:
            summary.add_row(
                "N/A",
                f"{table.source_schema_name}.{table.source_table_name}",
                table_type,
                "Not Started",
                "N/A"
            )
            
    console.print(summary)


# In[ ]:


async def process_table_batch(
    client: fabric.FabricRestClient,
    synapse_utils: SynapseExtractUtils,
    workspace_id: str,
    pipeline_id: str,
    tables: List[ExtractConfigDetails],
    run_year: int,
    run_month: int,
    run_day: int,
    max_parallel: int = 3,
    poll_interval: int = poll_interval_seconds,
    batch_type: str = "Default",
    master_execution_id: Optional[str] = None
) -> None:
    # TODO: review statuses
    """
    Process a batch of tables with pipeline triggering and monitoring.
    
    Args:
        client: Authenticated Fabric REST client
        synapse_utils: Synapse extraction utilities
        workspace_id: Fabric workspace ID
        pipeline_id: ID of the pipeline to run
        tables: List of tables to extract in this batch
        run_year: Year for partitioning
        run_month: Month for partitioning
        run_day: Day for partitioning
        max_parallel: Maximum number of parallel pipelines
        poll_interval: Interval for polling pipeline status (seconds)
        batch_type: Type of batch ("Snapshot" or "Incremental") for logging
        master_execution_id: ID that links related executions together
    """
    terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
    
    # Queue of tables waiting to be processed
    queue = tables.copy()
    
    # Active pipelines currently running
    active_pipelines = []
    
    # Print initial summary for this batch
    print(f"\nStarting {batch_type} batch processing")
    print(f"Tables to process in this batch: {len(tables)}")
    print(f"Maximum parallel pipelines: {max_parallel}")
    
    # Initialize counters for tracking overall progress
    completed_count = 0
    total_count = len(tables)
    
    # Initial batch of pipelines
    initial_batch = queue[:max_parallel]
    queue = queue[max_parallel:]
    
    # Start initial batch
    for table in initial_batch:
        try:
            # Generate execution ID and timestamps before starting
            table.execution_id = table.execution_id or str(uuid.uuid4())
            table.master_execution_id = master_execution_id
            table.start_date = timestamp_now()
            
            # Log the table as "Started" BEFORE triggering the pipeline
            # This helps prevent race conditions by marking the table as in-progress
            # even before the pipeline is triggered
            try:
                await synapse_utils.insert_log_record(
                    execution_id=str(table.execution_id),
                    cfg_synapse_connection_name=table.synapse_connection_name,
                    source_schema_name=table.source_schema_name,
                    source_table_name=table.source_table_name,
                    extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                    partition_clause=table.partition_clause
                        .replace("@year", str(run_year))
                        .replace("@month", str(run_month))
                        .replace("@day", str(run_day)),
                    status="Started",
                    error_messages="",
                    start_date=table.start_date,
                    master_execution_id=master_execution_id
                )
            except Exception as log_error:
                # If logging fails, print the error but continue with pipeline trigger
                print(f"Warning: Failed to write initial log record for {table.source_schema_name}_{table.source_table_name}: {str(log_error)}")
                print(f"Continuing with pipeline trigger...")
            
            # Now trigger the pipeline
            job_id = await trigger_pipeline(
                client=client,
                workspace_id=workspace_id,
                pipeline_id=pipeline_id,
                payload=table.data_pipeline_payload
            )
            
            # Update table with job information
            table.job = DataPipelineJob(job_id=job_id, status="NotStarted")
            active_pipelines.append(table)
            print(f"Started job for {table.source_schema_name}_{table.source_table_name}")
            
            # Update the log record with the job ID
            try:
                await synapse_utils.insert_log_record(
                    execution_id=str(table.execution_id),
                    cfg_synapse_connection_name=table.synapse_connection_name,
                    source_schema_name=table.source_schema_name,
                    source_table_name=table.source_table_name,
                    extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                    partition_clause=table.partition_clause
                        .replace("@year", str(run_year))
                        .replace("@month", str(run_month))
                        .replace("@day", str(run_day)),
                    status="Running",
                    error_messages="",
                    start_date=table.start_date,
                    master_execution_id=master_execution_id
                )
            except Exception as log_error:
                # If logging fails, print the error but continue with pipeline execution
                print(f"Warning: Failed to update log record for {table.source_schema_name}_{table.source_table_name}: {str(log_error)}")
                print(f"Pipeline job will continue running, but logging may be incomplete.")

        except Exception as e:
            # Handle error in pipeline trigger
            print(f"ERROR: Failed to trigger pipeline for {table.source_schema_name}.{table.source_table_name}: {str(e)}")
            completed_count += 1
            
            # Create failed job record
            table.job = DataPipelineJob(job_id="error", status="Failed")
            table.finish_date = timestamp_now()  # Set finish time for error cases
            
            # Log error
            try:
                await synapse_utils.insert_log_record(
                    execution_id=str(table.execution_id),
                    cfg_synapse_connection_name=table.synapse_connection_name,
                    source_schema_name=table.source_schema_name,
                    source_table_name=table.source_table_name,
                    extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                    partition_clause=table.partition_clause
                        .replace("@year", str(run_year))
                        .replace("@month", str(run_month))
                        .replace("@day", str(run_day)),
                    status="Failed",
                    error_messages=f"Failed to start pipeline: {str(e)}",
                    start_date=table.start_date,
                    finish_date=timestamp_now(),
                    master_execution_id=master_execution_id
                )
            except Exception as log_error:
                # If logging fails, just print the error
                print(f"Warning: Failed to write error log for {table.source_schema_name}_{table.source_table_name}: {str(log_error)}")
    
    # Add an initial delay before checking status to give the API time to register the jobs
    if active_pipelines:
        print(f"\nWaiting {poll_interval} seconds before first status check to let API register pipeline jobs...")
        await asyncio.sleep(poll_interval)

    # Process remaining tables as pipelines complete
    while active_pipelines or queue:
        # Print current progress
        print(f"\nProgress: {completed_count}/{total_count} tables processed. Active pipelines: {len(active_pipelines)}")
        
        # Check status of active pipelines
        completed = []
        for table in active_pipelines:
            try:
                table_name = f"{table.source_schema_name}_{table.source_table_name}"
                status, error_message = await check_pipeline(
                    client=client,
                    table_name=table_name,
                    workspace_id=workspace_id,
                    pipeline_id=pipeline_id,
                    job_id=table.job.job_id
                )

                if status is None and "NotFound" in error_message:
                    # Handle transient errors
                    continue
                
                if status in terminal_states or table.job.status == "Failed":
                    # Pipeline completed, add to completed list
                    table.job.status = status
                    completed.append(table)
                    completed_count += 1
                    print(f"Completed job for {table_name}: {table.job.status}")
                    
                    # Log completion
                    ts = timestamp_now()
                    table.finish_date = ts
                    
                    try:
                        await synapse_utils.insert_log_record(
                            execution_id=str(table.execution_id),
                            cfg_synapse_connection_name=table.synapse_connection_name,
                            source_schema_name=table.source_schema_name,
                            source_table_name=table.source_table_name,
                            extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                            partition_clause=table.partition_clause
                                .replace("@year", str(run_year))
                                .replace("@month", str(run_month))
                                .replace("@day", str(run_day)),
                            status=status,
                            error_messages=error_message,
                            start_date=table.start_date,
                            finish_date=ts,
                            update_date=ts,
                            output_path=str(table.output_path) if table.job.status == "Completed" else None,
                            master_execution_id=master_execution_id
                        )
                    except Exception as log_error:
                        # If logging fails, print the error but continue processing
                        print(f"Warning: Failed to write completion log for {table_name}: {str(log_error)}")

                else:
                    # Pipeline still running, update status if changed
                    prev_status = table.job.status
                    table.job.status = status or prev_status
                    
                    if status != prev_status:
                        # Log status change
                        ts = timestamp_now()
                        
                        try:
                            await synapse_utils.insert_log_record(
                                execution_id=str(table.execution_id),
                                cfg_synapse_connection_name=table.synapse_connection_name,
                                source_schema_name=table.source_schema_name,
                                source_table_name=table.source_table_name,
                                extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                                partition_clause=table.partition_clause
                                    .replace("@year", str(run_year))
                                    .replace("@month", str(run_month))
                                    .replace("@day", str(run_day)),
                                status=status,
                                error_messages="",
                                start_date=table.start_date,
                                update_date=ts,
                                master_execution_id=master_execution_id
                            )
                        except Exception as log_error:
                            # If logging fails, print the error but continue processing
                            print(f"Warning: Failed to write status update log for {table_name}: {str(log_error)}")

            except Exception as e:
                # Handle error in pipeline status check
                table_name = f"{table.source_schema_name}_{table.source_table_name}"
                print(f"ERROR: Failed checking status for {table_name}: {str(e)}")
                
                # Check if we have a retry count attribute, if not initialize it
                retry_count = getattr(table, 'status_check_retry_count', 0) + 1
                table.status_check_retry_count = retry_count
                
                if retry_count > 3:  # Max retries for status checks
                    # Too many retries, mark as failed
                    table.job.status = "Failed"
                    completed.append(table)
                    completed_count += 1
                    print(f"Failed job for {table_name} after {retry_count} status check failures")
                    
                    # Log error
                    ts = timestamp_now()
                    table.finish_date = ts
                    
                    try:
                        await synapse_utils.insert_log_record(
                            execution_id=str(table.execution_id),
                            cfg_synapse_connection_name=table.synapse_connection_name,
                            source_schema_name=table.source_schema_name,
                            source_table_name=table.source_table_name,
                            extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                            partition_clause=table.partition_clause
                                .replace("@year", str(run_year))
                                .replace("@month", str(run_month))
                                .replace("@day", str(run_day)),
                            status="Failed",
                            error_messages=f"Error checking status after {retry_count} attempts: {str(e)}",
                            start_date=table.start_date,
                            finish_date=ts,
                            update_date=ts,
                            master_execution_id=master_execution_id
                        )
                    except Exception as log_error:
                        # If logging fails, print the error but continue processing
                        print(f"Warning: Failed to write failure log for {table_name}: {str(log_error)}")

                else:
                    # We'll try again next time around the loop
                    print(f"Status check error for {table_name}, will retry (attempt {retry_count})")
                    # Don't add to completed list so we'll check again
        
        # Remove completed pipelines from active list
        for table in completed:
            active_pipelines.remove(table)
        
        # Start new pipelines if slots are available
        slots_available = max_parallel - len(active_pipelines)
        if slots_available > 0 and queue:
            print(f"\nStarting {min(slots_available, len(queue))} new pipeline(s)...")
            new_batch = queue[:slots_available]
            queue = queue[slots_available:]
            
            for table in new_batch:
                try:
                    # Generate execution ID and timestamps before starting
                    table_name = f"{table.source_schema_name}_{table.source_table_name}"
                    table.execution_id = table.execution_id or str(uuid.uuid4())
                    table.master_execution_id = master_execution_id
                    table.start_date = timestamp_now()
                    
                    # Log the table as "Started" BEFORE triggering the pipeline
                    # This helps prevent race conditions
                    try:
                        await synapse_utils.insert_log_record(
                            execution_id=str(table.execution_id),
                            cfg_synapse_connection_name=table.synapse_connection_name,
                            source_schema_name=table.source_schema_name,
                            source_table_name=table.source_table_name,
                            extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                            partition_clause=table.partition_clause
                                .replace("@year", str(run_year))
                                .replace("@month", str(run_month))
                                .replace("@day", str(run_day)),
                            status="Started",
                            error_messages="",
                            start_date=table.start_date,
                            master_execution_id=master_execution_id
                        )
                    except Exception as log_error:
                        # If logging fails, print the error but continue with pipeline trigger
                        print(f"Warning: Failed to write initial log for {table_name}: {str(log_error)}")
                        print(f"Continuing with pipeline trigger...")
                    
                    # Now trigger the pipeline
                    job_id = await trigger_pipeline(
                        client=client,
                        workspace_id=workspace_id,
                        pipeline_id=pipeline_id,
                        payload=table.data_pipeline_payload
                    )
                    
                    # Update table with job information
                    table.job = DataPipelineJob(job_id=job_id, status="NotStarted")
                    active_pipelines.append(table)
                    print(f"Started job for {table_name}")
                    
                    # Update log record with job ID
                    try:
                        await synapse_utils.insert_log_record(
                            execution_id=str(table.execution_id),
                            cfg_synapse_connection_name=table.synapse_connection_name,
                            source_schema_name=table.source_schema_name,
                            source_table_name=table.source_table_name,
                            extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                            partition_clause=table.partition_clause
                                .replace("@year", str(run_year))
                                .replace("@month", str(run_month))
                                .replace("@day", str(run_day)),
                            status="Running",
                            error_messages="",
                            start_date=table.start_date,
                            master_execution_id=master_execution_id
                        )
                    except Exception as log_error:
                        # If logging fails, print the error but continue with pipeline execution
                        print(f"Warning: Failed to update log for {table_name}: {str(log_error)}")
                        print(f"Pipeline job will continue running, but logging may be incomplete.")

                except Exception as e:
                    # Handle error in pipeline trigger
                    table_name = f"{table.source_schema_name}_{table.source_table_name}"
                    print(f"ERROR: Failed to trigger pipeline for {table_name}: {str(e)}")
                    table.job = DataPipelineJob(job_id="error", status="Failed")
                    table.finish_date = timestamp_now()  # Set finish time for error cases
                    completed_count += 1
                    
                    # Log error
                    ts = timestamp_now()

                    try:
                        ts = timestamp_now()
                        await synapse_utils.insert_log_record(
                            execution_id=str(table.execution_id),
                            cfg_synapse_connection_name=table.synapse_connection_name,
                            source_schema_name=table.source_schema_name,
                            source_table_name=table.source_table_name,
                            extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                            partition_clause=table.partition_clause
                                .replace("@year", str(run_year))
                                .replace("@month", str(run_month))
                                .replace("@day", str(run_day)),
                            status="Failed",
                            error_messages=f"Failed to start pipeline: {str(e)}",
                            start_date=table.start_date,
                            finish_date=ts,
                            update_date=ts,
                            master_execution_id=master_execution_id
                        )
                    except Exception as log_error:
                        # If logging fails, just print the error
                        print(f"Warning: Failed to write error log for {table_name}: {str(log_error)}")

            if new_batch:
                print(f"Waiting {poll_interval} seconds to let API register new pipeline jobs...")
                await asyncio.sleep(poll_interval)

        # Only sleep if there are active pipelines or items in queue
        if active_pipelines or queue:
            await asyncio.sleep(poll_interval)
    
    # Print batch completion message
    print(f"\n{'-' * 80}")
    print(f"{batch_type.upper()} BATCH COMPLETED")
    print(f"Total tables processed in this batch: {completed_count}/{total_count}")
    print(f"{'-' * 80}\n")


# In[ ]:


def check_snapshot_tables_already_processed(synapse_utils, master_execution_id: str) -> set:
    """
    Check if snapshot tables have already been processed or are currently in progress
    in the current master execution.
    
    Args:
        synapse_utils: SynapseExtractUtils instance
        master_execution_id: The master execution ID to check
        
    Returns:
        A set of tuples (schema_name, table_name) of snapshot tables already processed or in progress
    """
    try:
        # Check if master_execution_id is provided
        if not master_execution_id:
            print("No master_execution_id provided for snapshot table check")
            return set()
            
        # Load the log table data
        log_table_uri = synapse_utils.log_table_uri
        
        # Check if log table exists
        try:
            log_df = DeltaTable(log_table_uri).to_pyarrow_table().to_pandas()
        except Exception as e:
            print(f"Log table does not exist yet or cannot be accessed: {str(e)}")
            return set()
        
        # Filter for executions with the current master_execution_id
        # Include tables that are in any state (Reserved, Started, Running, Completed, Failed)
        # This prevents the race condition where multiple execution groups try to process the same snapshot table
        if 'master_execution_id' not in log_df.columns or 'status' not in log_df.columns:
            print("Log table does not have required columns (master_execution_id, status)")
            return set()
        
        # Check for any tables with this master_execution_id - not just completed ones
        # This prevents race conditions when tables are being processed simultaneously
        processed_logs = log_df[log_df['master_execution_id'] == master_execution_id]
        
        if processed_logs.empty:
            print(f"No tables found for master execution ID: {master_execution_id}")
            return set()
        
        # Filter for snapshot tables (empty partition_clause)
        snapshot_logs = processed_logs[
            (processed_logs['partition_clause'].isna()) | 
            (processed_logs['partition_clause'] == '')
        ]
        
        if snapshot_logs.empty:
            print(f"No snapshot tables found for master execution ID: {master_execution_id}")
            return set()
        
        # Create a set of (schema_name, table_name) tuples for all snapshot tables in this execution
        # This includes both completed and in-progress tables
        processed_snapshots = set(
            zip(snapshot_logs['source_schema_name'], snapshot_logs['source_table_name'])
        )
        
        # Group by status for detailed reporting
        status_counts = snapshot_logs.groupby('status').size().to_dict()
        
        print(f"Found {len(processed_snapshots)} snapshot tables for master execution ID: {master_execution_id}")
        print(f"Status breakdown: {status_counts}")
        
        for schema, table in processed_snapshots:
            statuses = snapshot_logs[
                (snapshot_logs['source_schema_name'] == schema) & 
                (snapshot_logs['source_table_name'] == table)
            ]['status'].tolist()
            print(f"  - {schema}.{table} (Status: {', '.join(statuses)})")
        
        return processed_snapshots
        
    except Exception as e:
        print(f"Error checking for processed snapshot tables: {str(e)}")
        return set()

async def reserve_snapshot_tables(
    synapse_utils, 
    tables: List[ExtractConfigDetails], 
    master_execution_id: str
) -> List[ExtractConfigDetails]:
    """
    Pre-reserve snapshot tables by marking them in the log table with 'Reserved' status.
    This helps prevent race conditions in parallel processing.
    
    Args:
        synapse_utils: SynapseExtractUtils instance
        tables: List of tables to check
        master_execution_id: The master execution ID for this execution
        
    Returns:
        List of tables that were successfully reserved and should be executed
    """
    if not master_execution_id:
        print("No master_execution_id provided, skipping snapshot reservation")
        return []
    
    # Filter for snapshot tables only
    snapshot_tables = [table for table in tables if not table.partition_clause]
    
    if not snapshot_tables:
        print("No snapshot tables to reserve")
        return []
    
    print(f"Pre-reserving {len(snapshot_tables)} snapshot tables to prevent race conditions")
    
    # First, check what's already reserved/processed to avoid duplicate entries
    already_processed = check_snapshot_tables_already_processed(synapse_utils, master_execution_id)
    
    # Now reserve each table that isn't already processed
    reserved_tables = []
    for table in snapshot_tables:
        table_key = (table.source_schema_name, table.source_table_name)
        
        if table_key in already_processed:
            print(f"  - {table.source_schema_name}.{table.source_table_name} already reserved or processed, skipping")
            continue
        
        # Generate execution ID and timestamps for reservation
        table.execution_id = table.execution_id or str(uuid.uuid4())
        table.master_execution_id = master_execution_id
        table.start_date = timestamp_now()
        
        # Insert reservation record in the log table
        try:
            await synapse_utils.insert_log_record(
                execution_id=str(table.execution_id),
                cfg_synapse_connection_name=table.synapse_connection_name,
                source_schema_name=table.source_schema_name,
                source_table_name=table.source_table_name,
                extract_file_name=f"{table.source_schema_name}_{table.source_table_name}",
                partition_clause="",  # Snapshot tables have empty partition clause
                status="Reserved",  # Special status to indicate reservation
                error_messages="",
                start_date=table.start_date,
                master_execution_id=master_execution_id
            )
            reserved_tables.append(table)
            print(f"  - Reserved {table.source_schema_name}.{table.source_table_name}")
        except Exception as e:
            print(f"  - Failed to reserve {table.source_schema_name}.{table.source_table_name}: {str(e)}")
    
    print(f"Successfully reserved {len(reserved_tables)} new snapshot tables")
    return reserved_tables


# ## Initialisation

# In[ ]:


client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()
console = Console()  # Keep for tables but use regular print for most output

lakehouse = LakehouseUtils(config_workspace_id, config_lakehouse_id)
config_utils = ConfigUtils(lakehouse)

configs = config_utils.get_configs_as_object(fabric_environment)
if not configs:
    raise ValueError(f"No configuration found for environment: {fabric_environment}")

config_table_uri = f"{lakehouse.lakehouse_tables_uri()}config_synapse_extracts"
extract_config_df = DeltaTable(config_table_uri).to_pyarrow_table().to_pandas()

# Get datasource details for Synapse connection
datasource_name = extract_config_df.loc[0, "synapse_datasource_name"]
datasource_location = extract_config_df.loc[0, "synapse_datasource_location"]
pipeline_id = extract_config_df.loc[0, "pipeline_id"]

synapse_utils = SynapseExtractUtils(
    datasource_name=datasource_name,
    datasource_location=datasource_location,
    workspace_id=config_workspace_id,
    lakehouse_id=config_lakehouse_id
)

# Generate master_execution_id if not provided
if master_execution_id is None:
    master_execution_id = str(uuid.uuid4())
    print(f"No master_execution_id provided. Generated new ID: {master_execution_id}")
else:
    print(f"Using provided master_execution_id: {master_execution_id}")

# Filter by execution group if specified
if execution_group is not None:
    filtered_df = extract_config_df[
        (extract_config_df["execution_group"] == execution_group) &
        (extract_config_df["active_yn"] == "Y")
    ]
    print(f"Filtered to {len(filtered_df)} tables in execution group {execution_group}")
else:
    filtered_df = extract_config_df[extract_config_df["active_yn"] == "Y"]
    print(f"Processing all {len(filtered_df)} active tables")

# Apply snapshot tables filtering with detailed logging
original_table_count = len(filtered_df)
snapshot_count = len(filtered_df[filtered_df["partition_clause"].str.strip() == ""])
incremental_count = original_table_count - snapshot_count

print(f"\nTable breakdown:")
print(f"- Total active tables: {original_table_count}")
print(f"- Snapshot tables (no partition clause): {snapshot_count}")
print(f"- Incremental tables (with partition clause): {incremental_count}")

# Get the set of already processed or in-progress snapshot tables for this master execution
processed_snapshots = set()
if master_execution_id:
    # Check for snapshots - this now includes both completed and in-progress tables
    processed_snapshots = check_snapshot_tables_already_processed(synapse_utils, master_execution_id)
    print(f"Retrieved {len(processed_snapshots)} snapshot tables to exclude from processing")

# Apply filtering based on snapshot tables
if not include_snapshot_tables:
    # Filter out all snapshot tables when include_snapshot_tables is False
    filtered_df = filtered_df[filtered_df["partition_clause"].str.strip() != ""]
    print(f"\nExcluded all {snapshot_count} snapshot tables because include_snapshot_tables=False")
    print(f"Remaining tables: {len(filtered_df)}")
elif processed_snapshots:
    # Filter out previously processed or in-progress snapshot tables when we have a master_execution_id
    # First identify snapshot tables
    snapshot_mask = filtered_df["partition_clause"].str.strip() == ""
    
    # Create a mask for tables already processed or in progress
    processed_mask = filtered_df.apply(
        lambda row: (row["source_schema_name"], row["source_table_name"]) in processed_snapshots 
        if snapshot_mask.loc[row.name] else False,
        axis=1
    )
    
    # Detailed logging to help with debugging
    if processed_mask.any():
        print("\nSnapshot tables being excluded (already processed or in progress):")
        excluded_tables = filtered_df[processed_mask]
        for idx, row in excluded_tables.iterrows():
            print(f"  - {row['source_schema_name']}.{row['source_table_name']}")
    
    # Remove already processed or in-progress snapshot tables
    filtered_df = filtered_df[~processed_mask]
    
    processed_count = processed_mask.sum()
    remaining_snapshot_count = snapshot_count - processed_count
    
    print(f"\nSnapshot tables processing:")
    print(f"- Already processed or in-progress snapshot tables for master_execution_id {master_execution_id}: {processed_count}")
    print(f"- Remaining unprocessed snapshot tables: {remaining_snapshot_count}")
    print(f"- Total tables to process: {len(filtered_df)} (of original {original_table_count})")
else:
    print(f"\nIncluding all {snapshot_count} snapshot tables")
    if master_execution_id:
        print(f"No snapshot tables are processed or in progress yet for master_execution_id: {master_execution_id}")

# Create list of tables to extract from filtered DataFrame
tables_to_extract = [
    ExtractConfigDetails(**table) 
    for table in filtered_df.to_dict(orient='records')
]

# Pre-reserve all snapshot tables to prevent race conditions in parallel processing
# And add the successfully reserved tables to the execution list
reserved_tables = []
if include_snapshot_tables and master_execution_id:
    reserved_tables = await reserve_snapshot_tables(synapse_utils, tables_to_extract, master_execution_id)
    
    # Check for tables that were reserved in previous runs but not executed
    # Get list of tables with 'Reserved' status for this master execution ID
    try:
        log_table_uri = synapse_utils.log_table_uri
        log_df = DeltaTable(log_table_uri).to_pyarrow_table().to_pandas()
        
        if not log_df.empty and 'master_execution_id' in log_df.columns and 'status' in log_df.columns:
            # Find reserved tables that haven't been updated to Started/Running/Completed/Failed
            reserved_logs = log_df[
                (log_df['master_execution_id'] == master_execution_id) & 
                (log_df['status'] == 'Reserved')
            ]
            
            if not reserved_logs.empty:
                print(f"\nFound {len(reserved_logs)} previously reserved tables that need to be executed")
                
                # Check if these tables are already in our tables_to_extract list
                existing_tables = {(t.source_schema_name, t.source_table_name): t for t in tables_to_extract}
                
                for idx, row in reserved_logs.iterrows():
                    table_key = (row['source_schema_name'], row['source_table_name'])
                    
                    # Skip if already in our list
                    if table_key in existing_tables:
                        print(f"  - {table_key[0]}.{table_key[1]} is already in the execution list")
                        continue
                    
                    # Find the table in the config DataFrame
                    config_row = extract_config_df[
                        (extract_config_df['source_schema_name'] == table_key[0]) & 
                        (extract_config_df['source_table_name'] == table_key[1])
                    ]
                    
                    if not config_row.empty:
                        # Create the table object
                        table_data = config_row.iloc[0].to_dict()
                        table = ExtractConfigDetails(**table_data)
                        
                        # Add the execution details from the log
                        table.execution_id = row['execution_id']
                        table.start_date = row['start_date']
                        table.master_execution_id = master_execution_id
                        
                        # Add to our reserved tables
                        reserved_tables.append(table)
                        print(f"  - Added {table_key[0]}.{table_key[1]} to execution list")
    except Exception as e:
        print(f"Error checking for previously reserved tables: {str(e)}")

# Make sure we're processing all the reserved tables (from this run and previous runs)
if reserved_tables:
    print(f"\nAdding {len(reserved_tables)} reserved snapshot tables to the extraction queue")
    
    # Check for duplicates before adding
    existing_tables = {(t.source_schema_name, t.source_table_name) for t in tables_to_extract}
    
    # Add only tables that aren't already in the list
    for table in reserved_tables:
        table_key = (table.source_schema_name, table.source_table_name)
        if table_key not in existing_tables:
            tables_to_extract.append(table)
            existing_tables.add(table_key)

print("\n" + "=" * 80)
print("SYNAPSE EXTRACTION CONFIGURATION")
print("=" * 80)
print(f"Date parameters: Year={run_year}, Month={run_month}, Day={run_day}")
print(f"Execution group: {execution_group if execution_group is not None else 'All'}")
print(f"Total tables selected: {len(tables_to_extract)}")
print(f"Include snapshot tables: {include_snapshot_tables}")
print(f"Master execution ID: {master_execution_id}")
print(f"Max parallel pipelines: {max_parallel_pipelines}")
print("=" * 80 + "\n")


# In[71]:


for table in tables_to_extract:
    if table.partition_clause != '':
        print(f"{table.source_schema_name}.{table.source_table_name}")
        print(table)
        print()


# ## Execute extraction

# In[72]:


sql_template = synapse_utils.get_extract_sql_template()
prepare_extract_payloads(
    tables=tables_to_extract,
    sql_template=sql_template,
    run_year=run_year,
    run_month=run_month,
    run_day=run_day
)


# In[73]:


# Debug: View SQL for a specific table if needed
def view_sql_for_table(table_name):
    """View the generated SQL for a specific table by name."""
    for table in tables_to_extract:
        if f"{table.source_schema_name}.{table.source_table_name}" == str(table_name):
            if table.data_pipeline_payload and "executionData" in table.data_pipeline_payload:
                script = table.data_pipeline_payload["executionData"]["parameters"]["script_content"]
                print(script)
                return
# view_sql_for_table("EDL.HANA_FCT_SALES_CLAIM_LINE_ITEM")


# In[ ]:


await trigger_and_monitor_pipelines(
    client=client,
    synapse_utils=synapse_utils,
    workspace_id=workspace_id,
    pipeline_id=pipeline_id,
    tables=tables_to_extract,
    run_year=run_year,
    run_month=run_month,
    run_day=run_day,
    max_parallel=max_parallel_pipelines,
    poll_interval=poll_interval_seconds,
    master_execution_id=master_execution_id
)


# ## Extraction summary

# In[ ]:


# Compute execution summary
successful = len([t for t in tables_to_extract if t.job and t.job.status == "Completed"])
failed = len([t for t in tables_to_extract if t.job and t.job.status == "Failed"])
not_started = len([t for t in tables_to_extract if not t.job])
other = len(tables_to_extract) - successful - failed - not_started
total = len(tables_to_extract)

# Display summary using print statements
print("\n" + "=" * 80)
print(f"EXTRACTION SUMMARY - Group {execution_group if execution_group is not None else 'All'}")
print("=" * 80)

if total > 0:
    print(f"{'Status':<15} {'Count':<10} {'Percentage':<15}")
    print(f"{'-' * 15} {'-' * 10} {'-' * 15}")
    print(f"{'Completed':<15} {successful:<10} {successful/total*100:.1f}%")
    print(f"{'Failed':<15} {failed:<10} {failed/total*100:.1f}%")
    print(f"{'Not Started':<15} {not_started:<10} {not_started/total*100:.1f}%")
    print(f"{'Other Status':<15} {other:<10} {other/total*100:.1f}%")
    print(f"{'-' * 40}")
    print(f"{'Total':<15} {total:<10} 100.0%")
else:
    print("No tables to process (0)")

print("=" * 80 + "\n")

summary_table = Table(title=f"Detailed Extraction Results")
summary_table.add_column("Status", style="bold")
summary_table.add_column("Count", style="bold")
summary_table.add_column("Percentage", style="bold")

if total > 0:
    summary_table.add_row("Completed", str(successful), f"{successful/total*100:.1f}%")
    summary_table.add_row("Failed", str(failed), f"{failed/total*100:.1f}%")
    summary_table.add_row("Not Started", str(not_started), f"{not_started/total*100:.1f}%")
    summary_table.add_row("Other Status", str(other), f"{other/total*100:.1f}%")
    summary_table.add_row("Total", str(total), "100.0%")
else:
    summary_table.add_row("No tables", "0", "0%")

console = Console()
console.print(summary_table)

# Return execution status (for orchestration)
result = {
    "total": total,
    "successful": successful,
    "failed": failed,
    "not_started": not_started,
    "other": other,
    "execution_group": execution_group,
    "run_date": f"{run_year}-{run_month:02d}-{run_day:02d}"
}
print(json.dumps(result))

