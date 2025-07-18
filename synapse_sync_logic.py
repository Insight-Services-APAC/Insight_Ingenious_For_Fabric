#!/usr/bin/env python
# coding: utf-8

# ## synapse_extract_daily_driver
# 
# New notebook

# # **Synapse Extract Daily Driver**
# Author: Sanath More  
# Date: 2025-07-14
# 
# ## Overview
# 
# The Synapse extraction process automates the extraction of data from Azure Synapse Analytics to Parquet files stored in ADLS. It supports two primary modes:
# 
# 1. **Daily Extraction** - Runs for a specified date range (4-day sliding window) for incremental extracts whilst capturing a snapshot of all other objects
# 2. **Historical Extraction** - Processes specific tables/dates as defined in JSON input parameter `WORK_ITEMS_JSON`
# 
# The process leverages Microsoft Fabric's REST API to trigger pipelines that execute CETAS (CREATE EXTERNAL TABLE AS SELECT) operations in Synapse.
# 
# ## Architecture
# 
# 
# ### Key Components:
# 
# - **Driver Notebook** (`synapse_extract_daily_driver`) - Main orchestration notebook
# - **Configuration Tables** - Store metadata about extraction objects and environments
# - **Fabric Pipeline** - Executes the actual CETAS operations
# - **Retry Helper** (`synapse_extract_retry_helper`) - Assists with retrying failed extractions
# 
# ## Configuration
# 
# ### Primary Notebook Parameters
# 
# | Parameter | Description | Default |
# |-----------|-------------|---------|
# | `MASTER_EXECUTION_ID` | Unique ID for grouping extractions (auto-generated if None) | `None` |
# | `WORK_ITEMS_JSON` | JSON-encoded list for historical mode (passed to this notebook if execution triggered via `synapse_extract_historical_driver`) | `None` |
# | `MAX_CONCURRENCY` | Maximum parallel extractions | `10` |
# | `INCLUDE_SNAPSHOTS` | Controls whether to include snapshot tables | `True` |
# | `DAILY_RUN_START_DATE` | Start date for daily extraction | Current date - 4 days |
# | `DAILY_RUN_END_DATE` | End date for daily extraction | Current date |
# 
# ### Configuration Tables
# 
# 1. **`synapse_extract_objects`** - Contains metadata about source objects to extract:
#    - Source schema and table/view names
#    - Extraction mode (incremental/snapshot)
#    - Filtering clauses
#    - Execution group (for ordering)
# 
# | Column | Type | Description |
# |--------|------|-------------|
# | `synapse_connection_name` | STRING | Name of the Synapse connection |
# | `source_schema_name` | STRING | Schema name in the source database |
# | `source_table_name` | STRING | Table or view name in the source database |
# | `extract_mode` | STRING | 'snapshot' (full extract) or 'incremental' (date-based) |
# | `single_date_filter` | STRING | SQL filter for single date extracts (e.g., 'WHERE DATE_SK = @date') |
# | `date_range_filter` | STRING | SQL filter for date range extracts (e.g., 'WHERE DATE_SK BETWEEN @start_date AND @end_date') |
# | `execution_group` | INT | Order grouping for dependencies (lower numbers execute first) |
# | `active_yn` | STRING | 'Y' or 'N' flag to include/exclude tables from extraction |
# | `pipeline_id` | STRING | ID of the Fabric pipeline to execute |
# | `synapse_datasource_name` | STRING | Name of the external data source in Synapse |
# | `synapse_datasource_location` | STRING | Location URL for the external data source |
# 
# 2. **`synapse_extract_run_log`** - Records execution history:
#    - Execution IDs and parameters
#    - Source and target information
#    - Timing and status details
# 
# | Column | Type | Description |
# |--------|------|-------------|
# | `master_execution_id` | STRING | Unique ID grouping related extractions |
# | `execution_id` | STRING | Unique ID for individual extraction |
# | `pipeline_job_id` | STRING | Fabric pipeline run ID |
# | `execution_group` | INT | Matches the execution_group from objects table |
# | `master_execution_parameters` | STRING | JSON-encoded parameters for the execution |
# | `trigger_type` | STRING | 'Manual' or 'Scheduled' |
# | `config_synapse_connection_name` | STRING | Name of the Synapse connection |
# | `source_schema_name` | STRING | Schema name of the extracted object |
# | `source_table_name` | STRING | Table/view name of the extracted object |
# | `extract_mode` | STRING | 'incremental' or 'snapshot' |
# | `extract_start_dt` | DATE | Start date for incremental extraction (NULL for snapshots) |
# | `extract_end_dt` | DATE | End date for incremental extraction (NULL for snapshots) |
# | `partition_clause` | STRING | SQL WHERE clause used for filtering |
# | `output_path` | STRING | Output path where Parquet files are stored |
# | `extract_file_name` | STRING | Base name for the extracted files |
# | `external_table` | STRING | Name of external table created |
# | `start_timestamp` | TIMESTAMP | When the extraction started |
# | `end_timestamp` | TIMESTAMP | When the extraction completed |
# | `duration_sec` | DOUBLE | Duration in seconds |
# | `status` | STRING | 'Queued', 'Claimed', 'Running', 'Completed', 'Failed', etc. |
# | `error_messages` | STRING | Error details if failed |
# 
# ## Extraction Process
# 
# The extraction process follows these steps:
# 
# 1. **Initialisation**:
#    - Load configuration for the specified Fabric environment
#    - Initialise utilities and parameters for extractions
# 
# 2. **Work Item Construction**:
#    - For daily mode: Generate work items based on active tables in the config table
#    - For historical mode: Parse work items from the provided JSON (generated by notebook `synapse_extract_historical_driver`)
# 
# 3. **Payload Preparation**:
#    - Generate SQL scripts for each extraction
#    - Create path and naming structures
#    - Prepare pipeline payloads
# 
# 4. **Orchestration**:
#    - Pre-log all extractions with "Queued" status
#    - Process each execution group sequentially
#    - Within each group, process extractions in parallel up to `MAX_CONCURRENCY`
# 
# 5. **Monitoring and Logging**:
#    - Update status in the run log at each stage
#    - Poll pipelines until completion
#    - Calculate and record execution statistics
# 
# 6. **Summary Generation**:
#    - Compile statistics on successful and failed extractions
#    - Display detailed error information for any failures
# 
# ## Extraction Modes
# 
# ### Incremental Mode
# 
# Extracts data for a specific date range using filters:
# ```sql
# where DATE_SK between 20230101 and 20230104
# ```
# 
# Data is organised in paths:
# ```
# exports/incremental/{SCHEMA}_{OBJECT_NAME}/YYYY/MM/DD/
# ```
# 
# ### Snapshot Mode
# 
# Extracts the entire table without date filtering. Data is stored in paths:
# ```
# exports/snapshot/{SCHEMA}_{OBJECT_NAME}/
# ```
# 
# ## Execution Groups
# 
# Tables are organised into execution groups to control extraction order. All extractions in a group must complete before moving to the next group.
# 
# ## Error Handling
# 
# The process includes error handling:
# - Exponential backoff with jitter for retries
# - Detailed logging of all errors
# - Status tracking in the run log table
# - Continuation options to handle partial failures
# - If programmatic error handling is insufficient, e.g. in cases of Fabric capacity limits, the user can trigger a retry for a given `master_execution_id` for incomplete work items using notebook `synapse_extract_retry_helper`.
# 
# ## Monitoring and Troubleshooting
# 
# ### Log Table Queries
# 
# To check extraction status:
# ```sql
# select *
# from synapse_extract_run_log
# where master_execution_id = 'your-execution-id'
# ```
# 
# ### Retry Process
# 
# For failed extractions, use the `synapse_extract_retry_helper` notebook:
# 1. Set the `MASTER_EXECUTION_ID` of the failed/incomplete run
# 2. Run the notebook to retry incomplete extractions
# 
# ## Performance Considerations
# 
# - Adjust `MAX_CONCURRENCY` based on Fabric capacity
# - Group similar tables in the same execution group
# - Consider excluding snapshot tables during historical extractions
# 

# ![download.png](attachment:ae412f27-b96b-4c5e-99a9-180b5345a633.png)

# ## Architecture diagram generation

# In[1]:


GENERATE_DIAGRAM = False


# In[2]:


if GENERATE_DIAGRAM:
    from IPython import get_ipython
    get_ipython().run_line_magic(
        "pip", "install diagrams cairosvg --quiet"
    )


# In[3]:


if GENERATE_DIAGRAM:
    import base64
    import cairosvg
    from urllib.request import urlretrieve
    from diagrams import Diagram, Cluster, Edge
    from diagrams.azure.analytics import SynapseAnalytics
    from diagrams.azure.storage import DataLakeStorage
    from diagrams.generic.storage import Storage
    from diagrams.custom import Custom
    from pathlib import Path
    from IPython.display import Image, display

    fabric_icons = ["notebook", "data_warehouse", "pipeline"]
    base_url = (
        "https://raw.githubusercontent.com/FabricTools/fabric-icons/"
        "refs/heads/main/node_modules/%40fabric-msft/svg-icons/dist/svg"
    )
    for icon in fabric_icons:
        svg = f"{icon}_64_item.svg"
        png = f"{icon}_64_item.png"
        if not Path(png).exists():
            cairosvg.svg2png(url=f"{base_url}/{svg}", write_to=png, dpi=1000)

    with Diagram(
        "Synapse Data Extraction - Fabric Architecture",
        filename="synapse_extract_architecture_diagram",
        outformat="png",
        show=False,
        graph_attr={
            "rankdir": "LR",
            "nodesep": "2.5",
            "ranksep": "3",
            "pad": "2.5",
            "splines": "ortho",
            "remincross": "true",
            "fontname": "Helvetica",
            "overlap": "false",
        },
        node_attr={
            "fontname": "Helvetica", 
            "fontsize": "10",
            "margin": "0.4,0.3",
            "height": "1.2",
        },
        edge_attr={
            "fontname": "Helvetica",
            "fontsize": "9",
            "labeldistance": "2",
            "labelangle": "0",
            "minlen": "2",
        },
    ) as diag:

        # source
        with Cluster("Azure", graph_attr={"margin": "20"}):
            synapse = SynapseAnalytics("Synapse\n(Tables / Views)")

        # destination
        with Cluster("Destination", graph_attr={"margin": "20"}):
            adls = DataLakeStorage("ADLS Storage\n(Parquet files)")
            with Cluster("ADLS Structure", graph_attr={"margin": "15"}):
                incremental = Storage("Incremental\n/exports/incremental/{SCHEMA}_{TABLE}/YYYY/MM/DD/")
                snapshot = Storage("Snapshot\n/exports/snapshot/{SCHEMA}_{TABLE}/")

        # fabric
        with Cluster("Fabric", graph_attr={"margin": "20"}):
            config_tables = Custom("Configuration Tables", "data_warehouse_64_item.png")

            # orchestration
            with Cluster("Orchestration Layer", graph_attr={"margin": "25"}):
                historical = Custom("Historical Driver", "notebook_64_item.png")
                daily = Custom("Daily Driver", "notebook_64_item.png")
                retry = Custom("Retry Helper", "notebook_64_item.png")

            # execution
            with Cluster("Execution Layer", graph_attr={"margin": "20"}):
                pipeline = Custom("Fabric Pipeline", "pipeline_64_item.png")

        config_tables >> Edge(
            xlabel="Read config", 
            penwidth="1.2", 
            color="darkgreen", 
            minlen="1.5"
        ) >> daily
        
        daily >> Edge(
            xlabel="Write logs", 
            penwidth="1.2", 
            color="darkblue",
            constraint="false",
            ltail="cluster_Orchestration Layer",
            lhead="cluster_Fabric",
            style="curved"
        ) >> config_tables

        config_tables >> Edge(style="invis", weight="0.5") >> historical
        config_tables >> Edge(style="invis", weight="0.5") >> retry

        historical >> Edge(
            xlabel="Optional: pass work-items JSON", 
            style="dashed", 
            color="gray",
            minlen="2"
        ) >> daily
        
        retry >> Edge(
            xlabel="Optional: re-queue failures", 
            style="dashed", 
            color="gray",
            minlen="1.5"
        ) >> daily

        daily >> Edge(
            xlabel="Trigger CETAS", 
            penwidth="1.2"
        ) >> pipeline
        
        pipeline >> Edge(
            xlabel="Execute CETAS", 
            penwidth="1.2"
        ) >> synapse
        
        synapse >> Edge(
            xlabel="Write Parquet", 
            penwidth="1.2"
        ) >> adls
        
        synapse >> Edge(style="invis", weight="0.5") >> incremental
        synapse >> Edge(style="invis", weight="0.5") >> snapshot


# In[4]:


if GENERATE_DIAGRAM:
    from IPython.display import Image, display
    img_path = "synapse_extract_architecture_diagram.png"
    display(Image(filename=img_path))


# ## Import libraries

# In[5]:


import asyncio
import nest_asyncio
import json
import logging
import uuid
import time
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import sempy.fabric as fabric
import numpy as np
import pyarrow as pa
from delta.tables import DeltaTable

import pyspark.sql.functions as F
from pyspark.sql.types import *


# ## Configure logging

# In[6]:


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# ## Define parameters

# In[7]:


MASTER_EXECUTION_ID = None  # auto-UUID if None
WORK_ITEMS_JSON     = None  # JSON-encoded list for historical mode
MAX_CONCURRENCY     = 10

INCLUDE_SNAPSHOTS   = True  # sets whether to include snapshot tables in the extraction
DAILY_RUN_START_DATE : date = (datetime.utcnow() - timedelta(days=4)).date()
DAILY_RUN_END_DATE : date = datetime.utcnow().date()


# ## Define constants

# In[8]:


WORKSPACE_ID = "2e464f52-4667-4cd9-9bc9-ed8887658567"
LAKEHOUSE_ID = "34b88168-9d0e-468a-81b1-35054a98e9b8"
FABRIC_ENVIRONMENT = "development"


# In[9]:


POLL_INTERVAL    = 30
FAST_RETRY_SEC   = 3
TRANSIENT_HTTP   = {429, 500, 503, 504, 408}


# In[10]:


MASTER_EXECUTION_PARAMETERS = {
    "start_date": {
        "year": DAILY_RUN_START_DATE.year,
        "month": DAILY_RUN_START_DATE.month,
        "day": DAILY_RUN_START_DATE.day
    },
    "end_date": {
        "year": DAILY_RUN_END_DATE.year,
        "month": DAILY_RUN_END_DATE.month,
        "day": DAILY_RUN_END_DATE.day
    }
}

TRIGGER_TYPE = "Manual"


# ## Utility classes

# ### Lakehouse Utils

# In[11]:


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


# ### Config Utils

# In[12]:


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
        table = DeltaTable.forPath(
            spark,
            self.fabric_environments_table_uri
        ).toDF().toPandas()
        row = table[table["fabric_environment"] == fabric_environment]
        return row.iloc[0].to_dict() if not row.empty else None

    def get_configs_as_object(self, fabric_environment: str) -> Optional[FabricConfig]:
        """Get configuration for a specific environment as a FabricConfig object."""
        row_dict = self.get_configs_as_dict(fabric_environment)
        return FabricConfig(**row_dict) if row_dict else None


# ### Synapse Extract Utils

# In[13]:


class SynapseExtractUtils: 
    """
    Utility class for Synapse data extraction operations.
    
    This class provides functionality for creating CETAS (CREATE EXTERNAL TABLE AS SELECT) operations,
    managing extraction logs, and handling error conditions during the data extraction process.
    
    Attributes:
        LOG_SCHEMA (StructType): Schema definition for the extraction run log table
        datasource_name (str): Name of the Synapse datasource
        datasource_location (str): Location URI of the Synapse datasource
        config_table_uri (str): URI to the configuration table containing extraction objects
        log_table_uri (str): URI to the extraction run log table
    """
    
    # Schema defined once as a class attribute
    LOG_SCHEMA = StructType([
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
        StructField("error_messages", StringType(), True)
    ])

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
        self.config_table_uri = f"{base_lh_table_path}/synapse_extract_objects"
        self.log_table_uri = f"{base_lh_table_path}/synapse_extract_run_log"

    def get_extract_sql_template(self) -> str:
        """
        Get the SQL template for CETAS extraction.
        
        This template includes:
        - Creation of the parquet file format if it doesn't exist
        - Creation of the external data source if it doesn't exist
        - Dropping any existing external table with the same name
        - Creating a new external table with data from the source table
        
        Returns:
            str: SQL template with placeholders for dynamic elements
        """
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
            name = '{self.datasource_name.strip()}'
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

    def _with_retry(self, fn, max_retries=5, initial_delay=0.5):
        """
        Execute a function with exponential backoff retry logic.
        
        Args:
            fn: Function to execute
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds before first retry
            
        Returns:
            Result of the function if successful
            
        Raises:
            Exception: Re-raises the last exception after all retries are exhausted
        """
        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                return fn()
            except Exception as exc:
                # If last attempt, re-raise
                if attempt == max_retries:
                    raise
                # Log and sleep with jitter
                logger.warning(f"Attempt {attempt}/{max_retries} failed: {exc}, retrying in {delay:.2f}s...")
                time.sleep(delay + random.uniform(0, delay))
                delay *= 2  # exponential backoff

    def bulk_insert_queued_extracts(
        self,
        extraction_payloads: List[Dict], 
        master_execution_id: str,
        master_execution_parameters: Dict = None,
        trigger_type: str = "Manual"
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
        ts_now = datetime.utcnow()
        
        for item in extraction_payloads:
            # Convert string dates to date objects if needed
            extract_start = None
            extract_end = None
            if item.get("extract_start"):
                extract_start = datetime.strptime(item["extract_start"], "%Y-%m-%d").date() if isinstance(item["extract_start"], str) else item["extract_start"]
            if item.get("extract_end"):
                extract_end = datetime.strptime(item["extract_end"], "%Y-%m-%d").date() if isinstance(item["extract_end"], str) else item["extract_end"]
            
            record = {
                "master_execution_id": master_execution_id,
                "execution_id": str(uuid.uuid4()),
                "pipeline_job_id": None,
                "execution_group": item.get("execution_group"),
                "master_execution_parameters": json.dumps(master_execution_parameters) if master_execution_parameters else None,
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
                "error_messages": None
            }
            records.append(record)
        
        def do_insert():
            df = spark.createDataFrame(records, schema=self.LOG_SCHEMA)
            df.write.format("delta").mode("append").partitionBy("master_execution_id").save(self.log_table_uri)
            
        try:
            self._with_retry(do_insert)
            logger.info(f"Successfully queued {len(extraction_payloads)} extractions in log table")
            # Return mapping using external table name as key (guaranteed unique)
            return {r['external_table']: r['execution_id'] for r in records}
        except Exception as exc:
            logger.error(f"Failed to bulk insert log records after retries: {exc}")
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
        Update a log record in the extraction run log table.
        
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
        
        current_time = datetime.utcnow()
        
        updates = {
            "master_execution_id": master_execution_id,
            "execution_id": execution_id,
            "status": status,
            "error_messages": error,
            "end_timestamp": None,
            "end_timestamp_int": None
        }

        if status in {"Completed", "Failed", "Cancelled", "Deduped"}:
            updates["end_timestamp"] = current_time

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

        # Dynamically build schema based on update values
        schema = StructType([
            StructField(
                key,
                LongType() if isinstance(value, int)
                else DoubleType() if isinstance(value, float)
                else TimestampType() if isinstance(value, datetime)
                else StringType(),
                True,
            )
            for key, value in updates.items()
        ])

        def _exec_update() -> None:
            df = spark.createDataFrame([updates], schema=schema)

            if "end_timestamp" in updates and updates["end_timestamp"] is not None:
                df = (
                    df
                    .withColumn(
                        "end_timestamp_int",
                        F.date_format(F.col("end_timestamp"), "yyyyMMddHHmmssSSS")
                        .cast(LongType())
                    )
                )
                updates["end_timestamp_int"] = int(updates["end_timestamp"].strftime("%Y%m%d%H%M%S%f")[:-3])

            delta_table = DeltaTable.forPath(spark, self.log_table_uri)
            (
                delta_table.alias("t")
                .merge(
                    source=df.alias("u"),
                    condition="""
                        t.execution_id            = u.execution_id
                        and t.master_execution_id = u.master_execution_id
                    """
                )
                .whenMatchedUpdate(
                    set={k: f"u.{k}" for k in updates if k != "execution_id"},
                )
                .execute()
            )

        try:
            self._with_retry(_exec_update)
        except Exception as exc:
            logger.error(f"Failed to update log record after retries: {exc}")
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

        df = spark.read.format("delta").load(self.log_table_uri)
        
        df = df.filter(F.col("master_execution_id") == master_execution_id)
        df = df.filter(F.col("source_schema_name") == rec["source_schema"])
        df = df.filter(F.col("source_table_name") == rec["source_table"])
        
        # Handle nullable extract start/end dates
        if rec["extract_start"] is None:
            df = df.filter(F.col("extract_start_dt").isNull())
        else:
            df = df.filter(F.col("extract_start_dt") == rec["extract_start"])
            
        if rec["extract_end"] is None:
            df = df.filter(F.col("extract_end_dt").isNull())
        else:
            df = df.filter(F.col("extract_end_dt") == rec["extract_end"])
        
        # Check if any matching record exists
        return df.limit(1).count() > 0


# ### Pipeline Utils

# In[14]:


async def trigger_pipeline(
    workspace_id: str,
    pipeline_id: str,
    payload: Dict[str, Any],
    max_retries: int = 5,
    retry_delay: int = 2,  # initial seconds
    backoff_factor: float = 1.5  # exponential
) -> str:
    """
    Triggers a Fabric pipeline execution with error handling and retry logic.
    
    This function attempts to start a pipeline execution via the Fabric REST API, handling
    both transient and permanent errors with an exponential backoff retry strategy.
    
    Args:
        workspace_id: The ID of the Fabric workspace containing the pipeline
        pipeline_id: The ID of the pipeline to trigger
        payload: Dictionary containing the pipeline execution parameters
        max_retries: Maximum number of retry attempts for transient failures
        retry_delay: Initial delay in seconds before first retry
        backoff_factor: Multiplier for exponential backoff between retries
    
    Returns:
        str: The job ID of the successfully triggered pipeline
        
    Raises:
        RuntimeError: If the pipeline couldn't be triggered after all retries,
                     or if a permanent client error occurs (non-retryable)
    
    Note:
        - Uses exponential backoff with jitter for retries
        - HTTP 429, 408, and 5xx errors are considered transient and will be retried
        - Other client errors (4xx) are considered permanent after max_retries
    """

    client = fabric.FabricRestClient()
    trigger_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    
    for attempt in range(1, max_retries + 1):
        try:
            response = client.post(trigger_url, json=payload)
            
            # Success
            if response.status_code == 202:
                response_location = response.headers.get('Location', '')
                job_id = response_location.rstrip("/").split("/")[-1]    
                logger.info(f"Pipeline triggered successfully. Job ID: {job_id}")
                return job_id
            
            # Handle transient errors
            elif response.status_code >= 500 or response.status_code in [429, 408]:
                # Server errors (5xx), rate limiting (429), or timeout (408)
                logger.warning(f"Attempt {attempt}/{max_retries} - transient HTTP {response.status_code}: {response.text}")
            else:
                # Client errors (4xx) other than rate limits are likely permanent
                if attempt == max_retries:
                    logger.error(f"Client error HTTP {response.status_code}: {response.text}")
                    raise RuntimeError(f"Client error HTTP {response.status_code}: {response.text}")
                logger.warning(f"Attempt {attempt}/{max_retries} - client error HTTP {response.status_code}: {response.text}")
        
        except Exception as exc:
            # Re-raise on the last attempt
            if attempt == max_retries:
                logger.error(f"Exhausted retries - raising exception", exc_info=True)
                raise
            
            # Log the exception and continue
            error_msg = str(exc)
            is_network_error = "timeout" in error_msg.lower() or "connection" in error_msg.lower()
            error_type = "Network error" if is_network_error else "Unexpected error"
            logger.warning(f"Attempt {attempt}/{max_retries} - {error_type}: {error_msg}")
        
        # Apply backoff with jitter (don't sleep on the last attempt)
        if attempt < max_retries:
            sleep_time = retry_delay * (backoff_factor ** (attempt - 1))
            # Add jitter (±20%) to avoid thundering herd problem
            jitter = 0.8 + (0.4 * np.random.random())
            sleep_time = sleep_time * jitter

            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
            await asyncio.sleep(sleep_time)
    
    raise RuntimeError(f"Failed to trigger pipeline after {max_retries} attempts")


async def poll_job(job_url: str) -> str:
    """
    Continuously poll a Fabric pipeline job until it reaches a terminal state.
    
    This function monitors the execution status of a pipeline job by periodically 
    checking its state via the Fabric REST API. It implements several reliability 
    mechanisms including grace periods for newly created jobs, handling of transient 
    errors, and recognition of terminal states.
    
    Args:
        job_url: The URL of the job to poll, in format 
                 "v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
    
    Returns:
        str: The final state of the job ("Completed", "Failed", "Cancelled", or "Deduped")
    
    Raises:
        HTTPError: If the job URL is invalid or if persistent API errors occur
        
    Note:
        - Uses a grace period (POLL_INTERVAL) for jobs that might not be immediately visible
        - Handles transient HTTP errors (429, 408, 5xx) with automatic retries
        - Early non-terminal states like "Failed" during the grace period will be rechecked
        - Waits POLL_INTERVAL seconds between normal status checks and FAST_RETRY_SEC 
          seconds during the grace period
    """
    client = fabric.FabricRestClient()
    t0          = time.monotonic()
    grace_end   = t0 + POLL_INTERVAL

    while True:
        resp = client.get(job_url)

        # transient errors
        if resp.status_code in TRANSIENT_HTTP:
            logger.warning(f"Transient HTTP error {resp.status_code} when polling job, retrying...")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        # 404 before job appears
        if resp.status_code == 404:
            if time.monotonic() < grace_end:
                await asyncio.sleep(FAST_RETRY_SEC)
                continue
            logger.error(f"Job not found after grace period: {job_url}")
            resp.raise_for_status()

        resp.raise_for_status()
        state = (resp.json().get("status") or "Unknown").title()

        # early non-terminal states during grace window
        if state in {"Failed", "NotStarted", "Unknown"} and time.monotonic() < grace_end:
            logger.info(f"Job in early state {state}, retrying in grace period...")
            await asyncio.sleep(FAST_RETRY_SEC)
            continue

        # terminal states as per MS Fabric REST API documentation
        if state in {"Completed", "Failed", "Cancelled", "Deduped"}:
            logger.info(f"Job reached terminal state: {state}")
            return state

        # still executing, wait
        logger.debug(f"Job still running in state {state}, polling...")
        await asyncio.sleep(POLL_INTERVAL)


# ## Helper functions

# In[15]:


def _build_path_components(item: Dict) -> Dict[str, str]:
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
        f"exports/{load_tier}/" # TODO: remove v3 reference
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


# In[16]:


def _build_partition_clause(item: Dict) -> str:
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


# In[17]:


def _create_sql_script(
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
        sql_template: SQL template string
        schema_name: Source schema name
        table_name: Source table name
        external_table_name: Name for the external table
        partition_clause: SQL WHERE clause for partitioning
        location_path: Output path for the extracted data
    
    Returns:
        Complete SQL script
    """
    return (
        sql_template
        .replace("@TableSchema", schema_name)
        .replace("@TableName", table_name)
        .replace("@ExternalTableName", external_table_name)
        .replace("@PartitionClause", partition_clause)
        .replace("@LocationPath", location_path)
    )


# In[18]:


def _enrich_work_item(item: Dict, path_info: Dict, partition_clause: str, script: str) -> Dict:
    """
    Add computed fields to the work item for extraction processing.
    
    Args:
        item: Original work item dictionary
        path_info: Dictionary with path components
        partition_clause: Generated partition clause
        script: Generated SQL script
    
    Returns:
        Enriched work item with additional fields
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


# In[19]:


def prepare_extract_payloads(work_items: List[Dict], sql_template: str) -> List[Dict]:
    """
    Prepare the CETAS SQL statements and pipeline payloads for each extraction work item.
    
    This function transforms the basic work items into complete extraction payloads by:
    1. Generating appropriate output paths and naming conventions based on extraction mode
    2. Building SQL WHERE clauses for data filtering (partition clauses)
    3. Creating the complete CETAS SQL scripts by substituting values into the template
    4. Enriching each work item with additional metadata needed for execution
    
    The function coordinates several helper functions:
    - _build_path_components: Creates paths and table naming based on extraction mode
    - _build_partition_clause: Generates SQL filtering based on date parameters
    - _create_sql_script: Substitutes values into the SQL template
    - _enrich_work_item: Adds execution details to work items
    
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
        path_info = _build_path_components(item)
        
        # Build appropriate partition clause
        partition_clause = _build_partition_clause(item)
        
        # Create the SQL script by replacing placeholders in template
        script = _create_sql_script(
            sql_template, 
            item["source_schema"], 
            item["source_table"], 
            path_info["external_table_name"], 
            partition_clause, 
            path_info["full_path"]
        )
        
        # Add fields to the work item
        enriched_item = _enrich_work_item(item, path_info, partition_clause, script)
        extraction_payloads.append(enriched_item)
    
    logger.info(f"Successfully prepared {len(extraction_payloads)} extraction payloads")
    return extraction_payloads


# ## Initialise utilities

# In[20]:


lakehouse_utils = LakehouseUtils(WORKSPACE_ID, LAKEHOUSE_ID)
config_utils = ConfigUtils(lakehouse_utils)


# In[21]:


fabric_config = config_utils.get_configs_as_object(FABRIC_ENVIRONMENT)
if not fabric_config:
    error_msg = f"No configuration found for environment: {FABRIC_ENVIRONMENT}"
    logger.error(error_msg)
    raise ValueError(error_msg)
    
logger.info(f"Loaded configuration for environment: {FABRIC_ENVIRONMENT}")


# In[22]:


synapse_extract_objects_table_uri = f"{lakehouse_utils.lakehouse_tables_uri()}synapse_extract_objects"
synapse_extract_objects_df = DeltaTable.forPath(spark, synapse_extract_objects_table_uri).toDF().filter("active_yn = 'Y'")


# In[23]:


# Get data source and pipeline configuration
synapse_config_row = synapse_extract_objects_df.limit(1).collect()[0]
synapse_datasource_name = synapse_config_row["synapse_datasource_name"]
synapse_datasource_location = synapse_config_row["synapse_datasource_location"]
synapse_extract_pipeline_id = synapse_config_row["pipeline_id"]


# In[24]:


logger.info(f"Datasource: {synapse_datasource_name}")
logger.info(f"Datasource Location: {synapse_datasource_location}")
logger.info(f"Pipeline ID: {synapse_extract_pipeline_id}")


# In[25]:


synapse_extract_utils = SynapseExtractUtils(
    datasource_name=synapse_datasource_name,
    datasource_location=synapse_datasource_location,
    workspace_id=WORKSPACE_ID,
    lakehouse_id=LAKEHOUSE_ID
)


# ## Construct extract work items

# In[26]:


if WORK_ITEMS_JSON:
    # Historical extraction, driven by `synapse_extract_historical_driver`
    work_items: List[Dict] = json.loads(WORK_ITEMS_JSON)
    master_execution_id = MASTER_EXECUTION_ID or work_items[0]["master_execution_id"]
    mode = "Historical"

    logger.info(f"HISTORICAL EXTRACTION MODE - Master Execution ID: {master_execution_id}")
    logger.info(f"Tables to process: {len(work_items)}")
    
    # Add the mode to each work item if not already present
    for item in work_items:
        item["mode"] = "Historical"

        for field in ["extract_start", "extract_end", "single_date_filter", "date_range_filter", "partition_clause"]:
            if field in item and item[field] == "None":
                item[field] = None
    
    # Filter out snapshots if INCLUDE_SNAPSHOTS is False
    if not INCLUDE_SNAPSHOTS:
        original_count = len(work_items)
        work_items = [item for item in work_items if item["extract_mode"] != "snapshot"]
        filtered_count = original_count - len(work_items)
        if filtered_count > 0:
            logger.info(f"Excluded {filtered_count} snapshot tables based on INCLUDE_SNAPSHOTS=False")
else:
    # Daily extraction
    master_execution_id = MASTER_EXECUTION_ID or str(uuid.uuid4())

    logger.info(f"DAILY EXTRACTION MODE - Master Execution ID: {master_execution_id}")
    logger.info(f"Date Range: {DAILY_RUN_START_DATE} to {DAILY_RUN_END_DATE}")

    # Create extract work items for each active table in the objects table
    table_configs = synapse_extract_objects_df.collect()

    # Filter out snapshots if INCLUDE_SNAPSHOTS is False
    if not INCLUDE_SNAPSHOTS:
        original_count = table_configs.count()
        table_configs = table_configs.filter("extract_mode != 'snapshot'")
        filtered_count = original_count - table_configs.count()
        if filtered_count > 0:
            logger.info(f"Excluded {filtered_count} snapshot tables based on INCLUDE_SNAPSHOTS=False")
    
    # Create work items from the filtered table configs
    work_items = [
        dict(
            master_execution_id = master_execution_id,
            execution_group     = table_config["execution_group"],
            source_schema       = table_config["source_schema_name"],
            source_table        = table_config["source_table_name"],
            single_date_filter  = table_config["single_date_filter"] if "single_date_filter" in table_config else None,
            date_range_filter   = table_config["date_range_filter"] if "date_range_filter" in table_config else None,
            extract_mode        = table_config["extract_mode"],
            extract_start       = str(DAILY_RUN_START_DATE) if table_config["extract_mode"] == "incremental" else None,
            extract_end         = str(DAILY_RUN_END_DATE) if table_config["extract_mode"] == "incremental" else None,
            mode                = "Daily"
        )
        for table_config in table_configs
    ]
    mode = "Daily"


# In[27]:


# Get the SQL template from the SynapseExtractUtils
sql_template = synapse_extract_utils.get_extract_sql_template()

# Prepare extract payloads for all work items
extraction_payloads = prepare_extract_payloads(work_items, sql_template)

# Sort extraction payloads by execution_group to ensure they are processed in order
extraction_payloads.sort(key=lambda x: x.get('execution_group', float('inf')))
logger.info(f"Extraction payloads sorted by execution_group for ordered processing")


# ## Orchestrate extraction

# In[ ]:


CONCURRENCY_LIMITER = asyncio.Semaphore(MAX_CONCURRENCY)
async def process_extract(extract_item: Dict, execution_id_map: Dict[str, str]):
    """
    Process a single data extraction asynchronously with comprehensive error handling.
    
    This function handles the entire lifecycle of a table extraction:
    1. Acquires a concurrency slot via semaphore to limit parallel extractions
    2. Claims the extraction by updating its status in the log table
    3. Validates required configuration parameters
    4. Triggers the extraction pipeline with the prepared payload
    5. Updates the status to "Running" with pipeline job ID
    6. Polls the job until completion (Completed, Failed, Cancelled, or Deduped)
    7. Updates the final status with execution details
    
    The function uses exception handling at each step to ensure all errors are 
    logged and status updates are attempted even when operations fail.
    
    Args:
        extract_item: Dictionary containing extraction details with keys:
            - source_schema: Schema name of the source table
            - source_table: Name of the source table
            - master_execution_id: ID grouping related extractions
            - external_table: Full name of external table (for lookup in execution_id_map)
            - pipeline_payload: Payload for the pipeline execution
            - output_path: Path where extracted files will be stored
            - extract_file_name: Base filename for extraction
            - external_table_name: Name for the external table
        
        execution_id_map: Dictionary mapping external table names to execution IDs
                          that were pre-generated during the queueing process
    
    Returns:
        Tuple of (success: bool, table_info: str, status: str, error_message: Optional[str]):
            - success: True if extraction completed successfully
            - table_info: Formatted string of schema.table for logging
            - status: Final status of the extraction (Completed, Failed, etc.)
            - error_message: Error details if failed, None otherwise
    
    Raises:
        No exceptions are raised - all errors are caught and returned as part of the result tuple
    """
    table_info = f"{extract_item['source_schema']}.{extract_item['source_table']}"
    queue_start_time = time.monotonic()  # Time including queue wait

    async with CONCURRENCY_LIMITER:
        start_time = time.monotonic()  # Start timing the actual execution (after acquiring semaphore)

        # Get the pre-assigned execution_id for this table
        execution_id = execution_id_map[extract_item['external_table']]
        
        # Update status to "Claimed" to indicate processing has started
        try:
            synapse_extract_utils.update_log_record(
                status="Claimed",
                execution_id=execution_id,
                master_execution_id=extract_item['master_execution_id']
            )
        except Exception as status_exc:
            logger.error(f"Failed to update status for {table_info}: {status_exc}", exc_info=True)
            return (False, table_info, "Failed", f"Status update error: {str(status_exc)}")

        logger.info(f"Claimed table {table_info} for processing (execution_id: {execution_id})")

        try:
            # Validate workspace ID and pipeline ID
            if not fabric_config.config_workspace_id:
                error_msg = "config_workspace_id must be set"
                logger.error(error_msg)
                raise ValueError(error_msg)
            if not synapse_extract_pipeline_id:
                error_msg = "synapse_extract_pipeline_id must be set"
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.info(f"Processing {table_info} - Triggering pipeline...")
            
            # Trigger the pipeline with the prepared payload
            job_id = await trigger_pipeline(
                workspace_id=fabric_config.config_workspace_id,
                pipeline_id=synapse_extract_pipeline_id,
                payload=extract_item['pipeline_payload']
            )
            
            # Update extract status to Running with job ID
            try:
                synapse_extract_utils.update_log_record(
                    status="Running",
                    master_execution_id=extract_item['master_execution_id'],
                    execution_id=execution_id,
                    pipeline_job_id=job_id
                )
            except Exception as status_exc:
                logger.error(f"Failed to update status for {table_info}: {status_exc}", exc_info=True)
                return (False, table_info, "Failed", f"Status update error: {str(status_exc)}")

            logger.info(f"Running {table_info} - Job ID: {job_id}")

            # Poll for completion
            job_url = (f"v1/workspaces/{fabric_config.config_workspace_id}"
                      f"/items/{synapse_extract_pipeline_id}/jobs/instances/{job_id}")
            final_state = await poll_job(job_url)
            
            # Calculate duration in seconds (actual processing time)
            duration_sec = time.monotonic() - start_time

            # Update final status, log output_path if successful
            try:
                synapse_extract_utils.update_log_record(
                    status=final_state,
                    master_execution_id=extract_item['master_execution_id'],
                    execution_id=execution_id,
                    output_path=extract_item.get("output_path") if final_state in {"Completed", "Deduped"} else None,
                    extract_file_name=extract_item.get("extract_file_name") if final_state in {"Completed", "Deduped"} else None,
                    external_table=extract_item.get("external_table") if final_state in {"Completed", "Deduped"} else None,
                    duration_sec=duration_sec
                )
            except Exception as status_exc:
                logger.error(f"Failed to update status for {table_info}: {status_exc}", exc_info=True)
                return (False, table_info, "Failed", f"Status update error: {str(status_exc)}")

            status_icon = "✅" if final_state in {"Completed", "Deduped"} else "⚠️"

            if final_state in {"Completed", "Deduped"}:
                logger.info(f"{final_state} - {table_info} - Duration: {duration_sec:.2f}s")
            else:
                logger.warning(f"{final_state} - {table_info} - Duration: {duration_sec:.2f}s")
            
            # Return success if the final state is "Completed" or "Deduped"
            success = final_state in {"Completed", "Deduped"}
            return (success, table_info, final_state, None)
            
        except Exception as exc:
            duration_sec = time.monotonic() - start_time
            error_message = str(exc)
            try:
                synapse_extract_utils.update_log_record(
                    status="Failed",
                    master_execution_id=extract_item['master_execution_id'],
                    execution_id=execution_id,
                    error=error_message,
                    duration_sec=duration_sec,  # Add duration in seconds
                    output_path=None
                )
            except Exception as status_exc:
                logger.error(f"Failed to update status for {table_info}: {status_exc}", exc_info=True)
                return (False, table_info, "Failed", f"Status update error: {str(status_exc)}")
            logger.error(f"Error processing {table_info} - Duration: {duration_sec:.2f}s", exc_info=True)
            return (False, table_info, "Failed", error_message)


# In[ ]:


def run_async_orchestration():
    """
    Run the complete extraction orchestration process with proper asyncio handling.
    
    This function coordinates the entire extraction workflow:
    1. Initialises or reuses an asyncio event loop with nest_asyncio
    2. Pre-logs all planned extractions with 'Queued' status
    3. Groups extraction payloads by execution_group for ordered processing
    4. Processes each execution group sequentially, with parallelism within groups
    5. Compiles statistics on successful and failed extractions
    
    The function implements a two-level concurrency model:
    - Sequential processing between execution groups (dependencies)
    - Parallel processing within each group (up to MAX_CONCURRENCY)
    
    Execution groups allow for dependency management, where tables in higher
    numbered groups can depend on extractions in lower numbered groups.
    
    Returns:
        Dict: Extraction summary with the following keys:
            - master_execution_id: Unique ID for this batch of extractions
            - extraction_mode: "Daily" or "Historical"
            - total_tables: Total number of tables processed
            - successful_extractions: Number of successful extractions
            - failed_extractions: Number of failed extractions
            - skipped_extractions: Number of skipped extractions
            - success_rate: Percentage of successful extractions
            - execution_groups: List of execution groups that were processed
            - failed_details: Detailed information on failed extractions
            - completion_time: Timestamp when the process completed
    
    Notes:
        - Uses global variables: master_execution_id, extraction_payloads, 
          MAX_CONCURRENCY, MASTER_EXECUTION_PARAMETERS, TRIGGER_TYPE, and mode
        - Relies on synapse_extract_utils for database operations
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        nest_asyncio.apply()

    logger.info(f"Starting extraction process - Master Execution ID: {master_execution_id}")
    logger.info(f"Max Concurrency: {MAX_CONCURRENCY}")

    # Bulk insert all extractions as 'Queued' before processing
    execution_id_map = synapse_extract_utils.bulk_insert_queued_extracts(
        extraction_payloads=extraction_payloads, 
        master_execution_id=master_execution_id,
        master_execution_parameters=MASTER_EXECUTION_PARAMETERS,
        trigger_type=TRIGGER_TYPE
    )
    
    # Group extraction payloads by execution_group
    grouped_payloads = {}
    for payload in extraction_payloads:
        group = payload.get('execution_group', float('inf'))
        if group not in grouped_payloads:
            grouped_payloads[group] = []
        grouped_payloads[group].append(payload)
    
    # Get all unique execution groups and sort them
    execution_groups = sorted(grouped_payloads.keys())
    
    all_results = []
    
    # Process each execution group sequentially
    for group in execution_groups:
        group_payloads = grouped_payloads[group]
        
        logger.info(f"Processing execution group {group} ({len(group_payloads)} extractions)")

        # Run all tasks in this group with the configured concurrency
        group_results = loop.run_until_complete(
            asyncio.gather(*(process_extract(payload, execution_id_map) for payload in group_payloads), return_exceptions=False)
        )
        
        # Add results from this group to the overall results
        all_results.extend(group_results)
        
        # Show summary for this group
        group_success = sum(1 for result in group_results if result[0])
        group_failed = len(group_results) - group_success
        logger.info(f"Group {group} completed: {group_success} successful, {group_failed} failed")
    
    # Aggregate and summarise results from all groups
    total_extractions = len(all_results)
    successful_extractions = sum(1 for result in all_results if result[0])
    failed_extractions = total_extractions - successful_extractions
    skipped_extractions = sum(1 for result in all_results if result[2] == "Skipped")
    
    # Get details of each status type
    failed_details = [
        {"table": result[1], "status": result[2], "error": result[3]}
        for result in all_results if not result[0]
    ]
    
    # Prepare summary
    extraction_summary = {
        "master_execution_id": master_execution_id,
        "extraction_mode": mode,
        "total_tables": total_extractions,
        "successful_extractions": successful_extractions,
        "failed_extractions": failed_extractions,
        "skipped_extractions": skipped_extractions,
        "success_rate": f"{(successful_extractions/total_extractions)*100:.1f}%" if total_extractions > 0 else "N/A",
        "execution_groups": execution_groups,
        "failed_details": failed_details,
        "completion_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    return extraction_summary


# In[ ]:


logger.info("Starting extraction orchestration process")
summary = run_async_orchestration()
logger.info("Extraction orchestration process completed")


# In[ ]:


logger.info(f"EXTRACTION SUMMARY - Master Execution ID: {summary['master_execution_id']}")
logger.info(f"Mode: {summary['extraction_mode']} • Completed: {summary['completion_time']}")
logger.info(f"Total: {summary['total_tables']}, Succeeded: {summary['successful_extractions']}, "
            f"Failed: {summary['failed_extractions']}, Skipped: {summary['skipped_extractions']}")
logger.info(f"Success Rate: {summary['success_rate']}")

if summary['failed_extractions'] > 0:
    logger.warning(f"FAILED TASKS DETAILS: {summary['failed_extractions']} extractions failed")
    
    for i, task in enumerate(summary['failed_details'], 1):
        if task.get('error'):
            logger.error(f"Failed extraction {i}: {task['table']} - Status: {task['status']} - Error: {task['error']}")
        else:
            logger.error(f"Failed extraction {i}: {task['table']} - Status: {task['status']}")
    
    # If high failure rate, log warning
    if summary['failed_extractions'] / summary['total_tables'] > 0.25:

        logger.warning(f"HIGH FAILURE RATE DETECTED! Success rate: {summary['success_rate']}")
        logger.warning("Review failed tasks and consider using synapse_extract_retry_helper.ipynb for retries.")
else:
    logger.info("All extractions completed successfully!")

