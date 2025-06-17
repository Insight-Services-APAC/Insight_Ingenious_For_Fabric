# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a29c9d15-c24e-4779-8344-0c7c237b3990",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "50fbcab0-7d56-46f7-90f6-80ceb00ac86d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a29c9d15-c24e-4779-8344-0c7c237b3990"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Execute Spark SQL DDL

# MARKDOWN ********************

# ## Install libraries

# CELL ********************

# Install diagram generation and SVG dependencies for architecture diagram

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install diagrams==0.24.4 CairoSVG==2.8.2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import libraries

# CELL ********************

# MAGIC %%spark  
# MAGIC import com.microsoft.spark.fabric.tds.implicits.read.FabricSparkTDSImplicits._
# MAGIC import com.microsoft.spark.fabric.tds.implicits.write.FabricSparkTDSImplicits._
# MAGIC import com.microsoft.spark.fabric.Constants
# MAGIC import org.apache.spark.sql.SaveMode

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric # Fabric Spark integration
from com.microsoft.spark.fabric.Constants import Constants # Fabric env constants
import json
import notebookutils
import re
import uuid

from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    StringType
)

from typing import (
    List,
    Dict,
    Optional,
    Tuple
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric                                           # Fabric Spark integration
from com.microsoft.spark.fabric.Constants import Constants                  # Fabric env constants
import json                                                                 # JSON parsing
import logging
import notebookutils                                                        # Fabric notebook FS utils
import re                                                                   # Regex for DDL parsing
import uuid                                                                 # Unique run identifiers
from datetime import datetime                                              # Timestamps for logging
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType  # Log schema
from typing import List, Dict, Optional, Tuple                              # Type hints


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define parameters

# CELL ********************

DDL_SCRIPTS_BASE_DIR = "Files/ddl_scripts"
DDL_SCRIPTS_SUBDIR = "metcash_metadata_extract_20250611"

LH_DDL_DIR = f"{DDL_SCRIPTS_BASE_DIR}/LH/{DDL_SCRIPTS_SUBDIR}/"
WH_DDL_DIR = f"{DDL_SCRIPTS_BASE_DIR}/WH/{DDL_SCRIPTS_SUBDIR}/"


DROP_BEFORE_CREATE = True

FABRIC_LAKEHOUSE = "LH"
FABRIC_WAREHOUSE = "WH"
FABRIC_WAREHOUSE_SCHEMA = None

LOG_TABLE = f"{FABRIC_WAREHOUSE}.log.ddl_execution_log"
DEBUG = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base OneLake path for all DDL scripts
DDL_SCRIPTS_BASE_DIR = "Files/ddl_scripts"

# Subfolder for this metadata extract (update per run)
DDL_SCRIPTS_SUBDIR = "metcash_metadata_extract_20250611"

# Path for Lakehouse DDL files
LH_DDL_DIR = f"{DDL_SCRIPTS_BASE_DIR}/LH/{DDL_SCRIPTS_SUBDIR}/"

# Path for Warehouse DDL files
WH_DDL_DIR = f"{DDL_SCRIPTS_BASE_DIR}/WH/{DDL_SCRIPTS_SUBDIR}/"

# Whether to DROP objects before CREATE
DROP_BEFORE_CREATE = True

# Identifiers for Fabric environments
FABRIC_LAKEHOUSE = "LH"
FABRIC_WAREHOUSE = "WH"
FABRIC_WAREHOUSE_SCHEMA = None  # Defaults to primary schema

# Target log table in Warehouse
LOG_TABLE = f"{FABRIC_WAREHOUSE}.log.ddl_execution_log"

# Debug flag for diagram rendering
DEBUG = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate diagram

# CELL ********************

if DEBUG == True:
    import cairosvg
    from IPython.display import (
        display,
        Image
    )
    from diagrams import Diagram, Edge, Cluster
    from diagrams.azure.analytics import SynapseAnalytics
    from diagrams.azure.storage import DataLakeStorage
    from diagrams.azure.compute import FunctionApps
    from diagrams.azure.database import SQLDatabases
    from diagrams.custom import Custom

    class FabricDiagramGenerator:

        def __init__(self, diagram_title: str, filename: str = "architecture_diagram"):
            self.diagram_title = diagram_title
            self.filename = filename
            self.fabric_icons_base_url = "https://raw.githubusercontent.com/FabricTools/fabric-icons/refs/heads/main/node_modules/%40fabric-msft/svg-icons/dist/svg"
            self.downloaded_icons = {}
            
        def download_fabric_icons(self, icon_names: List[str], size: int = 64) -> Dict[str, str]:
            for icon_name in icon_names:
                icon_svg = f"{icon_name}_{size}_item.svg"
                url = f"{self.fabric_icons_base_url}/{icon_svg}"
                icon_png = f"{icon_name}_{size}_item.png"
                
                try:
                    cairosvg.svg2png(url=url, write_to=icon_png, dpi=1000)
                    self.downloaded_icons[icon_name] = icon_png
                except Exception as e:
                    print(f"Could not download {icon_name} icon: {e}")
                    
            return self.downloaded_icons
        
        def create_diagram(self, 
                        clusters: Dict[str, Dict[str, any]], 
                        connections: List[Tuple[str, str, str]],
                        show: bool = False,
                        outformat: str = "png"):

            with Diagram(self.diagram_title, show=show, outformat=outformat, filename=self.filename):
                components = {}
                
                # create clusters recursively
                def create_cluster_components(cluster_data, parent_cluster=None):
                    for cluster_name, cluster_info in cluster_data.items():
                        if parent_cluster:
                            # create nested cluster
                            with Cluster(cluster_name):
                                add_components(cluster_info)
                        else:
                            # create top-level cluster
                            with Cluster(cluster_name):
                                add_components(cluster_info)
                
                def add_components(cluster_info):
                    # add components to current cluster
                    if "components" in cluster_info:
                        for comp_id, (display_name, icon_type, icon_file) in cluster_info["components"].items():
                            if icon_type == "custom":
                                components[comp_id] = Custom(display_name, icon_file)
                            elif icon_type == "synapse":
                                components[comp_id] = SynapseAnalytics(display_name)
                            elif icon_type == "storage":
                                components[comp_id] = DataLakeStorage(display_name)
                            elif icon_type == "function":
                                components[comp_id] = FunctionApps(display_name)
                            elif icon_type == "sql":
                                components[comp_id] = SQLDatabases(display_name)
                            # Add more icon types as needed
                    
                    # handle subclusters
                    if "subclusters" in cluster_info:
                        create_cluster_components(cluster_info["subclusters"], parent_cluster=True)
                
                # create all clusters and components
                create_cluster_components(clusters)
                
                # create connections
                for source_id, target_id, label in connections:
                    if source_id in components and target_id in components:
                        components[source_id] >> Edge(label=label) >> components[target_id]

    def generate_fabric_architecture_diagram(
        title: str,
        fabric_icons: List[str],
        clusters_config: Dict[str, Dict[str, any]],
        connections_config: List[Tuple[str, str, str]],
        filename: str = "architecture"
    ):
        generator = FabricDiagramGenerator(title, filename)
        
        generator.download_fabric_icons(fabric_icons)
        
        generator.create_diagram(clusters_config, connections_config)
        
        return f"{filename}.png"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DEBUG == True:
    fabric_icons = ["notebook", "lakehouse", "data_warehouse", "pipeline"]

    clusters = {
        "Fabric": {
            "components": {
                "notebook": ("execute_ddl_scripts", "custom", "notebook_64_item.png")
            },
            "subclusters": {
                "OneLake": {
                    "components": {
                        "ddl_input": ("ddl_scripts/", "custom", "lakehouse_64_item.png")
                    }
                },
                "Lakehouse": {
                    "components": {
                        "lakehouse_sql": ("Lakehouse SQL Endpoint", "custom", "lakehouse_64_item.png")
                    }
                },
                "Warehouse": {
                    "components": {
                        "pipeline": ("DDL_Exec_Pipeline", "custom", "pipeline_64_item.png"),
                        "warehouse": ("Fabric_SQL_Warehouse", "custom", "data_warehouse_64_item.png")
                    }
                }
            }
        }
    }

    connections = [
        ("ddl_input",   "notebook", "(1) Load DDL scripts from Lakehouse"),
        ("notebook",    "lakehouse_sql", "(2) Execute DDL in Lakehouse"),
        ("notebook",    "pipeline", "(3) Trigger Fabric pipeline for Warehouse execution"),
        ("pipeline",    "warehouse", "(4) Pipeline runs DDL in Warehouse")
    ]

    display(
        Image(
            generate_fabric_architecture_diagram(
                title="Execute DDL Scripts",
                fabric_icons=fabric_icons,
                clusters_config=clusters,
                connections_config=connections,
                filename="execute_ddl_scripts_arch_diagram"
            )
        )
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configure logging

# CELL ********************

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema for ddl_execution_log entries
_LOG_SCHEMA = StructType([
    StructField("notebook_execution_id", StringType(), nullable = False),
    StructField("notebook_start_time", TimestampType(), nullable=False),
    StructField("notebook_end_time", TimestampType(), nullable=True),
    StructField("notebook_name", StringType(), nullable=True),
    StructField("ddl_script_execution_id", StringType(), nullable=False),
    StructField("ddl_script_order", IntegerType(), nullable=True),
    StructField("ddl_file_name", StringType(), nullable=True),
    StructField("ddl_script_start_time", TimestampType(), nullable=True),
    StructField("ddl_script_end_time", TimestampType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("pipeline_status", StringType(), nullable=True),
    StructField("error_message", StringType(), nullable=True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def insert_ddl_execution_log(
    notebook_execution_id: str,
    notebook_start_time: datetime,
    notebook_name: str,
    ddl_script_execution_id: str,
    ddl_script_order: int,
    status: str,
    notebook_end_time: Optional[datetime] = None,
    ddl_file_name: Optional[str] = None,
    ddl_script_start_time: Optional[datetime] = None,
    ddl_script_end_time: Optional[datetime] = None,
    pipeline_status: Optional[int] = None,
    error_message: Optional[str] = None
) -> datetime:

    df = spark.createDataFrame(
        [
            (
                notebook_execution_id,
                notebook_start_time,
                notebook_end_time,
                notebook_name,
                ddl_script_execution_id,
                ddl_script_order,
                ddl_file_name,
                ddl_script_start_time,
                ddl_script_end_time,
                status,
                pipeline_status,
                error_message
            )
        ],
        schema=_LOG_SCHEMA
    )

    df.write \
      .mode("append") \
      .synapsesql(f"{FABRIC_WAREHOUSE}.log.ddl_execution_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run notebook `execute_sql_fabric_warehouse`

# CELL ********************

%run execute_sql_fabric_warehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define functions

# MARKDOWN ********************

# ### list_sql_files

# CELL ********************

def generate_sql_file_list(root_dir: str):
    """Recursively list all .sql files under root_dir."""
    files = []
    for object in notebookutils.fs.ls(root_dir):
        if object.isFile and object.name.lower().endswith(".sql"):
            files.append(object)
        elif object.isDir:
            files.extend(generate_sql_file_list(object.path))
    return files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### load_ddl_from_onelake

# CELL ********************

def load_ddl_from_onelake(file_path: str) -> str:
    try:
        df = spark.read.text(file_path)

        if df.count() == 0:
            raise ValueError(f"Empty file: {file_path}")

        lines = [row.value for row in df.collect()]
        parsed_lines = [line for line in lines if not line.strip().startswith("--")] # remove lines that are explicitly commented out
        
        return "\n".join(parsed_lines)
    
    except Exception as e:
        logger.error(f"Error loading DDL from {file_path}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### validate_ddl

# CELL ********************

def validate_ddl(
    ddl: str,
    file_name: str
) -> Tuple[bool, str]:

    upper_ddl = ddl.upper()

    # confirm valid Fabric SQL operations, keep just `create` for now
    valid_operations = [
        "CREATE",
    ]

    # check if DDL contains any valid operations
    if not any(operation in upper_ddl for operation in valid_operations):
        return False, f"No valid DDL operation found in {file_name}"

    # basic syntax validation
    if ddl.count("(") != ddl.count(")"):
        return False, "Unbalanced number of parentheses"

    if ddl.count("'") % 2 != 0:
        return False, "Unbalanced number of quotes"

    return True, ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### extract_table_name_regex

# CELL ********************

def extract_table_name_regex(ddl: str) -> Optional[str]:
    """
    Extract table name from DDL, including optional schema prefix.
    
    Returns:
      - "[schema].[table]" (with brackets/backticks preserved) when schema-qualified,
      - "table" (no brackets/backticks) when unqualified,
      - None if no CREATE TABLE is found.
    """
    pattern = re.compile(
        r"(?i)CREATE\s+TABLE\s+"
        r"(?P<qualified>"
        r"(?:`[^`]+`|\[[^\]]+\]|[A-Za-z0-9_]+)"               # schema or table identifier
        r"(?:\s*\.\s*(?:`[^`]+`|\[[^\]]+\]|[A-Za-z0-9_]+))?"  # optional .table
        r")"
    )
    
    match = pattern.search(ddl)
    if not match:
        return None
    
    full_name = match.group('qualified').strip()
    
    # if there's a dot, it's schema-qualified; normalise spacing but keep delimiters
    if '.' in full_name:
        left, right = [part.strip() for part in full_name.split('.', 1)]
        return f"{left}.{right}"
    
    # otherwise strip any enclosing backticks or brackets
    return full_name.strip('`[]')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configure logging variables

# CELL ********************

notebook_execution_id = str(uuid.uuid4())
run_start = datetime.now()
notebook_name = notebookutils.runtime.context['currentNotebookName']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Get Lakehouse DDL files 

# CELL ********************

ddl_files_lh = generate_sql_file_list(LH_DDL_DIR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{len(ddl_files_lh)} Lakehouse DDL files found in {LH_DDL_DIR}:")
for f in ddl_files_lh:
    if "/table/" in f.path: print(f"Table: {f.name}")
    if "/view/" in f.path: print(f"View: {f.name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Get Warehouse DDL files

# CELL ********************

ddl_files_wh = generate_sql_file_list(WH_DDL_DIR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{len(ddl_files_wh)} Warehouse DDL files found in {WH_DDL_DIR}:")
for f in ddl_files_wh:
    if "/table/" in f.path: print(f"Table: {f.name}")
    if "/view/" in f.path: print(f"View: {f.name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Lakehouse DDLs

# CELL ********************

for index, file in enumerate(ddl_files_lh, start=1):
    file_path = file.path
    file_name = file.name

    ddl_script_execution_id = str(uuid.uuid4())

    insert_ddl_execution_log(
        notebook_execution_id = notebook_execution_id,
        notebook_start_time = run_start,
        notebook_end_time = None,
        notebook_name = notebook_name,
        ddl_script_execution_id = ddl_script_execution_id,
        ddl_script_order = index,
        status = "In Progress",
        ddl_file_name = file_name
    )

    print(f"[LH] Executing: {file_name}")

    try:
        ddl = load_ddl_from_onelake(file_path)
        is_valid, error_message = validate_ddl(ddl, file_name)
        if not is_valid:
            print(f"[LH][Error] Validation failed for {file_name}: {error_message}")

            insert_ddl_execution_log(
                notebook_execution_id = notebook_execution_id,
                notebook_start_time = run_start,
                notebook_name = notebook_name,
                ddl_script_execution_id = ddl_script_execution_id,
                ddl_script_order = index,
                status = "Failed",
                ddl_file_name = file_name,
                error_message = error_message
            )

            continue

        table_name = extract_table_name_regex(ddl)
        if DROP_BEFORE_CREATE and table_name:
            print(f"[LH] Dropping existing table {table_name} ...")
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            print(f"[LH] Dropped {table_name}")

        ddl_script_start_time = datetime.now()

        spark.sql(ddl)

        ddl_script_end_time = datetime.now()
        
        print(f"[LH] Success: {file_name}\n")

        insert_ddl_execution_log(
            notebook_execution_id = notebook_execution_id,
            notebook_start_time = run_start,
            notebook_name = notebook_name,
            ddl_script_execution_id = ddl_script_execution_id,
            ddl_script_order = index,
            status = "Succeeded",
            ddl_file_name = file_name,
            ddl_script_start_time = ddl_script_start_time,
            ddl_script_end_time = ddl_script_end_time,
            notebook_end_time = datetime.now() if index == len(ddl_files_lh) else None
        )

    except Exception as e:
        logger.error(f"[Lakehouse] Failed in {file_name}: {str(e)}")

        insert_ddl_execution_log(
            notebook_execution_id = notebook_execution_id,
            notebook_start_time = run_start,
            notebook_name = notebook_name,
            ddl_script_execution_id = ddl_script_execution_id,
            ddl_script_order = index,
            status = "Failed",
            ddl_file_name = file_name,
            error_message = str(e),
            notebook_end_time = datetime.now() if index == len(ddl_files_lh) else None
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Warehouse DDLs (via pipeline)

# CELL ********************

for index, file in enumerate(ddl_files_wh, start=1):
    file_path = file.path
    file_name = file.name

    ddl_script_execution_id = str(uuid.uuid4())

    insert_ddl_execution_log(
        notebook_execution_id = notebook_execution_id,
        notebook_start_time = run_start,
        notebook_name = notebook_name,
        ddl_script_execution_id = ddl_script_execution_id,
        ddl_script_order = index,
        status = "In Progress",
        ddl_file_name = file_name
    )

    print(f"[WH] Executing: {file_name}")

    try:
        ddl = load_ddl_from_onelake(file_path=file_path)
        is_valid, error_message = validate_ddl(ddl, file_name)
        if not is_valid:
            print(f"[WH][Error] Validation failed for {file_name}: {error_message}")

            insert_ddl_execution_log(
                notebook_execution_id = notebook_execution_id,
                notebook_start_time = run_start,
                notebook_name = notebook_name,
                ddl_script_execution_id = ddl_script_execution_id,
                ddl_script_order = index,
                status = "Failed",
                ddl_file_name = file_name,
                ddl_script_start_time = None,
                ddl_script_end_time = None,
                pipeline_status = None,
                error_message = error_message
            )

            continue

        table_name = extract_table_name_regex(ddl)
        if DROP_BEFORE_CREATE and table_name:
            print(f"[WH] Added `DROP TABLE IF EXISTS` statement to DDL for table: {table_name}")
            ddl = f"DROP TABLE IF EXISTS {table_name};\n" + ddl

        ddl_script_start_time = datetime.now()

        pipeline_status, exit_message = await run_pipeline_with_sql(script_content=ddl)

        if pipeline_status == "Failed":
            # log error, continue
            insert_ddl_execution_log(
                notebook_execution_id = notebook_execution_id,
                notebook_start_time = run_start,
                notebook_name = notebook_name,
                ddl_script_execution_id = ddl_script_execution_id,
                ddl_script_order = index,
                status = "Failed",
                ddl_file_name = file_name,
                ddl_script_start_time = ddl_script_start_time,
                ddl_script_end_time = datetime.now(),
                pipeline_status = pipeline_status,
                error_message = exit_message,
                notebook_end_time = datetime.now() if index == len(ddl_files_lh) else None
            )
            continue

        ddl_script_end_time = datetime.now()
        
        print(f"[WH] Success: {file_name} (Status: {pipeline_status})\n" + '=' * 60)

        insert_ddl_execution_log(
            notebook_execution_id = notebook_execution_id,
            notebook_start_time = run_start,
            notebook_name = notebook_name,
            ddl_script_execution_id = ddl_script_execution_id,
            ddl_script_order = index,
            status = "Succeeded",
            ddl_file_name = file_name,
            ddl_script_start_time = ddl_script_start_time,
            ddl_script_end_time = ddl_script_end_time,
            pipeline_status = pipeline_status,
            error_message = None,
            notebook_end_time = datetime.now() if index == len(ddl_files_lh) else None
        )
    
    except Exception as e:
        logger.error(f"[Warehouse] Failed in {file_name}: {str(e)}")

        insert_ddl_execution_log(
            notebook_execution_id = notebook_execution_id,
            notebook_start_time = run_start,
            notebook_name = notebook_name,
            ddl_script_execution_id = ddl_script_execution_id,
            ddl_script_order = index,
            status = "Failed",
            ddl_file_name = file_name,
            ddl_script_start_time = None,
            ddl_script_end_time = None,
            pipeline_status = None,
            error_message = str(e),
            notebook_end_time = datetime.now() if index == len(ddl_files_lh) else None
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
