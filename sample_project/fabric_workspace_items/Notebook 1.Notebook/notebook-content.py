# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "23ba8378-7884-4ac7-bab5-10e11af43d6c",
# META       "default_lakehouse_name": "config",
# META       "default_lakehouse_workspace_id": "50fbcab0-7d56-46f7-90f6-80ceb00ac86d",
# META       "known_lakehouses": [
# META         {
# META           "id": "23ba8378-7884-4ac7-bab5-10e11af43d6c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType, LongType
schema = StructType(
    [
        StructField("execution_id", StringType(), nullable=False),
        StructField("cfg_target_lakehouse_workspace_prefix", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("error_messages", StringType(), nullable=True),
        StructField("start_date", LongType(), nullable=False),
        StructField("finish_date", LongType(), nullable=False),
        StructField("update_date", LongType(), nullable=False)
    ]
)
empty_df = spark.createDataFrame([], schema)
(
    empty_df.write.format("delta")
    .option("parquet.vorder.default", "true")
    .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
    .save(
        f"abfss://Metcash_Test@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Tables/log_parquet_loads"  # noqa: E501
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
sys.path.append("/Workspace/Shared/libs")

from config import get_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install diagrams
%pip install cairosvg

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from deltalake import DeltaTable, write_deltalake
table_path = 'abfss://Metcash_Test@onelake.dfs.fabric.microsoft.com/LH.Lakehouse/Tables/bkp_fct_food_sales_allowance_daily_v' 
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}
dt = DeltaTable(table_path, storage_options=storage_options)
limited_data = dt.to_pyarrow_dataset().head(1000).to_pandas()
display(limited_data)

# Write data frame to Lakehouse
# write_deltalake(table_path, limited_data, mode='overwrite')

# If the table is too large and might cause an Out of Memory (OOM) error,
# you can try using the code below. However, please note that delta_scan with default lakehouse is currently in preview.
# import duckdb
# display(duckdb.sql("select * from delta_scan('/lakehouse/default/Tables/dbo/bigdeltatable') limit 1000 ").df())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, base64
from diagrams import Diagram, Edge, Node, Cluster
from diagrams.azure.analytics import SynapseAnalytics
from diagrams.azure.storage import DataLakeStorage
from diagrams.onprem.compute import Server
from diagrams.generic.storage import Storage
from urllib.request import urlretrieve
from diagrams.custom import Custom
import cairosvg

fabric_icons = ["notebook","data_warehouse","links", "lakehouse", "pipeline"]

page = "https://raw.githubusercontent.com/FabricTools/fabric-icons/refs/heads/main/node_modules/%40fabric-msft/svg-icons/dist/svg"
for f in fabric_icons:
  icon_svg = f"{f}_64_item.svg"
  url = f"{page}/{icon_svg}"
  icon_png = f"{f}_64_item.png"
  cairosvg.svg2png(url=url,write_to=icon_png, dpi=1000)
  


with Diagram("Extraction From Synapse - Tables or Views", show=False, outformat="png", filename="arch_notebook"):
    
    
    with Cluster("Azure"):
      synapse = SynapseAnalytics("Synapse")
      adls = DataLakeStorage("ADLS Storage")      

    with Cluster("Fabric"):               
        config_wh = Custom("Configuration WH", "data_warehouse_64_item.png")        
        extract_notebook = Custom("extract_from_synapse", "notebook_64_item.png")

        with Cluster("Azure Connectivity"):
          extract_svc = Custom("extract_from_synapse", "pipeline_64_item.png")
    
    
    extract_notebook >> Edge(label="(2) Trigger Execution Per Table / View : Passing in CETAS Script")  >> extract_svc     
    extract_svc >> Edge(label="(3) Execute CETAS") >> synapse    
    extract_notebook << Edge(label="(1) Read Config") << config_wh  
    config_wh << Edge(label="(2) Write Logs") << extract_notebook
    synapse >> Edge(label="(2) Write Parquet") >> adls
    

from IPython.display import Image
Image("arch_notebook.png")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


with Diagram("Fabric ↔ Synapse ↔ ADLS ↔ EDW", show=False, outformat="png", filename="arch_notebook"):
    azure   = Node("Azure")
    
    with Cluster("Azure"):
      synapse = SynapseAnalytics("Synapse")
      adls = DataLakeStorage("ADLS Storage")
      synapse >> adls


    with Cluster("Fabric"):
        
        edw_lh = Custom("EDW LH", "lakehouse_64_item.png")
        edw_wh = Custom("EDW WH", "data_warehouse_64_item.png")
        load_lh = Custom("load_to_lh_tables", "notebook_64_item.png")
        load_wh = Custom("load_to_wh_tables", "notebook_64_item.png")
        config_wh = Custom("Configuration WH", "data_warehouse_64_item.png")        
        extract_notebook = Custom("extract_from_synapse", "notebook_64_item.png")

        with Cluster("Azure Connectivity"):
          extract_svc = Custom("extract_from_synapse", "pipeline_64_item.png")
          shortcuts = Custom("Shortcuts", "links_64_item.png")

    
    
    cetas       = Node("CETAS")
    extract_notebook >> extract_svc >> synapse
    extract_notebook >> config_wh
    extract_notebook << config_wh
    
    adls >> shortcuts
    
    shortcuts >> edw_lh
    shortcuts >> edw_wh

    
    edw_lh << Edge() >> load_lh
    edw_wh << Edge() >> load_wh
   
    load_lh >> config_wh
    load_lh << config_wh
    load_wh >> config_wh
    load_wh << config_wh

    config_wh >> extract_svc
    config_wh << extract_svc

from IPython.display import Image
Image("arch_notebook.png")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
