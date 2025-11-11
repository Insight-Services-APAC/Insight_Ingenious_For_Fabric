# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# [comment]: # (Attach Default Lakehouse Markdown Cell)
# # 📌 Attach Default Lakehouse
# ❗**Note the code in the cell that follows is required to programatically attach the lakehouse and enable the running of spark.sql(). If this cell fails simply restart your session as this cell MUST be the first command executed on session start.**

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "lh_bronze",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📦 Pip
# Pip installs reqired specifically for this template should occur here

# CELL ********************

import importlib

jsonpickle_loader = importlib.find_loader('jsonpickle')
if jsonpickle_loader is None:
    print("Install jsonpickle")
    !pip install jsonpickle
else:
    print("jsonpickle Already Installed")

tabulate_loader = importlib.find_loader('tabulate')
if tabulate_loader is None:
    print("Install tabulate")
    !pip install tabulate
else:
    print("tabulate Already Installed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔗 Imports

# CELL ********************

from notebookutils import mssparkutils # type: ignore
from dataclasses import dataclass
import jsonpickle # type: ignore
import pandas as pd # type: ignore
from tabulate import tabulate # type: ignore
import json
from pyspark.sql.functions import * # type: ignore
import os
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🌐 Global Variables

# CELL ********************

gv_lakehouse = 'lh_bronze'
gv_log_lakehouse = 'lh_log'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# CELL ********************

@dataclass
class NotebookResult:    
    notebook: str
    start_time: float
    status: str
    error: str
    execution_time: float
    run_order: int
    
@dataclass
class FileListing:
    """Class for Files - Attributes: name, directory"""
    name: str
    directory: str

def get_file_content_using_notebookutils(file):
    """Get the content of a file using notebookutils."""
    #return self.mssparkutils.fs.head(file, 1000000000)
    data = spark.sparkContext.wholeTextFiles(file).collect() # type: ignore

    # data is a list of tuples, where the first element is the file path and the second element is the content of the file
    file_content = data[0][1]

    return file_content

def remove_file_using_notebookutils(file):
    """Remove a file using notebookutils."""
    try:
        mssparkutils.fs.rm(file, True)
    except:
        pass


def create_path_using_notebookutils(path):
    """Create a path using notebookutils."""
    mssparkutils.fs.mkdirs(path)

def walk_directory_using_notebookutils(path):
    """Walk a directory using notebookutils."""
    # List the files in the directory
    files = mssparkutils.fs.ls(path)

    # Initialize the list of all files
    all_files = []

    # Iterate over the files
    for file in files:
        # If the file is a directory, recursively walk the directory
        if file.isDir:
            all_files.extend(
                walk_directory_using_notebookutils(file.path))
        else:
            # If the file is not a directory, add it to the list of all files
            directory = os.path.dirname(file.path)
            name = file.name
            all_files.append(FileListing(
                name=name, directory=directory))

    return all_files

def call_child_notebook(notebook, batch_id, master_notebook):
        mssparkutils.notebook.run(notebook, 1800,{"pm_batch_id": batch_id, "pm_master_notebook": master_notebook}) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔒 Embed HASH information 

# CELL ********************

# First make sure that current hash info is the latest for the environment
embedded_hashcheck = 0 # type: ignore
if embedded_hashcheck == 0:
    print('Metadata Hash Check Bypassed')
else:
    mssparkutils.notebook.run("metadata_dbt_project_extract", 1800)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

embedded_hashes = {} # type: ignore
RelativePathForMetaData = "Files/MetaExtracts/"

def get_hash(file, hashes):
    ret = ""
    for h in hashes:
        if(h['file'] == file):
            return h['hash']
    return ret

##Hashcheck: BYPASS = 0, WARNING = 1, ERROR = 2
if embedded_hashcheck == 0:
    print('Metadata Hash Check Bypassed')
else:
    current_hashes = json.loads(get_file_content_using_notebookutils(RelativePathForMetaData + 'MetaHashes.json'))
    if current_hashes != embedded_hashes:
        for h in embedded_hashes:
            print(
                    h['file'] + '\n \t Emb Hash: ' + get_hash(h['file'], embedded_hashes) + '\n \t Env Hash: ' + get_hash(h['file'], current_hashes)
            )
        if embedded_hashcheck==1:
            print('Warning!: Hashes do not match. Its recommended to re-generate the dbt project using the latest extract of the target environment metadata.')
        else:
            raise Exception('ERROR, Hashes do not match. Its recommended to re-generate the dbt project using the latest extract of the target environment metadata.')
    else:
        print('Metadata Hashes Match 😏')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🗄️ Prepare Logging

# MARKDOWN ********************

# ## Create or Alter Tables

# CELL ********************

sql = f'''
CREATE TABLE IF NOT EXISTS {gv_log_lakehouse}.execution_log (
  notebook STRING,
  start_time DOUBLE,
  status STRING,
  error STRING,
  execution_time DOUBLE,
  run_order INT,
  batch_id string,
  master_notebook STRING  
)
USING DELTA
'''

spark.sql(sql) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql = f'''
CREATE TABLE IF NOT EXISTS {gv_log_lakehouse}.batch (
  batch_id STRING,
  start_time LONG,
  status STRING,
  master_notebook STRING
)
USING DELTA
'''

spark.sql(sql) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the master_notebook column exists in the batch table
schema_check_sql = f"DESCRIBE {gv_log_lakehouse}.execution_log"
schema_check_df = spark.sql(schema_check_sql) # type: ignore

# Check if the master_notebook column exists in the schema
if 'master_notebook' not in [row['col_name'] for row in schema_check_df.collect()]:
    # Add the master_notebook column to the table
    alter_table_sql = f'''
    ALTER TABLE {gv_log_lakehouse}.execution_log
    ADD COLUMN master_notebook STRING
    '''
    spark.sql(alter_table_sql) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the master_notebook column exists in the batch table
schema_check_sql = f"DESCRIBE {gv_log_lakehouse}.batch"
schema_check_df = spark.sql(schema_check_sql) # type: ignore

# Check if the master_notebook column exists in the schema
if 'master_notebook' not in [row['col_name'] for row in schema_check_df.collect()]:
    # Add the master_notebook column to the table
    alter_table_sql = f'''
    ALTER TABLE {gv_log_lakehouse}.batch
    ADD COLUMN master_notebook STRING
    '''
    spark.sql(alter_table_sql) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Log Related SQL Functions 

# CELL ********************



def close_batch(batch_id, master_notebook, status):
    sql = f'''
    UPDATE {gv_log_lakehouse}.batch
    SET status = '{status}'
    WHERE batch_id = '{str(batch_id)}' 
    AND master_notebook = '{str(master_notebook)}' '''

    spark.sql(sql) # type: ignore

def get_open_batch(master_notebook):
    sql = f'''
    SELECT MAX(batch_id) AS LatestBatchID FROM {gv_log_lakehouse}.batch WHERE status = 'open' AND master_notebook = '{str(master_notebook)}'
    '''

    return spark.sql(sql).collect()[0]['LatestBatchID'] # type: ignore

def insert_new_batch(batch_id, master_notebook):
    sql = f'''
    INSERT INTO {gv_log_lakehouse}.batch
    SELECT '{batch_id}' AS batch_id, UNIX_TIMESTAMP() AS start_time, 'open' AS status, '{str(master_notebook)}' AS master_notebook
    '''

    spark.sql(sql) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Insert a New Batch

# CELL ********************

new_batch_id = str(uuid.uuid4())
master_notebook = mssparkutils.runtime.context.get('currentNotebookName')
insert_new_batch(new_batch_id, master_notebook) # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Executions for Each Run Order Below:

# MARKDOWN ********************

# ## Run Order 0

# CELL ********************

call_child_notebook("master_dbt_project_notebook_0", new_batch_id, master_notebook)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run Order 1

# CELL ********************

call_child_notebook("master_dbt_project_notebook_1", new_batch_id, master_notebook)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📜 Execution Report

# CELL ********************

# Read the log for this batch execution
df_execution_log = spark.sql(f"SELECT * FROM {gv_log_lakehouse}.execution_log WHERE batch_id = '{new_batch_id}' AND master_notebook = '{master_notebook}'") # type: ignore
# Check if any have not succeeded
failed_results = df_execution_log.filter(col("status") != "success") # type: ignore
succeeded_results = df_execution_log.filter(col("status") == "success") # type: ignore

if failed_results.count() == 0:    
    print("Batch Succeeded")
    display(succeeded_results)
else:
    print("Batch Failed")
    display(failed_results)

close_batch(new_batch_id, master_notebook, 'closed') # type: ignore


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
