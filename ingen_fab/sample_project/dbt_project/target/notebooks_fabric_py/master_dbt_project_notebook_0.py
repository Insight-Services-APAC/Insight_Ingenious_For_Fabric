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

# # #️⃣ Parameters

# PARAMETERS CELL ********************

pm_batch_id = None
pm_master_notebook = None

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
from concurrent.futures import ThreadPoolExecutor
import json
import time
import jsonpickle # type: ignore
import json
from pyspark.sql.types import * # type: ignore
from pyspark.sql.functions import * # type: ignore
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# CELL ********************

notebook_files1 = ['model.dbt_project.cities', 'model.dbt_project.countries'] # type: ignore
run_order1 = 0 # type: ignore

# Define a function to execute a notebook and return the results
@dataclass
class NotebookResult:    
    notebook: str
    start_time: int
    status: str
    error: str
    execution_time: int
    run_order: int

def execute_notebook(notebook_file):
    start_time = time.time()

    try:
        mssparkutils.notebook.run(notebook_file, 1800)
        status = 'success'
        error = None
    except Exception as e:
        status = 'error'
        error = str(e)

    execution_time = time.time() - start_time
    run_order = run_order1

    result = NotebookResult(notebook_file, start_time, status, error, execution_time,run_order)
    return result

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load the Execution Log


# CELL ********************

# Define the schema for the DataFrame
schema = StructType([ # type: ignore
    StructField("notebook", StringType(), True), # type: ignore
    StructField("start_time", DoubleType(), True), # type: ignore
    StructField("status", StringType(), True), # type: ignore
    StructField("error", StringType(), True), # type: ignore
    StructField("execution_time", DoubleType(), True), # type: ignore
    StructField("run_order", IntegerType(), True), # type: ignore
    StructField("batch_id", StringType(), True) # type: ignore
])

# Create an empty DataFrame with the defined schema
failed_results = spark.createDataFrame([], schema=schema) # type: ignore
# Read the log for this batch execution
df_execution_log = spark.sql(f"SELECT * FROM lh_log.execution_log WHERE batch_id = '{pm_batch_id}' AND master_notebook = '{pm_master_notebook}'") # type: ignore
if df_execution_log.count() > 0:
    
    # Check if any have not succeeded
    failed_results = df_execution_log.filter(col("status") != "success") # type: ignore

    # Print the failed results
    for row in failed_results.collect():
        print(f"Notebook {row['notebook']} failed with error: {row['error']}")

    # Check if have succeeded
    succeeded_results = df_execution_log.filter(col("status") == "success") # type: ignore

    # Print the succeeded results
    for row in succeeded_results.collect():
        print(f"Notebook {row['notebook']} succeeded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Execute Notebooks 

# CELL ********************

# Define the schema for the Log DataFrame
schema = StructType([ # type: ignore
    StructField("notebook", StringType(), True), # type: ignore
    StructField("start_time", DoubleType(), True), # type: ignore
    StructField("status", StringType(), True), # type: ignore
    StructField("error", StringType(), True), # type: ignore
    StructField("execution_time", DoubleType(), True), # type: ignore
    StructField("run_order", IntegerType(), True) # type: ignore
])

if failed_results.count() == 0:
    new_results = []
    # Use a ThreadPoolExecutor to run the notebooks in parallel
    # Execute the notebooks and collect the results
    with ThreadPoolExecutor(max_workers=5) as executor: # type: ignore
        new_results = list(executor.map(execute_notebook, notebook_files1)) # type: ignore

    # Write the results to the log file
    df_log = spark.createDataFrame(new_results, schema=schema) # type: ignore
    df_log = df_log.withColumn("batch_id", lit(f'{pm_batch_id}')) # type: ignore
    df_log = df_log.withColumn("master_notebook", lit(f'{pm_master_notebook}')) # type: ignore
    df_log.write.format("delta").mode("append").saveAsTable("lh_log.execution_log")
else:
    print("Failures in previous run_order... supressing execution")
    raise Exception("Failures in previous run_order... supressing execution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
