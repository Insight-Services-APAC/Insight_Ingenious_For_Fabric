# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************


# Define the lakehouses and their orchestrators
lakehouses_to_run = [
    {'name': 'Config', 'orchestrator': '0_orchestrator_Config'},
    {'name': 'EDW', 'orchestrator': '0_orchestrator_EDW'},
]

# Fabric Configurations 
fabric_environment: str = "development" 
config_lakehouse_name = "LH1"  # name of your Lakehouse
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"
edw_workspace_id = '50fbcab0-7d56-46f7-90f6-80ceb00ac86d'
edw_warehouse_id = '8a26f137-fd0b-47f6-92e1-e4551593c751'
edw_warehouse_name = 'WH'
edw_lakehouse_id = '8a26f137-fd0b-47f6-92e1-e4551593c751'
edw_lakehouse_name = 'LH'
legacy_synapse_connection_name = 'synapse_connection'
synapse_export_shortcut_path_in_onelake = 'exports/'
full_reset = False  




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions

# CELL ********************




from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import List, Callable, Optional
import inspect, hashlib
from dataclasses import dataclass, asdict
from notebookutils import mssparkutils

class lakehouse_utils:
    def __init__(self, target_workspace_id, target_lakehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id

    @staticmethod
    def check_if_table_exists(table_path):
        table_exists = False
        try:
            if DeltaTable.isDeltaTable(spark, table_path):
                table_exists = True
                #print(f"Delta table already exists at path: {table_path}, skipping creation.")
        except Exception as e:
            # If the path does not exist or is inaccessible, isDeltaTable returns False or may throw.
            # Treat exceptions as "table does not exist".
            #print(f"Could not verify Delta table existence at {table_path} (exception: {e}); assuming it does not exist.")
            table_exists = False
        return table_exists

    def lakehouse_tables_uri(self):
        return f"abfss://{self.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self.target_lakehouse_id}/Tables/"        

    def write_to_lakehouse_table(
        df,
        table_name: str,        
        format: str = "delta",
        mode: str = "overwrite",
        options: dict = None
    ):
        writer = df.write.format(format).mode(mode)        

        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        writer.save(f"{self.lakehouse_tables_uri()}{table_name}")
    
    def drop_all_tables(self):
            # Base ABFSS endpoint and the ‘Tables’ root
            lakehouse_root = (
                f"abfss://{self.target_workspace_id}"
                f"@onelake.dfs.fabric.microsoft.com/"
                f"{self.target_lakehouse_id}/Tables"
            )
            # ──────────────────────────────────────────────────────────────────────────────

            # 2. START spark and get Hadoop FS handle
            # ──────────────────────────────────────────────────────────────────────────────
            spark = SparkSession.builder.getOrCreate()

            # Access Hadoop’s FileSystem via the JVM gateway
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

            # Path object for the Tables/ directory
            root_path = spark._jvm.org.apache.hadoop.fs.Path(lakehouse_root)

            # ──────────────────────────────────────────────────────────────────────────────
            # 3. Iterate, detect Delta tables, and delete
            # ──────────────────────────────────────────────────────────────────────────────

            for status in fs.listStatus(root_path):
                table_path_obj = status.getPath()
                table_path = table_path_obj.toString()  # e.g. abfss://…/Tables/my_table

                try:
                    # Check if this directory is a Delta table
                    if DeltaTable.isDeltaTable(spark, table_path):
                        # Delete the directory (recursive=True)
                        deleted = fs.delete(table_path_obj, True)
                        if deleted:
                            print(f"✔ Dropped Delta table at: {table_path}")
                        else:
                            print(f"✖ Failed to delete: {table_path}")
                    else:
                        print(f"— Skipping non-Delta path: {table_path}")
                except Exception as e:
                    # e.g. permission issue, or not a Delta table
                    print(f"⚠ Error checking/deleting {table_path}: {e}")

            print("✅ All eligible Delta tables under ‘Tables/’ have been dropped.")



class ddl_utils:
    def __init__(self, target_workspace_id, target_lakehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id
        self.execution_log_table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/" \
            f"{target_lakehouse_id}/Tables/ddl_script_executions"
        self.initialise_ddl_script_executions_table()

    @staticmethod
    def execution_log_schema() -> StructType:
        return StructType([
            StructField("script_id", StringType(), nullable=False),
            StructField("script_name", StringType(), nullable=False),
            StructField("execution_status", StringType(), nullable=False),
            StructField("update_date", TimestampType(), nullable=False)
        ])

    def print_log(self):
        df = spark.read.format("delta").load(self.execution_log_table_path)
        display(df)


    def check_if_script_has_run(self,script_id) -> bool:
        from pyspark.sql.functions import col
        df = spark.read.format("delta").load(self.execution_log_table_path)
        #display(df)
        # Build filter condition
        cond = col("script_id") == script_id
        status = "Success"
        if status is not None:
            cond = cond & (col("execution_status") == status)
        matched = df.filter(cond).limit(1).take(1)
        if(len(matched)) == 0:
            #print("matched:", matched)
            return False
        else:   
            return True

    def print_skipped_script_execution(self, guid, object_name):
        print(f"skipping {guid}:{object_name} as the script has already run on workspace_id:" \
            f"{self.target_workspace_id} | lakehouse_id {self.target_lakehouse_id}")


    def write_to_execution_log(self, object_guid, object_name, script_status):
        data = [(object_guid, object_name, script_status, datetime.now())]
        new_df = spark.createDataFrame(data, ddl_utils.execution_log_schema())
        new_df.write \
        .format("delta") \
        .mode("append") \
        .save(self.execution_log_table_path)


    def run_once(
        self,
        work_fn: callable,
        object_name: str,
        guid: str 
    ):
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
        """
        # 1. Auto-derive GUID if not provided
        if guid is None:
            try:
                src = inspect.getsource(work_fn)
            except (OSError, TypeError):
                raise ValueError("work_fn must be a named function defined at top-level")
            # compute SHA256 and take first 12 hex chars
            digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
            guid = digest
            print(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(guid):
            try:
                work_fn()
                self.write_to_execution_log(object_guid=guid,
                                    object_name=object_name,
                                    script_status="Success")
            except Exception as e:
                print(f"Error in work_fn for {guid}: {e}")
                self.write_to_execution_log(object_guid=guid,
                                    object_name=object_name,
                                    script_status="Failure")                
                mssparkutils.notebook.exit(f"Script {object_name} with guid {guid} failed with error: {e}")
        else:
            self.print_skipped_script_execution(guid, object_name)

    def initialise_ddl_script_executions_table(self):
        guid="b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"        
        if(lakehouse_utils.check_if_table_exists(self.execution_log_table_path) == False):
            empty_df = spark.createDataFrame([], ddl_utils.execution_log_schema())
            (
                empty_df.write
                .format("delta")
                .option("parquet.vorder.default","true")
                .mode("errorIfExists")  # will error if table exists; change to "overwrite" to replace.
                .save(self.execution_log_table_path)
            )
            self.write_to_execution_log(object_guid=guid, object_name=object_name, script_status="Success")
        else:
            print(f"Skipping {object_name} as it already exists")


class config_utils:
    @dataclass
    class FabricConfig:
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
        full_reset: bool
        update_date: Optional[datetime] = None  # If you're tracking timestamps
        
        def get_attribute(self, attr_name: str) -> any:
            """Get attribute value by string name with error handling."""
            if hasattr(self, attr_name):
                return getattr(self, attr_name)
            else:
                raise AttributeError(f"FabricConfig has no attribute '{attr_name}'")
    
    
    def __init__(self, config_workspace_id, config_lakehouse_id):
        self.fabric_environments_table_uri = f"abfss://{config_workspace_id}@onelake.dfs.fabric.microsoft.com/" \
            f"{config_lakehouse_id}/Tables/config_fabric_environments"
        self._configs: dict[string,any] = {}

    @staticmethod
    def config_schema() -> StructType:
        return StructType([
            StructField("fabric_environment", StringType(), nullable=False),
            StructField("config_workspace_id", StringType(), nullable=False),
            StructField("config_lakehouse_id", StringType(), nullable=False),
            StructField("edw_workspace_id", StringType(), nullable=False),
            StructField("edw_warehouse_id", StringType(), nullable=False),
            StructField("edw_warehouse_name", StringType(), nullable=False),            
            StructField("edw_lakehouse_id", StringType(), nullable=False),
            StructField("edw_lakehouse_name", StringType(), nullable=False),
            StructField("legacy_synapse_connection_name", StringType(), nullable=False),
            StructField("synapse_export_shortcut_path_in_onelake", StringType(), nullable=False),
            StructField("full_reset", BooleanType(), nullable=False),
            StructField("update_date", TimestampType(), nullable=False)
        ])

    def get_configs_as_dict(self, fabric_environment: str):
        df = spark.read.format("delta").load(self.fabric_environments_table_uri)
        df_filtered = df.filter(df.fabric_environment == fabric_environment)

        # Convert to a list of Row objects (dict-like)
        configs = df_filtered.collect()
        
        # Convert to a list of dictionaries
        config_dicts = [row.asDict() for row in configs]

        # If expecting only one config per environment, return just the first dict
        return config_dicts[0] if config_dicts else None

    def get_configs_as_object(self, fabric_environment: str):
        df = spark.read.format("delta").load(self.fabric_environments_table_uri)
        row = df.filter(df.fabric_environment == fabric_environment).limit(1).collect()

        if not row:
            return None
        
        return config_utils.FabricConfig(**row[0].asDict())

    def merge_config_record(self, config: 'config_utils.FabricConfig'):        
        if config.update_date is None:
            config.update_date = datetime.now()
        data = [tuple(asdict(config).values())]
        
        
        df = spark.createDataFrame(data, config_utils.config_schema())

        if(lakehouse_utils.check_if_table_exists(self.fabric_environments_table_uri) == False):
            print('creating fabric environments table') 
            df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(self.fabric_environments_table_uri)
        else:
            print('updating fabric environments table') 
            target_table = DeltaTable.forPath(spark, self.fabric_environments_table_uri)
            # Perform the MERGE operation using environment as the key
            target_table.alias("t").merge(
                df.alias("s"),
                "t.fabric_environment = s.fabric_environment"
            ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Insert the Configs Into The Configuration Table

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)

cr = config_utils.FabricConfig(
    fabric_environment,
    config_workspace_id,
    config_lakehouse_id,
    edw_workspace_id,
    edw_warehouse_id,
    edw_warehouse_name,    
    edw_lakehouse_id,
    edw_lakehouse_name,
    legacy_synapse_connection_name,
    synapse_export_shortcut_path_in_onelake,
    full_reset
)

cu.merge_config_record(cr)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run All Lakehouse Orchestrators in Parallel

# CELL ********************




# Import required libraries
from notebookutils import mssparkutils
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

# Initialize variables
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
start_time = datetime.now()
results = {}

print(f"Starting parallel orchestration for all lakehouses")
print(f"Start time: {start_time}")
print(f"Workspace ID: {workspace_id}")
print(f"Total lakehouses to process: 2")
print("="*60)


# Define function to run a single lakehouse orchestrator
def run_lakehouse_orchestrator(lakehouse_name, orchestrator_name):
    """Run a single lakehouse orchestrator and return the result."""
    result = {
        'lakehouse': lakehouse_name,
        'orchestrator': orchestrator_name,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None,
        'status': 'Running',
        'error': None,
        'exit_value': None
    }
    
    try:
        print(f"[{result['start_time']}] Starting orchestrator for {lakehouse_name}")
        
        params = {
            "fabric_environment": fabric_environment,
            "config_workspace_id": config_workspace_id,
            "config_lakehouse_id": config_lakehouse_id,
            "target_lakehouse_config_prefix": f"{lakehouse_name}",
            "full_reset": full_reset
        }

        # Run the lakehouse orchestrator
        notebook_result = mssparkutils.notebook.run(
            f"{orchestrator_name}_Lakehouses_ddl_scripts",
            timeout_seconds=7200,  # 2 hour timeout per lakehouse
            arguments=params
        )
        
        result['end_time'] = datetime.now()
        result['duration'] = result['end_time'] - result['start_time']
        if notebook_result == 'success':
            result['status'] = 'Success'
        else:
            raise Exception(f"Notebook returned unexpected result: {notebook_result}")
        
        print(f"[{result['end_time']}] ✓ Completed {lakehouse_name} in {result['duration']}")
        
    except Exception as e:
        result['end_time'] = datetime.now()
        result['duration'] = result['end_time'] - result['start_time']
        result['status'] = 'Failed'
        result['error'] = str(e)
        
        print(f"[{result['end_time']}] ✗ Failed {lakehouse_name} after {result['duration']}")
        print(f"  Error: {result['error']}")
    
    return result


# Execute all lakehouse orchestrators in parallel
print("\nStarting parallel execution of lakehouse orchestrators...")
print("="*60)


# Run orchestrators in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=2) as executor:
    # Submit all tasks
    future_to_lakehouse = {
        executor.submit(
            run_lakehouse_orchestrator, 
            lakehouse['name'], 
            lakehouse['orchestrator']
        ): lakehouse['name'] 
        for lakehouse in lakehouses_to_run
    }
    
    # Process completed tasks as they finish
    for future in as_completed(future_to_lakehouse):
        lakehouse_name = future_to_lakehouse[future]
        try:
            result = future.result()
            results[lakehouse_name] = result
        except Exception as exc:
            print(f'Lakehouse {lakehouse_name} generated an exception: {exc}')
            results[lakehouse_name] = {
                'lakehouse': lakehouse_name,
                'status': 'Exception',
                'error': str(exc)
            }


# Generate detailed summary report
end_time = datetime.now()
total_duration = end_time - start_time

print("\n" + "="*60)
print("ORCHESTRATION SUMMARY REPORT")
print("="*60)
print(f"Total execution time: {total_duration}")
print(f"Total lakehouses: 2")

# Count results
success_count = sum(1 for r in results.values() if r['status'] == 'Success')
failed_count = sum(1 for r in results.values() if r['status'] == 'Failed')
exception_count = sum(1 for r in results.values() if r['status'] == 'Exception')

print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")
print(f"Exceptions: {exception_count}")
print("\n" + "-"*60)


# Detailed results for each lakehouse
print("\nDETAILED RESULTS BY LAKEHOUSE:")
print("="*60)

for lakehouse_name in sorted(results.keys()):
    result = results[lakehouse_name]
    print(f"\n{lakehouse_name}:")
    print(f"  Status: {result['status']}")
    
    if 'duration' in result and result['duration']:
        print(f"  Duration: {result['duration']}")
    
    if result['status'] == 'Success' and 'exit_value' in result:
        print(f"  Exit value: {result['exit_value']}")
    elif result['status'] in ['Failed', 'Exception'] and 'error' in result:
        print(f"  Error: {result['error']}")


# Create a summary table using markdown
summary_data = []
for lakehouse_name in sorted(results.keys()):
    result = results[lakehouse_name]
    status_icon = "✓" if result['status'] == 'Success' else "✗"
    duration_str = str(result.get('duration', 'N/A'))
    summary_data.append(f"| {lakehouse_name} | {status_icon} {result['status']} | {duration_str} |")

markdown_table = f"""
## Execution Summary Table

| Lakehouse | Status | Duration |
|-----------|--------|----------|
{''.join(summary_data)}

**Total Execution Time:** {total_duration}
"""

print(markdown_table)


# Final status and exit
if failed_count == 0 and exception_count == 0:
    final_message = f"✓ All {success_count} lakehouses processed successfully!"
    print(f"\n{final_message}")
    mssparkutils.notebook.exit(final_message)
else:
    final_message = f"Completed with {failed_count + exception_count} failures out of 2 lakehouses"
    print(f"\n✗ {final_message}")
    mssparkutils.notebook.exit(final_message)
    raise Exception(final_message)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
