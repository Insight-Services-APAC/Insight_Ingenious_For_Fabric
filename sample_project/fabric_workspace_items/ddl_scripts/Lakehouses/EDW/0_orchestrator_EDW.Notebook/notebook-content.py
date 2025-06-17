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



#lakehouse_name = "LH"  # name of your Lakehouse
#config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
#config_lakehouse_id = "a29c9d15-c24e-4779-8344-0c7c237b3990"

# Target for DDL scripts
target_lakehouse_config_prefix = "edw"

# Fabric Configurations 
fabric_environment: str = "development" 
config_lakehouse_name = "LH1"  # name of your Lakehouse
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"


# ONLY USE THIS IN DEV
full_reset = False # ⚠️ Full reset -- DESTRUCTIVE - will drop all tables






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

# ## Instantiate the Helper Classes

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Drop all Tables in Target LH

# CELL ********************



if full_reset == True:
    from ipywidgets import ToggleButtons, VBox, Label, Button
    from IPython.display import display as pdisplay
    from IPython.display import clear_output

    
    configs = cu.get_configs_as_object(fabric_environment)

    target_workspace_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id")
    target_lakehouse_id =  configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
    
    # Build the widget
    prompt = Label("❗ This will drop all tables — do you want to proceed?")
    yesno = ToggleButtons(options=["No","Yes"], description="Confirm Destructive Action:")
    go = Button(description="Submit", button_style="warning")

    out = VBox([prompt, yesno, go])
    pdisplay(out)

    # Define what happens on click
    def on_click(b):
        clear_output()  # hide the widget after click
        if yesno.value == "Yes":
            print("Dropping tables…")
            lu = lakehouse_utils(target_workspace_id, target_lakehouse_id)
            lu.drop_all_tables()
        else:
            print("Operation cancelled.")

    go.on_click(on_click)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run the lakehouse DDL Notebooks

# CELL ********************



# Import required libraries
from notebookutils import mssparkutils
import sys
from datetime import datetime

# Initialize variables
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
success_count = 0
failed_notebook = None
start_time = datetime.now()

# Define execution function
def execute_notebook(notebook_name, index, total, timeout_seconds=3600):
    """Execute a single notebook and handle success/failure."""
    global success_count
    
    try:
        
        print(f"{'='*60}")
        print(f"Executing notebook {index}/{total}:{notebook_name}")
        print(f"{'='*60}")
        params = {
            "fabric_environment": fabric_environment,
            "config_workspace_id": config_workspace_id,
            "config_lakehouse_id": config_lakehouse_id,
            "target_lakehouse_config_prefix": target_lakehouse_config_prefix,
            'useRootDefaultLakehouse': True
        }
        # Run the notebook
        result = mssparkutils.notebook.run(
            notebook_name,
            timeout_seconds,
            params
        )
        
        if (result == 'success'):
            success_count += 1
        else: 
            raise Exception({"result": result}) 

        print(f"✓ Successfully executed: {notebook_name}")
        print(f"Exit value: {result}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to execute: {notebook_name}")
        print(f"Error: {str(e)}")
        
        # Stop execution on failure
        error_msg = f"Orchestration stopped due to failure in notebook: {notebook_name}. Error: {str(e)}"
        mssparkutils.notebook.exit(error_msg)
        return False

print(f"Starting orchestration for EDW lakehouse")
print(f"Start time: {start_time}")
print(f"Workspace ID: {workspace_id}")
print(f"Total notebooks to execute: 1")
print("="*60)
execute_notebook("001_Initial_Creation_EDW_Lakehouses_ddl_scripts", 1, 1)

# Final Summary
end_time = datetime.now()
duration = end_time - start_time

print(f"{'='*60}")
print(f"Orchestration Complete!")
print(f"{'='*60}")
print(f"End time: {end_time}")
print(f"Duration: {duration}")
print(f"Total notebooks: 1")
print(f"Successfully executed: {success_count}")
print(f"Failed: 1 - {success_count}")

if success_count == 1:
    print("✓ All notebooks executed successfully!")
    mssparkutils.notebook.exit("success")
else:
    print(f"✗ Orchestration completed with failures")
    mssparkutils.notebook.exit(f"Orchestration completed with {success_count}/1 successful executions")



