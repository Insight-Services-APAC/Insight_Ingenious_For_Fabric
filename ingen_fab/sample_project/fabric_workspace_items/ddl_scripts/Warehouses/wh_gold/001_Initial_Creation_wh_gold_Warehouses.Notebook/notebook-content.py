# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark",
# META     "display_name": "PySpark (Synapse)"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "language_group": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************


# Default parameters
# Add default parameters here




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Load Python Libraries and Initialize Environment

# CELL ********************

import sys

# Check if running in Fabric environment
if "notebookutils" in sys.modules:
    import sys
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    
    
    run_mode = "fabric"
    sys.path.insert(0, mount_path)

    
    # PySpark environment - spark session should be available
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()
        
    spark = None
    
    mount_path = None
    run_mode = "local"

import traceback

def load_python_modules_from_path(base_path: str, relative_files: list[str], max_chars: int = 1_000_000_000):
    """
    Executes Python files from a Fabric-mounted file path using notebookutils.fs.head.
    
    Args:
        base_path (str): The root directory where modules are located.
        relative_files (list[str]): List of relative paths to Python files (from base_path).
        max_chars (int): Max characters to read from each file (default: 1,000,000).
    """
    success_files = []
    failed_files = []

    for relative_path in relative_files:
        if base_path.startswith("file:") or base_path.startswith("abfss:"):
            full_path = f"{base_path}/{relative_path}"
        else:
            full_path = f"file:{base_path}/{relative_path}"
        try:
            print(f"🔄 Loading: {full_path}")
            code = notebookutils.fs.head(full_path, max_chars)
            exec(code, globals())  # Use globals() to share context across modules
            success_files.append(relative_path)
        except Exception as e:
            failed_files.append(relative_path)
            print(f"❌ Error loading {relative_path}")
            print(f"   Error type: {type(e).__name__}")
            print(f"   Error message: {str(e)}")
            print(f"   Stack trace:")
            traceback.print_exc()

    print("\n✅ Successfully loaded:")
    for f in success_files:
        print(f" - {f}")

    if failed_files:
        print("\n⚠️ Failed to load:")
        for f in failed_files:
            print(f" - {f}")

def clear_module_cache(prefix: str):
    """Clear module cache for specified prefix"""
    for mod in list(sys.modules):
        if mod.startswith(prefix):
            print("deleting..." + mod)
            del sys.modules[mod]

# Clear the module cache only when running in Fabric environment
# When running locally, module caching conflicts can occur in parallel execution
if run_mode == "fabric":
    # Check if ingen_fab modules are present in cache (indicating they need clearing)
    ingen_fab_modules = [mod for mod in sys.modules.keys() if mod.startswith(('ingen_fab.python_libs', 'ingen_fab'))]
    
    if ingen_fab_modules:
        print(f"Found {len(ingen_fab_modules)} ingen_fab modules to clear from cache")
        clear_module_cache("ingen_fab.python_libs")
        clear_module_cache("ingen_fab")
        print("✓ Module cache cleared for ingen_fab libraries")
    else:
        print("ℹ No ingen_fab modules found in cache - already cleared or first load")




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************


import sys

if "notebookutils" in sys.modules and hasattr(notebookutils, 'fs'):
    # Fabric mode - load from mounted path
    mount_path = notebookutils.fs.getMountPath("/config_files")
    from datetime import datetime
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/python/sql_templates.py",
        "ingen_fab/python_libs/python/warehouse_utils.py",
        "ingen_fab/python_libs/python/warehouse_ddl_utils.py",
        "ingen_fab/python_libs/python/pipeline_utils.py"
    ]

    # Use the mount path directly for load_python_modules_from_path
    load_python_modules_from_path(mount_path, files_to_load)
    
    # Verify all required modules are available after loading
    print("📋 Verifying loaded modules...")
    try:
        # Test if config functions are available
        if 'get_configs_as_object' in globals():
            test_config = get_configs_as_object()
            print("✅ Config utilities loaded successfully")
        else:
            print("⚠️ get_configs_as_object function not found in globals")
            
        # Test if WarehouseDDLUtils is available
        if 'WarehouseDDLUtils' in globals():
            print("✅ WarehouseDDLUtils class loaded successfully")
        else:
            print("⚠️ WarehouseDDLUtils class not found in globals")
            
        # Test if SQLTemplates is available
        if 'SQLTemplates' in globals():
            print("✅ SQLTemplates class loaded successfully")
        else:
            print("⚠️ SQLTemplates class not found in globals")
            
        # Show available globals for debugging
        relevant_globals = [k for k in globals().keys() if not k.startswith('_') and ('config' in k.lower() or 'warehouse' in k.lower() or 'ddl' in k.lower() or 'utils' in k.lower() or 'sql' in k.lower() or 'template' in k.lower())]
        print(f"📋 Relevant loaded globals: {relevant_globals}")
        
    except Exception as e:
        print(f"⚠️ Error testing loaded modules: {str(e)}")
        print("This is expected if config variables are not yet injected")
else:
    # Local mode - direct imports
    from datetime import datetime
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.python.sql_templates import SQLTemplates
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.warehouse_ddl_utils import WarehouseDDLUtils
    from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils
    notebookutils = NotebookUtilsFactory.create_instance()



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📦 Install Required Packages and Import Libraries

# CELL ********************


# Try to install pyodbc for warehouse connections (if pip is available)
try:
    import pyodbc
    print("✅ pyodbc already available")
except ImportError:
    try:
        get_ipython().run_line_magic('pip', 'install pyodbc')
        import pyodbc
        print("✅ pyodbc installed successfully")
    except Exception as e:
        print(f"⚠️ Could not install pyodbc via pip magic: {str(e)}")
        print("📋 Please ensure pyodbc is available in your environment")
        print("   - In Fabric, pyodbc may already be pre-installed")
        print("   - In local environments, install via: pip install pyodbc")

# Required packages for Python structures and utilities
import struct
from itertools import chain, repeat
try:
    import pyodbc
except ImportError:
    print("❌ pyodbc not available - warehouse connections will not work")
    pyodbc = None

import sempy.fabric as fabric
import pandas as pd
from notebookutils import mssparkutils

# Set the ODBC Driver Code for AAD Token handling
SQL_COPT_SS_ACCESS_TOKEN = 1256



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ⚙️ Configuration Settings

# CELL ********************


# variableLibraryInjectionStart: var_lib


# variableLibraryInjectionEnd: var_lib



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Configuration Loading and Warehouse Setup

# CELL ********************


import sys

target_warehouse_config_prefix = "wh_gold"

# Determine execution mode and load configuration utilities
if "notebookutils" in sys.modules and hasattr(notebookutils, 'fs'):
    # Fabric mode - need to load config utilities from mounted path
    mount_path = notebookutils.fs.getMountPath("/config_files")
    try:
        # Try to use existing functions first
        configs: ConfigsObject = get_configs_as_object()
        print("✅ Config utilities already available")
    except NameError as e:
        print(f"🔄 Config function not available: {str(e)}")
        print("Available globals:", [k for k in globals().keys() if 'config' in k.lower()])
        print("🔄 Loading config utilities directly...")
        print(f"📁 Mount path: {mount_path}")
        
        # Try different path formats that work with notebookutils.fs.head
        config_paths_to_try = [
            f"file:{mount_path}/ingen_fab/python_libs/common/config_utils.py",
            f"{mount_path}/ingen_fab/python_libs/common/config_utils.py",
            "/config_files/ingen_fab/python_libs/common/config_utils.py"
        ]
        
        config_loaded = False
        for config_path in config_paths_to_try:
            try:
                print(f"🔄 Trying path: {config_path}")
                config_code = notebookutils.fs.head(config_path, 1_000_000)
                exec(config_code)
                # Test if the function is now available
                configs: ConfigsObject = get_configs_as_object()
                print(f"✅ Config utilities loaded successfully from: {config_path}")
                config_loaded = True
                break
            except Exception as e:
                print(f"❌ Failed with path {config_path}: {str(e)}")
                continue
        
        if not config_loaded:
            print("❌ All direct config loading attempts failed")
            print("🔄 Falling back to re-running load_python_modules_from_path...")
            # Try reloading all modules again
            files_to_reload = ["ingen_fab/python_libs/common/config_utils.py"]
            load_python_modules_from_path(mount_path, files_to_reload)
            try:
                configs: ConfigsObject = get_configs_as_object()
                print("✅ Config utilities loaded via fallback method")
            except NameError:
                print("❌ Fallback method also failed")
                print("🔄 Attempting to continue without config - this may fail later...")
                configs = None
else:
    # Local mode - functions should already be imported
    configs: ConfigsObject = get_configs_as_object()

# Get config values if configs object is available
if configs is not None:
    target_warehouse_id = get_config_value(f"{target_warehouse_config_prefix.lower()}_warehouse_id")
    target_workspace_id = get_config_value(f"{target_warehouse_config_prefix.lower()}_workspace_id")
else:
    print("⚠️ Config object not available, using placeholder values")
    target_warehouse_id = "placeholder_warehouse_id"
    target_workspace_id = "placeholder_workspace_id"

print(f"🏢 FABRIC WAREHOUSE CONNECTION - EXECUTION MODE")
print(f"Target Warehouse ID: {target_warehouse_id}")
print(f"Target Workspace ID: {target_workspace_id}")
print("=" * 60)

# Get workspace information and warehouse connection details
workspace_id = spark.conf.get("trident.workspace.id")
workspace_items_df = fabric.list_items('Warehouse')

# Find the specific warehouse by ID
filtered_items_df = workspace_items_df[workspace_items_df['Id'] == target_warehouse_id]

if filtered_items_df.empty:
    print(f"❌ Warehouse with ID {target_warehouse_id} not found in workspace")
    raise ValueError(f"Warehouse {target_warehouse_id} not found")

# Get warehouse connection string from Fabric API
wh_connection_string = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/warehouses/{target_warehouse_id}/connectionString").json()
sql_endpoint = wh_connection_string['connectionString'].split(':')[0]

print(f"✅ Found warehouse: {filtered_items_df['Display Name'].values[0]}")
print(f"📡 SQL Endpoint: {sql_endpoint}")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔐 Establish Warehouse Connection

# CELL ********************


# Define the connection string using the retrieved endpoint
driver_name = "ODBC Driver 18 for SQL Server"
connection_string = "Driver={" + driver_name + "};Server=" + sql_endpoint + ",1433;Database=" + target_warehouse_id + ";Encrypt=Yes;TrustServerCertificate=No"

# Get and format the authentication token
token = mssparkutils.credentials.getToken("pbi")
token_as_bytes = bytes(token, "UTF-8")
encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes

# Establish the connection with token authentication
attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: token_bytes}

try:
    conn = pyodbc.connect(connection_string, autocommit=False, attrs_before=attrs_before)
    print("✅ Successfully connected to warehouse!")
    warehouse_connection_available = True
except Exception as e:
    print(f"❌ Failed to connect to warehouse: {str(e)}")
    warehouse_connection_available = False
    conn = None

print("=" * 60)

class FabricWarehouseConnection:
    """Fabric warehouse connection using proven PyODBC approach."""
    
    def __init__(self, target_warehouse_id: str, target_workspace_id: str, connection_string: str, token_bytes: bytes):
        self.target_warehouse_id = target_warehouse_id
        self.target_workspace_id = target_workspace_id
        self._target_warehouse_id = target_warehouse_id  # For compatibility
        self._target_workspace_id = target_workspace_id  # For compatibility
        self.connection_string = connection_string
        self.token_bytes = token_bytes
        self.attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: token_bytes}
        
        # Check if we can actually connect
        global warehouse_connection_available
        self.can_execute = warehouse_connection_available
    
    def get_connection(self):
        """Get a database connection to the warehouse."""
        if not self.can_execute:
            return None
        try:
            return pyodbc.connect(self.connection_string, autocommit=False, attrs_before=self.attrs_before)
        except Exception as e:
            print(f"⚠️ Connection failed: {str(e)}")
            return None
    
    def check_if_table_exists(self, table_name: str, schema_name: str = "dbo") -> bool:
        """Check if a table exists in the warehouse."""
        query = f"""
        SELECT COUNT(*) as count
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = '{table_name}' AND s.name = '{schema_name}'
        """
        
        if self.can_execute:
            try:
                result = self.execute_query(query)
                if result and len(result) > 0:
                    count = result[0][0]
                    exists = count > 0
                    print(f"🔍 Table {schema_name}.{table_name}: {'EXISTS' if exists else 'NOT FOUND'}")
                    return exists
            except Exception as e:
                print(f"⚠️ Error checking table existence: {str(e)}")
        
        print(f"🔍 CHECK TABLE EXISTS: {schema_name}.{table_name} (assuming NOT EXISTS)")
        return False
    
    def create_schema_if_not_exists(self, schema_name: str, max_retries: int = 3):
        """Create schema if it doesn't exist."""
        query = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}')
        BEGIN
            EXEC('CREATE SCHEMA [{schema_name}]')
        END
        """
        print(f"\n📁 CREATE SCHEMA: {schema_name}")
        print("DDL to execute against warehouse:")
        print(f"```sql\n{query}\n```")
        
        if self.can_execute:
            self.execute_ddl(query, f"Create schema {schema_name}", max_retries)
        else:
            print("⚠️ Execution disabled - DDL displayed only")

    @property
    def target_store_id(self) -> str:
        """Return target warehouse ID for compatibility."""
        return self._target_warehouse_id

    def execute_query(self, query: str, conn=None, params=None, max_retries: int = 3):
        """Execute a SELECT query and return results."""
        if not self.can_execute:
            print("⚠️ Query execution disabled")
            return []
            
        connection = conn or self.get_connection()
        if not connection:
            return []
            
        for attempt in range(max_retries):
            try:
                cursor = connection.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch results for SELECT queries
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    cursor.close()
                    if not conn:  # Only close if we created the connection
                        connection.close()
                    return results
                else:
                    cursor.close()
                    if not conn:
                        connection.close()
                    return []
                    
            except Exception as e:
                print(f"⚠️ Query execution attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    print(f"❌ Query failed after {max_retries} attempts")
                    if not conn:
                        connection.close()
                    return []
                else:
                    print(f"🔄 Retrying query execution (attempt {attempt + 2}/{max_retries})...")

    def execute_ddl(self, ddl_query: str, operation_name: str = "DDL Operation", max_retries: int = 3):
        """Execute DDL commands with proper transaction handling."""
        if not self.can_execute:
            print(f"⚠️ DDL execution disabled for: {operation_name}")
            return False
            
        connection = self.get_connection()
        if not connection:
            print(f"❌ Cannot establish connection for: {operation_name}")
            return False
            
        for attempt in range(max_retries):
            try:
                cursor = connection.cursor()
                cursor.execute(ddl_query)
                connection.commit()
                cursor.close()
                connection.close()
                print(f"✅ {operation_name} executed successfully")
                return True
                
            except pyodbc.Error as ex:
                sqlstate = ex.args
                print(f"❌ {operation_name} failed (attempt {attempt + 1}): {sqlstate}")
                connection.rollback()
                
                if attempt == max_retries - 1:
                    print(f"❌ {operation_name} failed after {max_retries} attempts")
                    connection.close()
                    return False
                else:
                    print(f"🔄 Retrying {operation_name} (attempt {attempt + 2}/{max_retries})...")
            except Exception as e:
                print(f"❌ Unexpected error in {operation_name} (attempt {attempt + 1}): {str(e)}")
                connection.rollback()
                if attempt == max_retries - 1:
                    connection.close()
                    return False

# Initialize the warehouse connection instance
if warehouse_connection_available and conn:
    warehouse_ddl_utils = FabricWarehouseConnection(
        target_warehouse_id, 
        target_workspace_id, 
        connection_string, 
        token_bytes
    )
    print("✅ Warehouse DDL utilities initialized")
    
    # Initialize DDL utilities with run-once tracking
    try:
        # Check if WarehouseDDLUtils is available
        if 'WarehouseDDLUtils' not in globals():
            print("⚠️ WarehouseDDLUtils not found in globals, attempting to import directly...")
            
            # Try to load it directly if in Fabric mode
            if "notebookutils" in sys.modules and hasattr(notebookutils, 'fs'):
                mount_path = notebookutils.fs.getMountPath("/config_files")
                
                # Try different path formats for WarehouseDDLUtils
                warehouse_ddl_paths_to_try = [
                    f"file:{mount_path}/ingen_fab/python_libs/python/warehouse_ddl_utils.py",
                    f"{mount_path}/ingen_fab/python_libs/python/warehouse_ddl_utils.py",
                    "/config_files/ingen_fab/python_libs/python/warehouse_ddl_utils.py"
                ]
                
                warehouse_ddl_loaded = False
                for warehouse_ddl_path in warehouse_ddl_paths_to_try:
                    try:
                        print(f"🔄 Trying WarehouseDDLUtils path: {warehouse_ddl_path}")
                        warehouse_ddl_utils_code = notebookutils.fs.head(warehouse_ddl_path, 1_000_000)
                        exec(warehouse_ddl_utils_code)
                        print(f"✅ WarehouseDDLUtils loaded successfully from: {warehouse_ddl_path}")
                        warehouse_ddl_loaded = True
                        break
                    except Exception as e:
                        print(f"❌ Failed with WarehouseDDLUtils path {warehouse_ddl_path}: {str(e)}")
                        continue
                
                if not warehouse_ddl_loaded:
                    print("❌ All WarehouseDDLUtils direct loading attempts failed")
                    print("🔄 Falling back to re-running load_python_modules_from_path for WarehouseDDLUtils...")
                    # Try reloading the specific module again
                    files_to_reload = ["ingen_fab/python_libs/python/warehouse_ddl_utils.py"]
                    load_python_modules_from_path(mount_path, files_to_reload)
                    
                    if 'WarehouseDDLUtils' not in globals():
                        print("❌ WarehouseDDLUtils still not available after fallback")
                        raise ImportError("WarehouseDDLUtils not available after all attempts")
                    else:
                        print("✅ WarehouseDDLUtils loaded via fallback method")
            else:
                # Local mode - should already be imported
                from ingen_fab.python_libs.python.warehouse_ddl_utils import WarehouseDDLUtils
        
        du = WarehouseDDLUtils(
            warehouse_connection=warehouse_ddl_utils,
            target_warehouse_id=target_warehouse_id,
            target_workspace_id=target_workspace_id
        )
        print("✅ Warehouse DDL execution tracking initialized")
        
    except Exception as e:
        print(f"❌ Failed to initialize WarehouseDDLUtils: {str(e)}")
        print("🔄 Continuing without run-once tracking...")
        du = None
        
else:
    warehouse_ddl_utils = None
    du = None
    print("⚠️ Warehouse DDL utilities not available - display mode only")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🏗️ DDL Generation and Execution

# CELL ********************


# Initialize SQL Templates
try:
    # Check if SQLTemplates is available
    if 'SQLTemplates' not in globals():
        print("⚠️ SQLTemplates not found in globals, attempting to import directly...")
        
        # Try to load it directly if in Fabric mode
        if "notebookutils" in sys.modules and hasattr(notebookutils, 'fs'):
            mount_path = notebookutils.fs.getMountPath("/config_files")
            
            # Try different path formats for SQLTemplates
            sql_templates_paths_to_try = [
                f"file:{mount_path}/ingen_fab/python_libs/python/sql_templates.py",
                f"{mount_path}/ingen_fab/python_libs/python/sql_templates.py",
                "/config_files/ingen_fab/python_libs/python/sql_templates.py"
            ]
            
            sql_templates_loaded = False
            for sql_templates_path in sql_templates_paths_to_try:
                try:
                    print(f"🔄 Trying SQLTemplates path: {sql_templates_path}")
                    sql_templates_code = notebookutils.fs.head(sql_templates_path, 1_000_000)
                    exec(sql_templates_code)
                    print(f"✅ SQLTemplates loaded successfully from: {sql_templates_path}")
                    sql_templates_loaded = True
                    break
                except Exception as e:
                    print(f"❌ Failed with SQLTemplates path {sql_templates_path}: {str(e)}")
                    continue
            
            if not sql_templates_loaded:
                print("❌ All SQLTemplates direct loading attempts failed")
                print("🔄 Falling back to re-running load_python_modules_from_path for SQLTemplates...")
                # Try reloading the specific module again
                files_to_reload = ["ingen_fab/python_libs/python/sql_templates.py"]
                load_python_modules_from_path(mount_path, files_to_reload)
                
                if 'SQLTemplates' not in globals():
                    print("❌ SQLTemplates still not available after fallback")
                    raise ImportError("SQLTemplates not available after all attempts")
                else:
                    print("✅ SQLTemplates loaded via fallback method")
        else:
            # Local mode - should already be imported
            from ingen_fab.python_libs.python.sql_templates import SQLTemplates
    
    sql_templates = SQLTemplates()
    print("✅ SQL Templates initialized successfully")
    
except Exception as e:
    print(f"❌ Failed to initialize SQLTemplates: {str(e)}")
    print("🔄 DDL generation will not be available")
    sql_templates = None


# =====================================================
# Individual SQL File Execution
# =====================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🏢 Cell for 001_wh_gold_create_schema.sql

# CELL ********************

# Script GUID: c0521b16dd5f
# File: 001_wh_gold_create_schema.sql

def work():
    """Execute warehouse SQL with run-once tracking"""
    sql = """
CREATE SCHEMA DW
    """
    
    print(f"🔄 Executing SQL from 001_wh_gold_create_schema.sql:")
    print(f"```sql\n{sql.strip()}\n```")
    
    # Use warehouse DDL utilities for execution
    if du and du.warehouse_connection.can_execute:
        try:
            result = du.warehouse_connection.execute_ddl(
                sql.strip(), 
                f"Execute 001_wh_gold_create_schema.sql", 
                max_retries=3
            )
            if result:
                print(f"✅ Successfully executed 001_wh_gold_create_schema.sql")
                return True
            else:
                print(f"❌ Failed to execute 001_wh_gold_create_schema.sql")
                return False
        except Exception as e:
            print(f"❌ Error executing 001_wh_gold_create_schema.sql: {str(e)}")
            return False
    else:
        print("⚠️ Warehouse connection not available - SQL displayed only")
        return True

# Execute with run-once tracking
du.run_once(work, "001_wh_gold_create_schema", "c0521b16dd5f")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🏢 Cell for 002_wh_gold_create_cities_view.sql

# CELL ********************

# Script GUID: 3522e7951a55
# File: 002_wh_gold_create_cities_view.sql

def work():
    """Execute warehouse SQL with run-once tracking"""
    sql = """
CREATE OR ALTER VIEW DW.vDim_Cities
as 
select * from [lh_gold].[dbo].[dim_cities]
    """
    
    print(f"🔄 Executing SQL from 002_wh_gold_create_cities_view.sql:")
    print(f"```sql\n{sql.strip()}\n```")
    
    # Use warehouse DDL utilities for execution
    if du and du.warehouse_connection.can_execute:
        try:
            result = du.warehouse_connection.execute_ddl(
                sql.strip(), 
                f"Execute 002_wh_gold_create_cities_view.sql", 
                max_retries=3
            )
            if result:
                print(f"✅ Successfully executed 002_wh_gold_create_cities_view.sql")
                return True
            else:
                print(f"❌ Failed to execute 002_wh_gold_create_cities_view.sql")
                return False
        except Exception as e:
            print(f"❌ Error executing 002_wh_gold_create_cities_view.sql: {str(e)}")
            return False
    else:
        print("⚠️ Warehouse connection not available - SQL displayed only")
        return True

# Execute with run-once tracking
du.run_once(work, "002_wh_gold_create_cities_view", "3522e7951a55")







# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📇 Print the execution log

# CELL ********************


if du:
    du.print_log()
else:
    print("📇 Execution log not available - DDL tracking not initialized")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🎯 Summary and Cleanup

# CELL ********************


print("\n🎯 DDL EXECUTION SUMMARY")
print("=" * 60)

if du and du.warehouse_connection.can_execute:
    print("✅ Warehouse connection was available")
    print("✅ DDL commands were executed against the warehouse with run-once tracking")
    print(f"🗂️ Target Warehouse: {target_warehouse_id}")
    print(f"📁 Target Workspace: {target_workspace_id}")
    print("🔄 Run-once tracking prevents duplicate executions")
else:
    print("⚠️ Warehouse connection was not available")
    print("📋 DDL commands were displayed only")

print(f"\n📊 Tables Processed: 0")


print("\n🎉 DDL Processing Complete!")
print("=" * 60)

# Clean up any remaining connections
try:
    if 'conn' in locals() and conn:
        conn.close()
        print("🧹 Connection cleaned up")
except:
    pass



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ✔️ If we make it to the end return a successful result

# CELL ********************


mssparkutils.notebook.exit("success")