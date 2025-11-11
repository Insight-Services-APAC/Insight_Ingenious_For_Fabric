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


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py"
    ]

    load_python_modules_from_path(mount_path, files_to_load)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Instantiate Required Classes 

# CELL ********************


target_lakehouse_config_prefix = "lh_bronze"
configs: ConfigsObject = get_configs_as_object()
target_lakehouse_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
target_workspace_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_workspace_id")

target_lakehouse = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark  # Pass the Spark session if available
)

du = ddl_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark  # Pass the Spark session if available
)

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🏃‍♂️‍➡️ Run DDL Cells 

# CELL ********************


# DDL cells are injected below:



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 001_application_cities_table_create.py

# CELL ********************

guid="395b97eae19d"
object_name = "001_application_cities_table_create"

def script_to_execute():
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql import Row
    from datetime import datetime
    
    # DDL script for creating an application cities table in lakehouse
    # This demonstrates the basic pattern for creating Delta tables with sample data
    # Based on the structure of application_cities.csv
    
    
    # Define the schema for the application cities table
    schema = StructType(
        [
            StructField("CityID", IntegerType(), nullable=True),
            StructField("CityName", StringType(), nullable=True),
            StructField("StateProvinceID", IntegerType(), nullable=True),
            StructField("LatestRecordedPopulation", IntegerType(), nullable=True),
            StructField("LastEditedBy", StringType(), nullable=True),
            StructField("ValidFrom", TimestampType(), nullable=True),
            StructField("ValidTo", TimestampType(), nullable=True),
        ]
    )
    
    # Sample data for the first 10 rows based on the CSV file
    sample_data = [
        Row(
            CityID=1,
            CityName="Aaronsburg",
            StateProvinceID=39,
            LatestRecordedPopulation=613,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=3,
            CityName="Abanda",
            StateProvinceID=1,
            LatestRecordedPopulation=192,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=4,
            CityName="Abbeville",
            StateProvinceID=42,
            LatestRecordedPopulation=5237,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=5,
            CityName="Abbeville",
            StateProvinceID=11,
            LatestRecordedPopulation=2908,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=6,
            CityName="Abbeville",
            StateProvinceID=1,
            LatestRecordedPopulation=2688,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=7,
            CityName="Abbeville",
            StateProvinceID=19,
            LatestRecordedPopulation=12257,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=8,
            CityName="Abbeville",
            StateProvinceID=25,
            LatestRecordedPopulation=419,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=9,
            CityName="Abbotsford",
            StateProvinceID=52,
            LatestRecordedPopulation=2310,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=10,
            CityName="Abbott",
            StateProvinceID=45,
            LatestRecordedPopulation=356,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CityID=11,
            CityName="Abbott",
            StateProvinceID=4,
            LatestRecordedPopulation=356,
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
    ]
    
    # Create DataFrame with the sample data
    df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)
    
    # Write the DataFrame to create the table with initial data
    target_lakehouse.write_to_table(
        df=df_with_data, table_name="cities", schema_name=""
    )
    

du.run_once(script_to_execute, "001_application_cities_table_create","395b97eae19d")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_application_countries_table_create.py

# CELL ********************

guid="baf5c8950c26"
object_name = "002_application_countries_table_create"

def script_to_execute():
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql import Row
    from datetime import datetime
    
    # DDL script for creating an application countries table in lakehouse
    # This demonstrates the basic pattern for creating Delta tables with sample data
    # Based on the structure of application_countries.csv
    
    
    # Define the schema for the application countries table
    schema = StructType(
        [
            StructField("CountryID", IntegerType(), nullable=True),
            StructField("CountryName", StringType(), nullable=True),
            StructField("FormalName", StringType(), nullable=True),
            StructField("IsoAlpha3Code", StringType(), nullable=True),
            StructField("IsoNumericCode", StringType(), nullable=True),
            StructField("CountryType", StringType(), nullable=True),
            StructField("LatestRecordedPopulation", LongType(), nullable=True),
            StructField("Continent", StringType(), nullable=True),
            StructField("Region", StringType(), nullable=True),
            StructField("Subregion", StringType(), nullable=True),
            StructField("LastEditedBy", StringType(), nullable=True),
            StructField("ValidFrom", TimestampType(), nullable=True),
            StructField("ValidTo", TimestampType(), nullable=True),
        ]
    )
    
    # Sample data for the first 10 rows based on the CSV file
    sample_data = [
        Row(
            CountryID=1,
            CountryName="Afghanistan",
            FormalName="Islamic State of Afghanistan",
            IsoAlpha3Code="AFG",
            IsoNumericCode="4",
            CountryType="UN Member State",
            LatestRecordedPopulation=28400000,
            Continent="Asia",
            Region="Asia",
            Subregion="Southern Asia",
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=3,
            CountryName="Albania",
            FormalName="Republic of Albania",
            IsoAlpha3Code="ALB",
            IsoNumericCode="8",
            CountryType="UN Member State",
            LatestRecordedPopulation=3785031,
            Continent="Europe",
            Region="Europe",
            Subregion="Southern Europe",
            LastEditedBy="20",
            ValidFrom=datetime(2013, 7, 1, 16, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=4,
            CountryName="Algeria",
            FormalName="People's Democratic Republic of Algeria",
            IsoAlpha3Code="DZA",
            IsoNumericCode="12",
            CountryType="UN Member State",
            LatestRecordedPopulation=34178188,
            Continent="Africa",
            Region="Africa",
            Subregion="Northern Africa",
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=6,
            CountryName="Andorra",
            FormalName="Principality of Andorra",
            IsoAlpha3Code="AND",
            IsoNumericCode="20",
            CountryType="UN Member State",
            LatestRecordedPopulation=87243,
            Continent="Europe",
            Region="Europe",
            Subregion="Southern Europe",
            LastEditedBy="15",
            ValidFrom=datetime(2015, 7, 1, 16, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=7,
            CountryName="Angola",
            FormalName="People's Republic of Angola",
            IsoAlpha3Code="AGO",
            IsoNumericCode="24",
            CountryType="UN Member State",
            LatestRecordedPopulation=12799293,
            Continent="Africa",
            Region="Africa",
            Subregion="Middle Africa",
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=10,
            CountryName="Antigua and Barb.",
            FormalName="Antigua and Barbuda",
            IsoAlpha3Code="ATG",
            IsoNumericCode="28",
            CountryType="UN Member State",
            LatestRecordedPopulation=85632,
            Continent="North America",
            Region="Americas",
            Subregion="Caribbean",
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=11,
            CountryName="Argentina",
            FormalName="Argentine Republic",
            IsoAlpha3Code="ARG",
            IsoNumericCode="32",
            CountryType="UN Member State",
            LatestRecordedPopulation=42550127,
            Continent="South America",
            Region="Americas",
            Subregion="South America",
            LastEditedBy="15",
            ValidFrom=datetime(2015, 7, 1, 16, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=12,
            CountryName="Armenia",
            FormalName="Republic of Armenia",
            IsoAlpha3Code="ARM",
            IsoNumericCode="51",
            CountryType="UN Member State",
            LatestRecordedPopulation=2967004,
            Continent="Asia",
            Region="Asia",
            Subregion="Western Asia",
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=15,
            CountryName="Australia",
            FormalName="Commonwealth of Australia",
            IsoAlpha3Code="AUS",
            IsoNumericCode="36",
            CountryType="UN Member State",
            LatestRecordedPopulation=22997671,
            Continent="Oceania",
            Region="Oceania",
            Subregion="Australia and New Zealand",
            LastEditedBy="15",
            ValidFrom=datetime(2015, 7, 1, 16, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
        Row(
            CountryID=16,
            CountryName="Austria",
            FormalName="Republic of Austria",
            IsoAlpha3Code="AUT",
            IsoNumericCode="40",
            CountryType="UN Member State",
            LatestRecordedPopulation=8210281,
            Continent="Europe",
            Region="Europe",
            Subregion="Western Europe",
            LastEditedBy="1",
            ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
            ValidTo=datetime(9999, 12, 31, 23, 59, 59),
        ),
    ]
    
    # Create DataFrame with the sample data
    df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)
    
    # Write the DataFrame to create the table with initial data
    target_lakehouse.write_to_table(
        df=df_with_data, table_name="countries", schema_name=""
    )
    

du.run_once(script_to_execute, "002_application_countries_table_create","baf5c8950c26")

def script_to_execute():
    print("Script block is empty. No action taken.")






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📇 Print the execution log

# CELL ********************



du.print_log() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ If we make it to the end return a successful result

# CELL ********************



notebookutils.mssparkutils.notebook.exit("success")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

