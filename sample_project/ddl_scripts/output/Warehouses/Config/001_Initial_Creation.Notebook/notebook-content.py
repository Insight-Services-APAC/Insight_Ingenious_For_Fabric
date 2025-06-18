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



# THESE SHOULD BE SET BY THE SCHEDULER 
fabric_environment = "development"  # fabric environment, e.g. development, test, production
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"
target_lakehouse_config_prefix = ""
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








# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instantiate the Helper Classes

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)
configs = cu.get_configs_as_object(fabric_environment)
target_workspace_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id")
target_lakehouse_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
lu = lakehouse_utils(target_workspace_id, target_lakehouse_id)
du = ddl_utils(target_workspace_id, target_lakehouse_id)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 001_config_parquet_loads_create.sql

# CELL ********************

guid = ""
def work():
    sql = """


CREATE TABLE config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_id VARCHAR(300) NOT NULL,
    target_partition_columns VARCHAR(300) NOT NULL,
    target_sort_columns VARCHAR(300) NOT NULL,
    target_replace_where VARCHAR(300) NOT NULL,
    cfg_source_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_source_lakehouse_id VARCHAR(300) NOT NULL,
    cfg_source_file_path VARCHAR(300) NOT NULL,
    source_file_path VARCHAR(300) NOT NULL,
    source_file_name VARCHAR(300) NOT NULL,
    cfg_legacy_synapse_connection_name VARCHAR(300) NOT NULL,
    synapse_source_schema_name VARCHAR(300) NOT NULL,
    synapse_source_table_name VARCHAR(300) NOT NULL,
    synapse_partition_clause VARCHAR(1000) NOT NULL,
    execution_group INT NOT NULL,
    active_yn VARCHAR(1) NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"001_config_parquet_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 002_config_synapse_loads_create.sql

# CELL ********************

guid = ""
def work():
    sql = """


CREATE TABLE config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    execution_group INT NOT NULL,
    active_yn VARCHAR(1) NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"002_config_synapse_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 003_log_parquet_loads_create.sql

# CELL ********************

guid = ""
def work():
    sql = """


CREATE TABLE log.log_parquet_loads (
    execution_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_id VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000),
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"003_log_parquet_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 004_log_synapse_loads_create.sql

# CELL ********************

guid = ""
def work():
    sql = """


CREATE TABLE log.log_synapse_extracts (
    execution_id VARCHAR(300) NOT NULL,
    cfg_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    extract_file_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"004_log_synapse_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 005_config_synapse_loads_insert.sql

# CELL ********************

guid = ""
def work():
    sql = """


INSERT INTO config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name,
    source_schema_name,
    source_table_name,
    partition_clause,
    execution_group,
    active_yn
)
VALUES
('legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'where year = @year and month = @month', 1, 'Y');


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"005_config_synapse_loads_insert", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 006_config_parquet_loads_insert.sql

# CELL ********************

guid = ""
def work():
    sql = """


INSERT INTO config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id,
    cfg_target_lakehouse_id,
    target_partition_columns,
    target_sort_columns,
    target_replace_where,
    cfg_source_lakehouse_workspace_id,
    cfg_source_lakehouse_id,
    cfg_source_file_path,
    source_file_path,
    source_file_name,
    cfg_legacy_synapse_connection_name,
    synapse_source_schema_name,
    synapse_source_table_name,
    synapse_partition_clause,
    execution_group,
    active_yn
)
VALUES
('edw_workspace_id', 'edw_lakehouse_id', '', '', '', 'edw_lakehouse_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('edw_workspace_id', 'edw_lakehouse_id', 'year, month', 'year, month', 'WHERE year = @year AND month = @month', 'edw_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'WHERE year = @year AND month = @month', 1, 'Y');


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"006_config_parquet_loads_insert", guid)







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



from notebookutils import mssparkutils
mssparkutils.notebook.exit("success")



