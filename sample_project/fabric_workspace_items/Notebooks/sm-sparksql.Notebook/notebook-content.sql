-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "23ba8378-7884-4ac7-bab5-10e11af43d6c",
-- META       "default_lakehouse_name": "config",
-- META       "default_lakehouse_workspace_id": "50fbcab0-7d56-46f7-90f6-80ceb00ac86d",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "23ba8378-7884-4ac7-bab5-10e11af43d6c"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SHOW TBLPROPERTIES LH.bkp_fct_food_sales_allowance_daily_v

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

create table LH.abc (
	abc_id string,
	abc_address string,
	abc_date timestamp
)
USING PARQUET
TBLPROPERTIES(
	'delta.autoOptimize.optimizeWrite'    = 'true',
  	'delta.autoOptimize.autoCompact'      = 'true',
  	'delta.parquet.vorder.default'        = 'true'
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

show TBLPROPERTIES LH.abc 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from config_fabric_environments

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from config_parquet_loads

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from config_synapse_extracts

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from log_synapse_extracts;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
