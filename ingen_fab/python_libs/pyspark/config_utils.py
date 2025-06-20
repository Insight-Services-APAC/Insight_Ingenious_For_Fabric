from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType, LongType
from datetime import datetime
from pyspark.sql import SparkSession  # type: ignore # noqa: F401
from pyspark.sql.types import StructType  # type: ignore # noqa: F401
from typing import List, Callable, Optional
import inspect, hashlib
from dataclasses import dataclass, asdict
from notebookutils import mssparkutils  # type: ignore # noqa: F401

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
    
    
    def __init__(self, config_workspace_id: str, config_lakehouse_id: str) -> None:
        self.fabric_environments_table_uri = (
            f"abfss://{config_workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{config_lakehouse_id}/Tables/config_fabric_environments"
        )
        self._configs: dict[string, any] = {}

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
        
        
        df = spark.createDataFrame(
            data=data, schema=config_utils.config_schema()
        )

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

