from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    DateType,
    TimestampType,
    DecimalType,
    IntegerType    
)

# DDL script for creating table 'cities' in lakehouse 'lh_silver'

schema = StructType(
    [
        StructField('CityID', IntegerType(), True),
        StructField('CityName', StringType(), True),
        StructField('StateProvinceID', IntegerType(), True),
        StructField('LatestRecordedPopulation', IntegerType(), True),
        StructField('LastEditedBy', StringType(), True),
        StructField('ValidFrom', TimestampType(), True),
        StructField('ValidTo', TimestampType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="cities", schema_name=""
)
