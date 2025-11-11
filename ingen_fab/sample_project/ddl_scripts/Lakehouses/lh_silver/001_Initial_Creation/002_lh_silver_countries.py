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

# DDL script for creating table 'countries' in lakehouse 'lh_silver'

schema = StructType(
    [
        StructField('CountryID', IntegerType(), True),
        StructField('CountryName', StringType(), True),
        StructField('FormalName', StringType(), True),
        StructField('IsoAlpha3Code', StringType(), True),
        StructField('IsoNumericCode', StringType(), True),
        StructField('CountryType', StringType(), True),
        StructField('LatestRecordedPopulation', LongType(), True),
        StructField('Continent', StringType(), True),
        StructField('Region', StringType(), True),
        StructField('Subregion', StringType(), True),
        StructField('LastEditedBy', StringType(), True),
        StructField('ValidFrom', TimestampType(), True),
        StructField('ValidTo', TimestampType(), True),
        StructField('HappinessIndex', IntegerType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="countries", schema_name=""
)
