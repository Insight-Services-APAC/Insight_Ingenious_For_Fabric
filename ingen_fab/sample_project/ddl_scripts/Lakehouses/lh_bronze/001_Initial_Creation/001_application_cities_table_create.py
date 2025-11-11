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