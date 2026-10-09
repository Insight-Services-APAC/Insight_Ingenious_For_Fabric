from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# DDL script: the state_provinces table, the missing link between cities and countries.
# cities.StateProvinceID -> state_provinces.StateProvinceID, state_provinces.CountryID ->
# countries.CountryID. Every CountryID used here exists in the countries table.

schema = StructType(
    [
        StructField("StateProvinceID", IntegerType(), nullable=True),
        StructField("StateProvinceCode", StringType(), nullable=True),
        StructField("StateProvinceName", StringType(), nullable=True),
        StructField("CountryID", IntegerType(), nullable=True),
        StructField("SalesTerritory", StringType(), nullable=True),
        StructField("LatestRecordedPopulation", LongType(), nullable=True),
        StructField("LastEditedBy", StringType(), nullable=True),
        StructField("ValidFrom", TimestampType(), nullable=True),
        StructField("ValidTo", TimestampType(), nullable=True),
    ]
)

# (StateProvinceID, code, name, CountryID, sales territory, population)
state_provinces = [
    (1, "AL", "Alabama", 230, "Southeast", 4779736),
    (2, "AK", "Alaska", 230, "Far West", 710231),
    (3, "AZ", "Arizona", 230, "Southwest", 6392017),
    (4, "AR", "Arkansas", 230, "Southeast", 2915918),
    (5, "CA", "California", 230, "Far West", 37253956),
    (6, "CO", "Colorado", 230, "Rocky Mountain", 5029196),
    (7, "FL", "Florida", 230, "Southeast", 18801310),
    (8, "NY", "New York", 230, "Mideast", 19378102),
    (9, "TX", "Texas", 230, "Southwest", 25145561),
    (10, "WA", "Washington", 230, "Far West", 6724540),
    (11, "NSW", "New South Wales", 15, "Oceania", 8072163),
    (12, "VIC", "Victoria", 15, "Oceania", 6503491),
    (13, "QLD", "Queensland", 15, "Oceania", 5156138),
    (14, "BA", "Buenos Aires", 11, "South America", 17541141),
    (15, "CBA", "Cordoba", 11, "South America", 3978984),
    (16, "TR", "Tirana County", 3, "Europe", 912190),
    (17, "ALG", "Algiers Province", 4, "Africa", 2988145),
    (18, "ER", "Yerevan", 12, "Asia", 1092800),
]

rows = [
    Row(
        StateProvinceID=sid,
        StateProvinceCode=code,
        StateProvinceName=name,
        CountryID=country_id,
        SalesTerritory=territory,
        LatestRecordedPopulation=population,
        LastEditedBy="1",
        ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
        ValidTo=datetime(9999, 12, 31, 23, 59, 59),
    )
    for sid, code, name, country_id, territory, population in state_provinces
]

df = target_lakehouse.spark.createDataFrame(rows, schema)
target_lakehouse.write_to_table(df=df, table_name="state_provinces", schema_name="")
print(f"state_provinces: {df.count()} rows")
