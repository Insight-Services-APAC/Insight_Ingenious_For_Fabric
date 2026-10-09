from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# DDL script: more cities, spread over every state/province of state_provinces, so the
# geography chain city -> state/province -> country covers six countries. Appended to the
# existing cities table (same columns); the first ten cities keep their rows.

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

# (CityID, name, StateProvinceID, population)
cities = [
    (101, "Birmingham", 1, 212237),
    (102, "Anchorage", 2, 291826),
    (103, "Phoenix", 3, 1445632),
    (104, "Tucson", 3, 520116),
    (105, "Little Rock", 4, 193524),
    (106, "Los Angeles", 5, 3792621),
    (107, "San Francisco", 5, 805235),
    (108, "San Diego", 5, 1307402),
    (109, "Denver", 6, 600158),
    (110, "Miami", 7, 399457),
    (111, "Orlando", 7, 238300),
    (112, "New York", 8, 8175133),
    (113, "Buffalo", 8, 261310),
    (114, "Houston", 9, 2099451),
    (115, "Dallas", 9, 1197816),
    (116, "Austin", 9, 790390),
    (117, "Seattle", 10, 608660),
    (118, "Sydney", 11, 5231147),
    (119, "Newcastle", 11, 322278),
    (120, "Melbourne", 12, 4917750),
    (121, "Geelong", 12, 253269),
    (122, "Brisbane", 13, 2514184),
    (123, "Gold Coast", 13, 679127),
    (124, "La Plata", 14, 765378),
    (125, "Mar del Plata", 14, 614350),
    (126, "Cordoba", 15, 1391000),
    (127, "Tirana", 16, 418495),
    (128, "Algiers", 17, 2364230),
    (129, "Yerevan", 18, 1075800),
]

rows = [
    Row(
        CityID=city_id,
        CityName=name,
        StateProvinceID=state_province_id,
        LatestRecordedPopulation=population,
        LastEditedBy="1",
        ValidFrom=datetime(2013, 1, 1, 0, 0, 0),
        ValidTo=datetime(9999, 12, 31, 23, 59, 59),
    )
    for city_id, name, state_province_id, population in cities
]

new_cities = target_lakehouse.spark.createDataFrame(rows, schema)
existing = target_lakehouse.read_table(table_name="cities")
# keep the script safe to rerun: only cities that are not there yet are appended
to_add = new_cities.join(existing.select("CityID"), on="CityID", how="left_anti")
target_lakehouse.write_to_table(df=to_add, table_name="cities", schema_name="", mode="append")
print(f"cities: {target_lakehouse.read_table(table_name='cities').count()} rows")
