"""
Pytest-based tests for lakehouse_utils.

These tests require a real Spark/Fabric environment and will perform real writes/reads.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


@pytest.fixture(scope="module")
def utils() -> lakehouse_utils:
    workspace_id = "your-workspace-id-here"  # Replace with actual workspace ID
    lakehouse_id = "your-lakehouse-id-here"  # Replace with actual lakehouse ID
    return lakehouse_utils(workspace_id, lakehouse_id)

@pytest.fixture
def sales_schema() -> StructType:
    return StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True),
    ])

@pytest.fixture
def sales_data() -> list[tuple]:
    return [
        (1, "Laptop", "John Doe", 1200, "2024-06-01 10:00:00"),
        (2, "Mouse", "Jane Smith", 25, "2024-06-02 14:30:00"),
        (3, "Keyboard", "Bob Johnson", 75, "2024-06-03 09:15:00"),
        (4, "Monitor", "Alice Brown", 300, "2024-06-04 16:45:00"),
        (5, "Headphones", "Carol Wilson", 150, "2024-06-05 11:20:00"),
    ]

def test_lakehouse_uri(utils: lakehouse_utils) -> None:
    uri = utils.lakehouse_tables_uri()
    assert isinstance(uri, str)
    assert uri

def test_write_and_read_table(utils: lakehouse_utils, sales_schema: StructType, sales_data: list[tuple]) -> None:
    table_name = "sales_data"
    sales_df = utils.spark.createDataFrame(sales_data, sales_schema)
    utils.write_to_table(sales_df, table_name, mode="overwrite")
    assert utils.check_if_table_exists(table_name)
    read_df = utils.spark.read.format("delta").load(utils.lakehouse_tables_uri() + table_name)
    assert read_df.count() == len(sales_data)
    # Optionally check schema, data, etc.

def test_write_with_custom_options(utils: lakehouse_utils, sales_schema: StructType, sales_data: list[tuple]) -> None:
    table_name = "sales_data"
    sales_df = utils.spark.createDataFrame(sales_data, sales_schema)
    custom_options = {"mergeSchema": "true", "overwriteSchema": "true"}
    utils.write_to_table(sales_df, table_name, options=custom_options)
    assert utils.check_if_table_exists(table_name)

def test_append_mode(utils: lakehouse_utils, sales_schema: StructType) -> None:
    table_name = "sales_data"
    additional_data = [
        (6, "Tablet", "David Lee", 450, "2024-06-06 13:10:00")
    ]
    additional_df = utils.spark.createDataFrame(additional_data, sales_schema)
    pre_count = utils.spark.read.format("delta").load(utils.lakehouse_tables_uri() + table_name).count()
    utils.write_to_table(additional_df, table_name, mode="append")
    post_count = utils.spark.read.format("delta").load(utils.lakehouse_tables_uri() + table_name).count()
    assert post_count == pre_count + 1

def test_list_tables(utils: lakehouse_utils) -> None:
    tables = utils.list_tables()
    assert isinstance(tables, list)
    assert any("sales_data" in t for t in tables)
