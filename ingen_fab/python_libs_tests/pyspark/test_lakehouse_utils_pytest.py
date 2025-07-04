import datetime

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
def utils():
    # Ideally, fetch these from env or config
    return lakehouse_utils("your-workspace-id", "your-lakehouse-id")


@pytest.fixture
def sales_schema():
    return StructType(
        [
            StructField("sale_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("amount", IntegerType(), True),
            StructField("sale_date", StringType(), True),
        ]
    )


@pytest.fixture
def sales_data():
    return [
        (1, "Laptop", "John Doe", 1200, "2024-06-01 10:00:00"),
        (2, "Mouse", "Jane Smith", 25, "2024-06-02 14:30:00"),
        (3, "Keyboard", "Bob Johnson", 75, "2024-06-03 09:15:00"),
        (4, "Monitor", "Alice Brown", 300, "2024-06-04 16:45:00"),
        (5, "Headphones", "Carol Wilson", 150, "2024-06-05 11:20:00"),
    ]


@pytest.fixture
def customers_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("signup_date", TimestampType(), True),
        ]
    )


@pytest.fixture
def customers_data():
    return [
        (1, "John Doe", "john.doe@example.com", datetime.datetime(2023, 1, 15, 10, 0)),
        (
            2,
            "Jane Smith",
            "jane.smith@example.com",
            datetime.datetime(2023, 2, 20, 14, 30),
        ),
        (
            3,
            "Alice Brown",
            "alice.brown@example.com",
            datetime.datetime(2023, 3, 25, 9, 15),
        ),
    ]


@pytest.fixture
def orders_schema():
    return StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("total_amount", IntegerType(), True),
        ]
    )


@pytest.fixture
def orders_data():
    return [
        (101, 1, datetime.datetime(2023, 4, 10, 11, 0), 1200),
        (102, 2, datetime.datetime(2023, 4, 15, 16, 45), 300),
        (103, 3, datetime.datetime(2023, 4, 20, 13, 20), 450),
    ]


@pytest.fixture
def products_schema():
    return StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", IntegerType(), True),
        ]
    )


@pytest.fixture
def products_data():
    return [
        (1, "Laptop", "Electronics", 1200),
        (2, "Mouse", "Accessories", 25),
        (3, "Keyboard", "Accessories", 75),
    ]


def test_lakehouse_uri(utils):
    uri = utils.lakehouse_tables_uri()
    assert isinstance(uri, str), "lakehouse_tables_uri() did not return a string"
    assert uri, "lakehouse_tables_uri() returned an empty value"


def test_write_and_read_table(utils, sales_schema, sales_data):
    table_name = "sales_data"
    sales_df = utils.spark.createDataFrame(sales_data, sales_schema)
    utils.write_to_table(sales_df, table_name, mode="overwrite")
    assert utils.check_if_table_exists(table_name), (
        f"Table {table_name} was not created"
    )
    read_df = utils.get_table_row_count(table_name)
    assert read_df == len(sales_data), f"Row count mismatch: expected {len(sales_data)}"
    # Pretty print a sample row for debugging
    print(f"\n✅ Successfully wrote/read table '{table_name}'")


@pytest.mark.parametrize(
    "option,expected",
    [
        ({"mergeSchema": "true", "overwriteSchema": "true"}, True),
    ],
)
def test_write_with_custom_options(utils, sales_schema, sales_data, option, expected):
    table_name = "sales_data"
    sales_df = utils.spark.createDataFrame(sales_data, sales_schema)
    utils.write_to_table(sales_df, table_name, options=option)
    assert utils.check_if_table_exists(table_name) is expected, (
        f"Custom options {option} did not yield expected result"
    )


def test_append_mode(utils, sales_schema):
    table_name = "sales_data"
    additional_data = [(6, "Tablet", "David Lee", 450, "2024-06-06 13:10:00")]
    additional_df = utils.spark.createDataFrame(additional_data, sales_schema)

    pre_count = utils.get_table_row_count(table_name)
    utils.write_to_table(additional_df, table_name, mode="append")
    post_count = utils.get_table_row_count(table_name)
    assert post_count == pre_count + 1, (
        f"Append mode failed: before={pre_count}, after={post_count}"
    )


def test_list_tables(utils):
    tables = utils.list_tables()
    assert isinstance(tables, list), "list_tables() did not return a list"
    assert any("sales_data" in t for t in tables), (
        "sales_data not found in listed tables"
    )


def test_multiple_table_management(
    utils,
    customers_schema,
    customers_data,
    orders_schema,
    orders_data,
    products_schema,
    products_data,
):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    orders_df = utils.spark.createDataFrame(orders_data, orders_schema)
    products_df = utils.spark.createDataFrame(products_data, products_schema)
    utils.write_to_table(customers_df, "customers", mode="overwrite")
    utils.write_to_table(orders_df, "orders", mode="overwrite")
    utils.write_to_table(products_df, "products", mode="overwrite")
    assert utils.check_if_table_exists("customers")
    assert utils.check_if_table_exists("orders")
    assert utils.check_if_table_exists("products")


def test_overwrite_and_append_modes(utils, customers_schema, customers_data):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    utils.write_to_table(customers_df, "customers", mode="overwrite")
    pre_count = utils.get_table_row_count("customers")
    utils.write_to_table(customers_df, "customers", mode="overwrite")
    post_count = utils.get_table_row_count("customers")
    assert post_count == len(customers_data)
    # Append
    utils.write_to_table(customers_df, "customers", mode="append")
    appended_count = utils.get_table_row_count("customers")
    assert appended_count == post_count + len(customers_data)


def test_error_mode(utils, customers_schema, customers_data):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    utils.write_to_table(customers_df, "customers", mode="overwrite")
    with pytest.raises(Exception):
        utils.write_to_table(customers_df, "customers", mode="error")


def test_custom_write_options(utils, customers_schema, customers_data):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    options = {
        "mergeSchema": "true",
        "overwriteSchema": "true",
        "maxRecordsPerFile": "10000",
        "replaceWhere": "signup_date >= '2023-01-01'",
    }
    utils.write_to_table(customers_df, "customers", options=options)
    assert utils.check_if_table_exists("customers")


def test_table_existence_checking(utils, customers_schema, customers_data):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    table_path = "customers"
    if not utils.check_if_table_exists(table_path):
        utils.write_to_table(customers_df, "customers")
    assert utils.check_if_table_exists(table_path)
    # Now append
    utils.write_to_table(customers_df, "customers", mode="append")
    assert utils.check_if_table_exists(table_path)

def test_table_optimise(utils, customers_schema, customers_data):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    table_path = "customers"
    if not utils.check_if_table_exists(table_path):
        utils.write_to_table(customers_df, "customers")
    # check command runs
    utils.optimise_table(table_path)
    # mutate table, creating more small parquet files
    for _ in range(10):
        utils.write_to_table(customers_df, "customers")
    utils.optimise_table(table_path)


def test_cleanup_drop_all_tables(utils, customers_schema, customers_data):
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)
    utils.write_to_table(customers_df, "customers", mode="overwrite")
    # Drop all tables (WARNING: this will remove all tables in the lakehouse)
    utils.drop_all_tables()
    # After drop, table should not exist
    assert not utils.check_if_table_exists("customers")
