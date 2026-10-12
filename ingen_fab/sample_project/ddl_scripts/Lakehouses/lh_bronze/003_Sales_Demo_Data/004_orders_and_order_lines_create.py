from pyspark.sql import functions as F

# DDL script: orders and order_lines, generated and deterministic like customers and
# stock_items. Orders span two calendar years; every order has one to five lines. Change the
# count to scale the demo up or down (order_lines follows: about three lines per order).

ORDERS = 50000
FIRST_ORDER_DATE = "2024-01-01"
ORDER_DAYS = 731  # 2024-01-01 .. 2025-12-31

spark_session = target_lakehouse.spark
customer_count = target_lakehouse.read_table(table_name="customers").count()
stock_items = target_lakehouse.read_table(table_name="stock_items").select("StockItemID", "UnitPrice", "TaxRate")
stock_item_count = stock_items.count()


def bucket(seed, modulo, column="id"):
    """A repeatable pseudo-random integer in [0, modulo) for each row."""
    return F.pmod(F.xxhash64(F.col(column), F.lit(seed)), F.lit(modulo))


# --- orders ----------------------------------------------------------------------------------
orders = (
    spark_session.range(1, ORDERS + 1)
    .select(
        F.col("id").cast("int").alias("OrderID"),
        (bucket("customer", customer_count) + 1).cast("int").alias("CustomerID"),
        (bucket("salesperson", 20) + 1).cast("int").alias("SalespersonPersonID"),
        F.date_add(F.lit(FIRST_ORDER_DATE).cast("date"), bucket("date", ORDER_DAYS).cast("int")).alias("OrderDate"),
        (bucket("backorder", 100) < 4).alias("IsUndersupplyBackordered"),
        (bucket("lines", 5) + 1).cast("int").alias("LineCount"),
        (bucket("lead", 7) + 1).cast("int").alias("LeadDays"),
    )
    .withColumn("ExpectedDeliveryDate", F.expr("date_add(OrderDate, LeadDays)"))
)
target_lakehouse.write_to_table(
    df=orders.select(
        "OrderID", "CustomerID", "SalespersonPersonID", "OrderDate", "ExpectedDeliveryDate",
        "IsUndersupplyBackordered", F.lit("1").alias("LastEditedBy"),
    ),
    table_name="orders",
    schema_name="",
)
print(f"orders: {ORDERS} rows for {customer_count} customers")

# --- order lines: one row per (order, line number) -----------------------------------------
lines = (
    orders.select("OrderID", F.explode(F.sequence(F.lit(1), F.col("LineCount"))).alias("LineNumber"))
    .withColumn("line_key", F.col("OrderID") * 10 + F.col("LineNumber"))
    .withColumn("StockItemID", (bucket("item", stock_item_count, "line_key") + 1).cast("int"))
    .withColumn("Quantity", (bucket("quantity", 24, "line_key") + 1).cast("int"))
    .join(F.broadcast(stock_items), on="StockItemID", how="inner")
    .select(
        F.col("line_key").cast("int").alias("OrderLineID"),
        "OrderID",
        "LineNumber",
        "StockItemID",
        "Quantity",
        "UnitPrice",
        "TaxRate",
        F.lit("1").alias("LastEditedBy"),
    )
)
target_lakehouse.write_to_table(df=lines, table_name="order_lines", schema_name="")
print(f"order_lines: {target_lakehouse.read_table(table_name='order_lines').count()} rows")
