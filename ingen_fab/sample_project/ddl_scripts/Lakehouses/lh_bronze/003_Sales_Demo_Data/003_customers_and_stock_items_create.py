from pyspark.sql import functions as F

# DDL script: customers and stock_items, generated. The rows are deterministic (every value is
# a function of the row id), so every environment and every rerun gets identical data. Change
# the two counts to scale the demo up or down.

CUSTOMERS = 2000
STOCK_ITEMS = 200

spark_session = target_lakehouse.spark


def pick(column, values):
    """A value of `values` chosen by a column holding a non-negative integer."""
    return F.element_at(F.array(*[F.lit(v) for v in values]), (column % len(values)).cast("int") + 1)


def bucket(seed, modulo):
    """A repeatable pseudo-random integer in [0, modulo) for each row id."""
    return F.pmod(F.xxhash64(F.col("id"), F.lit(seed)), F.lit(modulo))


# --- customers -------------------------------------------------------------------------------
city_ids = [r.CityID for r in target_lakehouse.read_table(table_name="cities").select("CityID").orderBy("CityID").collect()]
categories = ["Novelty Shop", "Supermarket", "Computer Store", "Gift Store", "Corporate"]
buying_groups = ["Tailspin Toys", "Wingtip Toys", "Independent"]

customers = (
    spark_session.range(1, CUSTOMERS + 1)
    .select(
        F.col("id").cast("int").alias("CustomerID"),
        F.concat(F.lit("Customer "), F.lpad(F.col("id").cast("string"), 5, "0")).alias("CustomerName"),
        pick(bucket("category", 1000), categories).alias("CustomerCategory"),
        pick(bucket("group", 1000), buying_groups).alias("BuyingGroup"),
        pick(bucket("city", 100000), city_ids).cast("int").alias("DeliveryCityID"),
        (F.lit(1000) + bucket("credit", 60) * 1000).cast("decimal(18,2)").alias("CreditLimit"),
        F.date_add(F.lit("2018-01-01").cast("date"), bucket("opened", 2190).cast("int")).alias("AccountOpenedDate"),
        (bucket("hold", 100) < 3).alias("IsOnCreditHold"),
        F.concat(F.lit("customer"), F.col("id").cast("string"), F.lit("@example.com")).alias("EmailAddress"),
        F.lit("1").alias("LastEditedBy"),
    )
)
target_lakehouse.write_to_table(df=customers, table_name="customers", schema_name="")
print(f"customers: {customers.count()} rows over {len(city_ids)} cities")

# --- stock items -----------------------------------------------------------------------------
brands = ["Northwind", "Contoso", "Fabrikam", "Adventure Works", "Litware"]
colours = ["Black", "Blue", "Red", "White", "Yellow", "Grey"]
groups = ["Novelty Items", "Clothing", "Mugs", "Toys", "Packaging Materials", "Computing Novelties"]

stock_items = (
    spark_session.range(1, STOCK_ITEMS + 1)
    .select(
        F.col("id").cast("int").alias("StockItemID"),
        F.concat(F.lit("Stock item "), F.lpad(F.col("id").cast("string"), 4, "0")).alias("StockItemName"),
        pick(bucket("brand", 1000), brands).alias("Brand"),
        pick(bucket("colour", 1000), colours).alias("ColorName"),
        pick(bucket("group", 1000), groups).alias("StockGroup"),
        ((F.lit(200) + bucket("price", 19800)) / 100).cast("decimal(18,2)").alias("UnitPrice"),
        pick(bucket("tax", 100), [10.0, 15.0]).cast("decimal(5,2)").alias("TaxRate"),
        ((F.lit(50) + bucket("weight", 4950)) / 1000).cast("decimal(18,3)").alias("TypicalWeightPerUnit"),
        (bucket("chiller", 100) < 5).alias("IsChillerStock"),
        F.lit("1").alias("LastEditedBy"),
    )
)
target_lakehouse.write_to_table(df=stock_items, table_name="stock_items", schema_name="")
print(f"stock_items: {stock_items.count()} rows")
