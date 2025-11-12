from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
import datetime

# DDL script to add 'HappinessIndex' column to 'countries' table with default value 5

# Step 1: Read the existing table data
print("Step 1: Reading existing table data...")
existing_df = target_lakehouse.read_table(table_name="countries")

# Step 2: Rename the existing table to backup
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
backup_table_name = f"countries_backup_{timestamp}"

print(f"Step 2: Renaming existing table to {backup_table_name}...")
target_lakehouse.write_to_table(
    df=existing_df,
    table_name=backup_table_name,
    schema_name="",
    mode="overwrite"
)

# Step 3: Transform the data - add HappinessIndex column with default value 5
print("Step 3: Adding 'HappinessIndex' column with default value 5...")
df_with_happiness_index = existing_df.withColumn("HappinessIndex", lit(5).cast(IntegerType()))

options: dict[str, str] = {
"mergeSchema": "true"
}

# Step 4: Write the transformed data back to the original table name
print("Step 4: Writing data back to original table...")
target_lakehouse.write_to_table(
    df=df_with_happiness_index,
    table_name="countries",
    schema_name="",
    mode="overwrite",
    options=options
)

# Step 5: Backup table retained for safety
print("Step 5: Backup table retained for safety.")
print(f"Backup table '{backup_table_name}' has been created and retained.")
print("HappinessIndex column addition completed successfully!")

# Verification: Show the schema of the updated table
print("\nVerification - New table schema:")
updated_df = target_lakehouse.read_table(table_name="countries")
updated_df.printSchema()

print(f"\nSample data from updated table:")
updated_df.show(5)

print(f"\nCount of rows with HappinessIndex = 5:")
happiness_count = updated_df.filter(updated_df.HappinessIndex == 5).count()
total_count = updated_df.count()
print(f"Rows with HappinessIndex = 5: {happiness_count} out of {total_count} total rows")