# DDL Script Organization

[Home](../index.md) > [User Guide](index.md) > DDL Script Organization

This guide covers best practices for organizing, writing, and maintaining DDL (Data Definition Language) scripts in the Ingenious Fabric Accelerator.

## Overview

DDL scripts define the structure of your data platform - tables, schemas, views, and stored procedures. Proper organization ensures maintainability, version control, and reliable deployments across environments.

## Directory Structure

The Ingenious Fabric Accelerator uses a hierarchical folder structure to organize DDL scripts. Here's the structure from the sample project (`ingen_fab/sample_project/ddl_scripts/`):

```
ddl_scripts/
├── Lakehouses/
│   ├── lh_bronze/
│   │   ├── 001_Initial_Creation/
│   │   │   ├── 001_application_cities_table_create.py
│   │   │   └── 002_application_countries_table_create.py
│   │   └── 002_Change/
│   │       └── 001_countries_add_happiness_index.py
│   ├── lh_silver/
│   │   └── 001_Initial_Creation/
│   │       ├── 001_lh_silver_cities.py
│   │       └── 002_lh_silver_countries.py
│   └── lh_gold/
│       └── 001_Initial_Creation/
│           └── 001_lh_gold_dim_cities.py
└── Warehouses/
    └── wh_gold/
        └── 001_Initial_Creation/
            ├── 001_wh_gold_create_schema.sql
            └── 002_wh_gold_create_cities_view.sql
```

### Structure Principles

**Top Level: Target Type**
- `Lakehouses/`: Scripts for lakehouse objects (Delta tables, views)
- `Warehouses/`: Scripts for warehouse objects (SQL tables, views, procedures)

**Second Level: Lakehouse/Warehouse Name**
- Matches the actual lakehouse or warehouse name in Fabric
- Examples from sample project: `lh_bronze`, `lh_silver`, `lh_gold`, `wh_gold`

**Third Level: Release/Change Groups**
- Numbered folders representing releases or logical groupings
- Format: `###_Descriptive_Name`
- Examples from sample project: `001_Initial_Creation`, `002_Change`

**Fourth Level: DDL Script Files**
- Individual script files within each release folder
- Executed in alphanumeric order
- Format: `###_descriptive_name.py` or `###_descriptive_name.sql`

## Naming Conventions

### Folder Naming

**Pattern:** `###_Descriptive_Name`

- **Prefix with numbers** (001, 002, 003, etc.) to control execution order
- **Use PascalCase or Snake_Case** for readability
- **Be descriptive** about the purpose or release

**Examples:**
```
✅ Good (from sample project):
   001_Initial_Creation
   002_Change
   003_Add_Customer_Tables
   004_Performance_Indexes

❌ Avoid:
   setup
   changes
   new_stuff
   temp
```

### File Naming

**Pattern:** `###_descriptive_action.ext`

- **Prefix with numbers** (001, 002, 003, etc.) for execution order within folder
- **Use snake_case** for the descriptive part
- **Be specific** about what the script does
- **Use appropriate extension**: `.py` for Python/PySpark, `.sql` for SQL

**Examples:**
```
✅ Good (from sample project):
   001_application_cities_table_create.py
   002_application_countries_table_create.py
   001_countries_add_happiness_index.py
   001_wh_gold_create_schema.sql
   002_wh_gold_create_cities_view.sql

❌ Avoid:
   tables.py
   script1.sql
   new_file.py
   temp_test.sql
```

## Script Types and Extensions

### Python Scripts (`.py`)

Use for **Lakehouse DDL** with PySpark/Delta Lake operations:

**Example 1: Creating a table with sample data** (from `lh_bronze/001_Initial_Creation/001_application_cities_table_create.py`):

```python
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

# Sample data for the first few rows
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
]

# Create DataFrame with the sample data
df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)

# Write the DataFrame to create the table with initial data
target_lakehouse.write_to_table(
    df=df_with_data, table_name="cities", schema_name=""
)
```

**Example 2: Creating an empty table structure** (from `lh_silver/001_Initial_Creation/001_lh_silver_cities.py`):

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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
```

### SQL Scripts (`.sql`)

Use for **Warehouse DDL** with T-SQL operations:

**Example 1: Creating a schema** (from `wh_gold/001_Initial_Creation/001_wh_gold_create_schema.sql`):

```sql
CREATE SCHEMA DW
```

**Example 2: Creating a view** (from `wh_gold/001_Initial_Creation/002_wh_gold_create_cities_view.sql`):

```sql
CREATE OR ALTER VIEW DW.vDim_Cities
as 
select * from [lh_gold].[dbo].[dim_cities]
```

**More complex example with error handling:**

```sql
-- Create schemas for the data warehouse

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dim')
BEGIN
    EXEC('CREATE SCHEMA dim')
    PRINT '✅ Schema dim created successfully'
END
ELSE
BEGIN
    PRINT 'ℹ️ Schema dim already exists'
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fact')
BEGIN
    EXEC('CREATE SCHEMA fact')
    PRINT '✅ Schema fact created successfully'
END
GO
```

## Best Practices

### 1. Idempotent Operations

**Always write scripts that can be run multiple times safely:**

**Lakehouse (Python):**

The sample project demonstrates idempotency through the use of `target_lakehouse.write_to_table()` which handles table creation safely. For explicit SQL operations:

```python
# ✅ Good: Uses CREATE OR REPLACE or checks existence
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id INT,
    name STRING,
    email STRING,
    created_date TIMESTAMP
)
""")

# ✅ Also good: Using write_to_table (from sample project pattern)
schema = StructType([
    StructField('CustomerID', IntegerType(), True),
    StructField('CustomerName', StringType(), True),
])

empty_df = target_lakehouse.spark.createDataFrame([], schema)
target_lakehouse.write_to_table(
    df=empty_df, table_name="customers", schema_name=""
)
```

**Warehouse (SQL):**

The sample project uses simple `CREATE SCHEMA` and `CREATE OR ALTER VIEW` patterns:

```sql
-- ✅ Good: CREATE SCHEMA is idempotent in Fabric SQL
CREATE SCHEMA DW

-- ✅ Good: CREATE OR ALTER VIEW handles existing views
CREATE OR ALTER VIEW DW.vDim_Cities
AS 
SELECT * FROM [lh_gold].[dbo].[dim_cities]

-- ✅ Good for tables: Check existence first
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimCustomer')
BEGIN
    CREATE TABLE dim.DimCustomer (
        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID INT NOT NULL,
        CustomerName NVARCHAR(100)
    )
END
GO
```

### 2. Error Handling

**Lakehouse (Python):**

The sample project demonstrates safe table operations. For more complex scenarios, add explicit error handling:

```python
# Example with error handling
try:
    schema = StructType([
        StructField('CustomerID', IntegerType(), True),
        StructField('CustomerName', StringType(), True),
        StructField('Email', StringType(), True),
    ])
    
    empty_df = target_lakehouse.spark.createDataFrame([], schema)
    
    target_lakehouse.write_to_table(
        df=empty_df, 
        table_name="customers", 
        schema_name=""
    )
    print("✅ Customers table created successfully")
    
except Exception as e:
    print(f"❌ Failed to create customers table: {e}")
    raise
```

**Warehouse (SQL):**
```sql
BEGIN TRY
    BEGIN TRANSACTION
    
    -- Create table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactSales')
    BEGIN
        CREATE TABLE fact.FactSales (
            SalesKey BIGINT IDENTITY(1,1) PRIMARY KEY,
            DateKey INT NOT NULL,
            CustomerKey INT NOT NULL,
            Amount DECIMAL(18,2)
        )
        PRINT '✅ FactSales table created successfully'
    END
    
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION
    PRINT '❌ Error creating FactSales: ' + ERROR_MESSAGE()
    THROW
END CATCH
GO
```

### 3. Logging and Auditing

**Track script execution with print statements:**

From the sample project pattern in `002_Change/001_countries_add_happiness_index.py`:

```python
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
df_with_happiness_index = existing_df.withColumn(
    "HappinessIndex", 
    lit(5).cast(IntegerType())
)

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

print("✅ Successfully added HappinessIndex column to countries table")
```

### 4. Schema Evolution Pattern

The sample project demonstrates safe schema evolution in `002_Change/001_countries_add_happiness_index.py`:

**Key principles:**
1. Create a timestamped backup table
2. Read existing data
3. Transform with new schema
4. Write back with `mergeSchema: true`

```python
from pyspark.sql.functions import lit
import datetime

# Read existing table
print("Reading existing table data...")
existing_df = target_lakehouse.read_table(table_name="countries")

# Create backup with timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
backup_table_name = f"countries_backup_{timestamp}"

print(f"Creating backup: {backup_table_name}...")
target_lakehouse.write_to_table(
    df=existing_df,
    table_name=backup_table_name,
    schema_name="",
    mode="overwrite"
)

# Add new column
print("Adding new column...")
df_with_new_column = existing_df.withColumn("HappinessIndex", lit(5).cast(IntegerType()))

# Write back with schema merge enabled
options: dict[str, str] = {
    "mergeSchema": "true"
}

target_lakehouse.write_to_table(
    df=df_with_new_column,
    table_name="countries",
    schema_name="",
    mode="overwrite",
    options=options
)

print("✅ Schema evolution completed successfully")
```

### 5. Documentation and Comments

**Document your scripts thoroughly:**

From the sample project patterns:

```python
"""
DDL Script: 001_application_cities_table_create.py
Purpose: Create initial cities table in bronze lakehouse with sample data
Based on: application_cities.csv structure
Author: Data Engineering Team
Date: 2025-01-15
"""

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

# Create and write table
df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)
target_lakehouse.write_to_table(df=df_with_data, table_name="cities", schema_name="")
```

### 6. Organize by Dependency

**Order scripts by dependencies:**

From the sample project structure:

```
Lakehouses/
├── lh_bronze/                          # Raw data first
│   └── 001_Initial_Creation/
│       ├── 001_application_cities_table_create.py
│       └── 002_application_countries_table_create.py
├── lh_silver/                          # Then cleansed data
│   └── 001_Initial_Creation/
│       ├── 001_lh_silver_cities.py
│       └── 002_lh_silver_countries.py
└── lh_gold/                            # Finally aggregated/dimensional
    └── 001_Initial_Creation/
        └── 001_lh_gold_dim_cities.py

Warehouses/
└── wh_gold/
    └── 001_Initial_Creation/
        ├── 001_wh_gold_create_schema.sql      # Schemas first
        └── 002_wh_gold_create_cities_view.sql # Views reference schemas and tables
```

### 7. Separate Concerns

**Keep different types of operations in separate folders:**

From the sample project pattern:

```
ddl_scripts/Lakehouses/lh_bronze/
├── 001_Initial_Creation/          # Initial table creation
│   ├── 001_application_cities_table_create.py
│   └── 002_application_countries_table_create.py
└── 002_Change/                    # Schema changes and updates
    └── 001_countries_add_happiness_index.py

ddl_scripts/Warehouses/wh_gold/
└── 001_Initial_Creation/          # Schema and view creation
    ├── 001_wh_gold_create_schema.sql
    └── 002_wh_gold_create_cities_view.sql
```

**Extended pattern for larger projects:**

```
ddl_scripts/Warehouses/EDW/
├── 001_Initial_Setup/             # Structure creation
│   ├── 001_create_schemas.sql
│   └── 002_create_tables.sql
├── 002_Views/                     # Views
│   └── 001_create_reporting_views.sql
├── 003_Stored_Procedures/         # Procedures
│   └── 001_create_etl_procedures.sql
└── 004_Indexes/                   # Performance tuning
    └── 001_create_indexes.sql
```

## Compilation and Deployment

### Generate Notebooks from DDL Scripts

```bash
# Compile for warehouses
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse

# Compile for lakehouses
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Generated notebooks appear in:
# fabric_workspace_items/ddl_scripts/Lakehouses/lh_bronze/001_Initial_Creation.Notebook
# fabric_workspace_items/ddl_scripts/Lakehouses/lh_silver/001_Initial_Creation.Notebook
# fabric_workspace_items/ddl_scripts/Lakehouses/lh_gold/001_Initial_Creation.Notebook
# fabric_workspace_items/ddl_scripts/Warehouses/wh_gold/001_Initial_Creation.Notebook
```

### Deploy DDL Notebooks

```bash
# Check what will be deployed
ingen_fab deploy check

# Deploy to development
$env:FABRIC_ENVIRONMENT = "development"
ingen_fab deploy deploy
```

### Execution Order

The CLI compiles DDL scripts into notebooks that execute in this order:

1. **Alphabetically by lakehouse/warehouse** (lh_bronze, lh_gold, lh_silver, wh_gold)
2. **Alphabetically by folder** (001_Initial_Creation, then 002_Change, etc.)
3. **Alphabetically by file within folder** (001_application_cities_table_create.py, then 002_application_countries_table_create.py, etc.)
4. **Line by line within each script**

## Version Control Strategies

### Naming for Releases

**Use dated or versioned folders:**

```
ddl_scripts/Warehouses/EDW/
├── 001_Release_2025_01/
│   └── 001_initial_schema.sql
├── 002_Release_2025_02/
│   └── 001_add_customer_columns.sql
└── 003_Release_2025_03/
    └── 001_add_order_tables.sql
```

### Rollback Scripts

**Include rollback scripts when appropriate:**

```
002_Schema_Updates/
├── 001_add_new_columns.py
└── 001_add_new_columns_rollback.py  # Rollback version
```

## Common Patterns

### Pattern: Initial Table Creation with Sample Data

From the sample project (`lh_bronze/001_Initial_Creation/001_application_cities_table_create.py`):

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql import Row
from datetime import datetime

# Define the schema
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

# Sample data
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
]

# Create DataFrame and write to table
df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)
target_lakehouse.write_to_table(
    df=df_with_data, table_name="cities", schema_name=""
)
```

### Pattern: Creating Empty Table Structure

From the sample project (`lh_silver/001_Initial_Creation/001_lh_silver_cities.py`):

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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

# Create empty DataFrame with schema
empty_df = target_lakehouse.spark.createDataFrame([], schema)

# Write to create table structure
target_lakehouse.write_to_table(
    df=empty_df, table_name="cities", schema_name=""
)
```

### Pattern: Schema Evolution with Backup

From the sample project (`lh_bronze/002_Change/001_countries_add_happiness_index.py`):

```python
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
import datetime

# DDL script to add 'HappinessIndex' column to 'countries' table

# Step 1: Read the existing table data
print("Step 1: Reading existing table data...")
existing_df = target_lakehouse.read_table(table_name="countries")

# Step 2: Create backup with timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
backup_table_name = f"countries_backup_{timestamp}"

print(f"Step 2: Creating backup table {backup_table_name}...")
target_lakehouse.write_to_table(
    df=existing_df,
    table_name=backup_table_name,
    schema_name="",
    mode="overwrite"
)

# Step 3: Add new column with default value
print("Step 3: Adding 'HappinessIndex' column with default value 5...")
df_with_new_column = existing_df.withColumn(
    "HappinessIndex", 
    lit(5).cast(IntegerType())
)

# Step 4: Write back with schema merge
options: dict[str, str] = {
    "mergeSchema": "true"
}

print("Step 4: Writing data back to original table...")
target_lakehouse.write_to_table(
    df=df_with_new_column,
    table_name="countries",
    schema_name="",
    mode="overwrite",
    options=options
)

print("✅ Successfully added HappinessIndex column")
```

### Pattern: Warehouse Schema and View Creation

From the sample project (`wh_gold/001_Initial_Creation/`):

**Creating a schema** (`001_wh_gold_create_schema.sql`):
```sql
CREATE SCHEMA DW
```

**Creating a view that references lakehouse tables** (`002_wh_gold_create_cities_view.sql`):
```sql
CREATE OR ALTER VIEW DW.vDim_Cities
AS 
SELECT * FROM [lh_gold].[dbo].[dim_cities]
```

**Extended example with multiple schemas:**
```sql
-- Create multiple schemas for warehouse organization

CREATE SCHEMA stg  -- Staging layer
GO

CREATE SCHEMA dim  -- Dimension tables
GO

CREATE SCHEMA fact -- Fact tables
GO

CREATE SCHEMA rpt  -- Reporting views
GO

PRINT '✅ All schemas created successfully'
```

### Common Issues

**Issue: Scripts not executing in expected order**
- Solution: Verify folder and file numbering (001, 002, 003, etc.)
- Check for typos in folder names

**Issue: DDL compilation fails**
- Solution: Review DDL script syntax
- Check that lakehouse/warehouse names match folder names
- Ensure Python scripts use correct imports

**Issue: Variable replacement not working**
- Solution: Verify variable names match those in `variables.json`
- Check that variables are defined in environment-specific valueSet
- Use correct syntax: `{{variable_name}}`

**Issue: Notebooks not appearing after compilation**
- Solution: Check output directory: `fabric_workspace_items/ddl_scripts/`
- Verify generation mode matches script location (Warehouse vs Lakehouse)
- Review CLI output for errors

### Debugging Scripts

```bash
# Review generated notebook content for lh_bronze
cat fabric_workspace_items/ddl_scripts/Lakehouses/lh_bronze/001_Initial_Creation.Notebook/notebook-content.py

# Review generated notebook content for wh_gold
cat fabric_workspace_items/ddl_scripts/Warehouses/wh_gold/001_Initial_Creation.Notebook/notebook-content.py

# Check for syntax errors (Python)
python -m py_compile fabric_workspace_items/ddl_scripts/Lakehouses/lh_bronze/001_Initial_Creation.Notebook/notebook-content.py

# Verify variable replacement
grep "{{" fabric_workspace_items/ddl_scripts/Lakehouses/lh_bronze/001_Initial_Creation.Notebook/notebook-content.py
```

## Related Topics

- [CLI Reference - ddl command](cli_reference.md#ddl)
- [Common Tasks - Working with DDL Scripts](common_tasks.md#working-with-ddl-scripts)
- [Workflows - Development Cycle](workflows.md#development-cycle)
- [Environment Variables](../reference/environment-variables.md)
