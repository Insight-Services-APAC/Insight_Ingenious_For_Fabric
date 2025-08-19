# PostgreSQL Metastore Setup for Spark

## Overview
This directory contains cross-platform scripts to set up PostgreSQL as a Hive metastore for Apache Spark, compatible with both x64 and ARM architectures.

## Files
- `postgres_metastore_setup.sh` - Main setup script that installs and configures PostgreSQL
- `postgres_metastore_init.sh` - Reinitialization script to reset the metastore database
- `hive-schema-4.0.0.postgres.sql` - PostgreSQL schema for Hive metastore

## Setup Process

### 1. Run the Setup Script
```bash
./scripts/dev_container_scripts/spark_minimal/postgres_metastore_setup.sh
```

This script will:
- Install PostgreSQL 15 (works on both x64 and ARM)
- Create `metastore` database and `hive` user
- Configure PostgreSQL authentication
- Apply the Hive metastore schema (83 tables)
- Download PostgreSQL JDBC driver
- Configure Spark to use PostgreSQL metastore

### 2. Test the Setup
```bash
# Test with spark-sql
spark-sql -e "SHOW DATABASES"
spark-sql -e "CREATE DATABASE test_db; USE test_db; SHOW TABLES"

# Test with spark-shell (interactive)
spark-shell
```

## Configuration Details

### PostgreSQL Settings
- **Database**: `metastore` on `localhost:5432`
- **User**: `hive` / **Password**: `hivepassword`
- **Schema**: 83 tables from `hive-schema-4.0.0.postgres.sql`

### Spark Configuration
**File**: `$SPARK_HOME/conf/spark-defaults.conf`
```properties
spark.sql.catalogImplementation=hive
spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:postgresql://localhost:5432/metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName=org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionUserName=hive
spark.hadoop.javax.jdo.option.ConnectionPassword=hivepassword
```

**File**: `$SPARK_HOME/conf/hive-site.xml`
```xml
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:postgresql://localhost:5432/metastore</value>
</property>
```

## Tested Commands

### Basic Operations
```scala
// Show databases
spark.sql("SHOW DATABASES").show()

// Create database and table
spark.sql("CREATE DATABASE test_db")
spark.sql("USE test_db")
spark.sql("CREATE TABLE test_table (id INT, name STRING) USING PARQUET")

// Show tables
spark.sql("SHOW TABLES").show()

// Insert and query data
spark.sql("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
spark.sql("SELECT * FROM test_table").show()
```

## Verification

### PostgreSQL Metastore Tables
```bash
PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "\dt" | head -10
```

### Check Spark Metastore Integration
```bash
PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c 'SELECT d."NAME", t."TBL_NAME" FROM "TBLS" t JOIN "DBS" d ON t."DB_ID" = d."DB_ID";'
```

## Architecture Compatibility
- **x86_64 (x64)**: ✅ Tested and working
- **aarch64 (ARM)**: ✅ Tested and working

The setup uses PostgreSQL's official APT repository which provides native packages for both architectures.

## Troubleshooting

### Check PostgreSQL Status
```bash
sudo service postgresql status
pg_isready -h localhost -p 5432
```

### Verify Database Connection
```bash
PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "SELECT version();"
```

### Check Spark Configuration
```bash
spark-sql --version
cat $SPARK_HOME/conf/spark-defaults.conf | grep -i postgres
```

## Reinitializing
To reset the metastore database to a clean state:
```bash
./scripts/dev_container_scripts/spark_minimal/postgres_metastore_init.sh
```