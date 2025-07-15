# Clean up existing metastore database completely
echo "Recreating metastore database..."
mysql -u hive -phivepassword -h localhost -e "DROP DATABASE IF EXISTS metastore;"
mysql -u hive -phivepassword -h localhost -e "CREATE DATABASE metastore CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
echo "✅ Fresh metastore database created"

# Run scripts/dev_container_scripts/spark_minimal/hive-schema-4.0.0.mysql.sql
echo "Initializing Hive metastore schema..."
mysql -u hive -phivepassword -h localhost metastore < ./scripts/dev_container_scripts/spark_minimal/hive-schema-4.0.0.mysql.sql

# Verify schema was created
TABLES_COUNT=$(mysql -u hive -phivepassword -h localhost metastore -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='metastore';" -s --skip-column-names 2>/dev/null)
echo "Created $TABLES_COUNT metastore tables"

if [ "$TABLES_COUNT" -gt 0 ]; then
    echo "✅ Hive metastore schema initialized successfully"
else
    echo "❌ Hive metastore schema initialization failed"
    exit 1
fi