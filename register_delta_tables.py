#!/usr/bin/env python3
"""
Script to discover Delta tables in tmp/spark/Tables directory and register them
with the Spark session catalog so they can be queried.
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent))

from ingen_fab.python_libs.common.config_utils import get_configs_as_object
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

def discover_and_register_delta_tables():
    """Discover Delta tables in the file system and register them with Spark catalog."""

    print("=" * 60)
    print("DISCOVERING AND REGISTERING DELTA TABLES")
    print("=" * 60)

    # Initialize lakehouse
    target_lakehouse = lakehouse_utils(
        target_workspace_id=get_configs_as_object().config_workspace_id,
        target_lakehouse_id=get_configs_as_object().config_lakehouse_id,
    )

    # Path to the Delta tables directory
    tables_dir = Path("tmp/spark/Tables")

    if not tables_dir.exists():
        print(f"❌ Tables directory not found: {tables_dir}")
        return {}

    print(f"🔍 Scanning directory: {tables_dir.absolute()}")

    # Find all subdirectories that might be Delta tables
    potential_tables = []
    for item in tables_dir.iterdir():
        if item.is_dir():
            # Check if it looks like a Delta table (has _delta_log directory)
            delta_log = item / "_delta_log"
            if delta_log.exists():
                potential_tables.append(item)
                print(f"  📁 Found Delta table: {item.name}")
            else:
                print(f"  📂 Found directory (not Delta): {item.name}")

    print(f"\n🎯 Found {len(potential_tables)} Delta tables to register")

    registered_tables = {}

    for table_path in potential_tables:
        table_name = table_path.name
        full_path = f"file://{table_path.absolute()}"

        try:
            print(f"\n📋 Registering table: {table_name}")
            print(f"   Path: {full_path}")

            # Register table directly in the metastore using SQL
            target_lakehouse.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING DELTA
                LOCATION '{full_path}'
            """)

            # Get schema information from the registered table
            schema_df = target_lakehouse.spark.sql(f"DESCRIBE {table_name}")
            schema_rows = schema_df.collect()

            columns = []
            for row in schema_rows:
                if row.col_name and not row.col_name.startswith('#'):  # Skip comment lines
                    columns.append({
                        'name': row.col_name.lower(),
                        'type': row.data_type,
                        'comment': getattr(row, 'comment', None)
                    })

            registered_tables[table_name] = {
                'path': full_path,
                'columns': columns
            }

            print(f"   ✅ Registered successfully with {len(columns)} columns")

            # Show first few columns
            for col in columns[:3]:
                print(f"      - {col['name']} ({col['type']})")
            if len(columns) > 3:
                print(f"      ... and {len(columns) - 3} more columns")

        except Exception as e:
            print(f"   ❌ Failed to register {table_name}: {str(e)}")

    # Verify registration by showing all tables
    print(f"\n" + "=" * 40)
    print("VERIFYING REGISTERED TABLES")
    print("=" * 40)

    try:
        all_tables_df = target_lakehouse.spark.sql("SHOW TABLES")
        all_tables = [row.tableName for row in all_tables_df.collect()]

        print(f"📊 Total tables now visible in catalog: {len(all_tables)}")

        aw_tables = [t for t in all_tables if 'aw' in t.lower()]
        if aw_tables:
            print(f"🎯 AdventureWorks tables found: {len(aw_tables)}")
            for table in sorted(aw_tables):
                print(f"   - {table}")
        else:
            print("❓ No AdventureWorks tables found in catalog")

        # Show all tables for reference
        print(f"\n📋 All registered tables:")
        for table in sorted(all_tables):
            print(f"   - {table}")

    except Exception as e:
        print(f"❌ Error verifying tables: {str(e)}")

    return registered_tables

def test_table_queries(registered_tables):
    """Test querying some of the registered tables."""

    print(f"\n" + "=" * 60)
    print("TESTING TABLE QUERIES")
    print("=" * 60)

    # Initialize lakehouse
    target_lakehouse = lakehouse_utils(
        target_workspace_id=get_configs_as_object().config_workspace_id,
        target_lakehouse_id=get_configs_as_object().config_lakehouse_id,
    )

    # Test queries on AdventureWorks tables
    test_tables = [t for t in registered_tables.keys() if 'aw' in t.lower()]

    if not test_tables:
        print("❓ No AdventureWorks tables to test")
        return

    print(f"🧪 Testing queries on {len(test_tables)} AdventureWorks tables")

    for table_name in test_tables[:3]:  # Test first 3 tables
        try:
            print(f"\n📊 Testing table: {table_name}")

            # Get row count
            count_df = target_lakehouse.spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
            row_count = count_df.collect()[0].row_count
            print(f"   📈 Row count: {row_count:,}")

            # Show sample data
            if row_count > 0:
                sample_df = target_lakehouse.spark.sql(f"SELECT * FROM {table_name} LIMIT 3")
                print(f"   📋 Sample data:")
                sample_df.show(3, truncate=False)
            else:
                print(f"   📭 Table is empty")

        except Exception as e:
            print(f"   ❌ Error testing {table_name}: {str(e)}")

if __name__ == "__main__":
    print("Starting Delta table discovery and registration...")

    # Set required environment variables
    os.environ["FABRIC_ENVIRONMENT"] = "local"
    os.environ["FABRIC_WORKSPACE_REPO_DIR"] = "sample_project"

    try:
        registered_tables = discover_and_register_delta_tables()
        test_table_queries(registered_tables)

        print("\n" + "=" * 60)
        print("🎉 Delta table registration complete!")
        print(f"✅ Registered {len(registered_tables)} tables")
        print("💡 Tables are now available for querying in this Spark session")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Registration failed: {str(e)}")
        import traceback
        traceback.print_exc()