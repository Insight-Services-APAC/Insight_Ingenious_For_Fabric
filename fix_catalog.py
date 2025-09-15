#!/usr/bin/env python3
"""
Script to fix corrupted Spark catalog entries by dropping and recreating Delta tables
"""

import sys
import os
from pathlib import Path

# Add the project path to sys.path  
sys.path.insert(0, str(Path(__file__).parent))

# Import our existing utilities
import ingen_fab.python_libs.common.config_utils as cu
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

def main():
    # Use existing lakehouse_utils to create Spark session
    configs = cu.get_configs_as_object()
    lakehouse = lakehouse_utils(
        target_workspace_id=configs.config_workspace_id,
        target_lakehouse_id=configs.config_lakehouse_id
    )
    spark = lakehouse.spark
    
    # List of tables that exist as Delta files
    tables_dir = Path("tmp/spark/Tables")
    if not tables_dir.exists():
        print(f"Tables directory {tables_dir} does not exist!")
        return
    
    delta_tables = [d.name for d in tables_dir.iterdir() 
                   if d.is_dir() and (d / "_delta_log").exists()]
    
    print(f"Found {len(delta_tables)} Delta tables in storage:")
    for table in sorted(delta_tables):
        print(f"  - {table}")
    
    # Check which ones are corrupted in catalog
    corrupted_tables = []
    for table_name in delta_tables:
        try:
            if spark.catalog.tableExists(table_name):
                # Try to get table info - this will fail if path is corrupted
                table_info = spark.catalog.getTable(table_name)
                # Check if we can describe it
                spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
                print(f"✅ {table_name}: Catalog entry is valid")
            else:
                print(f"⚠️  {table_name}: Not in catalog, will recreate")
                corrupted_tables.append(table_name)
        except Exception as e:
            print(f"❌ {table_name}: Corrupted in catalog - {e}")
            corrupted_tables.append(table_name)
    
    if not corrupted_tables:
        print("\n🎉 All tables are correctly registered in catalog!")
        return
    
    print(f"\n🔧 Fixing {len(corrupted_tables)} corrupted tables...")
    
    # Drop corrupted entries and recreate
    for table_name in corrupted_tables:
        try:
            # Drop if exists (might fail, that's ok)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                print(f"  📤 Dropped corrupted entry for {table_name}")
            except:
                pass
            
            # Recreate with correct path  
            correct_path = f"file://{Path.cwd()}/tmp/spark/Tables/{table_name}"
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING DELTA
                LOCATION '{correct_path}'
            """)
            print(f"  ✅ Recreated {table_name} with path: {correct_path}")
            
            # Verify it works
            count = spark.table(table_name).count()
            print(f"     Verified: {count:,} rows")
            
        except Exception as e:
            print(f"  ❌ Failed to fix {table_name}: {e}")
    
    print(f"\n🎉 Catalog repair complete!")
    spark.stop()

if __name__ == "__main__":
    main()