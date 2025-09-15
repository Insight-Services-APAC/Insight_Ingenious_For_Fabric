#!/usr/bin/env python
"""
Example of how the enhanced create-python-classes command works with Python injection.

Directory structure:
sample_project/
├── dbt_project/
│   ├── target/sql/         # SQL JSON files from dbt
│   └── models_python/      # NEW: Python files to inject
│       ├── model.pmo_reports.dim_users.py
│       ├── seed.pmo_reports.ref__users.py  
│       └── test.pmo_reports.not_null_stg_users_user_name.226879cc53.py

The command will:
1. Read SQL from target/sql/*.json (as before)
2. Look for matching Python files in models_python/ (NEW)
3. Inject Python code as first step in execute_all() method (NEW)
"""

# Example content for models_python/model.pmo_reports.dim_users.py
example_model_python = '''
# Custom Python logic for dim_users model
print("Starting dim_users model processing...")

# Validate spark session
if not self.spark:
    raise RuntimeError("Spark session is not available")

# Custom data validation
source_df = self.spark.table("lakehouse.stg_users")
if source_df.count() == 0:
    raise RuntimeError("No data found in stg_users table")

print(f"Processing {source_df.count()} rows from stg_users")

# Custom transformations before SQL
# (This will run BEFORE the SQL statements from target/sql)
return source_df
'''

# Example content for models_python/seed.pmo_reports.ref__users.py  
example_seed_python = '''
# Custom Python logic for ref__users seed
print("Initializing ref__users seed data...")

# Check if seed data already exists
existing_data = self.spark.sql("SHOW TABLES LIKE 'ref__users'")
if existing_data.count() > 0:
    print("Seed table already exists, checking for updates needed")

# Custom seed validation logic
return None
'''

print("Enhanced create-python-classes Command Features:")
print("=" * 50)
print("1. Reads SQL JSON files from {dbt_project}/target/sql/")
print("2. Looks for matching Python files in {dbt_project}/models_python/")
print("3. Injects Python code as execute_injected_python() method")
print("4. Python code executes FIRST in execute_all() sequence")
print("5. Supports node_id matching with multiple naming patterns:")
print("   - {node_id}.py")
print("   - {node_id.replace('.', '_')}.py")
print("   - {raw_model_name}.py")
print("\nGenerated class structure:")
print("=" * 30)
print("""
class DimUsers:
    def execute_injected_python(self):  # NEW - from models_python/
        # Custom Python code executes first
        pass
    
    def finalize_data(self):           # From target/sql/
        # SQL statements execute second
        pass
        
    def execute_all(self):
        results = []
        
        # Step 1: Execute injected Python (if exists)
        injected_result = self.execute_injected_python()
        results.append(injected_result)
        
        # Step 2: Execute SQL statements
        result = self.finalize_data()
        results.append(result)
        
        return results
""")