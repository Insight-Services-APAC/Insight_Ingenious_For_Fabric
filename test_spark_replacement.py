#!/usr/bin/env python
"""
Test the spark reference replacement logic
"""
import re

def replace_spark_references(code):
    """Replace spark references with self.spark"""
    # First check if already has self.spark to avoid double replacement
    if 'self.spark' not in code:
        # Replace spark. at word boundaries
        code = re.sub(r'\bspark\.', 'self.spark.', code)
        # Replace standalone spark variable (not followed by .)
        code = re.sub(r'\bspark\b(?!\.)', 'self.spark', code)
    return code

# Test cases
test_cases = [
    # Case 1: Direct spark.sql() calls
    (
        "df = spark.sql('SELECT * FROM table')",
        "df = self.spark.sql('SELECT * FROM table')"
    ),
    
    # Case 2: spark.table() calls
    (
        "users_df = spark.table('lakehouse.users')",
        "users_df = self.spark.table('lakehouse.users')"
    ),
    
    # Case 3: spark.read operations
    (
        "data = spark.read.parquet('/path/to/file')",
        "data = self.spark.read.parquet('/path/to/file')"
    ),
    
    # Case 4: Standalone spark reference
    (
        "if spark:\n    print('Spark is available')",
        "if self.spark:\n    print('Spark is available')"
    ),
    
    # Case 5: spark createDataFrame
    (
        "df = spark.createDataFrame(data)",
        "df = self.spark.createDataFrame(data)"
    ),
    
    # Case 6: Multiple spark references in one line
    (
        "df1 = spark.sql('SELECT 1'); df2 = spark.sql('SELECT 2')",
        "df1 = self.spark.sql('SELECT 1'); df2 = self.spark.sql('SELECT 2')"
    ),
    
    # Case 7: spark in conditional
    (
        "if not spark:\n    raise RuntimeError('No spark session')",
        "if not self.spark:\n    raise RuntimeError('No spark session')"
    ),
    
    # Case 8: Already has self.spark (should not change)
    (
        "df = self.spark.sql('SELECT * FROM table')",
        "df = self.spark.sql('SELECT * FROM table')"
    ),
    
    # Case 9: Complex multiline example
    (
        """# Validate spark session
if not spark:
    raise RuntimeError("Spark session is not available")

# Read data
source_df = spark.table("lakehouse.stg_users")
count = source_df.count()

# Transform
result = spark.sql('''
    SELECT * FROM users
    WHERE active = true
''')""",
        """# Validate spark session
if not self.spark:
    raise RuntimeError("Spark session is not available")

# Read data
source_df = self.spark.table("lakehouse.stg_users")
count = source_df.count()

# Transform
result = self.spark.sql('''
    SELECT * FROM users
    WHERE active = true
''')"""
    ),
]

print("Testing spark reference replacement logic")
print("=" * 50)

all_passed = True
for i, (input_code, expected) in enumerate(test_cases, 1):
    result = replace_spark_references(input_code)
    passed = result == expected
    all_passed = all_passed and passed
    
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"\nTest {i}: {status}")
    print(f"Input:    {input_code[:50]}..." if len(input_code) > 50 else f"Input:    {input_code}")
    print(f"Expected: {expected[:50]}..." if len(expected) > 50 else f"Expected: {expected}")
    if not passed:
        print(f"Got:      {result[:50]}..." if len(result) > 50 else f"Got:      {result}")

print("\n" + "=" * 50)
print(f"Overall: {'All tests passed! ✓' if all_passed else 'Some tests failed ✗'}")

# Example of actual injected code transformation
print("\n" + "=" * 50)
print("Example transformation of models_python file:")
print("=" * 50)

example_code = """
# Custom Python logic for dim_users model
print("Starting dim_users model processing...")

# Validate spark session
if not spark:
    raise RuntimeError("Spark session is not available")

# Read source data
source_df = spark.table("lakehouse.stg_users")
if source_df.count() == 0:
    raise RuntimeError("No data found in stg_users table")

# Apply transformations
transformed_df = spark.sql('''
    SELECT 
        user_id,
        user_name,
        CURRENT_TIMESTAMP() as processed_at
    FROM stg_users
    WHERE is_active = true
''')

# Cache for performance
transformed_df.cache()
print(f"Processing {transformed_df.count()} active users")

return transformed_df
"""

transformed = replace_spark_references(example_code)
print("BEFORE:")
print(example_code)
print("\nAFTER:")
print(transformed)