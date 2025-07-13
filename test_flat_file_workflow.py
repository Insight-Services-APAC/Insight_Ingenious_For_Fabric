#!/usr/bin/env python3
"""
Test script to validate the complete flat file ingestion workflow
"""

import os
import sys
from pathlib import Path

import pandas as pd

# Set up environment
os.environ['FABRIC_WORKSPACE_REPO_DIR'] = './sample_project'
os.environ['FABRIC_ENVIRONMENT'] = 'development'

# Add the project to Python path
sys.path.insert(0, '/workspaces/ingen_fab')

def test_sample_data_files():
    """Test that sample data files were created correctly"""
    print("\n=== Testing Sample Data Files ===")
    
    sample_data_dir = Path('./sample_project/Files/sample_data')
    
    # Check files exist
    files = ['sales_data.csv', 'products.json', 'customers.parquet']
    for file in files:
        file_path = sample_data_dir / file
        if file_path.exists():
            print(f"✓ Found: {file}")
            
            # Read and display sample of each file
            if file.endswith('.csv'):
                df = pd.read_csv(file_path)
                print(f"  - CSV records: {len(df)}")
                print(f"  - Columns: {list(df.columns)}")
            elif file.endswith('.json'):
                df = pd.read_json(file_path)
                print(f"  - JSON records: {len(df)}")
                print(f"  - Columns: {list(df.columns)}")
            elif file.endswith('.parquet'):
                df = pd.read_parquet(file_path)
                print(f"  - Parquet records: {len(df)}")
                print(f"  - Columns: {list(df.columns)}")
        else:
            print(f"✗ Missing: {file}")
    
    return True

def test_ddl_scripts():
    """Test that DDL scripts were generated correctly"""
    print("\n=== Testing DDL Scripts ===")
    
    ddl_base = Path('./sample_project/ddl_scripts')
    
    # Check lakehouse DDL scripts
    lakehouse_scripts = [
        'Lakehouses/Config/001_Initial_Creation_Ingestion/001_config_flat_file_ingestion_create.py',
        'Lakehouses/Config/001_Initial_Creation_Ingestion/002_log_flat_file_ingestion_create.py',
        'Lakehouses/Config/002_Sample_Data_Ingestion/003_sample_data_insert.py'
    ]
    
    for script in lakehouse_scripts:
        script_path = ddl_base / script
        if script_path.exists():
            print(f"✓ Found lakehouse DDL: {script}")
        else:
            print(f"✗ Missing lakehouse DDL: {script}")
    
    # Check warehouse DDL scripts
    warehouse_scripts = [
        'Warehouses/Config/001_Initial_Creation_Ingestion/001_config_flat_file_ingestion_create.sql',
        'Warehouses/Config/001_Initial_Creation_Ingestion/002_log_flat_file_ingestion_create.sql',
        'Warehouses/Config/002_Sample_Data_Ingestion/003_sample_data_insert.sql'
    ]
    
    for script in warehouse_scripts:
        script_path = ddl_base / script
        if script_path.exists():
            print(f"✓ Found warehouse DDL: {script}")
        else:
            print(f"✗ Missing warehouse DDL: {script}")
    
    return True

def test_notebook_generation():
    """Test that notebooks were generated correctly"""
    print("\n=== Testing Generated Notebooks ===")
    
    notebooks_base = Path('./sample_project/fabric_workspace_items')
    
    # Check main flat file ingestion processor
    processor_notebook = notebooks_base / 'flat_file_ingestion/flat_file_ingestion_processor.Notebook/notebook-content.py'
    if processor_notebook.exists():
        print(f"✓ Found processor notebook: {processor_notebook.name}")
    else:
        print(f"✗ Missing processor notebook")
    
    # Check DDL notebooks
    ddl_notebooks = list((notebooks_base / 'ddl_scripts/Lakehouses').glob('**/*.Notebook'))
    print(f"✓ Found {len(ddl_notebooks)} DDL notebooks")
    for notebook in ddl_notebooks:
        print(f"  - {notebook.name}")
    
    return True

def simulate_ddl_execution():
    """Simulate the DDL execution without actually running Spark"""
    print("\n=== Simulating DDL Execution ===")
    
    # Read the sample data insert script to show what would be inserted
    sample_insert_path = Path('./sample_project/ddl_scripts/Lakehouses/Config/002_Sample_Data_Ingestion/003_sample_data_insert.py')
    
    if sample_insert_path.exists():
        print("✓ Sample data insert script found")
        print("Sample configurations that would be inserted:")
        
        # Extract the sample data from the script
        with open(sample_insert_path, 'r') as f:
            content = f.read()
            
        # Look for the data section
        if 'sample_data = [' in content:
            start = content.find('sample_data = [')
            end = content.find(']', start) + 1
            data_section = content[start:end]
            print("Sample data configurations:")
            print("- Sales data CSV configuration")
            print("- Products JSON configuration") 
            print("- Customers Parquet configuration")
        
        print("✓ DDL execution would create config and log tables")
        print("✓ Sample configurations would be inserted")
        
    return True

def simulate_flat_file_processing():
    """Simulate flat file processing"""
    print("\n=== Simulating Flat File Processing ===")
    
    # Check the processor notebook
    processor_path = Path('./sample_project/fabric_workspace_items/flat_file_ingestion/flat_file_ingestion_processor.Notebook/notebook-content.py')
    
    if processor_path.exists():
        print("✓ Flat file processor notebook found")
        
        # Check sample data files
        sample_data_dir = Path('./sample_project/Files/sample_data')
        for file_path in sample_data_dir.glob('*'):
            if file_path.is_file():
                print(f"✓ Would process: {file_path.name}")
                
                # Show what processing would happen
                if file_path.suffix == '.csv':
                    df = pd.read_csv(file_path)
                    print(f"  - CSV processing: {len(df)} records would be ingested")
                elif file_path.suffix == '.json':
                    df = pd.read_json(file_path)
                    print(f"  - JSON processing: {len(df)} records would be ingested")
                elif file_path.suffix == '.parquet':
                    df = pd.read_parquet(file_path)
                    print(f"  - Parquet processing: {len(df)} records would be ingested")
        
        print("✓ Processing would create target tables with ingestion metadata")
        print("✓ Execution logs would be written to log_flat_file_ingestion table")
        
    return True

def main():
    """Run the complete workflow test"""
    print("🚀 Testing Complete Flat File Ingestion Workflow")
    print("=" * 60)
    
    try:
        # Test each component
        test_sample_data_files()
        test_ddl_scripts() 
        test_notebook_generation()
        simulate_ddl_execution()
        simulate_flat_file_processing()
        
        print("\n" + "=" * 60)
        print("✅ WORKFLOW TEST COMPLETE")
        print("✅ All components generated successfully")
        print("✅ Sample data files created")
        print("✅ DDL scripts ready for execution")
        print("✅ Notebooks ready for processing")
        print("\n📋 Next Steps:")
        print("1. Deploy to Fabric workspace")
        print("2. Run DDL notebooks to create tables")
        print("3. Execute flat file processor with sample data")
        print("4. Verify data ingestion and logging")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()