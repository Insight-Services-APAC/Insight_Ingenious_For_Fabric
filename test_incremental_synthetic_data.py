#!/usr/bin/env python3
"""
Test script for incremental synthetic data generation functionality

MIGRATION NOTICE:
This test file uses the deprecated IncrementalSyntheticDataGenerationCompiler.
Please update tests to use UnifiedSyntheticDataGenerator from unified_commands.py instead.
"""

import sys
import os
from pathlib import Path

# Add the project to Python path
sys.path.insert(0, '/workspaces/Insight_Ingenious_For_Fabric')

# Set environment variables
os.environ['FABRIC_ENVIRONMENT'] = 'local'
os.environ['FABRIC_WORKSPACE_REPO_DIR'] = 'sample_project'

def test_incremental_compiler_import():
    """Test that we can import the incremental compiler"""
    try:
        from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
        print("✅ IncrementalSyntheticDataGenerationCompiler imported successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to import IncrementalSyntheticDataGenerationCompiler: {e}")
        return False

def test_enhanced_dataset_configs():
    """Test that enhanced dataset configurations work"""
    try:
        from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
        
        compiler = IncrementalSyntheticDataGenerationCompiler()
        configs = compiler.get_enhanced_predefined_dataset_configs()
        
        print(f"✅ Retrieved {len(configs)} enhanced dataset configurations")
        
        # Check that configurations have incremental settings
        for dataset_id, config in configs.items():
            if "incremental_config" not in config:
                print(f"❌ Dataset {dataset_id} missing incremental_config")
                return False
            
            if "table_configs" not in config:
                print(f"❌ Dataset {dataset_id} missing table_configs")
                return False
            
            # Check table configurations
            table_configs = config["table_configs"]
            for table_name, table_config in table_configs.items():
                if "type" not in table_config:
                    print(f"❌ Table {table_name} in {dataset_id} missing type")
                    return False
                
                table_type = table_config["type"]
                if table_type not in ["snapshot", "incremental"]:
                    print(f"❌ Table {table_name} in {dataset_id} has invalid type: {table_type}")
                    return False
        
        print("✅ All enhanced dataset configurations are valid")
        
        # Print a sample configuration
        sample_config = next(iter(configs.values()))
        print(f"\n📋 Sample configuration structure:")
        print(f"   Dataset ID: {sample_config['dataset_id']}")
        print(f"   Incremental config keys: {list(sample_config['incremental_config'].keys())}")
        print(f"   Number of tables: {len(sample_config['table_configs'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test enhanced dataset configs: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_notebook_compilation():
    """Test notebook compilation for incremental generation"""
    try:
        from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
        from datetime import date
        
        # Create a temporary output directory
        output_dir = Path("/tmp/test_incremental_notebooks")
        output_dir.mkdir(exist_ok=True)
        
        compiler = IncrementalSyntheticDataGenerationCompiler(
            fabric_workspace_repo_dir=str(output_dir)
        )
        
        # Get a sample dataset configuration
        configs = compiler.get_enhanced_predefined_dataset_configs()
        sample_config = configs["retail_oltp_small"]
        
        # Test single date notebook compilation
        try:
            notebook_path = compiler.compile_incremental_dataset_notebook(
                dataset_config=sample_config,
                generation_date=date(2024, 1, 15),
                target_environment="lakehouse",
                generation_mode="python",
                path_format="nested"
            )
            
            print(f"✅ Single date notebook compiled: {notebook_path}")
            
            # Check that the file exists
            if notebook_path.exists():
                print("✅ Notebook file exists")
            else:
                print("❌ Notebook file does not exist")
                return False
                
        except Exception as e:
            print(f"❌ Failed to compile single date notebook: {e}")
            return False
        
        # Test series notebook compilation
        try:
            series_notebook_path = compiler.compile_incremental_dataset_series_notebook(
                dataset_config=sample_config,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                target_environment="lakehouse",
                generation_mode="python",
                path_format="nested",
                batch_size=10
            )
            
            print(f"✅ Series notebook compiled: {series_notebook_path}")
            
            # Check that the file exists
            if series_notebook_path.exists():
                print("✅ Series notebook file exists")
            else:
                print("❌ Series notebook file does not exist")
                return False
                
        except Exception as e:
            print(f"❌ Failed to compile series notebook: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test notebook compilation: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_incremental_utils_import():
    """Test that we can import the incremental utils"""
    try:
        from ingen_fab.python_libs.pyspark.incremental_synthetic_data_utils import IncrementalSyntheticDataGenerator
        print("✅ IncrementalSyntheticDataGenerator imported successfully")
        
        # Test basic initialization
        generator = IncrementalSyntheticDataGenerator(
            lakehouse_utils_instance=None,
            seed=42,
            state_table_name="test_state"
        )
        
        print("✅ IncrementalSyntheticDataGenerator initialized successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to import/initialize IncrementalSyntheticDataGenerator: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing Incremental Synthetic Data Generation")
    print("=" * 50)
    
    tests = [
        ("Compiler Import", test_incremental_compiler_import),
        ("Enhanced Dataset Configs", test_enhanced_dataset_configs),
        ("Notebook Compilation", test_notebook_compilation),
        ("Incremental Utils Import", test_incremental_utils_import),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔍 Testing: {test_name}")
        print("-" * 30)
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Incremental synthetic data generation is ready to use.")
        
        print("\n💡 Next steps:")
        print("   1. Try the CLI commands:")
        print("      ingen_fab synthetic-data list-incremental-datasets")
        print("      ingen_fab synthetic-data generate-incremental retail_oltp_small --date 2024-01-15")
        print("   2. Review the documentation at docs/packages/incremental_synthetic_data_generation.md")
        print("   3. Customize configurations for your specific use case")
        
        return 0
    else:
        print("❌ Some tests failed. Please review the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
