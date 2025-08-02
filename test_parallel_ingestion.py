#!/usr/bin/env python3
"""
Test script to demonstrate parallel processing within execution groups
"""
import sys
import time
from datetime import datetime

sys.path.insert(0, '/workspaces/ingen_fab')

from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import FlatFileIngestionConfig
from ingen_fab.python_libs.pyspark.flat_file_ingestion_pyspark import PySparkFlatFileIngestionOrchestrator
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

def create_test_configs():
    """Create test configurations with multiple configs per execution group"""
    configs = []
    
    # Group 1: Two configurations that should run in parallel
    configs.append(FlatFileIngestionConfig(
        config_id="test_group1_config1",
        config_name="Test Group 1 - Config 1 (Customer Data)",
        source_file_path="synthetic_data/single/retail_oltp_small/customers.csv",
        source_file_format="csv",
        target_workspace_id="test",
        target_datastore_id="test", 
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="test_customers_1",
        execution_group=1,
        active_yn="Y",
        source_is_folder=True,
        write_mode="overwrite",
        has_header=True,
        file_delimiter=",",
        encoding="utf-8"
    ))
    
    configs.append(FlatFileIngestionConfig(
        config_id="test_group1_config2", 
        config_name="Test Group 1 - Config 2 (Customer Data Copy)",
        source_file_path="synthetic_data/single/retail_oltp_small/customers.csv",
        source_file_format="csv",
        target_workspace_id="test",
        target_datastore_id="test",
        target_datastore_type="lakehouse", 
        target_schema_name="raw",
        target_table_name="test_customers_2",
        execution_group=1,
        active_yn="Y",
        source_is_folder=True,
        write_mode="overwrite",
        has_header=True,
        file_delimiter=",",
        encoding="utf-8"
    ))
    
    # Group 2: Single configuration
    configs.append(FlatFileIngestionConfig(
        config_id="test_group2_config1",
        config_name="Test Group 2 - Single Config",
        source_file_path="synthetic_data/single/retail_oltp_small/customers.csv",
        source_file_format="csv",
        target_workspace_id="test",
        target_datastore_id="test",
        target_datastore_type="lakehouse",
        target_schema_name="raw", 
        target_table_name="test_customers_group2",
        execution_group=2,
        active_yn="Y",
        source_is_folder=True,
        write_mode="overwrite",
        has_header=True,
        file_delimiter=",",
        encoding="utf-8"
    ))
    
    return configs

def main():
    print("🚀 Testing Parallel Processing within Execution Groups")
    print("=" * 60)
    
    # Create test configurations
    configs = create_test_configs()
    
    print(f"📋 Created {len(configs)} test configurations:")
    for config in configs:
        print(f"  - {config.config_name} (Group {config.execution_group})")
    
    # Set up lakehouse utils
    lakehouse = lakehouse_utils("test_workspace", "test_lakehouse")
    
    # Create orchestrator (mock services for testing)
    class MockDiscovery:
        def discover_files(self, config):
            from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import FileDiscoveryResult
            return [FileDiscoveryResult(file_path=config.source_file_path)]
    
    class MockProcessor:
        def read_file(self, config, file_path):
            from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import ProcessingMetrics
            # Simulate processing time
            time.sleep(2)  # 2 second processing time
            metrics = ProcessingMetrics()
            metrics.source_row_count = 1000
            metrics.records_processed = 1000
            return None, metrics  # Mock DataFrame
            
        def write_data(self, data, config):
            from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import ProcessingMetrics
            time.sleep(1)  # 1 second write time
            metrics = ProcessingMetrics()
            metrics.records_inserted = 1000
            return metrics
            
        def validate_data(self, data, config):
            return {"is_valid": True, "row_count": 1000, "column_count": 5, "errors": []}
    
    class MockLogging:
        def log_execution_start(self, config, execution_id, partition_info=None):
            print(f"    📝 Logged start for {config.config_name}")
            
        def log_execution_completion(self, config, execution_id, metrics, status, partition_info=None):
            print(f"    📝 Logged completion for {config.config_name} ({status})")
            
        def log_execution_error(self, config, execution_id, error_message, error_details, partition_info=None):
            print(f"    📝 Logged error for {config.config_name}: {error_message}")
    
    # Create orchestrator
    orchestrator = PySparkFlatFileIngestionOrchestrator()
    orchestrator.discovery_service = MockDiscovery()
    orchestrator.processor_service = MockProcessor() 
    orchestrator.logging_service = MockLogging()
    
    # Run the test
    execution_id = f"test_parallel_{int(datetime.now().timestamp())}"
    
    print(f"\n⏰ Starting execution at {datetime.now().strftime('%H:%M:%S')}")
    start_time = time.time()
    
    # Process configurations
    results = orchestrator.process_configurations(configs, execution_id)
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    print(f"\n⏰ Completed execution at {datetime.now().strftime('%H:%M:%S')}")
    print(f"⏱️  Total Duration: {total_duration:.2f} seconds")
    
    # Display results
    print(f"\n📊 Results Summary:")
    print(f"  - Total Configurations: {results['total_configurations']}")
    print(f"  - Successful: {results['successful']}")
    print(f"  - Failed: {results['failed']}")
    print(f"  - No Data Found: {results['no_data_found']}")
    print(f"  - Execution Groups Processed: {results['execution_groups_processed']}")
    
    # Expected timing analysis
    print(f"\n📈 Performance Analysis:")
    print(f"  - Expected time with sequential processing: ~{len(configs) * 3:.0f} seconds")
    print(f"  - Expected time with parallel groups: ~9 seconds (3 groups * 3 seconds each)")
    print(f"  - Actual time: {total_duration:.2f} seconds")
    
    if total_duration < len(configs) * 2:
        print("  ✅ Parallel processing appears to be working!")
    else:
        print("  ⚠️  Processing may still be sequential")

if __name__ == "__main__":
    main()