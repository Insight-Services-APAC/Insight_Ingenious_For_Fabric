# Python-specific implementations for flat file ingestion
# Uses warehouse_utils and standard Python libraries

import time
import uuid
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple

from ..interfaces.flat_file_ingestion_interface import (
    FlatFileIngestionConfig,
    FileDiscoveryResult,
    ProcessingMetrics,
    FlatFileDiscoveryInterface,
    FlatFileProcessorInterface,
    FlatFileLoggingInterface,
    FlatFileIngestionOrchestrator
)
from ..common.flat_file_ingestion_utils import (
    DatePartitionUtils,
    FilePatternUtils,
    ConfigurationUtils,
    ProcessingMetricsUtils,
    ErrorHandlingUtils
)


class PythonFlatFileDiscovery(FlatFileDiscoveryInterface):
    """Python implementation of file discovery using warehouse_utils"""
    
    def __init__(self, warehouse_utils):
        self.warehouse_utils = warehouse_utils
    
    def discover_files(self, config: FlatFileIngestionConfig) -> List[FileDiscoveryResult]:
        """Discover files based on configuration using warehouse_utils"""
        
        if config.import_pattern != "date_partitioned":
            # For non-date-partitioned imports, return single file path
            return [FileDiscoveryResult(file_path=config.source_file_path)]
        
        discovered_files = []
        base_path = config.source_file_path.rstrip('/')
        
        try:
            print(f"🔍 Detected date-partitioned import for {config.config_name}")
            print(f"🔍 Discovering files with pattern: {config.file_discovery_pattern}")
            print(f"🔍 Base path: {base_path}")
            
            # For warehouse scenarios, we might need to implement file system discovery
            # This is a simplified version that can be extended based on warehouse_utils capabilities
            print(f"⚠️ Date-partitioned discovery not fully implemented for warehouse scenarios")
            print(f"Falling back to single file processing")
            
            discovered_files = [FileDiscoveryResult(file_path=config.source_file_path)]
            
        except Exception as e:
            print(f"⚠️ Warning: File discovery failed: {e}")
            # Fallback to single file processing
            discovered_files = [FileDiscoveryResult(file_path=config.source_file_path)]
        
        return discovered_files
    
    def extract_date_from_folder_name(self, folder_name: str, date_format: str) -> Optional[str]:
        """Extract date from folder name based on format"""
        return DatePartitionUtils.extract_date_from_folder_name(folder_name, date_format)
    
    def extract_date_from_path(self, file_path: str, base_path: str, date_format: str) -> Optional[str]:
        """Extract date from file path based on format"""
        return DatePartitionUtils.extract_date_from_path(file_path, base_path, date_format)
    
    def is_date_in_range(self, date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        return DatePartitionUtils.is_date_in_range(date_str, start_date, end_date)
    
    def date_already_processed(self, date_partition: str, config: FlatFileIngestionConfig) -> bool:
        """Check if a date has already been processed (for skip_existing_dates feature)"""
        try:
            # Query the log table to see if this date has been successfully processed
            query = f"""
                SELECT COUNT(*) as count_records
                FROM log_flat_file_ingestion 
                WHERE config_id = '{config.config_id}' 
                AND source_file_path LIKE '%{date_partition.replace('-', '')}%'
                AND status = 'completed'
            """
            
            result = self.warehouse_utils.execute_query(query)
            return result[0]['count_records'] > 0 if result else False
            
        except Exception as e:
            print(f"⚠️ Could not check if date {date_partition} already processed: {e}")
            # If we can't check, assume not processed to be safe
            return False


class PythonFlatFileProcessor(FlatFileProcessorInterface):
    """Python implementation of flat file processing using warehouse_utils and pandas"""
    
    def __init__(self, warehouse_utils):
        self.warehouse_utils = warehouse_utils
    
    def read_file(self, config: FlatFileIngestionConfig, file_path: str) -> Tuple[pd.DataFrame, ProcessingMetrics]:
        """Read a file based on configuration and return DataFrame with metrics"""
        read_start = time.time()
        metrics = ProcessingMetrics()
        
        try:
            # Get file reading options based on configuration
            options = ConfigurationUtils.get_file_read_options(config)
            
            # Read file using pandas based on format
            if config.source_file_format.lower() == 'csv':
                df = pd.read_csv(
                    file_path,
                    sep=config.file_delimiter,
                    header=0 if config.has_header else None,
                    encoding=config.encoding,
                    quotechar=config.quote_character,
                    escapechar=config.escape_character if config.escape_character != '\\' else None,
                    na_values=[config.null_value] if config.null_value else None,
                    comment=config.comment_character,
                    skipinitialspace=not config.ignore_leading_whitespace,
                    skip_blank_lines=True
                )
            elif config.source_file_format.lower() == 'json':
                df = pd.read_json(file_path, encoding=config.encoding)
            elif config.source_file_format.lower() == 'parquet':
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {config.source_file_format}")
            
            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = len(df)
            metrics.records_processed = metrics.source_row_count
            
            print(f"Read {metrics.source_row_count} records from source file in {metrics.read_duration_ms}ms")
            
            return df, metrics
            
        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            print(f"❌ Error reading file {file_path}: {e}")
            raise
    
    def read_folder(self, config: FlatFileIngestionConfig, folder_path: str) -> Tuple[pd.DataFrame, ProcessingMetrics]:
        """Read all files in a folder based on configuration"""
        # For Python/warehouse scenarios, folder reading might be limited
        # This could be extended to handle specific folder structures
        print(f"⚠️ Folder reading not fully implemented for Python/warehouse scenarios")
        return self.read_file(config, folder_path)
    
    def write_data(self, data: pd.DataFrame, config: FlatFileIngestionConfig) -> ProcessingMetrics:
        """Write data to target destination using warehouse_utils"""
        write_start = time.time()
        metrics = ProcessingMetrics()
        
        try:
            # Get target row count before write (for reconciliation)
            try:
                count_query = f"SELECT COUNT(*) as count_records FROM {config.target_schema_name}.{config.target_table_name}"
                result = self.warehouse_utils.execute_query(count_query)
                metrics.target_row_count_before = result[0]['count_records'] if result else 0
            except Exception:
                metrics.target_row_count_before = 0
            
            # Convert pandas DataFrame to format suitable for warehouse_utils
            # This depends on the specific warehouse_utils implementation
            
            if config.write_mode == "overwrite":
                # Truncate table first
                truncate_query = f"TRUNCATE TABLE {config.target_schema_name}.{config.target_table_name}"
                self.warehouse_utils.execute_query(truncate_query)
            
            # Insert data using warehouse_utils
            # This is a simplified version - actual implementation would depend on warehouse_utils capabilities
            table_name = f"{config.target_schema_name}.{config.target_table_name}"
            
            # For now, use bulk insert method if available
            self.warehouse_utils.bulk_insert_dataframe(data, table_name)
            
            # Get target row count after write
            try:
                count_query = f"SELECT COUNT(*) as count_records FROM {config.target_schema_name}.{config.target_table_name}"
                result = self.warehouse_utils.execute_query(count_query)
                metrics.target_row_count_after = result[0]['count_records'] if result else 0
            except Exception:
                metrics.target_row_count_after = 0
            
            # Calculate metrics
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            
            if config.write_mode == "overwrite":
                metrics.records_inserted = metrics.target_row_count_after
                metrics.records_deleted = metrics.target_row_count_before
            elif config.write_mode == "append":
                metrics.records_inserted = metrics.target_row_count_after - metrics.target_row_count_before
            
            print(f"Wrote data to {table_name} in {metrics.write_duration_ms}ms")
            
            return ProcessingMetricsUtils.calculate_performance_metrics(metrics)
            
        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            print(f"❌ Error writing data to {config.target_schema_name}.{config.target_table_name}: {e}")
            raise
    
    def validate_data(self, data: pd.DataFrame, config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Validate data based on configuration rules"""
        validation_results = {
            'is_valid': True,
            'row_count': 0,
            'column_count': 0,
            'errors': []
        }
        
        try:
            validation_results['row_count'] = len(data)
            validation_results['column_count'] = len(data.columns)
            
            # Basic validation
            if validation_results['row_count'] == 0:
                validation_results['errors'].append("No data found in source")
                validation_results['is_valid'] = False
            
            # Check for null values if required
            if data.isnull().any().any():
                null_columns = data.columns[data.isnull().any()].tolist()
                validation_results['errors'].append(f"Null values found in columns: {null_columns}")
                # Don't mark as invalid for null values unless specifically configured
            
            # Additional validation rules can be implemented here
            # based on config.data_validation_rules
            
        except Exception as e:
            validation_results['errors'].append(f"Validation failed: {e}")
            validation_results['is_valid'] = False
        
        return validation_results


class PythonFlatFileLogging(FlatFileLoggingInterface):
    """Python implementation of flat file ingestion logging using warehouse_utils"""
    
    def __init__(self, warehouse_utils):
        self.warehouse_utils = warehouse_utils
    
    def log_execution_start(self, config: FlatFileIngestionConfig, execution_id: str, 
                           partition_info: Optional[Dict[str, str]] = None) -> None:
        """Log the start of a file processing execution"""
        try:
            # For warehouse implementation, we can store partition info as JSON in a future enhancement
            # Currently just log the basic execution start
            log_query = f"""
                INSERT INTO log_flat_file_ingestion 
                (log_id, config_id, execution_id, job_start_time, status, source_file_path, target_table_name)
                VALUES (
                    '{str(uuid.uuid4())}',
                    '{config.config_id}',
                    '{execution_id}',
                    GETDATE(),
                    'running',
                    '{config.source_file_path}',
                    '{config.target_table_name}'
                )
            """
            
            self.warehouse_utils.execute_query(log_query)
            
        except Exception as e:
            print(f"⚠️ Failed to log execution start: {e}")
    
    def log_execution_completion(self, config: FlatFileIngestionConfig, execution_id: str,
                               metrics: ProcessingMetrics, status: str,
                               partition_info: Optional[Dict[str, str]] = None) -> None:
        """Log the completion of a file processing execution"""
        try:
            # For warehouse implementation, we can store partition info as JSON in a future enhancement
            # Currently just log the basic execution completion
            log_query = f"""
                INSERT INTO log_flat_file_ingestion 
                (log_id, config_id, execution_id, job_start_time, job_end_time, status, 
                 source_file_path, target_table_name, records_processed, records_inserted, 
                 records_updated, records_deleted, records_failed)
                VALUES (
                    '{str(uuid.uuid4())}',
                    '{config.config_id}',
                    '{execution_id}',
                    GETDATE(),
                    GETDATE(),
                    '{status}',
                    '{config.source_file_path}',
                    '{config.target_table_name}',
                    {metrics.records_processed},
                    {metrics.records_inserted},
                    {metrics.records_updated},
                    {metrics.records_deleted},
                    {metrics.records_failed}
                )
            """
            
            self.warehouse_utils.execute_query(log_query)
            
        except Exception as e:
            print(f"⚠️ Failed to log execution completion: {e}")
    
    def log_execution_error(self, config: FlatFileIngestionConfig, execution_id: str,
                          error_message: str, error_details: str,
                          partition_info: Optional[Dict[str, str]] = None) -> None:
        """Log an execution error"""
        try:
            # Escape single quotes in error messages
            escaped_error_message = error_message.replace("'", "''")
            escaped_error_details = error_details.replace("'", "''")
            
            # For warehouse implementation, we can store partition info as JSON in a future enhancement
            # Currently just log the basic execution error
            log_query = f"""
                INSERT INTO log_flat_file_ingestion 
                (log_id, config_id, execution_id, job_start_time, job_end_time, status, 
                 source_file_path, target_table_name, error_message, error_details,
                 records_processed, records_inserted, records_updated, records_deleted, records_failed)
                VALUES (
                    '{str(uuid.uuid4())}',
                    '{config.config_id}',
                    '{execution_id}',
                    GETDATE(),
                    GETDATE(),
                    'failed',
                    '{config.source_file_path}',
                    '{config.target_table_name}',
                    '{escaped_error_message}',
                    '{escaped_error_details}',
                    0, 0, 0, 0, 0
                )
            """
            
            self.warehouse_utils.execute_query(log_query)
            
            
        except Exception as e:
            print(f"⚠️ Failed to log execution error: {e}")


class PythonFlatFileIngestionOrchestrator(FlatFileIngestionOrchestrator):
    """Python implementation of flat file ingestion orchestrator"""
    
    def process_configuration(self, config: FlatFileIngestionConfig, execution_id: str) -> Dict[str, Any]:
        """Process a single configuration"""
        start_time = time.time()
        result = {
            'config_id': config.config_id,
            'config_name': config.config_name,
            'status': 'pending',
            'metrics': ProcessingMetrics(),
            'errors': []
        }
        
        try:
            print(f"\n=== Processing {config.config_name} ===")
            print(f"Source: {config.source_file_path}")
            print(f"Target: {config.target_schema_name}.{config.target_table_name}")
            print(f"Format: {config.source_file_format}")
            print(f"Write Mode: {config.write_mode}")
            
            # Validate configuration
            validation_errors = ConfigurationUtils.validate_config(config)
            if validation_errors:
                result['errors'] = validation_errors
                result['status'] = 'failed'
                return result
            
            # Log execution start
            self.logging_service.log_execution_start(config, execution_id)
            
            # Discover files
            discovered_files = self.discovery_service.discover_files(config)
            
            if not discovered_files:
                result['status'] = 'no_data_found'
                print(f"⚠️ No source data found for {config.config_name}. Skipping write and reconciliation operations.")
                return result
            
            # Process discovered files sequentially
            all_metrics = []
            
            for file_result in discovered_files:
                try:
                    # Read file
                    df, read_metrics = self.processor_service.read_file(config, file_result.file_path)
                    
                    if read_metrics.source_row_count == 0:
                        print(f"⚠️ No data found in file: {file_result.file_path}")
                        continue
                    
                    # Validate data
                    validation_result = self.processor_service.validate_data(df, config)
                    if not validation_result['is_valid']:
                        result['errors'].extend(validation_result['errors'])
                        continue
                    
                    # Write data
                    write_metrics = self.processor_service.write_data(df, config)
                    
                    # Combine metrics
                    combined_metrics = ProcessingMetrics(
                        read_duration_ms=read_metrics.read_duration_ms,
                        write_duration_ms=write_metrics.write_duration_ms,
                        records_processed=read_metrics.records_processed,
                        records_inserted=write_metrics.records_inserted,
                        records_updated=write_metrics.records_updated,
                        records_deleted=write_metrics.records_deleted,
                        source_row_count=read_metrics.source_row_count,
                        target_row_count_before=write_metrics.target_row_count_before,
                        target_row_count_after=write_metrics.target_row_count_after
                    )
                    
                    all_metrics.append(combined_metrics)
                    
                except Exception as file_error:
                    error_details = ErrorHandlingUtils.format_error_details(file_error, {
                        'file_path': file_result.file_path,
                        'config_id': config.config_id
                    })
                    result['errors'].append(f"Error processing file {file_result.file_path}: {file_error}")
                    print(f"❌ Error processing file {file_result.file_path}: {file_error}")
            
            # Aggregate metrics
            if all_metrics:
                result['metrics'] = ProcessingMetricsUtils.merge_metrics(all_metrics)
                result['status'] = 'completed'
                
                # Calculate processing time
                end_time = time.time()
                processing_duration = end_time - start_time
                result['metrics'].total_duration_ms = int(processing_duration * 1000)
                
                print(f"\nProcessing completed in {processing_duration:.2f} seconds")
                print(f"Records processed: {result['metrics'].records_processed}")
                print(f"Records inserted: {result['metrics'].records_inserted}")
                print(f"Records updated: {result['metrics'].records_updated}")
                print(f"Records deleted: {result['metrics'].records_deleted}")
                print(f"Source rows: {result['metrics'].source_row_count}")
                print(f"Target rows before: {result['metrics'].target_row_count_before}")
                print(f"Target rows after: {result['metrics'].target_row_count_after}")
                print(f"Row count reconciliation: {result['metrics'].row_count_reconciliation_status}")
                
                # Log completion
                self.logging_service.log_execution_completion(config, execution_id, result['metrics'], 'completed')
            else:
                result['status'] = 'no_data_processed'
                print(f"Processing completed in {time.time() - start_time:.2f} seconds (no data processed)")
                print("Row count reconciliation: skipped (no source data)")
        
        except Exception as e:
            result['status'] = 'failed'
            result['errors'].append(str(e))
            error_details = ErrorHandlingUtils.format_error_details(e, {
                'config_id': config.config_id,
                'config_name': config.config_name
            })
            self.logging_service.log_execution_error(config, execution_id, str(e), str(error_details))
            print(f"Error processing {config.config_name}: {e}")
        
        return result
    
    def process_configurations(self, configs: List[FlatFileIngestionConfig], execution_id: str) -> Dict[str, Any]:
        """Process multiple configurations"""
        results = {
            'execution_id': execution_id,
            'total_configurations': len(configs),
            'successful': 0,
            'failed': 0,
            'no_data_found': 0,
            'configurations': []
        }
        
        for config in configs:
            config_result = self.process_configuration(config, execution_id)
            results['configurations'].append(config_result)
            
            if config_result['status'] == 'completed':
                results['successful'] += 1
            elif config_result['status'] == 'failed':
                results['failed'] += 1
            elif config_result['status'] in ['no_data_found', 'no_data_processed']:
                results['no_data_found'] += 1
        
        return results