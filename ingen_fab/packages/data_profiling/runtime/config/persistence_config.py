"""Configuration classes for persistence layer."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum

from .base_config import BaseConfig, ConfigValidator


class PersistenceBackend(Enum):
    """Supported persistence backends."""
    LAKEHOUSE = "lakehouse"
    MEMORY = "memory"  
    DATABASE = "database"
    FILE_SYSTEM = "file_system"


class TableFormat(Enum):
    """Supported table formats for persistence."""
    DELTA = "delta"
    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class CompressionType(Enum):
    """Supported compression types."""
    NONE = "none"
    SNAPPY = "snappy"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"


@dataclass
class TableConfig:
    """Configuration for individual persistence tables."""
    table_name: str
    format: TableFormat = TableFormat.DELTA
    compression: CompressionType = CompressionType.SNAPPY
    partition_columns: List[str] = field(default_factory=list)
    write_mode: str = "append"  # append, overwrite, merge
    retention_days: Optional[int] = None
    enable_versioning: bool = True
    optimize_on_write: bool = True
    custom_properties: Dict[str, str] = field(default_factory=dict)
    
    def validate(self) -> List[str]:
        """Validate table configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_required(self.table_name, "table_name"))
        errors.extend(ConfigValidator.validate_choices(
            self.write_mode, ["append", "overwrite", "merge"], "write_mode"
        ))
        
        if self.retention_days is not None:
            errors.extend(ConfigValidator.validate_range(
                self.retention_days, min_val=1, field_name="retention_days"
            ))
        
        return errors


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    connection_string: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    driver: Optional[str] = None
    connection_pool_size: int = 5
    connection_timeout: int = 30
    custom_properties: Dict[str, str] = field(default_factory=dict)
    
    def validate(self) -> List[str]:
        """Validate connection configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_range(
            self.connection_pool_size, min_val=1, field_name="connection_pool_size"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.connection_timeout, min_val=1, field_name="connection_timeout"
        ))
        
        if self.port is not None:
            errors.extend(ConfigValidator.validate_range(
                self.port, min_val=1, max_val=65535, field_name="port"
            ))
        
        return errors


class PersistenceConfig(BaseConfig):
    """Main configuration for persistence layer."""
    
    def __init__(self):
        """Initialize persistence configuration with defaults."""
        super().__init__()
        
        # Basic settings
        self.backend: PersistenceBackend = PersistenceBackend.LAKEHOUSE
        self.enabled: bool = True
        self.async_writes: bool = True
        self.batch_size: int = 1000
        self.max_retries: int = 3
        self.retry_delay_seconds: float = 1.0
        
        # Storage settings
        self.base_path: Optional[str] = None
        self.workspace_id: Optional[str] = None
        self.lakehouse_id: Optional[str] = None
        
        # Table configurations
        self.tables: Dict[str, TableConfig] = {
            "metadata": TableConfig(
                table_name="tiered_profile_metadata",
                format=TableFormat.DELTA,
                write_mode="append",
                partition_columns=["run_date"],
                retention_days=90
            ),
            "schemas": TableConfig(
                table_name="tiered_profile_schemas", 
                format=TableFormat.DELTA,
                write_mode="append",
                partition_columns=["run_date"],
                retention_days=30
            ),
            "profiles": TableConfig(
                table_name="tiered_profile_profiles",
                format=TableFormat.DELTA,
                write_mode="append", 
                partition_columns=["run_date", "table_name"],
                retention_days=365
            ),
            "progress": TableConfig(
                table_name="tiered_profile_progress",
                format=TableFormat.DELTA,
                write_mode="overwrite",  # Always overwrite progress
                retention_days=7
            )
        }
        
        # Connection settings (for database backend)
        self.connection: ConnectionConfig = ConnectionConfig()
        
        # Performance settings
        self.write_parallelism: int = 4
        self.checkpoint_interval: int = 100
        self.enable_compression: bool = True
        self.enable_statistics: bool = True
        
        # Cleanup settings
        self.auto_cleanup: bool = True
        self.cleanup_schedule: str = "daily"  # daily, weekly, monthly
        self.vacuum_retention_hours: int = 168  # 7 days
        
        # Monitoring settings
        self.enable_metrics: bool = True
        self.metrics_retention_days: int = 30
        
        # Advanced settings
        self.transaction_isolation: str = "READ_COMMITTED"
        self.enable_schema_evolution: bool = True
        self.custom_properties: Dict[str, Any] = {}
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "backend": PersistenceBackend.LAKEHOUSE.value,
            "enabled": True,
            "async_writes": True,
            "batch_size": 1000,
            "max_retries": 3,
            "write_parallelism": 4,
            "auto_cleanup": True,
            "cleanup_schedule": "daily",
            "vacuum_retention_hours": 168,
            "enable_metrics": True
        }
    
    def validate(self) -> bool:
        """Validate persistence configuration."""
        errors = []
        
        # Validate basic settings
        errors.extend(ConfigValidator.validate_choices(
            self.backend, list(PersistenceBackend), "backend"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.batch_size, min_val=1, field_name="batch_size"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.max_retries, min_val=0, field_name="max_retries"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.retry_delay_seconds, min_val=0.0, field_name="retry_delay_seconds"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.write_parallelism, min_val=1, field_name="write_parallelism"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.checkpoint_interval, min_val=1, field_name="checkpoint_interval"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.vacuum_retention_hours, min_val=1, field_name="vacuum_retention_hours"
        ))
        errors.extend(ConfigValidator.validate_choices(
            self.cleanup_schedule, ["daily", "weekly", "monthly", "disabled"], "cleanup_schedule"
        ))
        errors.extend(ConfigValidator.validate_choices(
            self.transaction_isolation, 
            ["READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"],
            "transaction_isolation"
        ))
        
        # Backend-specific validation
        if self.backend == PersistenceBackend.LAKEHOUSE:
            if not self.workspace_id:
                errors.append("workspace_id is required for lakehouse backend")
            if not self.lakehouse_id:
                errors.append("lakehouse_id is required for lakehouse backend")
        
        elif self.backend == PersistenceBackend.DATABASE:
            connection_errors = self.connection.validate()
            errors.extend([f"connection.{error}" for error in connection_errors])
            
            if not (self.connection.connection_string or 
                   (self.connection.host and self.connection.database)):
                errors.append("Either connection_string or (host + database) required for database backend")
        
        elif self.backend == PersistenceBackend.FILE_SYSTEM:
            if not self.base_path:
                errors.append("base_path is required for file_system backend")
        
        # Validate table configurations
        for table_key, table_config in self.tables.items():
            table_errors = table_config.validate()
            errors.extend([f"tables.{table_key}.{error}" for error in table_errors])
        
        # Store validation results
        self._metadata.validation_errors = errors
        
        return len(errors) == 0
    
    def get_table_config(self, table_key: str) -> Optional[TableConfig]:
        """Get configuration for a specific table."""
        return self.tables.get(table_key)
    
    def add_table_config(self, table_key: str, config: TableConfig):
        """Add or update table configuration."""
        self.tables[table_key] = config
    
    def configure_for_backend(self, backend: PersistenceBackend):
        """Configure persistence for a specific backend."""
        self.backend = backend
        
        if backend == PersistenceBackend.LAKEHOUSE:
            # Lakehouse-optimized settings
            self.async_writes = True
            self.batch_size = 10000
            self.write_parallelism = 8
            self.enable_compression = True
            
            # Use Delta format for all tables
            for table_config in self.tables.values():
                table_config.format = TableFormat.DELTA
                table_config.compression = CompressionType.SNAPPY
                table_config.optimize_on_write = True
        
        elif backend == PersistenceBackend.MEMORY:
            # Memory-optimized settings
            self.async_writes = False
            self.batch_size = 100
            self.write_parallelism = 1
            self.enable_compression = False
            self.auto_cleanup = False
        
        elif backend == PersistenceBackend.DATABASE:
            # Database-optimized settings
            self.async_writes = True
            self.batch_size = 5000
            self.write_parallelism = 4
            self.enable_compression = False
            
        elif backend == PersistenceBackend.FILE_SYSTEM:
            # File system-optimized settings
            self.async_writes = True
            self.batch_size = 1000
            self.write_parallelism = 2
            
            # Use Parquet for file system
            for table_config in self.tables.values():
                table_config.format = TableFormat.PARQUET
                table_config.compression = CompressionType.SNAPPY
    
    def get_full_table_name(self, table_key: str) -> str:
        """Get the full table name including any prefixes."""
        table_config = self.get_table_config(table_key)
        if not table_config:
            raise ValueError(f"Unknown table key: {table_key}")
        
        return table_config.table_name
    
    def get_table_path(self, table_key: str) -> str:
        """Get the full path for a table based on backend."""
        table_config = self.get_table_config(table_key)
        if not table_config:
            raise ValueError(f"Unknown table key: {table_key}")
        
        if self.backend == PersistenceBackend.LAKEHOUSE:
            if self.workspace_id and self.lakehouse_id:
                return f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}/Tables/{table_config.table_name}"
            else:
                return f"Tables/{table_config.table_name}"
        
        elif self.backend == PersistenceBackend.FILE_SYSTEM:
            if self.base_path:
                return f"{self.base_path}/{table_config.table_name}"
            else:
                return table_config.table_name
        
        else:
            return table_config.table_name
    
    def should_partition_table(self, table_key: str) -> bool:
        """Check if a table should be partitioned."""
        table_config = self.get_table_config(table_key)
        return table_config and len(table_config.partition_columns) > 0
    
    def get_write_options(self, table_key: str) -> Dict[str, str]:
        """Get write options for a table."""
        table_config = self.get_table_config(table_key)
        if not table_config:
            return {}
        
        options = {
            "mode": table_config.write_mode,
            "format": table_config.format.value
        }
        
        if table_config.compression != CompressionType.NONE:
            options["compression"] = table_config.compression.value
        
        if table_config.optimize_on_write:
            options["optimizeWrite"] = "true"
        
        if table_config.enable_versioning and table_config.format == TableFormat.DELTA:
            options["versionAsOf"] = "latest"
        
        # Add custom properties
        options.update(table_config.custom_properties)
        
        return options
    
    def estimate_storage_requirements(self, estimated_rows_per_day: int) -> Dict[str, str]:
        """Estimate storage requirements based on daily row counts."""
        estimates = {}
        
        # Rough estimates for each table type
        table_estimates = {
            "metadata": estimated_rows_per_day * 0.001,  # 1KB per metadata row
            "schemas": estimated_rows_per_day * 0.002,   # 2KB per schema row  
            "profiles": estimated_rows_per_day * 0.01,   # 10KB per profile row
            "progress": estimated_rows_per_day * 0.0005  # 0.5KB per progress row
        }
        
        total_daily_gb = 0
        for table_key, daily_gb in table_estimates.items():
            table_config = self.get_table_config(table_key)
            if table_config and table_config.retention_days:
                total_gb = daily_gb * table_config.retention_days
                estimates[table_key] = f"{total_gb:.2f} GB"
                total_daily_gb += daily_gb
        
        estimates["total_daily"] = f"{total_daily_gb:.2f} GB/day"
        estimates["total_monthly"] = f"{total_daily_gb * 30:.2f} GB/month"
        
        return estimates