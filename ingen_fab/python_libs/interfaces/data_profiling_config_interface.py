"""Interface for data profiling configuration."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class DataProfilingConfig:
    """Configuration for data profiling tasks."""
    
    config_id: str
    table_name: str
    schema_name: Optional[str] = None
    target_workspace_id: Optional[str] = None
    target_datastore_id: Optional[str] = None
    target_datastore_type: Optional[str] = None  # 'lakehouse' or 'warehouse'
    profile_frequency: str = "daily"
    profile_type: str = "basic"
    sample_size: Optional[str] = None
    columns_to_profile: Optional[str] = None
    quality_thresholds_json: Optional[str] = None
    validation_rules_json: Optional[str] = None
    alert_enabled: bool = False
    alert_threshold: Optional[str] = None
    alert_recipients: Optional[str] = None
    last_profile_date: Optional[str] = None
    last_profile_status: Optional[str] = None
    execution_group: int = 1
    active_yn: str = "Y"
    created_date: Optional[str] = None
    modified_date: Optional[str] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None