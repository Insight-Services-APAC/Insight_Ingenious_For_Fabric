"""Result dataclasses for the export framework."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from ingen_fab.python_libs.pyspark.export.common.constants import ExecutionStatus


@dataclass
class ExportMetrics:
    """Metrics for an export operation."""

    rows_exported: int = 0
    files_created: int = 0
    total_bytes: int = 0
    duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rows_exported": self.rows_exported,
            "files_created": self.files_created,
            "total_bytes": self.total_bytes,
            "duration_ms": self.duration_ms,
        }


@dataclass
class ExportResult:
    """Result of an export operation."""

    export_group_name: str
    export_name: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    metrics: ExportMetrics = field(default_factory=ExportMetrics)
    file_paths: List[str] = field(default_factory=list)
    trigger_file_path: Optional[str] = None
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "export_group_name": self.export_group_name,
            "export_name": self.export_name,
            "status": str(self.status),
            "metrics": self.metrics.to_dict(),
            "file_paths": self.file_paths,
            "trigger_file_path": self.trigger_file_path,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }
