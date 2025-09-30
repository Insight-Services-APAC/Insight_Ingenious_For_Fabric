"""
Date-Based File Path Generation Utilities

This module provides flexible file path generation for synthetic data with support
for various date-based naming patterns and folder structures.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class FilePathTemplate:
    """Template for generating file paths with date-based patterns."""

    pattern_name: str
    path_template: str
    description: str
    supports_partitioning: bool = False
    partition_columns: List[str] = None

    def __post_init__(self):
        if self.partition_columns is None:
            self.partition_columns = []


class DateBasedFilePathGenerator:
    """Generator for date-based file paths with flexible patterns."""

    # Predefined path patterns
    PATTERNS = {
        "nested_daily": FilePathTemplate(
            pattern_name="nested_daily",
            path_template="{base_path}/{yyyy}/{mm}/{dd}/{table_name}",
            description="Nested daily folders: /2024/01/15/table_name",
            supports_partitioning=True,
            partition_columns=["year", "month", "day"],
        ),
        "nested_monthly": FilePathTemplate(
            pattern_name="nested_monthly",
            path_template="{base_path}/{yyyy}/{mm}/{table_name}_{yyyymmdd}",
            description="Nested monthly with date in filename: /2024/01/table_name_20240115",
            supports_partitioning=True,
            partition_columns=["year", "month"],
        ),
        "nested_yearly": FilePathTemplate(
            pattern_name="nested_yearly",
            path_template="{base_path}/{yyyy}/{table_name}_{yyyymmdd}",
            description="Nested yearly with date in filename: /2024/table_name_20240115",
            supports_partitioning=True,
            partition_columns=["year"],
        ),
        "flat_with_date": FilePathTemplate(
            pattern_name="flat_with_date",
            path_template="{base_path}/{yyyymmdd}_{table_name}",
            description="Flat structure with date prefix: /20240115_table_name",
            supports_partitioning=False,
        ),
        "flat_with_date_suffix": FilePathTemplate(
            pattern_name="flat_with_date_suffix",
            path_template="{base_path}/{table_name}_{yyyymmdd}",
            description="Flat structure with date suffix: /table_name_20240115",
            supports_partitioning=False,
        ),
        "hive_partitioned": FilePathTemplate(
            pattern_name="hive_partitioned",
            path_template="{base_path}/year={yyyy}/month={mm}/day={dd}/{table_name}",
            description="Hive-style partitioning: /year=2024/month=01/day=15/table_name",
            supports_partitioning=True,
            partition_columns=["year", "month", "day"],
        ),
        "hive_monthly": FilePathTemplate(
            pattern_name="hive_monthly",
            path_template="{base_path}/year={yyyy}/month={mm}/{table_name}_{yyyymmdd}",
            description="Hive monthly partitioning: /year=2024/month=01/table_name_20240115",
            supports_partitioning=True,
            partition_columns=["year", "month"],
        ),
        "iso_week": FilePathTemplate(
            pattern_name="iso_week",
            path_template="{base_path}/{yyyy}/week_{ww}/{table_name}_{yyyymmdd}",
            description="ISO week based: /2024/week_03/table_name_20240115",
            supports_partitioning=True,
            partition_columns=["year", "week"],
        ),
        "quarter_based": FilePathTemplate(
            pattern_name="quarter_based",
            path_template="{base_path}/{yyyy}/Q{q}/{table_name}_{yyyymmdd}",
            description="Quarter based: /2024/Q1/table_name_20240115",
            supports_partitioning=True,
            partition_columns=["year", "quarter"],
        ),
    }

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._custom_patterns = {}

    def add_custom_pattern(
        self,
        pattern_name: str,
        path_template: str,
        description: str = "",
        supports_partitioning: bool = False,
        partition_columns: List[str] = None,
    ):
        """Add a custom path pattern."""
        self._custom_patterns[pattern_name] = FilePathTemplate(
            pattern_name=pattern_name,
            path_template=path_template,
            description=description,
            supports_partitioning=supports_partitioning,
            partition_columns=partition_columns or [],
        )
        self.logger.info(f"Added custom pattern: {pattern_name}")

    def get_available_patterns(self) -> Dict[str, str]:
        """Get all available patterns with descriptions."""
        patterns = {}

        # Add predefined patterns
        for name, pattern in self.PATTERNS.items():
            patterns[name] = pattern.description

        # Add custom patterns
        for name, pattern in self._custom_patterns.items():
            patterns[name] = pattern.description

        return patterns

    def generate_path(
        self,
        pattern: str,
        base_path: str,
        table_name: str,
        generation_date: date,
        custom_pattern: str = None,
        file_extension: str = "parquet",
        include_extension: bool = True,
    ) -> str:
        """
        Generate file path based on pattern and date.

        Args:
            pattern: Pattern name or "custom"
            base_path: Base directory path
            table_name: Name of the table
            generation_date: Date for path generation
            custom_pattern: Custom pattern template (used when pattern="custom")
            file_extension: File extension to append
            include_extension: Whether to include the file extension

        Returns:
            Generated file path
        """
        if pattern == "custom" and custom_pattern:
            template = FilePathTemplate(
                pattern_name="custom",
                path_template=custom_pattern,
                description="Custom pattern",
            )
        else:
            template = self._get_pattern_template(pattern)

        if not template:
            raise ValueError(f"Unknown pattern: {pattern}")

        # Generate date components
        date_components = self._generate_date_components(generation_date)

        # Replace placeholders in template
        path = template.path_template.format(base_path=base_path.rstrip("/"), table_name=table_name, **date_components)

        # Add file extension if requested
        if include_extension and file_extension:
            if not file_extension.startswith("."):
                file_extension = f".{file_extension}"
            path += file_extension

        return path

    def generate_paths_for_date_range(
        self,
        pattern: str,
        base_path: str,
        table_name: str,
        start_date: date,
        end_date: date,
        file_extension: str = "parquet",
    ) -> List[str]:
        """Generate file paths for a date range."""
        paths = []
        current_date = start_date

        while current_date <= end_date:
            path = self.generate_path(
                pattern=pattern,
                base_path=base_path,
                table_name=table_name,
                generation_date=current_date,
                file_extension=file_extension,
            )
            paths.append(path)
            current_date += datetime.timedelta(days=1)

        return paths

    def generate_partition_info(self, pattern: str, generation_date: date) -> Dict[str, Any]:
        """Generate partition information for a given pattern and date."""
        template = self._get_pattern_template(pattern)

        if not template or not template.supports_partitioning:
            return {}

        date_components = self._generate_date_components(generation_date)
        partition_info = {}

        for col in template.partition_columns:
            if col == "year":
                partition_info["year"] = date_components["yyyy"]
            elif col == "month":
                partition_info["month"] = date_components["mm"]
            elif col == "day":
                partition_info["day"] = date_components["dd"]
            elif col == "quarter":
                partition_info["quarter"] = date_components["q"]
            elif col == "week":
                partition_info["week"] = date_components["ww"]

        return partition_info

    def validate_path_pattern(self, pattern_template: str) -> Tuple[bool, List[str]]:
        """Validate a path pattern template."""
        issues = []

        required_placeholders = ["{base_path}", "{table_name}"]
        for placeholder in required_placeholders:
            if placeholder not in pattern_template:
                issues.append(f"Missing required placeholder: {placeholder}")

        # Check for valid date placeholders
        valid_date_placeholders = [
            "{yyyy}",
            "{mm}",
            "{dd}",
            "{yyyymmdd}",
            "{q}",
            "{ww}",
            "year={yyyy}",
            "month={mm}",
            "day={dd}",
        ]

        has_date_component = any(placeholder in pattern_template for placeholder in valid_date_placeholders)
        if not has_date_component:
            issues.append("Pattern should include at least one date component")

        return len(issues) == 0, issues

    def extract_date_from_path(self, file_path: str, pattern: str, table_name: str) -> Optional[date]:
        """Extract generation date from a file path using the specified pattern."""
        template = self._get_pattern_template(pattern)
        if not template:
            return None

        try:
            # This is a simplified extraction - in practice, you'd want more robust parsing
            # For now, we'll look for YYYYMMDD patterns in the path
            import re

            # Look for 8-digit date patterns
            date_match = re.search(r"(\d{8})", file_path)
            if date_match:
                date_str = date_match.group(1)
                return datetime.strptime(date_str, "%Y%m%d").date()

            # Look for separated date components
            year_match = re.search(r"(\d{4})", file_path)
            if year_match:
                # This would require more sophisticated parsing based on the pattern
                # For now, return None for unsupported extraction
                pass

        except Exception as e:
            self.logger.warning(f"Could not extract date from path {file_path}: {e}")

        return None

    def organize_paths_by_date(self, file_paths: List[str], pattern: str, table_name: str) -> Dict[str, List[str]]:
        """Organize file paths by their generation dates."""
        organized = {}

        for path in file_paths:
            generation_date = self.extract_date_from_path(path, pattern, table_name)
            if generation_date:
                date_key = generation_date.isoformat()
                if date_key not in organized:
                    organized[date_key] = []
                organized[date_key].append(path)
            else:
                # Group unrecognized paths under "unknown"
                if "unknown" not in organized:
                    organized["unknown"] = []
                organized["unknown"].append(path)

        return organized

    def generate_directory_structure(self, pattern: str, base_path: str, generation_date: date) -> str:
        """Generate just the directory structure (without filename)."""
        template = self._get_pattern_template(pattern)
        if not template:
            raise ValueError(f"Unknown pattern: {pattern}")

        date_components = self._generate_date_components(generation_date)

        # Remove the table_name placeholder to get just directory structure
        dir_template = template.path_template.rsplit("/{table_name}", 1)[0]

        directory = dir_template.format(base_path=base_path.rstrip("/"), **date_components)

        return directory

    def _get_pattern_template(self, pattern: str) -> Optional[FilePathTemplate]:
        """Get pattern template by name."""
        if pattern in self.PATTERNS:
            return self.PATTERNS[pattern]
        elif pattern in self._custom_patterns:
            return self._custom_patterns[pattern]
        else:
            return None

    def _generate_date_components(self, generation_date: date) -> Dict[str, str]:
        """Generate all possible date components for template replacement."""
        return {
            "yyyy": generation_date.strftime("%Y"),
            "mm": generation_date.strftime("%m"),
            "dd": generation_date.strftime("%d"),
            "yyyymmdd": generation_date.strftime("%Y%m%d"),
            "yyyy_mm_dd": generation_date.strftime("%Y-%m-%d"),
            "q": str((generation_date.month - 1) // 3 + 1),  # Quarter
            "ww": generation_date.strftime("%U"),  # Week number (Sunday as first day)
            "iso_ww": generation_date.strftime("%W"),  # ISO week number (Monday as first day)
        }


class FilePathManager:
    """Manager for file path operations and validation."""

    def __init__(self, path_generator: DateBasedFilePathGenerator = None):
        self.path_generator = path_generator or DateBasedFilePathGenerator()
        self.logger = logging.getLogger(__name__)

    def ensure_directory_exists(self, file_path: str) -> str:
        """Ensure the directory for a file path exists."""
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
            self.logger.debug(f"Ensured directory exists: {directory}")
        return directory

    def validate_file_path(self, file_path: str) -> Tuple[bool, List[str]]:
        """Validate a file path for common issues."""
        issues = []

        # Check for invalid characters (basic check)
        invalid_chars = ["<", ">", ":", '"', "|", "?", "*"]
        for char in invalid_chars:
            if char in file_path:
                issues.append(f"Invalid character '{char}' in path")

        # Check path length (basic check for very long paths)
        if len(file_path) > 260:  # Windows MAX_PATH limitation
            issues.append("Path may be too long for some systems")

        # Check for double slashes
        if "//" in file_path or "\\\\" in file_path:
            issues.append("Path contains double slashes")

        return len(issues) == 0, issues

    def normalize_path(self, file_path: str) -> str:
        """Normalize a file path for consistency."""
        # Convert to forward slashes and remove double slashes
        normalized = file_path.replace("\\", "/").replace("//", "/")

        # Remove trailing slash if present
        if normalized.endswith("/") and len(normalized) > 1:
            normalized = normalized[:-1]

        return normalized

    def get_path_components(self, file_path: str) -> Dict[str, str]:
        """Extract components from a file path."""
        path_obj = Path(file_path)

        return {
            "directory": str(path_obj.parent),
            "filename": path_obj.name,
            "stem": path_obj.stem,
            "extension": path_obj.suffix,
            "parts": list(path_obj.parts),
        }

    def generate_backup_path(self, original_path: str) -> str:
        """Generate a backup path for an existing file."""
        path_obj = Path(original_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        backup_name = f"{path_obj.stem}_backup_{timestamp}{path_obj.suffix}"
        backup_path = path_obj.parent / backup_name

        return str(backup_path)

    def list_files_by_pattern(
        self,
        base_directory: str,
        pattern: str,
        table_name: str,
        start_date: date = None,
        end_date: date = None,
    ) -> List[str]:
        """List files matching a specific pattern and date range."""
        try:
            if not os.path.exists(base_directory):
                return []

            all_files = []
            for root, dirs, files in os.walk(base_directory):
                for file in files:
                    full_path = os.path.join(root, file)
                    all_files.append(full_path)

            # Filter by pattern and date range if specified
            filtered_files = []
            for file_path in all_files:
                if table_name in file_path:
                    if start_date and end_date:
                        file_date = self.path_generator.extract_date_from_path(file_path, pattern, table_name)
                        if file_date and start_date <= file_date <= end_date:
                            filtered_files.append(file_path)
                    else:
                        filtered_files.append(file_path)

            return filtered_files

        except Exception as e:
            self.logger.error(f"Error listing files: {e}")
            return []
