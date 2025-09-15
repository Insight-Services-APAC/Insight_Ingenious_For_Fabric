"""Configuration classes for data quality assessment and thresholds."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, Tuple
from enum import Enum

from .base_config import BaseConfig, ConfigValidator


class QualityDimension(Enum):
    """Data quality dimensions."""
    COMPLETENESS = "completeness"       # Non-null percentage
    UNIQUENESS = "uniqueness"           # Distinct values percentage
    VALIDITY = "validity"               # Valid format/values percentage  
    CONSISTENCY = "consistency"         # Consistent with business rules
    ACCURACY = "accuracy"               # Correct values percentage
    TIMELINESS = "timeliness"          # Data freshness/recency
    INTEGRITY = "integrity"             # Referential integrity


class ThresholdLevel(Enum):
    """Quality threshold levels."""
    CRITICAL = "critical"    # Must be above this level
    WARNING = "warning"      # Should be above this level
    TARGET = "target"        # Ideal level to achieve


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class QualityThreshold:
    """Individual quality threshold configuration."""
    dimension: QualityDimension
    critical_threshold: float = 0.0    # Below this = critical alert
    warning_threshold: float = 0.0     # Below this = warning alert
    target_threshold: float = 1.0      # Above this = excellent quality
    enabled: bool = True
    description: str = ""
    custom_rules: List[str] = field(default_factory=list)
    
    def validate(self) -> List[str]:
        """Validate threshold configuration."""
        errors = []
        
        # Validate threshold ranges
        for threshold_name in ["critical_threshold", "warning_threshold", "target_threshold"]:
            threshold_value = getattr(self, threshold_name)
            errors.extend(ConfigValidator.validate_range(
                threshold_value, min_val=0.0, max_val=1.0, field_name=threshold_name
            ))
        
        # Validate threshold ordering
        if self.critical_threshold > self.warning_threshold:
            errors.append("critical_threshold cannot be greater than warning_threshold")
        
        if self.warning_threshold > self.target_threshold:
            errors.append("warning_threshold cannot be greater than target_threshold")
        
        return errors
    
    def assess_quality(self, value: float) -> Tuple[ThresholdLevel, AlertSeverity]:
        """Assess quality level for a given value."""
        if value < self.critical_threshold:
            return ThresholdLevel.CRITICAL, AlertSeverity.CRITICAL
        elif value < self.warning_threshold:
            return ThresholdLevel.WARNING, AlertSeverity.MEDIUM
        elif value >= self.target_threshold:
            return ThresholdLevel.TARGET, AlertSeverity.LOW
        else:
            return ThresholdLevel.WARNING, AlertSeverity.LOW


@dataclass
class OutlierDetectionConfig:
    """Configuration for outlier detection."""
    enabled: bool = True
    method: str = "iqr"  # iqr, z_score, isolation_forest
    iqr_multiplier: float = 1.5
    z_score_threshold: float = 3.0
    isolation_forest_contamination: float = 0.1
    max_outliers_percentage: float = 5.0  # Alert if more than this % are outliers
    
    def validate(self) -> List[str]:
        """Validate outlier detection configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_choices(
            self.method, ["iqr", "z_score", "isolation_forest"], "method"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.iqr_multiplier, min_val=0.1, field_name="iqr_multiplier"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.z_score_threshold, min_val=1.0, field_name="z_score_threshold"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.isolation_forest_contamination, min_val=0.0, max_val=0.5, 
            field_name="isolation_forest_contamination"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.max_outliers_percentage, min_val=0.0, max_val=100.0,
            field_name="max_outliers_percentage"
        ))
        
        return errors


@dataclass
class ValidationRule:
    """Individual data validation rule."""
    name: str
    description: str
    rule_type: str  # regex, range, list, custom
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: AlertSeverity = AlertSeverity.MEDIUM
    enabled: bool = True
    
    def validate(self) -> List[str]:
        """Validate validation rule configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_required(self.name, "name"))
        errors.extend(ConfigValidator.validate_choices(
            self.rule_type, ["regex", "range", "list", "custom", "sql"], "rule_type"
        ))
        
        # Rule-type specific validation
        if self.rule_type == "range":
            if "min" not in self.parameters and "max" not in self.parameters:
                errors.append("Range rule must specify 'min' or 'max' parameter")
        elif self.rule_type == "list":
            if "allowed_values" not in self.parameters:
                errors.append("List rule must specify 'allowed_values' parameter")
        elif self.rule_type == "regex":
            if "pattern" not in self.parameters:
                errors.append("Regex rule must specify 'pattern' parameter")
        elif self.rule_type == "custom":
            if "function" not in self.parameters:
                errors.append("Custom rule must specify 'function' parameter")
        elif self.rule_type == "sql":
            if "query" not in self.parameters:
                errors.append("SQL rule must specify 'query' parameter")
        
        return errors


@dataclass
class ColumnQualityConfig:
    """Quality configuration for individual columns."""
    column_name: str
    data_type: Optional[str] = None
    thresholds: Dict[QualityDimension, QualityThreshold] = field(default_factory=dict)
    validation_rules: List[ValidationRule] = field(default_factory=list)
    outlier_detection: Optional[OutlierDetectionConfig] = None
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> List[str]:
        """Validate column quality configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_required(self.column_name, "column_name"))
        
        # Validate thresholds
        for dimension, threshold in self.thresholds.items():
            threshold_errors = threshold.validate()
            errors.extend([f"thresholds.{dimension.value}.{error}" for error in threshold_errors])
        
        # Validate validation rules
        for i, rule in enumerate(self.validation_rules):
            rule_errors = rule.validate()
            errors.extend([f"validation_rules[{i}].{error}" for error in rule_errors])
        
        # Validate outlier detection
        if self.outlier_detection:
            outlier_errors = self.outlier_detection.validate()
            errors.extend([f"outlier_detection.{error}" for error in outlier_errors])
        
        return errors


class QualityConfig(BaseConfig):
    """Main configuration for data quality assessment."""
    
    def __init__(self):
        """Initialize quality configuration with defaults."""
        super().__init__()
        
        # Global settings
        self.enabled: bool = True
        self.assessment_mode: str = "comprehensive"  # basic, standard, comprehensive
        self.enable_alerts: bool = True
        self.alert_threshold: AlertSeverity = AlertSeverity.MEDIUM
        
        # Global thresholds (apply to all columns unless overridden)
        self.global_thresholds: Dict[QualityDimension, QualityThreshold] = {
            QualityDimension.COMPLETENESS: QualityThreshold(
                dimension=QualityDimension.COMPLETENESS,
                critical_threshold=0.50,
                warning_threshold=0.80,
                target_threshold=0.95,
                description="Percentage of non-null values"
            ),
            QualityDimension.UNIQUENESS: QualityThreshold(
                dimension=QualityDimension.UNIQUENESS,
                critical_threshold=0.10,
                warning_threshold=0.50,
                target_threshold=0.90,
                description="Percentage of unique values"
            ),
            QualityDimension.VALIDITY: QualityThreshold(
                dimension=QualityDimension.VALIDITY,
                critical_threshold=0.70,
                warning_threshold=0.90,
                target_threshold=0.98,
                description="Percentage of valid format values"
            ),
            QualityDimension.CONSISTENCY: QualityThreshold(
                dimension=QualityDimension.CONSISTENCY,
                critical_threshold=0.80,
                warning_threshold=0.90,
                target_threshold=0.95,
                description="Consistency with business rules"
            )
        }
        
        # Column-specific configurations
        self.column_configs: Dict[str, ColumnQualityConfig] = {}
        
        # Table-level settings
        self.table_level_checks: bool = True
        self.cross_column_validation: bool = True
        self.referential_integrity_checks: bool = True
        
        # Outlier detection settings
        self.global_outlier_detection: OutlierDetectionConfig = OutlierDetectionConfig()
        
        # Reporting settings
        self.generate_quality_report: bool = True
        self.quality_score_weights: Dict[QualityDimension, float] = {
            QualityDimension.COMPLETENESS: 0.3,
            QualityDimension.UNIQUENESS: 0.2,
            QualityDimension.VALIDITY: 0.3,
            QualityDimension.CONSISTENCY: 0.2
        }
        
        # Advanced settings
        self.enable_statistical_tests: bool = False
        self.enable_drift_detection: bool = False
        self.drift_threshold: float = 0.1  # 10% change triggers drift alert
        
        # Performance settings
        self.sample_for_quality_check: bool = True
        self.quality_check_sample_size: float = 0.1
        self.parallel_quality_checks: bool = True
        
        # Custom settings
        self.custom_quality_metrics: Dict[str, Dict[str, Any]] = {}
        self.business_rules: List[ValidationRule] = []
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "enabled": True,
            "assessment_mode": "comprehensive",
            "enable_alerts": True,
            "alert_threshold": AlertSeverity.MEDIUM.value,
            "table_level_checks": True,
            "cross_column_validation": True,
            "generate_quality_report": True,
            "sample_for_quality_check": True,
            "quality_check_sample_size": 0.1,
            "parallel_quality_checks": True
        }
    
    def validate(self) -> bool:
        """Validate quality configuration."""
        errors = []
        
        # Validate basic settings
        errors.extend(ConfigValidator.validate_choices(
            self.assessment_mode, ["basic", "standard", "comprehensive"], "assessment_mode"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.quality_check_sample_size, min_val=0.001, max_val=1.0, 
            field_name="quality_check_sample_size"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.drift_threshold, min_val=0.0, max_val=1.0, field_name="drift_threshold"
        ))
        
        # Validate global thresholds
        for dimension, threshold in self.global_thresholds.items():
            threshold_errors = threshold.validate()
            errors.extend([f"global_thresholds.{dimension.value}.{error}" for error in threshold_errors])
        
        # Validate quality score weights sum to 1.0
        total_weight = sum(self.quality_score_weights.values())
        if abs(total_weight - 1.0) > 0.01:  # Allow small floating point differences
            errors.append(f"Quality score weights must sum to 1.0, got {total_weight}")
        
        # Validate individual weights
        for dimension, weight in self.quality_score_weights.items():
            errors.extend(ConfigValidator.validate_range(
                weight, min_val=0.0, max_val=1.0, field_name=f"quality_score_weights.{dimension.value}"
            ))
        
        # Validate column configurations
        for column_name, column_config in self.column_configs.items():
            column_errors = column_config.validate()
            errors.extend([f"column_configs.{column_name}.{error}" for error in column_errors])
        
        # Validate global outlier detection
        outlier_errors = self.global_outlier_detection.validate()
        errors.extend([f"global_outlier_detection.{error}" for error in outlier_errors])
        
        # Validate business rules
        for i, rule in enumerate(self.business_rules):
            rule_errors = rule.validate()
            errors.extend([f"business_rules[{i}].{error}" for error in rule_errors])
        
        # Store validation results
        self._metadata.validation_errors = errors
        
        return len(errors) == 0
    
    def get_column_config(self, column_name: str) -> ColumnQualityConfig:
        """Get quality configuration for a specific column."""
        if column_name in self.column_configs:
            return self.column_configs[column_name]
        
        # Create default configuration
        return ColumnQualityConfig(
            column_name=column_name,
            thresholds=self.global_thresholds.copy(),
            outlier_detection=self.global_outlier_detection
        )
    
    def set_column_threshold(
        self, 
        column_name: str, 
        dimension: QualityDimension, 
        threshold: QualityThreshold
    ):
        """Set quality threshold for a specific column and dimension."""
        if column_name not in self.column_configs:
            self.column_configs[column_name] = ColumnQualityConfig(column_name=column_name)
        
        self.column_configs[column_name].thresholds[dimension] = threshold
    
    def add_validation_rule(self, column_name: str, rule: ValidationRule):
        """Add validation rule for a specific column."""
        if column_name not in self.column_configs:
            self.column_configs[column_name] = ColumnQualityConfig(column_name=column_name)
        
        self.column_configs[column_name].validation_rules.append(rule)
    
    def configure_for_mode(self, mode: str):
        """Configure quality assessment for specific mode."""
        if mode == "basic":
            # Basic quality checks only
            self.assessment_mode = "basic"
            self.table_level_checks = False
            self.cross_column_validation = False
            self.enable_statistical_tests = False
            self.sample_for_quality_check = True
            self.quality_check_sample_size = 0.05
            
            # Only check completeness and validity
            enabled_dimensions = [QualityDimension.COMPLETENESS, QualityDimension.VALIDITY]
            for dimension in list(self.global_thresholds.keys()):
                if dimension not in enabled_dimensions:
                    del self.global_thresholds[dimension]
        
        elif mode == "standard":
            # Standard quality checks
            self.assessment_mode = "standard"
            self.table_level_checks = True
            self.cross_column_validation = False
            self.enable_statistical_tests = False
            self.quality_check_sample_size = 0.1
        
        elif mode == "comprehensive":
            # Full quality assessment
            self.assessment_mode = "comprehensive"
            self.table_level_checks = True
            self.cross_column_validation = True
            self.enable_statistical_tests = True
            self.enable_drift_detection = True
            self.quality_check_sample_size = 0.2
    
    def get_effective_threshold(
        self, 
        column_name: str, 
        dimension: QualityDimension
    ) -> QualityThreshold:
        """Get effective threshold for a column and dimension."""
        column_config = self.get_column_config(column_name)
        
        # Return column-specific threshold if exists
        if dimension in column_config.thresholds:
            return column_config.thresholds[dimension]
        
        # Return global threshold
        if dimension in self.global_thresholds:
            return self.global_thresholds[dimension]
        
        # Return default threshold
        return QualityThreshold(dimension=dimension)
    
    def calculate_quality_score(
        self, 
        quality_metrics: Dict[QualityDimension, float]
    ) -> float:
        """Calculate overall quality score based on individual metrics."""
        total_score = 0.0
        total_weight = 0.0
        
        for dimension, metric_value in quality_metrics.items():
            if dimension in self.quality_score_weights:
                weight = self.quality_score_weights[dimension]
                threshold = self.global_thresholds.get(dimension)
                
                if threshold:
                    # Normalize score based on thresholds
                    if metric_value >= threshold.target_threshold:
                        normalized_score = 100.0
                    elif metric_value >= threshold.warning_threshold:
                        # Linear interpolation between warning and target
                        range_size = threshold.target_threshold - threshold.warning_threshold
                        score_range = 100.0 - 80.0
                        normalized_score = 80.0 + (metric_value - threshold.warning_threshold) / range_size * score_range
                    elif metric_value >= threshold.critical_threshold:
                        # Linear interpolation between critical and warning
                        range_size = threshold.warning_threshold - threshold.critical_threshold
                        score_range = 80.0 - 40.0
                        normalized_score = 40.0 + (metric_value - threshold.critical_threshold) / range_size * score_range
                    else:
                        # Below critical threshold
                        normalized_score = metric_value / threshold.critical_threshold * 40.0
                else:
                    # No threshold defined, use metric value directly
                    normalized_score = metric_value * 100.0
                
                total_score += normalized_score * weight
                total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def get_alert_rules(self) -> List[Dict[str, Any]]:
        """Get list of alert rules based on configuration."""
        rules = []
        
        for dimension, threshold in self.global_thresholds.items():
            if threshold.enabled:
                rules.append({
                    "dimension": dimension.value,
                    "type": "threshold",
                    "critical_threshold": threshold.critical_threshold,
                    "warning_threshold": threshold.warning_threshold,
                    "description": threshold.description
                })
        
        # Add business rules
        for rule in self.business_rules:
            if rule.enabled:
                rules.append({
                    "name": rule.name,
                    "type": "business_rule",
                    "rule_type": rule.rule_type,
                    "severity": rule.severity.value,
                    "description": rule.description,
                    "parameters": rule.parameters
                })
        
        return rules