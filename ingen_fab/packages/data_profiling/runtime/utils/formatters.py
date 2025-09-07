"""Report formatting utilities for data profiling output."""

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional


class OutputFormat(Enum):
    """Supported output formats for profiling reports."""
    JSON = "json"
    CSV = "csv" 
    HTML = "html"
    MARKDOWN = "markdown"
    PLAIN_TEXT = "text"


class ProfileReportFormatter:
    """Main formatter for profiling reports."""
    
    @staticmethod
    def format_profile_report(
        profile_data: Dict[str, Any],
        format_type: OutputFormat = OutputFormat.JSON,
        include_metadata: bool = True,
        pretty_print: bool = True
    ) -> str:
        """
        Format a complete profile report in the specified format.
        
        Args:
            profile_data: Dictionary containing profile results
            format_type: Output format
            include_metadata: Whether to include metadata sections
            pretty_print: Whether to format for readability
            
        Returns:
            Formatted report string
        """
        if format_type == OutputFormat.JSON:
            return ProfileReportFormatter._format_json(profile_data, pretty_print)
        elif format_type == OutputFormat.MARKDOWN:
            return ProfileReportFormatter._format_markdown(profile_data, include_metadata)
        elif format_type == OutputFormat.HTML:
            return ProfileReportFormatter._format_html(profile_data, include_metadata)
        elif format_type == OutputFormat.PLAIN_TEXT:
            return ProfileReportFormatter._format_text(profile_data, include_metadata)
        elif format_type == OutputFormat.CSV:
            return ProfileReportFormatter._format_csv(profile_data)
        else:
            raise ValueError(f"Unsupported format type: {format_type}")
    
    @staticmethod
    def _format_json(data: Dict[str, Any], pretty_print: bool = True) -> str:
        """Format data as JSON."""
        # Custom serializer for special types
        def json_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return float(obj)
            elif hasattr(obj, '__dict__'):
                return obj.__dict__
            return str(obj)
        
        if pretty_print:
            return json.dumps(data, indent=2, default=json_serializer, ensure_ascii=False)
        else:
            return json.dumps(data, default=json_serializer, ensure_ascii=False)
    
    @staticmethod
    def _format_markdown(data: Dict[str, Any], include_metadata: bool = True) -> str:
        """Format data as Markdown."""
        lines = ["# Data Profile Report\n"]
        
        if include_metadata and "metadata" in data:
            lines.extend(ProfileReportFormatter._format_metadata_markdown(data["metadata"]))
        
        if "table_summary" in data:
            lines.extend(ProfileReportFormatter._format_table_summary_markdown(data["table_summary"]))
        
        if "column_profiles" in data:
            lines.extend(ProfileReportFormatter._format_column_profiles_markdown(data["column_profiles"]))
        
        if "data_quality" in data:
            lines.extend(ProfileReportFormatter._format_data_quality_markdown(data["data_quality"]))
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_html(data: Dict[str, Any], include_metadata: bool = True) -> str:
        """Format data as HTML."""
        html_parts = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<title>Data Profile Report</title>",
            "<style>",
            ProfileReportFormatter._get_html_css(),
            "</style>",
            "</head>",
            "<body>",
            "<h1>Data Profile Report</h1>"
        ]
        
        if include_metadata and "metadata" in data:
            html_parts.extend(ProfileReportFormatter._format_metadata_html(data["metadata"]))
        
        if "table_summary" in data:
            html_parts.extend(ProfileReportFormatter._format_table_summary_html(data["table_summary"]))
        
        if "column_profiles" in data:
            html_parts.extend(ProfileReportFormatter._format_column_profiles_html(data["column_profiles"]))
        
        if "data_quality" in data:
            html_parts.extend(ProfileReportFormatter._format_data_quality_html(data["data_quality"]))
        
        html_parts.extend(["</body>", "</html>"])
        return "\n".join(html_parts)
    
    @staticmethod
    def _format_text(data: Dict[str, Any], include_metadata: bool = True) -> str:
        """Format data as plain text."""
        lines = ["DATA PROFILE REPORT", "=" * 50, ""]
        
        if include_metadata and "metadata" in data:
            lines.extend(ProfileReportFormatter._format_metadata_text(data["metadata"]))
        
        if "table_summary" in data:
            lines.extend(ProfileReportFormatter._format_table_summary_text(data["table_summary"]))
        
        if "column_profiles" in data:
            lines.extend(ProfileReportFormatter._format_column_profiles_text(data["column_profiles"]))
        
        if "data_quality" in data:
            lines.extend(ProfileReportFormatter._format_data_quality_text(data["data_quality"]))
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_csv(data: Dict[str, Any]) -> str:
        """Format column profiles as CSV."""
        if "column_profiles" not in data:
            return "No column profile data available"
        
        lines = ["Column,Data_Type,Count,Null_Count,Distinct_Count,Min,Max,Mean,Std_Dev"]
        
        for column_name, profile in data["column_profiles"].items():
            csv_row = [
                column_name,
                profile.get("data_type", ""),
                str(profile.get("count", "")),
                str(profile.get("null_count", "")),
                str(profile.get("distinct_count", "")),
                str(profile.get("min", "")),
                str(profile.get("max", "")),
                str(profile.get("mean", "")),
                str(profile.get("std_dev", ""))
            ]
            lines.append(",".join(csv_row))
        
        return "\n".join(lines)
    
    # Metadata formatting methods
    @staticmethod
    def _format_metadata_markdown(metadata: Dict[str, Any]) -> List[str]:
        """Format metadata section as Markdown."""
        lines = ["## Metadata\n"]
        
        for key, value in metadata.items():
            lines.append(f"**{key.replace('_', ' ').title()}:** {value}")
        
        lines.append("")
        return lines
    
    @staticmethod
    def _format_metadata_html(metadata: Dict[str, Any]) -> List[str]:
        """Format metadata section as HTML."""
        lines = ["<h2>Metadata</h2>", "<table class='metadata-table'>"]
        
        for key, value in metadata.items():
            key_formatted = key.replace('_', ' ').title()
            lines.append(f"<tr><td><strong>{key_formatted}</strong></td><td>{value}</td></tr>")
        
        lines.extend(["</table>", ""])
        return lines
    
    @staticmethod
    def _format_metadata_text(metadata: Dict[str, Any]) -> List[str]:
        """Format metadata section as plain text."""
        lines = ["METADATA", "-" * 20, ""]
        
        for key, value in metadata.items():
            key_formatted = key.replace('_', ' ').title()
            lines.append(f"{key_formatted}: {value}")
        
        lines.append("")
        return lines
    
    # Table summary formatting methods
    @staticmethod
    def _format_table_summary_markdown(summary: Dict[str, Any]) -> List[str]:
        """Format table summary as Markdown."""
        lines = ["## Table Summary\n"]
        
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        
        for key, value in summary.items():
            key_formatted = key.replace('_', ' ').title()
            lines.append(f"| {key_formatted} | {value} |")
        
        lines.append("")
        return lines
    
    @staticmethod
    def _format_table_summary_html(summary: Dict[str, Any]) -> List[str]:
        """Format table summary as HTML."""
        lines = ["<h2>Table Summary</h2>", "<table class='summary-table'>"]
        lines.append("<tr><th>Metric</th><th>Value</th></tr>")
        
        for key, value in summary.items():
            key_formatted = key.replace('_', ' ').title()
            lines.append(f"<tr><td>{key_formatted}</td><td>{value}</td></tr>")
        
        lines.extend(["</table>", ""])
        return lines
    
    @staticmethod
    def _format_table_summary_text(summary: Dict[str, Any]) -> List[str]:
        """Format table summary as plain text."""
        lines = ["TABLE SUMMARY", "-" * 20, ""]
        
        for key, value in summary.items():
            key_formatted = key.replace('_', ' ').title()
            lines.append(f"{key_formatted}: {value}")
        
        lines.append("")
        return lines
    
    # Column profiles formatting methods
    @staticmethod
    def _format_column_profiles_markdown(profiles: Dict[str, Any]) -> List[str]:
        """Format column profiles as Markdown."""
        lines = ["## Column Profiles\n"]
        
        for column_name, profile in profiles.items():
            lines.append(f"### {column_name}\n")
            lines.append("| Metric | Value |")
            lines.append("|--------|-------|")
            
            for key, value in profile.items():
                if key != "histogram" and key != "frequency_distribution":  # Skip complex nested data
                    key_formatted = key.replace('_', ' ').title()
                    formatted_value = ProfileReportFormatter._format_value(value)
                    lines.append(f"| {key_formatted} | {formatted_value} |")
            
            lines.append("")
        
        return lines
    
    @staticmethod
    def _format_column_profiles_html(profiles: Dict[str, Any]) -> List[str]:
        """Format column profiles as HTML."""
        lines = ["<h2>Column Profiles</h2>"]
        
        for column_name, profile in profiles.items():
            lines.append(f"<h3>{column_name}</h3>")
            lines.append("<table class='profile-table'>")
            lines.append("<tr><th>Metric</th><th>Value</th></tr>")
            
            for key, value in profile.items():
                if key != "histogram" and key != "frequency_distribution":  # Skip complex nested data
                    key_formatted = key.replace('_', ' ').title()
                    formatted_value = ProfileReportFormatter._format_value(value)
                    lines.append(f"<tr><td>{key_formatted}</td><td>{formatted_value}</td></tr>")
            
            lines.extend(["</table>", ""])
        
        return lines
    
    @staticmethod
    def _format_column_profiles_text(profiles: Dict[str, Any]) -> List[str]:
        """Format column profiles as plain text."""
        lines = ["COLUMN PROFILES", "-" * 20, ""]
        
        for column_name, profile in profiles.items():
            lines.append(f"Column: {column_name}")
            lines.append("~" * (len(column_name) + 8))
            
            for key, value in profile.items():
                if key != "histogram" and key != "frequency_distribution":  # Skip complex nested data
                    key_formatted = key.replace('_', ' ').title()
                    formatted_value = ProfileReportFormatter._format_value(value)
                    lines.append(f"  {key_formatted}: {formatted_value}")
            
            lines.append("")
        
        return lines
    
    # Data quality formatting methods
    @staticmethod
    def _format_data_quality_markdown(quality: Dict[str, Any]) -> List[str]:
        """Format data quality section as Markdown."""
        lines = ["## Data Quality Assessment\n"]
        
        if "overall_score" in quality:
            lines.append(f"**Overall Quality Score:** {quality['overall_score']:.1f}%\n")
        
        if "column_scores" in quality:
            lines.append("### Column Quality Scores\n")
            lines.append("| Column | Score | Issues |")
            lines.append("|--------|-------|--------|")
            
            for column, score_data in quality["column_scores"].items():
                score = score_data.get("score", 0)
                issues = ", ".join(score_data.get("issues", []))
                lines.append(f"| {column} | {score:.1f}% | {issues or 'None'} |")
        
        lines.append("")
        return lines
    
    @staticmethod
    def _format_data_quality_html(quality: Dict[str, Any]) -> List[str]:
        """Format data quality section as HTML."""
        lines = ["<h2>Data Quality Assessment</h2>"]
        
        if "overall_score" in quality:
            score = quality["overall_score"]
            score_class = "high" if score >= 80 else "medium" if score >= 60 else "low"
            lines.append(f"<p class='quality-score quality-{score_class}'>")
            lines.append(f"<strong>Overall Quality Score:</strong> {score:.1f}%")
            lines.append("</p>")
        
        if "column_scores" in quality:
            lines.extend([
                "<h3>Column Quality Scores</h3>",
                "<table class='quality-table'>",
                "<tr><th>Column</th><th>Score</th><th>Issues</th></tr>"
            ])
            
            for column, score_data in quality["column_scores"].items():
                score = score_data.get("score", 0)
                issues = ", ".join(score_data.get("issues", []))
                score_class = "high" if score >= 80 else "medium" if score >= 60 else "low"
                lines.append(f"<tr><td>{column}</td><td class='quality-{score_class}'>{score:.1f}%</td><td>{issues or 'None'}</td></tr>")
            
            lines.append("</table>")
        
        lines.append("")
        return lines
    
    @staticmethod
    def _format_data_quality_text(quality: Dict[str, Any]) -> List[str]:
        """Format data quality section as plain text."""
        lines = ["DATA QUALITY ASSESSMENT", "-" * 30, ""]
        
        if "overall_score" in quality:
            lines.append(f"Overall Quality Score: {quality['overall_score']:.1f}%")
            lines.append("")
        
        if "column_scores" in quality:
            lines.append("Column Quality Scores:")
            lines.append("~" * 22)
            
            for column, score_data in quality["column_scores"].items():
                score = score_data.get("score", 0)
                issues = score_data.get("issues", [])
                lines.append(f"  {column}: {score:.1f}%")
                if issues:
                    lines.append(f"    Issues: {', '.join(issues)}")
            
            lines.append("")
        
        return lines
    
    @staticmethod
    def _format_value(value: Any) -> str:
        """Format a value for display."""
        if value is None:
            return "null"
        elif isinstance(value, float):
            if value.is_integer():
                return str(int(value))
            else:
                return f"{value:.3f}"
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, bool):
            return str(value).lower()
        else:
            return str(value)
    
    @staticmethod
    def _get_html_css() -> str:
        """Get CSS styles for HTML reports."""
        return """
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2, h3 { color: #333; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; font-weight: bold; }
        .metadata-table { max-width: 600px; }
        .summary-table { max-width: 400px; }
        .profile-table { max-width: 500px; }
        .quality-table { max-width: 700px; }
        .quality-score { font-size: 18px; margin: 20px 0; }
        .quality-high { color: #28a745; }
        .quality-medium { color: #ffc107; }
        .quality-low { color: #dc3545; }
        """


class DataQualityFormatter:
    """Specialized formatter for data quality reports."""
    
    @staticmethod
    def format_quality_summary(
        quality_metrics: Dict[str, Any],
        threshold_config: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        Format data quality metrics into a standardized summary.
        
        Args:
            quality_metrics: Dictionary of quality metrics
            threshold_config: Optional thresholds for quality assessment
            
        Returns:
            Formatted quality summary
        """
        if threshold_config is None:
            threshold_config = {
                "completeness": 95.0,
                "uniqueness": 90.0,
                "validity": 98.0,
                "consistency": 95.0
            }
        
        summary = {
            "overall_assessment": "Unknown",
            "overall_score": 0.0,
            "metrics": {},
            "recommendations": []
        }
        
        total_score = 0
        metric_count = 0
        
        for metric_name, metric_value in quality_metrics.items():
            if isinstance(metric_value, (int, float)):
                # Determine status based on threshold
                threshold = threshold_config.get(metric_name, 80.0)
                
                if metric_value >= threshold:
                    status = "Good"
                elif metric_value >= (threshold * 0.8):
                    status = "Fair"
                else:
                    status = "Poor"
                
                summary["metrics"][metric_name] = {
                    "value": metric_value,
                    "threshold": threshold,
                    "status": status,
                    "percentage": f"{metric_value:.1f}%"
                }
                
                total_score += metric_value
                metric_count += 1
                
                # Add recommendations for poor metrics
                if status == "Poor":
                    summary["recommendations"].append(
                        f"Improve {metric_name.replace('_', ' ')} (currently {metric_value:.1f}%, target: {threshold:.1f}%)"
                    )
        
        # Calculate overall score and assessment
        if metric_count > 0:
            summary["overall_score"] = total_score / metric_count
            
            if summary["overall_score"] >= 90:
                summary["overall_assessment"] = "Excellent"
            elif summary["overall_score"] >= 80:
                summary["overall_assessment"] = "Good"
            elif summary["overall_score"] >= 70:
                summary["overall_assessment"] = "Fair"
            else:
                summary["overall_assessment"] = "Poor"
        
        return summary
    
    @staticmethod
    def create_quality_report_card(column_profiles: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a quality report card for all columns.
        
        Args:
            column_profiles: Dictionary of column profiles
            
        Returns:
            Quality report card
        """
        report_card = {
            "total_columns": len(column_profiles),
            "column_assessments": {},
            "summary_statistics": {
                "excellent_columns": 0,
                "good_columns": 0,
                "fair_columns": 0,
                "poor_columns": 0
            }
        }
        
        for column_name, profile in column_profiles.items():
            # Calculate column quality score
            quality_metrics = {
                "completeness": 100 - profile.get("null_percentage", 100),
                "validity": profile.get("validity", 0)
            }
            
            if "distinct_count" in profile and "count" in profile and profile["count"] > 0:
                quality_metrics["uniqueness"] = (profile["distinct_count"] / profile["count"]) * 100
            
            quality_summary = DataQualityFormatter.format_quality_summary(quality_metrics)
            
            # Determine grade
            score = quality_summary["overall_score"]
            if score >= 90:
                grade = "A"
                report_card["summary_statistics"]["excellent_columns"] += 1
            elif score >= 80:
                grade = "B"
                report_card["summary_statistics"]["good_columns"] += 1
            elif score >= 70:
                grade = "C"
                report_card["summary_statistics"]["fair_columns"] += 1
            else:
                grade = "D"
                report_card["summary_statistics"]["poor_columns"] += 1
            
            report_card["column_assessments"][column_name] = {
                "grade": grade,
                "score": score,
                "assessment": quality_summary["overall_assessment"],
                "key_issues": quality_summary["recommendations"]
            }
        
        return report_card


class ComparisonFormatter:
    """Formatter for profile comparison reports."""
    
    @staticmethod
    def format_profile_comparison(
        baseline_profile: Dict[str, Any],
        current_profile: Dict[str, Any],
        comparison_type: str = "drift_detection"
    ) -> Dict[str, Any]:
        """
        Format a comparison between two profiles.
        
        Args:
            baseline_profile: Baseline profile data
            current_profile: Current profile data
            comparison_type: Type of comparison (drift_detection, quality_trend, etc.)
            
        Returns:
            Formatted comparison report
        """
        comparison = {
            "comparison_type": comparison_type,
            "summary": {},
            "column_changes": {},
            "significant_changes": [],
            "recommendations": []
        }
        
        # Compare table-level metrics
        if "table_summary" in baseline_profile and "table_summary" in current_profile:
            comparison["summary"] = ComparisonFormatter._compare_table_summaries(
                baseline_profile["table_summary"],
                current_profile["table_summary"]
            )
        
        # Compare column profiles
        if "column_profiles" in baseline_profile and "column_profiles" in current_profile:
            comparison["column_changes"] = ComparisonFormatter._compare_column_profiles(
                baseline_profile["column_profiles"],
                current_profile["column_profiles"]
            )
            
            # Identify significant changes
            comparison["significant_changes"] = ComparisonFormatter._identify_significant_changes(
                comparison["column_changes"]
            )
        
        return comparison
    
    @staticmethod
    def _compare_table_summaries(baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
        """Compare table-level summaries."""
        comparison = {}
        
        for metric in ["row_count", "column_count"]:
            if metric in baseline and metric in current:
                baseline_val = baseline[metric]
                current_val = current[metric]
                
                if baseline_val != 0:
                    change_pct = ((current_val - baseline_val) / baseline_val) * 100
                else:
                    change_pct = 0 if current_val == 0 else float('inf')
                
                comparison[metric] = {
                    "baseline": baseline_val,
                    "current": current_val,
                    "absolute_change": current_val - baseline_val,
                    "percentage_change": change_pct
                }
        
        return comparison
    
    @staticmethod
    def _compare_column_profiles(baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
        """Compare column profiles."""
        comparison = {}
        
        all_columns = set(baseline.keys()) | set(current.keys())
        
        for column in all_columns:
            column_comparison = {
                "status": "unchanged",
                "changes": {}
            }
            
            if column not in baseline:
                column_comparison["status"] = "added"
            elif column not in current:
                column_comparison["status"] = "removed"
            else:
                # Compare metrics for existing columns
                baseline_col = baseline[column]
                current_col = current[column]
                
                metrics_to_compare = ["null_percentage", "distinct_count", "mean", "std_dev"]
                
                for metric in metrics_to_compare:
                    if metric in baseline_col and metric in current_col:
                        baseline_val = baseline_col[metric]
                        current_val = current_col[metric]
                        
                        if baseline_val != current_val:
                            if isinstance(baseline_val, (int, float)) and baseline_val != 0:
                                change_pct = ((current_val - baseline_val) / baseline_val) * 100
                            else:
                                change_pct = 0 if current_val == baseline_val else float('inf')
                            
                            column_comparison["changes"][metric] = {
                                "baseline": baseline_val,
                                "current": current_val,
                                "percentage_change": change_pct
                            }
                
                if column_comparison["changes"]:
                    column_comparison["status"] = "modified"
            
            comparison[column] = column_comparison
        
        return comparison
    
    @staticmethod
    def _identify_significant_changes(column_changes: Dict[str, Any], threshold: float = 10.0) -> List[Dict[str, Any]]:
        """Identify changes that exceed the significance threshold."""
        significant = []
        
        for column, changes in column_changes.items():
            if changes["status"] in ["added", "removed"]:
                significant.append({
                    "column": column,
                    "type": "schema_change",
                    "description": f"Column {column} was {changes['status']}"
                })
            elif changes["status"] == "modified":
                for metric, change_data in changes["changes"].items():
                    if abs(change_data["percentage_change"]) >= threshold:
                        significant.append({
                            "column": column,
                            "metric": metric,
                            "type": "metric_drift",
                            "change_percentage": change_data["percentage_change"],
                            "description": f"{metric} in {column} changed by {change_data['percentage_change']:.1f}%"
                        })
        
        return significant