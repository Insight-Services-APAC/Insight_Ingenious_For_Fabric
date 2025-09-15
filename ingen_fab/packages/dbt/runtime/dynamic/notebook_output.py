"""
Enhanced output formatting for dbt execution in notebooks.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import time
from IPython.display import display, HTML, clear_output
from pyspark.sql import DataFrame


class NotebookOutputFormatter:
    """Provides rich output formatting for dbt execution in notebooks."""

    def __init__(self, verbose: bool = True):
        """
        Initialize the notebook output formatter.

        Args:
            verbose: Whether to show detailed output
        """
        self.verbose = verbose
        self.start_time = None
        self.model_times = {}
        self.model_status = {}
        self.model_errors = {}  # Store error messages for failed models
        self.model_skip_reasons = {}  # Store reasons why models were skipped
        self.current_model = None
        self.execution_stats = {  # Track execution statistics
            'waiting': 0,
            'executing': 0,
            'queued': 0,
            'iteration': 0,
            'max_iterations': 0
        }

    def start_execution(self, total_models: int, execution_order: List[str]):
        """Display execution start information."""
        self.start_time = datetime.now()
        self.total_models = total_models
        self.execution_order = execution_order
        self.completed_models = 0

        html = f"""
        <div style="font-family: monospace; padding: 10px; background: #f8f9fa; border-radius: 5px;">
            <h3 style="color: #007bff;">🚀 Starting DBT Execution</h3>
            <div style="margin: 10px 0;">
                <strong>Total Models:</strong> {total_models}<br>
                <strong>Start Time:</strong> {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}<br>
                <strong>Execution Order:</strong>
                <div style="margin-left: 20px; margin-top: 5px;">
                    {self._format_execution_order(execution_order)}
                </div>
            </div>
        </div>
        """
        display(HTML(html))

    def _format_execution_order(self, order: List[str]) -> str:
        """Format execution order as HTML."""
        items = []
        for i, model in enumerate(order, 1):
            status = self.model_status.get(model, "pending")
            icon = self._get_status_icon(status)
            color = self._get_status_color(status)
            items.append(f'<span style="color: {color};">{i}. {icon} {model}</span>')
        return "<br>".join(items)

    def _get_status_icon(self, status: str) -> str:
        """Get icon for model status."""
        icons = {
            "pending": "⏳",
            "running": "🔄",
            "success": "✅",
            "failed": "❌",
            "skipped": "⏭️"
        }
        return icons.get(status, "❓")

    def _get_status_color(self, status: str) -> str:
        """Get color for model status."""
        colors = {
            "pending": "#6c757d",
            "running": "#007bff",
            "success": "#28a745",
            "failed": "#dc3545",
            "skipped": "#ffc107"
        }
        return colors.get(status, "#000000")

    def start_model(self, model_name: str, model_type: str = "model"):
        """Display model execution start."""
        self.current_model = model_name
        self.model_status[model_name] = "running"
        self.model_times[model_name] = {"start": datetime.now()}

        if self.verbose:
            self._update_progress_display()

    def complete_model(self, model_name: str, success: bool = True,
                      rows_affected: Optional[int] = None, error: Optional[str] = None):
        """Display model execution completion."""
        end_time = datetime.now()
        if model_name in self.model_times:
            self.model_times[model_name]["end"] = end_time
            duration = (end_time - self.model_times[model_name]["start"]).total_seconds()
        else:
            duration = 0

        self.model_status[model_name] = "success" if success else "failed"
        self.completed_models += 1

        # Store error if failed
        if not success and error:
            self.model_errors[model_name] = error

        if self.verbose:
            self._update_progress_display()

        # Show error details if failed
        if not success and error:
            self._display_error(model_name, error)

    def skip_model(self, model_name: str, failed_dependencies: Optional[List[str]] = None):
        """Mark a model as skipped with reason."""
        self.model_status[model_name] = "skipped"
        if failed_dependencies:
            self.model_skip_reasons[model_name] = failed_dependencies

    def update_execution_stats(self, waiting: int, executing: int, queued: int, iteration: int, max_iterations: int):
        """Update execution statistics for display."""
        self.execution_stats['waiting'] = waiting
        self.execution_stats['executing'] = executing
        self.execution_stats['queued'] = queued
        self.execution_stats['iteration'] = iteration
        self.execution_stats['max_iterations'] = max_iterations
        if self.verbose:
            self._update_progress_display()

    def _update_progress_display(self):
        """Update the progress display."""
        clear_output(wait=True)

        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0

        # Count all processed models (completed + failed + skipped)
        processed_models = sum(1 for status in self.model_status.values()
                              if status in ["success", "failed", "skipped"])
        progress_pct = (processed_models / self.total_models * 100) if self.total_models > 0 else 0

        # Create progress bar
        progress_bar = self._create_progress_bar(progress_pct)

        # Get execution statistics
        stats = self.execution_stats
        iteration_pct = (stats['iteration'] / stats['max_iterations'] * 100) if stats['max_iterations'] > 0 else 0

        html = f"""
        <div style="font-family: monospace; padding: 10px; background: #f8f9fa; border-radius: 5px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h3 style="color: #007bff; margin: 0;">🚀 DBT Execution Progress</h3>
                <div>
                    <button onclick="window.stop_dbt_execution = true"
                            style="background: #dc3545; color: white; border: none; padding: 5px 10px;
                                   border-radius: 3px; cursor: pointer; font-size: 12px;">
                        ⏹️ Stop Execution
                    </button>
                </div>
            </div>

            <div style="margin: 10px 0;">
                <strong>Progress:</strong> {processed_models}/{self.total_models} models ({progress_pct:.1f}%)
                <div style="background: #e9ecef; border-radius: 3px; height: 25px; margin: 5px 0;">
                    {progress_bar}
                </div>
            </div>

            <div style="margin: 10px 0; display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div>
                    <strong>Elapsed Time:</strong> {self._format_duration(elapsed)}<br>
                    <strong>Current Model:</strong> {self.current_model or 'None'}<br>
                </div>
                <div>
                    <strong>⏳ Waiting:</strong> <span style="color: #856404;">{stats['waiting']}</span><br>
                    <strong>📋 Queued:</strong> <span style="color: #fd7e14;">{stats.get('queued', 0)}</span><br>
                    <strong>🔄 Executing:</strong> <span style="color: #007bff;">{stats['executing']}</span><br>
                    <strong>🔁 Iteration:</strong> <span style="color: #6c757d;">{stats['iteration']}/{stats['max_iterations']} ({iteration_pct:.0f}%)</span>
                </div>
            </div>

            <details open>
                <summary style="cursor: pointer; font-weight: bold;">Model Status</summary>
                <div style="margin: 10px 0 0 20px;">
                    {self._format_model_status()}
                </div>
            </details>
        </div>
        """
        display(HTML(html))

    def _create_progress_bar(self, percentage: float) -> str:
        """Create an HTML progress bar."""
        # Determine color based on status
        has_failures = any(s == "failed" for s in self.model_status.values())
        has_skipped = any(s == "skipped" for s in self.model_status.values())

        if percentage >= 100:
            if has_failures:
                color = "#dc3545"  # Red for completed with failures
            elif has_skipped:
                color = "#ffc107"  # Yellow for completed with skips
            else:
                color = "#28a745"  # Green for fully successful
        else:
            color = "#007bff"  # Blue for in progress

        return f"""
        <div style="background: {color}; width: {percentage}%; height: 100%;
                    border-radius: 3px; display: flex; align-items: center;
                    justify-content: center; color: white; font-weight: bold;">
            {percentage:.1f}%
        </div>
        """

    def _format_model_status(self) -> str:
        """Format all model statuses."""
        rows = []

        # Create a list of models sorted by execution time (most recent first)
        models_with_time = []
        for model in self.execution_order:
            status = self.model_status.get(model, "pending")
            if status != "pending":  # Only show models that have been processed
                start_time = self.model_times.get(model, {}).get("start", datetime.min)
                models_with_time.append((start_time, model))

        # Sort by start time (newest first) and take the most recent 10
        models_with_time.sort(reverse=True)
        recent_models = [model for _, model in models_with_time[:10]]

        # Also show any currently running models at the top
        running_models = [m for m in self.execution_order
                         if self.model_status.get(m) == "running"]

        # Combine running models (at top) with recent completed models
        display_models = running_models + [m for m in recent_models if m not in running_models]

        for model in display_models[:10]:  # Show max 10 models
            status = self.model_status.get(model, "pending")
            icon = self._get_status_icon(status)
            color = self._get_status_color(status)

            duration = ""
            if model in self.model_times and "end" in self.model_times[model]:
                dur = (self.model_times[model]["end"] - self.model_times[model]["start"]).total_seconds()
                duration = f" ({dur:.1f}s)"
            elif model in self.model_times:
                dur = (datetime.now() - self.model_times[model]["start"]).total_seconds()
                duration = f" ({dur:.1f}s...)"

            rows.append(f'<div style="color: {color};">{icon} {model}{duration}</div>')

        # Show count of remaining models
        total_processed = len([s for s in self.model_status.values()
                              if s in ["success", "failed", "skipped", "running"]])
        remaining = self.total_models - total_processed
        if remaining > 0:
            rows.append(f'<div style="color: #6c757d; margin-top: 5px;">... and {remaining} pending</div>')

        return "".join(rows)

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            return f"{seconds/60:.1f} minutes"
        else:
            return f"{seconds/3600:.1f} hours"

    def _display_error(self, model_name: str, error: str):
        """Display error information."""
        html = f"""
        <div style="font-family: monospace; padding: 10px; background: #f8d7da;
                    border: 1px solid #f5c6cb; border-radius: 5px; margin: 10px 0;">
            <h4 style="color: #721c24;">❌ Error in {model_name}</h4>
            <pre style="white-space: pre-wrap; word-wrap: break-word; color: #721c24;">
{error[:500]}
            </pre>
        </div>
        """
        display(HTML(html))

    def display_results(self, success_count: int, failed_count: int, skipped_count: int):
        """Display final execution results."""
        total_duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0

        # Determine overall status
        if failed_count > 0:
            status_icon = "⚠️"
            status_color = "#dc3545"
            status_text = "Completed with Errors"
        else:
            status_icon = "✅"
            status_color = "#28a745"
            status_text = "Successfully Completed"

        html = f"""
        <div style="font-family: monospace; padding: 15px; background: #f8f9fa;
                    border-radius: 5px; border-left: 5px solid {status_color};">
            <h3 style="color: {status_color};">{status_icon} DBT Execution {status_text}</h3>

            <div style="margin: 15px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 5px;"><strong>Total Duration:</strong></td>
                        <td style="padding: 5px;">{self._format_duration(total_duration)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px;"><strong>Successful Models:</strong></td>
                        <td style="padding: 5px; color: #28a745;">✅ {success_count}</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px;"><strong>Failed Models:</strong></td>
                        <td style="padding: 5px; color: #dc3545;">❌ {failed_count}</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px;"><strong>Skipped Models:</strong></td>
                        <td style="padding: 5px; color: #ffc107;">⏭️ {skipped_count}</td>
                    </tr>
                </table>
            </div>

            {self._format_timing_summary()}
            {self._format_successful_models()}
            {self._format_failed_models()}
            {self._format_skipped_models()}
        </div>
        """
        display(HTML(html))

    def _format_timing_summary(self) -> str:
        """Format timing summary for completed models."""
        completed_models = [(m, t) for m, t in self.model_times.items() if "end" in t]
        if not completed_models:
            return ""

        # Sort by duration
        sorted_models = sorted(
            completed_models,
            key=lambda x: (x[1]["end"] - x[1]["start"]).total_seconds(),
            reverse=True
        )[:5]  # Show top 5 slowest

        rows = []
        for model, times in sorted_models:
            duration = (times["end"] - times["start"]).total_seconds()
            status = self.model_status.get(model, "unknown")
            icon = self._get_status_icon(status)
            rows.append(f"<tr><td>{icon} {model}</td><td>{duration:.2f}s</td></tr>")

        return f"""
        <details>
            <summary style="cursor: pointer; font-weight: bold; margin-top: 10px;">
                ⏱️ Top 5 Slowest Models
            </summary>
            <table style="margin: 10px 0 0 20px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <th style="text-align: left; padding: 5px;">Model</th>
                    <th style="text-align: left; padding: 5px;">Duration</th>
                </tr>
                {''.join(rows)}
            </table>
        </details>
        """

    def _format_successful_models(self) -> str:
        """Format list of successful resources grouped by type."""
        successful = [m for m, s in self.model_status.items() if s == "success"]
        if not successful:
            return ""

        # Group by resource type
        grouped = {
            'models': [],
            'tests': [],
            'seeds': [],
            'snapshots': [],
            'other': []
        }

        for resource in successful:
            if resource.startswith('model.'):
                grouped['models'].append(resource)
            elif resource.startswith('test.'):
                grouped['tests'].append(resource)
            elif resource.startswith('seed.'):
                grouped['seeds'].append(resource)
            elif resource.startswith('snapshot.'):
                grouped['snapshots'].append(resource)
            else:
                grouped['other'].append(resource)

        # Build HTML for each group
        sections_html = []
        for group_name, resources in grouped.items():
            if not resources:
                continue

            # Sort resources
            resources_sorted = sorted(resources)

            # Create rows for this group
            rows = []
            for resource in resources_sorted[:15]:  # Show first 15 per group
                # Get icon based on type
                icon = {
                    'models': '📊',
                    'tests': '🧪',
                    'seeds': '🌱',
                    'snapshots': '📸',
                    'other': '📦'
                }.get(group_name, '✅')

                # Get duration if available
                if resource in self.model_times and "end" in self.model_times[resource]:
                    duration = (self.model_times[resource]["end"] - self.model_times[resource]["start"]).total_seconds()
                    duration_str = f"{duration:.2f}s"
                else:
                    duration_str = "-"

                # Clean up the resource name for display
                display_name = resource.split('.', 1)[1] if '.' in resource else resource
                rows.append(f"<tr><td>{icon} {display_name}</td><td>{duration_str}</td></tr>")

            if len(resources) > 15:
                rows.append(f"<tr><td colspan='2' style='color: #6c757d;'>... and {len(resources) - 15} more {group_name}</td></tr>")

            if rows:
                sections_html.append(f"""
                    <tr><td colspan="2" style="padding: 10px 5px 5px 5px; font-weight: bold; color: #28a745;">
                        {group_name.capitalize()} ({len(resources)})
                    </td></tr>
                    {''.join(rows)}
                """)

        return f"""
        <details>
            <summary style="cursor: pointer; font-weight: bold; margin-top: 10px; color: #28a745;">
                ✅ Successful Execution ({len(successful)} total)
            </summary>
            <table style="margin: 10px 0 0 20px; border-collapse: collapse; width: 90%;">
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <th style="text-align: left; padding: 5px;">Resource</th>
                    <th style="text-align: left; padding: 5px; width: 100px;">Duration</th>
                </tr>
                {''.join(sections_html)}
            </table>
        </details>
        """

    def _format_failed_models(self) -> str:
        """Format list of failed resources with error details, grouped by type."""
        failed = [m for m, s in self.model_status.items() if s == "failed"]
        if not failed:
            return ""

        # Group by resource type
        grouped = {
            'models': [],
            'tests': [],
            'seeds': [],
            'snapshots': [],
            'other': []
        }

        for resource in failed:
            if resource.startswith('model.'):
                grouped['models'].append(resource)
            elif resource.startswith('test.'):
                grouped['tests'].append(resource)
            elif resource.startswith('seed.'):
                grouped['seeds'].append(resource)
            elif resource.startswith('snapshot.'):
                grouped['snapshots'].append(resource)
            else:
                grouped['other'].append(resource)

        # Build HTML for each group
        sections_html = []
        for group_name, resources in grouped.items():
            if not resources:
                continue

            # Sort resources
            resources_sorted = sorted(resources)

            # Create section header
            icon = {
                'models': '📊',
                'tests': '🧪',
                'seeds': '🌱',
                'snapshots': '📸',
                'other': '📦'
            }.get(group_name, '❌')

            sections_html.append(f"""
                <tr><td colspan="2" style="padding: 10px 5px 5px 5px; font-weight: bold; color: #dc3545;">
                    {icon} {group_name.capitalize()} ({len(resources)})
                </td></tr>
            """)

            # Add resources with errors
            for resource in resources_sorted[:10]:  # Show first 10 per group
                display_name = resource.split('.', 1)[1] if '.' in resource else resource
                error_msg = self.model_errors.get(resource, "Unknown error")
                # Truncate long error messages
                if len(error_msg) > 500:
                    error_msg = error_msg[:500] + "..."

                sections_html.append(f"""
                    <tr>
                        <td style="padding: 5px;">❌ {display_name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px 5px 10px 20px; color: #721c24; font-size: 0.9em;">
                            <pre style="white-space: pre-wrap; word-wrap: break-word; margin: 0;">{error_msg}</pre>
                        </td>
                    </tr>
                """)

            if len(resources) > 10:
                sections_html.append(f"<tr><td colspan='2' style='color: #6c757d;'>... and {len(resources) - 10} more {group_name}</td></tr>")

        return f"""
        <details>
            <summary style="cursor: pointer; font-weight: bold; margin-top: 10px; color: #dc3545;">
                ❌ Failed Execution ({len(failed)} total)
            </summary>
            <table style="margin: 10px 0 0 20px; border-collapse: collapse; width: 90%;">
                {''.join(sections_html)}
            </table>
        </details>
        """

    def _format_skipped_models(self) -> str:
        """Format list of skipped resources with detailed dependency information, grouped by type."""
        skipped = [m for m, s in self.model_status.items() if s == "skipped"]
        if not skipped:
            return ""

        # Group by resource type
        grouped = {
            'models': [],
            'tests': [],
            'seeds': [],
            'snapshots': [],
            'other': []
        }

        for resource in skipped:
            if resource.startswith('model.'):
                grouped['models'].append(resource)
            elif resource.startswith('test.'):
                grouped['tests'].append(resource)
            elif resource.startswith('seed.'):
                grouped['seeds'].append(resource)
            elif resource.startswith('snapshot.'):
                grouped['snapshots'].append(resource)
            else:
                grouped['other'].append(resource)

        # Build HTML for each group
        sections_html = []
        for group_name, resources in grouped.items():
            if not resources:
                continue

            # Sort resources
            resources_sorted = sorted(resources)

            # Create section header
            icon = {
                'models': '📊',
                'tests': '🧪',
                'seeds': '🌱',
                'snapshots': '📸',
                'other': '📦'
            }.get(group_name, '⏭️')

            sections_html.append(f"""
                <tr><td colspan="2" style="padding: 10px 5px 5px 5px; font-weight: bold; color: #ffc107;">
                    {icon} {group_name.capitalize()} ({len(resources)})
                </td></tr>
            """)

            # Add resources with skip reasons
            for resource in resources_sorted[:15]:  # Show first 15 per group
                display_name = resource.split('.', 1)[1] if '.' in resource else resource
                failed_deps = self.model_skip_reasons.get(resource, [])

                if failed_deps:
                    # Clean up dependency names for display
                    deps_display = []
                    for dep in failed_deps[:5]:
                        dep_display = dep.split('.', 1)[1] if '.' in dep else dep
                        # Add resource type icon
                        if dep.startswith('model.'):
                            dep_display = f"📊 {dep_display}"
                        elif dep.startswith('test.'):
                            dep_display = f"🧪 {dep_display}"
                        elif dep.startswith('seed.'):
                            dep_display = f"🌱 {dep_display}"
                        elif dep.startswith('snapshot.'):
                            dep_display = f"📸 {dep_display}"
                        elif dep.startswith('source.'):
                            dep_display = f"📁 {dep_display}"

                        deps_display.append(f"&nbsp;&nbsp;• {dep_display}")

                    deps_html = "<br>".join(deps_display)
                    if len(failed_deps) > 5:
                        deps_html += f"<br>&nbsp;&nbsp;• ... and {len(failed_deps) - 5} more"

                    # Check if any are sources (which means not in execution set)
                    has_sources = any(dep.startswith('source.') for dep in failed_deps)
                    if has_sources:
                        reason_text = "Depends on:"
                    else:
                        reason_text = "Dependencies not executed:"

                    reason_html = f"""
                        <div>{reason_text}</div>
                        <div style="font-size: 0.9em; color: #856404;">
                            {deps_html}
                        </div>
                    """
                else:
                    reason_html = "Not included in execution scope"

                sections_html.append(f"""
                    <tr>
                        <td style="padding: 5px; vertical-align: top;">⏭️ {display_name}</td>
                        <td style="padding: 5px;">{reason_html}</td>
                    </tr>
                """)

            if len(resources) > 15:
                sections_html.append(f"<tr><td colspan='2' style='color: #6c757d;'>... and {len(resources) - 15} more {group_name}</td></tr>")

        return f"""
        <details>
            <summary style="cursor: pointer; font-weight: bold; margin-top: 10px; color: #ffc107;">
                ⏭️ Skipped Execution ({len(skipped)} total)
            </summary>
            <table style="margin: 10px 0 0 20px; border-collapse: collapse; width: 90%;">
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <th style="text-align: left; padding: 5px; width: 40%;">Resource</th>
                    <th style="text-align: left; padding: 5px;">Reason / Failed Dependencies</th>
                </tr>
                {''.join(sections_html)}
            </table>
        </details>
        """

    def display_dataframe_preview(self, df: DataFrame, model_name: str, limit: int = 5):
        """Display a preview of a DataFrame result."""
        if not self.verbose:
            return

        try:
            count = df.count()
            preview_df = df.limit(limit).toPandas()

            html = f"""
            <div style="font-family: monospace; padding: 10px; background: #e7f3ff;
                        border-radius: 5px; margin: 10px 0;">
                <h4 style="color: #004085;">📊 Preview: {model_name} ({count:,} rows)</h4>
                {preview_df.to_html(index=False, max_rows=limit)}
            </div>
            """
            display(HTML(html))
        except Exception as e:
            # Silently skip preview on error
            pass


class SimpleOutputFormatter:
    """Simple text-based output formatter for non-notebook environments."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.start_time = None
        self.model_status = {}  # Track model status for compatibility
        self.model_skip_reasons = {}  # Track skip reasons for compatibility
        self.execution_stats = {  # Track execution statistics
            'waiting': 0,
            'executing': 0,
            'queued': 0,
            'iteration': 0,
            'max_iterations': 0
        }

    def start_execution(self, total_models: int, execution_order: List[str]):
        """Display execution start information."""
        self.start_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"Starting DBT Execution")
        print(f"Total Models: {total_models}")
        print(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

    def start_model(self, model_name: str, model_type: str = "model"):
        """Display model execution start."""
        self.model_status[model_name] = "running"
        if self.verbose:
            print(f"🔄 Executing {model_type}: {model_name}")

    def complete_model(self, model_name: str, success: bool = True,
                      rows_affected: Optional[int] = None, error: Optional[str] = None):
        """Display model execution completion."""
        self.model_status[model_name] = "success" if success else "failed"
        status = "✅" if success else "❌"
        msg = f"{status} {model_name}"
        if rows_affected is not None:
            msg += f" ({rows_affected:,} rows)"
        if error and not success:
            msg += f"\n   Error: {error[:200]}"
        print(msg)

    def skip_model(self, model_name: str, failed_dependencies: Optional[List[str]] = None):
        """Mark a model as skipped with reason."""
        self.model_status[model_name] = "skipped"
        if failed_dependencies:
            self.model_skip_reasons[model_name] = failed_dependencies
            deps_str = ", ".join(failed_dependencies[:3])
            if len(failed_dependencies) > 3:
                deps_str += f" and {len(failed_dependencies) - 3} more"
            print(f"⏭️ {model_name} (skipped due to: {deps_str})")
        else:
            print(f"⏭️ {model_name} (skipped)")

    def update_execution_stats(self, waiting: int, executing: int, queued: int, iteration: int, max_iterations: int):
        """Update execution statistics for display."""
        self.execution_stats['waiting'] = waiting
        self.execution_stats['executing'] = executing
        self.execution_stats['queued'] = queued
        self.execution_stats['iteration'] = iteration
        self.execution_stats['max_iterations'] = max_iterations
        # For simple formatter, occasionally show progress
        if iteration % 10 == 0 and self.verbose:
            print(f"🔁 Iteration {iteration}/{max_iterations} - ⏳ Waiting: {waiting}, 📋 Queued: {queued}, 🔄 Executing: {executing}")

    def display_results(self, success_count: int, failed_count: int, skipped_count: int):
        """Display final execution results."""
        duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        print(f"\n{'='*60}")
        print(f"DBT Execution Complete")
        print(f"Duration: {duration:.1f} seconds")
        print(f"✅ Success: {success_count}")
        print(f"❌ Failed: {failed_count}")
        print(f"⏭️ Skipped: {skipped_count}")
        print(f"{'='*60}\n")

    def display_dataframe_preview(self, df: DataFrame, model_name: str, limit: int = 5):
        """Display a preview of a DataFrame result."""
        if self.verbose:
            try:
                print(f"\nPreview of {model_name}:")
                df.show(limit, truncate=True)
            except:
                pass