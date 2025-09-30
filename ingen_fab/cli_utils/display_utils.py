from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class TableBuilder:
    """
    Utility for building and displaying tables with fallback for when rich is not available.
    """

    def __init__(self, console: Optional[Console] = None):
        self.console = console if console and RICH_AVAILABLE else None

    def create_summary_table(
        self,
        data: Dict[str, Any],
        title: str = "Summary",
        title_style: str = "bold blue",
        border_style: str = "blue",
    ) -> None:
        """Create and display a summary table."""
        if self.console:
            table = Table(
                title=f"[{title_style}]{title}[/{title_style}]",
                border_style=border_style,
            )
            table.add_column("Variable Name", style="cyan", no_wrap=True)
            table.add_column("Value", style="green")
            table.add_column("Type", style="yellow")

            for name, value in sorted(data.items()):
                value_str = str(value)
                if len(value_str) > 50:
                    value_str = value_str[:47] + "..."
                table.add_row(name, value_str, type(value).__name__)

            self.console.print(table)
            self.console.print(f"[dim]Total items: {len(data)}[/dim]\n")
        else:
            print(f"=== {title} ===")
            for name, value in sorted(data.items()):
                value_str = str(value)
                if len(value_str) > 50:
                    value_str = value_str[:47] + "..."
                print(f"{name}: {value_str} ({type(value).__name__})")
            print(f"Total items: {len(data)}\n")

    def create_statistics_table(
        self,
        stats: Dict[str, Any],
        total_files: int,
        updated_files_count: int,
        title: str = "Processing Statistics",
        border_style: str = "green",
    ) -> None:
        """Create and display a statistics table."""
        if self.console:
            stats_table = Table(title=f"[bold]{title}[/bold]", border_style=border_style)
            stats_table.add_column("Metric", style="cyan", no_wrap=True)
            stats_table.add_column("Count", style="green", justify="right")
            stats_table.add_column("Percentage", style="yellow", justify="right")

            stats_table.add_row("Total files processed", str(total_files), "100.0%")
            stats_table.add_row(
                "Files updated",
                str(updated_files_count),
                f"{(updated_files_count / total_files * 100):.1f}%" if total_files > 0 else "0.0%",
            )
            stats_table.add_row(
                "Files skipped (no changes)",
                str(stats["files_skipped"]),
                f"{(stats['files_skipped'] / total_files * 100):.1f}%" if total_files > 0 else "0.0%",
            )
            stats_table.add_row("Placeholder replacements", str(stats["placeholders_replaced"]), "-")
            stats_table.add_row("Code injections", str(stats["code_injected"]), "-")

            if stats["errors"] > 0:
                stats_table.add_row(
                    "Errors",
                    str(stats["errors"]),
                    f"{(stats['errors'] / total_files * 100):.1f}%" if total_files > 0 else "0.0%",
                )

            self.console.print(stats_table)
            self.console.print()
        else:
            print(f"=== {title} ===")
            print(f"Total files processed: {total_files} (100.0%)")
            print(
                f"Files updated: {updated_files_count} ({(updated_files_count / total_files * 100):.1f}%)"
                if total_files > 0
                else "Files updated: 0 (0.0%)"
            )
            print(
                f"Files skipped: {stats['files_skipped']} ({(stats['files_skipped'] / total_files * 100):.1f}%)"
                if total_files > 0
                else "Files skipped: 0 (0.0%)"
            )
            print(f"Placeholder replacements: {stats['placeholders_replaced']}")
            print(f"Code injections: {stats['code_injected']}")
            if stats["errors"] > 0:
                print(
                    f"Errors: {stats['errors']} ({(stats['errors'] / total_files * 100):.1f}%)"
                    if total_files > 0
                    else "Errors: 0 (0.0%)"
                )
            print()


class PanelBuilder:
    """
    Utility for building and displaying panels with fallback for when rich is not available.
    """

    def __init__(self, console: Optional[Console] = None):
        self.console = console if console and RICH_AVAILABLE else None

    def create_operation_panel(
        self,
        operation_details: List[str],
        title: str = "Operation Configuration",
        border_style: str = "blue",
        expand: bool = False,
    ) -> None:
        """Create and display an operation configuration panel."""
        content = "\n".join(operation_details)

        if self.console:
            panel = Panel(
                content,
                title=f"[bold]{title}[/bold]",
                border_style=border_style,
                expand=expand,
            )
            self.console.print(panel)
            self.console.print()
        else:
            print(f"--- {title} ---")
            print(content)
            print("---" + "-" * len(title) + "---")
            print()

    def create_results_panel(
        self,
        file_results: List[Tuple[Any, Any]],
        environment: str,
        title_prefix: str = "Results",
        border_style: str = "green",
        max_files_shown: int = 10,
    ) -> None:
        """Create and display a results panel showing processed files."""
        if not file_results:
            return

        if self.console:
            file_list = []
            for orig_file, output_file in file_results[:max_files_shown]:
                file_list.append(f"[cyan]{orig_file}[/cyan] → [green]{output_file}[/green]")

            if len(file_results) > max_files_shown:
                file_list.append(f"[dim]... and {len(file_results) - max_files_shown} more files[/dim]")

            panel_content = "\n".join(file_list)
            panel = Panel(
                panel_content,
                title=f"[bold]{title_prefix} ({environment})[/bold]",
                title_align="left",
                border_style=border_style,
            )
            self.console.print(panel)
        else:
            print(f"--- {title_prefix} ({environment}) ---")
            for orig_file, output_file in file_results[:max_files_shown]:
                print(f"  - {orig_file} → {output_file}")
            if len(file_results) > max_files_shown:
                print(f"  ... and {len(file_results) - max_files_shown} more files")
            print("---" + "-" * (len(title_prefix) + len(environment) + 3) + "---")

    def create_file_list_panel(
        self,
        files: List[Any],
        environment: str,
        title_prefix: str = "Updated Files",
        border_style: str = "green",
        max_files_shown: int = 10,
    ) -> None:
        """Create and display a panel showing a list of files."""
        if not files:
            return

        if self.console:
            file_list = []
            for file_path in files[:max_files_shown]:
                if hasattr(file_path, "name"):
                    file_name = file_path.name
                else:
                    file_name = str(file_path)
                file_list.append(f"[green]✓[/green] [cyan]{file_name}[/cyan]")

            if len(files) > max_files_shown:
                file_list.append(f"[dim]... and {len(files) - max_files_shown} more files[/dim]")

            panel_content = "\n".join(file_list)
            panel = Panel(
                panel_content,
                title=f"[bold]{title_prefix} ({environment})[/bold]",
                title_align="left",
                border_style=border_style,
            )
            self.console.print(panel)
        else:
            print(f"--- {title_prefix} ({environment}) ---")
            for file_path in files[:max_files_shown]:
                if hasattr(file_path, "name"):
                    file_name = file_path.name
                else:
                    file_name = str(file_path)
                print(f"  - {file_name}")
            if len(files) > max_files_shown:
                print(f"  ... and {len(files) - max_files_shown} more files")
            print("---" + "-" * (len(title_prefix) + len(environment) + 3) + "---")
