from __future__ import annotations

from typing import Optional

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.style import Style
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ConsoleStyles:
    """
    Centralized styles and helper methods for rich console output.
    """

    SUCCESS = Style(color="green", bold=True)
    WARNING = Style(color="yellow", bold=True)
    ERROR = Style(color="red", bold=True)
    INFO = Style(color="cyan", italic=True)
    DIM = Style(dim=True)
    TITLE = Style(color="magenta", bold=True)

    @staticmethod
    def print_success(console: Console, message: str) -> None:
        console.print(Text(message, style=ConsoleStyles.SUCCESS))

    @staticmethod
    def print_warning(console: Console, message: str) -> None:
        console.print(Text(message, style=ConsoleStyles.WARNING))

    @staticmethod
    def print_error(console: Console, message: str) -> None:
        console.print(Text(message, style=ConsoleStyles.ERROR))

    @staticmethod
    def print_info(console: Console, message: str) -> None:
        console.print(Text(message, style=ConsoleStyles.INFO))

    @staticmethod
    def print_dim(console: Console, message: str) -> None:
        console.print(Text(message, style=ConsoleStyles.DIM))

    @staticmethod
    def print_title(console: Console, message: str) -> None:
        console.print(Text(message, style=ConsoleStyles.TITLE))


class MessageHelpers:
    """
    Enhanced console utilities with fallback for when rich is not available.
    """

    def __init__(self, console: Optional[Console] = None):
        self.console = console if console and RICH_AVAILABLE else None

    def print_info(self, message: str) -> None:
        """Print info message with rich formatting if available."""
        if self.console:
            self.console.print(f"[blue]ℹ[/blue] {message}")
        else:
            print(f"ℹ {message}")

    def print_success(self, message: str) -> None:
        """Print success message with rich formatting if available."""
        if self.console:
            self.console.print(f"[green]✓[/green] {message}")
        else:
            print(f"✓ {message}")

    def print_warning(self, message: str) -> None:
        """Print warning message with rich formatting if available."""
        if self.console:
            self.console.print(f"[yellow]⚠[/yellow] {message}")
        else:
            print(f"⚠ {message}")

    def print_error(self, message: str) -> None:
        """Print error message with rich formatting if available."""
        if self.console:
            self.console.print(f"[red]✗[/red] {message}")
        else:
            print(f"✗ {message}")

    def print_rule(self, title: str, style: str = "bold blue") -> None:
        """Print a rule separator with title."""
        if self.console:
            self.console.print(Rule(f"[{style}]{title}[/{style}]"))
        else:
            print(f"=== {title} ===")

    def print_panel(
        self,
        content: str,
        title: str = None,
        border_style: str = "blue",
        expand: bool = False,
    ) -> None:
        """Print content in a panel."""
        if self.console:
            panel = Panel(
                content,
                title=f"[bold]{title}[/bold]" if title else None,
                border_style=border_style,
                expand=expand,
            )
            self.console.print(panel)
        else:
            if title:
                print(f"--- {title} ---")
            print(content)
            if title:
                print("---" + "-" * len(title) + "---")

    # Formatting helpers for common patterns
    @staticmethod
    def format_label_value(label: str, value: str, label_color: str = "cyan") -> str:
        """Format a label-value pair with consistent styling."""
        return f"[{label_color}]{label}:[/{label_color}] {value}"

    @staticmethod
    def format_check_item(text: str, checked: bool = True) -> str:
        """Format a checkmark item."""
        check = "✓" if checked else "✗"
        color = "green" if checked else "red"
        return f"[{color}]{check}[/{color}] {text}"

    @staticmethod
    def format_count(label: str, count: int, label_color: str = "cyan") -> str:
        """Format a count with label."""
        return MessageHelpers.format_label_value(label, str(count), label_color)

    @staticmethod
    def format_path(label: str, path: str, label_color: str = "cyan") -> str:
        """Format a path with label."""
        return MessageHelpers.format_label_value(label, str(path), label_color)

    @staticmethod
    def format_status(label: str, status: str, label_color: str = "cyan") -> str:
        """Format a status with label."""
        return MessageHelpers.format_label_value(label, status, label_color)

    @staticmethod
    def format_operations_list(operations: list[str]) -> str:
        """Format a list of operations."""
        if not operations:
            return ""
        return MessageHelpers.format_label_value(
            "Operations", "\n" + "\n".join(f"  {op}" for op in operations)
        )

    def create_operation_details(
        self,
        environment: str = None,
        total_files: int = None,
        operations: list[str] = None,
        output_dir: str = None,
        target_directory: str = None,
        mode: str = None,
        files_found_label: str = "Total files",
    ) -> list[str]:
        """Create standardized operation details list."""
        details = []

        if environment:
            details.append(self.format_status("Environment", environment))

        if target_directory:
            details.append(self.format_path("Target directory", target_directory))

        if total_files is not None:
            details.append(self.format_count(files_found_label, total_files))

        if operations:
            details.append(self.format_operations_list(operations))

        if output_dir:
            details.append(self.format_path("Output directory", output_dir))
        elif mode:
            details.append(self.format_status("Mode", mode))

        return details
