from __future__ import annotations

from rich.console import Console
from rich.style import Style
from rich.text import Text


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
