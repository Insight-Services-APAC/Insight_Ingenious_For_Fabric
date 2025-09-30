from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TextColumn,
        TimeElapsedColumn,
    )

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ProgressTracker:
    """
    Reusable progress tracking utilities with fallback for when rich is not available.
    """

    def __init__(self, console: Optional[Console] = None):
        self.console = console if console and RICH_AVAILABLE else None
        self._progress = None
        self._task_id = None

    def create_progress_bar(
        self, total: int, description: str = "Processing...", spinner_name: str = "dots"
    ) -> Optional[Progress]:
        """Create a rich progress bar if available."""
        if not self.console:
            return None

        self._progress = Progress(
            SpinnerColumn(spinner_name=spinner_name),
            MofNCompleteColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
            transient=False,
        )
        return self._progress

    def add_task(self, description: str, total: int) -> Optional[TaskID]:
        """Add a task to the progress bar."""
        if self._progress:
            self._task_id = self._progress.add_task(description, total=total)
            return self._task_id
        return None

    def update_task(self, task_id: Optional[TaskID], **kwargs) -> None:
        """Update a task in the progress bar."""
        if self._progress and task_id is not None:
            self._progress.update(task_id, **kwargs)

    def advance_task(self, task_id: Optional[TaskID], advance: int = 1) -> None:
        """Advance a task in the progress bar."""
        if self._progress and task_id is not None:
            self._progress.advance(task_id, advance)

    def __enter__(self):
        """Context manager entry."""
        if self._progress:
            return self._progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._progress:
            return self._progress.__exit__(exc_type, exc_val, exc_tb)
        return False

    def simple_progress_tracker(
        self,
        items: list[Any],
        description: str = "Processing items...",
        project_path: Optional[Path] = None,
    ):
        """
        Simple progress tracker that yields items with progress updates.
        Falls back to simple enumeration when rich is not available.
        """
        total = len(items)

        if self.console and total > 1:
            with self.create_progress_bar(total, description) as progress:
                if progress:
                    task_id = self.add_task(description, total)

                    for idx, item in enumerate(items):
                        # Update description with current item info
                        if hasattr(item, "name"):
                            item_name = item.name
                        elif isinstance(item, Path):
                            if project_path:
                                try:
                                    item_name = str(item.relative_to(project_path))
                                except ValueError:
                                    item_name = item.name
                            else:
                                item_name = item.name
                        else:
                            item_name = str(item)

                        self.update_task(task_id, description=f"[cyan]Processing:[/cyan] {item_name}")

                        yield item

                        self.advance_task(task_id)
                else:
                    # Fallback when progress creation fails
                    for item in items:
                        yield item
        else:
            # Fallback for single item or when rich not available
            for item in items:
                yield item
