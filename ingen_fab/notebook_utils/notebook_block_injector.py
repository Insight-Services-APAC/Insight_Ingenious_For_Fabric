from __future__ import annotations
import os
import re
from pathlib import Path
from typing import List, Dict, Any
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax


class NotebookContentFinder:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.console = Console()
        self.base_dir = base_dir or (Path.cwd() / "fabric_workspace_items")

    def find_content_blocks(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Find all content blocks in a file marked with # _blockstart: and # _blockend:

        Args:
            file_path: Path to the notebook-content.py file

        Returns:
            List of dictionaries containing block information
        """
        blocks = []

        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()
            lines = content.splitlines()

        i = 0
        while i < len(lines):
            line = lines[i]

            # Check for block start
            start_match = re.match(r"#\s*_blockstart:\s*(\w+)", line)
            if start_match:
                block_name = start_match.group(1)
                start_line = i

                # Find the corresponding block end
                j = i + 1
                block_content_lines = []
                end_line = None

                while j < len(lines):
                    # Check for block end with matching name
                    end_match = re.match(rf"#\s*_?blockend:\s*{block_name}", lines[j])
                    if end_match:
                        end_line = j
                        break
                    block_content_lines.append(
                        (j, lines[j])
                    )  # Store line number with content
                    j += 1

                if end_line:
                    # Find the actual content to be replaced
                    replacement_start_idx = None
                    replacement_end_idx = len(block_content_lines)

                    # Prefer to start after the first CELL marker
                    for idx, (line_num, content_line) in enumerate(block_content_lines):
                        if "# CELL ********************" in content_line:
                            replacement_start_idx = idx + 1
                            break

                    # If no CELL marker, fall back to the first MARKDOWN section
                    if replacement_start_idx is None:
                        for idx, (line_num, content_line) in enumerate(
                            block_content_lines
                        ):
                            if "# MARKDOWN ********************" in content_line:
                                replacement_start_idx = idx + 1
                                break

                    # Find where the content ends (before the last METADATA section)
                    # Work backwards to find the last METADATA section
                    for idx in range(len(block_content_lines) - 1, -1, -1):
                        if (
                            "# METADATA ********************"
                            in block_content_lines[idx][1]
                        ):
                            replacement_end_idx = idx
                            break

                    # Extract replacement content and line numbers
                    if replacement_start_idx is None:
                        replacement_start_idx = 0

                    if replacement_start_idx >= replacement_end_idx:
                        replacement_end_idx = len(block_content_lines)

                    replacement_lines = block_content_lines[
                        replacement_start_idx:replacement_end_idx
                    ]

                    if replacement_lines:
                        actual_start_line = (
                            replacement_lines[0][0] + 1
                        )  # Convert to 1-based
                        actual_end_line = (
                            replacement_lines[-1][0] + 1
                        )  # Convert to 1-based
                        replacement_content = "\n".join(
                            [line[1] for line in replacement_lines]
                        ).strip()
                    else:
                        actual_start_line = start_line + 2
                        actual_end_line = start_line + 2
                        replacement_content = ""

                    blocks.append(
                        {
                            "block_name": block_name,
                            "start_line": start_line + 1,  # 1-based line numbers
                            "end_line": end_line + 1,
                            "total_lines": end_line - start_line + 1,
                            "replacement_content": replacement_content,
                            "replacement_start_line": actual_start_line,
                            "replacement_end_line": actual_end_line,
                            "file_path": str(file_path),
                        }
                    )

                    i = end_line + 1
                else:
                    self.console.print(
                        f"[yellow]Warning: No matching blockend found for block '"
                        f"{block_name}' at line {start_line + 1}[/yellow]"
                    )
                    i += 1
            else:
                i += 1

        return blocks

    def find_notebook_content_files(
        self, base_dir: Path | None = None
    ) -> List[Dict[str, Any]]:
        """
        Scan directory recursively for all notebook-content.py files.

        Args:
            base_dir: The base directory to start scanning from

        Returns:
            List of dictionaries containing file information
        """
        base_dir = base_dir or self.base_dir
        notebook_files = []

        # Walk through all directories
        for root, dirs, files in os.walk(base_dir):
            for file in files:
                if file == "notebook-content.py":
                    file_path = Path(root) / file

                    # Get parent directory name (should be *.Notebook)
                    notebook_dir = file_path.parent
                    notebook_name = notebook_dir.name

                    # Get relative path from base directory
                    relative_path = file_path.relative_to(base_dir)

                    # Try to extract lakehouse name from path
                    lakehouse = None
                    parts = relative_path.parts
                    if len(parts) > 2:
                        lakehouse = parts[0]

                    notebook_info = {
                        "absolute_path": file_path,
                        "relative_path": str(relative_path),
                        "notebook_name": notebook_name,
                        "lakehouse": lakehouse,
                    }

                    notebook_files.append(notebook_info)

        return notebook_files

    def display_blocks_summary(self, all_blocks: Dict[str, List[Dict]]) -> None:
        """Display a summary of all found blocks across all files."""

        # Count total blocks
        total_blocks = sum(len(blocks) for blocks in all_blocks.values())

        self.console.print(
            Panel.fit(
                f"[bold cyan]Found {total_blocks} content blocks across {len(all_blocks)} files[/bold cyan]",
                border_style="cyan",
            )
        )
        self.console.print()

        if total_blocks == 0:
            self.console.print("[yellow]No content blocks found.[/yellow]")
            return

        # Create summary table
        table = Table(title="Content Blocks Summary", show_lines=True)
        table.add_column("Notebook", style="cyan", no_wrap=True)
        table.add_column("Block Name", style="green")
        table.add_column("Block Lines", justify="right", style="yellow")
        table.add_column("Replace Lines", justify="right", style="magenta")
        table.add_column("Content Preview", style="dim")

        for file_path, blocks in all_blocks.items():
            notebook_name = Path(file_path).parent.name

            for block in blocks:
                # Create content preview (first 50 chars)
                content_preview = block["replacement_content"].replace("\n", " ")
                if len(content_preview) > 50:
                    content_preview = content_preview[:47] + "..."

                table.add_row(
                    notebook_name,
                    block["block_name"],
                    f"{block['start_line']}-{block['end_line']}",
                    f"{block['replacement_start_line']}-{block['replacement_end_line']}",
                    content_preview,
                )

        self.console.print(table)

        # Show detailed content for each block
        self.console.print("\n[bold]Detailed Block Contents:[/bold]\n")

        for file_path, blocks in all_blocks.items():
            if blocks:
                notebook_name = Path(file_path).parent.name
                self.console.print(f"[bold cyan]📓 {notebook_name}[/bold cyan]")
                self.console.print(f"[dim]{file_path}[/dim]\n")

                for block in blocks:
                    self.console.print(
                        f"[bold green]Block: {block['block_name']}[/bold green]"
                    )
                    self.console.print(
                        f"Full block: lines {block['start_line']}-{block['end_line']}"
                    )
                    self.console.print(
                        f"Content to replace: lines {block['replacement_start_line']}-{block['replacement_end_line']}"
                    )
                    self.console.print("[bold]Content to be replaced:[/bold]")

                    # Display the replacement content with syntax highlighting
                    if block["replacement_content"]:
                        syntax = Syntax(
                            block["replacement_content"],
                            "python",
                            theme="monokai",
                            line_numbers=True,
                            start_line=block["replacement_start_line"],
                        )
                        self.console.print(syntax)
                    else:
                        self.console.print("[dim italic]<empty content>[/dim italic]")

                    self.console.print("-" * 60 + "\n")

    def scan_and_display_blocks(self) -> dict[str, list[dict[str, Any]]]:
        fabric_workspace_dir = self.base_dir
        self.console.print(
            f"[bold blue]Scanning directory:[/bold blue] {fabric_workspace_dir}"
        )
        if not fabric_workspace_dir.exists():
            self.console.print(
                f"[red]Error: Directory does not exist: {fabric_workspace_dir}[/red]"
            )
            return {}
        try:
            notebook_files = self.find_notebook_content_files(fabric_workspace_dir)
            if not notebook_files:
                self.console.print(
                    "[yellow]No notebook-content.py files found.[/yellow]"
                )
                return {}
            all_blocks = {}
            with self.console.status(
                "[bold green]Scanning for content blocks..."
            ) as status:
                for notebook_info in notebook_files:
                    file_path = notebook_info["absolute_path"]
                    status.update(f"Scanning {notebook_info['notebook_name']}...")
                    try:
                        blocks = self.find_content_blocks(file_path)
                        if blocks:
                            all_blocks[str(file_path)] = blocks
                    except Exception as e:
                        self.console.print(
                            f"[red]Error processing {file_path}: {str(e)}[/red]"
                        )
            self.display_blocks_summary(all_blocks)
            return all_blocks
        except Exception as e:
            self.console.print(f"[red]Error scanning directory: {str(e)}[/red]")
            import traceback

            traceback.print_exc()
            return {}


def main() -> dict[str, list[dict[str, Any]]]:
    finder = NotebookContentFinder()
    return finder.scan_and_display_blocks()


if __name__ == "__main__":
    main()
