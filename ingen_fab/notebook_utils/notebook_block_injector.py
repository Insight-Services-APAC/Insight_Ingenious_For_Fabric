import os
import re
import shutil
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

        self.script_path = Path(__file__).resolve()
        self.script_dir = self.script_path.parent
        self.project_root_dir = self.script_dir.parent
        self.python_libs_dir = self.project_root_dir / "python_libs"

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
                start_line_idx = i  # 0-based index of _blockstart line

                # Find the corresponding block end
                j = i + 1
                block_content_lines_with_idx = []  # Stores (original_line_num_0_based, line_content)
                end_line_idx = None  # 0-based index of _blockend line

                while j < len(lines):
                    # Check for block end with matching name
                    end_match = re.match(rf"#\s*_?blockend:\s*{block_name}", lines[j])
                    if end_match:
                        end_line_idx = j
                        break
                    block_content_lines_with_idx.append(
                        (j, lines[j])
                    )  # Store 0-based original line number with content
                    j += 1

                if end_line_idx is not None:
                    replacement_start_idx_in_block_content = 0
                    replacement_end_idx_in_block_content = len(
                        block_content_lines_with_idx
                    )

                    # Find where the content ends: BEFORE the last '# METADATA ********************' section.
                    for idx in range(len(block_content_lines_with_idx) - 1, -1, -1):
                        if (
                            "# METADATA ********************"
                            in block_content_lines_with_idx[idx][1]
                        ):
                            # --- MODIFIED LINE HERE ---
                            replacement_end_idx_in_block_content = idx  # Original line, included METADATA line index for slicing
                            if (
                                replacement_end_idx_in_block_content > 0
                            ):  # Ensure we don't go negative
                                replacement_end_idx_in_block_content -= (
                                    1  # Exclude the empty line before METADATA
                                )
                            break

                    replacement_lines_data = []
                    if (
                        replacement_start_idx_in_block_content
                        < replacement_end_idx_in_block_content
                    ):
                        replacement_lines_data = block_content_lines_with_idx[
                            replacement_start_idx_in_block_content:replacement_end_idx_in_block_content
                        ]

                    if replacement_lines_data:
                        actual_start_line = replacement_lines_data[0][0] + 1
                        actual_end_line = replacement_lines_data[-1][0] + 1
                        replacement_content = "\n".join(
                            [line_tuple[1] for line_tuple in replacement_lines_data]
                        ).strip()
                    else:
                        actual_start_line = start_line_idx + 2
                        actual_end_line = end_line_idx
                        replacement_content = ""

                    blocks.append(
                        {
                            "block_name": block_name,
                            "start_line": start_line_idx + 1,
                            "end_line": end_line_idx + 1,
                            "total_lines": end_line_idx - start_line_idx + 1,
                            "replacement_content": replacement_content,
                            "replacement_start_line": actual_start_line,
                            "replacement_end_line": actual_end_line,
                            "file_path": str(file_path),
                        }
                    )

                    i = end_line_idx + 1
                else:
                    self.console.print(
                        f"[yellow]Warning: No matching blockend found for block '"
                        f"{block_name}' at line {start_line_idx + 1}[/yellow]"
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

    def replace_content_block(self, block_name: str) -> str | None:
        """
        Finds and returns the content of a file within the python_libs directory
        based on the provided block_name, which is in 'subdirectory_filename' format.

        Args:
            block_name: The name of the block to replace, e.g., 'pyspark_dd_utils'.
                        This will be mapped to a file like 'python_libs/pyspark/dd_utils.py'.

        Returns:
            The content of the file if found, otherwise None.
        """
        parts = block_name.split("_", 1)

        if len(parts) < 2:
            self.console.print(
                f"[yellow]Warning: Block name '{block_name}' does not match expected 'subdirectory_filename' format.[/yellow]"
            )
            return None

        subdirectory = parts[0]
        filename_base = parts[1]

        if filename_base.endswith("_"):
            filename_base = filename_base[:-1]

        replacement_file_path = (
            self.python_libs_dir / subdirectory / f"{filename_base}.py"
        )

        self.console.print(
            f"\n[bold blue]Looking for replacement file: {replacement_file_path}...[/bold blue]"
        )

        if not replacement_file_path.exists():
            self.console.print(
                f"[red]Error: Replacement file not found for block '{block_name}': {replacement_file_path}[/red]"
            )
            return None

        try:
            with replacement_file_path.open("r", encoding="utf-8") as f:
                content = f.read()

            self.console.print(
                f"[green]Successfully loaded replacement content for block '{block_name}' from {replacement_file_path}[/green]"
            )
            return content
        except Exception as e:
            self.console.print(
                f"[red]Error reading replacement file {replacement_file_path}: {str(e)}[/red]"
            )
            return None

    def apply_replacements(self, all_blocks: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Applies the replacement of content blocks in the notebook-content.py files.

        Args:
            all_blocks: A dictionary where keys are file paths and values are
                        lists of block information found in those files.
        """

        for file_path_str, blocks in all_blocks.items():
            file_path = Path(file_path_str)
            if not file_path.exists():
                self.console.print(
                    f"[red]Skipping {file_path_str}: File not found.[/red]"
                )
                continue

            # --- CRITICAL: Create a backup before modifying the file ---
            self.console.print(
                f"\n[bold blue]Creating backup for {file_path.name}...[/bold blue]"
            )
            backup_file_path = file_path.with_suffix(file_path.suffix + ".bak")
            try:
                shutil.copyfile(file_path, backup_file_path)
                self.console.print(f"[green]Backup created: {backup_file_path}[/green]")
            except Exception as e:
                self.console.print(
                    f"[red]Error creating backup for {file_path.name}: {str(e)}. Skipping replacement for this file.[/red]"
                )
                continue
            # --- End of Backup Block ---

            original_lines = file_path.open("r", encoding="utf-8").readlines()
            new_lines = list(original_lines)  # Create a mutable copy

            # Track if any actual changes were made to this file
            file_was_modified = False

            # Sort blocks by their start line in reverse order to avoid index shifting issues
            blocks_to_process = sorted(
                blocks, key=lambda b: b["replacement_start_line"], reverse=True
            )

            for block in blocks_to_process:
                block_name = block["block_name"]
                replacement_content = self.replace_content_block(block_name)

                if replacement_content is None:
                    self.console.print(
                        f"[yellow]Skipping replacement for block '{block_name}' in {file_path.name} due to missing or unreadable replacement content.[/yellow]"
                    )
                    continue  # Move to the next block

                # If we got here, we have valid replacement content, so this file will be modified
                file_was_modified = True

                # Adjust for 0-based indexing for Python list manipulation
                start_replace_idx = block["replacement_start_line"] - 1
                end_replace_idx = block["replacement_end_line"] - 1

                if start_replace_idx > end_replace_idx:
                    self.console.print(
                        f"[dim]Block '{block_name}' in {file_path.name} had empty or minimal content. Inserting new content.[/dim]"
                    )
                    end_replace_idx = start_replace_idx - 1

                replacement_lines = [
                    line + "\n" for line in replacement_content.splitlines()
                ]
                if (
                    replacement_lines
                    and not original_lines[-1].endswith("\n")
                    and start_replace_idx + len(replacement_lines)
                    >= len(original_lines)
                ):
                    replacement_lines[-1] = replacement_lines[-1].rstrip("\n")

                if not replacement_lines:
                    replacement_lines = ["\n"]

                # Perform the replacement: delete old lines and insert new ones
                del new_lines[start_replace_idx : end_replace_idx + 1]
                for k, line_to_insert in enumerate(replacement_lines):
                    new_lines.insert(start_replace_idx + k, line_to_insert)

                self.console.print(
                    f"[green]Successfully replaced content for block '{block_name}' in {file_path.name}.[/green]"  # <-- Log success for individual block here
                )

            # --- Write the modified content back to the file ONLY IF CHANGES WERE MADE ---
            if file_was_modified:
                try:
                    with file_path.open("w", encoding="utf-8") as f:
                        f.writelines(new_lines)
                    self.console.print(
                        f"[green]\nFile update complete: {file_path.name} has been modified.[/green]\n"
                    )
                except Exception as e:
                    self.console.print(
                        f"[red]Error writing to {file_path.name}: {str(e)}[/red]"
                    )
            else:
                self.console.print(
                    f"[yellow]\nNo blocks were successfully replaced in {file_path.name}. File remains unchanged.[/yellow]\n"
                )


def main(apply_replacements: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    finder = NotebookContentFinder()
    all_blocks = finder.scan_and_display_blocks()

    if apply_replacements and all_blocks:
        finder.apply_replacements(all_blocks)

    return all_blocks


if __name__ == "__main__":
    main()
