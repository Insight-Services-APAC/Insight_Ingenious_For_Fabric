import os
import re
from pathlib import Path
from typing import List, Dict, Tuple
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
from rich import print as rprint

# Initialize Rich console
console = Console()

def find_content_blocks(file_path: Path) -> List[Dict[str, any]]:
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
        start_match = re.match(r'#\s*_blockstart:\s*(\w+)', line)
        if start_match:
            block_name = start_match.group(1)
            start_line = i
            
            # Find the corresponding block end
            j = i + 1
            block_content_lines = []
            end_line = None
            
            while j < len(lines):
                # Check for block end with matching name
                end_match = re.match(rf'#\s*_?blockend:\s*{block_name}', lines[j])
                if end_match:
                    end_line = j
                    break
                block_content_lines.append((j, lines[j]))  # Store line number with content
                j += 1
            
            if end_line:
                # Find the actual content to be replaced
                replacement_start_idx = None
                replacement_end_idx = len(block_content_lines)
                
                # Find where the actual content starts (after first MARKDOWN section)
                for idx, (line_num, content_line) in enumerate(block_content_lines):
                    if '# MARKDOWN ********************' in content_line:
                        replacement_start_idx = idx + 1
                        break
                
                # Find where the content ends (before the last METADATA section)
                # Work backwards to find the last METADATA section
                for idx in range(len(block_content_lines) - 1, -1, -1):
                    if '# METADATA ********************' in block_content_lines[idx][1]:
                        replacement_end_idx = idx
                        break
                
                # Extract replacement content and line numbers
                if replacement_start_idx is not None and replacement_start_idx < replacement_end_idx:
                    replacement_lines = block_content_lines[replacement_start_idx:replacement_end_idx]
                    # Get actual line numbers
                    if replacement_lines:
                        actual_start_line = replacement_lines[0][0] + 1  # Convert to 1-based
                        actual_end_line = replacement_lines[-1][0] + 1   # Convert to 1-based
                        replacement_content = '\n'.join([line[1] for line in replacement_lines]).strip()
                    else:
                        actual_start_line = start_line + 2
                        actual_end_line = start_line + 2
                        replacement_content = ""
                else:
                    # If no MARKDOWN/METADATA markers found, use default
                    if len(block_content_lines) > 0:
                        actual_start_line = block_content_lines[0][0] + 1
                        actual_end_line = block_content_lines[-1][0] + 1
                        replacement_content = '\n'.join([line[1] for line in block_content_lines]).strip()
                    else:
                        actual_start_line = start_line + 2
                        actual_end_line = end_line
                        replacement_content = ""
                
                blocks.append({
                    'block_name': block_name,
                    'start_line': start_line + 1,  # 1-based line numbers
                    'end_line': end_line + 1,
                    'total_lines': end_line - start_line + 1,
                    'replacement_content': replacement_content,
                    'replacement_start_line': actual_start_line,
                    'replacement_end_line': actual_end_line,
                    'file_path': str(file_path)
                })
                
                i = end_line + 1
            else:
                console.print(f"[yellow]Warning: No matching blockend found for block '{block_name}' at line {start_line + 1}[/yellow]")
                i += 1
        else:
            i += 1
    
    return blocks

def find_notebook_content_files(base_dir: Path) -> List[Dict[str, any]]:
    """
    Scan directory recursively for all notebook-content.py files.
    
    Args:
        base_dir: The base directory to start scanning from
        
    Returns:
        List of dictionaries containing file information
    """
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

def display_blocks_summary(all_blocks: Dict[str, List[Dict]]) -> None:
    """Display a summary of all found blocks across all files."""
    
    # Count total blocks
    total_blocks = sum(len(blocks) for blocks in all_blocks.values())
    
    console.print(Panel.fit(
        f"[bold cyan]Found {total_blocks} content blocks across {len(all_blocks)} files[/bold cyan]",
        border_style="cyan"
    ))
    console.print()
    
    if total_blocks == 0:
        console.print("[yellow]No content blocks found.[/yellow]")
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
            content_preview = block['replacement_content'].replace('\n', ' ')
            if len(content_preview) > 50:
                content_preview = content_preview[:47] + "..."
            
            table.add_row(
                notebook_name,
                block['block_name'],
                f"{block['start_line']}-{block['end_line']}",
                f"{block['replacement_start_line']}-{block['replacement_end_line']}",
                content_preview
            )
    
    console.print(table)
    
    # Show detailed content for each block
    console.print("\n[bold]Detailed Block Contents:[/bold]\n")
    
    for file_path, blocks in all_blocks.items():
        if blocks:
            notebook_name = Path(file_path).parent.name
            console.print(f"[bold cyan]📓 {notebook_name}[/bold cyan]")
            console.print(f"[dim]{file_path}[/dim]\n")
            
            for block in blocks:
                console.print(f"[bold green]Block: {block['block_name']}[/bold green]")
                console.print(f"Full block: lines {block['start_line']}-{block['end_line']}")
                console.print(f"Content to replace: lines {block['replacement_start_line']}-{block['replacement_end_line']}")
                console.print("[bold]Content to be replaced:[/bold]")
                
                # Display the replacement content with syntax highlighting
                if block['replacement_content']:
                    syntax = Syntax(
                        block['replacement_content'], 
                        "python", 
                        theme="monokai", 
                        line_numbers=True,
                        start_line=block['replacement_start_line']
                    )
                    console.print(syntax)
                else:
                    console.print("[dim italic]<empty content>[/dim italic]")
                
                console.print("-" * 60 + "\n")

def main():
    """Main function to scan for notebook-content.py files and identify content blocks."""
    
    # Define the base directory to scan
    fabric_workspace_dir = Path.cwd() / "fabric_workspace_items"
    
    # Alternative: Allow user to specify directory
    import sys
    if len(sys.argv) > 1:
        fabric_workspace_dir = Path(sys.argv[1])
    
    console.print(f"[bold blue]Scanning directory:[/bold blue] {fabric_workspace_dir}")
    
    if not fabric_workspace_dir.exists():
        console.print(f"[red]Error: Directory does not exist: {fabric_workspace_dir}[/red]")
        return
    
    # Find all notebook-content.py files
    try:
        notebook_files = find_notebook_content_files(fabric_workspace_dir)
        
        if not notebook_files:
            console.print("[yellow]No notebook-content.py files found.[/yellow]")
            return
        
        # Find content blocks in each file
        all_blocks = {}
        
        with console.status("[bold green]Scanning for content blocks...") as status:
            for notebook_info in notebook_files:
                file_path = notebook_info["absolute_path"]
                status.update(f"Scanning {notebook_info['notebook_name']}...")
                
                try:
                    blocks = find_content_blocks(file_path)
                    if blocks:
                        all_blocks[str(file_path)] = blocks
                except Exception as e:
                    console.print(f"[red]Error processing {file_path}: {str(e)}[/red]")
        
        # Display results
        display_blocks_summary(all_blocks)
        
        return all_blocks
            
    except Exception as e:
        console.print(f"[red]Error scanning directory: {str(e)}[/red]")
        import traceback
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    blocks = main()