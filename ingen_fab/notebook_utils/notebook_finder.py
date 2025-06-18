from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional


class NotebookContentFinder:
    """Find ``notebook-content.py`` files inside a base directory."""

    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir)

    def find(self) -> List[Dict[str, Optional[str]]]:
        """Return information about all ``notebook-content.py`` files."""
        notebook_files: List[Dict[str, Optional[str]]] = []

        for root, _, files in os.walk(self.base_dir):
            for file in files:
                if file == "notebook-content.py":
                    file_path = Path(root) / file
                    notebook_dir = file_path.parent
                    notebook_name = notebook_dir.name
                    relative_path = file_path.relative_to(self.base_dir)

                    lakehouse: Optional[str] = None
                    parts = relative_path.parts
                    if len(parts) > 2:
                        lakehouse = parts[0]

                    notebook_files.append(
                        {
                            "absolute_path": str(file_path),
                            "relative_path": str(relative_path),
                            "notebook_name": notebook_name,
                            "lakehouse": lakehouse,
                        }
                    )
        return notebook_files
