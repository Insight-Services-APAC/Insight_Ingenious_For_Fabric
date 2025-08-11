#!/usr/bin/env python3
"""
Generate CLI --help snippets for inclusion in docs via pymdownx.snippets.

Writes files under docs/snippets/cli/*.md with fenced code blocks of --help output.

Usage:
  python scripts/generate_cli_help_snippets.py

Assumptions:
  - Can execute the CLI via `python -m ingen_fab.cli`.
  - No Fabric environment setup is required for --help (Typer exits before validation).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SNIPPETS_DIR = ROOT / "docs" / "snippets" / "cli"


COMMANDS = {
    "root_help": [],
    "deploy_help": ["deploy"],
    "deploy_get_metadata_help": ["deploy", "get-metadata"],
    "ddl_help": ["ddl"],
    "init_help": ["init"],
    "notebook_help": ["notebook"],
    "test_help": ["test"],
    "package_help": ["package"],
    "libs_help": ["libs"],
    "dbt_help": ["dbt"],
    "extract_help": ["extract"],
}


def run_help(args: list[str]) -> str:
    cmd = ["python", "-m", "ingen_fab.cli", *args, "--help"]
    env = dict(**os.environ)
    # Ensure validation passes for subcommands that check env
    env.setdefault("FABRIC_ENVIRONMENT", "development")
    env.setdefault("FABRIC_WORKSPACE_REPO_DIR", str(ROOT / "sample_project"))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    out = result.stdout.strip()
    err = result.stderr.strip()
    # Some CLIs print help to stderr; include both to be safe.
    text = "\n".join(x for x in (out, err) if x)
    if not text:
        text = f"(no output from: {' '.join(cmd)})\n"
    return text


def write_snippet(name: str, content: str) -> None:
    SNIPPETS_DIR.mkdir(parents=True, exist_ok=True)
    path = SNIPPETS_DIR / f"{name}.md"
    fence = "```text\n" + content + "\n```\n"
    path.write_text(fence, encoding="utf-8")
    print(f"Wrote {path.relative_to(ROOT)}")


def main() -> None:
    for name, args in COMMANDS.items():
        help_text = run_help(args)
        write_snippet(name, help_text)


if __name__ == "__main__":
    main()
