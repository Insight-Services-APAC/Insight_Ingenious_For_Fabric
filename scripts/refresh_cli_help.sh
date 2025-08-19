#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

echo "[refresh] Generating CLI --help snippets..."
python "$ROOT/scripts/generate_cli_help_snippets.py"
echo "[refresh] Done. Snippets written under docs/snippets/cli/"

