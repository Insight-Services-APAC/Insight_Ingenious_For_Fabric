#!/bin/bash

# Validate all table documentation files
# Usage: validate-all-docs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/validate-all-docs.py"

python "$PYTHON_SCRIPT"