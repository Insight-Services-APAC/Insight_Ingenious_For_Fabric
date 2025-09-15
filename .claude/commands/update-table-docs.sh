#!/bin/bash

# Update table documentation using structured data model
# Usage: update-table-docs <table_name> [options]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/update_table_docs.py"

# Check if table name provided
if [ $# -lt 1 ]; then
    echo "Usage: update-table-docs <table_name> [--d365-url URL] [--dataverse-url URL] [--fo-url URL] [--notes 'notes']"
    echo "Example: update-table-docs dbo.contact --dataverse-url 'https://learn.microsoft.com/...'"
    exit 1
fi

# Run the Python script with all arguments
python "$PYTHON_SCRIPT" "$@"