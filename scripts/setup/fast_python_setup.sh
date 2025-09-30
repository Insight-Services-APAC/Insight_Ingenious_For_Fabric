#!/bin/bash
# Fast Python setup for ingen_fab container

# Remove Spark from PYTHONPATH for faster imports
export PYTHONPATH_ORIGINAL="$PYTHONPATH"
export PYTHONPATH=""

# Use uv's Python for better performance
alias fast-python="uv run python"
alias fast-pip="uv pip"

# For development work, use uv directly
alias dev-run="uv run"
alias dev-sync="uv sync"

# Restore Spark environment when needed
restore_spark_env() {
    export PYTHONPATH="$PYTHONPATH_ORIGINAL"
    echo "Spark environment restored"
}

# Test performance
echo "Testing Python import performance..."
time python -c "import sys"

echo ""
echo "Fast Python setup complete!"
echo "Use 'fast-python' for improved performance"
echo "Use 'restore_spark_env' to restore Spark when needed"
