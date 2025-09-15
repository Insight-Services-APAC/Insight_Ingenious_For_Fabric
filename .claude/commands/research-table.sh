#!/bin/bash
# Command: research-table
# Usage: research-table <table_name>
# Description: Research and update documentation for a specific table

if [ -z "$1" ]; then
    echo "Usage: research-table <table_name>"
    echo "Example: research-table dbo.account"
    exit 1
fi

TABLE_NAME="$1"
METADATA_FILE="/workspaces/i4f/database-docs/tracking/tables/$TABLE_NAME/metadata.yml"

if [ ! -f "$METADATA_FILE" ]; then
    echo "❌ Metadata file not found: $METADATA_FILE"
    exit 1
fi

echo "🔍 Researching documentation for: $TABLE_NAME"
echo "📄 Metadata file: $METADATA_FILE"
echo ""
echo "Current metadata:"
cat "$METADATA_FILE"
echo ""
echo "✅ Ready for Claude to research and update this table's documentation."