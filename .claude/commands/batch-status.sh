#!/bin/bash
# Command: batch-status
# Usage: batch-status
# Description: Show table documentation progress

TRACKING_DIR="/workspaces/i4f/database-docs/tracking"
ENTITIES_FILE="$TRACKING_DIR/entities.yml"

echo "=== Table Documentation Progress ==="
echo ""

# Count total tables
TOTAL_TABLES=$(grep -c "^- dbo\." "$ENTITIES_FILE" 2>/dev/null || echo "1194")

# Count verified tables
VERIFIED=0
NEEDS_REVIEW=0
NOT_PROCESSED=0

for metadata_file in "$TRACKING_DIR/tables"/*/metadata.yml; do
    if [ -f "$metadata_file" ]; then
        if grep -q "docs_verified: verified" "$metadata_file"; then
            ((VERIFIED++))
        elif grep -q "docs_verified: needs_review" "$metadata_file"; then
            ((NEEDS_REVIEW++))
        else
            ((NOT_PROCESSED++))
        fi
    fi
done

echo "📊 Progress Summary:"
echo "   Total tables: $TOTAL_TABLES"
echo "   ✅ Verified: $VERIFIED"
echo "   ⚠️  Needs review: $NEEDS_REVIEW" 
echo "   ⏳ Not processed: $NOT_PROCESSED"
echo ""

if [ $((VERIFIED + NEEDS_REVIEW)) -gt 0 ]; then
    PERCENT=$((((VERIFIED + NEEDS_REVIEW) * 100) / TOTAL_TABLES))
    echo "📈 Completion: $PERCENT%"
fi

# Show recently updated tables
echo ""
echo "🕐 Recently updated tables:"
find "$TRACKING_DIR/tables" -name "metadata.yml" -newer "$TRACKING_DIR/entities.yml" | head -10 | while read file; do
    table_name=$(basename $(dirname "$file"))
    echo "   - $table_name"
done