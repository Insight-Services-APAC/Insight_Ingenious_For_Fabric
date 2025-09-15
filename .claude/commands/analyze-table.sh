#!/bin/bash
# Table Analysis Command Wrapper
# Usage: ./analyze-table.sh <table_name> [schema_name]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
SCHEMA_NAME="dbo"
TABLE_NAME=""

# Parse arguments
if [ $# -eq 0 ]; then
    echo -e "${RED}Error: Table name is required${NC}"
    echo "Usage: $0 <table_name> [schema_name]"
    echo ""
    echo "Examples:"
    echo "  $0 account"
    echo "  $0 contact dbo"
    echo "  $0 custtable"
    exit 1
fi

TABLE_NAME="$1"
if [ $# -ge 2 ]; then
    SCHEMA_NAME="$2"
fi

echo -e "${BLUE}🚀 Fabric Data Warehouse Table Analysis${NC}"
echo -e "${BLUE}=======================================${NC}"
echo -e "📊 Table: ${GREEN}${SCHEMA_NAME}.${TABLE_NAME}${NC}"
echo -e "📅 Started: $(date)"
echo ""

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo -e "${YELLOW}⚠️  Activating virtual environment...${NC}"
    source .venv/bin/activate
fi

# Check if required environment variables are set
if [[ -z "$FABRIC_SQL_SERVER" && -z "$FABRIC_DATABASE" ]]; then
    echo -e "${YELLOW}⚠️  Setting default environment variables...${NC}"
    export FABRIC_ENVIRONMENT="local"
    export FABRIC_WORKSPACE_REPO_DIR="sample_project"
fi

# Execute the analysis
echo -e "${GREEN}🔍 Starting comprehensive table analysis...${NC}"
echo ""

python .claude/commands/analyze_table.py "$TABLE_NAME" --schema "$SCHEMA_NAME" --save

echo ""
echo -e "${GREEN}✅ Analysis completed successfully!${NC}"
echo -e "${BLUE}📁 Check the generated JSON file for detailed results${NC}"
echo -e "${BLUE}📖 Use the results to create comprehensive documentation${NC}"