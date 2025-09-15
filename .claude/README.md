# Claude Configuration and Tools

This directory contains reusable prompts, commands, hooks, and configuration for systematic database analysis and documentation.

## 📁 Directory Structure

```
.claude/
├── commands/           # Custom commands for automation
├── hooks/             # Automated analysis hooks  
├── prompts/           # Reusable analysis prompts
├── config.json        # Configuration and settings
└── README.md          # This file
```

## 🚀 Quick Start

### Analyze a Table (Recommended)
```bash
# Simple shell wrapper (easiest)
./claude/commands/analyze-table.sh account

# Python command with options
python .claude/commands/analyze_table.py contact --schema dbo --save

# Manual hook execution
python .claude/hooks/table_analysis_hook.py contact --schema dbo
```

## 📋 Available Tools

### Commands (`commands/`)

#### `analyze_table.py`
**Purpose**: Comprehensive table analysis with structured output  
**Features**:
- Systematic 6-step analysis process
- All SQL queries logged and displayed
- JSON output for documentation integration
- Data quality metrics and business insights

**Usage**:
```bash
# Basic analysis
python .claude/commands/analyze_table.py account

# With options
python .claude/commands/analyze_table.py contact --schema dbo --save --format json

# Help
python .claude/commands/analyze_table.py --help
```

#### `analyze-table.sh`
**Purpose**: Shell wrapper with environment setup  
**Features**:
- Automatic virtual environment activation
- Environment variable configuration
- Colored output and progress indicators
- Simple command-line interface

**Usage**:
```bash
# Make executable (first time only)
chmod +x .claude/commands/analyze-table.sh

# Analyze tables
./claude/commands/analyze-table.sh account
./claude/commands/analyze-table.sh contact dbo
```

### Hooks (`hooks/`)

#### `table_analysis_hook.py`
**Purpose**: Automated analysis hook for integration  
**Features**:
- Can be triggered programmatically
- Comprehensive logging and output
- JSON results for downstream processing
- Error handling and recovery

**Usage**:
```bash
# Direct execution
python .claude/hooks/table_analysis_hook.py account --schema dbo

# Integration with other tools
from table_analysis_hook import analyze_table
results = analyze_table('account', 'dbo')
```

### Prompts (`prompts/`)

#### `comprehensive_table_analysis.md`
**Purpose**: Complete systematic template for manual analysis  
**Use When**: Manual analysis or when commands aren't available  
**Features**: Step-by-step SQL queries, documentation template

#### `table_analysis_quick_start.md`
**Purpose**: Quick reference for essential analysis  
**Use When**: Rapid assessment or time-sensitive analysis  
**Features**: Essential queries only, streamlined workflow

## 🔧 Configuration

### Environment Setup
```bash
# Required for Fabric Data Warehouse
export FABRIC_SQL_SERVER="your-server-name"
export FABRIC_DATABASE="your-database-name"

# Optional
export FABRIC_ENVIRONMENT="local"
export FABRIC_WORKSPACE_REPO_DIR="sample_project"
```

### Virtual Environment
```bash
# Activate before using any tools
source .venv/bin/activate

# Or let analyze-table.sh do it automatically
```

## 📊 Analysis Process

All tools follow the same systematic 6-step process:

### Step 1: Basic Table Information
- Column structure and categorization
- Row count and basic statistics
- Primary key identification

### Step 2: Sample Data Analysis  
- Representative data samples
- Business pattern identification
- Data format analysis

### Step 3: Data Quality Assessment
- Completeness metrics for key fields
- Data quality scoring (✅ ⚠️ ❌)
- Null value patterns

### Step 4: State/Status Distribution
- D365 statecode/statuscode analysis
- Active vs inactive record distribution
- Business state patterns

### Step 5: Data Freshness Analysis
- Loading pattern analysis
- Data recency and update frequency
- ETL process insights

### Step 6: Relationship Analysis
- Foreign key field identification
- Cross-table dependency mapping
- Integration point discovery

## 📈 Output Examples

### Console Output
```
🚀 Starting comprehensive analysis of dbo.account
📅 Analysis started: 2025-08-22 10:30:00
================================================================================

🔍 STEP 1: BASIC TABLE INFORMATION
==================================================
📝 Get table column structure
SQL Query:
```sql
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE...
```
✅ Query executed successfully
📊 Found 323 columns
...
```

### JSON Output
```json
{
  "table_name": "account",
  "schema_name": "dbo",
  "analysis_timestamp": "2025-08-22T10:30:00",
  "analysis_steps": {
    "basic_info": {
      "total_columns": 323,
      "row_count": 229594,
      "column_categories": {
        "core_business": 6,
        "system_fields": 20,
        "relationship_fields": 31,
        "custom_extended": 266
      }
    },
    "data_quality": {
      "name": {
        "completeness_percentage": 100.0,
        "status": "Excellent"
      },
      "telephone1": {
        "completeness_percentage": 26.1,
        "status": "Poor"
      }
    }
  },
  "sql_queries_executed": [...]
}
```

## 🎯 Integration with Documentation

### MkDocs Integration
```bash
# 1. Run analysis
./claude/commands/analyze-table.sh account

# 2. Use results to create documentation
# - Copy SQL queries to documentation
# - Include sample data and metrics
# - Add business context from D365 knowledge

# 3. Update table documentation
# Use analysis results to populate comprehensive table docs
```

### Workflow Integration
```bash
# Batch analysis of multiple tables
for table in account contact lead opportunity; do
    ./claude/commands/analyze-table.sh $table
done

# Results can be processed for automated documentation
```

## ⚠️ Important Notes

### SQL Query Visibility
- **All SQL queries are displayed** to maintain transparency
- Queries are logged in results for documentation
- Manual verification and customization possible

### Data Privacy
- **No sensitive data is stored** in analysis results
- Sample data can be sanitized if needed
- Query logging can be disabled for sensitive environments

### Performance Considerations
- Analysis uses `TOP 1000` for sampling to avoid performance issues
- Row counts use efficient COUNT(*) queries
- Timeout handling for large tables

## 🔍 Troubleshooting

### Common Issues

#### "Command not found"
```bash
# Make sure you're in the project root
cd /workspaces/i4f

# Make scripts executable
chmod +x .claude/commands/analyze-table.sh
```

#### "Module not found"
```bash
# Activate virtual environment
source .venv/bin/activate

# Check if tools are installed
python -c "from fabric_sql_query_tool import FabricWarehouseQueryTool"
```

#### "Connection failed"
```bash
# Check environment variables
echo $FABRIC_SQL_SERVER
echo $FABRIC_DATABASE

# Verify Azure CLI authentication
az login
```

### Getting Help
```bash
# Command help
python .claude/commands/analyze_table.py --help

# Configuration reference
cat .claude/config.json

# Prompt templates
ls .claude/prompts/
```

## 🚀 Future Enhancements

- **Automated documentation generation**: Direct markdown output
- **Cross-table relationship mapping**: Automatic relationship discovery
- **Performance optimization recommendations**: Query and index suggestions
- **Data lineage tracking**: Source system mapping
- **Scheduled analysis**: Automated periodic analysis
- **Integration with CI/CD**: Automated documentation updates

---

This toolset provides a comprehensive, systematic approach to database table analysis that combines automation with transparency, ensuring consistent, thorough documentation across your entire data warehouse.