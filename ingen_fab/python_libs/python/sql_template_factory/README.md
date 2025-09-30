# SQL Templates

This directory contains SQL templates organized by database dialect. Templates use Jinja2 syntax for parameter substitution.

## Directory Structure

```text
sql_templates/
├── fabric/
│   ├── check_table_exists.sql.jinja
│   ├── create_table_from_values.sql.jinja
│   ├── drop_table.sql.jinja
│   ├── insert_row.sql.jinja
│   └── list_tables.sql.jinja
└── sqlserver/
    ├── check_table_exists.sql.jinja
    ├── create_table_from_values.sql.jinja
    ├── drop_table.sql.jinja
    ├── insert_row.sql.jinja
    └── list_tables.sql.jinja
```

## Usage

```python
from sql_templates import SQLTemplates

# Create instance for specific dialect
templates = SQLTemplates(dialect="fabric")  # or "sqlserver"

# Render a template
sql = templates.render("check_table_exists",
                      schema_name="my_schema",
                      table_name="my_table")
```

## Available Templates

### check_table_exists

Checks if a table exists in the database.

**Parameters:**

- `schema_name`: The schema/database name
- `table_name`: The table name

### drop_table

Drops a table if it exists.

**Parameters:**

- `schema_name`: The schema/database name
- `table_name`: The table name

### create_table_from_values

Creates a table from a VALUES clause.

**Parameters:**

- `schema_name`: The schema/database name
- `table_name`: The table name
- `values_clause`: The VALUES clause content (e.g., "(1, 'test'), (2, 'test2')")
- `column_names`: Comma-separated column names (e.g., "id, name")

### insert_row

Inserts a single row into a table.

**Parameters:**

- `schema_name`: The schema/database name
- `table_name`: The table name
- `row_values`: Comma-separated values to insert (e.g., "1, 'test'")

### list_tables

Lists tables in the database.

**Parameters:**

- `prefix` (optional): Filter tables by name prefix

## Adding New Templates

1. Create the `.sql.jinja` file in both `fabric/` and `sqlserver/` directories
2. Use Jinja2 syntax for parameter substitution: `{{ parameter_name }}`
3. Run generate_templates.py to update the SQLTemplates class with the new templates
