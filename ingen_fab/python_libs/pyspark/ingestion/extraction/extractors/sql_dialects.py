# SQL Dialect Support
# Data-driven SQL dialect configuration for multi-database support

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, List, Optional


@dataclass
class SQLDialect:
    """Configuration for a SQL dialect."""
    name: str
    quote_start: str
    quote_end: str


# Dialect registry
DIALECTS = {
    "sqlserver": SQLDialect("sqlserver", "[", "]"),
    "synapse": SQLDialect("synapse", "[", "]"),  # Same as sqlserver, enables CETAS
    "postgres": SQLDialect("postgres", '"', '"'),
    "oracle": SQLDialect("oracle", '"', '"'),
}


def get_dialect(db_type: str) -> SQLDialect:
    """
    Get dialect by name.

    Args:
        db_type: Database type (sqlserver, synapse, postgres, oracle)

    Returns:
        SQLDialect instance

    Raises:
        ValueError: If db_type is not recognized
    """
    if db_type not in DIALECTS:
        available = ", ".join(sorted(DIALECTS.keys()))
        raise ValueError(f"Unknown db_type: '{db_type}'. Supported: {available}")
    return DIALECTS[db_type]


def quote_identifier(identifier: str, dialect: SQLDialect) -> str:
    """
    Quote a column or table identifier.

    Args:
        identifier: Column or table name
        dialect: SQL dialect to use

    Returns:
        Quoted identifier (e.g., [column] for sqlserver, "column" for postgres)
    """
    return f"{dialect.quote_start}{identifier}{dialect.quote_end}"


def format_value(value: Any, dialect: SQLDialect) -> str:
    """
    Format a Python value as a SQL literal.

    Args:
        value: Python value (None, str, int, float, date, datetime, bool)
        dialect: SQL dialect to use

    Returns:
        SQL literal string
    """
    if value is None:
        return "NULL"

    if isinstance(value, datetime):
        formatted = value.strftime('%Y-%m-%d %H:%M:%S.%f')
        if dialect.name == "oracle":
            return f"TO_TIMESTAMP('{formatted}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        return f"'{formatted}'"

    if isinstance(value, date):
        formatted = value.strftime('%Y-%m-%d')
        if dialect.name == "oracle":
            return f"TO_DATE('{formatted}', 'YYYY-MM-DD')"
        return f"'{formatted}'"

    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(value, bool):
        if dialect.name == "oracle":
            return "1" if value else "0"
        return "TRUE" if value else "FALSE"

    return str(value)


def build_select(
    table: str,
    dialect: SQLDialect,
    schema: Optional[str] = None,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    incremental_column: Optional[str] = None,
    watermark_value: Optional[Any] = None,
    range_start: Optional[Any] = None,
    range_end: Optional[Any] = None,
    range_start_inclusive: bool = False,
) -> str:
    """
    Build a SELECT query.

    Args:
        table: Table name
        dialect: SQL dialect to use
        schema: Optional schema name
        columns: Optional list of columns (None = SELECT *)
        where_clause: Optional static WHERE clause
        incremental_column: Column for incremental extraction
        watermark_value: Last watermark value (extract rows > this value)
        range_start: Range start value (exclusive by default, use range_start_inclusive=True for >=)
        range_end: Range end value (exclusive, <) - pass the day AFTER the last day to include
        range_start_inclusive: If True, use >= for range_start; if False, use >

    Returns:
        SELECT query string

    Note:
        Query pattern for chunked extraction:
        - First chunk: `col > start AND col < end` (start is watermark or incremental_start)
        - Subsequent chunks: `col >= start AND col < end` (include boundary excluded by previous <)

        The < operator for end ensures clean date boundaries:
        - `< '2025-01-08'` captures all data through 2025-01-07 23:59:59.999...
        - No need for microsecond calculations

        Standard watermark (non-chunked): `col > watermark` with no end boundary
    """
    # SELECT clause
    if columns:
        cols = ", ".join(quote_identifier(c, dialect) for c in columns)
        select_clause = f"SELECT {cols}"
    else:
        select_clause = "SELECT *"

    # FROM clause
    if schema:
        from_clause = f"FROM {quote_identifier(schema, dialect)}.{quote_identifier(table, dialect)}"
    else:
        from_clause = f"FROM {quote_identifier(table, dialect)}"

    # WHERE clause
    where_parts = []

    if where_clause:
        where_parts.append(f"({where_clause})")

    # Range filters for chunked extraction (takes precedence over watermark)
    if incremental_column and (range_start is not None or range_end is not None):
        quoted_col = quote_identifier(incremental_column, dialect)
        if range_start is not None:
            formatted_start = format_value(range_start, dialect)
            # Use >= for first run (inclusive), > for subsequent (exclusive)
            op = ">=" if range_start_inclusive else ">"
            where_parts.append(f"{quoted_col} {op} {formatted_start}")
        if range_end is not None:
            formatted_end = format_value(range_end, dialect)
            where_parts.append(f"{quoted_col} < {formatted_end}")
    # Standard watermark filter (only if range not provided)
    elif watermark_value is not None and incremental_column:
        formatted_value = format_value(watermark_value, dialect)
        where_parts.append(
            f"{quote_identifier(incremental_column, dialect)} > {formatted_value}"
        )

    # Combine
    if where_parts:
        where_combined = " AND ".join(where_parts)
        return f"{select_clause} {from_clause} WHERE {where_combined}"

    return f"{select_clause} {from_clause}"


def build_cetas_wrapper(
    select_query: str,
    output_path: str,
    data_source: str,
    file_format: str,
    external_table: str,
) -> str:
    """
    Wrap a SELECT query in Synapse CETAS (Create External Table As Select).

    Generates a complete CETAS script with:
    1. DROP IF EXISTS - ensures external table doesn't exist before creating
    2. CREATE EXTERNAL TABLE AS SELECT - writes data to the location
    3. DROP EXTERNAL TABLE - cleanup after data is written (table not needed)

    Args:
        select_query: The SELECT query to wrap
        output_path: Output location path
        data_source: Synapse external data source name
        file_format: Synapse file format name (default: ParquetFileFormat)
        external_table: External table name (default: exports.temp_extract)

    Returns:
        Complete CETAS script with pre-drop, create, and post-drop statements
    """
    return f"""IF OBJECT_ID('{external_table}', 'U') IS NOT NULL
    DROP EXTERNAL TABLE {external_table};

CREATE EXTERNAL TABLE {external_table}
WITH (
    LOCATION = '{output_path}',
    DATA_SOURCE = [{data_source}],
    FILE_FORMAT = [{file_format}]
)
AS
{select_query};

DROP EXTERNAL TABLE {external_table};"""
