import csv
from pathlib import Path
from rich.console import Console

console = Console()

def generate_ddl_scripts(ctx, lakehouse):
    """
    Generate one DDL Python script per table from the metadata CSV for the specified lakehouse.
    Each script is named like '001_<lakehouse>_<table>.py' and follows the pyspark StructType pattern.
    """
    workspace_dir = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    if not workspace_dir:
        console.print("[red]Fabric workspace repo dir not provided.[/red]")
        return

    workspace_dir = Path(workspace_dir)
    metadata_csv = workspace_dir / "metadata" / "lakehouse_metadata_all.csv"
    if not metadata_csv.exists():
        console.print(f"[red]Metadata CSV not found:[/red] {metadata_csv}")
        return

    # Output directory for DDL scripts
    ddl_dir = workspace_dir / "ddl_scripts" / "Lakehouses" / lakehouse / "generated"
    ddl_dir.mkdir(parents=True, exist_ok=True)

    # Map TSQL types to PySpark types
    type_map = {
        "bigint": "LongType()",
        "int": "IntegerType()",
        "decimal": "DecimalType(32,9)",
        "bit": "BooleanType()",
        "nvarchar": "StringType()",
        "varchar": "StringType()",
        "date": "DateType()",
        "datetime": "TimestampType()",
        "datetime2": "TimestampType()",
        "timestamp": "TimestampType()",
        "string": "StringType()",
        # Add more mappings as needed
    }

    tables = {}
    try:
        with metadata_csv.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lakehouse_name = row.get("lakehouse_name", "").strip()
                table_name = row.get("table_name", "").strip()
                column_name = row.get("column_name", "").strip()
                tsql_type = row.get("data_type", "").strip().lower()

                # Filter by lakehouse parameter
                if lakehouse_name != lakehouse:
                    continue

                if not lakehouse_name or not table_name or not column_name:
                    continue

                # Map to PySpark type, default to StringType if unknown
                spark_type = type_map.get(tsql_type.split('(')[0], "StringType()")

                if table_name not in tables:
                    tables[table_name] = []

                tables[table_name].append({
                    "name": column_name,
                    "spark_type": spark_type,
                })

        # Generate one Python file per table
        for idx, (table_name, columns) in enumerate(sorted(tables.items()), start=1):
            filename = f"{idx:03d}_{lakehouse}_{table_name}.py"
            file_path = ddl_dir / filename

            # Build StructField lines
            struct_fields = []
            for col in columns:
                struct_fields.append(f"        StructField('{col['name']}', {col['spark_type']}, True),")

            file_content = f"""from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    DateType,
    TimestampType,
    DecimalType,
    IntegerType    
)

# DDL script for creating table '{table_name}' in lakehouse '{lakehouse}'

schema = StructType(
    [
{chr(10).join(struct_fields)}
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="{table_name}", schema_name=""
)
"""

            with file_path.open("w", encoding="utf-8") as f:
                f.write(file_content)

            console.print(f"[green]✓[/green] Created {file_path}")

    except Exception as e:
        console.print(f"[red]Error generating DDL scripts: {e}[/red]")