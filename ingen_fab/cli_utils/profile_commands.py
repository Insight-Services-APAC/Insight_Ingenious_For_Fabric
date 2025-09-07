"""CLI commands for data profiling functionality."""

import typer
from typing import Optional, List
from pathlib import Path
import json
from enum import Enum

app = typer.Typer(help="Data profiling commands")


class OutputFormat(str, Enum):
    """Output formats for profiling reports."""
    json = "json"
    html = "html"
    markdown = "markdown"


class ProfileLevel(str, Enum):
    """Profiling depth levels."""
    basic = "basic"
    statistical = "statistical"
    data_quality = "data_quality"
    full = "full"


@app.command()
def dataset(
    source: str = typer.Argument(..., help="Dataset source (table name or file path)"),
    output_file: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file path"),
    format: OutputFormat = typer.Option(OutputFormat.markdown, "--format", "-f", help="Output format"),
    level: ProfileLevel = typer.Option(ProfileLevel.basic, "--level", "-l", help="Profiling depth"),
    columns: Optional[List[str]] = typer.Option(None, "--columns", "-c", help="Specific columns to profile"),
    sample_size: Optional[float] = typer.Option(None, "--sample", "-s", help="Sample fraction (0-1)"),
    environment: Optional[str] = typer.Option(None, "--env", "-e", help="Environment to use")
):
    """Profile a dataset and generate quality report."""
    import os
    from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.packages.data_profiling.libs.interfaces.data_profiling_interface import ProfileType
    import ingen_fab.python_libs.common.config_utils as cu
    
    try:
        # Get config lakehouse utils first (it will create/reuse spark session)
        configs = cu.get_configs_as_object()
        config_lakehouse = lakehouse_utils(
            target_workspace_id=configs.config_workspace_id,
            target_lakehouse_id=configs.config_lakehouse_id
        )
        
        # Get spark session from lakehouse utils
        spark = config_lakehouse.spark
        
        # Create profiler with config lakehouse
        profiler = TieredProfiler(lakehouse=config_lakehouse, spark=spark)
        
        # Map CLI level to ProfileType
        profile_type_map = {
            "basic": ProfileType.BASIC,
            "statistical": ProfileType.STATISTICAL,
            "data_quality": ProfileType.DATA_QUALITY,
            "full": ProfileType.FULL
        }
        profile_type = profile_type_map[level.value]
        
        # Run profiling
        typer.echo(f"Profiling dataset: {source}")
        profile = profiler.profile_dataset(
            dataset=source,
            profile_type=profile_type,
            columns=columns,
            sample_size=sample_size
        )
        
        # Generate report
        report = profiler.generate_quality_report(profile, format.value)
        
        # Output results
        if output_file:
            output_file.write_text(report)
            typer.echo(f"Profile report saved to: {output_file}")
        else:
            typer.echo(report)
        
        # Show summary
        typer.echo(f"\n✅ Profiling complete!")
        typer.echo(f"  Rows: {profile.row_count:,}")
        typer.echo(f"  Columns: {profile.column_count}")
        if profile.data_quality_score:
            typer.echo(f"  Quality Score: {profile.data_quality_score:.2%}")
        
    except Exception as e:
        typer.echo(f"❌ Error profiling dataset: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def compare(
    dataset1: str = typer.Argument(..., help="First dataset to compare"),
    dataset2: str = typer.Argument(..., help="Second dataset to compare"),
    output_file: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file path"),
    level: ProfileLevel = typer.Option(ProfileLevel.basic, "--level", "-l", help="Profiling depth")
):
    """Compare profiles of two datasets to detect drift."""
    from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.packages.data_profiling.libs.interfaces.data_profiling_interface import ProfileType
    import ingen_fab.python_libs.common.config_utils as cu
    
    try:
        # Get config lakehouse utils
        configs = cu.get_configs_as_object()
        config_lakehouse = lakehouse_utils(
            target_workspace_id=configs.config_workspace_id,
            target_lakehouse_id=configs.config_lakehouse_id
        )
        
        # Get spark session from lakehouse utils
        spark = config_lakehouse.spark
        
        profiler = TieredProfiler(lakehouse=config_lakehouse, spark=spark)
        
        # Map level to ProfileType
        profile_type_map = {
            "basic": ProfileType.BASIC,
            "statistical": ProfileType.STATISTICAL,
            "data_quality": ProfileType.DATA_QUALITY,
            "full": ProfileType.FULL
        }
        profile_type = profile_type_map[level.value]
        
        # Profile both datasets
        typer.echo(f"Profiling dataset 1: {dataset1}")
        profile1 = profiler.profile_dataset(dataset1, profile_type)
        
        typer.echo(f"Profiling dataset 2: {dataset2}")
        profile2 = profiler.profile_dataset(dataset2, profile_type)
        
        # Compare profiles
        typer.echo("Comparing profiles...")
        comparison = profiler.compare_profiles(profile1, profile2)
        
        # Format and output results
        comparison_json = json.dumps(comparison, indent=2, default=str)
        
        if output_file:
            output_file.write_text(comparison_json)
            typer.echo(f"Comparison saved to: {output_file}")
        else:
            typer.echo(comparison_json)
        
        # Show summary
        typer.echo(f"\n📊 Comparison Summary:")
        typer.echo(f"  Row count change: {comparison['row_count_change']:+,} ({comparison['row_count_change_pct']:+.1f}%)")
        typer.echo(f"  Column count change: {comparison['column_count_change']:+}")
        
        if comparison.get('added_columns'):
            typer.echo(f"  Added columns: {', '.join(comparison['added_columns'])}")
        if comparison.get('removed_columns'):
            typer.echo(f"  Removed columns: {', '.join(comparison['removed_columns'])}")
        
    except Exception as e:
        typer.echo(f"❌ Error comparing datasets: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def validate(
    dataset: str = typer.Argument(..., help="Dataset to validate"),
    rules_file: Path = typer.Argument(..., help="JSON file containing validation rules"),
    output_file: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file path")
):
    """Validate a dataset against quality rules."""
    from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    import ingen_fab.python_libs.common.config_utils as cu
    
    # Load rules
    if not rules_file.exists():
        typer.echo(f"❌ Rules file not found: {rules_file}", err=True)
        raise typer.Exit(code=1)
    
    try:
        rules = json.loads(rules_file.read_text())
    except json.JSONDecodeError as e:
        typer.echo(f"❌ Invalid JSON in rules file: {e}", err=True)
        raise typer.Exit(code=1)
    
    try:
        # Get config lakehouse utils
        configs = cu.get_configs_as_object()
        config_lakehouse = lakehouse_utils(
            target_workspace_id=configs.config_workspace_id,
            target_lakehouse_id=configs.config_lakehouse_id
        )
        
        # Get spark session from lakehouse utils
        spark = config_lakehouse.spark
        
        profiler = TieredProfiler(lakehouse=config_lakehouse, spark=spark)
        
        # Validate dataset
        typer.echo(f"Validating dataset: {dataset}")
        typer.echo(f"Using {len(rules)} validation rules")
        
        validation_results = profiler.validate_against_rules(dataset, rules)
        
        # Format and output results
        results_json = json.dumps(validation_results, indent=2, default=str)
        
        if output_file:
            output_file.write_text(results_json)
            typer.echo(f"Validation results saved to: {output_file}")
        else:
            typer.echo(results_json)
        
        # Show summary
        success_rate = validation_results['success_rate']
        status_emoji = "✅" if success_rate == 100 else "⚠️" if success_rate >= 80 else "❌"
        
        typer.echo(f"\n{status_emoji} Validation Summary:")
        typer.echo(f"  Success rate: {success_rate:.1f}%")
        typer.echo(f"  Passed rules: {validation_results['passed_rules']}/{validation_results['total_rules']}")
        
        if validation_results['violations']:
            typer.echo(f"  Violations found: {len(validation_results['violations'])}")
            for violation in validation_results['violations'][:5]:  # Show first 5
                typer.echo(f"    - {violation.get('rule', 'Unknown rule')}")
        
    except Exception as e:
        typer.echo(f"❌ Error validating dataset: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def suggest_rules(
    dataset: str = typer.Argument(..., help="Dataset to analyze"),
    output_file: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file for rules"),
    min_completeness: float = typer.Option(0.95, help="Minimum completeness threshold"),
    min_uniqueness: float = typer.Option(0.95, help="Minimum uniqueness threshold")
):
    """Suggest data quality rules based on dataset profile."""
    from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.packages.data_profiling.libs.interfaces.data_profiling_interface import ProfileType
    import ingen_fab.python_libs.common.config_utils as cu
    
    try:
        # Get config lakehouse utils
        configs = cu.get_configs_as_object()
        config_lakehouse = lakehouse_utils(
            target_workspace_id=configs.config_workspace_id,
            target_lakehouse_id=configs.config_lakehouse_id
        )
        
        # Get spark session from lakehouse utils
        spark = config_lakehouse.spark
        
        profiler = TieredProfiler(lakehouse=config_lakehouse, spark=spark)
        
        # Profile dataset
        typer.echo(f"Analyzing dataset: {dataset}")
        profile = profiler.profile_dataset(dataset, ProfileType.FULL)
        
        # Generate rule suggestions
        rules = profiler.suggest_data_quality_rules(profile)
        
        # Format and output rules
        rules_json = json.dumps(rules, indent=2)
        
        if output_file:
            output_file.write_text(rules_json)
            typer.echo(f"Suggested rules saved to: {output_file}")
        else:
            typer.echo(rules_json)
        
        # Show summary
        typer.echo(f"\n📋 Suggested {len(rules)} data quality rules")
        
        rule_types = {}
        for rule in rules:
            rule_type = rule.get('type', 'unknown')
            rule_types[rule_type] = rule_types.get(rule_type, 0) + 1
        
        for rule_type, count in rule_types.items():
            typer.echo(f"  {rule_type}: {count} rules")
        
    except Exception as e:
        typer.echo(f"❌ Error suggesting rules: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def compile(
    target_datastore: str = typer.Option("lakehouse", "--target", "-t", help="Target datastore: lakehouse, warehouse, or both"),
    include_samples: bool = typer.Option(False, "--include-samples", help="Include sample DDL data"),
    include_config: bool = typer.Option(True, "--include-config", help="Include configuration notebook"),
    fabric_workspace_repo_dir: Optional[Path] = typer.Option(None, "--workspace-dir", "-w", help="Fabric workspace repository directory")
):
    """Compile data profiling package notebooks and DDL scripts."""
    try:
        from ingen_fab.packages.data_profiling import compile_data_profiling_package
        from ingen_fab.python_libs.common.utils.path_utils import PathUtils
        
        # Use provided workspace dir or get from environment
        if fabric_workspace_repo_dir:
            workspace_dir = str(fabric_workspace_repo_dir)
        else:
            workspace_dir = PathUtils.get_workspace_repo_dir()
        
        typer.echo(f"🔧 Compiling data profiling package for {target_datastore}...")
        
        # Compile the package
        results = compile_data_profiling_package(
            fabric_workspace_repo_dir=workspace_dir,
            target_datastore=target_datastore,
            include_samples=include_samples,
            template_vars={"include_config": include_config}
        )
        
        if results["success"]:
            typer.echo(f"✅ Successfully compiled data profiling package!")
            
            if isinstance(results["notebook_file"], list):
                for notebook in results["notebook_file"]:
                    if notebook:
                        typer.echo(f"  📓 Notebook: {notebook}")
            else:
                if results["notebook_file"]:
                    typer.echo(f"  📓 Notebook: {results['notebook_file']}")
            
            if results.get("config_file"):
                if isinstance(results["config_file"], list):
                    for config in results["config_file"]:
                        if config:
                            typer.echo(f"  ⚙️  Config: {config}")
                else:
                    typer.echo(f"  ⚙️  Config: {results['config_file']}")
            
            typer.echo(f"  📄 DDL Scripts: {len(results['ddl_files'])} files")
            
        else:
            typer.echo(f"❌ Failed to compile data profiling package", err=True)
            for error in results.get("errors", []):
                typer.echo(f"  Error: {error}", err=True)
            raise typer.Exit(code=1)
            
    except Exception as e:
        typer.echo(f"❌ Error compiling package: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def generate_config(
    tables: Optional[List[str]] = typer.Option(None, "--table", "-t", help="Specific tables to configure"),
    profile_type: ProfileLevel = typer.Option(ProfileLevel.full, "--level", "-l", help="Default profiling level"),
    frequency: str = typer.Option("daily", "--frequency", "-f", help="Profiling frequency"),
    output_file: Optional[Path] = typer.Option(None, "--output", "-o", help="Output configuration file")
):
    """Generate data profiling configuration for tables."""
    try:
        from ingen_fab.python_libs.common import config_utils as cu
        from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
        import uuid
        from datetime import datetime
        
        # Get workspace configuration
        configs = cu.get_configs_as_object()
        
        # Initialize lakehouse utils (it will create/reuse spark session)
        config_lakehouse = lakehouse_utils(
            target_workspace_id=configs.config_workspace_id,
            target_lakehouse_id=configs.config_lakehouse_id
        )
        
        # Get spark session from lakehouse utils
        spark = config_lakehouse.spark
        
        # Get tables to configure
        if tables:
            tables_to_configure = tables
        else:
            # Get all tables from the lakehouse
            tables_to_configure = config_lakehouse.list_tables()
            typer.echo(f"Found {len(tables_to_configure)} tables in lakehouse")
        
        # Generate configuration records
        config_records = []
        for table_name in tables_to_configure:
                config_record = {
                    "config_id": str(uuid.uuid4()),
                    "table_name": table_name,
                    "schema_name": None,
                    "target_workspace_id": configs.config_workspace_id,  # Default to config workspace
                    "target_datastore_id": configs.config_lakehouse_id,  # Default to config lakehouse
                    "target_datastore_type": "lakehouse",  # Default to lakehouse
                    "profile_frequency": frequency,
                    "profile_type": profile_type.value,
                    "sample_size": None,
                    "columns_to_profile": None,
                    "quality_thresholds_json": json.dumps({
                        "completeness": 0.95,
                        "uniqueness": 0.99,
                        "validity": 0.98,
                        "consistency": 0.95
                    }),
                    "validation_rules_json": None,
                    "alert_enabled": False,
                    "alert_threshold": "0.90",
                    "alert_recipients": None,
                    "last_profile_date": None,
                    "last_profile_status": None,
                    "execution_group": 1,
                    "active_yn": "Y",
                    "created_date": datetime.now(),
                    "modified_date": None,
                    "created_by": "profile_config_generator",
                    "modified_by": None
                }
                config_records.append(config_record)
        
        # Save configuration
        if output_file:
            # Save to JSON file
            with open(output_file, 'w') as f:
                json.dump(config_records, f, indent=2, default=str)
            typer.echo(f"Configuration saved to: {output_file}")
        else:
            # Save to configuration table
            from pyspark.sql.types import (
                BooleanType, IntegerType, StringType, StructField, 
                StructType, TimestampType
            )
            
            config_schema = StructType([
                StructField("config_id", StringType(), False),
                StructField("table_name", StringType(), False),
                StructField("schema_name", StringType(), True),
                StructField("target_workspace_id", StringType(), True),
                StructField("target_datastore_id", StringType(), True),
                StructField("target_datastore_type", StringType(), True),
                StructField("profile_frequency", StringType(), False),
                StructField("profile_type", StringType(), False),
                StructField("sample_size", StringType(), True),
                StructField("columns_to_profile", StringType(), True),
                StructField("quality_thresholds_json", StringType(), True),
                StructField("validation_rules_json", StringType(), True),
                StructField("alert_enabled", BooleanType(), False),
                StructField("alert_threshold", StringType(), True),
                StructField("alert_recipients", StringType(), True),
                StructField("last_profile_date", TimestampType(), True),
                StructField("last_profile_status", StringType(), True),
                StructField("execution_group", IntegerType(), False),
                StructField("active_yn", StringType(), False),
                StructField("created_date", TimestampType(), False),
                StructField("modified_date", TimestampType(), True),
                StructField("created_by", StringType(), False),
                StructField("modified_by", StringType(), True)
            ])
            
            config_df = spark.createDataFrame(config_records, config_schema)
            config_lakehouse.write_to_table(
                df=config_df,
                table_name="config_data_profiling",
                mode="append"
            )
            typer.echo(f"Configuration saved to config_data_profiling table")
        
        typer.echo(f"✅ Generated configuration for {len(config_records)} tables")
            
    except Exception as e:
        typer.echo(f"❌ Error generating configuration: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()