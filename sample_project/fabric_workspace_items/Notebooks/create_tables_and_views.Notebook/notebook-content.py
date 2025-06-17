# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a29c9d15-c24e-4779-8344-0c7c237b3990",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "50fbcab0-7d56-46f7-90f6-80ceb00ac86d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a29c9d15-c24e-4779-8344-0c7c237b3990"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Generate Spark SQL DDL

# MARKDOWN ********************

# Author     : Sanath More
# 
# Date       : 
# 
# Description:
# 1. Read table & view metadata from Excel
# 2. Map source types to Spark & T-SQL types
# 3. Generate DDL per object
# 4. Write scripts to LH/WH directories
# 5. Log execution status in warehouse


# MARKDOWN ********************

# ## Import libraries

# CELL ********************

# core libs & Fabric notebook utilities
import notebookutils                              # Fabric FS/runtime context helpers
import pandas as pd                               # DataFrame & Excel I/O
import os                                         # OS path operations
import uuid                                       # For generating execution_id

# Date/time & path helpers
from datetime import datetime                     # Timestamp for logging
from pathlib import Path                          # Path manipulation

# Type hints
from typing import List, Dict, Tuple, Optional, Union

# Display & diagramming (DEBUG only)
from IPython.display import Image, display         # For debug-mode architecture diagrams

# Logging
import logging

import textwrap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Define parameters

# PARAMETERS CELL ********************

SCHEMA_METADATA_DIR = "/lakehouse/default/Files/config/ddl/"
SCHEMA_METADATA_FILE = "metcash_metadata_extract_20250611.xlsx"
SCHEMA_METADATA_FILE_PATH = SCHEMA_METADATA_DIR + SCHEMA_METADATA_FILE

BASE_OUTPUT_DIR = "/lakehouse/default/Files/ddl_scripts/"
LH_DDL_OUTPUT_DIR = f"{BASE_OUTPUT_DIR}LH/{os.path.splitext(SCHEMA_METADATA_FILE)[0]}/" # add file name without extension to output dir
WH_DDL_OUTPUT_DIR = f"{BASE_OUTPUT_DIR}WH/{os.path.splitext(SCHEMA_METADATA_FILE)[0]}/"

PREFIX_SCHEMA = True

DEBUG = True # for production deployments, set to False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

SCHEMA_METADATA_DIR = "/lakehouse/default/Files/config/ddl/"
SCHEMA_METADATA_FILE = "metcash_metadata_extract_20250611.xlsx"
EXCEL_PATH = SCHEMA_METADATA_DIR + SCHEMA_METADATA_FILE
VIEW_SHEET = "View_DataDictionary"
VIEW_SCHEMA_COL = "View-Schema"
VIEW_NAME_COL = "View_name"
TABLE_SHEET = "Table_DataDictionary"
TABLE_SCHEMA_COL = "Table Schema"
TABLE_NAME_COL = "Table_Name"
PREFIX_SCHEMA = True
DEBUG = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

SHEET_SPECS = [
    ("view",  VIEW_SHEET,  VIEW_SCHEMA_COL,  VIEW_NAME_COL),
    ("table", TABLE_SHEET, TABLE_SCHEMA_COL, TABLE_NAME_COL),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Generate diagram

# CELL ********************

if DEBUG == True:
    %pip install diagrams cairosvg

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if DEBUG == True:
    import cairosvg
    from IPython.display import (
        display,
        Image
    )
    from diagrams import Diagram, Edge, Cluster
    from diagrams.azure.analytics import SynapseAnalytics
    from diagrams.azure.storage import DataLakeStorage
    from diagrams.azure.compute import FunctionApps
    from diagrams.azure.database import SQLDatabases
    from diagrams.custom import Custom

    class FabricDiagramGenerator:

        def __init__(self, diagram_title: str, filename: str = "architecture_diagram"):
            self.diagram_title = diagram_title
            self.filename = filename
            self.fabric_icons_base_url = "https://raw.githubusercontent.com/FabricTools/fabric-icons/refs/heads/main/node_modules/%40fabric-msft/svg-icons/dist/svg"
            self.downloaded_icons = {}
            
        def download_fabric_icons(self, icon_names: List[str], size: int = 64) -> Dict[str, str]:
            for icon_name in icon_names:
                icon_svg = f"{icon_name}_{size}_item.svg"
                url = f"{self.fabric_icons_base_url}/{icon_svg}"
                icon_png = f"{icon_name}_{size}_item.png"
                
                try:
                    cairosvg.svg2png(url=url, write_to=icon_png, dpi=1000)
                    self.downloaded_icons[icon_name] = icon_png
                except Exception as e:
                    print(f"Could not download {icon_name} icon: {e}")
                    
            return self.downloaded_icons
        
        def create_diagram(self, 
                        clusters: Dict[str, Dict[str, any]], 
                        connections: List[Tuple[str, str, str]],
                        show: bool = False,
                        outformat: str = "png"):

            with Diagram(self.diagram_title, show=show, outformat=outformat, filename=self.filename):
                components = {}
                
                # create clusters recursively
                def create_cluster_components(cluster_data, parent_cluster=None):
                    for cluster_name, cluster_info in cluster_data.items():
                        if parent_cluster:
                            # create nested cluster
                            with Cluster(cluster_name):
                                add_components(cluster_info)
                        else:
                            # create top-level cluster
                            with Cluster(cluster_name):
                                add_components(cluster_info)
                
                def add_components(cluster_info):
                    # add components to current cluster
                    if "components" in cluster_info:
                        for comp_id, (display_name, icon_type, icon_file) in cluster_info["components"].items():
                            if icon_type == "custom":
                                components[comp_id] = Custom(display_name, icon_file)
                            elif icon_type == "synapse":
                                components[comp_id] = SynapseAnalytics(display_name)
                            elif icon_type == "storage":
                                components[comp_id] = DataLakeStorage(display_name)
                            elif icon_type == "function":
                                components[comp_id] = FunctionApps(display_name)
                            elif icon_type == "sql":
                                components[comp_id] = SQLDatabases(display_name)
                            # Add more icon types as needed
                    
                    # handle subclusters
                    if "subclusters" in cluster_info:
                        create_cluster_components(cluster_info["subclusters"], parent_cluster=True)
                
                # create all clusters and components
                create_cluster_components(clusters)
                
                # create connections
                for source_id, target_id, label in connections:
                    if source_id in components and target_id in components:
                        components[source_id] >> Edge(label=label) >> components[target_id]

    def generate_fabric_architecture_diagram(
        title: str,
        fabric_icons: List[str],
        clusters_config: Dict[str, Dict[str, any]],
        connections_config: List[Tuple[str, str, str]],
        filename: str = "architecture"
    ):
        generator = FabricDiagramGenerator(title, filename)
        
        generator.download_fabric_icons(fabric_icons)
        
        generator.create_diagram(clusters_config, connections_config)
        
        return f"{filename}.png"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if DEBUG == True:

    fabric_icons = ["notebook", "lakehouse", "data_warehouse"]

    clusters = {
        "Fabric": {
            "components": {
                "notebook": ("create_tables_and_views", "custom", "notebook_64_item.png")
            },
            "subclusters": {
                "OneLake": {
                    "components": {
                        "config_file": (
                            "Metcash_sample.xlsx", 
                            "custom", 
                            "lakehouse_64_item.png"
                        ),
                        "ddl_output": (
                            "ddl_scripts/", 
                            "custom", 
                            "lakehouse_64_item.png"
                        )
                    }
                },
                "Configuration Warehouse": {
                    "components": {
                        "log_wh": (
                            "[WH].[log].[ddl_generation_log]", 
                            "custom", 
                            "data_warehouse_64_item.png"
                        )
                    }
                }
            }
        }
    }

    connections = [
        ("config_file", "notebook",     "(1) Load metadata from Lakehouse"),
        ("notebook",    "ddl_output",   "(2) Generate and write DDL SQL files to Lakehouse"),
        ("notebook",    "log_wh",       "(3) Log execution status into WH")
    ]

    display(
        Image(
            generate_fabric_architecture_diagram(
                title="Generate Spark SQL DDL",
                fabric_icons=fabric_icons,
                clusters_config=clusters,
                connections_config=connections,
                filename="create_tables_and_views_arch_diagram"
            )
        )
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Configure logging

# CELL ********************

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def insert_notebook_log(
    execution_id: str,
    notebook_name: str,
    source_file: str
) -> datetime:

    conn = notebookutils.data.connect_to_artifact("WH")
    start_time = datetime.now()

    insert_sql = """
    INSERT INTO [log].ddl_generation_log (
        execution_id,
        notebook_name,
        source_file,
        start_time,
        status
    )
    VALUES (?, ?, ?, ?, 'In Progress')
    """
    conn.execute(insert_sql, (
        execution_id,
        notebook_name,
        source_file,
        start_time
    ))

    return start_time


def update_notebook_log(
    execution_id: str,
    status: str,
    error_message: str,
):

    conn = notebookutils.data.connect_to_artifact("WH")

    update_sql = """
    UPDATE [log].ddl_generation_log
    SET
        end_time      = ?,
        status        = ?,
        error_message = ?
    WHERE
        execution_id = ?
    """
    conn.execute(update_sql, (
        datetime.now(),
        status,
        error_message,
        execution_id
    ))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Define functions

# MARKDOWN ********************

# ### map_to_spark_type

# CELL ********************

TYPE_MAPPINGS_LH = {
    "string_types": {
        "types": ["varchar", "nvarchar", "char", "nchar", "text", "string"],
        "spark_type": "STRING"
    },
    "double_types": {
        "types": ["float", "double", "real"],
        "spark_type": "DOUBLE"
    },
    "bigint_types": {
        "types": ["bigint", "long"],
        "spark_type": "BIGINT"
    },
    "int_types": {
        "types": ["int", "integer", "smallint", "tinyint"],
        "spark_type": "INT"
    },
    "decimal_types": {
        "types": ["decimal", "numeric", "number"],
        "spark_type": "DECIMAL",
        "parameterised": True
    },
    "date_types": {
        "types": ["date"],
        "spark_type": "DATE"
    },
    "timestamp_types": {
        "types": ["datetime", "timestamp", "timestamptz", "datetime2"],
        "spark_type": "TIMESTAMP"
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def map_to_spark_type(
    src_type: str, 
    max_len: Optional[Union[int, float]], 
    num_prec: Optional[Union[int, float]], 
    num_scale: Optional[Union[int, float]]
) -> str:
    """
    Maps a source metadata type to a Spark SQL type.

    Parameters
    ----------
    src_type : str
        Raw type name from metadata (e.g. "decimal", "varchar").
    max_len : int | float | None
        Maximum character length (if applicable).
    num_prec : int | float | None
        Numeric precision (if DECIMAL).
    num_scale : int | float | None
        Numeric scale (if DECIMAL).

    Returns
    -------
    str
        A Spark type string (e.g. "DECIMAL(10,2)" or "STRING").
    """
    src_type = str(src_type).strip().lower()

    for mapping in TYPE_MAPPINGS_LH.values():
        if src_type in mapping["types"]:
            spark_type = mapping["spark_type"]

            if mapping.get("parameterised") and spark_type == "DECIMAL":
                if pd.notna(num_prec) and pd.notna(num_scale):
                    return f"DECIMAL({int(num_prec)},{int(num_scale)})"
            
            return spark_type
    
    print(f"Unhandled data type encountered: {src_type}, using as-is.")
    return str(src_type).upper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### map_to_tsql_type

# CELL ********************

TYPE_MAPPINGS_TSQL = {
    # Fixed-length character strings; must have a length, default to 1 if missing
    "fixed_char_types": {
        "types": ["char", "nchar"],
        "tsql_type": "char",
        "parameterised": True
    },
    # Variable-length character strings; default to MAX if missing
    "varchar_types": {
        "types": ["varchar", "nvarchar"],
        "tsql_type": "VARCHAR",
        "parameterised": True
    },
    # TEXT is a stand-alone non-parameterised type
    "text_type": {
        "types": ["text"],
        "tsql_type": "TEXT",
        "parameterised": False
    },

    # Exact numerics
    "decimal_types": {
        "types": ["decimal", "numeric", "number"],
        "tsql_type": "DECIMAL",
        "parameterised": True
    },

    # Approximate numerics
    "float_types": {
        "types": ["float", "real"],
        "tsql_type": "FLOAT",
        "parameterised": False
    },

    # Integers
    "bigint_types": {
        "types": ["bigint", "long"],
        "tsql_type": "BIGINT",
        "parameterised": False
    },
    "int_types": {
        "types": ["int", "integer"],
        "tsql_type": "INT",
        "parameterised": False
    },
    "smallint_type": {
        "types": ["smallint"],
        "tsql_type": "SMALLINT",
        "parameterised": False
    },
    "tinyint_type": {
        "types": ["tinyint"],
        "tsql_type": "TINYINT",
        "parameterised": False
    },

    # Date & time
    "date_type": {
        "types": ["date"],
        "tsql_type": "DATE",
        "parameterised": False
    },
    "time_type": {
        "types": ["time"],
        "tsql_type": "TIME",
        "parameterised": True   # allows TIME(3), etc.
    },
    "datetime2_type": {
        "types": ["datetime", "datetime2", "timestamp"],
        "tsql_type": "DATETIME2",
        "parameterised": True   # allows DATETIME2(x), etc.
    }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# reverse lookup
TSQL_MAPPING_LOOKUP = {
    t: mapping
    for mapping in TYPE_MAPPINGS_TSQL.values()
    for t in mapping["types"]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def map_to_tsql_type(
    src_type: str, 
    max_len: Optional[Union[int, float]], 
    num_prec: Optional[Union[int, float]], 
    num_scale: Optional[Union[int, float]]
) -> str:
    """
    Converts a source type name to its T-SQL equivalent, including any needed parameters.

    - Unrecognised types are returned upper-cased.
    - DECIMAL → DECIMAL(p,s) or DECIMAL(p) if scale is missing.
    - TIME/DATETIME2 → TYPE(s) or TYPE(6) by default.
    - CHAR/NCHAR → TYPE(n) with default n=1.
    - VARCHAR/NVARCHAR → TYPE(n) or TYPE(MAX) if length is unspecified.
    - Other parameterised types use max_len if provided.

    Parameters
    ----------
    src_type : str
        Source type name (case-insensitive).
    max_len : int|float|None
        Length for char/varchar types or other parameterised types.
    num_prec : int|float|None
        Precision for DECIMAL.
    num_scale : int|float|None
        Scale for DECIMAL or fractional precision for temporal types.

    Returns
    -------
    str
        The mapped T-SQL type, with parameters if applicable.
    """

    src = src_type.strip().lower()
    mapping = TSQL_MAPPING_LOOKUP.get(src)
    
    if not mapping:
        print(f"Unhandled data type: {src_type}, passing through as-is.")
        return src.upper()

    tsql = mapping["tsql_type"] or src.upper()
    if not mapping["parameterised"]:
        return tsql

    # DECIMAL(p,s)
    if tsql == "DECIMAL":
        if pd.notna(num_prec):
            if pd.notna(num_scale):
                return f"{tsql}({int(num_prec)},{int(num_scale)})"
            return f"{tsql}({int(num_prec)})"
        return tsql

    # TIME(p) or DATETIME2(p)
    if tsql in ("TIME", "DATETIME2"):
        if pd.notna(num_scale) and num_scale <= 6:
            return f"{tsql}({int(num_scale)})"
        return f"{tsql}(6)"

    # CHAR/NCHAR: default length 1
    if tsql in ("CHAR", "NCHAR"):
        length = int(max_len) if pd.notna(max_len) else 1
        return f"{tsql}({length})"

    # VARCHAR/NVARCHAR: default to MAX
    if tsql in ("VARCHAR", "NVARCHAR"):
        if pd.notna(max_len):
            return f"{tsql}({int(max_len)})"
        return f"{tsql}(MAX)"

    # catch-all
    if pd.notna(max_len):
        return f"{tsql}({int(max_len)})"
    
    return tsql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### _load_and_prep_df

# CELL ********************

def _load_and_prep_df(
    excel_path: str,
    sheet_name: str
) -> pd.DataFrame:
    """
    Loads one sheet of the Excel metadata file and uppercase the 'IsNullable' column.

    Parameters
    ----------
    excel_path : str
        Full path to the metadata Excel file.
    sheet_name : str
        Name of the sheet to read (e.g. 'Table_DataDictionary').

    Returns
    -------
    pd.DataFrame
        Prepared DataFrame with uppercase 'IsNullable'.
    """
    try:
        with pd.ExcelFile(excel_path, engine="openpyxl") as xls:
            if sheet_name not in xls.sheet_names:
                logger.error(f"Sheet '{sheet_name}' not found in {excel_path}; available: {xls.sheet_names}")
                raise ValueError(f"Sheet '{sheet_name}' not found in {excel_path}; available: {xls.sheet_names}")
            df = pd.read_excel(xls, sheet_name=sheet_name)
    except FileNotFoundError:
        logger.error(f"Excel file not found: {excel_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading sheet '{sheet_name}': {e}")
        raise

    # sanity check cols
    required = ["ColumnName","Columntypes","Maxlength","NumericPrecision","NumericScale","IsNullable"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.error(f"Missing {missing} columns in {sheet_name} of {excel_path}")
        raise ValueError

    df["IsNullable"] = (
        df["IsNullable"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### build_column_blocks

# CELL ********************

def build_column_blocks(
    df_subset: pd.DataFrame,
) -> Tuple[str, str]:
    """
    Generates comma-separated column definition blocks for Lakehouse (Spark) and Warehouse (T-SQL).

    Iterates over each row in `df_subset`, maps source types to Spark and T-SQL types,
    applies nullability, and returns two newline-joined strings:
      1. Lakehouse columns with backtick-quoted names and Spark types.
      2. Warehouse columns with bracket-quoted names and T-SQL types.

    Parameters
    ----------
    df_subset : pandas.DataFrame
        Subset of metadata containing these columns:
          - "ColumnName": target column name
          - "Columntypes": source data type
          - "Maxlength": max character length (optional)
          - "NumericPrecision": decimal precision (optional)
          - "NumericScale": decimal scale or temporal precision (optional)
          - "IsNullable": whether the column is nullable ("YES"/"NO")

    Returns
    -------
    Tuple[str, str]
        A tuple of two strings:
        - Spark SQL column definitions (Lakehouse).
        - T-SQL column definitions (Warehouse).
    """

    lh_cols, wh_cols = [], []
    for _, row in df_subset.iterrows():
        # parse metadata cols
        col       = row.get("ColumnName")
        src       = row["Columntypes"]
        max_len   = row.get("Maxlength", None)
        prec      = row.get("NumericPrecision", None)
        scale     = row.get("NumericScale", None)
        nullable  = str(row["IsNullable"]).strip().upper() != "NO"

        # map types
        spark_t = map_to_spark_type(src, max_len, prec, scale)
        tsql_t  = map_to_tsql_type(src, max_len, prec, scale)

        # Lakehouse (defaults NULL)
        lh_cols.append(
            f"    `{col}` {spark_t}" + ("" if nullable else " NOT NULL")
        )
        # Warehouse (explicit NULL/NOT NULL)
        wh_cols.append(
            f"    [{col}] {tsql_t}" + (" NULL" if nullable else " NOT NULL")
        )

    return ",\n".join(lh_cols), ",\n".join(wh_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### apply_lakehouse_optimisations

# CELL ********************

def apply_lakehouse_optimisations(
    lakehouse_ddl: str
) -> str:
    optimisations = textwrap.dedent("""
        USING PARQUET
        TBLPROPERTIES(
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'delta.parquet.vorder.default'     = 'true'
        )
    """).strip()

    return f"{lakehouse_ddl}\n{optimisations}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### generate_ddl_from_excel

# CELL ********************

def generate_ddl_from_excel(
    excel_path: str,
    prefix_schema: bool,
    sheet_specs: list[tuple[str,str,str,str]] = SHEET_SPECS
) -> pd.DataFrame:
    """
    Reads metadata from an Excel workbook and produces DDL statements for both Lakehouse and Warehouse.

    This function loads the “View_DataDictionary” and “Table_DataDictionary” sheets from the Excel file
    at `excel_path`, groups rows by schema and object name, builds column definitions via
    `build_column_blocks`, and then assembles `CREATE TABLE` statements for each view and table.
    Object naming can be either schema-prefixed or schema-qualified based on `prefix_schema`.

    Parameters
    ----------
    excel_path : str
        Filesystem path to the Excel file containing the metadata sheets:
        - “View_DataDictionary” for views
        - “Table_DataDictionary” for tables
    prefix_schema : bool
        If True, object names are prefixed as `<schema>_<name>`.
        If False, they use
        `<schema>.<name>` (with appropriate quoting for Lakehouse and Warehouse).

    Returns
    -------
    pandas.DataFrame
        A DataFrame with one row per object, containing:
        - object_name: the normalised name used in DDL
        - create_table_ddl_lh: the Lakehouse (Spark) CREATE TABLE statement
        - create_table_ddl_wh: the T-SQL CREATE TABLE statement for the Warehouse
        - object_type: either "view" or "table"
    """
    results: List[Dict] = []

    for obj_type, sheet_name, schema_col, name_col in sheet_specs:
        sub = _load_and_prep_df(excel_path, sheet_name).dropna(subset=[schema_col, name_col])

        for (schema, name), group in sub.groupby([schema_col, name_col], sort=False):
            cols_lh, cols_wh = build_column_blocks(group)

            if prefix_schema:
                object_name = f"{schema}_{name}"
                lh_name       = f"`{object_name}`"
                wh_name       = f"[{object_name}]"
            else:
                object_name= f"{schema}.{name}"
                lh_name    = f"`{schema}`.`{name}`"
                wh_name    = f"[{schema}].[{name}]"

            results.append({
                "object_name":          object_name,
                "create_table_ddl_lh":  f"CREATE TABLE {lh_name} (\n{cols_lh}\n)",
                "create_table_ddl_wh":  f"CREATE TABLE {wh_name} (\n{cols_wh}\n)",
                "object_type":          obj_type
            })

    return pd.DataFrame(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Generate DDLs, output to Lakehouse

# MARKDOWN ********************

# ### Begin logging

# CELL ********************

execution_id = str(uuid.uuid4())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Log notebook execution start
run_start = insert_notebook_log(
    execution_id = execution_id,
    notebook_name = notebookutils.runtime.context['currentNotebookName'],
    source_file = SCHEMA_METADATA_FILE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Execute logic

# CELL ********************

"""
Generate DDL files for Lakehouse and Warehouse from table/view metadata,
write each CREATE TABLE statement to a separate .sql file,
log success or failure for this notebook run.
"""
errors = []
try:
    # Load DataFrame of DDL statements from Excel metadata
    ddls_df = generate_ddl_from_excel(EXCEL_PATH, PREFIX_SCHEMA)

    # Iterate over each object (table or view) to write out SQL files
    for _, row in ddls_df.iterrows():
        
        object_name = row["object_name"]
        obj_type = row.get("object_type")

        # Clean object name for filename: remove quotes/dots and replace with underscore
        object_name_clean = object_name.replace("`", "").replace(".", "_")
        object_filename = f"{object_name_clean}.sql"

        # --- Write Lakehouse DDL ---
        # Determine output directory for this object type under Lakehouse path
        try:
            lh_dir = Path(LH_DDL_OUTPUT_DIR) / obj_type # create subdir for obj type (table/view)
            lh_dir.mkdir(parents = True, exist_ok= True)
            lh_path = lh_dir / object_filename

            ddl_lh = row['create_table_ddl_lh'] # Lakehouse CREATE TABLE statement
            ddl_lh = apply_lakehouse_optimisations(ddl_lh)
            with lh_path.open("w", encoding="utf-8") as f_lh:
                f_lh.write(ddl_lh + "\n")
        except Exception as e:
            logger.error(f"Failed writing Lakehouse DDL {lh_path}: {str(e)}")
            errors.append((object_name, str(e)))
        # --- Write Warehouse DDL ---
        # Determine output directory for this object type under Warehouse path
        try:
            wh_dir = Path(WH_DDL_OUTPUT_DIR) / obj_type
            wh_dir.mkdir(parents=True, exist_ok=True)
            wh_path = wh_dir / object_filename

            ddl_wh = row['create_table_ddl_wh'] # Warehouse CREATE TABLE statement
            with wh_path.open("w", encoding="utf-8") as f_wh:
                f_wh.write(ddl_wh + "\n")
        except Exception as e:
            logger.error(f"Failed writing Warehouse DDL {wh_path}: {str(e)}")

    # Update notebook log to mark successful completion
    update_notebook_log(
        execution_id = execution_id,
        status = "Succeeded",
        error_message = None
    )

except Exception as e:
    # On any error, capture exception message and update notebook log as failed
    update_notebook_log(
        execution_id = execution_id,
        status = "Failed",
        error_message = str(e)
    )
    notebookutils.notebook.exit(f"Failed: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
