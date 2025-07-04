

class SynapseExtractUtils: 
    """Utility class for Synapse data extraction operations."""

    def __init__(self, datasource_name: str, datasource_location: str, workspace_id: str, lakehouse_id: str):
        """
        Initialise the Synapse extract utilities.
        
        Args:
            datasource_name: Name of the Synapse datasource
            datasource_location: Location of the Synapse datasource
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
        """
        self.datasource_name = datasource_name
        self.datasource_location = datasource_location
        base_lh_table_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"
        self.log_table_uri = f"{base_lh_table_path}/log_synapse_extracts"
        self.config_table_uri = f"{base_lh_table_path}/config_synapse_extracts"

    def get_extract_sql_template(self) -> str:
        """Get the SQL template for CETAS extraction."""
        return f"""
    IF NOT EXISTS (
        SELECT
            *
        FROM
            sys.external_file_formats 
        WHERE
            name = 'parquet'
    )
    CREATE EXTERNAL FILE FORMAT [parquet] WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

    IF NOT EXISTS (
        SELECT
            *
        FROM
            sys.external_data_sources
        WHERE
            name = '{self.datasource_name}'
    )
    CREATE EXTERNAL DATA SOURCE [{self.datasource_name.strip()}] WITH (
        LOCATION = '{self.datasource_location.strip()}',
        TYPE = HADOOP
    );

    IF OBJECT_ID('exports.@ExternalTableName', 'U') IS NOT NULL
    DROP EXTERNAL TABLE exports.@ExternalTableName;

    CREATE EXTERNAL TABLE exports.@ExternalTableName WITH (
        LOCATION = '@LocationPath', 
        DATA_SOURCE = [{self.datasource_name.strip()}],
        FILE_FORMAT = parquet 
    ) AS
    SELECT
        *
    FROM
        [@TableSchema].[@TableName]
    @PartitionClause;
    """

    async def insert_log_record(
        self,
        execution_id: str,
        cfg_synapse_connection_name: str,
        source_schema_name: str,
        source_table_name: str,
        extract_file_name: str,
        partition_clause: str,
        status: str,
        error_messages: str,
        start_date: Optional[int] = None, 
        finish_date: Optional[int] = None, 
        update_date: Optional[int] = None,
        output_path: Optional[str] = None,
        master_execution_id: Optional[str] = None
    ) -> None:
        """
        Insert a log record into the log_synapse_extracts table with retry mechanism.
        
        Args:
            execution_id: Unique ID for this execution
            cfg_synapse_connection_name: Name of the Synapse connection
            source_schema_name: Schema containing the source table
            source_table_name: Name of the source table
            extract_file_name: Name of the extract file
            partition_clause: SQL clause for partitioning
            status: Current status of the extraction
            error_messages: Any error messages
            start_date: Start timestamp
            finish_date: Finish timestamp
            update_date: Update timestamp
            output_path: Path to the output file
            master_execution_id: ID that links related executions together
        """
        # Default the dates only if they are not provided
        start_ts = start_date if start_date is not None else timestamp_now()
        finish_ts = finish_date if finish_date is not None else timestamp_now()
        update_ts = update_date if update_date is not None else timestamp_now()

        # Step 1: Prepare schema
        schema = pa.schema([
            ("execution_id", pa.string()),
            ("cfg_synapse_connection_name", pa.string()),
            ("source_schema_name", pa.string()),
            ("source_table_name", pa.string()),
            ("extract_file_name", pa.string()),
            ("partition_clause", pa.string()),
            ("status", pa.string()),
            ("error_messages", pa.string()),
            ("start_date", pa.int64()),
            ("finish_date", pa.int64()),
            ("update_date", pa.int64()),
            ("output_path", pa.string()),
            ("master_execution_id", pa.string())
        ])

        # Step 2: Create DataFrame
        df = pd.DataFrame([{
            "execution_id": execution_id,
            "cfg_synapse_connection_name": cfg_synapse_connection_name,
            "source_schema_name": source_schema_name,
            "source_table_name": source_table_name,
            "extract_file_name": extract_file_name,
            "partition_clause": partition_clause,
            "status": status,
            "error_messages": error_messages,
            "start_date": start_ts,
            "finish_date": finish_ts,
            "update_date": update_ts,
            "output_path": output_path,
            "master_execution_id": master_execution_id
        }])

        # Step 3: Convert to Arrow Table
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Add a small random delay before writing to reduce contention with parallel writes
        await random_delay_before_logging()

        # Step 4: Write to Delta (append) with retry mechanism
        max_retries = 5
        retry_delay_base = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                write_deltalake(self.log_table_uri, table, mode="append", schema=schema)
                # If successful, break out of retry loop
                return
            except Exception as e:
                error_msg = str(e)
                # Check if it's a timeout error
                if "OperationTimedOut" in error_msg or "timeout" in error_msg.lower() or attempt < max_retries - 1:
                    # Calculate exponential backoff with jitter
                    delay = retry_delay_base * (2 ** attempt) * (0.5 + np.random.random())
                    print(f"Delta write attempt {attempt+1}/{max_retries} failed for {source_schema_name}.{source_table_name} with error: {error_msg[:100]}...")
                    print(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    # On last attempt, or if not a timeout error, re-raise
                    print(f"Failed to write log record for {source_schema_name}.{source_table_name} after {attempt+1} attempts: {error_msg}")
                    raise