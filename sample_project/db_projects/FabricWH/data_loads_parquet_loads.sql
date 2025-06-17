

INSERT INTO
    [config].[parquet_loads] (
        [target_lakehouse_workspace_id],
        [target_lakehouse_name],
        [target_partition_columns],
        [target_sort_columns],
        [target_replace_where],
        
        [source_lakehouse_workspace_id],
        [source_lakehouse_name],
        [source_file_path],
        [source_file_name],    
        
        -- Allows link back to the Synapse Extracts table
        [synapse_connection_name],
        [synapse_source_schema_name],
        [synapse_source_table_name],
        [synapse_partition_clause],

        [execution_group],
        [active_yn]
    )
VALUES
    (
        @EDW_Lakehouse_Workspace_Id,
        @EDW_Lakehouse_Name,        
        '',
        '',
        '',

        @EDW_Lakehouse_Workspace_Id,
        @EDW_Lakehouse_Name,
        @Synapse_Export_Shortcut_Path_In_Onelake+'dbo_dim_customer',
        '', 

        'synapse_connection',
        'dbo',
        'dim_customer',
        '',
                
        1,
        'Y'
    ),
    (
        
        @EDW_Lakehouse_Workspace_Id,
        @EDW_Lakehouse_Name,        
        'year, month',
        'year, month',
        'WHERE year = @year AND month = @month',

        @EDW_Lakehouse_Workspace_Id,
        @EDW_Lakehouse_Name,
        @Synapse_Export_Shortcut_Path_In_Onelake+'dbo_dim_customer',
        '', 

        'synapse_connection',
        'dbo',
        'fact_transactions',
        'WHERE year = @year AND month = @month',
                
        1,
        'Y'
    );