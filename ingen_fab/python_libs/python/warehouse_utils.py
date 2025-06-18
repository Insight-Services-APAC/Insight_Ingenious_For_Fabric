import logging
import notebookutils  # type: ignore # noqa: F401


class warehouse_utils:
    def __init__(self, target_workspace_id, target_warehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id

    def get_connection(self):
        try:
            return notebookutils.data.connect_to_artifact(self.target_warehouse_id, self.target_workspace_id)
        except Exception as e:
            logging.error(f"Failed to connect to artifact: {e}")
            raise

    def execute_query(self, conn, query: str):
        try:
            logging.info(f"Executing query: {query}")
            result = conn.query(query)
            logging.info("Query executed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error executing query: {query}. Error: {e}")
            raise

    def check_if_table_exists(self, table_name):
        try:
            conn = self.get_connection()
            query = f"SELECT TOP 1 * FROM {table_name}"
            self.execute_query(conn, query)
            return True
        except Exception as e:
            logging.error(f"Error checking if table {table_name} exists: {e}")
            return False
        
    def write_to_warehouse_table(
        self,
        df,
        table_name: str,
        mode: str = "overwrite",
        options: dict = None
    ):
        try:
            conn = self.get_connection()
            pandas_df = df
        
            # Handle different write modes
            if mode == "overwrite":
                # Drop table if exists
                drop_query = f"DROP TABLE IF EXISTS {table_name}"
                self.execute_query(conn, drop_query)
                
                # Create table from dataframe using SELECT INTO syntax
                values = []
                for _, row in pandas_df.iterrows():
                    row_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
                    values.append(f"({row_values})")
                
                # Get column names from DataFrame
                column_names = ', '.join(pandas_df.columns)
                values_clause = ', '.join(values)
                
                create_query = f"SELECT * INTO {table_name} FROM (VALUES {values_clause}) AS v({column_names})"
                self.execute_query(conn, create_query)
            
            elif mode == "append":
                # Insert data into existing table
                for _, row in pandas_df.iterrows():
                    row_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
                    insert_query = f"INSERT INTO {table_name} VALUES ({row_values})"
                    self.execute_query(conn, insert_query)
            
            elif mode == "error" or mode == "errorifexists":
                # Check if table exists
                if self.check_if_table_exists(table_name):
                    raise ValueError(f"Table {table_name} already exists")
                # Create table from dataframe using SELECT INTO syntax
                values = []
                for _, row in pandas_df.iterrows():
                    row_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
                    values.append(f"({row_values})")
                
                # Get column names from DataFrame
                column_names = ', '.join(pandas_df.columns)
                values_clause = ', '.join(values)
                
                create_query = f"SELECT * INTO {table_name} FROM (VALUES {values_clause}) AS v({column_names})"
                self.execute_query(conn, create_query)
            
            elif mode == "ignore":
                # Only write if table doesn't exist
                if not self.check_if_table_exists(table_name):
                    values = []
                    for _, row in pandas_df.iterrows():
                        row_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
                        values.append(f"({row_values})")
                    
                    # Get column names from DataFrame
                    column_names = ', '.join(pandas_df.columns)
                    values_clause = ', '.join(values)
                    
                    create_query = f"SELECT * INTO {table_name} FROM (VALUES {values_clause}) AS v({column_names})"
                    self.execute_query(conn, create_query)
        except Exception as e:
            logging.error(f"Error writing to table {table_name} with mode {mode}: {e}")
            raise

    def drop_all_tables(self, table_prefix=None):
        try:
            conn = self.get_connection()
            query = f"SHOW TABLES LIKE '{table_prefix}%'" if table_prefix else "SHOW TABLES"
            tables = self.execute_query(conn, query)

            for table in tables:
                table_name = table["name"] if isinstance(table, dict) else table[0]
                try:
                    drop_query = f"DROP TABLE {table_name}"
                    self.execute_query(conn, drop_query)
                    logging.info(f"✔ Dropped table: {table_name}")
                except Exception as e:
                    logging.error(f"⚠ Error dropping table {table_name}: {e}")

            logging.info("✅ All eligible tables have been dropped.")
        except Exception as e:
            logging.error(f"Error dropping tables with prefix {table_prefix}: {e}")
            raise
