# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

async def trigger_pipeline(client, workspace_id, pipeline_id, payload):
    trigger_url = f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    response = client.post(trigger_url, json=payload)
    if response.status_code != 202:
        raise Exception(f"❌ Failed to trigger pipeline: {response.status_code}\n{response.text}")

    response_location = response.headers['Location']
    # get last part of url from response_location 
    job_id = response_location.rstrip("/").split("/")[-1]    
    print(f"✅ Pipeline triggered. Pipeline: {job_id}")
    return job_id


async def check_pipeline(client, progress, task, workspace_id, pipeline_id, job_id):
    status_url = f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
    terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
    
    job_status_response = client.get(status_url)
    job_status_data = job_status_response.json()
    status = job_status_data.get("status")
   
    #if status == "Completed":
    #    progress.update(task, description=f"Job {job_id}: ✅{status}")        
    #elif status == "Failed":
    #    progress.update(task, description=f"Job {job_id}: ❌{status}")        
    #else:
    #    progress.update(task, description=f"Job {job_id}: ⚠️{status}")        
    
    return status


async def insert_log_record(
            synapse_connection_name,
            source_schema_name,
            source_table_name,
            extract_file_name,
            partition_clause,
            status,
            error_messages
    ):
    from datetime import datetime

    # Step 1: Connect to the Warehouse artifact
    conn = notebookutils.data.connect_to_artifact("WH")

    # Step 2: Define the SQL INSERT query (parameterized)
    insert_sql = """
    INSERT INTO log.synapse_extracts (
        synapse_connection_name,
        source_schema_name,
        source_table_name,
        extract_file_name,
        partition_clause,
        status,
        error_messages,
        update_date
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Step 3: Provide the values to insert
    values = (
        synapse_connection_name,
        source_schema_name,
        source_table_name,
        extract_file_name,
        partition_clause,
        status,
        error_messages,
        datetime.now()            # update_date
    )

    # Step 4: Execute the insert
    conn.execute(insert_sql, values)
    #print("✅ Record inserted successfully.")

async def update_log_record(
            synapse_connection_name,
            source_schema_name,
            source_table_name,
            extract_file_name,
            partition_clause,
            status,
            error_messages
    ):
    from datetime import datetime

    # Step 1: Connect to the Warehouse artifact
    conn = notebookutils.data.connect_to_artifact("WH")

    # Step 2: Define the SQL INSERT query (parameterized)
    update_sql = """
    UPDATE log.synapse_extracts 
    SET extract_file_name = ?,
        partition_clause = ?,
        status = ?,
        error_messages = ?,
        update_date = ?
    WHERE synapse_connection_name = ? AND source_schema_name = ? AND source_table_name = ?;
    """

    # Step 3: Provide the values to insert
    values = (        
        extract_file_name,
        partition_clause,
        status,
        error_messages,
        datetime.now(),
        synapse_connection_name,
        source_schema_name,
        source_table_name
    )

    # Step 4: Execute the insert
    conn.execute(update_sql, values)
    #print("✅ Record inserted successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
