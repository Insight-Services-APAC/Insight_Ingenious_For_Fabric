# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Execute Fabric Warehouse SQL with Pipeline

# CELL ********************

%pip install -U semantic-link

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import asyncio
import requests
import time
import sempy.fabric as fabric

from requests.exceptions import HTTPError

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Define constants

# CELL ********************

PIPELINE_ID = "91c0cc27-c4f6-42e1-8091-2fdfa4c6f2a7"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

async def _trigger_pipeline(client, workspace_id, payload, pipeline_id):
    trigger_url = f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    response = client.post(trigger_url, json=payload)
    if response.status_code != 202:
        raise Exception(f"❌ Failed to trigger pipeline: {response.status_code}\n{response.text}")

    response_location = response.headers['Location']
    # get last part of url from response_location 
    job_id = response_location.rstrip("/").split("/")[-1]    
    print(f"✅ Pipeline triggered. Pipeline: {job_id}")
    return job_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

async def _check_pipeline(client, workspace_id, pipeline_id, job_id):
    status_url = f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"

    # Perform the GET request and check for HTTP-level errors
    response = client.get(status_url)

    # try:
    #     response.raise_for_status()
    # except HTTPError as error:
    #     code = error.response.status_code
    #     return "Failed"

    data = response.json()
    status = data.get("status")

    if status == "Failed" and "failureReason" in data:
        fr = data["failureReason"]
        msg = fr.get("message", "")
        if fr.get("errorCode") == "RequestExecutionFailed" and "NotFound" in msg:
            print(f"[INFO] Transient check-failure, retrying: {fr.get('message')!r}")
            return None
        return status, msg

    print(f"[STATUS] Pipeline job_id={job_id} current status: {status}")

    return status, ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

async def run_pipeline_with_sql(
    script_content: str,
    pipeline_id: str  = PIPELINE_ID,
    poll_interval: int = 10
) -> str:
    """
    Trigger an MS Fabric pipeline by passing `script_content` as the "script_content" parameter value.

    Returns:
        The final pipeline status (one of: Completed, Failed, Cancelled, Deduped).
    """
    client = fabric.FabricRestClient()
    workspace_id = fabric.get_workspace_id()
    parameter_name = "script_content"

    payload = {
        "executionData": {
            "parameters": {
                parameter_name: script_content
            }
        }
    }

    job_id = await _trigger_pipeline(
        client=client,
        workspace_id=workspace_id,
        pipeline_id=pipeline_id,
        payload=payload
    )

    if not job_id:
        return "Failed"

    terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
    current_status = None

    while True:
        current_status, message = await _check_pipeline(
            client=client,
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            job_id=job_id
        )
        if current_status in terminal_states:
            return current_status, message
        await asyncio.sleep(poll_interval)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
