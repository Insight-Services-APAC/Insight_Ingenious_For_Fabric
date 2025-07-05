
from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils


@pytest.fixture
def pipeline_utils(monkeypatch) -> PipelineUtils:
    mock_client = MagicMock()
    monkeypatch.setattr(
        "ingen_fab.python_libs.python.pipeline_utils.PipelineUtils._get_pipeline_client",
        lambda self: mock_client
    )
    pu = PipelineUtils()
    return pu


@pytest.mark.asyncio
async def test_trigger_pipeline_success(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 202
    mock_response.headers = {"Location": "/v1/jobs/instances/12345"}
    pipeline_utils.client.post.return_value = mock_response

    job_id = await pipeline_utils.trigger_pipeline(
        workspace_id="wsid",
        pipeline_id="pid",
        payload={"param": "value"}
    )
    assert job_id == "12345"
    pipeline_utils.client.post.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_pipeline_retries_on_transient_error(pipeline_utils):
    # First call: 500, Second call: 202
    mock_response1 = MagicMock(status_code=500, text="Server error", headers={})
    mock_response2 = MagicMock(status_code=202, headers={"Location": "/v1/jobs/instances/abcde"})
    pipeline_utils.client.post.side_effect = [mock_response1, mock_response2]

    job_id = await pipeline_utils.trigger_pipeline(
        workspace_id="wsid",
        pipeline_id="pid",
        payload={}
    )
    assert job_id == "abcde"
    assert pipeline_utils.client.post.call_count == 2


@pytest.mark.asyncio
async def test_trigger_pipeline_fails_on_client_error(pipeline_utils):
    mock_response = MagicMock(status_code=400, text="Bad request", headers={})
    pipeline_utils.client.post.return_value = mock_response

    with pytest.raises(Exception) as exc:
        await pipeline_utils.trigger_pipeline("wsid", "pid", {})
    assert "Failed to trigger pipeline" in str(exc.value)


@pytest.mark.asyncio
async def test_check_pipeline_status_success(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "Completed"}
    pipeline_utils.client.get.return_value = mock_response

    status, error = await pipeline_utils.check_pipeline(
        table_name="table1",
        workspace_id="wsid",
        pipeline_id="pid",
        job_id="jid"
    )
    assert status == "Completed"
    assert error == ""


@pytest.mark.asyncio
async def test_check_pipeline_status_failure_reason(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "Failed",
        "failureReason": {"message": "Some error", "errorCode": "RequestExecutionFailed"}
    }
    pipeline_utils.client.get.return_value = mock_response

    status, error = await pipeline_utils.check_pipeline(
        table_name="table1",
        workspace_id="wsid",
        pipeline_id="pid",
        job_id="jid"
    )
    # Should return 'Failed' and error message for this failure
    assert status == "Failed"
    assert "Some error" in error
