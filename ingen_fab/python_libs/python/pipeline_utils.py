import asyncio
import importlib.util
import logging
import time
import uuid
from typing import Any, Dict, Optional

import numpy as np
import requests

# Import error categorization
from ingen_fab.python_libs.python.error_categorization import (
    error_categorizer,
)

logger = logging.getLogger(__name__)

# Constants for polling that match the original file
POLL_INTERVAL = 30
FAST_RETRY_SEC = 3
TRANSIENT_HTTP = {429, 500, 503, 504, 408}

# Global mock client instance for sharing job state across instances
_mock_client_instance = None


class MockPipelineClient:
    """Mock client for local testing when fabric_environment is 'local'."""

    def __init__(self):
        self.job_statuses = {}  # Store job statuses for tracking

    def post(self, endpoint: str, json: dict) -> "MockResponse":
        """Mock POST request for pipeline triggering."""
        if "jobs/instances" in endpoint:
            # Generate a mock job ID
            job_id = str(uuid.uuid4())
            # Store initial status
            self.job_statuses[job_id] = "Running"

            logger.info(f"🧪 Mock pipeline triggered - Job ID: {job_id}")

            # Mock successful response
            return MockResponse(
                status_code=202,
                headers={"Location": f"/v1/jobs/instances/{job_id}"},
                json_data={"jobId": job_id},
            )

        return MockResponse(status_code=404, json_data={"error": "Not found"})

    def get(self, endpoint: str) -> "MockResponse":
        """Mock GET request for pipeline status checking."""
        if "jobs/instances" in endpoint:
            # Extract job ID from endpoint
            job_id = endpoint.split("/")[-1]

            if job_id in self.job_statuses:
                current_status = self.job_statuses[job_id]

                # Simulate status progression for testing
                if current_status == "Running":
                    # 70% chance to complete, 20% to keep running, 10% to fail
                    rand = np.random.random()
                    if rand < 0.7:
                        self.job_statuses[job_id] = "Completed"
                        status = "Completed"
                    elif rand < 0.9:
                        status = "Running"
                    else:
                        self.job_statuses[job_id] = "Failed"
                        status = "Failed"
                else:
                    status = current_status

                logger.debug(
                    f"🧪 Mock pipeline status check - Job ID: {job_id}, Status: {status}"
                )

                return MockResponse(
                    status_code=200, json_data={"status": status, "jobId": job_id}
                )
            else:
                return MockResponse(
                    status_code=404, json_data={"error": "Job not found"}
                )

        return MockResponse(status_code=404, json_data={"error": "Not found"})


class MockResponse:
    """Mock response object for testing."""

    def __init__(self, status_code: int, json_data: dict = None, headers: dict = None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}
        self.text = str(json_data) if json_data else ""

    def json(self) -> dict:
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class PipelineUtils:
    """
    Utility class for managing Fabric pipelines with robust error handling and retry logic.
    Supports local testing with mock responses when fabric_environment is 'local'.
    """

    def __init__(self):
        """
        Initialize the PipelineUtils class.
        Automatically detects local mode using get_configs_as_object().fabric_environment == "local".
        """
        self.client = self._get_pipeline_client()

    def _use_semantic_link(self) -> bool:
        return importlib.util.find_spec("semantic-link") is not None

    def _get_pipeline_client(self):
        # Check if we should use local testing mode
        try:
            from ingen_fab.python_libs.common.config_utils import get_configs_as_object

            config = get_configs_as_object()
            if config.fabric_environment == "local":
                global _mock_client_instance
                if _mock_client_instance is None:
                    _mock_client_instance = MockPipelineClient()
                logger.info("🧪 Using mock pipeline client for local testing")
                return _mock_client_instance
        except ImportError:
            logger.warning(
                "Could not import config_utils, assuming non-local environment"
            )

        # Use real Fabric client for non-local environments
        if self._use_semantic_link():
            import sempy.fabric as fabric  # type: ignore  # noqa: I001

            return fabric.FabricRestClient()
        else:
            from azure.identity import DefaultAzureCredential

            # Create a class that mathes semantic-link's FabricRestClient interface
            class FabricRestClient:
                def __init__(self, base_url: str, credential: DefaultAzureCredential):
                    self.base_url = base_url
                    self.session = requests.Session()
                    token = credential.get_token(
                        "https://api.fabric.microsoft.com/.default"
                    ).token
                    self.session.headers.update(
                        {
                            "Authorization": f"Bearer {token}",
                            "Content-Type": "application/json",
                        }
                    )

                def post(self, endpoint: str, json: dict) -> requests.Response:
                    url = f"{self.base_url}/{endpoint}"
                    return self.session.post(url, json=json)

                def get(self, endpoint: str) -> requests.Response:
                    url = f"{self.base_url}/{endpoint}"
                    return self.session.get(url)

            credential = DefaultAzureCredential()
            return FabricRestClient(
                base_url="https://api.fabric.microsoft.com", credential=credential
            )

    async def trigger_pipeline(
        self, workspace_id: str, pipeline_id: str, payload: Dict[str, Any]
    ) -> str:
        """
        Trigger a Fabric pipeline job via REST API with robust retry logic matching original complexity.

        This function attempts to start a pipeline execution via the Fabric REST API, handling
        both transient and permanent errors with an exponential backoff retry strategy.

        Args:
            workspace_id: The ID of the Fabric workspace containing the pipeline
            pipeline_id: The ID of the pipeline to trigger
            payload: Dictionary containing the pipeline execution parameters

        Returns:
            str: The job ID of the successfully triggered pipeline

        Raises:
            RuntimeError: If the pipeline couldn't be triggered after all retries,
                         or if a permanent client error occurs (non-retryable)

        Note:
            - Uses exponential backoff with jitter for retries
            - HTTP 429, 408, and 5xx errors are considered transient and will be retried
            - Other client errors (4xx) are considered permanent after max_retries
        """
        max_retries = 5
        retry_delay = 2  # Initial delay in seconds
        backoff_factor = 1.5  # Exponential backoff multiplier  # noqa: F841

        trigger_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"

        for attempt in range(1, max_retries + 1):
            try:
                response = self.client.post(trigger_url, json=payload)

                # Success
                if response.status_code == 202:
                    response_location = response.headers.get("Location", "")
                    job_id = response_location.rstrip("/").split("/")[-1]
                    logger.info(f"Pipeline triggered successfully. Job ID: {job_id}")
                    return job_id

                # Handle HTTP errors using error categorization
                else:
                    # Categorize HTTP error
                    category, severity, is_retryable = (
                        error_categorizer.categorize_http_error(
                            response.status_code, response.text
                        )
                    )

                    # Log with appropriate context
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    error_categorizer.log_error(
                        Exception(error_msg),
                        category,
                        severity,
                        context={
                            "attempt": attempt,
                            "max_retries": max_retries,
                            "operation": "trigger_pipeline",
                        },
                    )

                    # Check if we should retry
                    if attempt == max_retries or not is_retryable:
                        raise RuntimeError(
                            f"HTTP {response.status_code}: {response.text}"
                        )

                    # Calculate delay based on error category
                    sleep_time = error_categorizer.get_retry_delay(
                        category, attempt, retry_delay
                    )
                    jitter = 0.8 + (0.4 * np.random.random())
                    sleep_time = sleep_time * jitter

                    logger.info(
                        f"Retrying in {sleep_time:.2f} seconds (category: {category.value}, severity: {severity.value})"
                    )
                    await asyncio.sleep(sleep_time)
                    continue

            except Exception as exc:
                # Categorize the error
                category, severity, is_retryable = (
                    error_categorizer.categorize_exception(exc)
                )

                # Log error with appropriate context
                error_categorizer.log_error(
                    exc,
                    category,
                    severity,
                    context={
                        "attempt": attempt,
                        "max_retries": max_retries,
                        "operation": "trigger_pipeline",
                    },
                )

                # Re-raise on the last attempt or if non-retryable
                if attempt == max_retries or not is_retryable:
                    logger.error(
                        "Exhausted retries or non-retryable error - raising exception",
                        exc_info=True,
                    )
                    raise

                # Calculate delay based on error category
                sleep_time = error_categorizer.get_retry_delay(
                    category, attempt, retry_delay
                )

                # Add jitter (±20%) to avoid thundering herd problem
                jitter = 0.8 + (0.4 * np.random.random())
                sleep_time = sleep_time * jitter

                logger.info(
                    f"Retrying in {sleep_time:.2f} seconds (category: {category.value}, severity: {severity.value})"
                )
                await asyncio.sleep(sleep_time)

        raise RuntimeError(f"Failed to trigger pipeline after {max_retries} attempts")

    async def check_pipeline(
        self, table_name: str, workspace_id: str, pipeline_id: str, job_id: str
    ) -> tuple[Optional[str], str]:
        """
        Check the status of a pipeline job with enhanced error handling matching original complexity.

        Args:
            table_name: Name of the table being processed for display
            workspace_id: Fabric workspace ID
            pipeline_id: ID of the pipeline being checked
            job_id: The job ID to check

        Returns:
            Tuple of (status, error_message)
            - status: Pipeline status or None for transient errors
            - error_message: Detailed error message if applicable
        """
        status_url = (
            f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
        )

        try:
            response = self.client.get(status_url)

            # Handle HTTP error status codes
            if response.status_code >= 400:
                if response.status_code == 404:
                    # Job not found - could be a temporary issue or job is still initializing
                    logger.info(
                        f"Pipeline {job_id} for {table_name}: Job not found (404) - may be initializing"
                    )
                    return None, f"Job not found (404): {job_id}"
                elif response.status_code >= 500 or response.status_code in [429, 408]:
                    # Server-side error or rate limiting - likely temporary
                    logger.info(
                        f"Pipeline {job_id} for {table_name}: Server error ({response.status_code})"
                    )
                    return (
                        None,
                        f"Server error ({response.status_code}): {response.text[:100]}",
                    )
                else:
                    # Other client errors (4xx)
                    logger.error(
                        f"Pipeline {job_id} for {table_name}: API error ({response.status_code})"
                    )
                    return (
                        "Error",
                        f"API error ({response.status_code}): {response.text[:100]}",
                    )

            # Parse the JSON response
            try:
                data = response.json()
            except Exception as e:
                # Invalid JSON in response
                logger.warning(
                    f"Pipeline {job_id} for {table_name}: Invalid response format"
                )
                return None, f"Invalid response format: {str(e)}"

            status = data.get("status")

            # Handle specific failure states with more context
            if status == "Failed" and "failureReason" in data:
                fr = data["failureReason"]

                # Use error categorization for pipeline failures
                category, severity, is_retryable = (
                    error_categorizer.categorize_pipeline_failure(fr)
                )

                # Log with appropriate context
                error_categorizer.log_error(
                    Exception(f"Pipeline failure: {fr}"),
                    category,
                    severity,
                    context={
                        "job_id": job_id,
                        "table_name": table_name,
                        "operation": "pipeline_execution",
                    },
                )

                # Return None for retryable errors, status for permanent ones
                if is_retryable:
                    logger.info(
                        f"Pipeline {job_id} for {table_name}: Retryable failure ({category.value})"
                    )
                    return None, fr.get("message", "")
                else:
                    logger.error(
                        f"Pipeline {job_id} for {table_name}: ❌ {status} - {fr.get('errorCode', '')}: {fr.get('message', '')[:100]}"
                    )
                    return status, fr.get("message", "")

            # Log status update with appropriate level
            if status in {"Completed", "Deduped"}:
                logger.info(f"Pipeline {job_id} for {table_name}: ✅ {status}")
            elif status == "Failed":
                logger.error(f"Pipeline {job_id} for {table_name}: ❌ {status}")
            elif status in {"Running", "NotStarted", "Pending", "Queued"}:
                logger.debug(f"Pipeline {job_id} for {table_name}: ⏳ {status}")
            else:
                logger.info(f"Pipeline {job_id} for {table_name}: {status}")

            return status, ""

        except Exception as e:
            error_msg = str(e)

            # Categorize exceptions with more specificity
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                # Network-related issues are transient
                logger.warning(
                    f"Pipeline {job_id} for {table_name}: Network error: {error_msg[:100]}"
                )
                return None, f"Network error: {error_msg}"
            elif "not valid for Guid" in error_msg:
                # Invalid GUID format - this is likely a client error
                logger.error(
                    f"Pipeline {job_id} for {table_name}: Invalid job ID format"
                )
                return "Error", f"Invalid job ID format: {error_msg}"
            elif "httperror" in error_msg.lower():
                # HTTP errors that weren't caught above
                logger.warning(
                    f"Pipeline {job_id} for {table_name}: HTTP error: {error_msg[:100]}"
                )
                return None, f"HTTP error: {error_msg}"
            else:
                # Unexpected exceptions
                logger.error(
                    f"Failed to check pipeline status for {table_name}: {error_msg[:100]}"
                )
                return None, error_msg

    async def poll_job(self, job_url: str, table_name: str = "") -> str:
        """
        Continuously poll a Fabric pipeline job until it reaches a terminal state.

        This function monitors the execution status of a pipeline job by periodically
        checking its state via the Fabric REST API. It implements several reliability
        mechanisms including grace periods for newly created jobs, handling of transient
        errors, and recognition of terminal states.

        Args:
            job_url: The URL of the job to poll, in format
                     "v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
            table_name: Optional table name for logging context

        Returns:
            str: The final state of the job ("Completed", "Failed", "Cancelled", or "Deduped")

        Raises:
            HTTPError: If the job URL is invalid or if persistent API errors occur

        Note:
            - Uses a grace period (POLL_INTERVAL) for jobs that might not be immediately visible
            - Handles transient HTTP errors (429, 408, 5xx) with automatic retries
            - Early non-terminal states like "Failed" during the grace period will be rechecked
            - Waits POLL_INTERVAL seconds between normal status checks and FAST_RETRY_SEC
              seconds during the grace period
        """
        t0 = time.monotonic()
        grace_end = t0 + POLL_INTERVAL

        while True:
            try:
                resp = self.client.get(job_url)

                # transient errors
                if resp.status_code in TRANSIENT_HTTP:
                    logger.warning(
                        f"Transient HTTP error {resp.status_code} when polling job {table_name}, retrying..."
                    )
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # 404 before job appears
                if resp.status_code == 404:
                    if time.monotonic() < grace_end:
                        await asyncio.sleep(FAST_RETRY_SEC)
                        continue
                    logger.error(f"Job not found after grace period: {job_url}")
                    resp.raise_for_status()

                resp.raise_for_status()

                # Get status from response
                response_data = resp.json()
                state = (response_data.get("status") or "Unknown").title()

                # early non-terminal states during grace window
                if (
                    state in {"Failed", "NotStarted", "Unknown"}
                    and time.monotonic() < grace_end
                ):
                    logger.info(
                        f"Job {table_name} in early state {state}, retrying in grace period..."
                    )
                    await asyncio.sleep(FAST_RETRY_SEC)
                    continue

                # terminal states as per MS Fabric REST API documentation
                if state in {"Completed", "Failed", "Cancelled", "Deduped"}:
                    logger.info(f"Job {table_name} reached terminal state: {state}")
                    return state

                # still executing, wait
                logger.debug(
                    f"Job {table_name} still running in state {state}, polling..."
                )
                await asyncio.sleep(POLL_INTERVAL)

            except Exception as e:
                # Handle exceptions during polling
                error_msg = str(e)
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    logger.warning(
                        f"Network error polling job {table_name}: {error_msg}"
                    )
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
                else:
                    logger.error(
                        f"Unexpected error polling job {table_name}: {error_msg}"
                    )
                    raise

    async def trigger_pipeline_with_polling(
        self,
        workspace_id: str,
        pipeline_id: str,
        payload: Dict[str, Any],
        table_name: str = "",
    ) -> str:
        """
        Trigger a pipeline and poll it to completion, returning the final status.

        This combines the trigger and polling operations into a single method that
        matches the workflow from the original file.

        Args:
            workspace_id: Fabric workspace ID
            pipeline_id: Pipeline ID to trigger
            payload: Pipeline execution payload
            table_name: Table name for logging context

        Returns:
            Final status of the pipeline execution
        """
        # Trigger the pipeline
        job_id = await self.trigger_pipeline(workspace_id, pipeline_id, payload)

        # Construct the job URL for polling
        job_url = (
            f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
        )

        # Poll until completion
        final_status = await self.poll_job(job_url, table_name)

        return final_status
