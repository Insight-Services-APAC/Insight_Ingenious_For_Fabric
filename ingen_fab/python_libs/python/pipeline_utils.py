import asyncio
import importlib.util
import logging
import uuid
from typing import Any, Dict, Optional

import numpy as np
import requests

logger = logging.getLogger(__name__)

# Global mock client instance for sharing job state across instances
_mock_client_instance = None


class MockPipelineClient:
    """Mock client for local testing when fabric_environment is 'local'."""
    
    def __init__(self):
        self.job_statuses = {}  # Store job statuses for tracking
        
    def post(self, endpoint: str, json: dict) -> 'MockResponse':
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
                headers={'Location': f'/v1/jobs/instances/{job_id}'},
                json_data={"jobId": job_id}
            )
        
        return MockResponse(status_code=404, json_data={"error": "Not found"})
    
    def get(self, endpoint: str) -> 'MockResponse':
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
                
                logger.debug(f"🧪 Mock pipeline status check - Job ID: {job_id}, Status: {status}")
                
                return MockResponse(
                    status_code=200,
                    json_data={"status": status, "jobId": job_id}
                )
            else:
                return MockResponse(status_code=404, json_data={"error": "Job not found"})
        
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
            from ..common.config_utils import get_configs_as_object
            config = get_configs_as_object()
            if config.fabric_environment == "local":
                global _mock_client_instance
                if _mock_client_instance is None:
                    _mock_client_instance = MockPipelineClient()
                logger.info("🧪 Using mock pipeline client for local testing")
                return _mock_client_instance
        except ImportError:
            logger.warning("Could not import config_utils, assuming non-local environment")
        
        # Use real Fabric client for non-local environments
        if self._use_semantic_link():
            import sempy.fabric as fabric # type: ignore  # noqa: I001
            return fabric.FabricRestClient()
        else:
            from azure.identity import DefaultAzureCredential

            #Create a class that mathes semantic-link's FabricRestClient interface
            class FabricRestClient:
                def __init__(self, base_url: str, credential: DefaultAzureCredential):
                    self.base_url = base_url
                    self.session = requests.Session()
                    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
                    self.session.headers.update({
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    })

                def post(self, endpoint: str, json: dict) -> requests.Response:
                    url = f"{self.base_url}/{endpoint}"
                    return self.session.post(url, json=json)

                def get(self, endpoint: str) -> requests.Response:
                    url = f"{self.base_url}/{endpoint}"
                    return self.session.get(url)

            credential = DefaultAzureCredential()
            return FabricRestClient(base_url="https://api.fabric.microsoft.com", credential=credential)


    async def trigger_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        payload: Dict[str, Any]
    ) -> str:
        """
        Trigger a Fabric pipeline job via REST API with robust retry logic.
        
        Args:
            client: Authenticated Fabric REST client
            workspace_id: Fabric workspace ID
            pipeline_id: ID of the pipeline to run
            payload: Parameters to pass to the pipeline
            
        Returns:
            The job ID of the triggered pipeline
        """
        max_retries = 5
        retry_delay = 2  # Initial delay in seconds
        backoff_factor = 1.5  # Exponential backoff multiplier
        
        for attempt in range(1, max_retries + 1):
            try:
                trigger_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
                
                response = self.client.post(trigger_url, json=payload)
                
                # Check for successful response (202 Accepted)
                if response.status_code == 202:
                    # Extract job ID from Location header
                    response_location = response.headers.get('Location', '')
                    job_id = response_location.rstrip("/").split("/")[-1]    
                    print(f"✅ Pipeline triggered successfully. Job ID: {job_id}")
                    return job_id
                
                # Handle specific error conditions
                elif response.status_code >= 500 or response.status_code in [429, 408]:
                    # Server errors (5xx) or rate limiting (429) or timeout (408) are likely transient
                    error_msg = f"Transient error (HTTP {response.status_code}): {response.text[:100]}"
                    print(f"⚠️ Attempt {attempt}/{max_retries}: {error_msg}")
                else:
                    # Client errors (4xx) other than rate limits are likely permanent
                    error_msg = f"Client error (HTTP {response.status_code}): {response.text[:100]}"
                    if attempt == max_retries:
                        print(f"❌ Failed after {max_retries} attempts: {error_msg}")
                        raise Exception(f"Failed to trigger pipeline: {response.status_code}\n{response.text}")
                    else:
                        print(f"⚠️ Attempt {attempt}/{max_retries}: {error_msg}")
            
            except Exception as e:
                # Handle connection or other exceptions
                error_msg = str(e)
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    # Network-related errors are likely transient
                    print(f"⚠️ Attempt {attempt}/{max_retries}: Network error: {error_msg}")
                else:
                    # Re-raise non-network exceptions on the last attempt
                    if attempt == max_retries:
                        print(f"❌ Failed after {max_retries} attempts due to unexpected error: {error_msg}")
                        raise
                    else:
                        print(f"⚠️ Attempt {attempt}/{max_retries}: Unexpected error: {error_msg}")
            
            # Don't sleep on the last attempt
            if attempt < max_retries:
                # Calculate sleep time with exponential backoff and a bit of randomization
                sleep_time = retry_delay * (backoff_factor ** (attempt - 1))
                # Add jitter (±20%) to avoid thundering herd problem
                jitter = 0.8 + (0.4 * np.random.random())
                sleep_time = sleep_time * jitter
                
                print(f"🕒 Retrying in {sleep_time:.2f} seconds...")
                await asyncio.sleep(sleep_time)
        
        # This should only be reached if we exhaust all retries on a non-4xx error
        raise Exception(f"❌ Failed to trigger pipeline after {max_retries} attempts")


    async def check_pipeline(
        self,
        table_name: str,
        workspace_id: str,
        pipeline_id: str,
        job_id: str
    ) -> tuple[Optional[str], str]:
        """
        Check the status of a pipeline job with enhanced error handling.
        
        Args:
            client: Authenticated Fabric REST client
            table_name: Name of the table being processed for display
            workspace_id: Fabric workspace ID
            pipeline_id: ID of the pipeline being checked
            job_id: The job ID to check
            
        Returns:
            Tuple of (status, error_message)
        """
        status_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
        
        try:
            response = self.client.get(status_url)
            
            # Handle HTTP error status codes
            if response.status_code >= 400:
                if response.status_code == 404:
                    # Job not found - could be a temporary issue or job is still initializing
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Job not found (404) - may be initializing")
                    return None, f"Job not found (404): {job_id}"
                elif response.status_code >= 500 or response.status_code in [429, 408]:
                    # Server-side error or rate limiting - likely temporary
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Server error ({response.status_code})")
                    return None, f"Server error ({response.status_code}): {response.text[:100]}"
                else:
                    # Other client errors (4xx)
                    print(f"[ERROR] Pipeline {job_id} for {table_name}: API error ({response.status_code})")
                    return "Error", f"API error ({response.status_code}): {response.text[:100]}"
            
            # Parse the JSON response
            try:
                data = response.json()
            except Exception as e:
                # Invalid JSON in response
                print(f"[WARNING] Pipeline {job_id} for {table_name}: Invalid response format")
                return None, f"Invalid response format: {str(e)}"
            
            status = data.get("status")

            # Handle specific failure states with more context
            if status == "Failed" and "failureReason" in data:
                fr = data["failureReason"]
                msg = fr.get("message", "")
                error_code = fr.get("errorCode", "")
                
                # Check for specific transient errors
                if error_code == "RequestExecutionFailed" and "NotFound" in msg:
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Transient check-failure, retrying: {error_code}")
                    return None, msg
                
                # Resource constraints may be temporary
                if any(keyword in msg.lower() for keyword in ["quota", "capacity", "throttl", "timeout"]):
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Resource constraint issue: {error_code}")
                    return None, msg
                
                # Print failure with details
                print(f"Pipeline {job_id} for {table_name}: ❌ {status} - {error_code}: {msg[:100]}")
                return status, msg

            # Print status update with appropriate icon
            status_icons = {
                "Completed": "✅",
                "Failed": "❌",
                "Running": "⏳",
                "NotStarted": "⏳",
                "Pending": "⌛",
                "Queued": "🔄"
            }
            icon = status_icons.get(status, "⏳")
            print(f"Pipeline {job_id} for {table_name}: {icon} {status}")

            return status, ""
        
        except Exception as e:
            error_msg = str(e)
            
            # Categorize exceptions
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                # Network-related issues are transient
                print(f"[WARNING] Pipeline {job_id} for {table_name}: Network error: {error_msg[:100]}")
                return None, f"Network error: {error_msg}"
            elif "not valid for Guid" in error_msg:
                # Invalid GUID format - this is likely a client error
                print(f"[ERROR] Pipeline {job_id} for {table_name}: Invalid job ID format")
                return "Error", f"Invalid job ID format: {error_msg}"
            else:
                # Unexpected exceptions
                print(f"[ERROR] Failed to check pipeline status for {table_name}: {error_msg[:100]}")
                return None, error_msg