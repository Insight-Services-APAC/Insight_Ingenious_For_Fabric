"""
Standard interface for synapse orchestrator utilities.

This module defines the common interface for synapse extract orchestration
capabilities including pipeline management, job polling, and extract processing.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface


@dataclass
class WorkItem:
    """Represents a single extraction work item."""

    source_schema_name: str
    source_table_name: str
    extract_mode: str
    execution_group: int
    extract_start_dt: Optional[str] = None
    extract_end_dt: Optional[str] = None
    single_date_filter: Optional[str] = None
    date_range_filter: Optional[str] = None
    custom_select_sql: Optional[str] = None
    synapse_sync_fabric_pipeline_id: Optional[str] = None
    synapse_connection_name: Optional[str] = None
    synapse_datasource_name: Optional[str] = None
    synapse_datasource_location: Optional[str] = None
    synapse_external_table_schema: Optional[str] = None
    export_base_dir: Optional[str] = None
    # Run-scoped metadata (single source of truth; avoid separate config dicts)
    trigger_type: Optional[str] = None
    master_execution_parameters: Optional[Dict[str, Any]] = None


class SynapseOrchestratorInterface(ABC):
    """Abstract interface for Synapse extract orchestration utilities."""

    @abstractmethod
    def __init__(self, lakehouse: DataStoreInterface):
        """Initialise the orchestrator with a data store."""
        pass

    # Note: config lookup via orchestrator is deprecated; notebooks should pass
    # required values via variable library directly.

    @abstractmethod
    async def trigger_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        parameters: Dict[str, Any],
        max_retries: int = 3,
    ) -> Tuple[str, bool]:
        """
        Trigger a Fabric pipeline with enhanced retry logic.

        Args:
            workspace_id: The workspace ID containing the pipeline
            pipeline_id: The pipeline ID to trigger
            parameters: Parameters to pass to the pipeline
            max_retries: Maximum number of retry attempts

        Returns:
            Tuple of (job_id, success_flag)
        """
        pass

    @abstractmethod
    async def poll_job(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_id: str,
        timeout_minutes: int = 30,
        polling_interval: int = 10,
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Poll a pipeline job until completion with advanced error handling.

        Args:
            workspace_id: The workspace ID containing the job
            pipeline_id: The pipeline item ID
            job_id: The job ID to poll
            timeout_minutes: Maximum time to wait for completion
            polling_interval: Seconds between polling attempts

        Returns:
            Tuple of (final_status, success_flag, error_message)
        """
        pass

    @abstractmethod
    async def process_extract(
        self,
        work_item: WorkItem,
        config: Optional[Dict[str, Any]],
        master_execution_id: str,
        semaphore: asyncio.Semaphore,
        *,
        workspace_id: str,
        synapse_sync_fabric_pipeline_id: Optional[str] = None,
        extract_utils: Optional[Any] = None,
        execution_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Process a single extraction with complete lifecycle management.

        Args:
            work_item: The extraction work item to process
            config: Optional context/config dictionary
            master_execution_id: The master execution ID for grouping
            semaphore: Concurrency control semaphore
            workspace_id: Fabric workspace ID containing the pipeline
            synapse_sync_fabric_pipeline_id: Pipeline item ID to trigger
            extract_utils: Optional utils instance used for logging and parameter building
            execution_id: Optional pre-logged execution_id for log updates

        Returns:
            Tuple of (success_flag, error_message)
        """
        pass

    @abstractmethod
    async def run_async_orchestration(
        self,
        work_items: List[WorkItem],
        master_execution_id: str,
        max_concurrency: int = 10,
        *,
        workspace_id: str,
        synapse_sync_fabric_pipeline_id: Optional[str] = None,
        extract_utils: Optional[Any] = None,
        execution_id_map: Optional[Dict[str, str]] = None,
        external_table_map: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Master orchestrator with concurrency control and execution groups.

        Args:
            work_items: List of work items to process
            master_execution_id: The master execution ID for grouping
            max_concurrency: Maximum concurrent extractions
            workspace_id: Fabric workspace ID containing the pipeline
            synapse_sync_fabric_pipeline_id: Pipeline item ID to trigger
            extract_utils: Optional utils instance used for logging and parameter building
            execution_id_map: Optional mapping of external_table -> execution_id (pre-logged)
            external_table_map: Optional mapping of payload key -> external_table

        Returns:
            Dictionary containing execution summary and results
        """
        pass

    @abstractmethod
    def prepare_work_items(
        self,
        synapse_sync_fabric_pipeline_id: Optional[str],
        synapse_datasource_name: str,
        synapse_datasource_location: str,
        synapse_external_table_schema: str,
        *,
        trigger_type: Optional[str] = "Manual",
        master_execution_parameters: Optional[Dict[str, Any]] = None,
        work_items_json: Optional[str] = None,
        include_snapshots: bool = True,
        extraction_start_date: Optional[str] = None,
        extraction_end_date: Optional[str] = None,
        batch_id: Optional[int] = 1
    ) -> List[WorkItem]:
        """
        Prepare work items for processing based on configuration and parameters.

        Args:
            synapse_sync_fabric_pipeline_id: Default pipeline ID from varlib
            synapse_datasource_name: Datasource name from varlib
            synapse_datasource_location: Datasource location from varlib
            work_items_json: JSON string of specific work items (for historical mode)
            include_snapshots: Whether to include snapshot tables
            extraction_start_date: Start date for daily/historical extraction (YYYY-MM-DD)
            extraction_end_date: End date for daily/historical extraction (YYYY-MM-DD)

        Returns:
            List of prepared WorkItem objects
        """
        pass

    @abstractmethod
    def get_pipeline_parameters(
        self, work_item: WorkItem, config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Build pipeline parameters for a given work item.

        Args:
            work_item: The work item to build parameters for
            config: Optional context/config dictionary

        Returns:
            Dictionary of pipeline parameters
        """
        pass
