"""
PySpark implementation of synapse orchestrator utilities.

This module provides the PySpark-specific implementation for synapse extract 
orchestration capabilities including pipeline management, job polling, and 
extract processing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import nest_asyncio
import numpy as np
from pyspark.sql import SparkSession

try:
    import sempy.fabric as fabric
    from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
    FABRIC_AVAILABLE = True
except ImportError:
    FABRIC_AVAILABLE = False

import pyspark.sql.functions as F

from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface
from ingen_fab.python_libs.interfaces.synapse_orchestrator_interface import (
    SynapseOrchestratorInterface,
    WorkItem,
)

logger = logging.getLogger(__name__)

# Constants
DEFAULT_MAX_CONCURRENCY = 10
POLL_INTERVAL = 60
FAST_RETRY_SEC = 10
POLL_TIMEOUT_SEC = 1800  # 30 minutes
GRACE_WINDOW_SEC = 60    # Grace period for job appearance/early states
TRANSIENT_HTTP = {429, 500, 503, 504, 408}
TERMINAL_STATES = {"Completed", "Failed", "Cancelled", "Deduped"}
EARLY_STATES = {"Failed", "NotStarted", "Unknown"}



class SynapseOrchestrator(SynapseOrchestratorInterface):
    """PySpark implementation of Synapse extract orchestration utilities."""

    def __init__(self, lakehouse: DataStoreInterface):
        """Initialise the orchestrator with a data store."""
        self.lakehouse = lakehouse

    @staticmethod
    def resolve_pipeline_id_by_name(workspace_id: str, pipeline_name: str) -> str:
        """Resolve a Fabric pipeline GUID by its display name using sempy.fabric.

        Uses sempy.fabric where `fabric.list_items` returns a pandas
        DataFrame with columns: `Id`, `Display Name`, `Description`, `Type`, and `Workspace Id`.
        Filters items to DataPipeline type and matches Display Name case-insensitively to
        provided pipeline name.
        """
        if not FABRIC_AVAILABLE:
            raise RuntimeError("sempy.fabric is not available - cannot resolve pipeline by name")
        if not workspace_id or not str(workspace_id).strip():
            raise ValueError("workspace_id is required to resolve pipeline id by name")
        if not pipeline_name or not str(pipeline_name).strip():
            raise ValueError("pipeline_name is required to resolve pipeline id by name")

        logger.debug(f"Resolving pipeline '{pipeline_name}' in workspace '{workspace_id}'")

        try:
            items_df = fabric.list_items(workspace=workspace_id)
        except FabricHTTPException as fabric_exc:
            logger.error(f"Fabric HTTP error listing items in workspace '{workspace_id}': {fabric_exc}")
            raise RuntimeError(f"Failed to list items in workspace '{workspace_id}': {fabric_exc}")
        except WorkspaceNotFoundException as workspace_exc:
            logger.error(f"Workspace not found: '{workspace_id}': {workspace_exc}")
            raise LookupError(f"Workspace '{workspace_id}' not found: {workspace_exc}")
        except Exception as exc:
            logger.error(f"Unexpected error listing items in workspace '{workspace_id}': {exc}")
            raise RuntimeError(f"Unexpected error listing items in workspace '{workspace_id}': {exc}")

        # Ensure required columns exist
        required_cols = {"Id", "Display Name", "Type"}
        if items_df.empty:
            logger.warning(f"No items found in workspace '{workspace_id}'")
            raise LookupError(f"No items found in workspace '{workspace_id}'")

        missing = required_cols.difference(set(items_df.columns))  # type: ignore[arg-type]
        if missing:
            logger.error(f"Missing expected columns in list_items response: {missing}")
            raise RuntimeError(f"list_items missing expected columns: {', '.join(sorted(missing))}")

        # Filter to DataPipeline type and match pipeline name
        pipelines_df = items_df[items_df["Type"] == "DataPipeline"]
        logger.debug(f"Found {len(pipelines_df)} pipelines in workspace '{workspace_id}'")

        if pipelines_df.empty:
            raise LookupError(f"No pipelines found in workspace '{workspace_id}'")

        matches = pipelines_df[
            pipelines_df["Display Name"].str.casefold() == pipeline_name.casefold()
        ]

        count = len(matches)
        if count == 0:
            available_pipelines = pipelines_df["Display Name"].tolist()
            logger.warning(f"Pipeline '{pipeline_name}' not found. Available pipelines: {available_pipelines}")
            raise LookupError(f"No pipeline named '{pipeline_name}' found in workspace '{workspace_id}'")
        if count > 1:
            names = ", ".join(sorted(set(matches["Display Name"].tolist())))
            logger.error(f"Multiple pipelines with name '{pipeline_name}' found: {names}")
            raise LookupError(f"Multiple pipelines named '{pipeline_name}' found in workspace '{workspace_id}': {names}")

        pipeline_id = str(matches.iloc[0]["Id"])
        logger.info(f"Resolved pipeline '{pipeline_name}' to ID: {pipeline_id}")
        return pipeline_id


    async def trigger_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        parameters: Dict[str, Any],
        max_retries: int = 5,
        backoff_factor: float = 2.0
    ) -> Tuple[str, bool]:
        """Trigger a Fabric pipeline with enhanced retry logic."""
        if not FABRIC_AVAILABLE:
            raise RuntimeError("sempy.fabric is not available - cannot trigger pipeline")

        # Configure retry strategy following Microsoft recommendations
        retry_config = {
            "total": max_retries,
            "allowed_methods": ["POST"],
            "status_forcelist": [408, 429, 500, 502, 503, 504],
            "backoff_factor": backoff_factor,
            "raise_on_status": False
        }
        
        client = fabric.FabricRestClient(retry_config=retry_config)
        trigger_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"

        # Initial jitter to spread concurrent requests
        initial_jitter = np.random.uniform(0, 10.0)
        await asyncio.sleep(initial_jitter)
        
        try:
            # Ensure parameters are wrapped as expected by Fabric pipeline API
            payload = parameters
            if not isinstance(parameters, dict) or "executionData" not in parameters:
                payload = {"executionData": {"parameters": parameters or {}}}
            
            response = client.post(trigger_url, json=payload)
            
            if response.status_code == 202:
                response_location = response.headers.get('Location', '')
                job_id = response_location.rstrip("/").split("/")[-1]
                logger.info(f"Pipeline triggered successfully. Job ID: {job_id}")
                return job_id, True
            else:
                logger.error(f"Unexpected response {response.status_code}: {response.text}")
                return "", False
                
        except FabricHTTPException as fabric_exc:
            logger.error(f"Fabric HTTP error: {fabric_exc}")
            return "", False
        except WorkspaceNotFoundException as workspace_exc:
            logger.error(f"Workspace not found: {workspace_exc}")
            return "", False
        except Exception as exc:
            logger.error(f"Unexpected error triggering pipeline: {exc}")
            return "", False

    async def poll_job(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_id: str,
        timeout_minutes: int = POLL_TIMEOUT_SEC // 60,
        polling_interval: int = 60,
        fast_retry_sec: int = FAST_RETRY_SEC
    ) -> Tuple[str, bool, Optional[str]]:
        """Poll a pipeline job until completion with advanced error handling."""
        if not FABRIC_AVAILABLE:
            raise RuntimeError("sempy.fabric is not available - cannot poll job")

        client = fabric.FabricRestClient()
        job_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
        start_time = time.monotonic()
        grace_end = start_time + GRACE_WINDOW_SEC
        timeout_seconds = timeout_minutes * 60

        while True:
            elapsed_time = time.monotonic() - start_time
            if elapsed_time > timeout_seconds:
                error_msg = f"Polling timeout exceeded ({timeout_minutes} minutes) for job {job_id}"
                logger.error(error_msg)
                return "Timeout", False, error_msg

            try:
                resp = client.get(job_url)

                # Handle transient errors
                if resp.status_code in TRANSIENT_HTTP:
                    logger.warning(f"Transient HTTP error {resp.status_code} when polling job, retrying...")
                    await asyncio.sleep(polling_interval)
                    continue

                # Handle 404 before job appears
                if resp.status_code == 404:
                    if time.monotonic() < grace_end:
                        await asyncio.sleep(fast_retry_sec)
                        continue
                    error_msg = f"Job not found after grace period: {job_id}"
                    logger.error(error_msg)
                    return "NotFound", False, error_msg

                resp.raise_for_status()
                state = (resp.json().get("status") or "Unknown").title()

                # Handle early non-terminal states during grace window (align with notebook: include Failed)
                if state in EARLY_STATES and time.monotonic() < grace_end:
                    logger.info(f"Job in early state {state}, retrying in grace period...")
                    await asyncio.sleep(fast_retry_sec)
                    continue

                # Terminal states
                if state in TERMINAL_STATES:
                    logger.info(f"Job reached terminal state: {state}")
                    success = state in {"Completed", "Deduped"}
                    error_msg = None if success else f"Job ended in state: {state}"
                    return state, success, error_msg

                # Still executing, wait
                retry_after = int(resp.headers.get('Retry-After', polling_interval))
                logger.debug(f"Job still running in state {state}, polling in {retry_after}s...")
                await asyncio.sleep(retry_after)
        
            except FabricHTTPException as fabric_exc:
                if hasattr(fabric_exc, 'response') and fabric_exc.response.status_code in TRANSIENT_HTTP:
                    logger.warning("Transient Fabric HTTP error during polling, retrying...")
                    await asyncio.sleep(polling_interval)
                    client = fabric.FabricRestClient()
                    continue
                else:
                    error_msg = f"Non-transient Fabric HTTP error during polling: {fabric_exc}"
                    logger.error(error_msg)
                    return "Error", False, error_msg
        
            except Exception as exc:
                logger.warning(f"Unexpected error during polling: {exc}, retrying...")
                await asyncio.sleep(polling_interval)
                continue

    async def process_extract(
        self,
        work_item: WorkItem,
        config: Optional[Dict[str, Any]],
        master_execution_id: str,
        semaphore: asyncio.Semaphore,
        *,
        workspace_id: str,
        synapse_sync_fabric_pipeline_id: Optional[str] = None,
        extract_utils: Any | None = None,
        execution_id: str | None = None,
    ) -> Tuple[bool, Optional[str]]:
        """Process a single extraction with complete lifecycle management."""
        table_info = f"{work_item.source_schema_name}.{work_item.source_table_name}"
        
        async with semaphore:
            start_time = time.monotonic()
            
            try:
                # Configuration object is optional; validate only when provided
                # Pipeline workspace comes from explicit parameter 'pipeline_workspace_id'.

                # Build pipeline parameters using utils (preferred), else fallback
                pipeline_params = None
                if extract_utils is not None:
                    try:
                        # Prefer an explicit partition clause on the work item (e.g., from retry helper)
                        partition_clause = getattr(work_item, "partition_clause", None)
                        if not partition_clause:
                            partition_clause = extract_utils.build_partition_clause(
                                extract_mode=work_item.extract_mode,
                                single_date_filter=getattr(work_item, "single_date_filter", None),
                                date_range_filter=getattr(work_item, "date_range_filter", None),
                                extract_start_dt=work_item.extract_start_dt,
                                extract_end_dt=work_item.extract_end_dt,
                            )
                        path_info = extract_utils.build_path_components(
                            source_schema_name=work_item.source_schema_name,
                            source_table_name=work_item.source_table_name,
                            extract_mode=work_item.extract_mode,
                            extract_start_dt=work_item.extract_start_dt,
                            extract_end_dt=work_item.extract_end_dt,
                            export_base_dir=work_item.export_base_dir,
                        )
                        extraction_spec = {
                            "source_schema_name": work_item.source_schema_name,
                            "source_table_name": work_item.source_table_name,
                            "output_path": path_info.get("output_path"),
                            "partition_clause": partition_clause,
                            "external_table": path_info.get("external_table"),
                            # Include datasource and custom SQL to build valid CETAS
                            "synapse_connection_name": getattr(work_item, "synapse_connection_name", None),
                            "synapse_datasource_name": getattr(work_item, "synapse_datasource_name", None) or "",
                            "synapse_datasource_location": getattr(work_item, "synapse_datasource_location", None) or "",
                            "custom_select_sql": getattr(work_item, "custom_select_sql", None),
                        }
                        pipeline_params = extract_utils.build_pipeline_parameters_from_record(extraction_spec)
                    except Exception:
                        pipeline_params = None
                if pipeline_params is None:
                    pipeline_params = self.get_pipeline_parameters(work_item, config)
                
                logger.info(f"Processing {table_info} - Triggering pipeline...")
                
                # Use pipeline id resolved during work item creation process
                synapse_sync_fabric_pipeline_id = getattr(work_item, "synapse_sync_fabric_pipeline_id", None)
                if not synapse_sync_fabric_pipeline_id:
                    raise ValueError(
                        "Missing synapse_sync_fabric_pipeline_id on WorkItem. "
                        "Ensure overrides are resolved in prepare_work_items or caller before execution."
                    )

                # Trigger pipeline
                job_id, trigger_success = await self.trigger_pipeline(
                    workspace_id=workspace_id,
                    pipeline_id=synapse_sync_fabric_pipeline_id,
                    parameters=pipeline_params
                )
                
                if not trigger_success or not job_id:
                    error_msg = "Failed to trigger pipeline"
                    logger.error(f"Error triggering pipeline for {table_info}")
                    # Update log record as failed if available
                    if extract_utils and execution_id:
                        try:
                            extract_utils.update_log_record(
                                master_execution_id=master_execution_id,
                                execution_id=execution_id,
                                updates={
                                    "status": "Failed",
                                    "error_messages": error_msg,
                                },
                            )
                        except Exception:
                            logger.debug("Log update failed on trigger error; continuing")
                    return False, error_msg
                
                # Update log: mark as Running and set pipeline_job_id (UTC timestamps)
                if extract_utils and execution_id:
                    try:
                        extract_utils.update_log_record(
                            master_execution_id=master_execution_id,
                            execution_id=execution_id,
                            updates={
                                "status": "Running",
                                "pipeline_job_id": job_id,
                                "start_timestamp": datetime.now(timezone.utc),
                            },
                        )
                    except Exception:
                        logger.debug("Log update failed when marking Running; continuing")

                logger.info(f"Running {table_info} - Job ID: {job_id}")

                # Poll for completion
                final_state, poll_success, error_msg = await self.poll_job(
                    workspace_id=workspace_id,
                    pipeline_id=synapse_sync_fabric_pipeline_id,
                    job_id=job_id,
                    timeout_minutes=30
                )
                
                duration_sec = time.monotonic() - start_time
                
                if poll_success:
                    logger.info(f"{final_state} - {table_info} - Duration: {duration_sec:.2f}s")
                    # Update log: Completed
                    if extract_utils and execution_id:
                        try:
                            extract_utils.update_log_record(
                                master_execution_id=master_execution_id,
                                execution_id=execution_id,
                                updates={
                                    "status": "Completed",
                                    "end_timestamp": datetime.now(timezone.utc),
                                    "duration_sec": float(duration_sec),
                                },
                            )
                        except Exception:
                            logger.debug("Log update failed when marking Completed; continuing")
                    return True, None
                else:
                    logger.warning(f"{final_state} - {table_info} - Duration: {duration_sec:.2f}s")
                    if extract_utils and execution_id:
                        try:
                            extract_utils.update_log_record(
                                master_execution_id=master_execution_id,
                                execution_id=execution_id,
                                updates={
                                    "status": final_state if final_state else "Failed",
                                    "error_messages": error_msg or "",
                                    "end_timestamp": datetime.now(timezone.utc),
                                    "duration_sec": float(duration_sec),
                                },
                            )
                        except Exception:
                            logger.debug("Log update failed when marking terminal state; continuing")
                    return False, error_msg
                    
            except Exception as exc:
                duration_sec = time.monotonic() - start_time
                error_message = str(exc)
                logger.error(f"Error processing {table_info} - Duration: {duration_sec:.2f}s", exc_info=True)
                if extract_utils and execution_id:
                    try:
                        extract_utils.update_log_record(
                            master_execution_id=master_execution_id,
                            execution_id=execution_id,
                            updates={
                                "status": "Failed",
                                "error_messages": error_message,
                                "end_timestamp": datetime.now(timezone.utc),
                                "duration_sec": float(duration_sec),
                            },
                        )
                    except Exception:
                        logger.debug("Log update failed when handling unexpected error; continuing")
                return False, error_message

    async def run_async_orchestration(
        self,
        work_items: List[WorkItem],
        master_execution_id: str,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        *,
        workspace_id: str,
        synapse_sync_fabric_pipeline_id: Optional[str] = None,
        extract_utils: Any | None = None,
        execution_id_map: Dict[str, str] | None = None,
        external_table_map: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        """Run grouped orchestration with clear, minimal aggregation.

        Caller pre-logs "Queued" records and provides `execution_id_map` and
        `external_table_map`. We resolve `execution_id` per item, process groups
        sequentially and items within a group concurrently (bounded by `max_concurrency`).
        Returns a compact summary compatible with existing templates.
        """

        # Ensure an event loop exists and is usable in notebook/interactive contexts
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            nest_asyncio.apply()

        logger.info(f"Starting extraction process - Master Execution ID: {master_execution_id}")
        logger.info(f"Max Concurrency: {max_concurrency}")
        logger.info(f"Using pipeline ID: {synapse_sync_fabric_pipeline_id}")

        semaphore = asyncio.Semaphore(max_concurrency)

        # Group items by execution_group
        grouped: Dict[int, List[WorkItem]] = {}
        for wi in work_items:
            grouped.setdefault(wi.execution_group, []).append(wi)
        execution_groups = sorted(grouped.keys())

        # Pair of (WorkItem, (success, error)) for reliable aggregation
        all_pairs: List[Tuple[WorkItem, Tuple[bool, Optional[str]]]] = []

        async def run_one(item: WorkItem) -> Tuple[WorkItem, Tuple[bool, Optional[str]]]:
            # Build composite key and resolve execution_id
            key = f"{item.source_schema_name}.{item.source_table_name}|{item.extract_start_dt or ''}|{item.extract_end_dt or ''}|{item.extract_mode}"
            ext_table = external_table_map.get(key) if external_table_map else None

            exec_id: Optional[str] = None
            if execution_id_map and ext_table:
                candidates = [ext_table]
                if ext_table.startswith("exports."):
                    candidates.append(ext_table.split(".", 1)[-1])
                else:
                    candidates.append(f"exports.{ext_table}")
                for name in candidates:
                    exec_id = execution_id_map.get(name)
                    if exec_id:
                        break
                if exec_id is None:
                    logger.warning(
                        f"No execution_id found for external_table '{ext_table}' (candidates: {candidates})"
                    )

            result = await self.process_extract(
                item,
                None,
                master_execution_id,
                semaphore,
                workspace_id=workspace_id,
                synapse_sync_fabric_pipeline_id=synapse_sync_fabric_pipeline_id,
                extract_utils=extract_utils,
                execution_id=exec_id,
            )
            return item, result

        # Process groups sequentially
        for group in execution_groups:
            items = grouped[group]
            logger.info(f"Processing execution group {group} ({len(items)} extractions)")

            pairs = await asyncio.gather(*(run_one(it) for it in items), return_exceptions=False)
            all_pairs.extend(pairs)

            group_success = sum(1 for _, res in pairs if res[0])
            group_failed = len(pairs) - group_success
            logger.info(f"Group {group} completed: {group_success} successful, {group_failed} failed")

        # Aggregate results
        total = len(all_pairs)
        succeeded = sum(1 for _, res in all_pairs if res[0])
        failed = total - succeeded

        failed_details = [
            {
                "table": f"{wi.source_schema_name}.{wi.source_table_name}",
                "error": res[1],
            }
            for wi, res in all_pairs
            if not res[0]
        ]

        return {
            "master_execution_id": master_execution_id,
            "total_tables": total,
            "successful_extractions": succeeded,
            "failed_extractions": failed,
            "success_rate": f"{(succeeded/total)*100:.1f}%" if total > 0 else "N/A",
            "execution_groups": execution_groups,
            "failed_details": failed_details,
            "completion_time": datetime.now(timezone.utc).isoformat(),
        }

    def prepare_work_items(
        self,
        synapse_sync_fabric_pipeline_id: Optional[str],
        synapse_datasource_name: str,
        synapse_datasource_location: str,
        *,
        trigger_type: Optional[str] = "Manual",
        master_execution_parameters: Optional[Dict[str, Any]] = None,
        work_items_json: Optional[str] = None,
        include_snapshots: bool = True,
        extraction_start_date: Optional[str] = None,
        extraction_end_date: Optional[str] = None
    ) -> List[WorkItem]:
        """Prepare work items for processing based on configuration and parameters."""
        work_items = []

        try:
            # Helper to resolve and validate required per-item settings
            def _resolve_work_item_config(
                config_entry: Any,
                default_pipeline_id: Optional[str],
                default_ds_name: Optional[str],
                default_ds_loc: Optional[str],
            ) -> tuple[str, str, str]:
                """Resolve pipeline/datasource with per-row override, else defaults; error if missing."""
                row_pipeline = getattr(config_entry, "synapse_sync_fabric_pipeline_id", None) or (
                    config_entry.get("synapse_sync_fabric_pipeline_id") if isinstance(config_entry, dict) else None
                )
                row_ds_name = getattr(config_entry, "synapse_datasource_name", None) or (
                    config_entry.get("synapse_datasource_name") if isinstance(config_entry, dict) else None
                )
                row_ds_loc = getattr(config_entry, "synapse_datasource_location", None) or (
                    config_entry.get("synapse_datasource_location") if isinstance(config_entry, dict) else None
                )

                synapse_sync_fabric_pipeline_id = (row_pipeline or (default_pipeline_id or "")).strip()
                synapse_datasource_name = (row_ds_name or (default_ds_name or "")).strip()
                synapse_datasource_location = (row_ds_loc or (default_ds_loc or "")).strip()

                missing = []
                if not synapse_sync_fabric_pipeline_id:
                    missing.append("synapse_sync_fabric_pipeline_id")
                if not synapse_datasource_name:
                    missing.append("synapse_datasource_name")
                if not synapse_datasource_location:
                    missing.append("synapse_datasource_location")
                if missing:
                    raise ValueError(
                        "Missing required settings on WorkItem: "
                        + ", ".join(missing)
                        + ". Set them via config table overrides or varlib defaults."
                    )
                return synapse_sync_fabric_pipeline_id, synapse_datasource_name, synapse_datasource_location

            # Read synapse extract objects configuration
            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session found")
            
            config_table_uri = f"{self.lakehouse.lakehouse_tables_uri()}synapse_extract_objects"
            config_df = spark.read.format("delta").load(config_table_uri)
            
            # Filter active rows only
            active_objects = config_df.filter(F.col("active_yn") == "Y")

            # Capture defaults once; do not mutate these when rows provide overrides
            default_pipeline_id = synapse_sync_fabric_pipeline_id
            default_ds_name = synapse_datasource_name
            default_ds_loc = synapse_datasource_location
            
            if work_items_json:
                # parse JSON input for work items
                work_items_data = json.loads(work_items_json)
                for item in work_items_data:
                    # Resolve per‑item overrides; otherwise fall back to immutable defaults
                    resolved_pipeline_id, resolved_ds_name, resolved_ds_loc = _resolve_work_item_config(
                        item,
                        default_pipeline_id,
                        default_ds_name,
                        default_ds_loc,
                    )
                    work_items.append(WorkItem(
                        source_schema_name=item["source_schema_name"],
                        source_table_name=item["source_table_name"],
                        extract_mode=item.get("extract_mode", "snapshot"),
                        execution_group=item.get("execution_group", 1),
                        extract_start_dt=item.get("extract_start_dt"),
                        extract_end_dt=item.get("extract_end_dt"),
                        synapse_sync_fabric_pipeline_id=resolved_pipeline_id,  # per‑item resolved value
                        synapse_datasource_name=resolved_ds_name,
                        synapse_datasource_location=resolved_ds_loc,
                        synapse_connection_name=item.get("synapse_connection_name"),
                        export_base_dir=item.get("export_base_dir"),
                        trigger_type=item.get("trigger_type") or trigger_type,
                        master_execution_parameters=item.get("master_execution_parameters") or master_execution_parameters,
                    ))
            else:
                # Daily mode - prepare based on configuration
                objects_list = active_objects.collect()
                
                # Parse date range strings into dates for per-day expansion
                try:
                    start_dt = datetime.strptime(extraction_start_date, "%Y-%m-%d").date() if extraction_start_date else None
                    end_dt = datetime.strptime(extraction_end_date, "%Y-%m-%d").date() if extraction_end_date else None
                except Exception:
                    start_dt = end_dt = None
                
                for row in objects_list:
                    if not include_snapshots and row.extract_mode == "snapshot":
                        continue
                    
                    if row.extract_mode == "incremental" and start_dt and end_dt:
                        # Generate one work item per day (inclusive)
                        cur = start_dt
                        while cur <= end_dt:
                            day_str = cur.strftime("%Y-%m-%d")
                            # Resolve per‑item overrides; otherwise fall back to immutable defaults
                            resolved_pipeline_id, resolved_ds_name, resolved_ds_loc = _resolve_work_item_config(
                                row,
                                default_pipeline_id,
                                default_ds_name,
                                default_ds_loc,
                            )
                            work_items.append(WorkItem(
                                source_schema_name=row.source_schema_name,
                                source_table_name=row.source_table_name,
                                extract_mode=row.extract_mode,
                                execution_group=row.execution_group or 1,
                                extract_start_dt=day_str,
                                extract_end_dt=day_str,
                                single_date_filter=getattr(row, "single_date_filter", None),
                                date_range_filter=getattr(row, "date_range_filter", None),
                                custom_select_sql=getattr(row, "custom_select_sql", None),
                                synapse_sync_fabric_pipeline_id=resolved_pipeline_id,  # per‑item resolved value
                                synapse_connection_name=getattr(row, "synapse_connection_name", None),
                                synapse_datasource_name=resolved_ds_name,
                                synapse_datasource_location=resolved_ds_loc,
                                export_base_dir=getattr(row, "export_base_dir", None),
                                trigger_type=trigger_type,
                                master_execution_parameters=master_execution_parameters,
                            ))
                            cur += timedelta(days=1)
                    else:
                        # Snapshot or missing date range -> single item
                        # Resolve per‑item overrides; otherwise fall back to immutable defaults
                        resolved_pipeline_id, resolved_ds_name, resolved_ds_loc = _resolve_work_item_config(
                            row,
                            default_pipeline_id,
                            default_ds_name,
                            default_ds_loc,
                        )
                        work_items.append(WorkItem(
                            source_schema_name=row.source_schema_name,
                            source_table_name=row.source_table_name,
                            extract_mode=row.extract_mode,
                            execution_group=row.execution_group or 1,
                            extract_start_dt=extraction_start_date,
                            extract_end_dt=extraction_end_date,
                            single_date_filter=getattr(row, "single_date_filter", None),
                            date_range_filter=getattr(row, "date_range_filter", None),
                            custom_select_sql=getattr(row, "custom_select_sql", None),
                            synapse_sync_fabric_pipeline_id=resolved_pipeline_id,  # per‑item resolved value
                            synapse_connection_name=getattr(row, "synapse_connection_name", None),
                            synapse_datasource_name=resolved_ds_name,
                            synapse_datasource_location=resolved_ds_loc,
                            export_base_dir=getattr(row, "export_base_dir", None),
                            trigger_type=trigger_type,
                            master_execution_parameters=master_execution_parameters,
                        ))
            
            return work_items
            
        except Exception as e:
            logger.error(f"Error preparing work items: {e}")
            return []

    def get_pipeline_parameters(self, work_item: WorkItem, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Build pipeline parameters for a given work item.

        This is a minimal compatibility fallback used only when utils could not
        build CETAS-based parameters. When 'config' is provided and includes
        'synapse_export_shortcut_path_in_onelake', use it; otherwise fall back to
        a generic exports path.
        """
        base_path = None
        if config is not None:
            try:
                base_path = getattr(config, "synapse_export_shortcut_path_in_onelake", None)
            except Exception:
                base_path = None
        if not base_path:
            base_path = f"exports/{work_item.source_schema_name}/{work_item.source_table_name}"

        return {
            "script_content": f"SELECT * FROM [{work_item.source_schema_name}].[{work_item.source_table_name}]",
            "table_schema_and_name": f"{work_item.source_schema_name}_{work_item.source_table_name}",
            "parquet_file_path": base_path,
        }
