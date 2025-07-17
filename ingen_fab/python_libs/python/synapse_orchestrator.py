"""
Synapse Sync Orchestrator - Simplified and modular pipeline orchestration.

This module provides a clean separation of concerns for Synapse data extraction:
- Pipeline orchestration using PipelineUtils
- Logging and status tracking
- Asynchronous execution with proper error handling
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from ingen_fab.python_libs.python.synapse_extract_utils import SynapseExtractUtils

logger = logging.getLogger(__name__)


class SynapseOrchestrator:
    """
    Orchestrates Synapse data extraction pipelines with proper error handling and monitoring.
    """
    
    def __init__(
        self,
        synapse_extract_utils: SynapseExtractUtils,
        workspace_id: str,
        pipeline_id: str,
        max_concurrency: int = 10
    ):
        """
        Initialize the orchestrator.
        
        Args:
            synapse_extract_utils: Utility instance for Synapse operations
            workspace_id: Fabric workspace ID
            pipeline_id: Pipeline ID for extractions
            max_concurrency: Maximum concurrent pipeline executions
        """
        self.synapse_utils = synapse_extract_utils
        self.workspace_id = workspace_id
        self.pipeline_id = pipeline_id
        self.max_concurrency = max_concurrency
        self.concurrency_limiter = asyncio.Semaphore(max_concurrency)
    
    async def process_single_extraction(
        self,
        extract_item: Dict[str, Any],
        execution_id: str,
        master_execution_id: str
    ) -> tuple[bool, str, str, Optional[str]]:
        """
        Process a single data extraction with comprehensive error handling.
        
        Args:
            extract_item: Dictionary containing extraction details
            execution_id: Unique execution ID for this extraction
            master_execution_id: Master execution ID linking related extractions
            
        Returns:
            Tuple of (success, table_info, status, error_message)
        """
        table_info = f"{extract_item['source_schema']}.{extract_item['source_table']}"
        
        async with self.concurrency_limiter:
            start_time = time.monotonic()
            
            try:
                # Update status to "Claimed"
                await self._update_status(
                    execution_id=execution_id,
                    master_execution_id=master_execution_id,
                    status="Claimed"
                )
                
                logger.info(f"Processing {table_info} - Triggering pipeline...")
                
                # Trigger pipeline using PipelineUtils
                job_id = await self.synapse_utils.trigger_pipeline_extraction(
                    workspace_id=self.workspace_id,
                    pipeline_id=self.pipeline_id,
                    payload=extract_item['pipeline_payload'],
                    table_name=table_info
                )
                
                # Update status to "Running"
                await self._update_status(
                    execution_id=execution_id,
                    master_execution_id=master_execution_id,
                    status="Running",
                    pipeline_job_id=job_id
                )
                
                logger.info(f"Pipeline triggered for {table_info} - Job ID: {job_id}")
                
                # Poll pipeline until completion
                final_status = await self.synapse_utils.poll_pipeline_to_completion(
                    workspace_id=self.workspace_id,
                    pipeline_id=self.pipeline_id,
                    job_id=job_id,
                    table_name=table_info
                )
                
                # Calculate duration
                duration_sec = time.monotonic() - start_time
                
                # Update final status
                await self._update_status(
                    execution_id=execution_id,
                    master_execution_id=master_execution_id,
                    status=final_status,
                    duration_sec=duration_sec,
                    output_path=extract_item.get("output_path") if final_status in {"Completed", "Deduped"} else None,
                    extract_file_name=extract_item.get("extract_file_name") if final_status in {"Completed", "Deduped"} else None,
                    external_table=extract_item.get("external_table") if final_status in {"Completed", "Deduped"} else None
                )
                
                success = final_status in {"Completed", "Deduped"}
                
                if success:
                    logger.info(f"✅ {table_info} completed successfully - Duration: {duration_sec:.2f}s")
                else:
                    logger.warning(f"⚠️ {table_info} finished with status: {final_status} - Duration: {duration_sec:.2f}s")
                
                return (success, table_info, final_status, None)
                
            except Exception as exc:
                duration_sec = time.monotonic() - start_time
                error_message = str(exc)
                
                # Update status to "Failed"
                await self._update_status(
                    execution_id=execution_id,
                    master_execution_id=master_execution_id,
                    status="Failed",
                    duration_sec=duration_sec,
                    error=error_message
                )
                
                logger.error(f"❌ {table_info} failed - Duration: {duration_sec:.2f}s - Error: {error_message}")
                return (False, table_info, "Failed", error_message)
    
    async def _update_status(
        self,
        execution_id: str,
        master_execution_id: str,
        status: str,
        duration_sec: Optional[float] = None,
        pipeline_job_id: Optional[str] = None,
        output_path: Optional[str] = None,
        extract_file_name: Optional[str] = None,
        external_table: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """Helper method to update extraction status."""
        try:
            # Note: This would need to be adapted to work with the actual lakehouse utils
            # For now, we'll create a simplified version
            pass  # TODO: Implement status update using lakehouse utils
        except Exception as exc:
            logger.error(f"Failed to update status: {exc}")
    
    async def orchestrate_extractions(
        self,
        extraction_payloads: List[Dict[str, Any]],
        master_execution_id: str,
        master_execution_parameters: Dict[str, Any],
        trigger_type: str = "Manual"
    ) -> Dict[str, Any]:
        """
        Orchestrate multiple extractions with proper grouping and error handling.
        
        Args:
            extraction_payloads: List of extraction payloads
            master_execution_id: Master execution ID
            master_execution_parameters: Parameters for the execution
            trigger_type: Type of trigger (Manual/Scheduled)
            
        Returns:
            Execution summary with statistics
        """
        logger.info(f"Starting orchestration - Master ID: {master_execution_id}")
        logger.info(f"Total extractions: {len(extraction_payloads)}")
        logger.info(f"Max concurrency: {self.max_concurrency}")
        
        # Pre-log all extractions as "Queued"
        execution_id_map = await self._bulk_insert_queued_extracts(
            extraction_payloads=extraction_payloads,
            master_execution_id=master_execution_id,
            master_execution_parameters=master_execution_parameters,
            trigger_type=trigger_type
        )
        
        # Group by execution_group for sequential processing
        grouped_payloads = self._group_by_execution_group(extraction_payloads)
        execution_groups = sorted(grouped_payloads.keys())
        
        all_results = []
        
        # Process each execution group sequentially
        for group in execution_groups:
            group_payloads = grouped_payloads[group]
            logger.info(f"Processing execution group {group} ({len(group_payloads)} extractions)")
            
            # Process all extractions in this group concurrently
            group_tasks = [
                self.process_single_extraction(
                    extract_item=payload,
                    execution_id=execution_id_map[payload['external_table']],
                    master_execution_id=master_execution_id
                )
                for payload in group_payloads
            ]
            
            group_results = await asyncio.gather(*group_tasks, return_exceptions=False)
            all_results.extend(group_results)
            
            # Log group summary
            group_success = sum(1 for result in group_results if result[0])
            group_failed = len(group_results) - group_success
            logger.info(f"Group {group} completed: {group_success} successful, {group_failed} failed")
        
        # Generate final summary
        return self._generate_summary(all_results, master_execution_id, execution_groups)
    
    def _group_by_execution_group(self, extraction_payloads: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        """Group extraction payloads by execution_group."""
        grouped = {}
        for payload in extraction_payloads:
            group = payload.get('execution_group', float('inf'))
            if group not in grouped:
                grouped[group] = []
            grouped[group].append(payload)
        return grouped
    
    async def _bulk_insert_queued_extracts(
        self,
        extraction_payloads: List[Dict[str, Any]],
        master_execution_id: str,
        master_execution_parameters: Dict[str, Any],
        trigger_type: str
    ) -> Dict[str, str]:
        """Pre-log all extractions as 'Queued' and return execution ID mapping."""
        logger.info(f"Pre-logging {len(extraction_payloads)} extractions as 'Queued'...")
        
        # Generate execution IDs for all extractions
        execution_id_map = {}
        for payload in extraction_payloads:
            execution_id = str(uuid.uuid4())
            execution_id_map[payload['external_table']] = execution_id
        
        # TODO: Implement bulk insert using lakehouse utils
        # For now, return the mapping
        return execution_id_map
    
    def _generate_summary(
        self,
        all_results: List[tuple],
        master_execution_id: str,
        execution_groups: List[int]
    ) -> Dict[str, Any]:
        """Generate execution summary statistics."""
        total_extractions = len(all_results)
        successful_extractions = sum(1 for result in all_results if result[0])
        failed_extractions = total_extractions - successful_extractions
        skipped_extractions = sum(1 for result in all_results if result[2] == "Skipped")
        
        failed_details = [
            {"table": result[1], "status": result[2], "error": result[3]}
            for result in all_results if not result[0]
        ]
        
        return {
            "master_execution_id": master_execution_id,
            "total_tables": total_extractions,
            "successful_extractions": successful_extractions,
            "failed_extractions": failed_extractions,
            "skipped_extractions": skipped_extractions,
            "success_rate": f"{(successful_extractions/total_extractions)*100:.1f}%" if total_extractions > 0 else "N/A",
            "execution_groups": execution_groups,
            "failed_details": failed_details,
            "completion_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }