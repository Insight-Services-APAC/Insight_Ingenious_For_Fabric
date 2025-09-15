"""
Dynamic DAG Executor for dbt projects.

This module provides runtime DAG execution based on dynamically loaded
manifest dependencies.
"""

import logging
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from pyspark.sql import SparkSession

from .model_loader import DynamicModelLoader
from .sql_executor import DynamicSQLExecutor

# Try to import notebook output formatter
try:
    from IPython import get_ipython
    if get_ipython() is not None:
        from .notebook_output import NotebookOutputFormatter
        IN_NOTEBOOK = True
    else:
        from .notebook_output import SimpleOutputFormatter as NotebookOutputFormatter
        IN_NOTEBOOK = False
except ImportError:
    from .notebook_output import SimpleOutputFormatter as NotebookOutputFormatter
    IN_NOTEBOOK = False

logger = logging.getLogger(__name__)


class DynamicDAGExecutor:
    """
    Executes dbt DAG dynamically based on runtime-loaded dependencies.

    This class orchestrates the execution of dbt models, seeds, and tests
    by reading the dependency graph from manifest.json at runtime.
    """

    def __init__(
        self,
        spark: SparkSession,
        dbt_project_path: Path,
        max_workers: int = 4,
        cache_manifest: bool = True,
        verbose: bool = True,
        show_preview: bool = False
    ):
        """
        Initialize the DAG executor.

        Args:
            spark: Active SparkSession instance
            dbt_project_path: Path to the dbt project root
            max_workers: Maximum number of parallel workers
            cache_manifest: Whether to cache the manifest in memory
            verbose: Whether to show detailed output
            show_preview: Whether to show data previews in notebooks
        """
        self.spark = spark
        self.project_path = Path(dbt_project_path)
        self.max_workers = max_workers
        self.verbose = verbose
        self.show_preview = show_preview

        # Initialize loader and executor
        self.loader = DynamicModelLoader(self.project_path, cache_sql=cache_manifest)
        self.executor = DynamicSQLExecutor(spark, self.loader)

        # Initialize output formatter
        self.output_formatter = NotebookOutputFormatter(verbose=verbose)

        # Execution tracking
        self.execution_status: Dict[str, str] = {}  # pending, running, completed, failed
        self.execution_results: Dict[str, Any] = {}
        self.execution_times: Dict[str, float] = {}
        self.execution_errors: Dict[str, str] = {}

        # Build dependency graph
        self.dependencies = self.loader.build_dependency_graph()
        self._initialize_execution_status()

    def _initialize_execution_status(self) -> None:
        """Initialize execution status for all nodes."""
        # Mark sources as completed (they are assumed to exist)
        sources = self.loader.manifest.get("sources", {})
        for source_id in sources:
            self.execution_status[source_id] = "completed"

        # Mark executable nodes as pending
        for node_id in self.dependencies:
            if node_id not in self.execution_status:  # Don't override sources
                self.execution_status[node_id] = "pending"

    def validate_dag(self) -> Tuple[bool, List[str]]:
        """
        Validate that the dependency graph is a valid DAG (no cycles).

        Returns:
            Tuple of (is_valid, list_of_cycles_if_any)
        """
        visited = set()
        rec_stack = set()
        cycles = []

        def has_cycle(node_id: str, path: List[str]) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)

            for dep in self.dependencies.get(node_id, []):
                if dep not in visited:
                    if has_cycle(dep, path.copy()):
                        return True
                elif dep in rec_stack:
                    # Found cycle
                    cycle_start = path.index(dep)
                    cycle = path[cycle_start:] + [dep]
                    cycles.append(cycle)
                    return True

            rec_stack.remove(node_id)
            return False

        # Check all nodes
        for node in self.dependencies:
            if node not in visited:
                if has_cycle(node, []):
                    pass  # Cycle already recorded

        return len(cycles) == 0, cycles

    def get_ready_nodes(self) -> List[str]:
        """
        Get nodes that are ready to execute (all dependencies completed).

        Returns:
            List of node IDs ready for execution
        """
        ready = []

        for node_id, status in self.execution_status.items():
            if status != "pending":
                continue

            # Check if all dependencies are completed
            deps = self.dependencies.get(node_id, [])
            dependencies_ready = True

            for dep in deps:
                dep_status = self.execution_status.get(dep)
                if dep_status is None:
                    # Dependency not in execution status - check if it's a source
                    sources = self.loader.manifest.get("sources", {})
                    if dep in sources:
                        # Source exists, treat as completed
                        self.execution_status[dep] = "completed"
                    else:
                        # Unknown dependency, not ready
                        dependencies_ready = False
                        break
                elif dep_status != "completed":
                    # Dependency not completed yet
                    dependencies_ready = False
                    break

            if dependencies_ready:
                ready.append(node_id)

        return ready

    def execute_node(self, node_id: str) -> Any:
        """
        Execute a single node.

        Args:
            node_id: Node ID to execute

        Returns:
            Execution result
        """
        # Critical safeguard: Check if already executed/running/failed
        current_status = self.execution_status.get(node_id)
        if current_status in ["completed", "running", "failed"]:
            logger.warning(f"Attempted to execute {node_id} but status is {current_status}, skipping")
            return self.execution_results.get(node_id)

        # Check if this is a source - sources are not executed
        sources = self.loader.manifest.get("sources", {})
        if node_id in sources:
            logger.info(f"Skipping source (assumed to exist): {node_id}")
            self.execution_status[node_id] = "completed"
            return None

        start_time = time.time()
        self.execution_status[node_id] = "running"

        # Get node type for better display
        node_type = "model"
        if node_id.startswith("test."):
            node_type = "test"
        elif node_id.startswith("seed."):
            node_type = "seed"

        # Notify output formatter
        self.output_formatter.start_model(node_id, node_type)

        try:
            logger.info(f"Executing node: {node_id}")
            result = self.executor.execute_node(node_id)

            self.execution_status[node_id] = "completed"
            self.execution_results[node_id] = result
            self.execution_times[node_id] = time.time() - start_time

            # Get row count if available
            rows_affected = None
            if result is not None:
                try:
                    if hasattr(result, 'count'):
                        rows_affected = result.count()
                except:
                    pass

            # Notify output formatter
            self.output_formatter.complete_model(
                node_id, success=True, rows_affected=rows_affected
            )

            # Show preview if enabled and result is a DataFrame
            if self.show_preview and result is not None and IN_NOTEBOOK:
                try:
                    self.output_formatter.display_dataframe_preview(result, node_id)
                except:
                    pass

            logger.info(f"✓ Completed {node_id} in {self.execution_times[node_id]:.2f}s")
            return result

        except Exception as e:
            self.execution_status[node_id] = "failed"
            self.execution_errors[node_id] = str(e)
            self.execution_times[node_id] = time.time() - start_time

            # Notify output formatter
            self.output_formatter.complete_model(
                node_id, success=False, error=str(e)
            )

            logger.error(f"✗ Failed {node_id}: {e}")
            raise

    def execute_dag(
        self,
        target_nodes: Optional[List[str]] = None,
        exclude_nodes: Optional[List[str]] = None,
        fail_fast: bool = True,
        resource_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Execute the entire DAG or specific target nodes.

        Args:
            target_nodes: Specific nodes to execute (with dependencies)
            exclude_nodes: Nodes to exclude from execution
            fail_fast: Stop execution on first failure
            resource_types: Filter nodes by resource type (model, test, seed)

        Returns:
            Execution summary dictionary
        """
        start_time = time.time()
        executed = []
        failed = []
        skipped = []

        # Determine nodes to execute
        nodes_to_execute = self._determine_execution_set(
            target_nodes, exclude_nodes, resource_types
        )

        logger.info(f"Executing {len(nodes_to_execute)} nodes")

        # Reset status for nodes to execute
        for node_id in nodes_to_execute:
            self.execution_status[node_id] = "pending"

        # Get execution order for display
        execution_order = self._get_topological_order(nodes_to_execute)

        # Notify output formatter of execution start
        self.output_formatter.start_execution(
            total_models=len(nodes_to_execute),
            execution_order=execution_order
        )

        # Execute in topological order with parallelization
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            # Increase safety limit for complex DAGs with many dependencies
            # Tests can cause many iterations waiting for model dependencies
            max_iterations = max(len(nodes_to_execute) * 10, 1000)  # More generous limit
            iteration_count = 0
            stuck_detection_counter = 0
            last_progress_check = {"executed": 0, "failed": 0}

            # Stop flag that can be set by user interaction
            stop_execution = False

            while True:
                iteration_count += 1

                # Check for user stop request
                try:
                    # Check if stop button was clicked in notebook
                    from IPython.display import Javascript
                    if hasattr(Javascript, '_check_stop_execution'):
                        stop_execution = True
                        logger.info("User requested execution stop")
                        break
                except:
                    pass

                # Get ready nodes
                ready = [
                    node for node in self.get_ready_nodes()
                    if node in nodes_to_execute
                ]

                # Calculate waiting nodes (pending nodes that aren't ready due to dependencies)
                pending_nodes = [n for n in nodes_to_execute if self.execution_status.get(n) == "pending"]
                waiting_nodes = len([n for n in pending_nodes if n not in ready])

                # Update execution statistics
                if hasattr(self.output_formatter, 'update_execution_stats'):
                    # Count only futures that are actually running (not pending in ThreadPoolExecutor queue)
                    executing_count = len([f for f in futures.keys() if f.running()])
                    # Count futures that are queued but not yet running
                    queued_count = len([f for f in futures.keys() if not f.running() and not f.done()])

                    self.output_formatter.update_execution_stats(
                        waiting=waiting_nodes,
                        executing=executing_count,
                        queued=queued_count,
                        iteration=iteration_count,
                        max_iterations=max_iterations
                    )

                # Emergency safety check - but should never be hit with proper logic
                if iteration_count > max_iterations:
                    # Only stop if BOTH conditions are met:
                    # 1. We've exceeded the max iterations
                    # 2. No tasks are currently running
                    if not futures:
                        remaining_pending = len([n for n in nodes_to_execute
                                               if self.execution_status.get(n) == "pending"])
                        logger.error(f"EMERGENCY STOP: {max_iterations} iterations with no running tasks")
                        logger.error(f"This indicates a bug - {remaining_pending} nodes still pending")
                        break
                    # If futures exist, keep going regardless of iteration count

                # Check for progress stall (no new executions or failures)
                current_progress = {"executed": len(executed), "failed": len(failed)}
                if current_progress == last_progress_check:
                    stuck_detection_counter += 1
                    if stuck_detection_counter > 10:  # No progress for 10 iterations
                        logger.warning("No progress detected for 10 iterations, checking for stuck nodes")

                        # Find nodes that can never complete due to failed dependencies
                        stuck_nodes = self._find_stuck_nodes(nodes_to_execute, executed, failed)
                        if stuck_nodes:
                            logger.warning(f"Found {len(stuck_nodes)} stuck nodes due to failed dependencies")
                            for stuck_node, failed_deps in stuck_nodes.items():
                                self.execution_status[stuck_node] = "skipped"
                                skipped.append(stuck_node)
                                # Notify formatter about skipped model with failed dependencies
                                if hasattr(self.output_formatter, 'skip_model'):
                                    self.output_formatter.skip_model(stuck_node, failed_deps)
                                else:
                                    self.output_formatter.model_status[stuck_node] = "skipped"
                            # Reset counter and continue
                            stuck_detection_counter = 0
                        else:
                            # No stuck nodes found but no progress - must be done
                            break
                else:
                    last_progress_check = current_progress
                    stuck_detection_counter = 0

                if not ready and not futures:
                    # No more work to do
                    break

                # Submit ready nodes for execution
                for node_id in ready:
                    # Critical fix: Only submit if not already in futures AND not already executed/failed/running
                    current_status = self.execution_status.get(node_id)
                    # Check if node is already being executed (futures.values() contains node IDs)
                    already_submitted = node_id in futures.values()

                    if (not already_submitted and current_status == "pending"):
                        future = executor.submit(self.execute_node, node_id)
                        futures[future] = node_id
                    elif current_status != "pending":
                        # Debug: This should help identify why completed tasks are in ready list
                        logger.debug(f"Node {node_id} in ready list but status is {current_status}, skipping submission")
                    elif already_submitted:
                        logger.debug(f"Node {node_id} already submitted for execution, skipping")

                # Wait for at least one to complete
                if futures:
                    done, pending = self._wait_for_completion(futures, timeout=1.0)

                    for future in done:
                        node_id = futures[future]
                        try:
                            future.result()
                            executed.append(node_id)
                        except Exception as e:
                            failed.append(node_id)
                            if fail_fast:
                                # Cancel pending futures
                                for f in pending:
                                    f.cancel()
                                logger.error(f"Fail-fast triggered by {node_id}")
                                break
                        finally:
                            del futures[future]

                    if fail_fast and failed:
                        break

        # Mark remaining nodes as skipped
        for node_id in nodes_to_execute:
            if self.execution_status[node_id] == "pending":
                self.execution_status[node_id] = "skipped"
                skipped.append(node_id)
                # Notify formatter about skipped model
                if hasattr(self.output_formatter, 'skip_model'):
                    # Find which dependencies caused the skip
                    deps = self.dependencies.get(node_id, [])
                    failed_deps = []
                    incomplete_deps = []
                    all_deps_for_display = []

                    for dep in deps:
                        dep_status = self.execution_status.get(dep, "unknown")
                        if dep_status == "failed":
                            failed_deps.append(dep)
                        elif dep_status in ["pending", "skipped"]:
                            # This dependency was also not completed
                            incomplete_deps.append(dep)

                        # For display, show all direct dependencies if nothing specific failed
                        all_deps_for_display.append(dep)

                    # Priority: show failed deps, then incomplete deps, then all deps
                    if failed_deps:
                        skip_reason_deps = failed_deps[:5]
                    elif incomplete_deps:
                        skip_reason_deps = incomplete_deps[:5]
                    else:
                        # Show all direct dependencies if we can't determine specific cause
                        skip_reason_deps = all_deps_for_display[:5]

                    self.output_formatter.skip_model(node_id, skip_reason_deps if skip_reason_deps else None)
                else:
                    self.output_formatter.model_status[node_id] = "skipped"

        total_time = time.time() - start_time

        # Display final results
        self.output_formatter.display_results(
            success_count=len(executed),
            failed_count=len(failed),
            skipped_count=len(skipped)
        )

        return {
            "executed": executed,
            "failed": failed,
            "skipped": skipped,
            "total_nodes": len(nodes_to_execute),
            "success_rate": len(executed) / len(nodes_to_execute) if nodes_to_execute else 0,
            "total_time": total_time,
            "execution_times": self.execution_times,
            "errors": self.execution_errors
        }

    def _determine_execution_set(
        self,
        target_nodes: Optional[List[str]],
        exclude_nodes: Optional[List[str]],
        resource_types: Optional[List[str]]
    ) -> Set[str]:
        """
        Determine which nodes to execute based on filters.

        Args:
            target_nodes: Specific target nodes
            exclude_nodes: Nodes to exclude
            resource_types: Resource types to include

        Returns:
            Set of node IDs to execute
        """
        # Start with all executable nodes (exclude sources)
        sources = set(self.loader.manifest.get("sources", {}).keys())
        nodes = set(self.dependencies.keys()) - sources

        # Filter by resource type
        if resource_types:
            # Only include source type if explicitly requested
            if "source" not in resource_types:
                filtered = set()
                for rtype in resource_types:
                    filtered.update(self.loader.get_nodes_by_type(rtype))
                nodes &= filtered

        # If target nodes specified, include them and their dependencies
        if target_nodes:
            target_set = set()
            for target in target_nodes:
                # Only add target if it's not a source
                if target not in sources:
                    target_set.add(target)
                # Add all executable dependencies (excluding sources)
                deps = self._get_all_dependencies(target)
                target_set.update(deps - sources)
            nodes &= target_set

        # Exclude specified nodes
        if exclude_nodes:
            nodes -= set(exclude_nodes)

        return nodes

    def _get_all_dependencies(self, node_id: str) -> Set[str]:
        """
        Get all transitive dependencies for a node.

        Args:
            node_id: Node to get dependencies for

        Returns:
            Set of all dependency node IDs
        """
        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue

            visited.add(current)
            deps = self.dependencies.get(current, [])
            queue.extend(deps)

        return visited

    def _wait_for_completion(self, futures: dict, timeout: float = 1.0):
        """
        Wait for futures to complete with timeout.

        Args:
            futures: Dictionary of futures to wait for
            timeout: Timeout in seconds

        Returns:
            Tuple of (done, pending) futures
        """
        from concurrent.futures import wait, FIRST_COMPLETED

        if not futures:
            return set(), set()

        return wait(futures.keys(), timeout=timeout, return_when=FIRST_COMPLETED)

    def execute_parallel_batch(self, node_ids: List[str]) -> Dict[str, Any]:
        """
        Execute a batch of independent nodes in parallel.

        Args:
            node_ids: List of node IDs to execute in parallel

        Returns:
            Batch execution results
        """
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_node = {
                executor.submit(self.execute_node, node_id): node_id
                for node_id in node_ids
            }

            for future in as_completed(future_to_node):
                node_id = future_to_node[future]
                try:
                    result = future.result()
                    results[node_id] = {"status": "success", "result": result}
                except Exception as e:
                    results[node_id] = {"status": "failed", "error": str(e)}

        return results

    def get_execution_plan(
        self,
        target_nodes: Optional[List[str]] = None
    ) -> List[List[str]]:
        """
        Get the execution plan showing parallel batches.

        Args:
            target_nodes: Optional target nodes to plan for

        Returns:
            List of node ID batches that can run in parallel
        """
        if target_nodes:
            nodes = self._determine_execution_set(target_nodes, None, None)
            # Build sub-graph
            sub_graph = {
                node: [dep for dep in self.dependencies[node] if dep in nodes]
                for node in nodes
            }
        else:
            sub_graph = self.dependencies

        # Topological sort with level information
        in_degree = defaultdict(int)
        for node in sub_graph:
            for dep in sub_graph[node]:
                in_degree[dep] += 1

        # Find nodes with no dependencies
        queue = deque([node for node in sub_graph if in_degree[node] == 0])
        plan = []

        while queue:
            # All nodes in current queue can run in parallel
            current_batch = list(queue)
            plan.append(current_batch)

            queue.clear()
            for node in current_batch:
                # Find nodes that depend on current node
                for other_node, deps in sub_graph.items():
                    if node in deps:
                        in_degree[other_node] -= 1
                        if in_degree[other_node] == 0:
                            queue.append(other_node)

        return plan

    def reload_manifest(self) -> None:
        """Reload the manifest and rebuild dependency graph."""
        self.loader.reload_manifest()
        self.dependencies = self.loader.build_dependency_graph()
        self._initialize_execution_status()
        logger.info("Reloaded manifest and rebuilt dependency graph")

    def get_status_summary(self) -> Dict[str, int]:
        """
        Get summary of execution status.

        Returns:
            Dictionary with counts by status
        """
        summary = defaultdict(int)
        for status in self.execution_status.values():
            summary[status] += 1
        return dict(summary)

    def _get_topological_order(self, nodes: List[str]) -> List[str]:
        """
        Get topological ordering of nodes.

        Args:
            nodes: List of node IDs to order

        Returns:
            List of node IDs in topological order
        """
        # Build reverse dependency graph for sorting
        reverse_deps = defaultdict(set)
        in_degree = defaultdict(int)

        for node in nodes:
            deps = self.dependencies.get(node, [])
            for dep in deps:
                if dep in nodes:
                    reverse_deps[dep].add(node)
                    in_degree[node] += 1

        # Find nodes with no dependencies
        queue = deque([n for n in nodes if in_degree[n] == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Reduce in-degree for dependent nodes
            for dependent in reverse_deps[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def _find_stuck_nodes(
        self,
        nodes_to_execute: Set[str],
        executed: List[str],
        failed: List[str]
    ) -> Dict[str, List[str]]:
        """
        Find nodes that can never complete due to failed dependencies.

        Args:
            nodes_to_execute: Set of nodes being executed
            executed: List of successfully executed nodes
            failed: List of failed nodes

        Returns:
            Dict mapping stuck node IDs to their failed dependencies
        """
        stuck_nodes = {}
        completed_nodes = set(executed)
        failed_nodes = set(failed)

        for node_id in nodes_to_execute:
            if self.execution_status.get(node_id) == "pending":
                # Check if this node depends on any failed nodes
                deps = self.dependencies.get(node_id, [])
                failed_deps = [dep for dep in deps if dep in failed_nodes]

                if failed_deps:
                    # This node can never complete because it depends on failed nodes
                    stuck_nodes[node_id] = failed_deps
                    logger.debug(f"Node {node_id} is stuck due to failed dependencies: {failed_deps}")

        # Return dictionary for detailed tracking
        return stuck_nodes

    def reset_execution_state(self) -> None:
        """Reset all execution state."""
        self.execution_status.clear()
        self.execution_results.clear()
        self.execution_times.clear()
        self.execution_errors.clear()
        self._initialize_execution_status()
        logger.info("Reset execution state")