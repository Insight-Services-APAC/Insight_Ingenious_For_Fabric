"""
Dynamic SQL Executor for dbt projects.

This module provides runtime execution of dbt SQL statements
loaded dynamically from JSON files.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession

from .model_loader import DynamicModelLoader

logger = logging.getLogger(__name__)


class DynamicSQLExecutor:
    """
    Executes dbt SQL statements dynamically at runtime.

    This class works with DynamicModelLoader to execute SQL statements
    without requiring static code generation.
    """

    def __init__(self, spark: SparkSession, loader: DynamicModelLoader):
        """
        Initialize the SQL executor.

        Args:
            spark: Active SparkSession instance
            loader: DynamicModelLoader instance for accessing SQL and metadata
        """
        self.spark = spark
        self.loader = loader
        self.execution_history: List[Dict[str, Any]] = []

    def execute_node(
        self,
        node_id: str,
        execute_python_first: bool = True,
        return_last_result: bool = True
    ) -> Union[List[Optional[DataFrame]], Optional[DataFrame]]:
        """
        Execute all SQL statements for a given node.

        Args:
            node_id: The unique identifier of the node to execute
            execute_python_first: If True and Python code exists, execute it before SQL
            return_last_result: If True, return only the last result; otherwise return all

        Returns:
            Either the last DataFrame result or a list of all results
        """
        start_time = time.time()
        results = []
        errors = []

        try:
            # Get node metadata
            metadata = self.loader.get_node_metadata(node_id)
            logger.info(f"Executing node: {node_id} ({metadata.get('resource_type', 'unknown')})")

            # Check for Python model and execute if exists
            if execute_python_first:
                python_code = self.loader.get_python_model(node_id)
                if python_code:
                    result = self._execute_python_model(node_id, python_code)
                    results.append(result)
                    logger.info(f"Executed Python model for {node_id}")

            # Get and execute SQL statements
            sql_statements = self.loader.get_sql_statements(node_id)

            for i, sql in enumerate(sql_statements, 1):
                try:
                    result = self._execute_sql_statement(sql, node_id, i)
                    results.append(result)
                    logger.info(f"Executed SQL statement {i}/{len(sql_statements)} for {node_id}")
                except Exception as e:
                    error_msg = f"Failed to execute statement {i} for {node_id}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    raise RuntimeError(error_msg)

            # Record execution history
            execution_time = time.time() - start_time
            self.execution_history.append({
                "node_id": node_id,
                "status": "success",
                "execution_time": execution_time,
                "statement_count": len(sql_statements),
                "timestamp": time.time(),
                "metadata": metadata
            })

            logger.info(f"Successfully executed {node_id} in {execution_time:.2f}s")

            if return_last_result:
                return results[-1] if results else None
            return results

        except Exception as e:
            execution_time = time.time() - start_time
            self.execution_history.append({
                "node_id": node_id,
                "status": "failed",
                "execution_time": execution_time,
                "error": str(e),
                "timestamp": time.time(),
                "errors": errors
            })
            raise

    def _execute_sql_statement(
        self,
        sql: str,
        node_id: str,
        statement_num: int
    ) -> Optional[DataFrame]:
        """
        Execute a single SQL statement.

        Args:
            sql: SQL statement to execute
            node_id: Node identifier for logging
            statement_num: Statement number for logging

        Returns:
            DataFrame result if applicable, None otherwise
        """
        # Split by semicolon but be careful with strings
        statements = self._split_sql_statements(sql)

        result = None
        for statement in statements:
            statement = statement.strip()
            if not statement:
                continue

            try:
                result = self.spark.sql(statement)
                # Force evaluation for DDL statements
                if self._is_ddl_statement(statement):
                    result.collect()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to execute statement {statement_num} in {node_id}: {e}\n"
                    f"SQL: {statement[:500]}..."
                )

        return result

    def _execute_python_model(self, node_id: str, python_code: str) -> Any:
        """
        Execute Python model code.

        Args:
            node_id: Node identifier for logging
            python_code: Python code to execute

        Returns:
            Result of Python code execution
        """
        # Replace spark references with self.spark
        if 'self.spark' not in python_code:
            python_code = re.sub(r'\bspark\.', 'self.spark.', python_code)
            python_code = re.sub(r'\bspark\b(?!\.)', 'self.spark', python_code)

        # Create execution context
        exec_context = {
            'self': self,
            'spark': self.spark,
            'logger': logger,
            'node_id': node_id
        }

        try:
            exec(python_code, exec_context)
            return exec_context.get('result', None)
        except Exception as e:
            raise RuntimeError(f"Failed to execute Python model for {node_id}: {e}")

    def _split_sql_statements(self, sql: str) -> List[str]:
        """
        Split SQL string into individual statements.

        This handles quoted strings and comments properly.

        Args:
            sql: SQL string potentially containing multiple statements

        Returns:
            List of individual SQL statements
        """
        # Simple implementation - can be enhanced for complex cases
        statements = []
        current = []
        in_string = False
        string_char = None

        i = 0
        while i < len(sql):
            char = sql[i]

            # Handle string literals
            if char in ('"', "'") and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Handle statement separator
            if char == ';' and not in_string:
                if current:
                    statements.append(''.join(current).strip())
                    current = []
                i += 1
                continue

            current.append(char)
            i += 1

        # Add remaining statement
        if current:
            final_statement = ''.join(current).strip()
            if final_statement:
                statements.append(final_statement)

        return statements

    def _is_ddl_statement(self, sql: str) -> bool:
        """
        Check if SQL statement is a DDL statement.

        Args:
            sql: SQL statement to check

        Returns:
            True if DDL statement, False otherwise
        """
        ddl_keywords = [
            'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
            'RENAME', 'COMMENT', 'GRANT', 'REVOKE'
        ]

        sql_upper = sql.upper().strip()
        return any(sql_upper.startswith(keyword) for keyword in ddl_keywords)

    def execute_batch(
        self,
        node_ids: List[str],
        parallel: bool = False,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Execute multiple nodes in batch.

        Args:
            node_ids: List of node IDs to execute
            parallel: Whether to execute in parallel (if dependencies allow)
            max_workers: Maximum number of parallel workers

        Returns:
            Dictionary with execution results and statistics
        """
        results = {}
        failed = []
        succeeded = []

        if parallel:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_node = {
                    executor.submit(self.execute_node, node_id): node_id
                    for node_id in node_ids
                }

                for future in as_completed(future_to_node):
                    node_id = future_to_node[future]
                    try:
                        result = future.result()
                        results[node_id] = result
                        succeeded.append(node_id)
                        logger.info(f"Successfully executed {node_id}")
                    except Exception as e:
                        failed.append(node_id)
                        results[node_id] = {"error": str(e)}
                        logger.error(f"Failed to execute {node_id}: {e}")
        else:
            for node_id in node_ids:
                try:
                    result = self.execute_node(node_id)
                    results[node_id] = result
                    succeeded.append(node_id)
                    logger.info(f"Successfully executed {node_id}")
                except Exception as e:
                    failed.append(node_id)
                    results[node_id] = {"error": str(e)}
                    logger.error(f"Failed to execute {node_id}: {e}")

        return {
            "results": results,
            "succeeded": succeeded,
            "failed": failed,
            "total": len(node_ids),
            "success_rate": len(succeeded) / len(node_ids) if node_ids else 0
        }

    def get_execution_history(self) -> List[Dict[str, Any]]:
        """
        Get execution history.

        Returns:
            List of execution history records
        """
        return self.execution_history

    def clear_history(self) -> None:
        """Clear execution history."""
        self.execution_history.clear()
        logger.info("Cleared execution history")

    def validate_node(self, node_id: str) -> Dict[str, Any]:
        """
        Validate that a node can be executed.

        Args:
            node_id: Node ID to validate

        Returns:
            Validation result dictionary
        """
        validation = {
            "node_id": node_id,
            "valid": True,
            "issues": [],
            "warnings": []
        }

        try:
            # Check if node exists in manifest
            node = self.loader.get_node(node_id)
            if not node:
                validation["valid"] = False
                validation["issues"].append("Node not found in manifest")
                return validation

            # Check if SQL file exists
            try:
                sql_statements = self.loader.get_sql_statements(node_id)
                validation["sql_count"] = len(sql_statements)
            except FileNotFoundError:
                validation["valid"] = False
                validation["issues"].append("SQL file not found")
                return validation

            # Check dependencies
            dependencies = self.loader.get_dependencies(node_id)
            validation["dependencies"] = dependencies
            validation["dependency_count"] = len(dependencies)

            # Validate SQL syntax (basic check)
            for i, sql in enumerate(sql_statements, 1):
                if not sql.strip():
                    validation["warnings"].append(f"Statement {i} is empty")

            # Check for Python model
            python_model = self.loader.get_python_model(node_id)
            validation["has_python_model"] = python_model is not None

        except Exception as e:
            validation["valid"] = False
            validation["issues"].append(f"Validation error: {e}")

        return validation