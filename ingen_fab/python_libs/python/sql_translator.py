"""
SQL Translation Layer using SQLGlot for dialect conversion.

This module provides a unified interface for SQL translation from Fabric SQL
to other dialects (primarily PostgreSQL for local development).
"""

import logging
from typing import Any, Dict, List, Optional

import sqlglot
from sqlglot import transpile
from sqlglot.dialects import Dialect

from ingen_fab.python_libs.common.config_utils import get_configs_as_object

logger = logging.getLogger(__name__)


class SQLTranslatorError(Exception):
    """Exception raised when SQL translation fails."""
    pass


class FabricSQLTranslator:
    """
    Translates Fabric SQL to other dialects using SQLGlot.
    
    This class handles the translation of SQL statements from Microsoft Fabric
    SQL Server syntax to other database dialects, primarily PostgreSQL for
    local development.
    """
    
    # Mapping of local environment to target SQL dialect
    DIALECT_MAPPING = {
        "local": "postgres",
        "development": "fabric", 
        "test": "fabric",
        "production": "fabric"
    }
    
    # Custom translation rules for Fabric-specific patterns
    FABRIC_TO_POSTGRES_MAPPINGS = {
        # Information schema differences
        "INFORMATION_SCHEMA.SCHEMATA": "information_schema.schemata",
        "INFORMATION_SCHEMA.TABLES": "information_schema.tables", 
        "INFORMATION_SCHEMA.COLUMNS": "information_schema.columns",
        
        # SQL Server specific functions that need postgres equivalents
        "SCHEMA_NAME": "schema_name",
        "TABLE_SCHEMA": "table_schema",
        "TABLE_NAME": "table_name",
        "COLUMN_NAME": "column_name",
        "DATA_TYPE": "data_type",
    }
    
    def __init__(self, target_dialect: Optional[str] = None):
        """
        Initialize the SQL translator.
        
        Args:
            target_dialect: Target SQL dialect. If None, will be determined
                          from the fabric_environment configuration.
        """
        self.target_dialect = target_dialect or self._get_target_dialect()
        self.source_dialect = "fabric"  # Microsoft Fabric SQL dialect
        
    def _get_target_dialect(self) -> str:
        """Determine target dialect based on fabric_environment."""
        try:
            config = get_configs_as_object()
            environment = getattr(config, 'fabric_environment', 'production')
            return self.DIALECT_MAPPING.get(environment, "fabric")
        except Exception as e:
            logger.warning(f"Could not determine environment, defaulting to fabric: {e}")
            return "fabric"
    
    def translate_sql(self, sql: str, **kwargs) -> str:
        """
        Translate SQL from Fabric dialect to target dialect.
        
        Args:
            sql: SQL statement to translate
            **kwargs: Additional parameters for translation
            
        Returns:
            Translated SQL statement
            
        Raises:
            SQLTranslatorError: If translation fails
        """
        if self.target_dialect == "fabric":
            # No translation needed for Fabric environments
            return sql
            
        try:
            # Apply pre-translation mappings for Fabric-specific patterns
            translated_sql = self._apply_pre_translation_mappings(sql)
            
            # Use SQLGlot to translate
            result = transpile(
                translated_sql,
                read=self.source_dialect,
                write=self.target_dialect,
                pretty=True
            )
            
            if not result:
                raise SQLTranslatorError(f"SQLGlot returned empty result for SQL: {sql}")
            
            # Handle multiple statements
            if len(result) > 1:
                # Multiple statements - translate each and join with semicolons
                translated_statements = []
                for stmt in result:
                    fixed_stmt = self._apply_post_translation_fixes(stmt)
                    # Add semicolon if not present
                    if not fixed_stmt.rstrip().endswith(';'):
                        fixed_stmt = fixed_stmt.rstrip() + ';'
                    translated_statements.append(fixed_stmt)
                final_sql = '\n\n'.join(translated_statements)
            else:
                # Single statement
                final_sql = result[0]
                # Apply post-translation fixes
                final_sql = self._apply_post_translation_fixes(final_sql)
            
            logger.debug(f"Translated SQL from {self.source_dialect} to {self.target_dialect}:")
            logger.debug(f"Original: {sql}")
            logger.debug(f"Translated: {final_sql}")
            
            return final_sql
            
        except Exception as e:
            logger.error(f"Failed to translate SQL: {sql}")
            logger.error(f"Error: {str(e)}")
            raise SQLTranslatorError(f"SQL translation failed: {str(e)}") from e
    
    def _apply_pre_translation_mappings(self, sql: str) -> str:
        """Apply custom mappings before SQLGlot translation."""
        result = sql
        for fabric_pattern, postgres_pattern in self.FABRIC_TO_POSTGRES_MAPPINGS.items():
            result = result.replace(fabric_pattern, postgres_pattern)
            
        import re
        
        
        
        # Handle Fabric-specific PRIMARY KEY NONCLUSTERED ... NOT ENFORCED syntax
        if 'PRIMARY KEY NONCLUSTERED' in result.upper():
            # Convert: PRIMARY KEY NONCLUSTERED (column) NOT ENFORCED 
            # To:      PRIMARY KEY (column)
            pk_pattern = r'PRIMARY\s+KEY\s+NONCLUSTERED\s*\(([^)]+)\)\s*NOT\s+ENFORCED'
            result = re.sub(pk_pattern, r'PRIMARY KEY (\1)', result, flags=re.IGNORECASE)
        
        # Handle DROP CONSTRAINT IF EXISTS (convert to PostgreSQL syntax)
        if 'DROP CONSTRAINT IF EXISTS' in result.upper():
            # PostgreSQL uses slightly different syntax
            drop_pattern = r'ALTER\s+TABLE\s+([^\s]+)\s+DROP\s+CONSTRAINT\s+IF\s+EXISTS\s+([^\s;]+)'
            result = re.sub(drop_pattern, r'ALTER TABLE \1 DROP CONSTRAINT IF EXISTS \2', result, flags=re.IGNORECASE)
            
        return result
    
    def _apply_post_translation_fixes(self, sql: str) -> str:
        """Apply fixes after SQLGlot translation for known issues."""
        result = sql
        
        # Common post-translation fixes for PostgreSQL
        if self.target_dialect == "postgres":
            # Fix case sensitivity issues
            result = result.replace('"schema_name"', 'schema_name')
            result = result.replace('"table_name"', 'table_name') 
            result = result.replace('"column_name"', 'column_name')
            result = result.replace('"data_type"', 'data_type')
            
            # Handle INFORMATION_SCHEMA differences
            result = result.replace('INFORMATION_SCHEMA', 'information_schema')
            
            # Handle SQL Server bracket notation [schema].[table] -> schema.table
            import re
            bracket_pattern = r'\[([^\]]+)\]'
            result = re.sub(bracket_pattern, r'\1', result)
            
            # Remove NULLS FIRST/NULLS LAST from PRIMARY KEY constraints (PostgreSQL doesn't support this)
            if 'PRIMARY KEY' in result.upper():
                result = re.sub(r'\s+NULLS\s+(FIRST|LAST)', '', result, flags=re.IGNORECASE)
            
            # Handle SQL Server specific data types  
            result = re.sub(r'\bDATETIME2\(\d+\)', 'TIMESTAMP', result, flags=re.IGNORECASE)
            result = re.sub(r'\bTIMESTAMP\(\d+\)', 'TIMESTAMP', result, flags=re.IGNORECASE)
            result = re.sub(r'\bBYTEA\(\d+\)', 'BYTEA', result, flags=re.IGNORECASE)
            # Convert BIT to SMALLINT instead of BOOLEAN to maintain compatibility
            # This allows 0/1 values to work in both Fabric and PostgreSQL without conversion
            result = re.sub(r'\bBIT\b', 'SMALLINT', result, flags=re.IGNORECASE)
            
            # Handle SQL Server specific commands
            if result.strip().startswith('EXEC sp_rename'):
                # Convert SQL Server sp_rename to PostgreSQL ALTER TABLE RENAME
                rename_pattern = r"EXEC sp_rename '([^.]+)\.([^']+)', '([^']+)'"
                match = re.search(rename_pattern, result)
                if match:
                    schema_name, old_table_name, new_table_name = match.groups()
                    result = f"ALTER TABLE {schema_name}.{old_table_name} RENAME TO {new_table_name}"
            
            # Handle empty or comment-only SQL (like vacuum_table template)
            stripped = result.strip()
            if not stripped or stripped.startswith('--'):
                # For PostgreSQL, we can use a valid no-op statement
                result = "SELECT 1 AS no_op_result"
                

            
        return result
    
    def translate_batch(self, sql_statements: List[str], **kwargs) -> List[str]:
        """
        Translate multiple SQL statements.
        
        Args:
            sql_statements: List of SQL statements to translate
            **kwargs: Additional parameters for translation
            
        Returns:
            List of translated SQL statements
        """
        return [self.translate_sql(sql, **kwargs) for sql in sql_statements]
    
    def validate_translation(self, original_sql: str, translated_sql: str) -> bool:
        """
        Validate that the translated SQL is syntactically correct.
        
        Args:
            original_sql: Original SQL statement
            translated_sql: Translated SQL statement
            
        Returns:
            True if translation appears valid, False otherwise
        """
        try:
            # Parse the translated SQL to check for syntax errors
            sqlglot.parse(translated_sql, dialect=self.target_dialect)
            return True
        except Exception as e:
            logger.warning(f"Translation validation failed: {e}")
            logger.warning(f"Original: {original_sql}")
            logger.warning(f"Translated: {translated_sql}")
            return False


# Factory function for easy instantiation
def get_sql_translator(target_dialect: Optional[str] = None) -> FabricSQLTranslator:
    """
    Factory function to create a SQL translator instance.
    
    Args:
        target_dialect: Target SQL dialect. If None, will be determined
                      from the fabric_environment configuration.
                      
    Returns:
        FabricSQLTranslator instance
    """
    return FabricSQLTranslator(target_dialect=target_dialect)