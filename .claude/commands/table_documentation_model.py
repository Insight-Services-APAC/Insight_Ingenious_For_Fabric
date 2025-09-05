"""
Data model for table documentation metadata structure.
Provides consistent schema for table documentation files.
"""

import yaml
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Dict, Any, List


@dataclass
class TableInfo:
    """Core table information."""
    name: str
    schema: str
    qualified_name: str
    dataverse_logical_name: Optional[str] = None
    fabric_prefix_family: Optional[str] = None
    module_guess: Optional[str] = None
    standard_or_custom: str = "standard"
    evidence: Optional[str] = None


@dataclass
class TablePaths:
    """Path information for the table."""
    onelake_relative_path: str
    warehouse_schema: str
    warehouse_table: str
    warehouse_qualified_name: str


@dataclass
class Documentation:
    """Documentation URLs and verification status."""
    d365_doc_url: Optional[str] = None
    dataverse_doc_url: Optional[str] = None
    fo_module_doc_url: Optional[str] = None
    docs_verified: str = "unverified"
    verification_notes: Optional[str] = None
    last_verified: Optional[str] = None
    additional_urls: Optional[List[str]] = field(default_factory=list)


@dataclass
class TableDocumentation:
    """Complete table documentation structure."""
    table_info: TableInfo
    paths: TablePaths
    documentation: Documentation
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        return asdict(self)
    
    def to_yaml(self) -> str:
        """Convert to YAML string with proper formatting."""
        data = self.to_dict()
        return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableDocumentation':
        """Create instance from dictionary."""
        table_info = TableInfo(**data['table_info'])
        paths = TablePaths(**data['paths'])
        documentation = Documentation(**data['documentation'])
        return cls(table_info=table_info, paths=paths, documentation=documentation)
    
    @classmethod
    def from_yaml_file(cls, file_path: str) -> 'TableDocumentation':
        """Load from YAML file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)
    
    def save_to_file(self, file_path: str) -> None:
        """Save to YAML file with consistent formatting."""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(self.to_yaml())
    
    def update_documentation(self, 
                           d365_url: Optional[str] = None,
                           dataverse_url: Optional[str] = None, 
                           fo_url: Optional[str] = None,
                           verification_notes: Optional[str] = None,
                           additional_urls: Optional[List[str]] = None) -> None:
        """Update documentation URLs and mark as verified."""
        if d365_url:
            self.documentation.d365_doc_url = d365_url
        if dataverse_url:
            self.documentation.dataverse_doc_url = dataverse_url
        if fo_url:
            self.documentation.fo_module_doc_url = fo_url
        if verification_notes:
            self.documentation.verification_notes = verification_notes
        if additional_urls:
            self.documentation.additional_urls = additional_urls
            
        # Mark as verified and update timestamp
        self.documentation.docs_verified = "verified"
        self.documentation.last_verified = datetime.now().strftime('%Y-%m-%d')
    
    def validate(self) -> List[str]:
        """Validate the documentation structure and return any errors."""
        errors = []
        
        # Required fields
        if not self.table_info.name:
            errors.append("table_info.name is required")
        if not self.table_info.schema:
            errors.append("table_info.schema is required")
        if not self.table_info.qualified_name:
            errors.append("table_info.qualified_name is required")
            
        # Path validation
        if not self.paths.onelake_relative_path:
            errors.append("paths.onelake_relative_path is required")
        if not self.paths.warehouse_qualified_name:
            errors.append("paths.warehouse_qualified_name is required")
            
        # URL validation (basic)
        urls = [
            self.documentation.d365_doc_url,
            self.documentation.dataverse_doc_url, 
            self.documentation.fo_module_doc_url
        ]
        
        for url in urls:
            if url and not (url.startswith('http://') or url.startswith('https://')):
                errors.append(f"Invalid URL format: {url}")
                
        return errors


def create_table_documentation(table_name: str, 
                              schema: str = "dbo",
                              dataverse_logical_name: Optional[str] = None,
                              module_guess: Optional[str] = None) -> TableDocumentation:
    """Create a new TableDocumentation instance with sensible defaults."""
    
    qualified_name = f"{schema}.{table_name}"
    
    table_info = TableInfo(
        name=table_name,
        schema=schema,
        qualified_name=qualified_name,
        dataverse_logical_name=dataverse_logical_name or table_name,
        module_guess=module_guess
    )
    
    paths = TablePaths(
        onelake_relative_path=f"Tables/{table_name}",
        warehouse_schema=schema,
        warehouse_table=table_name,
        warehouse_qualified_name=qualified_name
    )
    
    documentation = Documentation()
    
    return TableDocumentation(
        table_info=table_info,
        paths=paths,
        documentation=documentation
    )