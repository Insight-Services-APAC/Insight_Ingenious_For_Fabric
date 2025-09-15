#!/usr/bin/env python3
"""
Update table documentation using structured data model.
Ensures consistent YAML formatting and validation.
"""
import argparse
import sys
from pathlib import Path

# Add the commands directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from table_documentation_model import TableDocumentation, create_table_documentation


def find_metadata_file(table_name: str, base_dir: str = "/workspaces/i4f/database-docs/tracking/tables") -> Path:
    """Find the metadata.yml file for a given table."""
    base_path = Path(base_dir)
    
    # Try exact match first
    exact_match = base_path / table_name / "metadata.yml"
    if exact_match.exists():
        return exact_match
    
    # Try without schema prefix
    if "." in table_name:
        schema, name = table_name.split(".", 1)
        no_schema_match = base_path / name / "metadata.yml"
        if no_schema_match.exists():
            return no_schema_match
    
    # Search for partial matches
    for table_dir in base_path.iterdir():
        if table_dir.is_dir():
            metadata_file = table_dir / "metadata.yml"
            if metadata_file.exists():
                if table_name.lower() in table_dir.name.lower():
                    return metadata_file
    
    raise FileNotFoundError(f"Could not find metadata file for table: {table_name}")


def main():
    parser = argparse.ArgumentParser(description="Update table documentation with structured validation")
    parser.add_argument("table_name", help="Table name (e.g., dbo.contact)")
    parser.add_argument("--d365-url", help="Dynamics 365 documentation URL")
    parser.add_argument("--dataverse-url", help="Dataverse documentation URL")
    parser.add_argument("--fo-url", help="Finance & Operations documentation URL")
    parser.add_argument("--notes", help="Verification notes")
    parser.add_argument("--additional-urls", nargs="*", help="Additional documentation URLs")
    parser.add_argument("--create", action="store_true", help="Create new metadata file if it doesn't exist")
    parser.add_argument("--validate-only", action="store_true", help="Only validate existing file without updating")
    
    args = parser.parse_args()
    
    try:
        metadata_path = find_metadata_file(args.table_name)
        print(f"Found metadata file: {metadata_path}")
        
        # Load existing documentation
        table_doc = TableDocumentation.from_yaml_file(str(metadata_path))
        print(f"Loaded documentation for: {table_doc.table_info.qualified_name}")
        
    except FileNotFoundError as e:
        if args.create:
            print(f"Creating new metadata file for: {args.table_name}")
            
            # Parse table name
            if "." in args.table_name:
                _, name = args.table_name.split(".", 1)
                schema = "dbo"
            else:
                schema, name = "dbo", args.table_name
            
            # Create new documentation
            table_doc = create_table_documentation(
                table_name=name,
                schema=schema,
                dataverse_logical_name=name if not name.startswith("cdm_") else name[4:],
                module_guess="Unknown"
            )
            
            # Create directory structure
            table_dir = Path("/workspaces/i4f/database-docs/tracking/tables") / args.table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            metadata_path = table_dir / "metadata.yml"
            
        else:
            print(f"Error: {e}")
            print("Use --create flag to create a new metadata file")
            return 1
    
    # Validate only mode
    if args.validate_only:
        errors = table_doc.validate()
        if errors:
            print("Validation errors:")
            for error in errors:
                print(f"  - {error}")
            return 1
        else:
            print("✓ Documentation is valid")
            return 0
    
    # Update documentation
    if any([args.d365_url, args.dataverse_url, args.fo_url, args.notes, args.additional_urls]):
        table_doc.update_documentation(
            d365_url=args.d365_url,
            dataverse_url=args.dataverse_url,
            fo_url=args.fo_url,
            verification_notes=args.notes,
            additional_urls=args.additional_urls
        )
        
        # Validate before saving
        errors = table_doc.validate()
        if errors:
            print("Validation errors:")
            for error in errors:
                print(f"  - {error}")
            return 1
        
        # Save updated documentation
        table_doc.save_to_file(str(metadata_path))
        print(f"✓ Updated documentation saved to: {metadata_path}")
        
        # Show what was updated
        doc = table_doc.documentation
        if args.d365_url:
            print(f"  D365 URL: {doc.d365_doc_url}")
        if args.dataverse_url:
            print(f"  Dataverse URL: {doc.dataverse_doc_url}")
        if args.fo_url:
            print(f"  F&O URL: {doc.fo_module_doc_url}")
        if doc.docs_verified == "verified":
            print(f"  Status: {doc.docs_verified} ({doc.last_verified})")
            
    else:
        print("No updates specified. Current documentation:")
        doc = table_doc.documentation
        print(f"  D365 URL: {doc.d365_doc_url or 'Not set'}")
        print(f"  Dataverse URL: {doc.dataverse_doc_url or 'Not set'}")
        print(f"  F&O URL: {doc.fo_module_doc_url or 'Not set'}")
        print(f"  Status: {doc.docs_verified}")
        if doc.last_verified:
            print(f"  Last verified: {doc.last_verified}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())