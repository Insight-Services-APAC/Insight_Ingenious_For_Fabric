#!/usr/bin/env python3
"""
Validate all table documentation files for consistency.
Reports validation errors and formatting issues.
"""

import sys
import yaml
from pathlib import Path

# Add the commands directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from table_documentation_model import TableDocumentation


def find_all_metadata_files(base_dir: str = "/workspaces/i4f/database-docs/tracking/tables"):
    """Find all metadata.yml files."""
    base_path = Path(base_dir)
    metadata_files = []
    
    for table_dir in base_path.iterdir():
        if table_dir.is_dir():
            metadata_file = table_dir / "metadata.yml"
            if metadata_file.exists():
                metadata_files.append(metadata_file)
    
    return sorted(metadata_files)


def main():
    metadata_files = find_all_metadata_files()
    print(f"Found {len(metadata_files)} metadata files to validate\n")
    
    total_errors = 0
    valid_count = 0
    error_count = 0
    
    for metadata_file in metadata_files:
        table_name = metadata_file.parent.name
        try:
            # Try to load with our model
            table_doc = TableDocumentation.from_yaml_file(str(metadata_file))
            errors = table_doc.validate()
            
            if errors:
                print(f"❌ {table_name}:")
                for error in errors:
                    print(f"   - {error}")
                print()
                error_count += 1
                total_errors += len(errors)
            else:
                print(f"✓ {table_name}")
                valid_count += 1
                
        except yaml.YAMLError as e:
            print(f"❌ {table_name}: YAML parsing error")
            print(f"   - {e}")
            print()
            error_count += 1
            total_errors += 1
            
        except Exception as e:
            print(f"❌ {table_name}: Structure error")
            print(f"   - {e}")
            print()
            error_count += 1
            total_errors += 1
    
    print(f"\nValidation Summary:")
    print(f"  ✓ Valid: {valid_count}")
    print(f"  ❌ Errors: {error_count}")
    print(f"  📊 Total issues: {total_errors}")
    
    if error_count > 0:
        print("\nTo fix issues, use: update-table-docs <table_name> --validate-only")
        return 1
    else:
        print("\n🎉 All documentation files are valid!")
        return 0


if __name__ == "__main__":
    sys.exit(main())