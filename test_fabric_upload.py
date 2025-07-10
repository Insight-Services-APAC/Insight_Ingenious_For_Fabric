#!/usr/bin/env python3
"""
Test script for uploading python_libs to Fabric config lakehouse.
"""

from pathlib import Path
from ingen_fab.az_cli.onelake_utils import OneLakeUtils
import traceback

def main():
    """Test the upload functionality."""
    
    # Initialize the Fabric API utils with environment
    # Replace 'development' with your actual environment name
    environment = "development_jr"
    project_path = Path("sample_project")
    
    try:
        onelake_utils = OneLakeUtils(environment=environment, project_path=project_path)
        
        print(f"Initialized OneLakeUtils for environment: {environment}")
        print(f"Workspace ID: {onelake_utils.workspace_id}")
        
        # Get config lakehouse ID
        config_lakehouse_id = onelake_utils.get_config_lakehouse_id()
        print(f"Config lakehouse ID: {config_lakehouse_id}")
        
        # Upload python_libs directory to config lakehouse
        print("\nStarting upload of python_libs to config lakehouse...")
        results = onelake_utils.upload_python_libs_to_config_lakehouse()
        
        print(f"\nUpload completed!")
        print(f"Total files processed: {results['total_files']}")
        print(f"Successful uploads: {len(results['successful'])}")
        print(f"Failed uploads: {len(results['failed'])}")
        
        if results['successful']:
            print("\nSuccessful uploads:")
            for result in results['successful'][:5]:  # Show first 5
                print(f"  ✓ {result['local_path']} -> {result['remote_path']}")
            if len(results['successful']) > 5:
                print(f"  ... and {len(results['successful']) - 5} more")
        
        if results['failed']:
            print("\nFailed uploads:")
            for result in results['failed']:
                print(f"  ✗ {result['local_path']}: {result['error']}")
                
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Stack trace:")
        print(traceback.format_exc())
        print("Make sure you're authenticated with Azure and have the correct permissions.")

if __name__ == "__main__":
    main()