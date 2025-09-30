#!/usr/bin/env python3
"""
Example demonstrating the notebook utils abstraction.

This example shows how to use the abstraction to write code that works
both in local development environments and Fabric notebook environments.
"""

import os

import pandas as pd

from ingen_fab.python_libs.python.notebook_utils_abstraction import (
    NotebookUtilsFactory,
    get_notebook_utils,
)


def main():
    """Main example function."""
    print("=== Notebook Utils Abstraction Example ===\n")

    # Get notebook utils - automatically detects environment
    utils = get_notebook_utils()

    print(f"Environment detected: {'Fabric' if utils.__class__.__name__ == 'FabricNotebookUtils' else 'Local'}")
    print(f"Utils available: {utils.is_available()}\n")

    # Example 1: Display data
    print("1. Display example:")
    sample_data = pd.DataFrame(
        {
            "Name": ["Alice", "Bob", "Charlie"],
            "Age": [25, 30, 35],
            "City": ["New York", "London", "Tokyo"],
        }
    )
    utils.display(sample_data)
    print()

    # Example 2: Display non-DataFrame object
    print("2. Display simple object:")
    utils.display("Hello from the notebook utils abstraction!")
    print()

    # Example 3: Secret management (local environment)
    print("3. Secret management:")
    try:
        # Set a test secret in environment
        os.environ["SECRET_TEST_API_KEY"] = "test-api-key-123"
        secret = utils.get_secret("TEST_API_KEY", "test-vault")
        print(f"Retrieved secret: {secret}")
    except Exception as e:
        print(f"Secret retrieval failed: {e}")
    print()

    # Example 4: Connection example (would work in both environments)
    print("4. Connection example:")
    try:
        # This would connect to local SQL Server in local env
        # or to Fabric warehouse in Fabric env
        conn = utils.connect_to_artifact("warehouse-id", "workspace-id")
        print(f"Connection created: {type(conn)}")

        # Close connection if it has a close method
        if hasattr(conn, "close"):
            conn.close()
            print("Connection closed")
    except Exception as e:
        print(f"Connection failed: {e}")
    print()

    # Example 5: Force local environment
    print("5. Force local environment:")
    local_utils = NotebookUtilsFactory.create_instance(force_local=True)
    print(f"Forced local utils: {local_utils.__class__.__name__}")
    print(f"Local utils available: {local_utils.is_available()}")
    print()

    # Example 6: Notebook exit (safe in local environment)
    print("6. Notebook exit example:")
    utils.exit_notebook("Example completed successfully!")
    print("Note: In local environment, this just logs the exit value")


if __name__ == "__main__":
    main()
