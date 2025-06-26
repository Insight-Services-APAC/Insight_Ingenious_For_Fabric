#!/usr/bin/env python3
"""
Test script to verify the dependency analysis and library injection functionality.
"""

from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator


def test_dependency_injection():
    """Test the library dependency injection functionality."""

    # Create a NotebookGenerator instance
    generator = NotebookGenerator(
        generation_mode=NotebookGenerator.GenerationMode.warehouse,
        output_mode=NotebookGenerator.OutputMode.local,
    )

    print("Testing dependency analysis and library injection...")

    # The library injection should have been called during initialization
    # Let's manually call it again to see the output
    generator.inject_python_libs_into_template()

    print("Dependency injection completed successfully!")


if __name__ == "__main__":
    test_dependency_injection()
