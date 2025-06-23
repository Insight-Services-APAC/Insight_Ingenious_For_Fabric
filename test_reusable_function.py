#!/usr/bin/env python3
"""
Test script to verify the refactored notebook creation functionality.
"""

from pathlib import Path
from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator

def test_reusable_function():
    """Test the reusable notebook creation function."""
    
    # Create a NotebookGenerator instance
    generator = NotebookGenerator(
        generation_mode=NotebookGenerator.GenerationMode.warehouse,
        output_mode=NotebookGenerator.OutputMode.local
    )
    
    # Test the reusable function
    test_content = """# Test Notebook Content
print("Hello, World!")
"""
    
    test_output_dir = Path("test_output")
    test_output_dir.mkdir(exist_ok=True)
    
    result_path = generator.create_notebook_with_platform(
        notebook_name="test_notebook",
        rendered_content=test_content,
        output_dir=test_output_dir,
        relative_path="test_relative_path"
    )
    
    print(f"Successfully created notebook at: {result_path}")
    
    # Verify files were created
    notebook_file = result_path / "notebook-content.py"
    platform_file = result_path / ".platform"
    
    if notebook_file.exists():
        print("✓ notebook-content.py created successfully")
    else:
        print("✗ notebook-content.py not found")
        
    if platform_file.exists():
        print("✓ .platform file created successfully")
    else:
        print("✗ .platform file not found")
    
    # Clean up
    import shutil
    if test_output_dir.exists():
        shutil.rmtree(test_output_dir)
        print("✓ Test cleanup completed")

if __name__ == "__main__":
    test_reusable_function()
