# Testing pip Installation Process

[Home](../index.md) > [Developer Guide](index.md) > Testing pip Installation Process

This guide provides a comprehensive test plan for verifying the pip installation process of the `insight-ingenious-for-fabric` package.

## Prerequisites

- Python 3.12 or higher
- Access to the project source code
- Git for cloning the repository
- Internet connection (for installing dependencies)

## Test Environment Setup

The test should be performed in an isolated environment to ensure clean results. A `pip_install_test/` directory has been added to `.gitignore` for this purpose.

## Comprehensive Test Steps

### 1. Environment Setup

=== "macOS/Linux"

    ```bash
    # Check if we're already in pip_install_test directory or create it
    if [[ "$(basename "$PWD")" == "pip_install_test" ]]; then
        echo "✓ Already in pip_install_test directory"
    elif [[ -d "pip_install_test" ]]; then
        echo "✓ pip_install_test directory exists, entering it"
        cd pip_install_test
    else
        echo "Creating pip_install_test directory..."
        mkdir -p pip_install_test
        cd pip_install_test
    fi

    # Check if .venv_pip_test is already active
    if [[ "$VIRTUAL_ENV" == *".venv_pip_test"* ]]; then
        echo "✓ .venv_pip_test is already active"
    else
        echo "Creating and activating .venv_pip_test using uv..."
        # Create isolated Python 3.12 virtual environment using uv
        uv venv .venv_pip_test --python 3.12
        source .venv_pip_test/bin/activate
    fi

    # Verify clean environment
    pip list
    which python
    python --version
    ```

=== "Windows"

    ```powershell
    # Check if we're already in pip_install_test directory or create it
    if ((Split-Path -Leaf (Get-Location).Path) -eq "pip_install_test") {
        "✓ Already in pip_install_test directory"
    } elseif (Test-Path -PathType Container "pip_install_test") {
        "✓ pip_install_test directory exists, entering it"
        cd pip_install_test
    } else {
        "Creating pip_install_test directory..."
        New-Item -ItemType Directory -Force pip_install_test
        cd pip_install_test
    }

    # Check if .venv_pip_test is already active
    if ("$env:VIRTUAL_ENV" -like "*.venv_pip_test*") {
        "✓ .venv_pip_test is already active"
    } else {
        "Creating and activating .venv_pip_test using uv..."
        # Create isolated Python 3.12 virtual environment using uv
        uv venv .venv_pip_test --python 3.12
        .venv_pip_test\Scripts\Activate.ps1
    }

    # Verify clean environment
    pip list
    Get-Command python
    python --version
    ```

### 2. Test Standard Installation from Source

=== "macOS/Linux"

    ```bash
    # Copy source to test location
    cp -r /workspaces/ingen_fab test_standard_install
    cd test_standard_install

    # Build and install
    pip install --upgrade pip setuptools wheel
    pip install .

    # Verify installation
    pip show insight-ingenious-for-fabric
    pip list | grep insight-ingenious-for-fabric
    ```

=== "Windows"

    ```powershell
    # Copy source to test location
    Copy-Item -Recurse /workspaces/ingen_fab test_standard_install
    cd test_standard_install

    # Build and install
    pip install --upgrade pip setuptools wheel
    pip install .

    # Verify installation
    pip show insight-ingenious-for-fabric
    pip list | Select-String insight-ingenious-for-fabric
    ```

### 3. Test CLI Entry Point

=== "macOS/Linux"

    ```bash
    # Test CLI is available
    which ingen_fab
    ingen_fab --help
    ingen_fab --version

    # Test each command group
    ingen_fab init --help
    ingen_fab deploy --help
    ingen_fab ddl --help
    ingen_fab test --help
    ingen_fab notebook --help
    ```

=== "Windows"

    ```powershell
    # Test CLI is available
    Get-Command ingen_fab
    ingen_fab --help
    ingen_fab --version

    # Test each command group
    ingen_fab init --help
    ingen_fab deploy --help
    ingen_fab ddl --help
    ingen_fab test --help
    ingen_fab notebook --help
    ```

### 4. Test Package Data Inclusion

Create a test script to verify all package data is included:

```python
# test_package_data.py
import os
from pathlib import Path
import ingen_fab

# Find package location
package_dir = Path(ingen_fab.__file__).parent

# Check for critical package data
data_paths = [
    "ddl_scripts/_templates",
    "notebook_utils/templates",
    "project_templates",
    "python_libs/python/sql_template_factory",
]

for path in data_paths:
    full_path = package_dir / path
    if full_path.exists():
        print(f"✓ Found: {path}")
        # List some files
        files = list(full_path.rglob("*"))[:3]
        for f in files:
            if f.is_file():
                print(f"  - {f.relative_to(package_dir)}")
    else:
        print(f"✗ Missing: {path}")
```

### 5. Test Editable Installation

=== "macOS/Linux"

    ```bash
    # Clean previous installation
    cd ..
    rm -rf test_standard_install
    pip uninstall -y insight-ingenious-for-fabric

    # Test editable install
    cp -r /workspaces/ingen_fab test_editable_install
    cd test_editable_install
    pip install -e .

    # Verify editable installation
    pip show insight-ingenious-for-fabric | grep Location
    pip list | grep insight-ingenious-for-fabric
    ```

=== "Windows"

    ```powershell
    # Clean previous installation
    cd ..
    Remove-Item -Recurse -Force test_standard_install
    pip uninstall -y insight-ingenious-for-fabric

    # Test editable install
    Copy-Item -Recurse /workspaces/ingen_fab test_editable_install
    cd test_editable_install
    pip install -e .

    # Verify editable installation
    pip show insight-ingenious-for-fabric | Select-String Location
    pip list | Select-String insight-ingenious-for-fabric
    ```

### 6. Test Installation with Extras

```bash
# Test with dev extras
pip install -e ".[dev]"
pip list | grep -E "(pytest|ruff|pre-commit)"

# Test with all extras
pip uninstall -y insight-ingenious-for-fabric
pip install -e ".[dev,docs,tests,dataprep]"
pip list | grep -E "(mkdocs|pytest|pandas)"
```

### 7. Test GitHub Dependency

The project includes a GitHub-hosted dependency. Verify it installs correctly:

```bash
# Verify fabric_cicd installation
pip list | grep fabric-cicd
python -c "import fabric_cicd; print(fabric_cicd.__version__)"
```

### 8. Test Build Artifacts

=== "macOS/Linux"

    ```bash
    # Build distribution packages
    pip install build
    python -m build

    # Check generated artifacts
    ls -la dist/
    tar -tzf dist/*.tar.gz | head -20
    unzip -l dist/*.whl | head -20
    ```

=== "Windows"

    ```powershell
    # Build distribution packages
    pip install build
    python -m build

    # Check generated artifacts
    Get-ChildItem -Force dist\
    tar -tzf (Get-Item dist\*.tar.gz).FullName | Select-Object -First 20
    python -m zipfile -l (Get-Item dist\*.whl).FullName | Select-Object -First 20
    ```

### 9. Functional Tests

=== "macOS/Linux"

    ```bash
    # Set required environment variables
    export FABRIC_ENVIRONMENT="local"
    export FABRIC_WORKSPACE_REPO_DIR="sample_project"

    # Test basic CLI functionality
    cd sample_project
    ingen_fab ddl compile --help
    ingen_fab test local --help

    # Test notebook template generation
    mkdir test_output
    ingen_fab ddl compile --output-mode local_file_system --output-path ../test_output --generation-mode Lakehouse
    ls -la ../test_output/
    ```

=== "Windows"

    ```powershell
    # Set required environment variables
    $env:FABRIC_ENVIRONMENT = "local"
    $env:FABRIC_WORKSPACE_REPO_DIR = "sample_project"

    # Test basic CLI functionality
    cd sample_project
    ingen_fab ddl compile --help
    ingen_fab test local --help

    # Test notebook template generation
    New-Item -ItemType Directory test_output
    ingen_fab ddl compile --output-mode local_file_system --output-path ../test_output --generation-mode Lakehouse
    Get-ChildItem -Force ..\test_output\
    ```

### 10. Import Tests

Test that all critical modules can be imported:

```python
# test_imports.py
try:
    import ingen_fab
    print("✓ Base package import")
    
    from ingen_fab.cli import app
    print("✓ CLI import")
    
    from ingen_fab.notebook_utils import SimpleNotebook
    print("✓ Notebook utils import")
    
    from ingen_fab.python_libs.python import lakehouse_utils
    print("✓ Python libs import")
    
    from ingen_fab.fabric_api import utils
    print("✓ Fabric API import")
    
except ImportError as e:
    print(f"✗ Import failed: {e}")
```

## Expected Results

### Success Criteria

- [ ] Package installs without errors
- [ ] CLI command `ingen_fab` is available in PATH
- [ ] All command groups accessible via CLI
- [ ] Package data (templates, configs) included
- [ ] Editable installation works correctly
- [ ] All extras install their dependencies
- [ ] GitHub-hosted dependency installs
- [ ] Build artifacts generated correctly
- [ ] Basic functionality works with proper env vars
- [ ] All critical imports succeed

### Common Issues

1. **Python Version**: Ensure Python 3.12+ is used (project requirement)
2. **GitHub Dependency**: May fail without internet or GitHub access
3. **Package Data**: Verify all Jinja templates and config files are included
4. **Entry Points**: CLI should be available immediately after installation

## Cleanup

=== "macOS/Linux"

    ```bash
    deactivate
    cd ../..
    rm -rf pip_install_test
    ```

=== "Windows"

    ```powershell
    deactivate
    cd ..\..
    Remove-Item -Recurse -Force pip_install_test
    ```

## Automation Script

For repeated testing, you can create an automation script that runs all these tests and reports results. This can be integrated into CI/CD pipelines for continuous validation of the installation process.