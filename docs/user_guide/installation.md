# Installation

[Home](../index.md) > [User Guide](index.md) > Installation

This guide will help you install the Ingenious Fabric Accelerator using pip.

## Requirements

Before installing, ensure your system meets these requirements:

- **Visual Studio Code**: Latest version 
- **Python 3.12 or higher**
- **pip** (comes with Python)
- **Microsoft Fabric workspace** (for deployment)
- **Azure CLI** (optional, for authentication)
- **ODBC Driver for SQL Server 18**: For Fabric lakehouse connectivity
- **Git**: For version control and repository management

## Installation

The instructions below assume that you are installing Ingenious in an empty directory and are in a Powershell Terminal in Visual Studio Code.

Note that the installations from Github below all show an installation from *main*. The recommended approach is to install from the latest release. See Ingenious [Releases](https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric/releases) - append the release number to the github path, for example @v1.0

### Standard Installation

Install the Ingenious Fabric Accelerator directly from pip:

=== "macOS/Linux"

    ```bash
    # Install the package (when available in PyPI)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git 
    ```

=== "Windows"

    ```powershell
    # Install the package (when available in PyPI)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

### Virtual Environment (Recommended)

It's recommended to use a virtual environment to avoid conflicts with other packages:

=== "macOS/Linux"

    ```bash
    # Create a virtual environment
    python -m venv fabric-env
    source fabric-env/bin/activate

    # Install the package (when available in PyPI)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

=== "Windows"

    ```powershell
    # Create a virtual environment
    python -m venv fabric-env
    fabric-env\Scripts\activate

    # Install the package (when available in PyPI)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

### Installing with Optional Dependencies

```bash
# Install with all optional dependencies (when available in PyPI)
pip install "insight-ingenious-for-fabric[dbt,dataprep]"
```

### Installing dbt adapter

If you are going to develop dbt models, you will need to install Insight's dbt adapter:

```bash
# Install with dbt support
pip install git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNB.git
```

## Environment Setup

### Environment Variables

After installation, configure your environment variables for the CLI:
```bash
# Set environment (development, UAT, production)
$env:FABRIC_ENVIRONMENT = "development"

# Set workspace directory 
$env:FABRIC_WORKSPACE_REPO_DIR = "dp"
```
The above assumes your working in a project "dp" in the development environment.

### Shell Configuration (Optional)

Add convenience aliases to your shell profile:

=== "macOS/Linux"

    ```bash
    # Add to ~/.bashrc or ~/.zshrc
    # Ensure pip packages are in PATH
    export PATH="$PATH:$HOME/.local/bin"

    # Create convenient aliases
    alias ifab="ingen_fab"
    alias ifab-help="ingen_fab --help"
    ```

=== "Windows"

    ```powershell
    # Add to PowerShell profile ($PROFILE)
    # Ensure pip packages are in PATH
    $env:PATH += ";$env:LOCALAPPDATA\Programs\Python\Python312\Scripts"

    # Create convenient aliases
    Set-Alias ifab ingen_fab
    Set-Alias ifab-help "ingen_fab --help"
    ```

## Verification

Verify your installation:

```bash
# Check the installation
pip show insight-ingenious-for-fabric

# Display CLI help
ingen_fab --help

```

Expected output ingen_fab --help:
```
Usage: ingen_fab [OPTIONS] COMMAND [ARGS]...

Options:
  --fabric-workspace-repo-dir  -fwd  Directory containing fabric workspace repository files
  --fabric-environment         -fe   The name of your fabric environment
  --help                            Show this message and exit.

Commands:
  deploy    Commands for deploying to environments and managing workspace items
  init      Commands for initializing solutions and projects
  ddl       Commands for compiling DDL notebooks
  test      Commands for testing notebooks and Python blocks
  notebook  Commands for managing and scanning notebook content
  package   Commands for running extension packages
  libs      Commands for compiling and managing Python libraries
  dbt       Commands for dbt integration
```

Expected output pip show insight-ingenious-for-fabric:
```

Name: insight-ingenious-for-fabric
Version: 0.1.0
Summary: Accelerator for building Microsoft Fabric Applications
Home-page:
Author:
Author-email:
License:
Location: C:\source\test\Ingenious_Test\fabric-env\Lib\site-packages
Requires: azure-cli, azure-identity, azure-storage-blob, azure-storage-file-datalake, deltalake, fabric_cicd, faker, jinja2, lazy-import, pandas, psycopg2-binary, pyarrow, pyodbc, requests, sqlglot, typer
Required-by:
```

## Getting Started

### Create Your First Project

Now that you have the CLI installed, create your first project:

=== "macOS/Linux"

    ```bash
    # Create a new project
    ingen_fab init new --project-name "dp"

    # Or create with sample configurations
    ingen_fab init new --project-name "dp" --with-samples

    # Set environment variables
    export FABRIC_WORKSPACE_REPO_DIR="dp"
    export FABRIC_ENVIRONMENT="development"
    ```

=== "Windows"

    ```powershell
    # Create a new project
    ingen_fab init new --project-name "dp"

    # Or create with sample configurations
    ingen_fab init new --project-name "dp" --with-samples

    # Set environment variables
    $env:FABRIC_WORKSPACE_REPO_DIR = "dp"
    $env:FABRIC_ENVIRONMENT = "development"
    ```

### Set Up Azure Authentication

For deploying to Fabric, set up authentication:

=== "macOS/Linux"

    ```bash
    # Option 1: Use Azure CLI (interactive)
    az login

    # Option 2: Use Service Principal (automated)
    export AZURE_TENANT_ID="your-tenant-id"
    export AZURE_CLIENT_ID="your-client-id"
    export AZURE_CLIENT_SECRET="your-client-secret"
    ```

=== "Windows"

    ```powershell
    # Option 1: Use Azure CLI (interactive)
    az login

    # Option 2: Use Service Principal (automated)
    $env:AZURE_TENANT_ID = "your-tenant-id"
    $env:AZURE_CLIENT_ID = "your-client-id"
    $env:AZURE_CLIENT_SECRET = "your-client-secret"
    ```

## Troubleshooting

### Common Issues

#### Command not found: ingen_fab

If the command is not recognized after installation:

```bash
# Check if the package is installed (assumes grep is installed)
pip list | grep insight-ingenious-for-fabric

# Check if the package is installed (without grep)
pip list

# Ensure pip scripts are in PATH
export PATH="$PATH:$HOME/.local/bin"
```

#### Permission Errors

If you encounter permission errors during installation, do an install for current user only:

```bash
# Install for current user only (when available in PyPI)
pip install --user insight-ingenious-for-fabric

```

Or Create a virtual environment (recommended)

=== "macOS/Linux"

    ```bash
    # Create a virtual environment
    python -m venv fabric-env
    source fabric-env/bin/activate

    # Install the package (when available in PyPI)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

=== "Windows"

    ```powershell
    # Create a virtual environment
    python -m venv fabric-env
    fabric-env\Scripts\activate

    # Install the package (when available in PyPI)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

#### Python Version Issues

Ensure you have Python 3.12 (Recommended) or higher:

```bash
python --version
# Should show Python 3.12.x

# If not, install Python 3.12
# On Ubuntu/Debian:
sudo apt update && sudo apt install python3.12

# On macOS with Homebrew:
brew install python@3.12

# On Windows:
# Download from https://www.python.org/downloads/
```

Ensure you have Python 3.12 (Recommended):

#### SSL Certificate Issues

If you revieved a SSL certificate error during installation, try installing using --native-tls flag :

```powershell

    # OInstall from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git --native-tls
    
```
#### Pre Release Error

If you revieved a an error indicating a package with pre-release marker, error during installation, try installing using --prerelease=allow flag :

```powershell

    # OInstall from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git --prerelease=allow
    
```

### Platform-Specific Notes

#### Windows
- Use PowerShell or Command Prompt as Administrator for global installation
- Virtual environment activation: `fabric-env\Scripts\activate`
- Add Python Scripts to PATH: Settings → System → Advanced → Environment Variables

#### macOS
- May need to use `pip3` instead of `pip`
- Install Command Line Tools if needed: `xcode-select --install`
- Consider using Homebrew for Python: `brew install python@3.12`

#### Linux
- May need to use `pip3` instead of `pip`
- Install pip if not available: `sudo apt install python3-pip`
- May need to add `~/.local/bin` to PATH

## Updating

To update to the latest version:

```bash
# Update the package (when available in PyPI)
pip install --upgrade insight-ingenious-for-fabric

# Or update from GitHub
pip install --upgrade git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
```

## Uninstalling

To remove the package:

```bash
# Uninstall the package (when available in PyPI)
pip uninstall insight-ingenious-for-fabric

# If using a virtual environment, you can just delete it
deactivate
rm -rf fabric-env  # On Windows: rmdir /s fabric-env
```

## Next Steps

Once installed, you can:

1. **[Get started quickly](quick_start.md)** with your first project
2. **[Learn the CLI commands](cli_reference.md)** available
3. **[Explore examples](../examples/index.md)** to see real-world usage
4. **[Follow workflows](workflows.md)** for best practices

## Developer Installation

If you need to contribute to the project or modify the source code, see the [Developer Guide](../developer_guide/index.md) for instructions on cloning the repository and setting up a development environment.

## Support

- **Documentation**: This guide and related documentation
- **Issues**: [GitHub Issues](https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric/issues)
- **CLI Help**: Run `ingen_fab --help` or `ingen_fab COMMAND --help`