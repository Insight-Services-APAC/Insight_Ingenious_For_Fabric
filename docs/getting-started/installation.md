# Installation

[Home](../index.md) > [Getting Started](installation.md) > Installation

This guide will help you install and set up the Ingenious Fabric Accelerator using pip.

!!! tip "Learning Path"
    **New User Journey**: Installation (you are here) → [Quick Start](quick-start.md) → [First Project](first-project.md) → [CLI Reference](../guides/cli-reference.md)

## Requirements

Before installing, ensure your system meets these requirements:

- **Python 3.12 or higher**
- **pip** (comes with Python)
- **Microsoft Fabric workspace** (for deployment)
- **Azure CLI** (optional, for authentication)

## Quick Installation

--8<-- "_includes/pip_install.md"

### Installing with Optional Dependencies

```bash
# Install with dbt support
pip install "insight-ingenious-for-fabric[dbt]"

# Install with all optional dependencies
pip install "insight-ingenious-for-fabric[dbt,dataprep]"
```

## Recommended Setup

### 1. Create Virtual Environment

=== "macOS/Linux"

    ```bash
    # Create and activate a virtual environment
    python -m venv fabric-env
    source fabric-env/bin/activate

    # Install the package
    pip install insight-ingenious-for-fabric
    ```

=== "Windows"

    ```powershell
    # Create and activate a virtual environment
    python -m venv fabric-env
    fabric-env\Scripts\activate

    # Install the package
    pip install insight-ingenious-for-fabric
    ```

### 2. Set Environment Variables

--8<-- "_includes/environment_setup.md"

### 3. Verify Installation

```bash
# Check the installation
pip show insight-ingenious-for-fabric

# Display CLI help
ingen_fab --help

# Check version
ingen_fab --version
```

Expected output:
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

## Platform-Specific Instructions

=== "macOS/Linux"

    ```bash
    # Ensure Python 3.12+
    python3 --version

    # Create virtual environment
    python3 -m venv fabric-env
    source fabric-env/bin/activate

    # Install package
    pip install insight-ingenious-for-fabric

    # Set environment variables
    export FABRIC_WORKSPACE_REPO_DIR="."
    export FABRIC_ENVIRONMENT="development"
    ```

=== "Windows"

    ```powershell
    # Create virtual environment
    python -m venv fabric-env
    fabric-env\Scripts\activate

    # Install package
    pip install insight-ingenious-for-fabric

    # Set environment variables (PowerShell)
    $env:FABRIC_WORKSPACE_REPO_DIR = "."
    $env:FABRIC_ENVIRONMENT = "development"
    ```

## Shell Configuration (Optional)

Add convenience aliases to your shell profile for faster access:

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

## Authentication Setup

### Azure CLI (Recommended)

```bash
# Install Azure CLI if needed
# See: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli

# Login to Azure
az login

# Verify access
az account show
```

### Service Principal (CI/CD)

=== "macOS/Linux"

    ```bash
    # Set service principal credentials
    export AZURE_TENANT_ID="your-tenant-id"
    export AZURE_CLIENT_ID="your-client-id"
    export AZURE_CLIENT_SECRET="your-client-secret"
    ```

=== "Windows"

    ```powershell
    # Set service principal credentials
    $env:AZURE_TENANT_ID = "your-tenant-id"
    $env:AZURE_CLIENT_ID = "your-client-id"
    $env:AZURE_CLIENT_SECRET = "your-client-secret"
    ```

## Troubleshooting

### Command Not Found

If `ingen_fab` is not recognized:

=== "macOS/Linux"

    ```bash
    # Ensure pip scripts are in PATH
    export PATH="$PATH:$HOME/.local/bin"

    # Or run as module
    python -m ingen_fab --help
    ```

=== "Windows"

    ```powershell
    # Ensure pip scripts are in PATH
    $env:PATH += ";$env:LOCALAPPDATA\Programs\Python\Python312\Scripts"

    # Or run as module
    python -m ingen_fab --help
    ```

### Permission Errors

=== "macOS/Linux"

    ```bash
    # Install for current user
    pip install --user insight-ingenious-for-fabric

    # Or use virtual environment (recommended)
    python -m venv fabric-env
    source fabric-env/bin/activate
    pip install insight-ingenious-for-fabric
    ```

=== "Windows"

    ```powershell
    # Install for current user
    pip install --user insight-ingenious-for-fabric

    # Or use virtual environment (recommended)
    python -m venv fabric-env
    fabric-env\Scripts\activate
    pip install insight-ingenious-for-fabric
    ```

### Python Version Issues

=== "macOS"

    ```bash
    # Check Python version
    python3 --version

    # Install Python 3.12+ if needed (using Homebrew)
    brew install python@3.12

    # Or download from python.org
    # https://www.python.org/downloads/macos/
    ```

=== "Linux"

    ```bash
    # Check Python version
    python3 --version

    # Install Python 3.12+ if needed
    # Ubuntu/Debian:
    sudo apt update && sudo apt install python3.12

    # RHEL/CentOS/Fedora:
    sudo dnf install python3.12

    # Or build from source
    ```

=== "Windows"

    ```powershell
    # Check Python version
    python --version

    # Install Python 3.12+ if needed
    # Download installer from:
    # https://www.python.org/downloads/windows/

    # Or use winget:
    winget install Python.Python.3.12
    ```

### Platform-Specific Notes

=== "macOS"

    - May need to use `pip3` instead of `pip`
    - Install Command Line Tools if needed: `xcode-select --install`
    - Consider using Homebrew for Python: `brew install python@3.12`
    - Ensure `~/.local/bin` is in your PATH

=== "Linux"

    - May need to use `pip3` instead of `pip`
    - Install pip if not available: `sudo apt install python3-pip`
    - May need to add `~/.local/bin` to PATH
    - On some distributions, use python3.12 explicitly

=== "Windows"

    - Use PowerShell or Command Prompt as Administrator for global installation
    - Virtual environment activation: `fabric-env\Scripts\activate`
    - Add Python Scripts to PATH: Settings → System → Advanced → Environment Variables
    - Consider using Windows Terminal for better CLI experience

## Updating

To update to the latest version:

```bash
# Update the package
pip install --upgrade insight-ingenious-for-fabric

# Or update from GitHub
pip install --upgrade git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
```

## Uninstalling

To remove the package:

```bash
# Uninstall the package
pip uninstall insight-ingenious-for-fabric

# If using a virtual environment, you can just delete it
deactivate
rm -rf fabric-env  # On Windows: rmdir /s fabric-env
```

## Next Steps

Once installed, proceed to:

1. **[Quick Start](quick-start.md)** - Create your first project
2. **[First Project](first-project.md)** - Detailed walkthrough
3. **[CLI Reference](../guides/cli-reference.md)** - Learn all commands

!!! tip "Learning Path"
    **Continue Learning**: Next, follow the [Quick Start](quick-start.md) guide to create your first project and explore the core features.

## Developer Installation

Need to modify the source code? See the [Developer Guide](../developer_guide/index.md#development-setup) for repository setup instructions.

## Support

- **Documentation**: This guide and related documentation
- **Issues**: [GitHub Issues](https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric/issues)
- **CLI Help**: Run `ingen_fab --help` or `ingen_fab COMMAND --help`
