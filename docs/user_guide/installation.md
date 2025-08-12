# Installation

[Home](../index.md) > [User Guide](index.md) > Installation

This guide will help you install the Ingenious Fabric Accelerator using pip.

## Requirements

Before installing, ensure your system meets these requirements:

- **Python 3.12 or higher**
- **pip** (comes with Python)
- **Microsoft Fabric workspace** (for deployment)
- **Azure CLI** (optional, for authentication)

## Installation

### Standard Installation

Install the Ingenious Fabric Accelerator directly from pip:

--8<-- "_includes/pip_install.md"

### Virtual Environment (Recommended)

It's recommended to use a virtual environment to avoid conflicts with other packages:

=== "macOS/Linux"

    ```bash
    # Create a virtual environment
    python -m venv fabric-env
    source fabric-env/bin/activate

    # Install the package
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

=== "Windows"

    ```powershell
    # Create a virtual environment
    python -m venv fabric-env
    fabric-env\Scripts\activate

    # Install the package
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

### Installing with Optional Dependencies

```bash
# Install with dbt support
pip install "insight-ingenious-for-fabric[dbt]"

# Install with all optional dependencies
pip install "insight-ingenious-for-fabric[dbt,dataprep]"
```

## Environment Setup

### Environment Variables

After installation, configure your environment variables for the CLI:

--8<-- "_includes/environment_setup.md"

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

## Getting Started

### Create Your First Project

Now that you have the CLI installed, create your first project:

=== "macOS/Linux"

    ```bash
    # Create a new project
    ingen_fab init new --project-name "My First Project"

    # Navigate to the project
    cd "My First Project"

    # Set environment variables
    export FABRIC_WORKSPACE_REPO_DIR="."
    export FABRIC_ENVIRONMENT="development"
    ```

=== "Windows"

    ```powershell
    # Create a new project
    ingen_fab init new --project-name "My First Project"

    # Navigate to the project
    cd "My First Project"

    # Set environment variables
    $env:FABRIC_WORKSPACE_REPO_DIR = "."
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
# Check if the package is installed
pip list | grep insight-ingenious-for-fabric

# Ensure pip scripts are in PATH
export PATH="$PATH:$HOME/.local/bin"

# Or use python -m to run the module
python -m ingen_fab --help
```

#### Permission Errors

If you encounter permission errors during installation:

```bash
# Install for current user only
pip install --user insight-ingenious-for-fabric

# Or use a virtual environment (recommended)
python -m venv fabric-env
source fabric-env/bin/activate
pip install insight-ingenious-for-fabric
```

#### Python Version Issues

Ensure you have Python 3.12 or higher:

```bash
python --version
# Should show Python 3.12.x or higher

# If not, install Python 3.12+
# On Ubuntu/Debian:
sudo apt update && sudo apt install python3.12

# On macOS with Homebrew:
brew install python@3.12

# On Windows:
# Download from https://www.python.org/downloads/
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