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

## Recommended Setup

### 1. Create Virtual Environment

```bash
# Create and activate a virtual environment
python -m venv fabric-env
source fabric-env/bin/activate  # On Windows: fabric-env\Scripts\activate

# Install the package
pip install insight-ingenious-for-fabric
```

### 2. Set Environment Variables

--8<-- "_includes/environment_setup.md"

### 3. Verify Installation

```bash
# Check the CLI is available
ingen_fab --help

# Create your first project
ingen_fab init new --project-name "My Project"
```

## Platform-Specific Instructions

### Windows

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

### macOS/Linux

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

```bash
# Set service principal credentials
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

## Troubleshooting

### Command Not Found

If `ingen_fab` is not recognized:

```bash
# Ensure pip scripts are in PATH
export PATH="$PATH:$HOME/.local/bin"

# Or run as module
python -m ingen_fab --help
```

### Permission Errors

```bash
# Install for current user
pip install --user insight-ingenious-for-fabric

# Or use virtual environment (recommended)
python -m venv fabric-env
source fabric-env/bin/activate
pip install insight-ingenious-for-fabric
```

### Python Version Issues

```bash
# Check Python version
python --version

# Install Python 3.12+ if needed
# Ubuntu/Debian:
sudo apt update && sudo apt install python3.12

# macOS (Homebrew):
brew install python@3.12

# Windows: Download from python.org
```

## Next Steps

Once installed, proceed to:

1. **[Quick Start](quick-start.md)** - Create your first project
2. **[First Project](first-project.md)** - Detailed walkthrough
3. **[CLI Reference](../guides/cli-reference.md)** - Learn all commands

## Developer Installation

Need to modify the source code? See the [Developer Guide](../developer_guide/index.md#development-setup) for repository setup instructions.