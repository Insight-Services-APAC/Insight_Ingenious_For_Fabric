# Installation

This guide will help you install and set up the Ingenious Fabric Accelerator on your system.

## Requirements

Before installing, ensure your system meets these requirements:

- **Python 3.12 or higher**
- **Git** (for cloning the repository)
- **Microsoft Fabric workspace** (for deployment)
- **Azure CLI** (optional, for authentication)

## Installation Methods

### Method 1: Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is the fastest way to install and manage Python dependencies:

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository (replace with your actual repository URL)
git clone <repository-url>
cd ingen_fab

# Install with uv (includes all development dependencies)
uv sync
```

### Method 2: Using pip

```bash
# Clone the repository (replace with your actual repository URL)
git clone <repository-url>
cd ingen_fab

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package in development mode
pip install -e .[dev]
```

### Method 3: Development Installation

For development work, install with all dependencies:

```bash
# Using uv (includes all dependency groups)
uv sync --all-extras

# Using pip (install with all optional dependencies)
pip install -e .[dev,docs]
```

## Environment Setup

### Environment Variables

Set these environment variables to avoid specifying them on each command:

```bash
# Project location
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"

# Target environment
export FABRIC_ENVIRONMENT="development"

# Authentication (for deployment)
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

### Shell Configuration

Add these to your shell profile (`.bashrc`, `.zshrc`, etc.):

```bash
# Add to ~/.bashrc or ~/.zshrc
export PATH="$PATH:$HOME/.local/bin"

# Optional: Create aliases
alias ifab="ingen_fab"
alias ifab-help="ingen_fab --help"
```

## Verification

Verify your installation by running:

```bash
# Display help
ingen_fab --help

# Run a basic command
ingen_fab init --help
```

Expected output:
```
Usage: ingen_fab [OPTIONS] COMMAND [ARGS]...

Options:
  --fabric-workspace-repo-dir  -fwd  Directory containing fabric workspace repository files
  --fabric-environment         -fe   The name of your fabric environment
  --help                            Show this message and exit.

Commands:
  deploy    Commands for deploying to environments and managing workspace items.
  init      Commands for initializing solutions and projects.
  ddl       Commands for compiling DDL notebooks.
  test      Commands for testing notebooks and Python blocks.
  notebook  Commands for managing and scanning notebook content.
  package   Commands for running extension packages.
  libs      Commands for compiling and managing Python libraries.
```

## Docker Installation (Optional)

For containerized environments:

```dockerfile
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy and install application
COPY . /app
WORKDIR /app
RUN uv sync

# Set entrypoint
ENTRYPOINT ["ingen_fab"]
```

## Troubleshooting

### Common Issues

#### Permission Errors
If you encounter permission errors:
```bash
# Use user installation
pip install --user -e .[dev]

# Or fix permissions
sudo chown -R $USER:$USER /path/to/ingen_fab
```

#### Python Version Issues
Check your Python version:
```bash
python --version
# Should show Python 3.12.x or higher
```

#### Import Errors
If you get import errors:
```bash
# Reinstall in development mode
pip install -e .

# Or check your PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Platform-Specific Notes

#### Windows
- Use PowerShell or Command Prompt
- Virtual environment activation: `.venv\Scripts\activate`
- Path separators: Use forward slashes in commands

#### macOS
- May need to install Command Line Tools: `xcode-select --install`
- Use Homebrew for additional dependencies if needed

#### Linux
- Install build tools: `sudo apt-get install build-essential`
- Some distributions may require additional packages

## Next Steps

Once installed, you can:

1. **[Get started quickly](quick_start.md)** with your first project
2. **[Learn the commands](cli_reference.md)** available
3. **[Explore examples](../examples/index.md)** to see real-world usage
4. **[Read the workflows](workflows.md)** for best practices

## Updating

To update to the latest version:

```bash
# With uv
uv sync --upgrade

# With pip
pip install --upgrade -e .[dev]
```

## Uninstalling

To remove the installation:

```bash
# With uv (removes environment completely)
uv clean

# With pip (if installed in development mode)
pip uninstall insight-ingenious-for-fabric

# Remove the repository directory
cd ..
rm -rf ingen_fab
```