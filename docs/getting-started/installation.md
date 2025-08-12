# Installation

[Home](../index.md) > [Getting Started](installation.md) > Installation

This guide will help you install and set up the Ingenious Fabric Accelerator on your system.

!!! tip "Learning Path"  
    **New User Journey**: Installation (you are here) → [Quick Start](quick-start.md) → [First Project](first-project.md) → [CLI Reference](../guides/cli-reference.md)

## Requirements

Before installing, ensure your system meets these requirements:

- **Python 3.12 or higher**
- **Git** (for cloning the repository)
- **Microsoft Fabric workspace** (for deployment)
- **Azure CLI** (optional, for authentication)

## Installation Methods

--8<-- "_includes/quick_install.md"

### Development Installation

For development work with all dependencies:

```bash
# Using uv (includes all dependency groups)
uv sync --all-extras

# Using pip (install with all optional dependencies)  
pip install -e .[dev,docs,dbt]
```

## Environment Setup

### Environment Variables

--8<-- "_includes/environment_setup.md"

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

1. **[Get started quickly](quick-start.md)** with your first project
2. **[Learn the commands](../guides/cli-reference.md)** available
3. **[Explore examples](../examples/index.md)** to see real-world usage
4. **[Read the workflows](../guides/workflows.md)** for best practices

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