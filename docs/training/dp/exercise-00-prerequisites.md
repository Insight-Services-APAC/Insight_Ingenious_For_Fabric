# Exercise 0 — Environment Setup

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 0

## Learning Objectives

By the end of this exercise you will be able to:

- Create a Python virtual environment and install `ingen_fab`
- Verify the CLI is working
- Authenticate with Azure
- Confirm your Fabric workspace is ready for the training exercises

---

## Step 1: Check Python version

You need Python 3.12 or higher.

```bash
python --version
```

If you see `Python 3.12.x` or higher, continue. If not, download Python 3.12 from [python.org](https://www.python.org/downloads/) before proceeding.

---

## Step 2: Create a virtual environment

Create and activate a virtual environment in an empty working directory. This keeps `ingen_fab` and its dependencies isolated.

=== "macOS / Linux"

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

=== "Windows (PowerShell)"

    ```powershell
    python -m venv .venv
    .venv\Scripts\Activate.ps1
    ```

You should see `(.venv)` in your terminal prompt once activated.

!!! tip "VS Code users"
    Open the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`), search **Python: Select Interpreter**, and choose the `.venv` interpreter. VS Code will then activate it automatically in new terminals.

---

## Step 3: Install ingen_fab

```bash
pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
```

!!! note "Installing a specific release"
    It is recommended to install from a tagged release rather than `main`. Check the [Releases page](https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric/releases) and append the tag, for example:
    ```bash
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git@v1.0
    ```

---

## Step 4: Install the dbt adapter

The dbt integration requires the `dbt-fabricsparknb` adapter, which is a separate install:

```bash
pip install git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNB.git
```

This installs `dbt_wrapper` on your PATH, which `ingen_fab dbt exec` uses to run dbt commands locally against your Fabric workspace.

---

## Step 5: Verify the installation

```bash
ingen_fab --help
dbt_wrapper --help
```

Expected output:

```
Usage: ingen_fab [OPTIONS] COMMAND [ARGS]...

Options:
  --version  -v    Show version and exit
  --help           Show this message and exit.

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

If you see `command not found`, ensure your virtual environment is activated and that pip scripts are on your PATH.

---

## Step 6: Set up Azure authentication

The CLI needs Azure credentials to deploy artefacts to Fabric.

=== "Interactive (recommended for training)"

    ```bash
    az login
    ```

    This opens a browser window. Sign in with the account that has access to your Fabric workspace.

    !!! tip "Azure CLI not installed?"
        Download it from [aka.ms/installazurecliwindows](https://aka.ms/installazurecliwindows) (Windows) or run `brew install azure-cli` (macOS).

=== "Service Principal (CI/CD)"

    ```bash
    export AZURE_TENANT_ID="your-tenant-id"
    export AZURE_CLIENT_ID="your-client-id"
    export AZURE_CLIENT_SECRET="your-client-secret"
    ```

Confirm the correct account is active:

```bash
az account show
```

---

## Step 7: Confirm your Fabric workspace

You need a Microsoft Fabric workspace. That's it — **you do not need to create Lakehouses or Warehouses manually.** The deploy step in Exercise 1 will create `lh_bronze`, `lh_silver`, `lh_gold`, and `wh_gold` automatically.

### Note down your Workspace ID

You'll need this in Exercise 1. Find it in the URL when you open your workspace in the Fabric UI — it's the GUID segment after `/groups/`.

---

## ✅ Checklist

Before moving to Exercise 1, confirm:

- [ ] Python 3.12+ installed
- [ ] Virtual environment created and activated
- [ ] `ingen_fab --help` returns the command list
- [ ] `dbt_wrapper --version` returns a version number
- [ ] `az account show` returns your account details
- [ ] Fabric workspace exists and you have the Workspace ID noted

---

**Next:** [Exercise 1 — Project Setup →](exercise-01-project-setup.md)
