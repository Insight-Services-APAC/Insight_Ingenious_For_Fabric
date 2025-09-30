# Master Prompt - Fabric Accelerator Full QA

**Branch:** `kokko-cleanup`

---

## General Rules (apply throughout)

- Always check that docs align with your actual setup experience.
  - Update only if incomplete, inaccurate, or missing.

- **You are allowed to debug ingen_fab library code if blocked.**

- Commit code changes incrementally in small, focused commits.

- Never commit temporary test directories (e.g., `test_workspace/`, `temp_project/`).

- Always activate the virtual environment and set required environment variables before running commands:
  ```bash
  source .venv/bin/activate
  export FABRIC_ENVIRONMENT="local"
  export FABRIC_WORKSPACE_REPO_DIR="sample_project"
  ```

---

## Sequential Steps

### Step 0 — Environment Setup
1. Verify the virtual environment is set up correctly (`uv sync` or `pip install -e .[dev]`).
2. Activate the virtual environment.
3. Set required environment variables: `FABRIC_ENVIRONMENT=local` and `FABRIC_WORKSPACE_REPO_DIR=sample_project`.
4. Run `pytest` to verify all tests pass.
5. Run linting: `ruff check .` and formatting: `ruff format .`.

---

### Step 1 — Documentation Validation
- Verify that `README.md` and `docs/` match the actual setup and usage experience.
- Test all command examples in the documentation.
- Update with gotchas, fixes, and missing steps if needed.
- Keep examples terse and copy/pasteable.

---

### Step 2 — CLI Commands Testing
- Test all CLI command groups systematically:
  1. `ingen_fab init` - Create new workspace projects
  2. `ingen_fab ddl compile` - Generate DDL notebooks (both Warehouse and Lakehouse modes)
  3. `ingen_fab deploy` - Deployment commands
  4. `ingen_fab test local python` - Test Python libraries
  5. `ingen_fab test local pyspark` - Test PySpark libraries
  6. `ingen_fab notebook` - Notebook utilities
- Validate outputs and verify generated files are correct.
- Update CLI documentation if commands behave differently than documented.

---

### Step 3 — DDL Generation Validation
- Test DDL notebook generation for both modes:
  1. Warehouse mode: `ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse`
  2. Lakehouse mode: `ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse`
- Verify generated notebooks in `sample_project/fabric_workspace_items/` are valid.
- Check that parameter cells and imports are correctly injected.
- Validate against all environment configurations (development, test, production).

---

### Step 4 — Python Libraries Testing
- Run comprehensive tests for Python libraries:
  1. `ingen_fab test local python` - CPython implementations
  2. `ingen_fab test local pyspark` - PySpark implementations
- Verify all abstraction layers work correctly (interfaces, implementations).
- Test common utilities in `python_libs/common/`.
- Validate that library code uses absolute imports only.

---

### Step 5 — Sample Project Validation
- Verify `sample_project/` structure and configuration:
  1. Check `platform_manifest_*.yml` files for all environments
  2. Validate variable library configurations in `fabric_workspace_items/config/var_lib.VariableLibrary/`
  3. Test workspace item definitions (lakehouses, warehouses, notebooks)
- Ensure sample project serves as a working reference for users.

---

### Step 6 — Integration Testing
- Run all integration tests marked with `@pytest.mark.e2e`.
- Verify end-to-end workflows:
  1. Project initialization
  2. DDL generation
  3. Notebook processing (scanning, injection, conversion)
- Test cross-environment compatibility (development, test, production).

---

### Step 7 — Code Quality and Standards
- Run full linting: `ruff check .`
- Run formatting: `ruff format .`
- Run pre-commit hooks: `pre-commit run --all-files`
- Verify no linting errors or warnings remain.
- Check that all code follows project patterns and conventions.

---

### Step 8 — Documentation Site Testing
- Serve documentation locally: `mkdocs serve --dev-addr=0.0.0.0:8000`
- Review all documentation pages for accuracy.
- Verify code examples work as documented.
- Check that navigation and structure are logical.
- Update any outdated or missing documentation.

---

## Constraints
- Never commit temporary test directories.
- Always use `uv` for package management.
- Always activate virtual environment before running commands.
- Maintain professional tone, no emojis.

---

## Terminator
**DO NOT STOP UNTIL THIS FULL TASK IS COMPLETED.**

---

## Keyword
**FABRICQA**
