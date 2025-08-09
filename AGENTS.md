# Repository Guidelines

## Environment Activation
- Activate and export env vars before working:
  `source .venv/bin/activate && export FABRIC_ENVIRONMENT=local && export FABRIC_WORKSPACE_REPO_DIR=sample_project`.
- For deploys, also set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`.

## Project Structure & Module Organization
- `ingen_fab/`: CLI entry (`cli.py`) and modules: `cli_utils/`, `ddl_scripts/`, `notebook_utils/`, `packages/`, `python_libs/`, `templates/`, `config_utils/`.
- `sample_project/`: Example Fabric workspace layout.
- `tests/` and root `test_*.py`: Unit tests and integration helpers.
- `docs/` + `mkdocs.yml`: Documentation site source.
- Key metadata: `pyproject.toml` (scripts, deps), `pytest.ini`, `.flake8`.

## Build, Test, and Development Commands
- Install (recommended): `uv sync` (uses groups in `pyproject.toml`).
- Install (pip): `python -m venv .venv && source .venv/bin/activate && pip install -e .[dev]`.
- Run CLI: `ingen_fab --help` (console script entry).
- Test: `pytest -q` or `pytest -q --cov=ingen_fab` (if `pytest-cov` synced).
- Lint/format: `ruff check .` and `ruff format` (auto-fix: `ruff check --fix .`).
- Hooks (if configured): `pre-commit run --all-files`.
- Docs: `uv sync --group docs && mkdocs serve` (or `./serve-docs.sh`).

## Coding Style & Naming Conventions
- Python 3.12+. Max line length 120 (`.flake8`).
- Use type hints and docstrings for public functions.
- Naming: `snake_case` for functions/modules, `PascalCase` for classes, `CONSTANT_CASE` for constants.
- CLI commands use Typer; keep command functions small, validated, and side‑effect free where possible.
- Prefer `pathlib`, `logging`, and explicit exceptions over prints and bare excepts.

## Architecture & Patterns
- CLI: Typer entry at `ingen_fab/cli.py`; command implementations in `ingen_fab/cli_utils/`.
- Templates: Jinja2 in `ingen_fab/ddl_scripts/_templates/` and `ingen_fab/notebook_utils/templates/`.
- Libraries: interfaces in `python_libs/interfaces/`; implementations in `python_libs/python/` and `python_libs/pyspark/`; shared in `python_libs/common/`.
- Abstractions: do not use raw Spark or filesystem ops; use helpers in `python_libs/*`. In `python_libs/`, use absolute imports (never relative).

## Testing Guidelines
- Framework: `pytest` (see `pytest.ini`). Place tests under `tests/` or as `test_*.py` at repo root.
- Use markers (e.g., `@pytest.mark.e2e`) for network/platform tests; these are skipped unless required env vars are set.
- Tests run offline by default. Aim for meaningful coverage of `ingen_fab/*`; add fixtures under `tests/conftest.py` when shared.
- Run locally: `pytest -q`; quick loop with `-k <keyword>`; generate coverage with `--cov` when available.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise subject (≤72 chars), details in body when needed. Example: `Refactor variable library utils`.
- Before PR: run `ruff check .` and `pytest` locally; include description, rationale, and any breaking changes.
- Link related issues, add screenshots or CLI output when useful, and update docs/templates when behavior changes.
- Keep PRs focused and small; prefer follow‑ups over unrelated changes.

## Key Configuration Files
- `pyproject.toml`, `pytest.ini`, `mkdocs.yml`.
- `sample_project/platform_manifest_*.yml` and `sample_project/fabric_workspace_items/config/var_lib.VariableLibrary/` for env/variable configuration.
