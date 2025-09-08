# Package Management: uv

This project uses uv for Python package and environment management.

## Common Commands
- **Run a command in the project environment:**
  `uv run <command>` (e.g., `uv run app.py` instead of `uv run python app.py`)

- **Add a dependency:**
  `uv add <package>` or `uv add <package> --dev` for dev dependencies

- **Remove a dependency:**
  `uv remove <package>` or `uv remove <package> --group dev` for dev dependencies

- **Sync environment with pyproject.toml and lockfile:**
  `uv sync`

- **Run tests (run after implementing changes to ensure nothing broke):**
  `uv run pytest`

- **List out packages in environment in a tree structure**
  `uv tree`

## Note

- Do **not** use `pip` or `pip-tools` directly; use `uv` commands above.

# Python Type Hinting

## Best Practices
- **Always type hint function parameters and return values.**
  ```python
  def process_data(items: list[str]) -> dict[str, int]:
      return {"count": len(items)}
  ```

- **Use modern union syntax** with `|` instead of `Union` (Python 3.10+).
  ```python
  # Good
  def get_user(user_id: int | str) -> User | None:
      pass

  # Avoid
  from typing import Union, Optional
  def get_user(user_id: Union[int, str]) -> Optional[User]:
      pass
  ```

- **Use `from __future__ import annotations`** for forward references and cleaner code.


# python linting
- **Use `ruff` for linting, code checking etc**

