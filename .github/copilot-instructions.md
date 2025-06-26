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


# Flask Best Practices

## Response Handling
- **Return dictionaries directly** from Flask routes instead of using `jsonify()`. Flask automatically converts dictionaries to JSON responses.
  ```python
  # Good
  return {"message": "success", "data": data}

  # Avoid
  return jsonify({"message": "success", "data": data})
  ```

## Error Handling
- **Use try/abort pattern** instead of Flask's `_or_404` or similar convenience functions for better error control.
  ```python
  # Good
  try:
      user = User.query.filter_by(id=user_id).one()
  except NoResultFound:
      abort(404, description="User not found")

  # Avoid
  user = User.query.get_or_404(user_id)
  ```

  ## Static Files

  - **Store static files** such as CSS, JavaScript, and images in the `static` directory.
  - **Store HTML Jinja templates** in the `templates` directory.
  - **For small snippets** of CSS or JavaScript, consider inlining them directly into your templates if it simplifies the project structure. However, for maintainability, lengthy JS code should always be kept separate.

# Frontend Libraries
## Bootstrap
- Use Bootstrap for all styling and layout.
- Prefer Bootstrap utility classes (e.g., `d-flex`, `mb-3`, `text-primary`) over custom CSS.
- Write custom CSS only if Bootstrap utilities are insufficient.
- Include Bootstrap via CDN in your base template.

## Alpine.js
- Use Alpine.js for client-side interactivity.
- Prefer Alpine.js directives (`x-data`, `x-show`, `x-on:click`) over vanilla JS or jQuery.
- Keep Alpine.js logic simple and declarative in HTML attributes.
- Include Alpine.js via CDN in your base template.