# Find and Fix mypy Errors

1. Run mypy to check for type errors in your codebase:
   ```bash
   uv run mypy .
   ```

2. Review the output for any reported errors.

3. For each error, update your code to fix the type issue as indicated by mypy.

4. Re-run the command until no errors are reported.

you can ignore issues with external dependencies by modifying pyproject.toml
