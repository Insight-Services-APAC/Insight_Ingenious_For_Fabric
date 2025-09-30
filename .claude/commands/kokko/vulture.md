# Find and Remove Dead Code with Vulture

Use `uv run vulture . --exclude .venv` to analyze this codebase for dead or unused code. For each item detected:

1. **Thoroughly and intensively verify** whether the code is truly unused or unreachable. Cross-check:
   * All internal references across the codebase (including dynamically referenced or imported code)
   * Any usage from entry points, config-based registries, decorators, metaprogramming, etc.
   * References via reflection, eval, exec, string-based access (e.g., getattr, globals)

2. **If you are absolutely certain the code is unused**, safely remove it.

3. **Run all tests** to ensure nothing breaks after removal:
   ```bash
   uv run pytest
   ```

4. **After each successful removal and test pass**, create a separate Git commit with a clear message, such as:
   ```bash
   git add .
   git commit -m "chore(cleanup): remove unused function <function_name> as detected by vulture"
   ```

Repeat this process for all remaining vulture reports. Do not batch deletions—handle one item at a time to maintain traceability and safety.
