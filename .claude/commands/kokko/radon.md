# Refactor Using Radon (Complexity & Maintainability)

Use Radon to identify high-complexity / low-maintainability hotspots and refactor them safely and incrementally.

## 1. Run Radon Reports
```bash
uv run radon cc -s -a .          # Cyclomatic complexity (A best → F worst)
uv run radon mi -s .              # Maintainability Index (100 best → 0 worst)
uv run radon hal .                # (Optional) Halstead metrics
```
Record functions / classes with:
- Complexity grade >= C (or numeric > 10)
- Maintainability Index < 65

## 2. Prioritize Hotspots
Rank by (a) worst grade, (b) frequency of change (git log -S / blame), (c) business criticality.

## 3. Establish Safety Net
For each target file/function:
1. Add / improve focused tests (behavioral, edge cases, error paths).
2. Run: `uv run pytest <relevant-tests>` (fast feedback) before refactor.

## 4. Refactor Tactics (Apply One At A Time)
- Extract Function / Method for cohesive blocks
- Decompose long conditional chains (strategy map, dict dispatch, guard clauses)
- Remove duplication (DRY) or inline trivial indirections
- Simplify boolean logic (early returns, De Morgan)
- Replace deep nesting with fail-fast exits
- Clarify names; remove commented‑out code
- Isolate side effects from pure logic
- Reduce parameter count (introduce small dataclass / typed object)
- Split large classes by responsibility (SRP)

After each micro-change:
```bash
uv run pytest <focused-scope>
uv run radon cc -s target_file.py
uv run radon mi -s target_file.py
```
Commit if green:
```bash
git add target_file.py tests/
git commit -m "refactor(radon): reduce complexity in <symbol> (C→B)"
```

## 5. Validate No Regression
When a file is “done”:
```bash
uv run radon cc -s -a .
uv run radon mi -s .
uv run pytest
uv run pre-commit run --all-files
```

## 6. Avoid Over-Refactoring
Stop when: (a) complexity ≤ B, (b) MI ≥ 70, (c) further change risks churn.

## 7. Handling Hard Cases
If complexity resists decomposition:
- Introduce a decision table / data-driven structure
- Split algorithm into phases (parse → transform → emit)
- Accept temporary adapter layer while migrating callers

## 8. Final Sweep
Run full quality gate:
```bash
uv run pytest && uv run pre-commit run --all-files && uv run mypy . --exclude venv
```
Then push:
```bash
git push
```

Goal: measurable reduction in worst complexity grades without behavior change.
