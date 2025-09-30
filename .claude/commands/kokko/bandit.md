# Security Hardening Using Bandit

Use Bandit to detect common Python security issues and refactor code safely.

## 1. Run Bandit Scan
```bash
uv run bandit -q -r . -ll        # -ll => only medium+ severity (adjust as needed)
# For full context
uv run bandit -r . -f screen
```
If repo large, target app dirs: `uv run bandit -r ingenious ingenious_extensions`.

## 2. Parse Findings
For each issue note:
- File:Line
- Test ID (e.g., B303)
- Severity / Confidence
- Short description

Create a working list sorted: High severity first, then Medium.

## 3. Classify Each Finding
Choose one:
- TRUE_POSITIVE → fix now
- NEEDS_REFACTOR → create safer abstraction then fix
- FALSE_POSITIVE → justify & suppress locally
- ACCEPT_RISK_TEMPORARILY → open tracking issue (include rationale + mitigation plan)

## 4. Fix Patterns
Common Bandit IDs and actions:
- B303/B304 (insecure hash) → use hashlib.sha256 / blake2b
- B102 (exec) / B602-B607 (subprocess shell) → remove shell=True, use args list
- B301 (pickle) → replace with json / safe serializer; if unavoidable isolate & document
- B108 (hardcoded tmp) → use tempfile module
- B105 (hardcoded password) → move to secret manager / env var
- B403 (import * requests) → explicit imports
- B410 (insecure yaml.load) → use yaml.safe_load
- B501 (request w/ verify=False) → enable cert validation or document internal CA
- B608 (SQL injection) → parameterize queries via driver placeholders

## 5. Implement Fix Incrementally
For each finding fixed:
```bash
uv run pytest -q
uv run bandit -q -r <affected_paths>
```
Commit if clean:
```bash
git add <files>
git commit -m "security(bandit): mitigate <TestID> in <symbol>"
```

## 6. Suppressing False Positives
Use the narrowest suppression:
```python
# nosec B608: parameterized via execute(params)
```
Document reasoning in code or SECURITY_NOTES.md.

## 7. Final Full Scan & Quality Gate
```bash
uv run bandit -r .
uv run pytest
uv run pre-commit run --all-files
uv run mypy . --exclude venv
```

Goal: Eliminate or mitigate all high-severity Bandit findings with clear traceability.
