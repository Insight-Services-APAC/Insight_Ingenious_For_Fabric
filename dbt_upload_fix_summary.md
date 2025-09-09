# DBT Upload Folder Structure Fix

## Problem
The `deploy upload-python-libs` command was not preserving the correct folder structure when uploading dbt packages to the config lakehouse, causing import path mismatches.

## Root Cause
The target prefix in the upload was incorrect:

**Before (incorrect):**
- Target: `ingen_fab/packages/dbt/{dbt_project}/runtime`
- Result: `ingen_fab/packages/dbt/dbt_project/runtime/models/staging/stg_users/model.py`

**Import statements expected:**
```python
from ingen_fab.packages.dbt.runtime.projects.dbt_project.models.staging.stg_users.model import Model
```

**Actual upload resulted in:**
```python
from ingen_fab.packages.dbt.dbt_project.runtime.models.staging.stg_users.model import Model
```

## Solution
Updated the target prefix to include the `projects` directory level:

**After (correct):**
- Target: `ingen_fab/packages/dbt/runtime/projects/{dbt_project}`
- Result: `ingen_fab/packages/dbt/runtime/projects/dbt_project/models/staging/stg_users/model.py`

## Files Changed
- `/workspaces/i4f/ingen_fab/az_cli/onelake_utils.py` 
  - Line 663: Updated target prefix from `f"ingen_fab/packages/dbt/{dbt_project_name}/runtime"` to `f"ingen_fab/packages/dbt/runtime/projects/{dbt_project_name}"`
  - Line 617: Updated info display to reflect correct target paths

## Expected Result
Now the uploaded files will match the import structure used by the DAG executor:
```
ingen_fab/packages/dbt/runtime/projects/dbt_project/
├── models/
│   ├── staging/
│   │   ├── stg_users/
│   │   │   ├── model.py
│   │   │   ├── not_null_user_name__DOT__226879cc53.py
│   │   │   └── unique_user_name__DOT__48d291ab79.py
│   │   └── stg_projects/
│   │       ├── model.py
│   │       ├── not_null_project_name__DOT__4f6191b02b.py
│   │       └── unique_project_name__DOT__2d8c1a147f.py
│   ├── gold/
│   └── silver/
├── seeds/
│   ├── ref__practices.py
│   └── ref__users.py
└── dag_executor.py
```

The absolute import paths in the DAG executor will now work correctly:
```python
from ingen_fab.packages.dbt.runtime.projects.dbt_project.models.staging.stg_users.model import Model
from ingen_fab.packages.dbt.runtime.projects.dbt_project.models.staging.stg_users.not_null_user_name__DOT__226879cc53 import NotNullUserNameDot226879cc53
```