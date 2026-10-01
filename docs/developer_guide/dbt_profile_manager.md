# DBT Profile Manager

[Home](../index.md) > [Developer Guide](index.md) > DBT Profile Manager

`ingen_fab/cli_utils/dbt_profile_manager.py` turns a project's Variable Library value sets
into a `profiles.yml` for the native `dbt-fabricspark` adapter. It is pure: it reads files,
builds a dictionary, validates it with the adapter's credentials class, and writes one file.
No prompts, no network, no state outside the project.

## Flow

```
dbt_project.yml ─ profile: ──────────────┐
valueSets/<env>.json ─ lakehouse ids ────┼─> build_profile() ─> validate_with_adapter() ─> profiles/profiles.yml
--lakehouse / dbt_default_lakehouse ─────┘
```

| Function | Role |
| --- | --- |
| `read_profile_name(dbt_project_dir)` | `profile:` from `dbt_project.yml`; error when absent |
| `read_value_set(project_path, environment)` | `variableOverrides` as a name-to-value dict |
| `discover_lakehouses(values)` | every `<prefix>_lakehouse_id` with a real id, as `LakehouseRef` (prefix, name, lakehouse id, workspace id); placeholders skipped |
| `choose_default_lakehouse(values, lakehouses, requested)` | `requested`, else `dbt_default_lakehouse`, else the only one; otherwise `ProfileError` naming the prefixes |
| `build_target(values, lakehouse, profile_name, environment, notebook)` | one target; `notebook=True` switches auth to `fabric_notebook`, otherwise `token_credential` with `DefaultAzureCredential` |
| `build_profile(...)` | `{profile_name: {target, outputs}}` with `<env>` and `<env>-notebook` per environment |
| `validate_with_adapter(profile)` | `FabricSparkCredentials.from_dict` on every target: the adapter's own required-field, GUID and credential-wiring checks |
| `write_profile(profile, dbt_project_dir)` | `<dbt_project>/profiles/profiles.yml` with a generated-file header |
| `ensure_profile(project_path, dbt_project, environment, lakehouse, all_environments)` | the whole thing; returns the profiles directory |

## Conventions the value set follows

- `<prefix>_lakehouse_id`, `<prefix>_lakehouse_name`, `<prefix>_workspace_id` (falls back to
  `fabric_deployment_workspace_id`) for every lakehouse.
- `dbt_default_lakehouse`: prefix or lakehouse name of the default target.
- `dbt_schema`: `dbo` for a schema-enabled default lakehouse; unset means the lakehouse name,
  which is how the adapter recognises a plain lakehouse at parse time.
- `dbt_threads`, `fabric_api_endpoint`: optional.

## Callers

- `dbt_commands.write_profile` (`ingen_fab dbt profile`): generate, show.
- `dbt_commands.run_dbt` (`ingen_fab dbt <verb>`): regenerate, then run `dbt` with
  `--project-dir`, `--profiles-dir`, `--target <environment>` and the pass-through arguments.
- `dbt_commands.write_orchestrator_notebook` (`ingen_fab dbt orchestrator`): reads the value
  set for the config lakehouse name; the notebook selects `<environment>-notebook` at run time
  from the Variable Library's `fabric_environment`.

## Tests

`tests/test_dbt_profile_manager.py`: profile name from the project file, discovery and
placeholder skipping, the selection rules, the two targets per environment, adapter
validation accepting a good target and rejecting a bad one, the golden file, the CLI proxy's
command line, and the rendered orchestrator notebook.

## Extending

- A new profile field: add it in `build_target`, extend the golden test, and rely on
  `validate_with_adapter` to catch a name the adapter does not know.
- A different default-lakehouse rule: `choose_default_lakehouse` is the only place.
- Another authentication mode (for example a raw access token): a third branch in
  `build_target` selected by a value-set variable, plus a target suffix in `build_profile`.
