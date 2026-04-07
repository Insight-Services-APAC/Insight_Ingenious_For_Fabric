# Exercise 7 — GraphQL API *(Stretch)*

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 7

## Learning Objectives

By the end of this exercise you will be able to:

- Create a Fabric GraphQL API endpoint over a Warehouse
- Expose selected views via GraphQL
- Query the API using the built-in editor
- Understand when GraphQL is appropriate vs direct SQL or reporting access

## Prerequisites

- [Exercise 4](exercise-04-gold-layer.md) completed — `DW.vDim_Cities` and/or `DW.vDim_CityGeography` exist in `wh_gold`
- Your Fabric workspace has **GraphQL API** enabled (check workspace settings)

## Background

Fabric's **GraphQL API for data** lets you expose Warehouse or Lakehouse data as a typed GraphQL endpoint — useful when applications need a structured API rather than direct SQL, or when you want to provide filtered, paginated access without writing custom code.

GraphQL APIs in Fabric are **auto-generated** from the schema of the tables/views you select.

## Steps

### 1. Create a GraphQL API item

1. In the Fabric workspace, select **+ New item** → **GraphQL API**
2. Name it `api_dp_geography`

### 2. Connect to `wh_gold`

1. Select **Warehouse** → `wh_gold`
2. Select the objects to expose:
   - `DW.vDim_Cities`
   - `DW.vDim_CityGeography`
3. Click **Confirm**

Fabric auto-generates the GraphQL schema from the view definitions.

### 3. Explore the generated schema

In the GraphQL editor, switch to the **Schema** tab. You should see generated types like:

```graphql
type vDim_Cities {
  CityID: Int
  CityName: String
  LatestRecordedPopulation: Int
  # ...
}

type Query {
  vDim_Cities(
    filter: vDim_CitiesFilter
    orderBy: vDim_CitiesOrderBy
    first: Int
    after: String
  ): vDim_CitiesConnection
}
```

### 4. Run a basic query

Switch to the **Query** tab and run:

```graphql
query GetCities {
  vDim_Cities {
    items {
      CityID
      CityName
      LatestRecordedPopulation
    }
  }
}
```

You should receive a JSON response with your 10 cities.

### 5. Try a filtered query

```graphql
query LargeCities {
  vDim_Cities(
    filter: { LatestRecordedPopulation: { gt: 5000 } }
    orderBy: { LatestRecordedPopulation: DESC }
  ) {
    items {
      CityName
      LatestRecordedPopulation
    }
  }
}
```

### 6. Call the API from Python (optional)

Fabric GraphQL APIs are secured with Entra ID. You can call the endpoint programmatically using `azure-identity` — the same authentication library IngenFab uses internally.

#### Option A — `DefaultAzureCredential` (recommended)

This uses your existing `az login` session, which means no app registration is required:

```python
import requests
from azure.identity import DefaultAzureCredential

WORKSPACE_ID = "<workspace-id>"   # from the Fabric UI URL
API_ID       = "<api-id>"         # from the GraphQL API item URL

credential = DefaultAzureCredential()
token = credential.get_token("https://api.fabric.microsoft.com/.default").token

endpoint = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
    f"/graphqlapis/{API_ID}/graphql"
)

query = """
{
  vDim_Cities(orderBy: { LatestRecordedPopulation: DESC }) {
    items {
      CityName
      LatestRecordedPopulation
    }
  }
}
"""

response = requests.post(
    endpoint,
    json={"query": query},
    headers={"Authorization": f"Bearer {token}"},
)
response.raise_for_status()

data = response.json()
for city in data["data"]["vDim_Cities"]["items"]:
    print(f"{city['CityName']:30s} {city['LatestRecordedPopulation']:>10,}")
```

!!! tip "Finding IDs"
    Open the GraphQL API item in the Fabric UI. The URL contains both IDs:
    `https://app.fabric.microsoft.com/groups/<workspace-id>/graphqlapis/<api-id>`

#### Option B — Interactive browser login

If `az login` is not available, use `InteractiveBrowserCredential` instead — it opens a browser window:

```python
from azure.identity import InteractiveBrowserCredential

credential = InteractiveBrowserCredential()
# ... rest of the code is the same as Option A
```

!!! note
    Both options require `azure-identity` and `requests`, which are already installed as IngenFab dependencies. If running outside the IngenFab virtual environment, install them with: `pip install azure-identity requests`

## Verification — Part A (Fabric UI)

1. `api_dp_geography` appears as an item in your Fabric workspace
2. The built-in query editor returns city data for the `GetCities` query
3. The filtered query returns only cities with population > 5,000
4. *(Optional)* The Python snippet returns a valid JSON response

---

## Part B — Version-Control & Deploy with IngenFab

Now that the GraphQL API exists in Fabric, bring it under source control so it can be promoted across environments.

### 7. Download the GraphQL API definition

From your project root (where `ingen_fab.yml` lives):

```bash
ingen_fab deploy download-artefact \
  -n "api_dp_geography" \
  -t GraphQLApi
```

This downloads the definition into `fabric_workspace_items/downloaded/GraphQLApi/api_dp_geography.GraphQLApi/`.

!!! note
    The download folder uses the convention `<display-name>.<item-type>/` — so you'll see the `.GraphQLApi` suffix appended to the name.

### 8. Inspect the downloaded files

```bash
find fabric_workspace_items/downloaded/GraphQLApi/api_dp_geography.GraphQLApi -type f
```

You should see:

```
fabric_workspace_items/downloaded/GraphQLApi/api_dp_geography.GraphQLApi/
├── graphql-definition.json    ← datasource connection + exposed objects
└── .platform                  ← Fabric item metadata
```

Open `.platform` — it contains the item type, display name, and a unique `logicalId`:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "GraphQLApi",
    "displayName": "api_dp_geography"
  },
  "config": {
    "version": "2.0",
    "logicalId": "a3f7c1d2-..."  
  }
}
```

!!! warning "Check your logicalId"
    The `logicalId` must be a **unique UUID** across all items in your project. If it shows `00000000-0000-0000-0000-000000000000`, generate a real one before deploying:
    ```bash
    python3 -c "import uuid; print(uuid.uuid4())"
    ```
    Then replace the zero-GUID in `.platform`. Duplicate logicalIds will cause `ingen_fab deploy deploy` to fail.

Open `graphql-definition.json` — note that it contains hard-coded IDs for the workspace and warehouse, plus the full field mappings for each exposed object:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/graphqlApi/definition/1.0.0/schema.json",
  "datasources": [
    {
      "objects": [
        {
          "actions": { "Query": "Enabled" },
          "fieldMappings": {
            "CityID": "CityID",
            "CityName": "CityName",
            "LatestRecordedPopulation": "LatestRecordedPopulation"
          },
          "graphqlType": "vDim_Cities",
          "relationships": [],
          "sourceObject": "DW.vDim_Cities",
          "sourceObjectType": "View"
        },
        {
          "actions": { "Query": "Enabled" },
          "fieldMappings": {
            "CityID": "CityID",
            "CityName": "CityName",
            "Continent": "Continent",
            "CountryName": "CountryName"
          },
          "graphqlType": "vDim_CityGeography",
          "relationships": [],
          "sourceObject": "DW.vDim_CityGeography",
          "sourceObjectType": "View"
        }
      ],
      "sourceItemId": "d98037b6-...",
      "sourceType": "Warehouse",
      "sourceWorkspaceId": "b6cf71ab-..."
    }
  ]
}
```

!!! info
    The `fieldMappings` list every column exposed via GraphQL. The snippet above is abbreviated — your download will include all columns from each view.

### 9. Move into the deployable location

The `downloaded/` folder is a **staging area** — items left there will cause duplicate-item errors during deployment. Move the item to its final location and remove the staging copy:

```bash
mkdir -p fabric_workspace_items/GraphQLApi
cp -r fabric_workspace_items/downloaded/GraphQLApi/api_dp_geography.GraphQLApi \
      fabric_workspace_items/GraphQLApi/

# Remove the staging copy to prevent duplicates during deploy
rm -rf fabric_workspace_items/downloaded/GraphQLApi/api_dp_geography.GraphQLApi
```

!!! warning "Always remove the staging copy"
    `ingen_fab deploy deploy` scans the **entire** `fabric_workspace_items/` directory — including `downloaded/`. If the same item exists in both locations, deployment will fail with `Duplicate logicalId` errors.

Your project structure should now look like:

```
your-project/
├── fabric_workspace_items/
│   ├── GraphQLApi/
│   │   └── api_dp_geography.GraphQLApi/
│   │       ├── graphql-definition.json
│   │       └── .platform
│   ├── downloaded/                  ← should be empty for this type
│   ├── SemanticModel/
│   │   └── sm_dp_geography/
│   └── ...
└── ingen_fab.yml
```

### 10. Add variable placeholders

To make the definition environment-portable, replace the hard-coded IDs in `graphql-definition.json` with variable-library placeholders.

**First, check which variables are available** in your variable library:

```bash
grep -i "warehouse\|workspace" \
  fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
```

You should see names like `wh_gold_warehouse_id` and `fabric_deployment_workspace_id`. Use these **exact names** in your placeholders:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/graphqlApi/definition/1.0.0/schema.json",
  "datasources": [
    {
      "objects": [ ... ],
      "sourceItemId": "{{varlib:wh_gold_warehouse_id}}",
      "sourceType": "Warehouse",
      "sourceWorkspaceId": "{{varlib:fabric_deployment_workspace_id}}"
    }
  ]
}
```

!!! tip
    Only the `sourceWorkspaceId` and `sourceItemId` values need to be replaced — leave the `objects` array (field mappings, actions, etc.) unchanged.

!!! danger "Variable names must match exactly"
    If a `{{varlib:...}}` placeholder doesn't match a variable in the library, it will be sent to Fabric as a raw string instead of a GUID — causing an `InvalidDefinitionInvalidPropertyType` error. The deploy log will warn: `⚠ Variable 'xxx' not found in library, keeping placeholder`. Always verify names before deploying.


### 11. Deploy to verify the round-trip

Run a deployment to confirm the GraphQL API is published with resolved variables:

```bash
ingen_fab deploy deploy
```

Look for output like:

```
Updated 1 GraphQL API files with variable substitution
```

After deployment, verify `api_dp_geography` still works in the Fabric UI by rerunning the `GetCities` query from Step 4.

## Verification — Part B (IngenFab)

- `fabric_workspace_items/GraphQLApi/api_dp_geography.GraphQLApi/graphql-definition.json` exists locally
- The definition uses `{{varlib:...}}` placeholders instead of hard-coded IDs
- `git log --oneline -1` shows your commit with the GraphQL API files
- `ingen_fab deploy deploy` reports the GraphQL API was published successfully
- The API still returns data when queried in the Fabric UI after redeployment

## Notes

- GraphQL APIs in Fabric are **read-only** against views by default — appropriate for analytical use cases
- The endpoint URL is shown in the API item's settings page
- Authentication uses the same Entra ID service principals as deployment
- During deployment, IngenFab processes all `graphql-definition.json` files for variable substitution — the same mechanism used for notebooks, semantic models, and pipelines
- You can filter deployment to only GraphQL APIs by setting the `ITEM_TYPES_TO_DEPLOY` environment variable: `export ITEM_TYPES_TO_DEPLOY="GraphQLApi"`

## Troubleshooting

??? failure "Duplicate logicalId '00000000-0000-0000-0000-000000000000'"
    **Cause:** Two or more items share the same `logicalId` in their `.platform` files. This commonly happens when:

    - The `downloaded/` staging copy was **not removed** after moving the item to its deploy location
    - Multiple downloaded items all have the default zero-GUID

    **Fix:**

    1. Remove any staging copies:
        ```bash
        rm -rf fabric_workspace_items/downloaded/GraphQLApi/api_dp_geography.GraphQLApi
        ```
    2. Assign a unique UUID to each item's `.platform`:
        ```bash
        python3 -c "import uuid; print(uuid.uuid4())"
        # Copy the output and paste it as the logicalId value
        ```

??? failure "ALM.InvalidDefinitionInvalidPropertyType — Failed to convert value for $.datasources[0].sourceItemId"
    **Cause:** A `{{varlib:...}}` placeholder was **not resolved** during variable substitution — the raw placeholder string was sent to Fabric instead of a GUID.

    **Fix:**

    1. Check the deploy log for warnings like `⚠ Variable 'xxx' not found in library`
    2. Look up the correct variable name:
        ```bash
        grep -i warehouse fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
        ```
    3. Update the placeholder in `graphql-definition.json` to match the exact variable name

??? failure "cp: No such file or directory"
    **Cause:** The folder name in the `cp` command doesn't match the actual downloaded folder. The downloaded folder is named `<display-name>.<item-type>/` using the item's **Fabric display name**.

    **Fix:**

    1. Check the actual folder name:
        ```bash
        ls fabric_workspace_items/downloaded/GraphQLApi/
        ```
    2. Use the exact folder name shown in the `cp` command

---

← [Exercise 6 — Power BI Report](exercise-06-report.md) | [Back to DP Training →](index.md)
