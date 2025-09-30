import json
import subprocess

# === CONFIGURATION ===
app_name = "fabric-sp-app"
role = "Reader"  # Can be changed to "Contributor", etc.

# === STEP 1: Create the App Registration ===
print("Creating app registration...")
create_app_cmd = [
    "az",
    "ad",
    "app",
    "create",
    "--display-name",
    app_name,
    "--sign-in-audience",
    "AzureADMyOrg",
]
app_output = subprocess.check_output(create_app_cmd)
app = json.loads(app_output)
app_id = app["appId"]
print(f"Created App ID: {app_id}")

# === STEP 2: Create the Service Principal ===
print("Creating service principal...")
create_sp_cmd = ["az", "ad", "sp", "create", "--id", app_id]
sp_output = subprocess.check_output(create_sp_cmd)
sp = json.loads(sp_output)
sp_object_id = sp["id"]
sp_app_id = sp["appId"]
print(f"Created Service Principal: {sp_app_id}")

# === STEP 3: Create a client secret ===
print("Creating client secret...")
create_secret_cmd = [
    "az",
    "ad",
    "app",
    "credential",
    "reset",
    "--id",
    app_id,
    "--append",
    "--display-name",
    "fabric-sp-secret",
]
secret_output = subprocess.check_output(create_secret_cmd)
secret = json.loads(secret_output)
client_secret = secret["password"]
print("Client Secret created.")

# === STEP 4 (optional): Assign Azure RBAC role at subscription or resource group level ===
# You may replace with actual scope such as resource group or Fabric resource
subscription_id = subprocess.check_output(["az", "account", "show", "--query", "id", "-o", "tsv"]).decode().strip()
scope = f"/subscriptions/{subscription_id}"
print(f"Assigning role '{role}' at scope: {scope}")
subprocess.run(
    [
        "az",
        "role",
        "assignment",
        "create",
        "--assignee",
        sp_app_id,
        "--role",
        role,
        "--scope",
        scope,
    ]
)

# === OUTPUT Credentials ===
print("\n==== Service Principal Credentials ====")
print(f"Client ID: {sp_app_id}")
print(f"Client Secret: {client_secret}")
print(f"Tenant ID: {app['publisherDomain']} (or use 'az account show --query tenantId')")
print("======================================")
