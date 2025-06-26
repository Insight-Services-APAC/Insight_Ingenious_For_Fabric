# === STEP 5: Add API Permissions for Livy Endpoint ===
import subprocess

app_id = "7ca469d6-2266-4611-a022-8af7505df6b8"
print("\n=== Adding API permissions for Livy endpoint ===")

# Get tenant ID
tenant_id = (
    subprocess.check_output(
        ["az", "account", "show", "--query", "tenantId", "-o", "tsv"]
    )
    .decode()
    .strip()
)

# The Livy endpoint in Microsoft Fabric requires Azure Storage API permissions
# Azure Storage API ID: e406a681-f3d4-42a8-90b6-c2b029497af1
storage_api_id = "e406a681-f3d4-42a8-90b6-c2b029497af1"

# Add delegated permission for user_impersonation
print("Adding Azure Storage user_impersonation permission...")
add_permission_cmd = [
    "az",
    "ad",
    "app",
    "permission",
    "add",
    "--id",
    app_id,
    "--api",
    storage_api_id,
    "--api-permissions",
    "03e0da56-190b-40ad-a80c-ea378c433f7f=Scope",  # user_impersonation
]
subprocess.run(add_permission_cmd, check=True)

# For Fabric workloads, also add the PowerBI Service API permissions
# PowerBI Service API ID: 00000009-0000-0000-c000-000000000000
powerbi_api_id = "00000009-0000-0000-c000-000000000000"

print("Adding Power BI Service permissions...")
add_powerbi_permission_cmd = [
    "az",
    "ad",
    "app",
    "permission",
    "add",
    "--id",
    app_id,
    "--api",
    powerbi_api_id,
    "--api-permissions",
    "a65a6bd9-0978-46d6-a261-36b3e6fdd32e=Scope",  # Workspace.Read.All
]
subprocess.run(add_powerbi_permission_cmd, check=True)

# Grant admin consent for the permissions
print("Granting admin consent for the permissions...")
grant_consent_cmd = ["az", "ad", "app", "permission", "admin-consent", "--id", app_id]
try:
    subprocess.run(grant_consent_cmd, check=True)
    print("Admin consent granted successfully.")
except subprocess.CalledProcessError:
    print(
        "WARNING: Could not grant admin consent automatically. You may need to grant consent manually in the Azure Portal."
    )
    print(
        f"Portal URL: https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/CallAnAPI/appId/{app_id}"
    )
