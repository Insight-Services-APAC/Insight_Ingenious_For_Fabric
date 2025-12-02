# Overview

This page provides the readers provide guidance to prepare Azure DevOps CI/CD for deployment to Microsoft Fabric Workspace.

# Pre-requisites

This section provides an overview of the design considerations and decision made for various components of Enterprise Data platform built in Microsoft Fabric.

## Subscription requirements

| **Requirement No** | **Requirements** | **Description** |
| --- | --- | --- |
| **REQ 1.1** | An Azure DevOps project | Create a new DevOps project or use an existing one. |
| **REQ 1.2** | MS Fabric workspaces | Create dedicated Fabric workspace for Dev,Test/UAT and Prod environment |
|  | Service Principal | * Create a new Service principal in Azure Entra and assign a default ‘Subscription’ * Save the ClientID and Client Secret in a key Vault * Azure Subscription name and id |
| **REQ 1.3** | Fabric Security Group | * As the best practice, create a new security group in Microsoft Entra ID * Add the created service principal as a member of this security group |
| **REQ 1.4** | Source Control | Create a dedicated repository within the DevOps project for Fabric artifacts and grant the service principal ‘Contributor’ access. |
| **REQ 1.5** | Azure DevOps Service Connections | Create a service connection in Azure DevOps using authentication through service principal to connect to Fabric. Section **4** “**Azure DevOps CI/CD Service Connection Setup** Guide for Microsoft Fabric” provides details of steps. |
| **REQ 1.6** | Resource with Admin access | Azure Admin – contact and email address  Fabric Admin – contact and email address  Azure DevOps Admin– contact and email address |
| **REQ 1.7** | DevOps Build User Access | Grant nominated user admin permission on the Azure DevOps Project |
| **REQ 1.8** | Key vault (Optional) |  |

The following sections provide details of the configuration.

## User License/Subscription/Access requirements

| **Requirement No** | **Requirements** | **Description** |
| --- | --- | --- |
| **L-REQ 1.1** | Azure DevOps License | For the user who will configure CI/CD pipelines in Azure DevOps. |
| **L-REQ 1.3** | Azure Portal Access | For the user who will provision Fabric capacity and Service Principal (SPN) in Azure. |
| **L-REQ 1.4** | Fabric Capacity Admin | For the user who will assign capacity to users or workspace |
| **L-REQ 1.5** | Fabric Admin | For the user who will update Fabric tenant level settings in Fabric/PowerBI portal |

# Set Up Environments in DevOps

## Creating the Environments (DEV, UAT, PROD)

### 1. Environments in Azure DevOps represent the resources (like VMs, Kubernetes clusters, or services) where your application is deployed.
### 2. Navigate to your Azure DevOps project.
### 3. In the left-hand navigation menu, click on Pipelines.
### 4. Click on Environments.
### 5. Click the New environment button.
### 6. Create the DEV Environment:
- Name: DEV
- Resource: Select None
- Click Create.
### 7. Repeat the process for the remaining two environments:
- Name: UAT
- Name: PROD

You should now see three new environments listed: DEV, UAT, and PROD.

## Creating the Variable Groups (Dev, UAT, Production)

Variable Groups are a collection of variables that you can use across multiple pipelines and stages.

### 1. In the left-hand navigation menu, under Pipelines, click on Library.
### 2. Click the + Variable group button.
### 3. Create the Dev Variable Group:
- Variable group name: Dev
- Description: (Optional) Variable group for Development environment.
- Add Variables:
- Click + Add.
- Name: FABRIC\_ENVIRONMENT
- Value: development
- Click + Add.
- Name: IS\_SINGLE\_WORKSPACE
- Value: Y
- Click Save.

## Repeat for UAT and Production

Repeat the process to create the remaining two variable groups, ensuring the FABRIC\_ENVIRONMENT variable is set correctly for each.

|  |  |  |
| --- | --- | --- |
| Variable Group Name | Variable | Value |
| UAT | FABRIC\_ENVIRONMENT | UAT |
|  | IS\_SINGLE\_WORKSPACE | Y |
| Production | FABRIC\_ENVIRONMENT | production |
|  | IS\_SINGLE\_WORKSPACE | Y |

Once finished, you will have three separate Variable Groups (Dev, UAT, Production) and three separate Environments (DEV, UAT, PROD). You can now link these Variable Groups to the corresponding stages in your YAML or Classic pipelines.

# Azure DevOps CI/CD Service Connection Setup Guide for Microsoft Fabric

This guide outlines the mandatory steps to configure a Service Connection in Azure DevOps, using a Service Principal, for deploying code to Microsoft Fabric via the Fabric CICD deployment library.

The process is divided into four main configuration phases:

## Phase 1: Create and Configure the Service Principal (Microsoft Entra ID)

You must first register an application in Microsoft Entra ID (Azure AD) to create the Service Principal (SPN) identity that the DevOps pipeline will use.

|  |  |  |
| --- | --- | --- |
| Step | Action | Details |
| 1.1 | **Register a New Application** | Navigate to the **Azure Portal** → **Microsoft Entra ID** → **App registrations** → **New registration**. Give it a meaningful name (e.g., fabric-cicd-spn). |
| 1.2 | **Capture Credentials** | After creation, note down the following required values for the Service Connection: **Application (client) ID** (Service Principal ID) **Directory (tenant) ID** (Tenant ID) |
| 1.3 | **Create a Client Secret** | Go to **Certificates & secrets** → **Client secrets** → **New client secret**. Set an appropriate expiry (e.g., 1 or 2 years). **Immediately copy the Secret Value** (not the Secret ID), as it will be masked after you leave the page. |
| 1.4 | **Configure API Permissions** | Go to **API permissions** → **Add a permission** → **Fabric BI Service** (or search for the relevant Fabric API scope if explicitly available).  Choose **Application permissions** (required for Service Principal/non-interactive calls). Select the following minimum required permission: OneLake.ReadWrite.All, Tenant.ReadWrite.All (Recommended for general Fabric deployment and CI/CD operations) **OR** Workspace.ReadWrite.All (If you only need to manage content within assigned workspaces).  **Mandatory:** After selecting the permissions, click **Grant admin consent for [Your Tenant Name]** to activate the Application Permissions. |
| 1.5 | **Grant Service Account the ‘Reader’ role on the Subscription** | See section 4.1.1 **Grant Service Account the Subscription access** |

![This image shows the "API permissions" page for an Azure Active Directory application named **fabric-devops-sp** in the Azure portal. - The left sidebar contains navigation options such as Overview, Quickstart, Integration assistant, and others under the "Manage" section, including API permissions (highlighted). - The main pane displays a list of configured API permissions for the app. - There are three permissions listed under "Power BI Service": - **OneLake.ReadWrite.All** (Delegated, no admin consent required) - **Tenant.ReadWrite.All** (Application, admin consent required and granted) - **Workspace.ReadWrite.All** (Delegated, no admin consent required) - Status icons indicate which permissions have been granted. - There are informational messages about tenant-wide consent and admin consent requirements at the top.](./images/image_001.png)

Figure 1 Example: SPN Permissions in Azure Entra

### Grant Service Account the Subscription access

Go to **Azure Portal** 🡪 **Subscription** 🡪 **Select the subscription you want to use** 🡪 **Access Control(IAM)** 🡪 **Add**

![This image shows the "Access control (IAM)" section of the Azure portal for a "Visual Studio Enterprise Subscription." The left sidebar includes navigation options like Overview, Activity log, Access control (IAM), Tags, and Diagnose and solve problems. The main pane displays tabs for Check access, Role assignments, Roles (selected), Deny assignments, and Classic administrators. There is a description explaining that a role definition is a collection of permissions, with options to view All roles, Job function roles, and Privileged administrator roles. There are also buttons for Add, Download role assignments, Refresh, Delete, and Feedback at the top.](./images/image_002.png)

Select **‘Reader’** role 🡪 **Next**

![This image shows the "Add role assignment" screen in the Azure portal, specifically under "Access control (IAM)" for a Visual Studio Enterprise Subscription. The interface allows users to assign roles to manage access to Azure resources. Key elements: - Tabs at the top: **Role**, **Members**, **Conditions**, **Review + assign**. - A description explaining that a role definition is a collection of permissions. - A search bar to filter roles by name, description, permission, or ID. - Role list table with columns for **Name** and **Description** (e.g., Reader, AcrDelete, AcrPull). - Buttons at the bottom: **Review + assign**, **Previous**, and **Next** (with "Next" highlighted). - The interface uses a dark theme.](./images/image_003.png)

Select **‘User, Group or Service Principal’**🡪 **Find the SPN name from search box 🡪 click the account 🡪 Click ‘Select’**

![This image shows the "Add role assignment" screen in the Azure portal, specifically under Access control (IAM) for a Visual Studio Enterprise Subscription. - The "Members" tab is active. - The selected role is "Reader." - The assignment is set to "User, group, or service principal." - No members have been selected yet in the main pane. - On the right, the "Select members" dialog is open, with a search for "fabri." - Several applications and groups matching "fabri" are listed. - "Fabric_api_app" (Application) is selected as a member. - At the bottom, there are "Select" and "Close" buttons to confirm or cancel the selection.](./images/image_004.png)

## Phase 2: Configure Tenant Settings in Microsoft Fabric

The Service Principal must be granted permission both at the tenant level (Admin setting) and the workspace level (Role assignment).

**A. Fabric Admin Portal Configuration (Tenant Level)**

![This image shows the "Admin API settings" configuration screen for enabling service principals to access read-only admin APIs in a Microsoft environment (likely Power BI or Microsoft Fabric). Key points: - **Service principals** can access read-only admin APIs, but only for a subset of the organization. - The feature is **Enabled** (toggle switch is on). - The setting applies to **Specific security groups** (not the entire organization). - The allowed security group is **FabricSecurityGroup**. - There is an option to "Except specific security groups," which is currently unchecked. The description explains that web apps registered in Microsoft Entra ID can use service principals for authentication, and only those added to allowed security groups will have read-only access to admin APIs.](./images/image_005.png)Sign in to the **Fabric admin portal**( <https://app.powerbi.com/admin-portal> ) as Fabric Admin. This step ensures that SPNs are generally allowed to interact with Fabric APIs.

|  |  |  |
| --- | --- | --- |
| Step | Action | Details |
| 2.1 | **Access Admin Settings** | In the **Microsoft Fabric portal**, go to **Settings** → **Admin Portal** → **Tenant settings**. |
| 2.2 | **Enable API Access** | * Locate the setting **"Service principals can use Fabric APIs"** (often under "Developer settings"). Turn it **ON**. * Locate the setting “**Service principals can access admin APIs used for update**”. Turn it **ON**. |
| 2.3 | **Apply Security Group** | If you restrict this setting to specific security groups, ensure your newly created Service Principal is added to that group. |

![This image shows a configuration screen for enabling service principals to call Fabric public APIs in an organization. The setting is currently **Enabled** (toggle switch is on). You can choose to apply this setting to: - The entire organization (radio button not selected) - Specific security groups (radio button selected) The selected security group is **FabricSecurityGroup**. There is also an option to "Except specific security groups" (checkbox not checked). At the bottom, there are **Apply** and **Cancel** buttons (both currently disabled). The description explains that service principals with the right roles and item permissions can call Fabric public APIs, and provides a link for more information.](./images/image_006.png)Figure 2 Example: o Service principals can access read-only admin APIs

Figure 3 Example: Service principals can access admin APIs used for update

**B. Workspace Role Assignment (Control Plane Permissions)**

The SPN needs a role on every source and target workspace in your deployment pipeline (e.g., Dev, Test, Prod).

|  |  |  |
| --- | --- | --- |
| Step | Action | Details |
| 2.4 | **Navigate to Workspace** | In the Fabric portal, go to the target workspace (e.g., "Dev"). |
| 2.5 | **Grant Access** | Click **Manage access** or **Workspace settings** → **Workspace access** → **Add people or groups**. |
| 2.6 | **Assign Role** | Search for the name of your Service Principal (fabric-cicd-spn). Assign it the **Contributor** or **Admin** role (Contributor is usually sufficient for deployment tasks). Repeat this for all relevant workspaces. |

## Phase 3: Create the Service Connection (Azure DevOps)

This connects the Service Principal's identity to your Azure DevOps project, allowing pipelines to authenticate.

|  |  |  |
| --- | --- | --- |
| Step | Action | Details |
| 3.1 | **Navigate to Project Settings** | In Azure DevOps, go to your project → **Project settings** (bottom left). |
| 3.2 | **Start New Connection** | Select **Service connections** → **New service connection**. |
| 3.3 | **Select Connection Type** | Choose **Azure Resource Manager** (recommended, although the pipeline ultimately calls the Fabric API, using this type correctly handles the SPN credentials). Select **Service principal (manual)**. |
| 3.4 | **Enter Authentication Details** | In the manual setup form, enter the following: |
|  | **Subscription ID** | *If your Fabric capacity is tied to a specific Azure subscription, enter it here. Otherwise, you may choose 'Management Group' or 'All subscriptions'.* |
|  | **Subscription Name** | The name associated with the Subscription ID. |
|  | **Service principal ID** | The **Application (client) ID** from Phase 1.2. |
|  | **Service principal key** | The **Secret Value** from Phase 1.3. |
|  | **Tenant ID** | The **Directory (tenant) ID** from Phase 1.2. |
| 3.5 | **Name the Connection** | Give it a clear name (e.g., Fabric-CICD-SPN-Connection). This is the name you will reference in your YAML pipeline. |
| 3.6 | **Verify and Save** | Select **Verify and save**. The verification should pass if the SPN is active and the credentials are correct. |

### Grant Federated Credential Permission

**Open the SPN in the Azure Portal🡪 Certificate & Select 🡪 Go to ‘Federated credentials’ tab 🡪 ‘Add Credential’**

![This image shows the "Certificates & secrets" section of an Azure App Registration named **purview-spn** in the Azure portal. The left sidebar lists options like Overview, Quickstart, Integration assistant, Diagnose and solve problems, Branding & properties, Authentication, Certificates & secrets (highlighted), Token configuration, API permissions, and Expose an API. The main pane displays three tabs: Certificates, Client secrets, and Federated credentials (selected). It explains that federated credentials allow other identities to impersonate this application by establishing trust with an external OpenID provider. There is an "Add credential" button and a table for listing federated credentials, which currently has one entry.](./images/image_007.png)

Fill the details. You need to get the Issuer and Subject details from Azure DevOps portal and use here.

![This image shows the "Edit service connection" dialog for Azure Resource Manager in Azure DevOps. It allows users to configure a service connection using App registration or managed identity (manual). Key fields and sections: - **Application (client) ID**: A text box for entering the Azure AD application (client) ID. - **Directory (tenant) ID**: A text box for entering the Azure AD tenant ID. - **Federation details**: Provides a link to a guide for adding a federated credential in Azure. - **Issuer**: A text box for the issuer URL, typically starting with `https://login.microsoftonline.com/`. - **Subject identifier**: A text box for the federation subject identifier, automatically created for the service connection. - **Security**: A checkbox to grant access permission to all pipelines. - **Buttons**: "Cancel" and "Verify and save" for submitting or canceling the configuration. Some sensitive information is obscured for privacy.](./images/image_008.png)

![This image shows the "Edit a credential" screen in the Azure portal, specifically under "App registrations" for an application named "purview-spn". Key sections and fields: - **Federated credential scenario:** Dropdown set to "Other issuer". - **Connect your account:** - **Issuer:** Text field with a URL starting with "https://login.microsoftonline.com/" (partially obscured). - **Type:** Radio buttons for "Explicit subject identifier" (selected) and "Claims matching expression (Preview)". - **Value:** Text field with a long string (partially obscured). - **Credential details:** - **Name:** Field with "purview-spn-cred-for-cicd-service-conn". - **Description:** Empty field (limit of 600 characters). - **Buttons:** "Update" and "Cancel" at the bottom. This screen is used to configure a federated credential for an Azure application, allowing it to authenticate using an external identity provider.](./images/image_009.png)

## Phase 4: Configure Repository Security for the Build Service

When a pipeline runs, it uses an identity called the Project Collection Build Service ([YourProjectName] Build Service ([YourOrganization])). This identity must have permission to use the Service Connection created in Phase 3.

|  |  |  |
| --- | --- | --- |
| Step | Action | Details |
| 4.1 | **Access Service Connection Security** | Go back to **Project settings** → **Service connections**. Select the connection you just created (Fabric-CICD-SPN-Connection). |
| 4.2 | **Set Pipeline Permissions** | Select the **Security** tab. |
| 4.3 | **Grant 'User' Role** | Locate the Project Collection Build Service user (e.g., [Fabric-Project] Build Service) and ensure it has the **User** role. |
| 4.4 | **Authorize Pipeline Use** | **Option A (Recommended):** After the pipeline fails its first run (due to lack of permission), a banner will appear asking you to permit the use of the resource. Select **Permit**. |
|  | **Option B (Less Secure):** On the Service Connection's **Security** tab, you can select the checkbox to **Grant access permission to all pipelines**. Only use this if you fully trust all pipelines in the project. |  |

![This image shows the **Security** settings for a repository in **Azure DevOps**. On the left, the **Project Settings** menu is visible, listing options like Overview, Teams, Permissions, Boards, Pipelines, and more. In the center, a list of repositories is displayed, with "if_demo" selected. On the right, the **User permissions** panel is open for the "Fabric Build Service (jpvanheerden0677)" user. Various permissions are listed, such as "Advanced Security: manage and dismiss alerts," "Bypass policies when pushing," and "Contribute." The "Contribute" permission is highlighted and set to "Allow." Other permissions are mostly set to "Not set." The panel also shows Azure DevOps Groups and individual users.](./images/image_010.png)

Figure 4 Example: Repository access for SPN

![This image shows the **Certificates & secrets** section of an Azure App Registration named **purview-spn** in the Microsoft Azure portal. The left sidebar displays navigation options like Overview, Quickstart, Integration assistant, and others under the "Manage" category. The main pane is focused on managing credentials for the application, with tabs for **Certificates**, **Client secrets**, and **Federated credentials**. The **Federated credentials** tab is selected, showing one federated credential named **purview-spn-cred-for-cicd-service-conn** with its subject identifier or claims matching expression partially visible. There are options to add new credentials and delete existing ones. The top bar includes search, feedback, and user account information.](./images/image_011.png)