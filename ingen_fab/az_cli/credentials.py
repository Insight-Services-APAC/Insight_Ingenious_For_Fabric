"""One place that decides how ingen_fab authenticates to Azure and Fabric.

fabric-cicd 1.x requires an explicit ``TokenCredential`` (the library's default-credential
fallback was removed in 1.0.0), and the Fabric REST and OneLake helpers each used to build
their own ``DefaultAzureCredential``. This module gives them a single resolution order:

1. A credential passed in by the caller.
2. A service principal, when ``AZURE_TENANT_ID``, ``AZURE_CLIENT_ID`` and
   ``AZURE_CLIENT_SECRET`` are all set (CI/CD and automation).
3. ``DefaultAzureCredential`` without the interactive browser step: workload identity,
   managed identity, Azure CLI login (``az login``), and the other non-interactive sources
   the SDK supports, in the SDK's own order.
"""

from __future__ import annotations

import os
from typing import Optional

from azure.core.credentials import TokenCredential
from azure.identity import ClientSecretCredential, DefaultAzureCredential

SERVICE_PRINCIPAL_ENV_VARS = (
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
)


def service_principal_configured() -> bool:
    """True when the three service-principal environment variables are all non-empty."""
    return all(os.environ.get(name) for name in SERVICE_PRINCIPAL_ENV_VARS)


def get_token_credential(
    credential: Optional[TokenCredential] = None,
) -> TokenCredential:
    """Return the credential ingen_fab should use for Azure and Fabric APIs.

    Args:
        credential: An explicit credential, returned unchanged when given.
    """
    if credential is not None:
        return credential

    if service_principal_configured():
        return ClientSecretCredential(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"],
        )

    return DefaultAzureCredential(exclude_interactive_browser_credential=True)
