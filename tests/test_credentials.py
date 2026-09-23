"""ingen_fab.az_cli.credentials: the single credential resolution order."""

from unittest import mock

from azure.identity import ClientSecretCredential, DefaultAzureCredential

from ingen_fab.az_cli import credentials


def _clear_sp(monkeypatch):
    for name in credentials.SERVICE_PRINCIPAL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_explicit_credential_is_returned_unchanged(monkeypatch):
    _clear_sp(monkeypatch)
    sentinel = mock.Mock(name="credential")
    assert credentials.get_token_credential(sentinel) is sentinel


def test_service_principal_when_all_three_variables_are_set(monkeypatch):
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    cred = credentials.get_token_credential()
    assert isinstance(cred, ClientSecretCredential)
    assert credentials.service_principal_configured() is True


def test_partial_service_principal_falls_back_to_default(monkeypatch):
    _clear_sp(monkeypatch)
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    assert credentials.service_principal_configured() is False
    assert isinstance(credentials.get_token_credential(), DefaultAzureCredential)


def test_empty_values_do_not_count_as_configured(monkeypatch):
    monkeypatch.setenv("AZURE_TENANT_ID", "")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    assert credentials.service_principal_configured() is False


def test_default_credential_excludes_the_interactive_browser(monkeypatch):
    _clear_sp(monkeypatch)
    with mock.patch.object(
        credentials, "DefaultAzureCredential", wraps=DefaultAzureCredential
    ) as dac:
        credentials.get_token_credential()
    dac.assert_called_once_with(exclude_interactive_browser_credential=True)
