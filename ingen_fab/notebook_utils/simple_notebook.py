import base64
import json
import time
from typing import Optional

import requests
from azure.identity import DefaultAzureCredential


class SimpleFabricNotebook:
    """Create and run a simple Fabric notebook via REST API."""

    def __init__(
        self, workspace_id: str, *, credential: Optional[DefaultAzureCredential] = None
    ) -> None:
        self.workspace_id = workspace_id
        self.credential = credential or DefaultAzureCredential()
        self.base_url = "https://api.fabric.microsoft.com/v1/workspaces"

    def _get_token(self) -> str:
        scope = "https://api.fabric.microsoft.com/.default"
        token = self.credential.get_token(scope)
        return token.token

    def create_notebook(self, display_name: str, code: str) -> str:
        """Upload a new notebook and return its artifact id."""
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "cells": [
                {
                    "cell_type": "code",
                    "source": [code],
                    "execution_count": None,
                    "outputs": [],
                    "metadata": {},
                }
            ],
            "metadata": {"language_info": {"name": "python"}},
        }
        payload = base64.b64encode(json.dumps(nb).encode()).decode()
        body = {
            "displayName": display_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": payload,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        url = f"{self.base_url}/{self.workspace_id}/items"
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        return response.json()["id"]

    def run_notebook(self, notebook_id: str) -> str:
        """Start a notebook run and return the job instance id."""
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        url = f"{self.base_url}/{self.workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
        response = requests.post(url, headers=headers, json={})
        response.raise_for_status()
        location = response.headers.get("Location")
        job_id = location.rstrip("/").split("/")[-1] if location else None
        return job_id

    def get_job_status(self, notebook_id: str, job_id: str) -> str:
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        url = f"{self.base_url}/{self.workspace_id}/items/{notebook_id}/jobs/instances/{job_id}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("status")

    def run_and_wait(
        self, display_name: str, code: str, poll_interval: int = 30, timeout: int = 600
    ) -> str:
        notebook_id = self.create_notebook(display_name, code)
        job_id = self.run_notebook(notebook_id)
        if not job_id:
            raise RuntimeError("Failed to start notebook job")
        elapsed = 0
        status = "Unknown"
        while elapsed < timeout:
            status = self.get_job_status(notebook_id, job_id)
            if status not in {"Queued", "Running"}:
                break
            time.sleep(poll_interval)
            elapsed += poll_interval
        return status
