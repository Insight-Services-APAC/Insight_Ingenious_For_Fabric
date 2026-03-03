"""Ingenious Fabric Accelerator - CLI tool for Microsoft Fabric workspace management."""

import json
import re
import subprocess
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

_DISTRIBUTION_NAME = "insight-ingenious-for-fabric"
_TAG_PATTERN = re.compile(r"^v?\d+\.\d+\.\d+.*$")


def _with_v_prefix(raw_version: str) -> str:
    if raw_version.startswith("v"):
        return raw_version
    return f"v{raw_version}"


def _format_non_release() -> str:
    return "installed from non-release (main)"


def _read_direct_url(dist):
    try:
        direct_url_text = dist.read_text("direct_url.json")
        if not direct_url_text:
            return None
        return json.loads(direct_url_text)
    except (OSError, json.JSONDecodeError):
        return None


def _version_from_distribution() -> str:
    dist = distribution(_DISTRIBUTION_NAME)
    direct_url = _read_direct_url(dist) or {}
    vcs_info = direct_url.get("vcs_info") or {}

    requested_revision = vcs_info.get("requested_revision")
    commit_id = vcs_info.get("commit_id")

    if requested_revision and _TAG_PATTERN.match(requested_revision):
        return _with_v_prefix(requested_revision)

    if commit_id:
        return _format_non_release()

    # Editable/local install (file://) with no vcs_info. Derive commit from local repo.
    direct_url_text = direct_url.get("url") or ""
    if direct_url_text.startswith("file://"):
        repo_root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--short=8", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            short_commit = result.stdout.strip()
            if short_commit:
                return _format_non_release()

    return _with_v_prefix(dist.version)

try:
    __version__ = _version_from_distribution()
except PackageNotFoundError:
    __version__ = "v0+unknown"
