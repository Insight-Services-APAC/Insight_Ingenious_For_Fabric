# spark_minimal dev container for ingen_fab development.
#
# The Python the project requires, a JVM for local Spark sessions (the pyspark wheel in the dev
# dependency group bundles Spark itself, so only Java is needed), git for the two git-sourced
# dependencies, ODBC Driver 18 for pyodbc, and uv. The project venv is created by the
# devcontainer's postCreateCommand (uv sync --all-extras) under /opt/uv, a named volume shared
# with the uv cache, so it lives off the slow Windows bind mount and survives image rebuilds.
#
# Why this base image replaced bitnami/spark, and the tested fallback: see readme.md.

FROM python:3.12-slim-bookworm

# uv, pinned, copied from the official image: no PyPI round trip, nothing in system Python.
COPY --from=ghcr.io/astral-sh/uv:0.12.15 /uv /uvx /usr/local/bin/

# JAVA_HOME: /usr/lib/jvm/default-java is the link Debian maintains to the installed JDK on
# every architecture (java-17-openjdk-amd64 on x86_64, java-17-openjdk-arm64 on ARM64).
# UV_CACHE_DIR / UV_PROJECT_ENVIRONMENT: cache and venv on the same volume, so uv hardlinks
# from cache into the venv and a re-sync after a rebuild is a no-op. The venv is on PATH so
# `python`, `ingen_fab`, `az` and `ruff` resolve without activation.
# FABRIC_ENVIRONMENT: ingen_fab refuses even --help without it; "local" is the placeholder set.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/default-java \
    UV_CACHE_DIR=/opt/uv/cache \
    UV_PROJECT_ENVIRONMENT=/opt/uv/venv \
    FABRIC_ENVIRONMENT=local \
    PATH=/opt/uv/venv/bin:$PATH

# OS layer: JVM, git, ODBC Driver 18, and the small tools a terminal session expects.
# default-jre-headless pulls OpenJDK 17 and creates the default-java link. The Microsoft apt
# repository line uses the image's own architecture (the repository publishes msodbcsql18 for
# amd64 and arm64), so the image builds natively on ARM64 hosts. apt accepts the ASCII-armored
# key directly, so no gnupg is needed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless git curl ca-certificates unixodbc procps less \
    && install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc -o /etc/apt/keyrings/microsoft.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.asc] https://packages.microsoft.com/debian/12/prod bookworm main" \
         > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# devcontainer.json mounts the repository at /workspaces/<folder name> and sets that as the
# workspace folder; the image only fixes the stable parent.
WORKDIR /workspaces
