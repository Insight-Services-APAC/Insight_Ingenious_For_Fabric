# spark_minimal dev container for ingen_fab development (issue #39).
#
# History: this container was `FROM bitnami/spark:4.0-debian-12`. Bitnami removed its free
# image catalog from Docker Hub in 2025; the tag no longer resolves and the build fails.
# `bitnamilegacy/spark:4.0.0-debian-12-r20` (the fallback proposed in #39) does build and does
# carry Python 3.12.11, Java 17 and Spark 4.0.0, but it is frozen (no further updates), 2 GB,
# runs as an unprivileged user with no git, no sudo and no writable apt, and every developer
# tool still had to be installed by hand afterwards. The official apache/spark images are
# Ubuntu 22.04 with Python 3.10, below ingen_fab's >=3.12 requirement.
#
# What this image is instead: the Python the project requires, a JVM for local Spark sessions
# (the pyspark wheel in the dev dependency group bundles Spark itself, so only Java is needed),
# git for the two git-sourced dependencies, ODBC Driver 18 for pyodbc, and uv. The project
# venv is created by the devcontainer's postCreateCommand (uv sync --all-extras) into
# /opt/venv so it lives on the container filesystem, not on the slow Windows bind mount.

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ACCEPT_EULA=Y \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    FABRIC_ENVIRONMENT=local \
    PATH=/opt/venv/bin:/root/.local/bin:$PATH

# OS layer: JVM, git, ODBC Driver 18, and the small tools a terminal session expects.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless git curl gnupg2 ca-certificates unixodbc procps less \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
         | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
         > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# uv: the project's package manager (pyproject + uv.lock). Pinned so the image is reproducible.
RUN pip install uv==0.12.15

WORKDIR /workspaces/Insight_Ingenious_For_Fabric

CMD ["bash"]
