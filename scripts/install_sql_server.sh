#!/usr/bin/env bash
# Unattended installation of SQL Server on Ubuntu
# Requires SA_PASSWORD environment variable to be set
set -euo pipefail

if [ -z "${SA_PASSWORD:-}" ]; then
  echo "SA_PASSWORD environment variable must be set" >&2
  exit 1
fi

# Install prerequisites
sudo apt-get update
sudo apt-get install -y wget curl gnupg2 software-properties-common

# Import the public repository GPG keys
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
# Register the Microsoft SQL Server Ubuntu repository
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/mssql-server-2019.list \
  | sudo tee /etc/apt/sources.list.d/mssql-server.list

sudo apt-get update
sudo apt-get install -y mssql-server

sudo MSSQL_PID=Developer MSSQL_SA_PASSWORD="$SA_PASSWORD" \
  /opt/mssql/bin/mssql-conf -n setup accept-eula

sudo systemctl enable --now mssql-server
