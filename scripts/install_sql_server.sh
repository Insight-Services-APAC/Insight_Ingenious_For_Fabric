#!/bin/bash
set -e

# ───── CUSTOMIZE THESE ─────
export MSSQL_SA_PASSWORD='<YourStrong!Passw0rd>'
export MSSQL_PID='developer'                   # Options: evaluation, developer, express, standard, enterprise, or key
export SQL_ENABLE_AGENT='y'                    # 'y' to enable SQL Server Agent
export SQL_INSTALL_FULLTEXT='n'                # 'y' to install Full‑Text Search
# Optional sysadmin user:
# export SQL_INSTALL_USER='myadmin'
# export SQL_INSTALL_USER_PASSWORD='AnotherStr0ng!'
# ──────────────────────────

# Validate SA password
if [ -z "$MSSQL_SA_PASSWORD" ]; then
  echo "❌ MSSQL_SA_PASSWORD must be set"
  exit 1
fi

echo "🔧 Importing Microsoft GPG key and adding repos..."
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null

# SQL Server repo
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-2022.list | sudo tee /etc/apt/sources.list.d/mssql-server-2022.list >/dev/null

# Tools repo
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null

echo "📦 Updating package list..."
sudo apt-get update -y

echo "💾 Installing SQL Server Engine..."
sudo apt-get install -y mssql-server

echo "⚙️ Running initial configuration..."
sudo MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" MSSQL_PID="$MSSQL_PID" /opt/mssql/bin/mssql-conf -n setup accept-eula

echo "🔧 Installing tools and dependencies..."
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev

echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc

if [[ "$SQL_ENABLE_AGENT" == "y" ]]; then
  echo "✅ Enabling SQL Server Agent..."
  sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true
fi

if [[ "$SQL_INSTALL_FULLTEXT" == "y" ]]; then
  echo "📖 Installing Full‑Text Search..."
  sudo apt-get install -y mssql-server-fts
fi

echo "🌐 Configuring firewall (allowing port 1433)..."
sudo ufw allow 1433/tcp
sudo ufw reload || true

echo "🔁 Restarting SQL Server service..."
sudo systemctl restart mssql-server

# Optional: create an additional sysadmin user
if [[ -n "$SQL_INSTALL_USER" && -n "$SQL_INSTALL_USER_PASSWORD" ]]; then
  echo "👤 Adding sysadmin login '$SQL_INSTALL_USER'..."
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "
    CREATE LOGIN [$SQL_INSTALL_USER] WITH PASSWORD=N'$SQL_INSTALL_USER_PASSWORD';
    ALTER SERVER ROLE [sysadmin] ADD MEMBER [$SQL_INSTALL_USER];
  "
fi

echo "🎉 Installation complete!"
echo "To connect: sqlcmd -S localhost -U SA -P '<YourPassword>'"
