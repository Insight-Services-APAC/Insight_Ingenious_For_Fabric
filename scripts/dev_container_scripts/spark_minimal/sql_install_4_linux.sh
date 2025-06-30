# SQL Server 
apt-get install gpg
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg 
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-preview.list | tee /etc/apt/sources.list.d/mssql-server-preview.list 
apt-get update -y 
apt-get install -y mssql-server

# SQL Server Client Tools 
curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
apt-get update
apt-get install mssql-tools18 unixodbc-dev