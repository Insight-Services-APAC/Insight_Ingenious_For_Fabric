###################################
# Prerequisites

# Update the list of packages
apt-get update

# Install pre-requisite packages.
apt-get install -y wget

# Get the version of Debian
source /etc/os-release

# Download the Microsoft repository GPG keys
wget -q https://packages.microsoft.com/config/debian/$VERSION_ID/packages-microsoft-prod.deb

# Register the Microsoft repository GPG keys
dpkg -i packages-microsoft-prod.deb

# Delete the Microsoft repository GPG keys file
rm packages-microsoft-prod.deb

# Update the list of packages after we added packages.microsoft.com
apt-get update

###################################
# Install PowerShell
apt-get install -y powershell

# Start PowerShell
pwsh