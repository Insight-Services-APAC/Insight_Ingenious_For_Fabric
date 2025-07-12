###################################
# Prerequisites

# Update the list of packages
apt-get update

# Install pre-requisite packages.
apt-get install -y wget


if [ "$(uname -m)" = "aarch64" ]; then
    # If ARM, install the necessary packages for ARM architecture
    # ARM Version Below:
    apt install -y \
        libc6 \
        libgcc-s1 \
        libgssapi-krb5-2 \
        libicu72 \
        libssl3 \
        libstdc++6 \
        zlib1g
    wget https://github.com/PowerShell/PowerShell/releases/download/v7.5.2/powershell-7.5.2-linux-arm64.tar.gz
    mkdir ~/powershell
    tar -xvf powershell-*-linux-arm64.tar.gz -C ~/powershell
    chmod +x ~/powershell/pwsh
    ln -s ~/powershell/pwsh /usr/local/bin/pwsh
    # Remove the downloaded tar file
    rm powershell-*-linux-arm64.tar.gz
else
    # If not ARM, we assume it's x86_64 and proceed with the x86_64 installation
    echo "Detected architecture is not ARM, proceeding with x86_64 installation."
    # If x86_64, install the necessary packages for x86_64 architecture
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
fi

