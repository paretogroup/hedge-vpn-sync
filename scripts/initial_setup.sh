#!/bin/bash

set -e

echo "=========================================="
echo "Hedge VPN Sync - Initial Setup Script"
echo "=========================================="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

if [ "$EUID" -ne 0 ]; then 
    error "Please run this script with sudo"
    exit 1
fi

info "Installing basic dependencies..."
apt-get install -y \
    curl \
    wget \
    git \
    ca-certificates \
    gnupg \
    lsb-release \
    openvpn \
    cifs-utils \
    net-tools \
    iputils-ping \
    > /dev/null

info "Installing Docker..."
apt-get remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true

if [ -f /etc/apt/sources.list.d/docker.list ]; then
    info "Removing old Docker repository configuration..."
    rm -f /etc/apt/sources.list.d/docker.list
fi

if [ -f /etc/os-release ]; then
    . /etc/os-release
    DISTRO_ID="$ID"
    DISTRO_CODENAME="$VERSION_CODENAME"
else
    error "Cannot detect distribution. /etc/os-release not found."
    exit 1
fi

if [ "$DISTRO_ID" != "ubuntu" ] && [ "$DISTRO_ID" != "debian" ]; then
    error "Unsupported distribution: $DISTRO_ID. This script supports Ubuntu and Debian only."
    exit 1
fi

info "Detected distribution: $DISTRO_ID $DISTRO_CODENAME"

install -m 0755 -d /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
    curl -fsSL https://download.docker.com/linux/$DISTRO_ID/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
fi

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/$DISTRO_ID \
  $DISTRO_CODENAME stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null

info "Updating package list..."
apt-get update -qq
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin > /dev/null

info "Checking Docker installation..."
if command -v docker &> /dev/null && docker --version &> /dev/null; then
    info "Docker installed successfully: $(docker --version)"
else
    error "Docker installation failed"
    exit 1
fi

if command -v docker compose &> /dev/null && docker compose version &> /dev/null; then
    info "Docker Compose installed successfully: $(docker compose version)"
else
    error "Docker Compose installation failed"
    exit 1
fi

if [ -n "$SUDO_USER" ]; then
    info "Adding user $SUDO_USER to docker group..."
    usermod -aG docker "$SUDO_USER"
    info "User $SUDO_USER added to docker group"
    warn "The user needs to logout and login again to use Docker without sudo"
fi

info "Creating necessary directories..."
mkdir -p /opt/hedge-vpn-sync
mkdir -p /mnt/pareto
mkdir -p /var/log
mkdir -p /etc/openvpn/client

touch /var/log/vpn_mount.log
touch /var/log/vpn-sync-cron.log
chmod 666 /var/log/vpn_mount.log
chmod 666 /var/log/vpn-sync-cron.log

info "Configuring permissions..."
chown -R "$SUDO_USER:$SUDO_USER" /opt/hedge-vpn-sync 2>/dev/null || true

info "Checking VPN configuration files..."
VPN_CONFIG="/etc/openvpn/client/client.conf"
SMB_CREDS="/root/.smbcredentials"

if [ ! -f "$VPN_CONFIG" ]; then
    warn "OpenVPN configuration file not found: $VPN_CONFIG"
    warn "You will need to create this file manually before using vpn_mount.sh"
fi

if [ ! -f "$SMB_CREDS" ]; then
    warn "SMB credentials file not found: $SMB_CREDS"
    warn "You will need to create this file manually before using vpn_mount.sh"
    warn "Expected format:"
    warn "  username=your_username"
    warn "  password=your_password"
fi

if [ -d "/opt/hedge-vpn-sync/.git" ]; then
    info "Repository already exists in /opt/hedge-vpn-sync"
else
    info "Repository has not been cloned yet"
    info "Execute the GitHub Actions deploy workflow to clone the repository"
fi

echo ""
echo "=========================================="
info "Initial setup completed successfully!"
echo "=========================================="
echo ""
info "Next steps:"
echo "  1. Configure the OpenVPN configuration file: $VPN_CONFIG"
echo "  2. Configure the SMB credentials: $SMB_CREDS"
echo "  3. Execute the GitHub Actions deploy workflow to clone and configure the repository"
echo "  4. Configure the necessary environment variables (file .env or system variables)"
echo ""
info "To verify the installation:"
echo "  - Docker: docker --version"
echo "  - Docker Compose: docker compose version"
echo "  - OpenVPN: openvpn --version"
echo ""