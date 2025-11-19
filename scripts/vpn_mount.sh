#!/bin/bash

VPN_CONFIG="/etc/openvpn/client/client.conf"
SMB_CREDS="/root/.smbcredentials"
VPN_PID="/var/run/openvpn_watchguard.pid"
REMOTE_SERVER="10.5.0.8"
REMOTE_SHARE="//$REMOTE_SERVER/dados/pareto"
MOUNT_POINT="/mnt/pareto"

# Detect sudo
if [ "$EUID" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
fi

# Store original gateway (for GCP SSH protection)
ORIG_GW=$(ip -4 route show default | head -n1 | awk '{print $3}')
ORIG_DEV=$(ip -4 route show default | head -n1 | awk '{print $5}')

cleanup() {
    echo ""
    echo "--- Stopping ---"

    # Restore original default route
    echo "Restoring original routing..."
    $SUDO ip route replace default via "$ORIG_GW" dev "$ORIG_DEV" || true

    if mountpoint -q "$MOUNT_POINT"; then
        echo "Unmounting $MOUNT_POINT..."
        $SUDO umount "$MOUNT_POINT" 2>/dev/null
    fi

    if $SUDO test -f "$VPN_PID"; then
        PID=$($SUDO cat "$VPN_PID")
        if [ -n "$PID" ]; then
            echo "Stopping OpenVPN..."
            $SUDO kill "$PID" 2>/dev/null
        fi
        $SUDO rm -f "$VPN_PID"
    fi

    echo "Connections closed. VPN off."
    exit
}

trap cleanup SIGINT SIGTERM

# Validation
if ! $SUDO test -f "$VPN_CONFIG"; then
    echo "Critical Error: VPN config not found."
    exit 1
fi

if ! $SUDO test -f "$SMB_CREDS"; then
    echo "Critical Error: SMB credentials not found."
    exit 1
fi

echo "--- Preparing safe routing for GCP ---"

# Keep metadata server out of VPN (VERY IMPORTANT)
$SUDO ip route add 169.254.169.254 via "$ORIG_GW" dev "$ORIG_DEV" 2>/dev/null

echo "--- Connecting VPN ---"

$SUDO openvpn --config "$VPN_CONFIG" \
    --daemon --writepid "$VPN_PID"

echo "Waiting for tunnel..."

CONNECTED=false
for i in {1..30}; do
    if ip a | grep -q "tun"; then
        if ping -c 1 -W 1 "$REMOTE_SERVER" &> /dev/null; then
            CONNECTED=true
            break
        fi
    fi
    sleep 1
done

if [ "$CONNECTED" = false ]; then
    echo "VPN connection timeout."
    cleanup
fi

echo "VPN connected."

echo "Mounting network unit..."
$SUDO mkdir -p "$MOUNT_POINT"

CURRENT_UID=$(id -u)
CURRENT_GID=$(id -g)

OPTS="credentials=$SMB_CREDS,iocharset=utf8,file_mode=0777,dir_mode=0777,uid=$CURRENT_UID,gid=$CURRENT_GID,vers=3.0,noserverino"

if $SUDO mount -t cifs "$REMOTE_SHARE" "$MOUNT_POINT" -o "$OPTS"; then
    echo "Mounted successfully."
else
    echo "Failed to mount."
    cleanup
fi

echo "System ready. Press Ctrl+C to disconnect."

# Keep alive loop
while true; do
    if ! ping -c 1 -W 1 "$REMOTE_SERVER" &>/dev/null; then
        echo "VPN lost. Exiting."
        cleanup
    fi
    sleep 5
done