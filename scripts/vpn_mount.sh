#!/bin/bash

VPN_CONFIG="/etc/openvpn/client/client.conf"
SMB_CREDS="/root/.smbcredentials"
VPN_PID="/var/run/openvpn_watchguard.pid"
REMOTE_SERVER="10.5.0.8"
REMOTE_SHARE="//$REMOTE_SERVER/dados/pareto"
MOUNT_POINT="/mnt/pareto"

# Detect if running as root - if so, don't use sudo
if [ "$EUID" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
fi

cleanup() {
    echo ""
    echo "--- Stopping ---"
    
    if mountpoint -q "$MOUNT_POINT"; then
        echo "Unmounting $MOUNT_POINT..."
        $SUDO umount "$MOUNT_POINT" 2>/dev/null
    fi

    if $SUDO test -f "$VPN_PID"; then
        PID=$($SUDO cat "$VPN_PID")
        if [ -n "$PID" ]; then
            echo "Stopping OpenVPN (PID $PID)..."
            $SUDO kill "$PID" 2>/dev/null
        fi
        $SUDO rm -f "$VPN_PID"
    else
        $SUDO killall openvpn 2>/dev/null
    fi
    
    echo "Connections closed."
    exit
}

trap cleanup SIGINT SIGTERM

if ! $SUDO test -f "$VPN_CONFIG"; then
    echo "Critical Error: $VPN_CONFIG not found."
    exit 1
fi

if ! $SUDO test -f "$SMB_CREDS"; then
    echo "Critical Error: $SMB_CREDS not found."
    exit 1
fi

echo "--- Connecting ---"
# Add AES-128-CBC to data-ciphers to support the server's cipher
# Ignore block-outside-dns option (Windows-specific, not supported in OpenVPN 2.6.14)
$SUDO openvpn --config "$VPN_CONFIG" \
    --data-ciphers "AES-256-GCM:AES-128-GCM:CHACHA20-POLY1305:AES-128-CBC" \
    --ignore-unknown-option block-outside-dns \
    --daemon --writepid "$VPN_PID"

echo "Waiting for tunnel establishment..."
CONNECTED=false
for i in {1..30}; do
    if ping -c 1 -W 1 "$REMOTE_SERVER" &> /dev/null; then
        echo "VPN connected successfully!"
        CONNECTED=true
        break
    fi
    sleep 1
done

if [ "$CONNECTED" = false ]; then
    echo "Timeout: Unable to connect to server after 30 seconds."
    exit 1
fi

echo "Mounting network unit..."
$SUDO mkdir -p "$MOUNT_POINT"

CURRENT_UID=$(id -u)
CURRENT_GID=$(id -g)

OPTS="credentials=$SMB_CREDS,iocharset=utf8,file_mode=0777,dir_mode=0777,uid=$CURRENT_UID,gid=$CURRENT_GID"

if $SUDO mount -t cifs "$REMOTE_SHARE" "$MOUNT_POINT" -o "$OPTS"; then
    echo "Network unit mounted successfully at $MOUNT_POINT."
else
    echo "Error mounting the SMB share."
    exit 1
fi

echo "System ready. Press Ctrl+C to disconnect and unmount."

# If running in background (no TTY), only keep the process alive
# Checking periodically if the mount is still active
if [ ! -t 0 ]; then
    echo "Running in background mode. Monitoring mount status..."
    while true; do
        if ! mountpoint -q "$MOUNT_POINT"; then
            echo "Warning: Mount point lost. Exiting to allow restart..."
            exit 1
        fi
        if ! ping -c 1 -W 1 "$REMOTE_SERVER" &> /dev/null; then
            echo "Warning: VPN connection lost. Exiting to allow restart..."
            exit 1
        fi
        sleep 30
    done
else
    # Interactive mode: simple loop
    while true; do
        sleep 1
    done
fi

