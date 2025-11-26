#!/bin/bash

set -euo pipefail

LOG_FILE=${LOG_FILE:-/var/log/vpn-mount.log}
mkdir -p "$(dirname "$LOG_FILE")"
exec > >(tee -a "$LOG_FILE")
exec 2>&1

VPN_CONFIG="/etc/openvpn/client/client.conf"
SMB_CREDS="/root/.smbcredentials"
VPN_PID="/var/run/wg_openvpn.pid"
REMOTE_SERVER="10.5.0.8"
REMOTE_SHARE="//$REMOTE_SERVER/dados/pareto"
MOUNT_POINT="/mnt/pareto"
MAX_PING_FAILURES=${MAX_PING_FAILURES:-10}
PING_INTERVAL=${PING_INTERVAL:-5}

if [ "$EUID" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
fi

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Critical Error: required command '$1' not found."
        exit 1
    fi
}

for bin in ip openvpn mount ping; do
    require_command "$bin"
done

ORIG_GW=$(ip -4 route show default | head -n1 | awk '{print $3}')
ORIG_DEV=$(ip -4 route show default | head -n1 | awk '{print $5}')

stop_openvpn() {
    if $SUDO test -f "$VPN_PID"; then
        PID=$($SUDO cat "$VPN_PID")
        if [ -n "$PID" ] && $SUDO kill -0 "$PID" 2>/dev/null; then
            echo "Stopping OpenVPN (PID: $PID)..."
            $SUDO kill "$PID" 2>/dev/null || true
            sleep 1
        fi
        $SUDO rm -f "$VPN_PID"
    fi

    if $SUDO pgrep -f "openvpn --config $VPN_CONFIG" >/dev/null 2>&1; then
        echo "Stopping stray OpenVPN processes..."
        $SUDO pkill -f "openvpn --config $VPN_CONFIG" || true
    fi
}

cleanup_mount() {
    if mountpoint -q "$MOUNT_POINT"; then
        echo "Unmounting $MOUNT_POINT..."
        $SUDO umount -f -l "$MOUNT_POINT" 2>/dev/null || true
        for i in {1..10}; do
            if ! mountpoint -q "$MOUNT_POINT"; then
                break
            fi
            sleep 1
        done
    fi
}

cleanup() {
    echo ""
    echo "--- Stopping ---"

    echo "Restoring original routing..."
    $SUDO ip route replace default via "$ORIG_GW" dev "$ORIG_DEV" || true

    cleanup_mount
    stop_openvpn

    echo "Connections closed. VPN off."
    exit
}

trap cleanup SIGINT SIGTERM

if ! $SUDO test -f "$VPN_CONFIG"; then
    echo "Critical Error: VPN config not found."
    exit 1
fi

if ! $SUDO test -f "$SMB_CREDS"; then
    echo "Critical Error: SMB credentials not found."
    exit 1
fi

echo "--- Resetting previous state ---"
cleanup_mount
stop_openvpn

echo "--- Preparing safe routing for GCP ---"

$SUDO ip route add 169.254.169.254 via "$ORIG_GW" dev "$ORIG_DEV" 2>/dev/null || true

echo "--- Connecting VPN ---"

$SUDO openvpn --config "$VPN_CONFIG" \
    --daemon --writepid "$VPN_PID"

echo "Waiting for tunnel..."

CONNECTED=false
for i in {1..30}; do
    if ip a | grep -q "tun" && ping -c 1 -W 1 "$REMOTE_SERVER" &> /dev/null; then
        CONNECTED=true
        break
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
    if $SUDO mount --make-shared "$MOUNT_POINT" 2>/dev/null; then
        echo "Mount propagation set to shared."
    else
        echo "Warning: could not set mount propagation to shared (continuing)."
    fi
else
    if mount | grep -q "^$REMOTE_SHARE on $MOUNT_POINT "; then
        echo "Share already mounted on $MOUNT_POINT. Reusing existing mount."
        echo "Mounted successfully."
    else
        echo "Failed to mount."
        cleanup
    fi
fi

echo "System ready. Press Ctrl+C to disconnect."

echo "--- Entering keep-alive loop ---"
PING_FAILURES=0
while true; do
    if ping -c 1 -W 1 "$REMOTE_SERVER" &>/dev/null; then
        PING_FAILURES=0
    else
        PING_FAILURES=$((PING_FAILURES + 1))
        echo "Ping failure $PING_FAILURES/$MAX_PING_FAILURES"
        if [ "$PING_FAILURES" -ge "$MAX_PING_FAILURES" ]; then
            echo "VPN lost after repeated ping failures. Exiting."
            cleanup
        fi
    fi
    sleep "$PING_INTERVAL"
done