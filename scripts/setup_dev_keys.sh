#!/bin/bash
# setup_dev_keys.sh - Setup SSH keys for development environment

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
KEYS_DIR="keys"
USERS_DIR="$KEYS_DIR/users"
CLIENT_KEYS_DIR="client_keys"

# Create directory structure
log_info "Setting up directory structure..."
mkdir -p "$KEYS_DIR" "$USERS_DIR" "$CLIENT_KEYS_DIR"
log_success "Directories created"

# Generate server host key (if not exists)
if [[ ! -f "$KEYS_DIR/ssh_host_rsa_key" ]]; then
    log_info "Generating server host key..."
    ssh-keygen -t rsa -b 4096 -f "$KEYS_DIR/ssh_host_rsa_key" -N "" -C "sftp-server-host"
    chmod 600 "$KEYS_DIR/ssh_host_rsa_key"
    chmod 644 "$KEYS_DIR/ssh_host_rsa_key.pub"
    log_success "Server host key generated"
else
    log_info "Server host key already exists"
fi

# Generate client keys and copy to server
users=("alice")

for user in "${users[@]}"; do
    client_key="$CLIENT_KEYS_DIR/${user}_key"
    server_pub="$USERS_DIR/${user}.pub"
    
    if [[ ! -f "$client_key" ]]; then
        log_info "Generating key pair for user: $user"
        ssh-keygen -t rsa -b 4096 -f "$client_key" -N "" -C "$user@sftp-client"
        chmod 600 "$client_key"
        chmod 644 "$client_key.pub"
        log_success "Generated key pair for $user"
    else
        log_info "Key pair for $user already exists"
    fi
    
    # Copy public key to server users directory
    cp "$client_key.pub" "$server_pub"
    chmod 644 "$server_pub"
    log_info "Copied public key to server: $server_pub"
done

# Verify key fingerprints
log_info "=== Key Fingerprints ==="
echo "Server host key:"
ssh-keygen -l -f "$KEYS_DIR/ssh_host_rsa_key.pub"

echo
echo "User keys:"
for user in "${users[@]}"; do
    echo "$user:"
    ssh-keygen -l -f "$USERS_DIR/${user}.pub"
done

log_success "Development environment setup complete!"

echo
echo "Next steps:"
echo "1. Update your .env with:"
echo "   SFTP_USERS=alice:secret:read+write+delete"
echo "2. Run: docker-compose up -d ingest_sftp"
echo "3. Test password auth: sftp -P 2222 alice@localhost"
echo "4. Test key auth: sftp -P 2222 -i client_keys/alice_key alice@localhost"