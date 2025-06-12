#!/bin/bash

# MM Bronze Installation Script
# This script sets up the development environment for mm_bronze data ingestion platform

set -e  # Exit on any error

echo "Setting up MM Bronze development environment..."
echo

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to print colored output
print_status() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

print_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

print_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

print_warning() {
    echo -e "\033[1;33m[WARNING]\033[0m $1"
}

# Step 1: Install uv if not already installed
print_status "Checking for uv installation..."
if command_exists uv; then
    print_success "uv is already installed ($(uv --version))"
else
    print_status "Installing uv..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        curl -LsSf https://astral.sh/uv/install.sh | sh
    else
        # Linux and other Unix-like systems
        curl -LsSf https://astral.sh/uv/install.sh | sh
    fi
    
    # Add uv to PATH for current session
    export PATH="$HOME/.cargo/bin:$PATH"
    
    if command_exists uv; then
        print_success "uv installed successfully ($(uv --version))"
    else
        print_error "Failed to install uv. Please install manually from https://github.com/astral-sh/uv"
        exit 1
    fi
fi

# Step 2: Install dependencies with uv sync
print_status "Installing dependencies with uv sync..."
if uv sync; then
    print_success "Dependencies installed successfully"
else
    print_error "Failed to install dependencies"
    exit 1
fi

# Step 3: Copy example.env to .env
print_status "Setting up environment configuration..."
if [ -f "example.env" ]; then
    if [ -f ".env" ]; then
        print_warning ".env file already exists, backing up to .env.backup"
        cp .env .env.backup
    fi
    cp example.env .env
    print_success "Copied example.env to .env"
else
    print_error "example.env file not found!"
    exit 1
fi

# Step 4: Create data directory structure
print_status "Creating data directory structure..."
mkdir -p ./data/raw_storage
print_success "Created ./data/raw_storage directory"

# Step 5: Run setup_dev_keys.sh
print_status "Setting up development SSH keys..."
if [ -f "scripts/setup_dev_keys.sh" ]; then
    chmod +x scripts/setup_dev_keys.sh
    if ./scripts/setup_dev_keys.sh; then
        print_success "Development SSH keys set up successfully"
    else
        print_warning "SSH key setup completed with warnings (this is usually normal)"
    fi
else
    print_error "scripts/setup_dev_keys.sh not found!"
    exit 1
fi

echo
print_success "MM Bronze installation completed successfully!"
echo
echo "Next steps:"
echo "  1. Review and customize the .env file for your environment"
echo "  2. Start the services with: docker compose up"
echo "  3. Run tests with: uv run pytest"
echo
echo "For more information, see the README.md file."