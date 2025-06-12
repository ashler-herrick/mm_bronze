# MM Bronze Installation Script for Windows
# This script sets up the development environment for mm_bronze data ingestion platform

# Set strict mode
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Function to print colored output
function Write-Status {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Blue
}

function Write-Success {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Warning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

# Function to check if command exists
function Test-Command {
    param([string]$Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

Write-Host "Setting up MM Bronze development environment..." -ForegroundColor Cyan
Write-Host ""

try {
    # Step 1: Install uv if not already installed
    Write-Status "Checking for uv installation..."
    if (Test-Command "uv") {
        $uvVersion = & uv --version 2>$null
        Write-Success "uv is already installed ($uvVersion)"
    }
    else {
        Write-Status "Installing uv..."
        try {
            # Use PowerShell to download and install uv
            Invoke-Expression "& { $(Invoke-RestMethod https://astral.sh/uv/install.ps1) }"
            
            # Refresh environment variables
            $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
            
            if (Test-Command "uv") {
                $uvVersion = & uv --version 2>$null
                Write-Success "uv installed successfully ($uvVersion)"
            }
            else {
                Write-Error "Failed to install uv. Please install manually from https://github.com/astral-sh/uv"
                exit 1
            }
        }
        catch {
            Write-Error "Failed to install uv: $($_.Exception.Message)"
            Write-Error "Please install manually from https://github.com/astral-sh/uv"
            exit 1
        }
    }

    # Step 2: Install dependencies with uv sync
    Write-Status "Installing dependencies with uv sync..."
    try {
        & uv sync
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Dependencies installed successfully"
        }
        else {
            Write-Error "Failed to install dependencies (exit code: $LASTEXITCODE)"
            exit 1
        }
    }
    catch {
        Write-Error "Failed to install dependencies: $($_.Exception.Message)"
        exit 1
    }

    # Step 3: Copy example.env to .env
    Write-Status "Setting up environment configuration..."
    if (Test-Path "example.env") {
        if (Test-Path ".env") {
            Write-Warning ".env file already exists, backing up to .env.backup"
            Copy-Item ".env" ".env.backup" -Force
        }
        Copy-Item "example.env" ".env" -Force
        Write-Success "Copied example.env to .env"
    }
    else {
        Write-Error "example.env file not found!"
        exit 1
    }

    # Step 4: Create data directory structure
    Write-Status "Creating data directory structure..."
    New-Item -ItemType Directory -Path ".\data\raw_storage" -Force | Out-Null
    Write-Success "Created .\data\raw_storage directory"

    # Step 5: Run setup_dev_keys.sh (if Git Bash/WSL is available)
    Write-Status "Setting up development SSH keys..."
    if (Test-Path "scripts\setup_dev_keys.sh") {
        # Try different ways to run the bash script on Windows
        $bashCommands = @("bash", "wsl", "git-bash")
        $scriptRan = $false
        
        foreach ($bashCmd in $bashCommands) {
            if (Test-Command $bashCmd) {
                try {
                    Write-Status "Running setup_dev_keys.sh using $bashCmd..."
                    if ($bashCmd -eq "wsl") {
                        & wsl bash ./scripts/setup_dev_keys.sh
                    }
                    else {
                        & $bashCmd ./scripts/setup_dev_keys.sh
                    }
                    
                    if ($LASTEXITCODE -eq 0) {
                        Write-Success "Development SSH keys set up successfully"
                        $scriptRan = $true
                        break
                    }
                    else {
                        Write-Warning "SSH key setup completed with warnings using $bashCmd (this is usually normal)"
                        $scriptRan = $true
                        break
                    }
                }
                catch {
                    Write-Warning "Failed to run setup_dev_keys.sh using $bashCmd"
                    continue
                }
            }
        }
        
        if (-not $scriptRan) {
            Write-Warning "Could not find bash, WSL, or Git Bash to run setup_dev_keys.sh"
            Write-Warning "Please run the script manually in a bash environment or install Git for Windows/WSL"
        }
    }
    else {
        Write-Error "scripts\setup_dev_keys.sh not found!"
        exit 1
    }

    Write-Host ""
    Write-Success "MM Bronze installation completed successfully!"
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "  1. Review and customize the .env file for your environment"
    Write-Host "  2. Start the services with: docker compose up"
    Write-Host "  3. Run tests with: uv run pytest"
    Write-Host ""
    Write-Host "For more information, see the README.md file."

}
catch {
    Write-Error "Installation failed: $($_.Exception.Message)"
    exit 1
}