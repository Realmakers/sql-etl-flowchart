#!/bin/bash
# setup_env.sh

# Define Node.js version and directory
NODE_VERSION="v18.16.0"
NODE_DIR="node-$NODE_VERSION-darwin-arm64"
NODE_TAR="$NODE_DIR.tar.gz"
DOWNLOAD_URL="https://nodejs.org/dist/$NODE_VERSION/$NODE_TAR"

# Check if Node.js directory exists
if [ ! -d "$NODE_DIR" ]; then
    echo "Downloading Node.js $NODE_VERSION..."
    curl -L -o "$NODE_TAR" "$DOWNLOAD_URL"
    
    if [ $? -ne 0 ]; then
        echo "Failed to download Node.js. Please check your network connection."
        exit 1
    fi
    
    echo "Extracting Node.js..."
    tar -xzf "$NODE_TAR"
    
    if [ $? -ne 0 ]; then
        echo "Failed to extract Node.js."
        exit 1
    fi
    
    # Clean up tar file
    rm "$NODE_TAR"
    echo "Node.js installed successfully in $PWD/$NODE_DIR"
else
    echo "Node.js directory found."
fi

# Add Node.js to PATH
export PATH="$PWD/$NODE_DIR/bin:$PATH"

# Verify installation
echo "Node.js version: $(node -v)"
echo "npm version: $(npm -v)"

# Install pnpm globally using the local npm
echo "Checking for pnpm..."
if ! command -v pnpm &> /dev/null; then
    echo "Installing pnpm..."
    npm install -g pnpm
else
    echo "pnpm is already installed."
fi

# Navigate to frontend directory and install dependencies
PROJECT_DIR="app/frontend"
if [ -d "$PROJECT_DIR" ]; then
    echo "Navigating to $PROJECT_DIR..."
    cd "$PROJECT_DIR"
    
    echo "Installing project dependencies with pnpm..."
    pnpm install
else
    echo "Directory $PROJECT_DIR not found!"
    exit 1
fi

echo "Setup complete! To use node/npm/pnpm in the future, run:"
echo "export PATH=\"$PWD/../$NODE_DIR/bin:\$PATH\""
