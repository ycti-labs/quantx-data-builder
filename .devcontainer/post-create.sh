#!/bin/bash
set -e

echo "🚀 Running post-create setup for QuantX Data Builder..."

# Install root dependencies
echo "📦 Installing root dependencies..."
# pip install --no-cache-dir -r requirements.txt

# Install Azure Functions dependencies
echo "📦 Installing Azure Functions dependencies..."
# cd azure_functions && pip install --no-cache-dir -r requirements.txt && cd ..

# Install Container dependencies
echo "📦 Installing Container dependencies..."
# cd container && pip install --no-cache-dir -r requirements.txt && cd ..

# Set up pre-commit hooks (optional)
# echo "🔧 Setting up development tools..."
# if [ -f ".pre-commit-config.yaml" ]; then
#     pip install pre-commit
#     pre-commit install
# fi

# Create .env template if it doesn't exist
if [ ! -f ".env" ]; then
    echo "📝 Creating .env template..."
    cat > .env << 'EOF'
# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT_NAME=
AZURE_STORAGE_CONTAINER_NAME=quantx-data
AZURE_TENANT_ID=

# Local Development Settings
LOCAL_DATA_ROOT=./data
LOCAL_META_ROOT=./meta

# Logging
LOG_LEVEL=INFO
EOF
fi

# Initialize git config for line endings
git config core.autocrlf false

# Verify installations
echo "✅ Verifying installations..."
python --version
pip --version
func --version
az --version

echo "✨ Setup complete! You're ready to start developing."
echo ""
echo "Quick start commands:"
echo "  - Run tests: pytest tests/"
echo "  - Format code: black ."
echo "  - Type check: mypy src/"
echo "  - Start Azure Functions: cd azure_functions && func start"
echo ""
