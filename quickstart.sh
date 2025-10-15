#!/bin/bash

# Quick Start Script for QuantX Data Builder
# This script helps you get started quickly with local development

set -e

echo "🚀 QuantX Data Builder - Quick Start"
echo "====================================="
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "⚙️  Creating .env file..."
    cat > .env << EOF
# Azure Storage (for cloud deployment)
AZURE_STORAGE_ACCOUNT=your_storage_account
AZURE_CONTAINER_NAME=finsight-data
AZURE_STORAGE_CONNECTION_STRING=your_connection_string

# Container Apps (for CLI)
FETCHER_AZURE_STORAGE_ACCOUNT=your_storage_account
FETCHER_AZURE_CONTAINER_NAME=finsight-data
FETCHER_LOG_LEVEL=INFO
USE_MANAGED_IDENTITY=false

# Local Development
LOCAL_STORAGE_ROOT=./data
EOF
    echo "⚠️  Please edit .env with your Azure credentials"
fi

# Create data directory
mkdir -p data/meta

echo ""
echo "✅ Setup complete!"
echo ""
echo "📚 Next steps:"
echo ""
echo "  1. Edit .env with your Azure credentials (or use --use-local for testing)"
echo ""
echo "  2. Test universe building:"
echo "     python container/cli.py refresh-universe --phase phase_1 --use-local"
echo ""
echo "  3. Test data fetching:"
echo "     python container/cli.py update-daily --use-local --lookback-days 5"
echo ""
echo "  4. View CLI help:"
echo "     python container/cli.py --help"
echo ""
echo "📖 Documentation:"
echo "  - README.md: Project overview"
echo "  - docs/GETTING_STARTED.md: Detailed setup guide"
echo "  - docs/IMPLEMENTATION_SUMMARY.md: Architecture details"
echo ""
echo "🎉 Happy coding!"
