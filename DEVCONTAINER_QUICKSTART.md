# 🚀 Getting Started with DevContainer

Welcome to the QuantX Data Builder development environment! This guide will help you get up and running quickly.

## 📋 Prerequisites

Before you begin, ensure you have:

- ✅ [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- ✅ [Visual Studio Code](https://code.visualstudio.com/) installed
- ✅ [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed in VS Code
- ✅ At least **8GB RAM** available for Docker
- ✅ At least **10GB disk space** for container images

## 🎯 Quick Start (3 Steps)

### Step 1: Open Project in Container

1. Open VS Code
2. Press `Ctrl+Shift+P` (Windows/Linux) or `Cmd+Shift+P` (Mac)
3. Type and select: **`Dev Containers: Open Folder in Container...`**
4. Navigate to the `quantx-data-builder` folder
5. Click **Select Folder**

**⏱️ First-time setup takes 5-10 minutes** while Docker builds the container and installs dependencies.

### Step 2: Wait for Setup

Watch the progress in the VS Code terminal. The setup script will:

- 🔧 Build the development container
- 📦 Install Python 3.11 and all dependencies
- 🔨 Install Azure CLI and Azure Functions Core Tools
- 📁 Create project directories
- ✅ Verify installations

You'll see: `✨ Setup complete! You're ready to start developing.`

### Step 3: Verify Installation

Open a new terminal in VS Code and run:

```bash
python --version    # Should show Python 3.11.x
az --version        # Should show Azure CLI version
func --version      # Should show Azure Functions Core Tools v4.x
pytest --version    # Should show pytest version
```

## 🎉 You're Ready!

Your development environment is now fully configured. Try these commands:

### Run Tests
```bash
pytest tests/ -v
```

### Check Code Quality
```bash
black --check src/
mypy src/
```

### Start Azure Functions Locally
```bash
cd azure_functions
func start
```

## 🔐 Azure Authentication

To access Azure resources, authenticate with:

```bash
az login --use-device-code
```

Follow the instructions to authenticate via your browser.

## 📚 Next Steps

- 📖 Read [.devcontainer/README.md](.devcontainer/README.md) for detailed container documentation
- 📖 Read [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for project documentation
- 🧪 Run tests: `pytest tests/`
- 🔍 Explore the codebase: `src/`, `azure_functions/`, `container/`

## 🆘 Troubleshooting

### Container Won't Build

**Problem:** Docker build fails or hangs

**Solution:**
```bash
# In VS Code, press Ctrl+Shift+P and select:
Dev Containers: Rebuild Container Without Cache
```

### Port Conflicts

**Problem:** Port 7071 is already in use

**Solution:**
```bash
# On Windows (PowerShell)
Get-Process -Id (Get-NetTCPConnection -LocalPort 7071).OwningProcess | Stop-Process

# On Linux/Mac
lsof -ti:7071 | xargs kill -9
```

### Azure CLI Login Issues

**Problem:** `az login` fails

**Solution:**
```bash
# Use device code flow
az login --use-device-code

# Or clear cache and retry
rm -rf ~/.azure
az login
```

### Python Packages Missing

**Problem:** Import errors for installed packages

**Solution:**
```bash
# Reinstall all dependencies
pip install -r requirements.txt
cd azure_functions && pip install -r requirements.txt
cd ../container && pip install -r requirements.txt
```

### Container Performance Issues

**Problem:** Container is slow

**Solution:**
1. Open Docker Desktop
2. Go to **Settings → Resources**
3. Increase allocated **Memory to 8GB** and **CPUs to 4**
4. Click **Apply & Restart**

## 💡 Pro Tips

### Use Integrated Terminal
- Open terminal: `` Ctrl+` ``
- Multiple terminals: Click `+` button
- Split terminal: Click split icon

### Quick Navigation
- **Command Palette**: `Ctrl+Shift+P`
- **Quick Open File**: `Ctrl+P`
- **Go to Symbol**: `Ctrl+Shift+O`
- **Search Everywhere**: `Ctrl+Shift+F`

### Debugging
- Set breakpoints by clicking left of line numbers
- Press `F5` to start debugging
- Use debug configurations in `.vscode/launch.json`

### VS Code Extensions
All recommended extensions will be automatically installed in the container. Check the Extensions panel to verify.

## 📞 Getting Help

- **VS Code Docs**: [Dev Containers](https://code.visualstudio.com/docs/devcontainers/containers)
- **Project Issues**: Check main [README.md](../README.md)
- **Azure Docs**: [Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/)

---

**Happy Coding! 🎉**
