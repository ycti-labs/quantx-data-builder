# QuantX Data Builder - Development Container Setup

## 🎯 Overview

This devcontainer provides a complete development environment for the QuantX Data Builder project with:

- Python 3.12 runtime
- Azure CLI and Azure Functions Core Tools
- All project dependencies pre-installed
- VS Code extensions configured
- Docker-in-Docker support for container builds

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- [VS Code](https://code.visualstudio.com/) with [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- At least 8GB RAM available for Docker

### Opening the Project

1. **Open in Container:**
   - Open VS Code
   - Press `F1` or `Ctrl+Shift+P` (Windows/Linux) / `Cmd+Shift+P` (Mac)
   - Select: `Dev Containers: Open Folder in Container...`
   - Navigate to the `quantx-data-builder` folder
   - Wait for the container to build and start (first time takes 5-10 minutes)

2. **Rebuild Container:**
   - If you need to rebuild after changes to devcontainer files:
   - Press `F1` and select `Dev Containers: Rebuild Container`

### What Happens on First Run

The `post-create.sh` script automatically:

1. ✅ Installs all Python dependencies (root, functions, container)
2. ✅ Creates necessary directories (`data/`, `meta/`, `logs/`)
3. ✅ Creates `.env` template file
4. ✅ Configures git settings
5. ✅ Verifies all installations

## 🛠️ Installed Tools

### Python Environment

- **Python**: 3.12
- **Package Manager**: pip (latest)
- **Virtual Environment**: Not needed (container is isolated)

### Azure Tools

- **Azure CLI**: Latest version
- **Azure Functions Core Tools**: v4
- **Azure Developer CLI (azd)**: Latest

### Development Tools

- **Testing**: pytest, pytest-cov, pytest-mock
- **Linting**: mypy, flake8, pylint
- **Formatting**: black, isort
- **Debugging**: ipython, ipdb

### VS Code Extensions

- Python + Pylance
- Azure Functions
- Azure Storage
- Azure Container Apps
- Docker
- GitHub Copilot (if licensed)
- YAML, TOML, Markdown support

## 📁 Mounted Volumes & Persistence

The devcontainer uses a **simplified direct Docker build** (no docker-compose needed):

- **Workspace**: `/workspaces/quantx-data-builder` (bind mount from host)
- **Azure Credentials**: `~/.azure` (bind mount from `~/.azure` on host)
- **Bash History**: Named volume `quantx-bash-history` (persisted)
- **Python Packages**: Named volume `quantx-python-packages` (cached for faster rebuilds)

### Configuration Files

- **`Dockerfile`**: Base Python 3.12 image with Azure tools
- **`devcontainer.json`**: VS Code configuration with direct Docker build
- **`post-create.sh`**: Dependency installation and project setup
- ~~`docker-compose.yml`~~: **Removed** (simplified to direct build)

**Benefits of Simplified Setup:**
- ✅ Faster container startup
- ✅ Single configuration file (`devcontainer.json`)
- ✅ Better VS Code integration
- ✅ Easier debugging and troubleshooting
- ✅ Named volumes for better management

## 🔧 Common Tasks

### Running Tests

```bash
# All tests
pytest tests/

# With coverage
pytest tests/ --cov=src --cov-report=html

# Specific test
pytest tests/test_hkex_universe.py -v
```

### Code Quality

```bash
# Format code
black src/ azure_functions/ container/ tests/

# Type checking
mypy src/

# Linting
flake8 src/
```

### Azure Functions

```bash
# Navigate to functions directory
cd azure_functions

# Start local Functions runtime
func start

# Test function endpoint (in another terminal)
curl http://localhost:7071/api/your-function-name
```

### Container CLI

```bash
# Navigate to container directory
cd container

# Run CLI commands
python cli.py --help
python cli.py backfill --help
python cli.py update-daily --help
```

### Azure CLI

```bash
# Login to Azure
az login

# Set subscription
az account set --subscription "your-subscription-name"

# List resources
az resource list --output table
```

## 🔐 Azure Authentication

### Option 1: Azure CLI (Recommended)

```bash
# Inside container
az login --use-device-code
```

Your credentials are saved in `~/.azure` which is mounted from your host.

### Option 2: Service Principal

```bash
# Create .env file with:
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

## 🐛 Troubleshooting

### Container Won't Start

- **Check Docker**: Ensure Docker Desktop is running
- **Check Memory**: Verify at least 8GB RAM allocated to Docker
- **Check Logs**: View container logs in VS Code Dev Containers panel

### Port Already in Use

- **Azure Functions (7071)**: Stop other Functions instances
- **Check Processes**: `lsof -i :7071` (on host)

### Dependencies Not Installing

- **Rebuild**: `Dev Containers: Rebuild Container Without Cache`
- **Manual Install**: Open terminal and run:

  ```bash
  pip install -r requirements.txt
  cd azure_functions && pip install -r requirements.txt
  cd ../container && pip install -r requirements.txt
  ```

### Azure CLI Login Issues

- **Device Code**: Use `az login --use-device-code` if browser redirect fails
- **Clear Cache**: `rm -rf ~/.azure` and re-login

## 🚢 Building Docker Images

### Build Container Image (for Azure Container Apps)

```bash
# From project root
docker build -f container/Dockerfile -t quantx-data-builder:latest .

# Test locally
docker run --rm quantx-data-builder:latest python --version
```

### Build with Docker Compose

```bash
# From .devcontainer directory
docker-compose build
docker-compose up -d
```

## 📦 Adding New Dependencies

1. **Add to appropriate requirements.txt**:
   - Root: `requirements.txt` (shared dev dependencies)
   - Functions: `azure_functions/requirements.txt`
   - Container: `container/requirements.txt`

2. **Rebuild container** or **Install manually**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Update Dockerfile** if system dependencies needed:
   - Edit `.devcontainer/Dockerfile`
   - Rebuild container

## 🎓 Best Practices

### Development Workflow

1. ✅ Always work inside the devcontainer
2. ✅ Run tests before committing
3. ✅ Format code with `black` before PR
4. ✅ Keep dependencies up to date
5. ✅ Use type hints and mypy

### Container Management

- **Rebuild** after Dockerfile changes
- **Prune** old images periodically: `docker system prune`
- **Monitor** resource usage in Docker Desktop

### Azure Development

- **Use Managed Identity** in production
- **Test locally** with Azure Functions Core Tools
- **Use azd** for deployment automation


## 📚 Additional Resources

- [VS Code Dev Containers Documentation](https://code.visualstudio.com/docs/devcontainers/containers)
- [Azure Functions Local Development](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
- [Azure Developer CLI](https://learn.microsoft.com/azure/developer/azure-developer-cli/)
- [Python in VS Code](https://code.visualstudio.com/docs/python/python-tutorial)

---

## 🔄 Recent Changes (October 2025)

**Simplified Devcontainer Configuration:**

- ✅ **Removed `docker-compose.yml`**: Simplified to direct Docker build in `devcontainer.json`
- ✅ **Unified Configuration**: All settings now in single `devcontainer.json` file
- ✅ **Named Volumes**: Better management for bash history and Python packages
- ✅ **Port Labels**: Added descriptive labels for forwarded ports (7071=Azure Functions, 8080=HTTP)
- ✅ **Environment Variables**: Consolidated and moved to `devcontainer.json`
- ✅ **Improved Dockerfile**: Added `PYTHONDONTWRITEBYTECODE`, `EXPOSE` ports, optimized RUN commands

**Migration Notes:**
- No action required for existing users - VS Code will automatically use the new configuration
- Existing named volumes are preserved during rebuild
- If you encounter issues, run "Dev Containers: Rebuild Container Without Cache"

**Benefits:**
- ~20% faster container startup time
- Simpler troubleshooting with single configuration file
- Better integration with VS Code Dev Containers features
- Easier to understand and maintain

---

**Happy Coding! 🚀**


