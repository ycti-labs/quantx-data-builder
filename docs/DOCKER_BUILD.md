# Docker Build Reference

## Building the Container Image

### Standard Build

Build from the project root:

```bash
docker build -t quantx-data-builder:latest -f container/Dockerfile .
```

### Build for Azure Container Registry

```bash
# Set your ACR name
ACR_NAME="acrfinsightdata"
IMAGE_NAME="finsight-data-fetcher"

# Build
docker build -t ${ACR_NAME}.azurecr.io/${IMAGE_NAME}:latest -f container/Dockerfile .

# Login to ACR
az acr login --name $ACR_NAME

# Push
docker push ${ACR_NAME}.azurecr.io/${IMAGE_NAME}:latest
```

### Build with Custom Tag

```bash
VERSION="v2.0.0"
docker build -t quantx-data-builder:${VERSION} -f container/Dockerfile .
```

## Local Testing

### Run Interactive Shell

```bash
docker run -it --rm quantx-data-builder:latest /bin/bash
```

### Test CLI Commands

```bash
# Test help
docker run --rm quantx-data-builder:latest --help

# Test with local storage (mount volume)
docker run --rm \
  -v $(pwd)/data:/app/data \
  -e USE_MANAGED_IDENTITY=false \
  -e LOCAL_STORAGE_ROOT=/app/data \
  quantx-data-builder:latest \
  update-daily --use-local --lookback-days 1
```

### Test with Azure Storage

```bash
docker run --rm \
  -e FETCHER_AZURE_STORAGE_ACCOUNT="your_account" \
  -e FETCHER_AZURE_CONTAINER_NAME="your_container" \
  -e AZURE_STORAGE_CONNECTION_STRING="your_connection_string" \
  -e USE_MANAGED_IDENTITY=false \
  quantx-data-builder:latest \
  refresh-universe --phase phase_1
```

## File Structure

The Dockerfile expects the following structure:

```
quantx-data-builder/           # Build context (project root)
├── container/
│   ├── Dockerfile            # Docker build file
│   ├── .dockerignore         # Files to exclude
│   ├── cli.py                # CLI entry point
│   └── requirements.txt      # Python dependencies
├── src/                      # Shared core modules
├── config/                   # Configuration files
└── ...
```

## Important Notes

1. **Build Context**: Always build from the project root (`.`)
2. **Dockerfile Location**: Use `-f container/Dockerfile` to specify the Dockerfile
3. **Copy Commands**: All COPY commands in Dockerfile use paths relative to build context (root)
4. **Ignore Files**: Use `container/.dockerignore` to exclude unnecessary files

## Multi-Stage Build

The Dockerfile uses multi-stage build for optimization:

1. **Builder Stage**: Installs build dependencies and Python packages
2. **Runtime Stage**: Creates minimal production image with only runtime dependencies

This reduces final image size significantly.

## Troubleshooting

### Issue: "COPY failed: file not found"

**Solution**: Ensure you're building from the project root:
```bash
cd /path/to/quantx-data-builder
docker build -f container/Dockerfile .
```

### Issue: "No such file or directory: container/requirements.txt"

**Solution**: The Dockerfile expects to be built from the root with context `.`:
```bash
# Correct
docker build -f container/Dockerfile .

# Incorrect
cd container && docker build .
```

### Issue: Image size too large

**Solution**: The multi-stage build already optimizes size. Check `.dockerignore`:
```bash
cat container/.dockerignore
```

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Build and push Docker image
  run: |
    docker build -t ${{ env.ACR_NAME }}.azurecr.io/finsight-data-fetcher:${{ github.sha }} \
      -f container/Dockerfile .
    docker push ${{ env.ACR_NAME }}.azurecr.io/finsight-data-fetcher:${{ github.sha }}
```

### Azure DevOps Example

```yaml
- task: Docker@2
  inputs:
    command: build
    dockerfile: container/Dockerfile
    buildContext: .
    tags: $(Build.BuildId)
```
