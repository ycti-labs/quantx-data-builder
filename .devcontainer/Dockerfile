FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

# Set environment variables for Python and Azure
ENV AZURE_FUNCTIONS_ENVIRONMENT=Development

# Install Azure Functions Core Tools
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg && \
    mv microsoft.gpg /etc/apt/trusted.gpg.d/microsoft.gpg && \
    sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/debian/$(lsb_release -rs 2>/dev/null | cut -d'.' -f 1)/prod $(lsb_release -cs 2>/dev/null) main" > /etc/apt/sources.list.d/dotnetdev.list' && \
    apt-get update && \
    apt-get install -y azure-functions-core-tools-4 && \
    rm -rf /var/lib/apt/lists/*
