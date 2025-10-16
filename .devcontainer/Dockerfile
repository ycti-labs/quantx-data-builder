# Development container for QuantX Data Builder
# Based on Python 3.11 with Azure tools and development dependencies

FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build tools
    build-essential \
    gcc \
    g++ \
    make \
    # Azure Functions Core Tools dependencies
    wget \
    gnupg \
    apt-transport-https \
    # Excel file processing
    libxml2-dev \
    libxslt1-dev \
    # For data science work
    libgomp1 \
    # Git and shell utilities
    git \
    curl \
    vim \
    nano \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Azure Functions Core Tools
RUN wget -q https://packages.microsoft.com/config/debian/11/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt-get update && \
    apt-get install -y azure-functions-core-tools-4 && \
    rm -rf /var/lib/apt/lists/*

# Install Python development tools globally
RUN pip install --no-cache-dir \
    # Development tools
    ipython \
    ipdb \
    # Code quality
    black \
    mypy \
    flake8 \
    pylint \
    isort \
    # Testing
    pytest \
    pytest-cov \
    pytest-mock \
    pytest-asyncio \
    # Build tools
    build \
    wheel \
    setuptools

# Create workspace directory
RUN mkdir -p /workspace && chown -R vscode:vscode /workspace

# Switch to non-root user
USER vscode

# Set working directory
WORKDIR /workspace

# Configure git for the vscode user
RUN git config --global core.autocrlf input && \
    git config --global init.defaultBranch main

# Create common directories
RUN mkdir -p /home/vscode/.azure /home/vscode/.local/share /home/vscode/.cache

CMD ["/bin/bash"]
