# Multi-stage build for optimal image size and security
FROM python:3.13-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

# Production stage
FROM python:3.13-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /root/.local /root/.local

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash --uid 1000 appuser

# Set working directory and permissions
WORKDIR /app
RUN chown -R appuser:appuser /app

# Copy application code
COPY --chown=appuser:appuser fetcher ./fetcher
COPY --chown=appuser:appuser meta ./meta
COPY --chown=appuser:appuser requirements.txt .

# Switch to non-root user
USER appuser

# Update PATH to include user packages
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import fetcher; print('OK')" || exit 1

# Default command - can be overridden for different operations
CMD ["python", "-m", "fetcher.cli", "status"]