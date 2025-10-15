# Multi-stage build for optimal image size
FROM python:3.11-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY container/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ============================================
# Final stage - minimal runtime image
# ============================================
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /root/.local /root/.local

# Copy shared source code (SAME as Functions uses)
COPY src/ ./src/
COPY config/ ./config/

# Copy container-specific CLI
COPY container/cli.py ./

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Update PATH to include user-installed packages
ENV PATH=/root/.local/bin:$PATH

# Set Python to unbuffered mode for better logging
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden in Container Apps Job)
ENTRYPOINT ["python", "cli.py"]
CMD ["--help"]