
FROM python:3.13-slim

WORKDIR /app

# Install production dependencies.
COPY requirements-prod.txt .
RUN pip install --no-cache-dir -r requirements-prod.txt

# Copy only the application code.
COPY src/ ./src/
COPY scripts/ ./scripts/

# Create a writable directory for temporary ingestion files.
RUN mkdir -p /app/data/raw /app/data/state \
    && useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app/data

# Run as a non-root user.
USER appuser

# Make Python output appear immediately in container logs.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "scripts.run_ingestion"]
