# syntax=docker/dockerfile:1
# Production Multi-Stage Dockerfile for CloudPulse Platform
# Target: AWS ECS Fargate / Google Cloud Run / Kubernetes Pod

FROM python:3.9-slim-bullseye AS builder

WORKDIR /install

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Final minimal runtime image
FROM python:3.9-slim-bullseye AS runner

WORKDIR /app

# Create non-root system user for CIS security compliance
RUN groupadd -r cloudpulse && useradd -r -g cloudpulse cloudpulse

# Copy dependencies from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY run.py .

# Grant permissions
RUN chown -R cloudpulse:cloudpulse /app
USER cloudpulse

# Operational Environment Variables
ENV PYTHONUNBUFFERED=1 \
    CLOUDPULSE_HOST=0.0.0.0 \
    CLOUDPULSE_PORT=8000 \
    CLOUDPULSE_ENV=production

EXPOSE 8000

# Cloud Container Health Check
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')" || exit 1

ENTRYPOINT ["python", "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
