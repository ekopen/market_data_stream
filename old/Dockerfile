FROM python:3.11-slim

# Prevents .pyc files and ensures stdout/stderr is unbuffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps (some Python wheels need build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your code
COPY . .

# No CMD here — docker-compose.yml will decide
