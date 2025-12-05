FROM python:3.12-slim

WORKDIR /app

# Install build dependencies and runtime dependencies for PotreeConverter and GDAL
RUN apt-get update && apt-get install -y \
    liblaszip8 \
    libboost-system1.83.0 \
    libboost-filesystem1.83.0 \
    libboost-program-options1.83.0 \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .

# Install GDAL Python bindings matching system GDAL version
RUN GDAL_VERSION=$(gdal-config --version) && \
    pip install --no-cache-dir GDAL==${GDAL_VERSION}

# Install remaining Python packages (excluding GDAL which is already installed)
RUN grep -v "^GDAL" requirements.txt > /tmp/requirements_filtered.txt && \
    pip install --no-cache-dir -r /tmp/requirements_filtered.txt

COPY . .

# Set execute permission on PotreeConverter
RUN chmod +x /app/bin/PotreeConverter

# Create symlink at /app/PotreeConverter for backward compatibility
RUN ln -s /app/bin/PotreeConverter /app/PotreeConverter

# Create symlink for liblaszip (system has liblaszip.so.8, PotreeConverter needs liblaszip.so)
RUN ln -s /usr/lib/x86_64-linux-gnu/liblaszip.so.8 /usr/lib/x86_64-linux-gnu/liblaszip.so

# Set environment variable
ENV POTREE_PATH=/app/bin/PotreeConverter

EXPOSE 8000

# Health check for Azure monitoring
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health', timeout=5)" || exit 1

# Run uvicorn directly; use PORT if set, else default to 8000
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
