# Deployment Guide - HWC Potree API

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [GDAL Installation](#gdal-installation)
- [Python Package Requirements](#python-package-requirements)
- [Environment Setup](#environment-setup)
- [Docker Deployment](#docker-deployment)
- [Azure Container Apps Deployment](#azure-container-apps-deployment)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [Rollback Procedures](#rollback-procedures)

---

## Overview

This guide covers the deployment of the HWC Potree API with orthophoto upload and COG conversion support. The application requires:

- Python 3.12 runtime
- MongoDB database
- Azure Blob Storage
- PotreeConverter binary
- **GDAL (Geospatial Data Abstraction Library)**

---

## Prerequisites

Before deploying, ensure you have:

1. **MongoDB Instance**

   - MongoDB Atlas (recommended) or self-hosted MongoDB
   - Connection string with read/write permissions
   - Database name: `hwc-potree`

2. **Azure Blob Storage**

   - Azure Storage Account
   - Container name: `hwc-potree`
   - Connection string with read/write permissions

3. **Docker Hub Account** (for CI/CD)

   - Account with push permissions
   - Access token for GitHub Actions

4. **Azure Container Apps** (for production)
   - Resource group: `LiDAR-Breakline-Generator`
   - Container app name: `hwc-potree-api`

---

## GDAL Installation

GDAL (Geospatial Data Abstraction Library) is required for orthophoto processing. It must be installed in the deployment environment.

### Docker Installation (Recommended)

GDAL is automatically installed via the Dockerfile. Ensure your Dockerfile includes:

```dockerfile
# Install GDAL and dependencies
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

# Verify GDAL installation
RUN gdalinfo --version && \
    gdalinfo --format COG
```

### Manual Installation (Linux Server)

If deploying to a Linux server without Docker:

**Ubuntu/Debian:**

```bash
# Update package list
sudo apt-get update

# Install GDAL
sudo apt-get install -y gdal-bin libgdal-dev python3-gdal

# Verify installation
gdalinfo --version
gdalinfo --format COG
```

**CentOS/RHEL:**

```bash
# Enable EPEL repository
sudo yum install -y epel-release

# Install GDAL
sudo yum install -y gdal gdal-devel gdal-python3

# Verify installation
gdalinfo --version
gdalinfo --format COG
```

### Version Requirements

- **Minimum Version:** GDAL 3.0
- **Recommended Version:** GDAL 3.4 or higher
- **Required Driver:** COG (Cloud Optimized GeoTIFF)

### Verification

After installation, verify GDAL is working:

```bash
# Check GDAL version
gdalinfo --version
# Expected: GDAL 3.x.x, released 2023/xx/xx

# Check COG driver support
gdalinfo --format COG
# Expected: Format Details with COG driver information

# Test COG conversion
gdal_translate --help | grep COG
# Expected: Output showing COG format options
```

---

## Python Package Requirements

### Core Dependencies

The application requires the following Python packages:

```txt
fastapi==0.104.1
uvicorn[standard]==0.24.0
pymongo==4.6.0
azure-storage-blob==12.19.0
python-multipart==0.0.6
pydantic==2.5.0
python-dotenv==1.0.0
requests==2.31.0
```

### GDAL Python Bindings

**Important:** GDAL Python bindings must match the installed GDAL version.

**Option 1: System Package (Recommended for Docker)**

```dockerfile
RUN apt-get install -y python3-gdal
```

**Option 2: pip Install (Match GDAL version)**

```bash
# Get GDAL version
GDAL_VERSION=$(gdal-config --version)

# Install matching Python bindings
pip install gdal==$GDAL_VERSION
```

**Option 3: Use pygdal (Alternative)**

```bash
pip install pygdal
```

### Installing Dependencies

**From requirements.txt:**

```bash
pip install -r requirements.txt
```

**Verify GDAL Python bindings:**

```python
python3 -c "from osgeo import gdal; print(gdal.__version__)"
```

---

## Environment Setup

### Required Environment Variables

Create a `.env` file with the following variables:

```bash
# MongoDB Configuration
MONGO_CONNECTION_STRING=mongodb+srv://user:password@cluster.mongodb.net/
NAME=hwc-potree

# Azure Storage Configuration
AZURE_STORAGE_CONNECTION_STRING_2=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...

# Application Configuration
PORT=8000
LOG_LEVEL=INFO

# Worker Configuration
WORKER_POLL_INTERVAL=5
JOB_CLEANUP_HOURS=72

# PotreeConverter Path
POTREE_PATH=/app/PotreeConverter
```

### Optional Environment Variables

```bash
# GDAL Configuration (usually not needed)
GDAL_DATA=/usr/share/gdal
PROJ_LIB=/usr/share/proj

# Performance Tuning
GDAL_CACHEMAX=512
GDAL_NUM_THREADS=ALL_CPUS
```

### Environment Variable Validation

Before deployment, validate all required variables are set:

```bash
# Check required variables
python3 -c "
import os
required = [
    'MONGO_CONNECTION_STRING',
    'AZURE_STORAGE_CONNECTION_STRING_2',
    'NAME'
]
missing = [v for v in required if not os.getenv(v)]
if missing:
    print(f'Missing variables: {missing}')
    exit(1)
print('All required variables set')
"
```

---

## Docker Deployment

### Building the Docker Image

1. **Clone the repository:**

```bash
git clone https://github.com/MaFalana/HWC-POTREE-API.git
cd HWC-POTREE-API
```

2. **Ensure PotreeConverter binary is in place:**

```bash
# Verify binary exists
ls -lh bin/PotreeConverter

# Make executable
chmod +x bin/PotreeConverter
```

3. **Build the Docker image:**

```bash
docker build -t hwc-potree-api:latest .
```

4. **Verify GDAL in the image:**

```bash
docker run --rm hwc-potree-api:latest gdalinfo --version
docker run --rm hwc-potree-api:latest gdalinfo --format COG
```

### Running the Docker Container

**Using docker run:**

```bash
docker run -d \
  --name hwc-potree-api \
  -p 8000:8000 \
  --env-file .env \
  hwc-potree-api:latest
```

**Using docker-compose:**

```yaml
version: "3.8"

services:
  api:
    build: .
    ports:
      - "8000:8000"
    env_file:
      - .env
    restart: unless-stopped
    healthcheck:
      test:
        [
          "CMD",
          "python",
          "-c",
          "import requests; requests.get('http://localhost:8000/health')",
        ]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s
```

```bash
docker-compose up -d
```

### Verifying the Deployment

```bash
# Check container is running
docker ps | grep hwc-potree-api

# Check logs
docker logs hwc-potree-api

# Test health endpoint
curl http://localhost:8000/health

# Test GDAL availability
docker exec hwc-potree-api gdalinfo --version
```

---

## Azure Container Apps Deployment

### Prerequisites

- Azure CLI installed and configured
- Docker Hub account with pushed image
- Azure Container Apps environment created

### Step 1: Push Image to Docker Hub

**Manual Push:**

```bash
# Tag image
docker tag hwc-potree-api:latest mfalana/hwc-potree-api:latest

# Login to Docker Hub
docker login

# Push image
docker push mfalana/hwc-potree-api:latest
```

**Automated Push (GitHub Actions):**

The repository includes a GitHub Actions workflow that automatically builds and pushes images on every push to `main`.

**Required GitHub Secrets:**

- `DOCKERHUB_USERNAME` - Your Docker Hub username
- `DOCKERHUB_TOKEN` - Your Docker Hub access token

### Step 2: Deploy to Azure Container Apps

**Using Azure CLI:**

```bash
# Login to Azure
az login

# Set subscription (if needed)
az account set --subscription "Your Subscription Name"

# Deploy container app
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --image docker.io/mfalana/hwc-potree-api:latest
```

**With specific commit SHA:**

```bash
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --image docker.io/mfalana/hwc-potree-api:<commit-sha>
```

### Step 3: Configure Environment Variables

**Set environment variables in Azure:**

```bash
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --set-env-vars \
    MONGO_CONNECTION_STRING="mongodb+srv://..." \
    AZURE_STORAGE_CONNECTION_STRING_2="DefaultEndpointsProtocol=..." \
    NAME="hwc-potree" \
    PORT="8000" \
    LOG_LEVEL="INFO"
```

**Or use Azure Portal:**

1. Navigate to Azure Container Apps
2. Select `hwc-potree-api`
3. Go to "Containers" → "Environment variables"
4. Add/update variables
5. Save and restart

### Step 4: Configure Health Probes

**Liveness Probe:**

```bash
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --set-env-vars \
    LIVENESS_PROBE_PATH="/health" \
    LIVENESS_PROBE_INTERVAL="30" \
    LIVENESS_PROBE_TIMEOUT="10"
```

**Readiness Probe:**

```bash
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --set-env-vars \
    READINESS_PROBE_PATH="/health" \
    READINESS_PROBE_INTERVAL="10" \
    READINESS_PROBE_TIMEOUT="5"
```

### Step 5: Configure Scaling

**Set scaling rules:**

```bash
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --min-replicas 1 \
  --max-replicas 5 \
  --scale-rule-name http-rule \
  --scale-rule-type http \
  --scale-rule-http-concurrency 100
```

### Step 6: Verify Deployment

```bash
# Check deployment status
az containerapp show \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --query properties.latestRevisionName

# Get application URL
az containerapp show \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --query properties.configuration.ingress.fqdn

# Test health endpoint
curl https://$(az containerapp show \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --query properties.configuration.ingress.fqdn -o tsv)/health
```

---

## Verification

### Post-Deployment Checks

After deployment, verify all features are working:

#### 1. Health Check

```bash
curl https://your-app-url.com/health
```

**Expected Response:**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:00:00Z",
  "services": {
    "mongodb": "connected",
    "azure_blob": "connected"
  }
}
```

#### 2. GDAL Availability

```bash
# For Docker
docker exec <container-id> gdalinfo --version

# For Azure Container Apps
az containerapp exec \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --command "gdalinfo --version"
```

#### 3. API Documentation

```bash
curl https://your-app-url.com/docs
```

Should return the Swagger UI HTML.

#### 4. Test Ortho Upload

```bash
# Create test project
curl -X POST "https://your-app-url.com/projects/upload" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "TEST-DEPLOY-001",
    "name": "Deployment Test",
    "client": "Test"
  }'

# Upload small test GeoTIFF
curl -X POST "https://your-app-url.com/projects/TEST-DEPLOY-001/ortho" \
  -F "file=@test_ortho.tif"

# Monitor job
curl "https://your-app-url.com/jobs/{job_id}"

# Clean up
curl -X DELETE "https://your-app-url.com/projects/TEST-DEPLOY-001/delete"
```

#### 5. Check Logs

**Docker:**

```bash
docker logs hwc-potree-api --tail 100
```

**Azure Container Apps:**

```bash
az containerapp logs show \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --follow
```

**Look for:**

- "Application startup: Initializing background worker"
- "Background worker thread started successfully"
- No GDAL-related errors
- Successful MongoDB and Azure connections

---

## Troubleshooting

### Issue: GDAL Not Found

**Symptoms:**

- Jobs fail with "gdalinfo: command not found"
- Health check shows errors

**Solution:**

```bash
# Verify GDAL in container
docker exec <container-id> which gdalinfo
docker exec <container-id> gdalinfo --version

# If missing, rebuild Docker image
docker build --no-cache -t hwc-potree-api:latest .
```

### Issue: COG Driver Not Available

**Symptoms:**

- Jobs fail with "COG driver not found"
- `gdalinfo --format COG` returns error

**Solution:**

```bash
# Check GDAL version (must be 3.0+)
docker exec <container-id> gdalinfo --version

# Rebuild with newer GDAL
# Update Dockerfile to use Ubuntu 22.04 or newer
```

### Issue: Python GDAL Bindings Mismatch

**Symptoms:**

- Import errors: "ImportError: No module named 'osgeo'"
- Version mismatch errors

**Solution:**

```bash
# Check GDAL version
GDAL_VERSION=$(docker exec <container-id> gdal-config --version)

# Reinstall matching Python bindings
docker exec <container-id> pip install gdal==$GDAL_VERSION
```

### Issue: Ortho Jobs Fail Immediately

**Symptoms:**

- Jobs go from "pending" to "failed" quickly
- Error message mentions GDAL

**Solution:**

1. Check worker logs for GDAL errors
2. Verify GDAL is in PATH
3. Test GDAL manually:

```bash
docker exec <container-id> gdalinfo /path/to/test.tif
```

### Issue: High Memory Usage During COG Conversion

**Symptoms:**

- Container OOM (Out of Memory) errors
- Slow processing for large files

**Solution:**

```bash
# Set GDAL cache limit
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --set-env-vars GDAL_CACHEMAX=256

# Increase container memory
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --cpu 2 \
  --memory 4Gi
```

---

## Rollback Procedures

### Rollback to Previous Docker Image

**Step 1: Identify previous working image**

```bash
# List recent images
docker images mfalana/hwc-potree-api

# Or check Docker Hub for tags
```

**Step 2: Deploy previous image**

```bash
az containerapp update \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --image docker.io/mfalana/hwc-potree-api:<previous-commit-sha>
```

**Step 3: Verify rollback**

```bash
# Check revision
az containerapp show \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --query properties.latestRevisionName

# Test health
curl https://your-app-url.com/health
```

### Rollback Database Changes

If database schema changes were made:

```bash
# Connect to MongoDB
mongosh "mongodb+srv://..."

# Switch to database
use hwc-potree

# Remove ortho field from projects (if needed)
db.projects.updateMany(
  {},
  { $unset: { ortho: "" } }
)
```

### Emergency Rollback

If the application is completely broken:

```bash
# Stop current revision
az containerapp revision deactivate \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --revision <current-revision>

# Activate previous revision
az containerapp revision activate \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --revision <previous-revision>
```

---

## Monitoring and Maintenance

### Log Monitoring

**Set up log streaming:**

```bash
az containerapp logs show \
  --name hwc-potree-api \
  --resource-group LiDAR-Breakline-Generator \
  --follow
```

**Key log patterns to monitor:**

- GDAL errors
- Job failures
- Memory warnings
- Azure connection issues

### Performance Monitoring

**Monitor metrics:**

```bash
az monitor metrics list \
  --resource /subscriptions/.../resourceGroups/LiDAR-Breakline-Generator/providers/Microsoft.App/containerApps/hwc-potree-api \
  --metric-names "Requests,CpuUsage,MemoryUsage"
```

### Regular Maintenance

**Weekly:**

- Check failed jobs: `GET /jobs?status=failed`
- Review error logs
- Monitor disk space

**Monthly:**

- Update dependencies
- Review and clean up old jobs
- Check SAS URL expiration

**Quarterly:**

- Update GDAL version
- Review and optimize COG conversion settings
- Performance testing with large files

---

## Support

For deployment issues:

1. Check logs first
2. Verify GDAL installation
3. Test with small files
4. Review GitHub Issues: https://github.com/MaFalana/HWC-POTREE-API/issues
5. Contact development team

---

## Appendix

### Dockerfile Example

```dockerfile
FROM python:3.12-slim

# Install system dependencies including GDAL
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy PotreeConverter binary
COPY bin/PotreeConverter /app/PotreeConverter
RUN chmod +x /app/PotreeConverter

# Verify GDAL installation
RUN gdalinfo --version && gdalinfo --format COG

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Start application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### requirements.txt Example

```txt
fastapi==0.104.1
uvicorn[standard]==0.24.0
pymongo==4.6.0
azure-storage-blob==12.19.0
python-multipart==0.0.6
pydantic==2.5.0
python-dotenv==1.0.0
requests==2.31.0
```

Note: GDAL Python bindings are installed via system package (python3-gdal) in the Dockerfile.
