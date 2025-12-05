# Quick Deployment Commands Reference

## Local Testing

### Install GDAL

```bash
# macOS (Homebrew)
brew install gdal

# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y gdal-bin libgdal-dev python3-gdal

# Verify installation
gdalinfo --version
```

### Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Run Tests

```bash
# Run all ortho tests
python -m pytest tests/test_ortho*.py -v

# Run deployment verification
python tests/test_deployment_verification.py

# Run backward compatibility tests
python -m pytest tests/test_backward_compatibility.py -v
```

## Docker Deployment

### Build Image

```bash
# Build with latest tag
docker build -t hwc-potree-api:latest .

# Build with specific tag
docker build -t hwc-potree-api:v1.0-ortho .
```

### Test Image Locally

```bash
# Run container
docker run -d \
  --name hwc-potree-api-test \
  -p 8000:8000 \
  -e AZURE_STORAGE_CONNECTION_STRING="your-connection-string" \
  -e AZURE_CONTAINER_NAME="your-container" \
  -e MONGODB_URI="your-mongodb-uri" \
  hwc-potree-api:latest

# Check logs
docker logs hwc-potree-api-test

# Run verification inside container
docker exec hwc-potree-api-test python tests/test_deployment_verification.py

# Stop and remove
docker stop hwc-potree-api-test
docker rm hwc-potree-api-test
```

### Verify GDAL in Container

```bash
# Check GDAL version
docker run --rm hwc-potree-api:latest gdalinfo --version

# Check COG driver
docker run --rm hwc-potree-api:latest gdalinfo --formats | grep COG

# Check Python bindings
docker run --rm hwc-potree-api:latest python -c "from osgeo import gdal; print(gdal.__version__)"
```

## Staging Deployment

### Azure Container Registry

```bash
# Login to ACR
az acr login --name your-registry

# Tag image
docker tag hwc-potree-api:latest your-registry.azurecr.io/hwc-potree-api:staging

# Push image
docker push your-registry.azurecr.io/hwc-potree-api:staging
```

### Azure Container Apps

```bash
# Update container app
az containerapp update \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group \
  --image your-registry.azurecr.io/hwc-potree-api:staging

# Check deployment status
az containerapp show \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group \
  --query "properties.latestRevisionName"

# View logs
az containerapp logs show \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group \
  --follow
```

### Azure App Service

```bash
# Deploy to App Service
az webapp config container set \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group \
  --docker-custom-image-name your-registry.azurecr.io/hwc-potree-api:staging

# Restart app
az webapp restart \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group

# Stream logs
az webapp log tail \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group
```

## Verification Commands

### Check API Health

```bash
# Health endpoint
curl https://your-staging-url/health

# Root endpoint (list endpoints)
curl https://your-staging-url/

# Check if ortho endpoint is available
curl https://your-staging-url/ | grep ortho
```

### Test Ortho Upload

```bash
# Create test project
PROJECT_ID="test-ortho-$(date +%s)"

# Upload ortho file
curl -X POST "https://your-staging-url/projects/${PROJECT_ID}/ortho" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@test_ortho.tif"

# Get job status
JOB_ID="<job_id_from_response>"
curl "https://your-staging-url/jobs/${JOB_ID}"

# Get project (should have ortho field)
curl "https://your-staging-url/projects/${PROJECT_ID}"
```

### Monitor Logs

```bash
# Docker logs
docker logs -f hwc-potree-api

# Azure Container Apps logs
az containerapp logs show \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group \
  --follow

# Azure App Service logs
az webapp log tail \
  --name hwc-potree-api-staging \
  --resource-group your-resource-group
```

## Production Deployment

### Tag for Production

```bash
# Tag staging image as production
docker tag your-registry.azurecr.io/hwc-potree-api:staging \
           your-registry.azurecr.io/hwc-potree-api:production

# Push production image
docker push your-registry.azurecr.io/hwc-potree-api:production
```

### Deploy to Production

```bash
# Azure Container Apps
az containerapp update \
  --name hwc-potree-api-prod \
  --resource-group your-resource-group \
  --image your-registry.azurecr.io/hwc-potree-api:production

# Azure App Service
az webapp config container set \
  --name hwc-potree-api-prod \
  --resource-group your-resource-group \
  --docker-custom-image-name your-registry.azurecr.io/hwc-potree-api:production

# Restart
az webapp restart \
  --name hwc-potree-api-prod \
  --resource-group your-resource-group
```

### Post-Deployment Verification

```bash
# Run verification script
python tests/test_deployment_verification.py

# Check health
curl https://your-production-url/health

# Test ortho upload with small file
curl -X POST "https://your-production-url/projects/TEST-PROD/ortho" \
  -F "file=@small_test.tif"
```

## Rollback Commands

### Quick Rollback (Docker)

```bash
# Revert to previous image
docker pull your-registry.azurecr.io/hwc-potree-api:previous-version

# Update deployment
az containerapp update \
  --name hwc-potree-api-prod \
  --resource-group your-resource-group \
  --image your-registry.azurecr.io/hwc-potree-api:previous-version
```

### Kubernetes Rollback

```bash
# Rollback to previous revision
kubectl rollout undo deployment/hwc-potree-api

# Check rollout status
kubectl rollout status deployment/hwc-potree-api

# View rollout history
kubectl rollout history deployment/hwc-potree-api
```

## Monitoring Commands

### Check Resource Usage

```bash
# Docker stats
docker stats hwc-potree-api

# Azure Container Apps metrics
az monitor metrics list \
  --resource /subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.App/containerApps/hwc-potree-api-prod \
  --metric "CpuUsage,MemoryUsage"
```

### Check Job Status

```bash
# List recent jobs
curl "https://your-url/jobs?limit=10"

# Check failed jobs
curl "https://your-url/jobs?status=failed"

# Check ortho jobs
curl "https://your-url/jobs?type=ortho"
```

### Check Azure Storage

```bash
# List ortho files
az storage blob list \
  --account-name your-storage-account \
  --container-name your-container \
  --prefix "ortho/" \
  --output table

# Check storage usage
az storage account show-usage \
  --name your-storage-account
```

## Troubleshooting Commands

### GDAL Issues

```bash
# Check GDAL in container
docker exec hwc-potree-api gdalinfo --version

# Check COG driver
docker exec hwc-potree-api gdalinfo --formats | grep COG

# Test GDAL conversion
docker exec hwc-potree-api gdal_translate \
  --version
```

### Worker Issues

```bash
# Check worker logs
docker logs hwc-potree-api | grep "ortho"

# Check worker process
docker exec hwc-potree-api ps aux | grep worker

# Restart worker
docker restart hwc-potree-api
```

### Database Issues

```bash
# Check MongoDB connection
docker exec hwc-potree-api python -c "
from pymongo import MongoClient
import os
client = MongoClient(os.getenv('MONGODB_URI'))
print('Connected:', client.server_info())
"

# Check projects with ortho
docker exec hwc-potree-api python -c "
from pymongo import MongoClient
import os
client = MongoClient(os.getenv('MONGODB_URI'))
db = client['hwc-potree']
count = db.projects.count_documents({'ortho': {'\\$exists': True}})
print(f'Projects with ortho: {count}')
"
```

## Environment Variables

### Required Variables

```bash
# Set environment variables
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
export AZURE_CONTAINER_NAME="your-container"
export MONGODB_URI="mongodb://..."
export POTREE_PATH="/app/bin/PotreeConverter"
```

### Optional Variables

```bash
# Feature flags
export ORTHO_UPLOAD_ENABLED="true"

# Performance tuning
export GDAL_CACHEMAX="512"
export GDAL_NUM_THREADS="4"
```

## Quick Reference

### Most Common Commands

```bash
# Build and test locally
docker build -t hwc-potree-api:latest .
docker run -d -p 8000:8000 --env-file .env hwc-potree-api:latest
python tests/test_deployment_verification.py

# Deploy to staging
docker tag hwc-potree-api:latest your-registry.azurecr.io/hwc-potree-api:staging
docker push your-registry.azurecr.io/hwc-potree-api:staging
az containerapp update --name hwc-potree-api-staging --image your-registry.azurecr.io/hwc-potree-api:staging

# Verify deployment
curl https://your-staging-url/health
python tests/test_deployment_verification.py

# Rollback if needed
az containerapp update --name hwc-potree-api-staging --image your-registry.azurecr.io/hwc-potree-api:previous
```

---

**Note:** Replace placeholders like `your-registry`, `your-resource-group`, `your-url` with your actual values.

For detailed explanations, see:

- `docs/ORTHO_DEPLOYMENT_GUIDE.md` - Complete deployment guide
- `docs/ROLLBACK_PLAN.md` - Rollback procedures
- `docs/DEPLOYMENT_SUMMARY.md` - Deployment overview
