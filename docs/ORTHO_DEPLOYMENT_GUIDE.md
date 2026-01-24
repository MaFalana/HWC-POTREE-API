# Ortho Upload Feature - Deployment Guide

This guide covers the deployment process for the orthophoto upload and COG conversion feature.

## Prerequisites

### System Requirements

- Python 3.12+
- GDAL 3.8.0+ with COG driver support
- Docker (for containerized deployment)
- Azure Blob Storage account
- MongoDB instance

### Required System Packages (Linux/Debian)

```bash
apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal
```

### Required Python Packages

See `requirements.txt` for the complete list. Key additions for ortho feature:

```
GDAL>=3.8.0,<4.0.0
```

## Deployment Steps

### 1. Update Dependencies

#### For Docker Deployment

The Dockerfile has been updated to include GDAL. Build the new image:

```bash
docker build -t hwc-potree-api:latest .
```

#### For Local/VM Deployment

Install GDAL system packages:

```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y gdal-bin libgdal-dev python3-gdal

# Verify installation
gdalinfo --version
gdal_translate --version
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

### 2. Verify GDAL Installation

Run the deployment verification script:

```bash
python tests/test_deployment_verification.py
```

This will check:

- GDAL command-line tools (gdalinfo, gdal_translate)
- GDAL Python bindings
- COG driver availability
- Other required packages
- Environment variables
- API health

### 3. Environment Configuration

No new environment variables are required. The ortho feature uses existing configuration:

- `AZURE_STORAGE_CONNECTION_STRING` - Azure Blob Storage connection
- `AZURE_CONTAINER_NAME` - Container for storing files
- `MONGODB_URI` - MongoDB connection string

### 4. Deploy to Staging

#### Docker Deployment

```bash
# Build image
docker build -t hwc-potree-api:staging .

# Run container
docker run -d \
  --name hwc-potree-api-staging \
  -p 8000:8000 \
  -e AZURE_STORAGE_CONNECTION_STRING="..." \
  -e AZURE_CONTAINER_NAME="..." \
  -e MONGODB_URI="..." \
  hwc-potree-api:staging
```

#### Azure Container Apps / App Service

Update your deployment configuration to use the new Docker image or update the application code.

### 5. Run Smoke Tests

After deployment, verify the API is working:

```bash
# Check health endpoint
curl http://your-staging-url/health

# Check root endpoint (should list ortho upload endpoint)
curl http://your-staging-url/

# Run full verification
python tests/test_deployment_verification.py
```

### 6. Test Ortho Upload

Upload a small test GeoTIFF to verify the feature works end-to-end:

```bash
# Create a test project first
PROJECT_ID="test-ortho-$(date +%s)"

# Upload a small test GeoTIFF
curl -X POST "http://your-staging-url/projects/${PROJECT_ID}/ortho" \
  -F "file=@test_ortho.tif"

# Poll job status
JOB_ID="<job_id_from_response>"
curl "http://your-staging-url/jobs/${JOB_ID}"

# Verify project has ortho field
curl "http://your-staging-url/projects/${PROJECT_ID}"
```

### 7. Monitor Performance

Monitor the following during initial deployment:

- Job processing times (should be <5min for small files, <30min for large)
- Memory usage during GDAL operations
- Disk space usage in temp directory
- Azure storage costs

### 8. Deploy to Production

Once staging tests pass:

1. Tag the Docker image for production
2. Update production deployment
3. Run verification tests on production
4. Monitor for any issues

```bash
# Tag for production
docker tag hwc-potree-api:staging hwc-potree-api:production

# Deploy to production (method depends on your infrastructure)
# ... deployment commands ...

# Verify production
python tests/test_deployment_verification.py
```

## Rollback Plan

See `ROLLBACK_PLAN.md` for detailed rollback procedures.

Quick rollback steps:

1. Revert to previous Docker image/code version
2. No database migration needed (ortho field is optional)
3. Existing projects without ortho continue to work
4. In-progress ortho jobs will fail but can be retried after rollback

## Troubleshooting

### GDAL Not Found

**Symptom:** `gdalinfo: command not found` or `ImportError: No module named 'osgeo'`

**Solution:**

```bash
# Install GDAL system packages
apt-get install -y gdal-bin libgdal-dev python3-gdal

# Reinstall Python GDAL bindings
pip install --no-cache-dir GDAL==$(gdal-config --version)
```

### COG Driver Not Available

**Symptom:** Job fails with "COG driver not available"

**Solution:**

```bash
# Check GDAL version (needs 3.1+)
gdalinfo --version

# List available drivers
gdalinfo --formats | grep COG

# If COG not listed, upgrade GDAL
apt-get install -y gdal-bin=3.8.*
```

### Memory Issues During Conversion

**Symptom:** Worker crashes or OOM errors during large file conversion

**Solution:**

- Increase worker memory allocation
- Add GDAL memory limits in worker code
- Process large files in chunks if possible

### Slow Conversion Times

**Symptom:** Conversions taking longer than expected

**Solution:**

- Check disk I/O performance
- Verify GDAL is using optimized builds
- Consider adjusting JPEG quality or tile size
- Monitor CPU usage during conversion

### Azure Upload Failures

**Symptom:** "Failed to upload ortho to Azure storage"

**Solution:**

- Verify Azure connection string is correct
- Check Azure storage account has sufficient space
- Verify network connectivity to Azure
- Check Azure storage account firewall rules

## Monitoring and Alerts

Set up monitoring for:

- **Job Failure Rate:** Alert if ortho job failure rate >10%
- **Processing Time:** Alert if average processing time exceeds thresholds
- **Disk Space:** Alert if temp directory >80% full
- **Memory Usage:** Alert if worker memory >90%
- **Azure Storage:** Monitor storage costs and usage

## Performance Benchmarks

Expected performance on recommended hardware:

| File Size | Expected Time | Memory Usage |
| --------- | ------------- | ------------ |
| <1GB      | <5 minutes    | ~2GB         |
| 1-5GB     | 5-15 minutes  | ~4GB         |
| 5-10GB    | 15-25 minutes | ~6GB         |
| 10-30GB   | 25-30 minutes | ~8GB         |

## Security Considerations

- GDAL commands run with subprocess timeout to prevent hanging
- File validation prevents malicious uploads
- URLs are public and permanent (no expiration)
- Temporary files are cleaned up after processing
- Error messages don't expose internal paths

## Support

For issues or questions:

1. Check this deployment guide
2. Review `docs/API_DOCUMENTATION.md` for API details
3. Check worker logs for error details
4. Review `tests/test_deployment_verification.py` output

## Changelog

### Version 1.0 (Initial Release)

- Added ortho upload endpoint
- Added COG conversion with GDAL
- Added thumbnail generation
- Added Azure storage integration
- Added project model updates
