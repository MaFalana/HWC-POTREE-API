# Docker Build Troubleshooting - GDAL Installation

## Issue: GDAL Installation Fails During Docker Build

### Error Message

```
ERROR: failed to build: failed to solve: process "/bin/sh -c pip install --no-cache-dir -r requirements.txt" did not complete successfully: exit code: 1
```

### Root Cause

The GDAL Python bindings in `requirements.txt` must match the system GDAL version installed via apt-get. When pip tries to install GDAL from requirements.txt, it may:

1. Try to install a version that doesn't match the system GDAL
2. Fail to compile because build tools aren't available
3. Have dependency conflicts

### Solution Implemented

The Dockerfile now:

1. Installs system GDAL and build tools first
2. Detects the system GDAL version using `gdal-config --version`
3. Installs matching GDAL Python bindings
4. Filters out GDAL from requirements.txt before installing other packages

### Updated Dockerfile Strategy

```dockerfile
# Install system GDAL and build tools
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install GDAL Python bindings matching system version
RUN GDAL_VERSION=$(gdal-config --version) && \
    pip install --no-cache-dir GDAL==${GDAL_VERSION}

# Install other packages (excluding GDAL)
RUN grep -v "^GDAL" requirements.txt > /tmp/requirements_filtered.txt && \
    pip install --no-cache-dir -r /tmp/requirements_filtered.txt
```

## Testing the Fix

### Local Build Test

```bash
docker build -t hwc-potree-api:test .
```

### Verify GDAL in Built Image

```bash
# Check GDAL version
docker run --rm hwc-potree-api:test gdalinfo --version

# Check Python bindings
docker run --rm hwc-potree-api:test python -c "from osgeo import gdal; print(gdal.__version__)"

# Check COG driver
docker run --rm hwc-potree-api:test gdalinfo --formats | grep COG
```

### Expected Output

```
GDAL 3.6.2, released 2023/01/02  # Version may vary
3.6.2                              # Python bindings match
COG -raster- (rw+): Cloud optimized GeoTIFF generator
```

## Alternative Solutions

### Option 1: Use System Python GDAL (Not Recommended)

```dockerfile
# Install python3-gdal from apt
RUN apt-get install -y python3-gdal

# Don't install GDAL via pip
```

**Pros:** Simpler, guaranteed compatibility
**Cons:** May be outdated, less control over version

### Option 2: Pin Specific GDAL Version

```dockerfile
# Install specific GDAL version
RUN apt-get install -y gdal-bin=3.6.2+dfsg-1

# Install matching Python bindings
RUN pip install GDAL==3.6.2
```

**Pros:** Reproducible builds
**Cons:** May not be available in all base images

### Option 3: Multi-stage Build (Most Robust)

```dockerfile
# Build stage
FROM python:3.12-slim as builder
RUN apt-get update && apt-get install -y \
    gdal-bin libgdal-dev build-essential
RUN GDAL_VERSION=$(gdal-config --version) && \
    pip wheel --no-cache-dir GDAL==${GDAL_VERSION}

# Runtime stage
FROM python:3.12-slim
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl
```

**Pros:** Smaller final image, faster rebuilds
**Cons:** More complex

## Common Issues and Fixes

### Issue: "gdal-config: command not found"

**Cause:** libgdal-dev not installed
**Fix:** Add `libgdal-dev` to apt-get install

### Issue: "error: command 'gcc' failed"

**Cause:** Build tools not installed
**Fix:** Add `build-essential` to apt-get install

### Issue: Version mismatch between system and Python GDAL

**Cause:** Hardcoded version in requirements.txt
**Fix:** Use dynamic version detection as shown above

### Issue: "COG driver not available"

**Cause:** GDAL version too old (need 3.1+)
**Fix:** Use newer base image or install GDAL from source

## Verification Checklist

After building, verify:

- [ ] Docker build completes successfully
- [ ] `gdalinfo --version` works in container
- [ ] `python -c "from osgeo import gdal"` works
- [ ] COG driver is available
- [ ] All other Python packages installed
- [ ] Application starts without errors

## Build Performance Tips

### Use BuildKit

```bash
DOCKER_BUILDKIT=1 docker build -t hwc-potree-api:latest .
```

### Cache Dependencies

```dockerfile
# Copy only requirements first
COPY requirements.txt .
RUN pip install ...

# Copy code later (better caching)
COPY . .
```

### Multi-platform Builds

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t hwc-potree-api:latest \
  --push .
```

## Debugging Build Failures

### Run Interactive Shell at Failure Point

```bash
# Build up to failing step
docker build --target builder -t debug .

# Run shell in that stage
docker run -it debug /bin/bash

# Test commands manually
gdal-config --version
pip install GDAL==...
```

### Check Build Logs

```bash
# Verbose output
docker build --progress=plain -t hwc-potree-api:latest .

# Save logs
docker build -t hwc-potree-api:latest . 2>&1 | tee build.log
```

### Test Specific Layer

```bash
# Build only up to specific step
docker build --target <stage-name> -t test .
```

## CI/CD Considerations

### GitHub Actions

```yaml
- name: Build Docker image
  run: |
    docker build \
      --build-arg BUILDKIT_INLINE_CACHE=1 \
      --cache-from hwc-potree-api:latest \
      -t hwc-potree-api:${{ github.sha }} \
      .
```

### Azure DevOps

```yaml
- task: Docker@2
  inputs:
    command: build
    Dockerfile: Dockerfile
    tags: |
      $(Build.BuildId)
      latest
```

## Related Documentation

- [ORTHO_DEPLOYMENT_GUIDE.md](./ORTHO_DEPLOYMENT_GUIDE.md) - Full deployment guide
- [DEPLOYMENT_COMMANDS.md](./DEPLOYMENT_COMMANDS.md) - Quick command reference
- [ROLLBACK_PLAN.md](./ROLLBACK_PLAN.md) - Rollback procedures

## Support

If you continue to have build issues:

1. Check GDAL version compatibility: https://gdal.org/
2. Check Python GDAL bindings: https://pypi.org/project/GDAL/
3. Review Docker build logs carefully
4. Test GDAL installation manually in a container

---

**Last Updated:** December 5, 2025
**GDAL Version Tested:** 3.6.2, 3.8.5
**Python Version:** 3.12
**Base Image:** python:3.12-slim
