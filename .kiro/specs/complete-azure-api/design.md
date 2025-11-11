# Design Document

## Overview

The HWC Potree API is a FastAPI-based backend service that processes LiDAR point cloud data (LAS/LAZ files) and converts them to Potree format for web-based 3D visualization. The system uses MongoDB for metadata persistence, Azure Blob Storage for file storage, and a background worker thread for asynchronous processing.

The architecture follows a modular design with separate routers for different concerns (projects, processing), a background worker for long-running tasks, and integration with Azure services for production deployment.

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Container                         │
│  ┌────────────────┐              ┌────────────────────┐     │
│  │  FastAPI App   │              │  Worker Thread     │     │
│  │  (HTTP Server) │              │  (Job Processor)   │     │
│  │                │              │                    │     │
│  │  - Projects    │              │  - Poll MongoDB    │     │
│  │  - Process     │              │  - Extract Meta    │     │
│  │  - Jobs        │              │  - Gen Thumbnail   │     │
│  └────────┬───────┘              │  - Run Potree      │     │
│           │                      └─────────┬──────────┘     │
│           │                                │                │
│           └────────────┬───────────────────┘                │
│                        │                                    │
└────────────────────────┼────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼────┐    ┌─────▼─────┐   ┌────▼────┐
    │ MongoDB │    │   Azure   │   │ Potree  │
    │ (Cosmos)│    │   Blob    │   │Converter│
    │         │    │  Storage  │   │ Binary  │
    └─────────┘    └───────────┘   └─────────┘
```

### Component Interaction Flow

**Project Creation:**

```
User → POST /projects/upload → MongoDB → Response
```

**Point Cloud Processing:**

```
User → POST /process/{id}/potree → Create Job → Response (job_id)
                                         ↓
                                    MongoDB (job record)
                                         ↓
Worker Thread → Poll Jobs → Process Job:
    1. Extract metadata (CRS, location, point count)
    2. Generate thumbnail (PDAL density map)
    3. Upload thumbnail to Azure
    4. Run PotreeConverter
    5. Upload Potree files to Azure
    6. Update project with URLs
    7. Cleanup temp files
    8. Mark job complete
```

## Components and Interfaces

### 1. API Routes

#### Projects Router (`routes/projects.py`)

**Endpoints:**

- `GET /projects/` - List all projects with pagination
- `POST /projects/upload` - Create new project
- `GET /projects/{id}` - Get project by ID
- `PUT /projects/{id}/update` - Update project metadata
- `DELETE /projects/{id}/delete` - Delete project and all associated files

**Models:**

- `Project` - Main project model with metadata
- `Location` - WGS84 coordinates (lat, lon, z)
- `CRS` - Coordinate Reference System info
- `ProjectResponse` - Extended response with timestamps

**Key Changes:**

- Add `import json` for tag parsing
- Fix `update_project` to accept `id` parameter correctly
- Add pagination support to `get_all_projects`

#### Process Router (`routes/process.py`)

**Endpoints:**

- `POST /process/{id}/potree` - Upload point cloud and start processing job

**Workflow:**

1. Accept file upload (LAS/LAZ)
2. Save to temporary location
3. Upload to Azure Blob Storage (`jobs/{job_id}.laz`)
4. Create job record in MongoDB
5. Return job_id and status immediately

**Key Changes:**

- Combine `/process/{id}` and `/process/{id}/potree` into single endpoint
- Remove synchronous processing
- Delegate to background worker

#### Jobs Router (`routes/jobs.py`)

**Endpoints:**

- `GET /jobs/{job_id}` - Get job status
- `GET /jobs/project/{project_id}` - Get all jobs for a project

**Response Format:**

```json
{
  "job_id": "abc123",
  "project_id": "XXXX-XXX-A",
  "status": "processing",
  "progress": {
    "step": "converting",
    "message": "Running PotreeConverter..."
  },
  "created_at": "2025-11-09T10:00:00Z",
  "updated_at": "2025-11-09T10:05:00Z"
}
```

### 2. Background Worker

#### Worker Module (`worker.py`)

**Purpose:** Process jobs asynchronously without blocking HTTP requests

**Implementation:**

```python
class JobWorker:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.running = True
        self.poll_interval = 5  # seconds

    def start(self):
        """Main worker loop"""
        while self.running:
            job = self.get_next_job()
            if job:
                self.process_job(job)
            else:
                time.sleep(self.poll_interval)

    def get_next_job(self):
        """Find oldest pending job"""
        return self.db.jobs.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "processing", "updated_at": datetime.utcnow()}},
            sort=[("created_at", 1)]
        )

    def process_job(self, job):
        """Execute job steps"""
        try:
            # 1. Extract metadata
            # 2. Generate thumbnail
            # 3. Run Potree conversion
            # 4. Upload files
            # 5. Update project
            # 6. Cleanup
            self.mark_complete(job)
        except Exception as e:
            self.mark_failed(job, str(e))
```

**Startup Integration:**

```python
# main.py
@app.on_event("startup")
async def startup_event():
    # Reset stale jobs
    DB.jobs.update_many(
        {"status": "processing"},
        {"$set": {"status": "pending"}}
    )
    # Start worker thread
    worker = JobWorker(DB)
    thread = threading.Thread(target=worker.start, daemon=True)
    thread.start()
```

### 3. Storage Layer

#### Database Manager (`storage/db.py`)

**Collections:**

- `Project` - Project metadata
- `Job` - Background job tracking

**New Methods:**

```python
# Job Management
def create_job(self, project_id: str, file_path: str) -> str
def get_job(self, job_id: str) -> Job
def update_job_status(self, job_id: str, status: str, **kwargs)
def get_jobs_by_project(self, project_id: str) -> List[Job]
def cleanup_old_jobs(self, hours: int = 72)
```

#### Azure Storage Manager (`storage/az.py`)

**Key Methods:**

```python
def upload_folder_with_structure(self, folder_path: str, blob_prefix: str):
    """Upload entire folder maintaining structure with correct MIME types"""

def upload_thumbnail(self, project_id: str, image_data: bytes) -> str:
    """Upload thumbnail and return SAS URL"""

def delete_project_files(self, project_id: str):
    """Delete all files for a project"""

def delete_job_file(self, job_id: str):
    """Delete temporary job file"""
```

**File Organization:**

```
Azure Blob Container: hwc-potree/
├── jobs/
│   └── {job_id}.laz          (temporary, deleted after processing)
├── {project_id}/
│   ├── thumbnail.png          (generated preview)
│   ├── metadata.json          (Potree metadata)
│   ├── hierarchy.bin          (Potree spatial index)
│   ├── octree.bin             (Potree point data)
│   └── ... (other Potree files)
```

### 4. Processing Utilities

#### Cloud Metadata Extractor (`utils/main.py`)

**Current Implementation:** Already complete

- Extracts CRS from LAS header
- Computes true mean center
- Transforms to WGS84
- Returns summary with point count

**No changes needed**

#### Thumbnail Generator (`utils/thumbnail.py`)

**New Module:**

```python
class ThumbnailGenerator:
    def __init__(self, size: int = 512):
        self.size = size

    def generate_from_las(self, las_path: str) -> bytes:
        """Generate thumbnail using PDAL"""
        # 1. Read point cloud with PDAL
        # 2. Extract XY coordinates and RGB if available
        # 3. Create 2D density map
        # 4. Apply colors if RGB exists
        # 5. Render to PIL Image with transparent background
        # 6. Maintain aspect ratio, fit to square
        # 7. Return PNG bytes
```

**Implementation Details:**

- Use PDAL to read points efficiently
- Create numpy array of XY coordinates
- Bin into 2D grid (density map)
- If RGB available, average colors per bin
- Use PIL to create image with RGBA mode
- Set alpha channel based on density
- Save as PNG with transparency

#### Potree Converter (`utils/potree.py`)

**Key Changes:**

```python
class PotreeConverter:
    def __init__(self):
        self.path = os.getenv("POTREE_PATH", "/app/PotreeConverter")

    def convert(self, input_path: str, output_dir: str, project: Project) -> str:
        """Convert LAS/LAZ to Potree format"""
        # 1. Create output directory
        # 2. Build command with proper args
        # 3. Run PotreeConverter subprocess
        # 4. Monitor progress
        # 5. Return path to output directory

    def upload_output(self, output_dir: str, project_id: str) -> str:
        """Upload Potree files to Azure and return viewer URL"""
        # 1. Upload all files with correct MIME types
        # 2. Generate SAS URL for viewer.html
        # 3. Return SAS URL
```

## Data Models

### Project Model

```python
class Project(BaseModel):
    id: str = Field(alias="_id")  # Job number (e.g., "XXXX-XXX-A")
    name: Optional[str]
    client: Optional[str]
    date: Optional[datetime]
    tags: List[str] = []
    description: Optional[str]

    # Point cloud data
    cloud: Optional[str]  # SAS URL to Potree viewer
    crs: Optional[CRS]
    location: Optional[Location]
    thumbnail: Optional[str]  # SAS URL to thumbnail.png

    # Metadata
    point_count: Optional[int]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
```

### Job Model

```python
class Job(BaseModel):
    id: str = Field(alias="_id")  # UUID
    project_id: str
    status: str  # "pending", "processing", "completed", "failed"

    # File paths
    file_path: str  # Local temp path
    azure_path: str  # Azure blob path (jobs/{job_id}.laz)

    # Progress tracking
    current_step: Optional[str]  # "metadata", "thumbnail", "conversion", "upload"
    progress_message: Optional[str]

    # Error handling
    error_message: Optional[str]
    retry_count: int = 0

    # Timestamps
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]
```

### Location Model

```python
class Location(BaseModel):
    lat: float = 0.0  # WGS84 latitude
    lon: float = 0.0  # WGS84 longitude
    z: float = 0.0    # Elevation
```

### CRS Model

```python
class CRS(BaseModel):
    id: str = Field(alias="_id")  # EPSG code (e.g., "EPSG:26916")
    name: Optional[str]  # Human-readable name
    proj4: Optional[str]  # Full proj4 string
```

## Error Handling

### Error Response Format

```python
class ErrorResponse(BaseModel):
    error: str
    message: str
    details: Optional[dict] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
```

### HTTP Status Codes

- `200 OK` - Successful request
- `201 Created` - Resource created
- `400 Bad Request` - Invalid input data
- `404 Not Found` - Resource not found
- `409 Conflict` - Resource already exists
- `500 Internal Server Error` - Server error

### Exception Handling Strategy

**API Layer:**

```python
@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"error": "Validation Error", "message": str(exc)}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "message": "An unexpected error occurred"}
    )
```

**Worker Layer:**

```python
def process_job(self, job):
    try:
        # Processing steps
        pass
    except FileNotFoundError as e:
        self.mark_failed(job, f"File not found: {e}")
    except subprocess.CalledProcessError as e:
        self.mark_failed(job, f"Potree conversion failed: {e}")
    except Exception as e:
        logger.error(f"Job {job['_id']} failed", exc_info=True)
        self.mark_failed(job, f"Unexpected error: {e}")
```

### Logging Strategy

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Azure captures stdout/stderr
    ]
)

logger = logging.getLogger(__name__)

# Usage
logger.info(f"Processing job {job_id}")
logger.warning(f"Job {job_id} taking longer than expected")
logger.error(f"Job {job_id} failed", exc_info=True)
```

## Testing Strategy

### Unit Tests

**Test Coverage:**

- `utils/main.py` - CloudMetadata extraction
- `utils/thumbnail.py` - Thumbnail generation
- `utils/potree.py` - Potree conversion (mocked)
- `storage/db.py` - Database operations (mocked)
- `storage/az.py` - Azure operations (mocked)

**Example Test:**

```python
def test_thumbnail_generation():
    generator = ThumbnailGenerator(size=512)
    thumbnail_bytes = generator.generate_from_las("test.laz")
    assert len(thumbnail_bytes) > 0
    # Verify it's a valid PNG
    img = Image.open(BytesIO(thumbnail_bytes))
    assert img.format == "PNG"
    assert img.mode == "RGBA"
```

### Integration Tests

**Test Scenarios:**

1. Create project → Upload point cloud → Check job status → Verify completion
2. Upload invalid file → Verify error handling
3. Delete project → Verify Azure files deleted
4. Worker processes multiple jobs → Verify correct order

### Manual Testing

**Local Development:**

```bash
# Build Docker image
docker build -t hwc-potree-api .

# Run with environment variables
docker run -p 8000:8000 \
  -e MONGO_CONNECTION_STRING="..." \
  -e AZURE_STORAGE_CONNECTION_STRING_2="..." \
  -e NAME="hwc-potree" \
  hwc-potree-api

# Test endpoints
curl http://localhost:8000/docs
```

## Deployment Configuration

### Docker Configuration

**Dockerfile:**

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy PotreeConverter binary
COPY bin/PotreeConverter /app/PotreeConverter
RUN chmod +x /app/PotreeConverter

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Run application
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
```

### Environment Variables

**Required:**

- `MONGO_CONNECTION_STRING` - MongoDB connection string
- `AZURE_STORAGE_CONNECTION_STRING_2` - Azure Blob Storage connection
- `NAME` - Database and container name

**Optional:**

- `PORT` - HTTP port (default: 8000)
- `POTREE_PATH` - Path to PotreeConverter binary (default: /app/PotreeConverter)
- `JOB_CLEANUP_HOURS` - Hours before job cleanup (default: 72)
- `WORKER_POLL_INTERVAL` - Worker poll interval in seconds (default: 5)
- `LOG_LEVEL` - Logging level (default: INFO)

### Health Check Endpoint

```python
@app.get("/health")
async def health_check():
    """Health check for Azure monitoring"""
    try:
        # Check MongoDB connection
        DB.client.server_info()
        # Check Azure connection
        DB.az.container_client.get_container_properties()
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow(),
            "services": {
                "mongodb": "connected",
                "azure_blob": "connected"
            }
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )
```

### CI/CD Pipeline

**GitHub Actions Workflow (`.github/workflows/docker-build.yml`):**

```yaml
name: Build and Push Docker Image

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Login to Docker Hub
        uses: docker/login-action@v2
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}

      - name: Build and push
        uses: docker/build-push-action@v4
        with:
          context: .
          push: true
          tags: |
            ${{ secrets.DOCKERHUB_USERNAME }}/hwc-potree-api:latest
            ${{ secrets.DOCKERHUB_USERNAME }}/hwc-potree-api:${{ github.sha }}
```

**Required Secrets:**

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

## Performance Considerations

### Optimization Strategies

1. **File Upload:**

   - Stream large files directly to Azure (avoid loading into memory)
   - Use multipart upload for files > 100MB

2. **Thumbnail Generation:**

   - Sample points if cloud > 10M points
   - Cache thumbnails in Azure
   - Generate at lower resolution for very large clouds

3. **Potree Conversion:**

   - Run with appropriate memory limits
   - Monitor subprocess for hangs/crashes
   - Set timeout for very large files

4. **Database Queries:**

   - Index on `project_id`, `status`, `created_at`
   - Use projection to limit returned fields
   - Implement pagination for list endpoints

5. **Worker Efficiency:**
   - Process one job at a time initially
   - Add concurrency if needed (thread pool)
   - Implement job priority queue if needed

### Resource Limits

**Recommended Azure Container Settings:**

- CPU: 2 cores
- Memory: 4GB
- Storage: 20GB (for temp files)

**File Size Limits:**

- Max upload: 2GB per file
- Recommended: < 500MB for best performance

## Security Considerations

### Authentication & Authorization

**Future Enhancement:**

- Add API key authentication
- Implement user roles (admin, user)
- Project-level access control

**Current State:**

- No authentication (internal use)
- CORS enabled for all origins

### Data Protection

1. **Sensitive Data:**

   - Connection strings in environment variables only
   - Never log credentials
   - Use Azure Key Vault for production secrets

2. **File Validation:**

   - Verify file extensions (.las, .laz only)
   - Check file magic numbers
   - Limit file sizes
   - Scan for malicious content

3. **Input Validation:**
   - Pydantic models validate all inputs
   - Sanitize project IDs (alphanumeric + hyphens only)
   - Prevent path traversal in file operations

### Azure Security

1. **Blob Storage:**

   - Use SAS tokens with expiration (72 hours)
   - Private container (no public access)
   - Regenerate access keys periodically

2. **MongoDB:**
   - Use SSL/TLS connections
   - Restrict IP access if possible
   - Use strong passwords

## Design Decisions and Rationale

### Why Background Worker Thread?

**Decision:** Use threading.Thread instead of Celery/RQ

**Rationale:**

- Simpler deployment (no Redis/RabbitMQ)
- Lower cost (no additional services)
- Sufficient for current scale
- Easy to migrate to Celery later if needed

### Why PDAL for Thumbnails?

**Decision:** Use PDAL density maps instead of Open3D rendering

**Rationale:**

- Works in headless Docker environment
- No X server or GPU needed
- Faster for large point clouds
- Simpler implementation

### Why SAS URLs?

**Decision:** Use time-limited SAS URLs instead of public blobs

**Rationale:**

- Security (no public access)
- Flexibility (can revoke access)
- Compliance (audit access)
- Cost (no CDN needed initially)

### Why Single Container?

**Decision:** Run API and worker in same container

**Rationale:**

- Simpler deployment to Azure
- Lower cost (one container)
- Shared filesystem for temp files
- Can split later if needed
