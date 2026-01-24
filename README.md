# HWC Potree API

A FastAPI-based backend service that processes LiDAR point cloud data (LAS/LAZ files) and converts them to Potree format for web-based 3D visualization. The system uses MongoDB for metadata persistence, Azure Blob Storage for file storage, and a background worker for asynchronous processing.

## Features

- Project management (create, read, update, delete)
- Pagination and search with filtering by name, client, and tags
- Background point cloud processing with job tracking
- **Orthophoto upload and Cloud Optimized GeoTIFF (COG) conversion**
- Job cancellation for in-progress processing tasks
- Automatic metadata extraction (CRS, location, point count)
- Thumbnail generation from point clouds and orthophotos
- Potree format conversion for web visualization
- Statistics dashboard with system-wide metrics
- Azure Blob Storage integration with public URLs
- Health check endpoint for monitoring

## Workflows

### Point Cloud Workflow

- User uploads a project
- User uploads a point cloud to a corresponding project
  - Point cloud is processed in background, meaning user can close out or go to different pages on frontend like Pix4D
- Uploaded point clouds (LAS/LAZ) are temporarily stored in Azure Blob Storage then converted to Potree format
- Output from conversion is stored in a folder with the name of the project.id

### Orthophoto Workflow

- User uploads an orthophoto (GeoTIFF) to an existing project
- File is validated and uploaded to Azure temporary storage
- Background worker processes the file:
  - Validates GeoTIFF format using GDAL
  - Converts to Cloud Optimized GeoTIFF (COG) for efficient web streaming
  - Generates thumbnail preview (512px wide)
  - Uploads COG and thumbnail to Azure
  - Updates project with ortho URLs
- User can view orthophoto alongside point cloud data

## Environment Variables

### Required Variables

The following environment variables must be set for the application to run:

| Variable                            | Description                                        | Example                                                         |
| ----------------------------------- | -------------------------------------------------- | --------------------------------------------------------------- |
| `MONGO_CONNECTION_STRING`           | MongoDB connection string (MongoDB Atlas or local) | `mongodb+srv://user:pass@cluster.mongodb.net/`                  |
| `AZURE_STORAGE_CONNECTION_STRING_2` | Azure Blob Storage connection string               | `DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...` |
| `NAME`                              | Database and container name                        | `hwc-potree`                                                    |

### Optional Variables

| Variable               | Description                                 | Default                |
| ---------------------- | ------------------------------------------- | ---------------------- |
| `PORT`                 | HTTP port for the API server                | `8000`                 |
| `POTREE_PATH`          | Path to PotreeConverter binary              | `/app/PotreeConverter` |
| `JOB_CLEANUP_HOURS`    | Hours before old jobs are deleted           | `72`                   |
| `WORKER_POLL_INTERVAL` | Worker polling interval in seconds          | `5`                    |
| `LOG_LEVEL`            | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO`                 |

## Local Development

### Prerequisites

- Docker and Docker Compose
- MongoDB instance (local or Atlas)
- Azure Blob Storage account
- **GDAL** (Geospatial Data Abstraction Library) - Required for orthophoto processing
  - Version 3.0 or higher recommended
  - Must include COG (Cloud Optimized GeoTIFF) driver support

### Running with Docker

1. Clone the repository:

```bash
git clone https://github.com/MaFalana/HWC-POTREE-API.git
cd HWC-POTREE-API
```

2. Create a `.env` file with your configuration:

```bash
MONGO_CONNECTION_STRING=mongodb+srv://user:pass@cluster.mongodb.net/
AZURE_STORAGE_CONNECTION_STRING_2=DefaultEndpointsProtocol=https;AccountName=...
NAME=hwc-potree
PORT=8000
```

3. Build and run the Docker container:

```bash
docker build -t hwc-potree-api .
docker run -p 8000:8000 --env-file .env hwc-potree-api
```

4. Access the API documentation at `http://localhost:8000/docs`

### Running with Docker Compose

```bash
docker-compose up --build
```

### PotreeConverter Setup

The application requires the PotreeConverter binary for Linux. The binary should be placed in the `bin/` directory:

```bash
# Create bin directory if it doesn't exist
mkdir -p bin

# Copy or download PotreeConverter for Linux
# The binary should be named 'PotreeConverter' and be executable
cp /path/to/PotreeConverter bin/
chmod +x bin/PotreeConverter
```

**Note:** The Windows version in `utils/Potree Converter 2.1.1/` is for reference only. The Docker container uses the Linux binary from `bin/PotreeConverter`.

### GDAL Setup

The application requires GDAL (Geospatial Data Abstraction Library) for orthophoto processing. GDAL is used to:

- Validate uploaded GeoTIFF files
- Convert GeoTIFF to Cloud Optimized GeoTIFF (COG) format
- Generate thumbnail previews

**Docker Installation (Recommended):**

GDAL is automatically installed in the Docker container via the Dockerfile. No manual installation needed.

**Local Development Installation:**

For local development without Docker, install GDAL:

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y gdal-bin python3-gdal
```

**macOS (using Homebrew):**

```bash
brew install gdal
pip install gdal==$(gdal-config --version)
```

**Windows:**

Download and install from: https://gdal.org/download.html

Or use OSGeo4W: https://trac.osgeo.org/osgeo4w/

**Verify Installation:**

```bash
# Check GDAL version
gdalinfo --version

# Check COG driver support
gdalinfo --format COG
```

**Expected Output:**

```
GDAL 3.x.x, released 2023/xx/xx
Format Details:
  Short Name: COG
  Long Name: Cloud Optimized GeoTIFF
  ...
```

### Testing Point Cloud Conversion Locally

To test Potree conversion without Docker:

1. Install PotreeConverter locally
2. Set the environment variable:

```bash
export POTREE_PATH=/path/to/PotreeConverter
```

3. Run the conversion:

```bash
python -c "
from utils.potree import PotreeConverter
converter = PotreeConverter()
converter.convert('input.laz', 'output_dir', project_obj)
"
```

## API Endpoints

### Projects

#### `GET /projects/`

List all projects with pagination, sorting, and filtering support.

**Query Parameters:**

- `skip` (optional): Number of records to skip (default: 0)
- `limit` (optional): Maximum number of records to return (default: 100)
- `sort_by` (optional): Field to sort by - "name", "client", "created_at", "updated_at" (default: "created_at")
- `sort_order` (optional): Sort order - "asc" or "desc" (default: "desc")
- `name` (optional): Filter by project name (case-insensitive partial match)
- `client` (optional): Filter by client name (case-insensitive partial match)
- `tags` (optional): Filter by tags (comma-separated list, matches any tag)

**Response:**

```json
[
  {
    "_id": "XXXX-XXX-A",
    "name": "Project Name",
    "client": "Client Name",
    "date": "2025-11-09T00:00:00Z",
    "tags": ["survey", "lidar"],
    "description": "Project description",
    "cloud": "https://storage.blob.core.windows.net/...",
    "thumbnail": "https://storage.blob.core.windows.net/...",
    "location": {
      "lat": 40.7128,
      "lon": -74.006,
      "z": 10.5
    },
    "crs": {
      "_id": "EPSG:26916",
      "name": "NAD83 / UTM zone 16N"
    },
    "point_count": 1500000,
    "created_at": "2025-11-09T10:00:00Z",
    "updated_at": "2025-11-09T10:30:00Z"
  }
]
```

#### `POST /projects/upload`

Create a new project.

**Request Body:**

```json
{
  "id": "XXXX-XXX-A",
  "name": "Project Name",
  "client": "Client Name",
  "date": "2025-11-09",
  "tags": ["survey", "lidar"],
  "description": "Project description"
}
```

**Response:** `201 Created`

```json
{
  "_id": "XXXX-XXX-A",
  "name": "Project Name",
  "client": "Client Name",
  "created_at": "2025-11-09T10:00:00Z",
  "updated_at": "2025-11-09T10:00:00Z"
}
```

#### `GET /projects/{id}`

Get a specific project by ID.

**Response:** `200 OK` or `404 Not Found`

#### `PUT /projects/{id}/update`

Update project metadata. Accepts partial updates.

**Request Body:**

```json
{
  "name": "Updated Project Name",
  "tags": ["survey", "lidar", "updated"]
}
```

**Response:** `200 OK`

#### `GET /projects/{id}`

Get a specific project by ID with all metadata including point cloud and ortho URLs.
  "thumbnail": "https://storage.blob.core.windows.net/.../thumbnail.png?sv=..."
}
```

#### `DELETE /projects/delete`

Batch delete multiple projects and all associated files from Azure Blob Storage.

**Request Body:**

```json
["PROJ-001", "PROJ-002", "PROJ-003"]
```

**Response:** `200 OK`

```json
{
  "message": "Batch deletion completed",
  "deleted": ["PROJ-001", "PROJ-002"],
  "failed": [
    {
      "id": "PROJ-003",
      "error": "Project not found"
    }
  ],
  "deleted_count": 2,
  "failed_count": 1,
  "total": 3
}
```

#### `DELETE /projects/{id}/delete`

Delete a project and all associated files from Azure Blob Storage.

**Response:** `200 OK`

```json
{
  "message": "Project XXXX-XXX-A deleted successfully"
}
```

### Orthophoto

#### `POST /projects/{project_id}/ortho`

Upload an orthophoto (GeoTIFF) file for a project and start background COG conversion.

**Path Parameters:**

- `project_id`: Project ID

**Form Data:**

- `file`: GeoTIFF file (.tif or .tiff, max 30GB)

**Response:** `202 Accepted`

```json
{
  "message": "Ortho upload accepted for processing",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "PROJ-001",
  "status": "pending",
  "created_at": "2024-01-15T10:30:00Z"
}
```

**Processing Steps:**

1. File uploaded to Azure Blob Storage (`jobs/{job_id}.tif`)
2. Job record created in MongoDB with status "pending"
3. Background worker picks up job and:
   - Validates GeoTIFF with `gdalinfo`
   - Converts to COG format with JPEG compression
   - Generates 512px thumbnail preview
   - Uploads COG and thumbnail to Azure
   - Updates project with ortho URLs
   - Cleans up temporary files

**Project Response After Processing:**

```json
{
  "_id": "PROJ-001",
  "name": "Highway Survey",
  "ortho": {
    "url": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho.png",
    "thumbnail": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho_thumbnail.png",
    "bounds": [[south, west], [north, east]]
  }
}
```

### Processing

#### `POST /process/{id}/potree`

Upload a point cloud file and start background processing job.

**Path Parameters:**

- `id`: Project ID

**Form Data:**

- `file`: LAS or LAZ file (multipart/form-data)
- `epsg` (optional): EPSG code for coordinate system (e.g., "26916")

**Response:** `200 OK`

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Job created successfully"
}
```

**Processing Steps:**

1. File uploaded to Azure Blob Storage (`jobs/{job_id}.laz`)
2. Job record created in MongoDB with status "pending"
3. Background worker picks up job and:
   - Extracts metadata (CRS, location, point count)
   - Generates thumbnail
   - Converts to Potree format
   - Uploads output to Azure
   - Updates project with URLs
   - Cleans up temporary files

### Jobs

#### `GET /jobs/{job_id}`

Get the status and progress of a specific job.

**Response:** `200 OK` or `404 Not Found`

```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "XXXX-XXX-A",
  "status": "processing",
  "current_step": "conversion",
  "progress_message": "Running PotreeConverter...",
  "created_at": "2025-11-09T10:00:00Z",
  "updated_at": "2025-11-09T10:05:00Z"
}
```

**Status Values:**

- `pending`: Job is waiting to be processed
- `processing`: Job is currently being processed
- `completed`: Job completed successfully
- `failed`: Job failed (check `error_message` field)

#### `GET /jobs/project/{project_id}`

Get all jobs associated with a specific project.

**Response:** `200 OK`

```json
[
  {
    "_id": "550e8400-e29b-41d4-a716-446655440000",
    "project_id": "XXXX-XXX-A",
    "status": "completed",
    "created_at": "2025-11-09T10:00:00Z",
    "completed_at": "2025-11-09T10:15:00Z"
  }
]
```

#### `POST /jobs/project/{project_id}/cancel`

Cancel all pending or processing jobs for a specific project.

**Response:** `200 OK`

```json
{
  "message": "Cancelled 2 job(s) for project PROJ-001",
  "project_id": "PROJ-001",
  "cancelled_jobs": ["job-uuid-1", "job-uuid-2"],
  "cancelled_count": 2,
  "skipped_count": 1
}
```

**Use Cases:**

- User uploaded wrong file and wants to stop all processing
- Clearing pending jobs before re-uploading

#### `POST /jobs/{job_id}/cancel`

Cancel a job that is pending or currently processing.

**Response:** `200 OK`

```json
{
  "message": "Job cancelled successfully",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "cancelled"
}
```

**Error Responses:**

- `404 Not Found`: Job does not exist
- `400 Bad Request`: Job is already completed, failed, or cancelled

### Statistics

#### `GET /stats`

Get system-wide statistics for dashboard display.

**Response:** `200 OK`

```json
{
  "total_projects": 150,
  "total_points": 45000000,
  "active_jobs": 3,
  "completed_jobs_24h": 12,
  "failed_jobs_24h": 1,
  "timestamp": "2025-11-25T10:00:00Z"
}
```

**Statistics Included:**

- `total_projects`: Total number of projects in the system
- `total_points`: Sum of all point counts across projects
- `active_jobs`: Number of jobs currently pending or processing
- `completed_jobs_24h`: Jobs completed in the last 24 hours
- `failed_jobs_24h`: Jobs that failed in the last 24 hours

### Health

#### `GET /health`

Health check endpoint for monitoring service status.

**Response:** `200 OK` (healthy) or `503 Service Unavailable` (unhealthy)

```json
{
  "status": "healthy",
  "timestamp": "2025-11-09T10:00:00Z",
  "services": {
    "mongodb": "connected",
    "azure_blob": "connected"
  }
}
```

## CI/CD Pipeline

### GitHub Actions Workflow

The repository includes an automated CI/CD pipeline that builds and pushes Docker images to Docker Hub on every push to the `main` branch.

**Workflow file:** `.github/workflows/deploy.yml`

**Triggers:**

- Push to `main` branch
- Manual workflow dispatch

**What it does:**

1. Checks out the code
2. Sets up Docker Buildx
3. Logs in to Docker Hub
4. Builds the Docker image
5. Pushes two tags:
   - `mfalana/hwc-potree-api:latest`
   - `mfalana/hwc-potree-api:<commit-sha>`
6. Provides deployment instructions in the workflow output

### Required GitHub Secrets

To enable the CI/CD pipeline, configure the following secrets in your GitHub repository:

1. Go to `Settings` → `Secrets and variables` → `Actions`
2. Add the following secrets:
   - `DOCKERHUB_USERNAME` - Your Docker Hub username
   - `DOCKERHUB_TOKEN` - Your Docker Hub access token (create at https://hub.docker.com/settings/security)

### Deployment to Azure Container Apps

After the GitHub Actions workflow completes, deploy the new image to Azure:

1. Open Azure Cloud Shell (https://shell.azure.com)

2. Run the update command (replace `<commit-sha>` with the actual commit SHA from the workflow):

```bash
az containerapp update \
  -g 'LiDAR-Breakline-Generator' \
  -n 'hwc-potree-api' \
  --image docker.io/mfalana/hwc-potree-api:<commit-sha>
```

3. Verify the deployment:

```bash
az containerapp show \
  -g 'LiDAR-Breakline-Generator' \
  -n 'hwc-potree-api' \
  --query properties.latestRevisionName
```

### Manual Deployment

If you need to deploy manually without using GitHub Actions:

1. Build the Docker image locally:

```bash
docker build -t mfalana/hwc-potree-api:manual .
```

2. Push to Docker Hub:

```bash
docker login
docker push mfalana/hwc-potree-api:manual
```

3. Update Azure Container App:

```bash
az containerapp update \
  -g 'LiDAR-Breakline-Generator' \
  -n 'hwc-potree-api' \
  --image docker.io/mfalana/hwc-potree-api:manual
```

## Architecture

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

## Testing

### Testing the API Locally

1. Start the application with Docker:

```bash
docker-compose up --build
```

2. Open the interactive API documentation:

```
http://localhost:8000/docs
```

3. Test the workflow:

**Step 1: Create a project**

```bash
curl -X POST "http://localhost:8000/projects/upload" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "TEST-001-A",
    "name": "Test Project",
    "client": "Test Client",
    "tags": ["test"]
  }'
```

**Step 2: Upload a point cloud**

```bash
curl -X POST "http://localhost:8000/process/TEST-001-A/potree" \
  -F "file=@/path/to/your/pointcloud.laz" \
  -F "epsg=26916"
```

**Step 3: Check job status**

```bash
curl "http://localhost:8000/jobs/{job_id}"
```

**Step 4: Cancel a job (if needed)**

```bash
curl -X POST "http://localhost:8000/jobs/{job_id}/cancel"
```

**Step 5: Get updated project**

```bash
curl "http://localhost:8000/projects/TEST-001-A"
```

**Step 6: Browse projects with filters**

```bash
# Get projects sorted by name
curl "http://localhost:8000/projects/?sort_by=name&sort_order=asc"

# Filter by client
curl "http://localhost:8000/projects/?client=Test%20Client"

# Filter by tags
curl "http://localhost:8000/projects/?tags=test,survey"
```

**Step 7: Upload an orthophoto (optional)**

```bash
curl -X POST "http://localhost:8000/projects/TEST-001-A/ortho" \
  -F "file=@/path/to/your/orthophoto.tif"
```

**Step 8: View system statistics**

```bash
curl "http://localhost:8000/stats"
```

### Verifying Azure Storage

After processing completes, verify files in Azure Blob Storage:

```bash
# List blobs in container
az storage blob list \
  --account-name <your-account> \
  --container-name hwc-potree \
  --prefix TEST-001-A/ \
  --output table
```

Expected structure:

```
TEST-001-A/
├── thumbnail.png
├── metadata.json
├── hierarchy.bin
├── octree.bin
└── ... (other Potree files)
```

## Troubleshooting

### Common Issues

**Issue: "Connection refused" when connecting to MongoDB**

- Verify `MONGO_CONNECTION_STRING` is correct
- Check network connectivity to MongoDB instance
- Ensure IP whitelist includes your IP (for MongoDB Atlas)

**Issue: "Azure Blob Storage authentication failed"**

- Verify `AZURE_STORAGE_CONNECTION_STRING_2` is correct
- Check that the storage account exists
- Ensure the container `hwc-potree` exists

**Issue: "PotreeConverter not found"**

- Verify `POTREE_PATH` environment variable
- Ensure PotreeConverter binary is executable: `chmod +x /app/PotreeConverter`
- Check that the binary is copied in Dockerfile

**Issue: Job stuck in "processing" status**

- Check worker logs: `docker logs <container-id>`
- Restart the application (worker resets stale jobs on startup)
- Check disk space for temporary files

**Issue: Thumbnail generation fails**

- Ensure PDAL is installed in Docker image
- Check that point cloud has valid XY coordinates
- Verify file is not corrupted

**Issue: "GDAL not found" or "gdalinfo command not found"**

- Verify GDAL is installed: `gdalinfo --version`
- Check GDAL is in system PATH
- For Docker: Rebuild container to ensure GDAL is installed
- For local: Install GDAL using package manager (see GDAL Setup section)

**Issue: Ortho job fails with "Invalid GeoTIFF file"**

- Verify file is a valid GeoTIFF: `gdalinfo your_file.tif`
- Check file has georeferencing information
- Ensure file is not corrupted or password-protected
- Try opening file in QGIS or similar GIS software

**Issue: COG conversion fails**

- Check GDAL supports COG driver: `gdalinfo --format COG`
- Verify GDAL version is 3.0 or higher
- Check disk space for temporary files
- Review worker logs for GDAL error messages

### Viewing Logs

**Docker logs:**

```bash
docker logs -f <container-id>
```

**Azure Container Apps logs:**

```bash
az containerapp logs show \
  -g 'LiDAR-Breakline-Generator' \
  -n 'hwc-potree-api' \
  --follow
```

### Health Check

Monitor service health:

```bash
curl http://localhost:8000/health
```

## Technology Stack

- **Python 3.12** - Runtime
- **FastAPI** - Web framework
- **MongoDB** - Database (via pymongo)
- **Azure Blob Storage** - File storage
- **PDAL** - Point cloud processing
- **PotreeConverter** - Point cloud format conversion
- **GDAL** - Geospatial data processing and COG conversion
- **Docker** - Containerization

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]

## Frontend Integration Guide

### API Base URL

Set your API base URL based on environment:

````typescript
const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";
```

### Required Fields for Project Creation

When creating a project, the following fields are **required**:

- `id` - Unique project identifier (e.g., "XXXX-XXX-A")
- `crs_id` - EPSG code (e.g., "26916")
- `crs_name` - Human-readable CRS name (e.g., "NAD83 UTM Zone 16N")
- `crs_proj4` - Proj4 string for coordinate system

**Example:**

```javascript
const formData = new FormData();
formData.append("id", "PROJ-2025-001");
formData.append("crs_id", "26916");
formData.append("crs_name", "NAD83 UTM Zone 16N");
formData.append(
  "crs_proj4",
  "+proj=utm +zone=16 +datum=NAD83 +units=m +no_defs"
);
formData.append("name", "Highway Survey Project");
formData.append("client", "DOT");

const response = await fetch(`${API_BASE_URL}/projects/upload`, {
  method: "POST",
  body: formData,
});
```

### File Upload with Progress

Upload point cloud files with progress tracking:

```javascript
async function uploadPointCloud(projectId, file, epsg, onProgress) {
  const formData = new FormData();
  formData.append("file", file);
  if (epsg) formData.append("epsg", epsg);

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();

    // Track upload progress
    xhr.upload.addEventListener("progress", (e) => {
      if (e.lengthComputable) {
        const percent = (e.loaded / e.total) * 100;
        onProgress(percent);
      }
    });

    xhr.addEventListener("load", () => {
      if (xhr.status === 200) {
        resolve(JSON.parse(xhr.responseText));
      } else {
        reject(new Error("Upload failed"));
      }
    });

    xhr.addEventListener("error", () => {
      reject(new Error("Upload failed"));
    });

    xhr.open("POST", `${API_BASE_URL}/process/${projectId}/potree`);
    xhr.send(formData);
  });
}
```

### Job Status Polling

Poll job status until completion:

```javascript
async function pollJobUntilComplete(jobId, onProgress) {
  const poll = async () => {
    const response = await fetch(`${API_BASE_URL}/jobs/${jobId}`);
    const job = await response.json();

    if (onProgress) onProgress(job);

    if (job.status === 'completed') {
      return job;
    } else if (job.status === 'failed') {
      throw new Error(job.error_message || 'Job failed');
    }

    // Continue polling
    await new Promise(resolve => setTimeout(resolve, 2000));
    return poll();
  };

  return poll();
}
```

### Public URLs

All file URLs are **public and permanent** - they never expire!

```javascript
// URLs are simple and direct - no expiration checking needed
const url = "https://storage.blob.core.windows.net/container/project-id/metadata.json";
fetch(url).then(response => response.json());
```

### Best Practices

1. **Always validate file types** before upload (`.las` or `.laz` only)
2. **Show upload progress** for better UX
3. **Poll job status** every 2-5 seconds (don't poll too frequently)
4. **Handle all error cases** (404, 400, 500, network errors)
5. **Cache project data** to avoid repeated API calls
6. **Show processing status** to users (pending, processing, completed, failed)
7. **Provide cancel option** for long uploads (though job will still process)
8. **Display thumbnails** as preview before full point cloud loads
9. **Validate CRS data** before project creation
10. **Use TypeScript** for type safety

### Rate Limiting

Currently, there are no rate limits enforced. For production, consider implementing rate limiting on your frontend to avoid overwhelming the API.

### File Size Limits

- Maximum file size: **30GB**
- Recommended: **< 10GB** for faster processing
- Potree automatically handles downsampling during conversion

### Support

For issues or questions:

- GitHub: https://github.com/MaFalana/HWC-POTREE-API
- Check `/health` endpoint for API status
- Review logs in Azure Container Apps

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]


## Frontend Integration Guide

### API Base URL

Set your API base URL based on environment:

```typescript
const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";
````

### Required Fields for Project Creation

When creating a project, the following fields are **required**:

- `id` - Unique project identifier (e.g., "XXXX-XXX-A")
- `crs_id` - EPSG code (e.g., "26916")
- `crs_name` - Human-readable CRS name (e.g., "NAD83 UTM Zone 16N")
- `crs_proj4` - Proj4 string for coordinate system

**Example:**

```javascript
const formData = new FormData();
formData.append("id", "PROJ-2025-001");
formData.append("crs_id", "26916");
formData.append("crs_name", "NAD83 UTM Zone 16N");
formData.append(
  "crs_proj4",
  "+proj=utm +zone=16 +datum=NAD83 +units=m +no_defs"
);
formData.append("name", "Highway Survey Project");
formData.append("client", "DOT");

const response = await fetch(`${API_BASE_URL}/projects/upload`, {
  method: "POST",
  body: formData,
});
```

### File Upload with Progress

Upload point cloud files with progress tracking:

```javascript
async function uploadPointCloud(projectId, file, epsg, onProgress) {
  const formData = new FormData();
  formData.append("file", file);
  if (epsg) formData.append("epsg", epsg);

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();

    // Track upload progress
    xhr.upload.addEventListener("progress", (e) => {
      if (e.lengthComputable) {
        const percent = (e.loaded / e.total) * 100;
        onProgress(percent);
      }
    });

    xhr.addEventListener("load", () => {
      if (xhr.status === 200) {
        resolve(JSON.parse(xhr.responseText));
      } else {
        reject(new Error("Upload failed"));
      }
    });

    xhr.addEventListener("error", () => {
      reject(new Error("Upload failed"));
    });

    xhr.open("POST", `${API_BASE_URL}/process/${projectId}/potree`);
    xhr.send(formData);
  });
}
```

### Job Status Polling

Poll job status until completion:

```javascript
async function pollJobUntil Complete(jobId, onProgress) {
  const poll = async () => {
    const response = await fetch(`${API_BASE_URL}/jobs/${jobId}`);
    const job = await response.json();

    if (onProgress) onProgress(job);

    if (job.status === 'completed') {
      return job;
    } else if (job.status === 'failed') {
      throw new Error(job.error_message || 'Job failed');
    }

    // Continue polling
    await new Promise(resolve => setTimeout(resolve, 2000));
    return poll();
  };

  return poll();
}
```

### Complete Workflow Example

```javascript
// Step 1: Create project
const projectData = new FormData();
projectData.append("id", "PROJ-2025-001");
projectData.append("crs_id", "26916");
projectData.append("crs_name", "NAD83 UTM Zone 16N");
projectData.append(
  "crs_proj4",
  "+proj=utm +zone=16 +datum=NAD83 +units=m +no_defs"
);
projectData.append("name", "My Project");
projectData.append("client", "Client Name");

const projectResponse = await fetch(`${API_BASE_URL}/projects/upload`, {
  method: "POST",
  body: projectData,
});
const project = await projectResponse.json();

// Step 2: Upload point cloud
const uploadResponse = await uploadPointCloud(
  project.ID,
  file,
  "26916",
  (percent) => console.log(`Upload: ${percent}%`)
);

// Step 3: Poll job status
const completedJob = await pollJobUntilComplete(uploadResponse.job_id, (job) =>
  console.log(`Status: ${job.status} - ${job.progress_message}`)
);

// Step 4: Get updated project with cloud URL
const updatedProjectResponse = await fetch(
  `${API_BASE_URL}/projects/${project.ID}`
);
const updatedProject = await updatedProjectResponse.json();

console.log("Potree URL:", updatedProject.cloud);
console.log("Thumbnail URL:", updatedProject.thumbnail);
```

### Error Handling

Always handle errors gracefully:

```javascript
try {
  const response = await fetch(`${API_BASE_URL}/projects/${id}`);

  if (response.status === 404) {
    console.error("Project not found");
    return;
  }

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const project = await response.json();
  // Use project data
} catch (error) {
  console.error("Failed to fetch project:", error);
  // Show user-friendly error message
}
```

### TypeScript Types

```typescript
interface CRS {
  _id: string;
  name: string;
  proj4: string;
}

interface Location {
  lat: number;
  lon: number;
  z: number;
}

interface Project {
  _id: string;
  name: string;
  client: string;
  date?: string;
  tags: string[];
  description?: string;
  cloud?: string;
  thumbnail?: string;
  crs: CRS;
  location: Location;
  point_count?: number;
  created_at: string;
  updated_at: string;
}

interface Job {
  _id: string;
  project_id: string;
  status: "pending" | "processing" | "completed" | "failed";
  current_step?: string;
  progress_message?: string;
  error_message?: string;
  created_at: string;
  updated_at: string;
  completed_at?: string;
}
```

### CORS Configuration

If you encounter CORS issues during development, the API already has CORS enabled for all origins. For production, you may want to restrict this in `main.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-frontend-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Public URLs

All file URLs are **public and permanent** - they never expire!

```javascript
// URLs are simple and direct - no expiration checking needed
const url = "https://storage.blob.core.windows.net/container/project-id/metadata.json";
fetch(url).then(response => response.json());
```

### Best Practices

1. **Always validate file types** before upload (`.las` or `.laz` only)
2. **Show upload progress** for better UX
3. **Poll job status** every 2-5 seconds (don't poll too frequently)
4. **Handle all error cases** (404, 400, 500, network errors)
5. **Cache project data** to avoid repeated API calls
6. **Show processing status** to users (pending, processing, completed, failed)
7. **Provide cancel option** for long uploads (though job will still process)
8. **Display thumbnails** as preview before full point cloud loads
9. **Validate CRS data** before project creation
10. **Use TypeScript** for type safety

### Rate Limiting

Currently, there are no rate limits enforced. For production, consider implementing rate limiting on your frontend to avoid overwhelming the API.

### File Size Limits

- Maximum file size: **30GB**
- Recommended: **< 10GB** for faster processing
- Potree automatically handles downsampling during conversion

### Support

For issues or questions:

- GitHub: https://github.com/MaFalana/HWC-POTREE-API
- Check `/health` endpoint for API status
- Review logs in Azure Container Apps
