# HWC Potree API Documentation

## Table of Contents

- [Overview](#overview)
- [Base URL](#base-url)
- [Authentication](#authentication)
- [Orthophoto Upload Feature](#orthophoto-upload-feature)
  - [POST /projects/{project_id}/ortho](#post-projectsproject_idortho)
  - [Workflow](#workflow)
  - [Error Responses](#error-responses)
  - [Use Cases](#use-cases)
- [Project Endpoints](#project-endpoints)
- [Job Endpoints](#job-endpoints)
- [Processing Endpoints](#processing-endpoints)
- [Statistics Endpoints](#statistics-endpoints)
- [Health Check](#health-check)

## Overview

The HWC Potree API is a FastAPI-based backend service for processing LiDAR point cloud data and orthophotos. It provides:

- Point cloud processing (LAS/LAZ to Potree format)
- Orthophoto processing (GeoTIFF to Cloud Optimized GeoTIFF)
- Project management with metadata
- Background job processing with status tracking
- Azure Blob Storage integration

## Base URL

**Development:** `http://localhost:8000`
**Production:** `https://your-production-url.com`

## Authentication

Currently, this API does not require authentication (internal use only).

---

## Orthophoto Upload Feature

### POST /projects/{project_id}/ortho

Upload a GeoTIFF orthophoto file for a project. The file will be automatically converted to Cloud Optimized GeoTIFF (COG) format with thumbnail generation.

#### Endpoint

```
POST /projects/{project_id}/ortho
```

#### Path Parameters

| Parameter    | Type   | Required | Description                      |
| ------------ | ------ | -------- | -------------------------------- |
| `project_id` | string | Yes      | Unique identifier of the project |

#### Request Body

**Content-Type:** `multipart/form-data`

| Field  | Type | Required | Description                            |
| ------ | ---- | -------- | -------------------------------------- |
| `file` | File | Yes      | GeoTIFF file (.tif or .tiff, max 30GB) |

#### Response

**Status Code:** `202 Accepted`

**Response Body:**

```json
{
  "message": "Ortho upload accepted for processing",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "PROJ-001",
  "status": "pending",
  "created_at": "2024-01-15T10:30:00Z"
}
```

#### Response Fields

| Field        | Type   | Description                                 |
| ------------ | ------ | ------------------------------------------- |
| `message`    | string | Success message                             |
| `job_id`     | string | UUID of the created job for status tracking |
| `project_id` | string | Project identifier                          |
| `status`     | string | Initial job status (always "pending")       |
| `created_at` | string | ISO 8601 timestamp of job creation          |

---

### Workflow

The orthophoto upload follows this workflow:

```
1. Upload File
   ↓
2. Validation (file type, size, project exists)
   ↓
3. Upload to Azure Temporary Storage
   ↓
4. Create Job (status: pending)
   ↓
5. Background Worker Processing:
   - Download file
   - Validate GeoTIFF with gdalinfo
   - Convert to COG format
   - Generate thumbnail (512px wide)
   - Upload COG and thumbnail to Azure
   - Update project with URLs
   ↓
6. Job Complete (status: completed)
```

**Processing Steps:**

1. **Validation** - Verify file is a valid GeoTIFF using `gdalinfo`
2. **COG Conversion** - Convert to Cloud Optimized GeoTIFF with:
   - JPEG compression (quality 85)
   - Tiling enabled (512px blocks)
   - Optimized for web streaming
3. **Thumbnail Generation** - Create 512px wide PNG preview
4. **Azure Upload** - Store files at:
   - COG: `{project_id}/ortho/ortho.tif`
   - Thumbnail: `{project_id}/ortho/ortho_thumbnail.png`
5. **Project Update** - Add ortho URLs to project document

**Monitoring Progress:**

Poll the job status endpoint to track progress:

```bash
GET /jobs/{job_id}
```

The `progress_message` field will show the current step:

- "Downloading file"
- "Validating file"
- "Converting to COG"
- "Generating thumbnail"
- "Uploading to Azure"
- "Ortho conversion completed"

---

### Error Responses

#### 400 Bad Request

**Cause:** Invalid file type or missing file

```json
{
  "detail": "Invalid file type. Only .tif and .tiff files are supported"
}
```

**Possible Reasons:**

- File extension is not `.tif` or `.tiff`
- No file provided in the request
- File is not a valid GeoTIFF (detected during processing)

---

#### 404 Not Found

**Cause:** Project does not exist

```json
{
  "detail": "Project with id PROJ-001 not found"
}
```

**Solution:** Verify the project ID exists using `GET /projects/{id}`

---

#### 413 Payload Too Large

**Cause:** File exceeds 30GB size limit

```json
{
  "detail": "File size exceeds 30GB limit (uploaded: 35.2GB)"
}
```

**Solution:** Reduce file size or split into multiple tiles

---

#### 500 Internal Server Error

**Cause:** Server error during upload or processing

```json
{
  "error": "Internal Server Error",
  "message": "An unexpected error occurred. Please try again later.",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Possible Reasons:**

- Azure storage connection failure
- Database connection failure
- Disk space issues

**Solution:** Check server logs and retry the request

---

### Use Cases

#### Use Case 1: Upload Orthophoto for New Project

```bash
# Step 1: Create project
curl -X POST "http://localhost:8000/projects/upload" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "PROJ-001",
    "name": "Highway Survey",
    "client": "DOT"
  }'

# Step 2: Upload orthophoto
curl -X POST "http://localhost:8000/projects/PROJ-001/ortho" \
  -F "file=@orthophoto.tif"

# Response:
# {
#   "message": "Ortho upload accepted for processing",
#   "job_id": "550e8400-e29b-41d4-a716-446655440000",
#   "project_id": "PROJ-001",
#   "status": "pending",
#   "created_at": "2024-01-15T10:30:00Z"
# }

# Step 3: Monitor job status
curl "http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000"

# Step 4: Get updated project with ortho URLs
curl "http://localhost:8000/projects/PROJ-001"

# Response includes:
# {
#   "_id": "PROJ-001",
#   "name": "Highway Survey",
#   "ortho": {
#     "url": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho.png",
#     "thumbnail": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho_thumbnail.png",
#     "bounds": [[south, west], [north, east]]
#   }
# }
```

---

#### Use Case 2: Replace Existing Orthophoto

```bash
# Upload new orthophoto (overwrites existing)
curl -X POST "http://localhost:8000/projects/PROJ-001/ortho" \
  -F "file=@new_orthophoto.tif"

# The old ortho files will be overwritten in Azure storage
# The project will be updated with new URLs
```

---

#### Use Case 3: Cancel Ortho Processing

```bash
# Step 1: Upload orthophoto
curl -X POST "http://localhost:8000/projects/PROJ-001/ortho" \
  -F "file=@orthophoto.tif"

# Step 2: Cancel the job
curl -X POST "http://localhost:8000/jobs/{job_id}/cancel"

# Response:
# {
#   "message": "Job cancelled successfully",
#   "job_id": "550e8400-e29b-41d4-a716-446655440000",
#   "status": "cancelled"
# }

# All temporary files will be cleaned up
```

---

#### Use Case 4: Handle Processing Errors

```bash
# Upload invalid file
curl -X POST "http://localhost:8000/projects/PROJ-001/ortho" \
  -F "file=@not_a_geotiff.tif"

# Job will be created, but will fail during validation
# Check job status:
curl "http://localhost:8000/jobs/{job_id}"

# Response:
# {
#   "_id": "550e8400-e29b-41d4-a716-446655440000",
#   "project_id": "PROJ-001",
#   "status": "failed",
#   "error_message": "Invalid GeoTIFF file: gdalinfo failed",
#   "created_at": "2024-01-15T10:30:00Z",
#   "completed_at": "2024-01-15T10:31:00Z"
# }
```

---

### JavaScript/TypeScript Example

```typescript
async function uploadOrthophoto(
  projectId: string,
  file: File
): Promise<string> {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(
    `http://localhost:8000/projects/${projectId}/ortho`,
    {
      method: "POST",
      body: formData,
    }
  );

  if (!response.ok) {
    if (response.status === 404) {
      throw new Error("Project not found");
    } else if (response.status === 400) {
      throw new Error(
        "Invalid file type. Only .tif and .tiff files are supported"
      );
    } else if (response.status === 413) {
      throw new Error("File too large. Maximum size is 30GB");
    } else {
      throw new Error("Upload failed");
    }
  }

  const data = await response.json();
  return data.job_id;
}

// Usage
const file = document.querySelector('input[type="file"]').files[0];
const jobId = await uploadOrthophoto("PROJ-001", file);
console.log("Job created:", jobId);

// Poll for completion
async function pollJobStatus(jobId: string): Promise<void> {
  while (true) {
    const response = await fetch(`http://localhost:8000/jobs/${jobId}`);
    const job = await response.json();

    console.log("Status:", job.status, "-", job.progress_message);

    if (job.status === "completed") {
      console.log("Processing complete!");
      break;
    } else if (job.status === "failed") {
      throw new Error(job.error_message);
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

await pollJobStatus(jobId);
```

---

### Python Example

```python
import requests
import time

def upload_orthophoto(project_id: str, file_path: str) -> str:
    """Upload orthophoto and return job ID."""
    url = f"http://localhost:8000/projects/{project_id}/ortho"

    with open(file_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(url, files=files)

    if response.status_code == 404:
        raise ValueError("Project not found")
    elif response.status_code == 400:
        raise ValueError("Invalid file type")
    elif response.status_code == 413:
        raise ValueError("File too large")
    elif response.status_code != 202:
        raise Exception(f"Upload failed: {response.text}")

    data = response.json()
    return data['job_id']

def poll_job_status(job_id: str) -> None:
    """Poll job status until completion."""
    url = f"http://localhost:8000/jobs/{job_id}"

    while True:
        response = requests.get(url)
        job = response.json()

        print(f"Status: {job['status']} - {job.get('progress_message', '')}")

        if job['status'] == 'completed':
            print("Processing complete!")
            break
        elif job['status'] == 'failed':
            raise Exception(f"Job failed: {job.get('error_message')}")

        time.sleep(2)

# Usage
job_id = upload_orthophoto('PROJ-001', 'orthophoto.tif')
print(f"Job created: {job_id}")
poll_job_status(job_id)
```

---

### Performance Expectations

| File Size | Expected Processing Time |
| --------- | ------------------------ |
| < 1GB     | 2-5 minutes              |
| 1-5GB     | 5-15 minutes             |
| 5-10GB    | 15-25 minutes            |
| 10-30GB   | 25-45 minutes            |

**Factors Affecting Performance:**

- File size and compression
- Server CPU and disk I/O
- Network bandwidth to Azure
- Concurrent job processing

---

### File Size Limits

- **Maximum:** 30GB
- **Recommended:** < 10GB for faster processing
- **Minimum:** No minimum (but very small files may not benefit from COG optimization)

---

### Supported File Formats

- **Input:** GeoTIFF (.tif, .tiff)
- **Output:** Cloud Optimized GeoTIFF (COG) with JPEG compression
- **Thumbnail:** PNG (512px wide)

**Requirements:**

- Valid GeoTIFF with georeferencing information
- Readable by GDAL
- Single-band or multi-band (RGB, RGBA)

---

### Storage Structure

Files are stored in Azure Blob Storage with the following structure:

```
hwc-potree/
├── jobs/
│   └── {job_id}.tif              # Temporary uploaded file (deleted after processing)
├── {project_id}/
│   ├── ortho/
│   │   ├── ortho.tif             # Cloud Optimized GeoTIFF
│   │   └── ortho_thumbnail.png   # Thumbnail preview
│   ├── metadata.json             # Potree metadata (point cloud)
│   └── thumbnail.png             # Point cloud thumbnail
```

---

### Public URLs

All file URLs are **public and permanent** - they never expire!

- **Format:** `https://{account}.blob.core.windows.net/{container}/{blob-path}`
- **Permissions:** Public read access
- **Expiration:** None - URLs are permanent

**Example:**

```javascript
// URLs are simple and permanent
const url = "https://storage.blob.core.windows.net/container/project-id/metadata.json";

// No expiration checking needed
fetch(url).then(response => response.json());
```

---

### Troubleshooting

#### Problem: "Invalid file type" error

**Solution:**

- Ensure file extension is `.tif` or `.tiff`
- Verify file is a valid GeoTIFF (open in QGIS or similar)
- Check file is not corrupted

---

#### Problem: Job fails with "Invalid GeoTIFF file"

**Solution:**

- Verify file can be opened with `gdalinfo`
- Check file has valid georeferencing
- Ensure file is not password-protected or encrypted

---

#### Problem: Job stuck in "processing" status

**Solution:**

- Wait longer (large files take time)
- Check server logs for errors
- Cancel and retry if stuck for > 1 hour

---

#### Problem: Thumbnail not generated

**Solution:**

- This is optional and won't fail the job
- Check server logs for thumbnail generation errors
- Thumbnail may fail for certain image formats

---

#### Problem: "Project not found" error

**Solution:**

- Verify project exists: `GET /projects/{id}`
- Check project ID spelling
- Create project first if it doesn't exist

---

## Project Endpoints

### GET /projects/

List all projects with pagination and filtering.

**Query Parameters:**

- `limit` (optional): Number of projects per page (default: 50)
- `offset` (optional): Number of projects to skip (default: 0)
- `sort_by` (optional): Field to sort by (default: "created_at")
- `sort_order` (optional): Sort order - "asc" or "desc" (default: "desc")
- `search` (optional): Search term for project name
- `client` (optional): Filter by client name
- `tags` (optional): Comma-separated list of tags

**Response:**

```json
{
  "Message": "Successfully retrieved a list of projects from database",
  "Projects": [
    {
      "_id": "PROJ-001",
      "name": "Highway Survey",
      "client": "DOT",
      "cloud": "https://...",
      "thumbnail": "https://...",
      "ortho": {
        "file": "https://...",
        "thumbnail": "https://..."
      }
    }
  ],
  "pagination": {
    "total": 150,
    "limit": 50,
    "offset": 0,
    "has_more": true
  }
}
```

---

### GET /projects/{id}

Get a specific project by ID.

**Response:**

```json
{
  "_id": "PROJ-001",
  "name": "Highway Survey",
  "client": "DOT",
  "cloud": "https://...",
  "thumbnail": "https://...",
  "ortho": {
    "url": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho.png",
    "thumbnail": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho_thumbnail.png",
    "bounds": [[south, west], [north, east]]
  },
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
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

---

### POST /projects/upload

Create a new project.

**Request Body:**

```json
{
  "id": "PROJ-001",
  "name": "Highway Survey",
  "client": "DOT",
  "tags": ["survey", "highway"],
  "description": "Highway survey project"
}
```

**Response:** `201 Created`

---

### DELETE /projects/{id}/delete

Delete a project and all associated files (including ortho files).

**Response:**

```json
{
  "message": "Project PROJ-001 deleted successfully"
}
```

---

## Job Endpoints

### GET /jobs/{job_id}

Get job status and progress.

**Response:**

```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "PROJ-001",
  "type": "ortho_conversion",
  "status": "processing",
  "progress_message": "Converting to COG",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:05:00Z"
}
```

**Status Values:**

- `pending` - Waiting to be processed
- `processing` - Currently being processed
- `completed` - Successfully completed
- `failed` - Failed (check `error_message`)
- `cancelled` - Cancelled by user

---

### POST /jobs/{job_id}/cancel

Cancel a pending or processing job.

**Response:**

```json
{
  "message": "Job cancelled successfully",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "cancelled"
}
```

---

## Processing Endpoints

### POST /process/{id}/potree

Upload a point cloud file for processing.

**Form Parameters:**

- `file` - LAS or LAZ file
- `epsg` (optional) - EPSG code for coordinate system

**Response:**

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Job created successfully"
}
```

---

## Statistics Endpoints

### GET /stats

Get system-wide statistics.

**Response:**

```json
{
  "total_projects": 150,
  "total_points": 45000000,
  "active_jobs": 3,
  "completed_jobs_24h": 12,
  "failed_jobs_24h": 1,
  "timestamp": "2024-01-15T10:00:00Z"
}
```

---

## Health Check

### GET /health

Check API health status.

**Response:**

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

---

## Rate Limits

Currently, no rate limits are enforced.

---

## Support

For issues or questions:

- GitHub: https://github.com/MaFalana/HWC-POTREE-API
- Check `/health` endpoint for API status
- Review server logs for detailed error information
