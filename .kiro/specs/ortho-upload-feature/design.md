# Design Document: Orthophoto Upload and COG Conversion

## Overview

This document describes the technical design for adding orthophoto upload and Cloud Optimized GeoTIFF (COG) conversion to the HWC Potree API. The feature will allow users to upload GeoTIFF files for their projects, which will be automatically converted to COG format and stored alongside point cloud data in Azure Blob Storage.

## Architecture

### High-Level Flow

```
User Upload → API Endpoint → Job Creation → Worker Processing → Azure Storage → Project Update
```

1. User uploads TIF/TIFF file via POST `/projects/{project_id}/ortho`
2. API validates file extension and project existence
3. File is uploaded to Azure temporary storage (`jobs/{job_id}.tif`)
4. Job is created with type "ortho_conversion" and queued
5. Worker picks up job and processes it:
   - Download file from Azure
   - Validate with gdalinfo
   - Convert to COG using gdal_translate
   - Generate thumbnail (optional)
   - Upload COG and thumbnail to Azure
   - Update project with ortho URLs
6. Job marked as completed, user can access ortho via project

### Component Diagram

```
┌─────────────────┐
│   FastAPI       │
│   Endpoint      │
│  /projects/     │
│  {id}/ortho     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Azure Blob    │
│   Storage       │
│  jobs/{id}.tif  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Job Queue     │
│   (MongoDB)     │
│  type: ortho_   │
│  conversion     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   JobWorker     │
│   - Download    │
│   - Validate    │
│   - Convert     │
│   - Thumbnail   │
│   - Upload      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Azure Blob    │
│   Storage       │
│  {project_id}/  │
│  ortho/         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Project       │
│   Document      │
│  ortho: {       │
│    file: url    │
│    thumbnail    │
│  }              │
└─────────────────┘
```

## Data Models

### Project Model Extension

Add `ortho` field to the existing Project model:

```python
class Ortho(BaseModel):
    file: Optional[str] = None  # SAS URL to COG file
    thumbnail: Optional[str] = None  # SAS URL to thumbnail PNG

class Project(BaseModel):
    # ... existing fields ...
    ortho: Optional[Ortho] = None
```

MongoDB document structure:

```json
{
  "_id": "PROJ-001",
  "name": "Highway Survey",
  "client": "DOT",
  "cloud": "https://...",
  "thumbnail": "https://...",
  "ortho": {
    "file": "https://storage.blob.core.windows.net/hwc-potree/PROJ-001/ortho/ortho.tif?sas_token",
    "thumbnail": "https://storage.blob.core.windows.net/hwc-potree/PROJ-001/ortho/ortho_thumbnail.png?sas_token"
  },
  "created_at": "2024-01-15T10:30:00Z"
}
```

### Job Model Extension

No changes needed to Job model structure. Ortho jobs will use existing fields:

- `type`: "ortho_conversion" (new job type)
- `status`: "pending" | "processing" | "completed" | "failed" | "cancelled"
- `progress_message`: Current step (e.g., "Validating file", "Converting to COG", "Generating thumbnail", "Uploading to Azure")

## API Endpoints

### POST /projects/{project_id}/ortho

Upload an orthophoto for a project.

**Request:**

```
POST /projects/PROJ-001/ortho
Content-Type: multipart/form-data

file: [binary TIF/TIFF file]
```

**Response (202 Accepted):**

```json
{
  "message": "Ortho upload accepted for processing",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "PROJ-001",
  "status": "pending",
  "created_at": "2024-01-15T10:30:00Z"
}
```

**Error Responses:**

- 400: Invalid file type or missing file
- 404: Project not found
- 413: File too large (>30GB)
- 500: Server error

### Existing Endpoints (Modified)

**GET /projects/{id}** - Include ortho field in response
**GET /projects/** - Include ortho field for each project
**DELETE /projects/{id}/delete** - Delete ortho files along with project

## Worker Processing

### OrthoProcessor Class

Create a new method in JobWorker class to handle ortho conversion:

```python
def process_ortho_job(self, job: Job) -> None:
    """Process an ortho conversion job."""
    try:
        # Update status
        self.db.update_job_status(job.id, "processing", "Downloading file")

        # Download from Azure
        local_file = self._download_ortho_file(job)

        # Check cancellation
        self._check_cancellation(job.id)

        # Validate file
        self.db.update_job_progress(job.id, "Validating file")
        self._validate_geotiff(local_file)

        # Check cancellation
        self._check_cancellation(job.id)

        # Convert to COG
        self.db.update_job_progress(job.id, "Converting to COG")
        cog_file = self._convert_to_cog(local_file)

        # Check cancellation
        self._check_cancellation(job.id)

        # Generate thumbnail (optional)
        self.db.update_job_progress(job.id, "Generating thumbnail")
        thumbnail_file = self._generate_ortho_thumbnail(cog_file)

        # Check cancellation
        self._check_cancellation(job.id)

        # Upload to Azure
        self.db.update_job_progress(job.id, "Uploading to Azure")
        ortho_urls = self._upload_ortho_to_azure(job.project_id, cog_file, thumbnail_file)

        # Update project
        self._update_project_ortho(job.project_id, ortho_urls)

        # Mark complete
        self.db.update_job_status(job.id, "completed", "Ortho conversion completed")

        # Cleanup
        self._cleanup_ortho_files(local_file, cog_file, thumbnail_file, job.id)

    except CancellationException:
        self._handle_ortho_cancellation(job)
    except Exception as e:
        self._handle_ortho_error(job, e)
```

### GDAL Commands

**Validation:**

```bash
gdalinfo /tmp/ortho_input.tif
```

**COG Conversion:**

```bash
gdal_translate \
  -of COG \
  -co COMPRESS=JPEG \
  -co QUALITY=85 \
  -co TILED=YES \
  -co BLOCKSIZE=512 \
  /tmp/ortho_input.tif \
  /tmp/ortho_output.tif
```

**Thumbnail Generation:**

```bash
gdal_translate \
  -of PNG \
  -outsize 512 0 \
  /tmp/ortho_output.tif \
  /tmp/ortho_thumbnail.png
```

### Helper Methods

```python
def _validate_geotiff(self, file_path: str) -> None:
    """Validate that file is a readable GeoTIFF using gdalinfo."""
    result = subprocess.run(
        ['gdalinfo', file_path],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise ValueError(f"Invalid GeoTIFF file: {result.stderr}")

def _convert_to_cog(self, input_path: str) -> str:
    """Convert GeoTIFF to COG format."""
    output_path = input_path.replace('.tif', '_cog.tif')
    result = subprocess.run([
        'gdal_translate',
        '-of', 'COG',
        '-co', 'COMPRESS=JPEG',
        '-co', 'QUALITY=85',
        '-co', 'TILED=YES',
        '-co', 'BLOCKSIZE=512',
        input_path,
        output_path
    ], capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"COG conversion failed: {result.stderr}")

    return output_path

def _generate_ortho_thumbnail(self, cog_path: str) -> Optional[str]:
    """Generate thumbnail from COG. Returns None if fails."""
    try:
        thumbnail_path = cog_path.replace('.tif', '_thumbnail.png')
        result = subprocess.run([
            'gdal_translate',
            '-of', 'PNG',
            '-outsize', '512', '0',
            cog_path,
            thumbnail_path
        ], capture_output=True, text=True)

        if result.returncode != 0:
            logger.warning(f"Thumbnail generation failed: {result.stderr}")
            return None

        return thumbnail_path
    except Exception as e:
        logger.warning(f"Thumbnail generation error: {e}")
        return None

def _upload_ortho_to_azure(self, project_id: str, cog_path: str, thumbnail_path: Optional[str]) -> dict:
    """Upload COG and thumbnail to Azure, return URLs."""
    # Upload COG
    cog_blob_name = f"{project_id}/ortho/ortho.tif"
    self.db.az.upload_file(
        file_path=cog_path,
        blob_name=cog_blob_name,
        overwrite=True
    )
    cog_url = self.db.az.generate_sas_url(cog_blob_name, hours_valid=720)

    # Upload thumbnail if exists
    thumbnail_url = None
    if thumbnail_path and os.path.exists(thumbnail_path):
        thumbnail_blob_name = f"{project_id}/ortho/ortho_thumbnail.png"
        self.db.az.upload_file(
            file_path=thumbnail_path,
            blob_name=thumbnail_blob_name,
            overwrite=True
        )
        thumbnail_url = self.db.az.generate_sas_url(thumbnail_blob_name, hours_valid=720)

    return {
        'file': cog_url,
        'thumbnail': thumbnail_url
    }

def _update_project_ortho(self, project_id: str, ortho_urls: dict) -> None:
    """Update project document with ortho URLs."""
    project = self.db.getProject({'_id': project_id})
    if not project:
        raise ValueError(f"Project {project_id} not found")

    project.ortho = Ortho(
        file=ortho_urls['file'],
        thumbnail=ortho_urls['thumbnail']
    )

    self.db.updateProject(project)

def _cleanup_ortho_files(self, *file_paths, job_id: str) -> None:
    """Clean up local temporary files and Azure job file."""
    # Delete local files
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted local file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to delete {file_path}: {e}")

    # Delete Azure job file
    try:
        self.db.az.delete_job_file(job_id)
    except Exception as e:
        logger.error(f"Failed to delete Azure job file: {e}")

def _handle_ortho_cancellation(self, job: Job) -> None:
    """Handle cancellation of ortho job."""
    logger.info(f"Ortho job {job.id} cancelled")

    # Cleanup local files
    temp_dir = f"/tmp/{job.id}"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    # Delete Azure job file
    try:
        self.db.az.delete_job_file(job.id)
    except Exception as e:
        logger.error(f"Failed to cleanup Azure job file: {e}")

    # Delete partial ortho files from Azure
    try:
        prefix = f"{job.project_id}/ortho/"
        # Note: Only delete if we were in the middle of uploading
        # This prevents deleting existing ortho if job cancelled early
    except Exception as e:
        logger.error(f"Failed to cleanup partial ortho files: {e}")

    # Update job
    self.db.update_job_status(
        job.id,
        "cancelled",
        "Job cancelled by user",
        completed_at=datetime.utcnow()
    )
```

## Azure Storage Structure

```
hwc-potree/
├── jobs/
│   └── {job_id}.tif              # Temporary uploaded file
├── {project_id}/
│   ├── ortho/
│   │   ├── ortho.tif             # COG file
│   │   └── ortho_thumbnail.png   # Thumbnail
│   ├── metadata.json             # Potree metadata
│   ├── thumbnail.png             # Point cloud thumbnail
│   └── ... (other Potree files)
```

## Database Operations

### New Methods in DatabaseManager

```python
def update_project_ortho(self, project_id: str, ortho_file_url: str, ortho_thumbnail_url: Optional[str]) -> bool:
    """Update project with ortho URLs."""
    result = self.projects_collection.update_one(
        {'_id': project_id},
        {
            '$set': {
                'ortho.file': ortho_file_url,
                'ortho.thumbnail': ortho_thumbnail_url,
                'updated_at': datetime.utcnow()
            }
        }
    )
    return result.modified_count > 0
```

No other database changes needed - existing job methods work for ortho jobs.

## Error Handling

### Error Scenarios and Responses

| Scenario                   | Detection Point   | Action                | User Message                                                 |
| -------------------------- | ----------------- | --------------------- | ------------------------------------------------------------ |
| Invalid file extension     | API endpoint      | Return 400            | "Invalid file type. Only .tif and .tiff files are supported" |
| Project not found          | API endpoint      | Return 404            | "Project with id {id} not found"                             |
| File too large             | API endpoint      | Return 413            | "File size exceeds 30GB limit"                               |
| Invalid GeoTIFF            | Worker validation | Fail job              | "Invalid GeoTIFF file: {gdalinfo error}"                     |
| GDAL conversion error      | Worker conversion | Fail job              | "COG conversion failed: {gdal error}"                        |
| Azure upload error         | Worker upload     | Fail job              | "Failed to upload ortho to Azure storage"                    |
| Thumbnail generation error | Worker thumbnail  | Log warning, continue | N/A (optional feature)                                       |
| Cancellation               | Worker (any step) | Cancel job, cleanup   | "Job cancelled by user"                                      |

### Cleanup on Error

When any error occurs:

1. Delete local temporary files
2. Delete Azure job file (`jobs/{job_id}.tif`)
3. Do NOT delete existing project ortho files (preserve previous ortho)
4. Mark job as failed with error message
5. Log full error with stack trace

## Security Considerations

1. **File Validation**: Validate file extension at API level, validate file content with gdalinfo at worker level
2. **Size Limits**: Enforce 30GB limit to prevent resource exhaustion
3. **SAS URLs**: Use 30-day expiration with read-only permissions
4. **Path Traversal**: Use project_id directly in blob paths, no user-provided paths
5. **Resource Limits**: GDAL commands run with subprocess timeout to prevent hanging
6. **Cleanup**: Always clean up temporary files, even on error
7. **Error Messages**: Don't expose internal paths or system details in error messages

## Performance Considerations

1. **Streaming**: Use streaming for Azure uploads/downloads to handle large files
2. **Compression**: JPEG compression reduces file size while maintaining quality
3. **Tiling**: COG tiling enables efficient partial reads
4. **Async Processing**: Job queue prevents blocking API requests
5. **Cancellation**: Check cancellation frequently to avoid wasted processing
6. **Thumbnail**: Optional thumbnail generation doesn't block main conversion
7. **Cleanup**: Immediate cleanup of temp files prevents disk space issues

## Testing Strategy

### Unit Tests

- File validation logic
- GDAL command construction
- Azure upload/download operations
- Project model serialization
- Error handling for each failure scenario

### Integration Tests

- End-to-end ortho upload workflow
- Cancellation during each processing step
- Overwriting existing ortho
- Multiple concurrent ortho jobs
- Large file handling (>10GB)

### Manual Testing

- Upload various GeoTIFF formats
- Test with corrupted files
- Test with non-GeoTIFF files
- Verify COG optimization with QGIS
- Verify thumbnail quality
- Test cancellation at each step

## Deployment Considerations

1. **GDAL Installation**: Ensure GDAL is installed on worker machines with COG driver
2. **Python Bindings**: Install osgeo.gdal Python package
3. **Disk Space**: Ensure sufficient temp disk space for large file processing
4. **Environment Variables**: No new environment variables needed
5. **Database Migration**: No migration needed (ortho field is optional)
6. **Backward Compatibility**: Existing projects without ortho field work unchanged

## Future Enhancements (Out of Scope)

- Multiple orthos per project
- Ortho reprojection/coordinate transformation
- Ortho mosaicking
- Advanced GDAL options (overviews, masks)
- Ortho editing/cropping
- Direct ortho viewing in API
- Ortho metadata extraction (resolution, bounds, etc.)
