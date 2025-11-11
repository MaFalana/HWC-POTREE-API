from fastapi import APIRouter, File, UploadFile, Form, HTTPException
from typing import Optional
import uuid
import logging

from config.main import *

logger = logging.getLogger(__name__)

# Process ROUTER

process_router = APIRouter(
    prefix="/process",
    tags=["Process"],
    responses={404: {"description": "Not found"}},
)


def _save_temp(upload: UploadFile) -> str:
    """Save uploaded file to temporary location"""
    try:
        suffix = os.path.splitext(upload.filename or "")[1] or ".laz"
        
        # Validate file extension
        if suffix.lower() not in ['.las', '.laz']:
            raise ValueError(f"Invalid file type: {suffix}. Only .las and .laz files are supported")
        
        fd, path = tempfile.mkstemp(suffix=suffix, prefix="pc_")
        with os.fdopen(fd, "wb") as f:
            shutil.copyfileobj(upload.file, f)
        upload.file.seek(0)
        
        logger.info(f"Saved uploaded file to: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to save uploaded file: {e}", exc_info=True)
        raise


@process_router.post(
    '/{id}/potree',
    summary="Upload and process point cloud",
    description="Upload a LAS/LAZ point cloud file and start background processing job.",
    response_description="Job information with job_id for status tracking"
)
async def process_point_cloud(
    id: str,
    file: UploadFile = File(..., description="LAS or LAZ point cloud file"),
    epsg: Optional[str] = Form(None, description="EPSG code for coordinate system (e.g., '26916')")
):
    """
    Upload a point cloud file and create a background processing job.
    
    This endpoint initiates asynchronous processing of a point cloud file. The file is
    uploaded to Azure Blob Storage and a background job is created to process it.
    
    **Processing Steps (performed by background worker):**
    1. Extract metadata (CRS, location, point count)
    2. Generate thumbnail preview
    3. Convert to Potree format for web visualization
    4. Upload all outputs to Azure Blob Storage
    5. Update project with URLs and metadata
    6. Clean up temporary files
    
    **Path Parameters:**
    - **id**: Project identifier (must exist)
    
    **Form Parameters:**
    - **file** (required): LAS or LAZ point cloud file
    - **epsg** (optional): EPSG code for coordinate system (e.g., "26916" for NAD83 UTM Zone 16N)
    
    **Supported File Types:**
    - .las (LAS format)
    - .laz (compressed LAS format)
    
    **File Size Limits:**
    - Maximum: 30GB (Potree automatically creates multi-resolution LODs)
    - Recommended: < 10GB for faster processing
    - Note: Larger files will take longer to process but Potree handles downsampling automatically
    
    **Example Request:**
    ```bash
    curl -X POST "http://localhost:8000/process/XXXX-XXX-A/potree" \\
      -F "file=@pointcloud.laz" \\
      -F "epsg=26916"
    ```
    
    **Returns:**
    - 200: Job created successfully
    - 400: Invalid file type or format
    - 404: Project not found
    - 409: Job already exists
    - 500: Server error
    
    **Example Response:**
    ```json
    {
      "message": "Job created successfully. Processing will begin shortly.",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "project_id": "XXXX-XXX-A",
      "status": "pending",
      "note": "Use GET /jobs/{job_id} to check the status of this job"
    }
    ```
    
    **Next Steps:**
    Use the returned `job_id` to check processing status:
    ```bash
    curl "http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000"
    ```
    """
    logger.info(f"Processing point cloud upload for project: {id}, file: {file.filename}")
    
    try:
        # Verify project exists
        project = DB.getProject({'_id': id})
        if not project:
            logger.warning(f"Project not found: {id}")
            raise HTTPException(status_code=404, detail=f"Project with id {id} not found")
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        logger.info(f"Created job {job_id} for project {id}")
        
        # Save uploaded file to temporary location
        try:
            temp_path = _save_temp(file)
            logger.info(f"Saved uploaded file to temporary location: {temp_path}")
        except ValueError as e:
            # Invalid file type
            logger.warning(f"Invalid file upload: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to save uploaded file: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to save uploaded file")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in process_point_cloud: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred")
    
    # Upload file to Azure Blob Storage (preserve original extension)
    file_extension = os.path.splitext(file.filename or "")[1] or ".laz"
    azure_path = f"jobs/{job_id}{file_extension}"
    try:
        logger.info(f"Uploading file to Azure: {azure_path}")
        DB.az.upload_file(temp_path, azure_path)
        logger.info(f"Successfully uploaded file to Azure: {azure_path}")
    except Exception as e:
        logger.error(f"Failed to upload file to Azure: {e}", exc_info=True)
        # Clean up temp file if Azure upload fails
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"Failed to upload file to Azure: {str(e)}")
    
    # Create job record in MongoDB with status="pending"
    try:
        logger.info(f"Creating job record in MongoDB: {job_id}")
        DB.create_job(
            project_id=id,
            file_path=temp_path,
            azure_path=azure_path,
            job_id=job_id
        )
        logger.info(f"Successfully created job record: {job_id}")
    except ValueError as e:
        # Job already exists (duplicate)
        logger.warning(f"Duplicate job creation attempt: {e}")
        # Clean up temp file and Azure blob
        if os.path.exists(temp_path):
            os.remove(temp_path)
        try:
            DB.az.delete_blob(azure_path)
        except:
            pass
        raise HTTPException(status_code=409, detail=f"Job already exists: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to create job record: {e}", exc_info=True)
        # Clean up temp file and Azure blob if job creation fails
        if os.path.exists(temp_path):
            os.remove(temp_path)
        try:
            DB.az.delete_blob(azure_path)
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")
    
    logger.info(f"Job {job_id} created successfully for project {id}")
    
    # Return job_id and status immediately
    return {
        "message": "Job created successfully. Processing will begin shortly.",
        "job_id": job_id,
        "project_id": id,
        "status": "pending",
        "note": "Use GET /jobs/{job_id} to check the status of this job"
    }