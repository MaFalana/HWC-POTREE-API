from fastapi import APIRouter, HTTPException
from storage.db import DatabaseManager
from models.Job import JobResponse
from typing import List
import logging

DB = DatabaseManager()

logger = logging.getLogger(__name__)

# Jobs ROUTER
jobs_router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    responses={404: {"description": "Not found"}},
)


@jobs_router.get(
    '/{job_id}',
    response_model=JobResponse,
    summary="Get job status",
    description="Retrieve the current status and progress of a processing job.",
    response_description="Job details including status, progress, and timestamps"
)
async def get_job(job_id: str):
    """
    Get the status and progress of a processing job.
    
    Use this endpoint to check the status of a point cloud processing job
    created by the `/process/{id}/potree` endpoint.
    
    **Path Parameters:**
    - **job_id**: Job identifier (UUID returned from process endpoint)
    
    **Job Status Values:**
    - `pending`: Job is waiting to be processed
    - `processing`: Job is currently being processed
    - `completed`: Job completed successfully
    - `failed`: Job failed (check error_message field)
    
    **Processing Steps:**
    - `metadata`: Extracting point cloud metadata
    - `thumbnail`: Generating thumbnail preview
    - `conversion`: Converting to Potree format
    - `upload`: Uploading files to Azure
    
    **Returns:**
    - 200: Job found and returned
    - 404: Job not found
    - 500: Server error
    
    **Example Response (Processing):**
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
    
    **Example Response (Completed):**
    ```json
    {
      "_id": "550e8400-e29b-41d4-a716-446655440000",
      "project_id": "XXXX-XXX-A",
      "status": "completed",
      "created_at": "2025-11-09T10:00:00Z",
      "updated_at": "2025-11-09T10:15:00Z",
      "completed_at": "2025-11-09T10:15:00Z"
    }
    ```
    
    **Example Response (Failed):**
    ```json
    {
      "_id": "550e8400-e29b-41d4-a716-446655440000",
      "project_id": "XXXX-XXX-A",
      "status": "failed",
      "error_message": "PotreeConverter failed: Invalid point cloud format",
      "created_at": "2025-11-09T10:00:00Z",
      "updated_at": "2025-11-09T10:05:00Z"
    }
    ```
    """
    logger.info(f"Retrieving job: {job_id}")
    
    try:
        job = DB.get_job(job_id)
        
        if not job:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail=f"Job with id {job_id} not found")
        
        logger.info(f"Retrieved job {job_id} with status: {job.status}")
        return job
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve job from database")


@jobs_router.get(
    '/project/{project_id}',
    response_model=List[JobResponse],
    summary="Get jobs by project",
    description="Retrieve all processing jobs associated with a specific project.",
    response_description="List of jobs sorted by creation date (newest first)"
)
async def get_jobs_by_project(project_id: str):
    """
    Get all jobs for a specific project.
    
    Returns a list of all processing jobs (past and present) for a project,
    sorted by creation date with the newest jobs first.
    
    **Path Parameters:**
    - **project_id**: Project identifier
    
    **Returns:**
    - 200: List of jobs (may be empty if no jobs exist)
    - 500: Server error
    
    **Example Response:**
    ```json
    [
      {
        "_id": "550e8400-e29b-41d4-a716-446655440000",
        "project_id": "XXXX-XXX-A",
        "status": "completed",
        "created_at": "2025-11-09T10:00:00Z",
        "completed_at": "2025-11-09T10:15:00Z"
      },
      {
        "_id": "660e8400-e29b-41d4-a716-446655440001",
        "project_id": "XXXX-XXX-A",
        "status": "failed",
        "error_message": "File corrupted",
        "created_at": "2025-11-08T15:30:00Z"
      }
    ]
    ```
    
    **Use Cases:**
    - View processing history for a project
    - Check if any jobs are currently processing
    - Debug failed processing attempts
    - Monitor job completion times
    """
    logger.info(f"Retrieving jobs for project: {project_id}")
    
    try:
        jobs = DB.get_jobs_by_project(project_id)
        logger.info(f"Retrieved {len(jobs)} jobs for project: {project_id}")
        return jobs
    except Exception as e:
        logger.error(f"Failed to retrieve jobs for project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve jobs from database")
