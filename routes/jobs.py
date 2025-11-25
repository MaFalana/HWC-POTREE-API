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


@jobs_router.post(
    '/{job_id}/cancel',
    summary="Cancel a job",
    description="Cancel a pending or processing job. Cannot cancel completed, failed, or already cancelled jobs.",
    response_description="Cancellation confirmation with job details"
)
async def cancel_job(job_id: str):
    """
    Cancel a processing job.
    
    Cancels a job that is currently pending or processing. The worker will
    detect the cancellation and stop processing, cleaning up any temporary files.
    
    **Path Parameters:**
    - **job_id**: Job identifier (UUID)
    
    **Cancellable States:**
    - `pending`: Job will be marked as cancelled immediately
    - `processing`: Job will be cancelled at the next checkpoint
    
    **Non-Cancellable States:**
    - `completed`: Job has already finished successfully
    - `failed`: Job has already failed
    - `cancelled`: Job is already cancelled
    
    **Returns:**
    - 200: Job cancelled successfully
    - 404: Job not found
    - 409: Job cannot be cancelled (already completed/failed/cancelled)
    - 500: Server error
    
    **Example Response (Success):**
    ```json
    {
      "message": "Job cancelled successfully",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "project_id": "XXXX-XXX-A",
      "status": "cancelled",
      "previous_status": "processing",
      "cancelled_at": "2025-11-12T10:00:00Z"
    }
    ```
    
    **Example Response (Conflict):**
    ```json
    {
      "detail": "Cannot cancel completed job"
    }
    ```
    """
    logger.info(f"Cancellation requested for job: {job_id}")
    
    try:
        # Validate job exists
        job = DB.get_job(job_id)
        
        if not job:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail=f"Job with id {job_id} not found")
        
        # Check job status and reject cancellation for completed/failed/cancelled jobs
        if job.status == "completed":
            logger.warning(f"Cannot cancel completed job: {job_id}")
            raise HTTPException(status_code=409, detail="Cannot cancel completed job")
        
        if job.status == "failed":
            logger.warning(f"Cannot cancel failed job: {job_id}")
            raise HTTPException(status_code=409, detail="Cannot cancel failed job")
        
        if job.status == "cancelled":
            logger.warning(f"Job already cancelled: {job_id}")
            raise HTTPException(status_code=409, detail="Job already cancelled")
        
        # Store previous status for response
        previous_status = job.status
        
        # Call DB.cancel_job() for valid cancellations
        from datetime import datetime
        cancelled_at = datetime.utcnow()
        success = DB.cancel_job(job_id, cancelled_at)
        
        if not success:
            logger.error(f"Failed to cancel job: {job_id}")
            raise HTTPException(status_code=500, detail="Failed to cancel job")
        
        # Return success response with job details
        logger.info(f"Successfully cancelled job {job_id} (previous status: {previous_status})")
        return {
            "message": "Job cancelled successfully",
            "job_id": job_id,
            "project_id": job.project_id,
            "status": "cancelled",
            "previous_status": previous_status,
            "cancelled_at": cancelled_at.isoformat() + "Z"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to cancel job")
