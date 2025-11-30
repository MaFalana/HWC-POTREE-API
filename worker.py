"""
Background worker for processing point cloud jobs asynchronously.

This module implements a JobWorker class that polls MongoDB for pending jobs
and processes them in a background thread.
"""

import os
import shutil
import time
import logging
from datetime import datetime
from typing import Optional
from storage.db import DatabaseManager
from models.Job import Job
from models.Project import Project, CRS, Location
from utils.main import CloudMetadata
from utils.thumbnail import ThumbnailGenerator
from utils.potree import PotreeConverter

# Configure logging
logger = logging.getLogger(__name__)


class CancellationException(Exception):
    """
    Custom exception raised when a job cancellation is detected.
    
    This exception is used to signal that a job has been cancelled
    and processing should stop immediately.
    """
    pass


class JobWorker:
    """
    Background worker that processes point cloud conversion jobs.
    
    The worker runs in a separate thread and continuously polls MongoDB
    for pending jobs. When a job is found, it processes it through the
    complete pipeline: metadata extraction, thumbnail generation, Potree
    conversion, and file upload.
    """
    
    def __init__(self, db: DatabaseManager, poll_interval: int = 5, cleanup_interval_hours: int = 1):
        """
        Initialize the JobWorker.
        
        Args:
            db: DatabaseManager instance for database operations
            poll_interval: Time in seconds between polling attempts (default: 5)
            cleanup_interval_hours: Hours between job cleanup runs (default: 1)
        """
        self.db = db
        self.poll_interval = poll_interval
        self.cleanup_interval_hours = cleanup_interval_hours
        self.last_cleanup_time = None
        self.running = False
        logger.info(f"JobWorker initialized with poll interval: {poll_interval}s, cleanup interval: {cleanup_interval_hours}h")
    
    def start(self):
        """
        Start the worker's main processing loop.
        
        This method runs continuously, polling for pending jobs and processing
        them one at a time. The loop continues until stop() is called.
        """
        self.running = True
        logger.info("JobWorker started")
        
        while self.running:
            try:
                # Check if it's time to run cleanup
                self._check_and_run_cleanup()
                
                # Get the next pending job
                job = self.get_next_job()
                
                if job:
                    logger.info(f"Found pending job: {job.id}")
                    self.process_job(job)
                    
                    # Run cleanup after each job as well
                    self._check_and_run_cleanup(force=True)
                else:
                    # No jobs available, wait before polling again
                    time.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                # Continue running even if an error occurs
                time.sleep(self.poll_interval)
        
        logger.info("JobWorker stopped")
    
    def stop(self):
        """
        Stop the worker's processing loop.
        
        This sets the running flag to False, which will cause the main loop
        to exit after completing the current iteration.
        """
        logger.info("Stopping JobWorker...")
        self.running = False
    
    def _check_and_run_cleanup(self, force: bool = False):
        """
        Check if it's time to run job cleanup and execute if needed.
        
        Cleanup runs:
        - Every hour (based on cleanup_interval_hours)
        - After each job if force=True
        - On first run (when last_cleanup_time is None)
        
        Args:
            force: If True, check if cleanup should run regardless of time interval
        """
        current_time = datetime.utcnow()
        
        # Determine if we should run cleanup
        should_cleanup = False
        
        if self.last_cleanup_time is None:
            # First run - always cleanup
            should_cleanup = True
            logger.info("Running initial job cleanup")
        elif force:
            # After job completion - check if enough time has passed
            time_since_cleanup = (current_time - self.last_cleanup_time).total_seconds() / 3600
            if time_since_cleanup >= self.cleanup_interval_hours:
                should_cleanup = True
                logger.info(f"Running job cleanup after job completion ({time_since_cleanup:.1f}h since last cleanup)")
        else:
            # Regular check - run if interval has passed
            time_since_cleanup = (current_time - self.last_cleanup_time).total_seconds() / 3600
            if time_since_cleanup >= self.cleanup_interval_hours:
                should_cleanup = True
                logger.info(f"Running scheduled job cleanup ({time_since_cleanup:.1f}h since last cleanup)")
        
        if should_cleanup:
            try:
                deleted_count = self.db.cleanup_old_jobs(hours=72)
                self.last_cleanup_time = current_time
                logger.info(f"Job cleanup completed: {deleted_count} old jobs deleted")
            except Exception as e:
                logger.error(f"Error during job cleanup: {e}", exc_info=True)
    
    def get_next_job(self) -> Optional[Job]:
        """
        Poll MongoDB for the next pending job and mark it as processing.
        
        This method uses find_one_and_update to atomically find a pending job
        and update its status to "processing". This prevents race conditions
        if multiple workers are running.
        
        Returns:
            Job object if a pending job was found, None otherwise
        """
        try:
            # Find the oldest pending job and atomically update it to processing
            result = self.db.jobsCollection.find_one_and_update(
                {"status": "pending"},
                {
                    "$set": {
                        "status": "processing",
                        "updated_at": datetime.utcnow()
                    }
                },
                sort=[("created_at", 1)],  # Get oldest job first (FIFO)
                return_document=True  # Return the updated document
            )
            
            if result:
                job = Job(**result)
                logger.info(f"Acquired job {job.id} for processing")
                return job
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting next job: {e}", exc_info=True)
            return None
    
    def _check_cancellation(self, job_id: str):
        """
        Check if a job has been cancelled and raise CancellationException if so.
        
        This method performs a lightweight database query to check only the
        cancelled field. If the job is cancelled, it raises CancellationException
        to stop processing immediately.
        
        Args:
            job_id: ID of the job to check
            
        Raises:
            CancellationException: If the job has been cancelled
        """
        try:
            if self.db.is_job_cancelled(job_id):
                logger.info(f"Job {job_id}: Cancellation detected")
                raise CancellationException(f"Job {job_id} was cancelled")
        except CancellationException:
            # Re-raise CancellationException
            raise
        except Exception as e:
            # Log other errors but don't stop processing
            logger.warning(f"Error checking cancellation for job {job_id}: {e}")
    
    def _cleanup_cancelled_job(self, job: Job, output_dir: Optional[str] = None):
        """
        Clean up resources for a cancelled job.
        
        This method is idempotent and handles errors gracefully. It attempts to:
        1. Delete the local temporary file
        2. Delete the Azure job file
        3. Delete any partial Potree output files from Azure
        
        Each cleanup operation is wrapped in error handling to ensure that
        failures in one operation don't prevent other cleanup operations.
        
        Args:
            job: Job object containing file paths to clean up
            output_dir: Optional local Potree output directory to clean up
        """
        logger.info(f"Job {job.id}: Starting cleanup for cancelled job")
        
        # Delete local temp file
        if job.file_path and os.path.exists(job.file_path):
            try:
                os.remove(job.file_path)
                logger.info(f"Job {job.id}: Deleted local temp file: {job.file_path}")
            except Exception as e:
                logger.error(f"Job {job.id}: Failed to delete local temp file {job.file_path}: {e}")
        
        # Delete Azure job file
        if job.azure_path:
            try:
                self.db.az.delete_blob(job.azure_path)
                logger.info(f"Job {job.id}: Deleted Azure job file: {job.azure_path}")
            except Exception as e:
                logger.error(f"Job {job.id}: Failed to delete Azure job file {job.azure_path}: {e}")
        
        # Delete partial Potree output files from Azure
        try:
            # Get the project to determine the blob prefix
            project = self.db.getProject({'_id': job.project_id})
            if project:
                blob_prefix = f"{project.id}/"
                # List and delete all blobs with this prefix
                deleted_count = 0
                try:
                    blobs = self.db.az.list_blobs(prefix=blob_prefix)
                    for blob in blobs:
                        try:
                            self.db.az.delete_blob(blob.name)
                            deleted_count += 1
                        except Exception as e:
                            logger.error(f"Job {job.id}: Failed to delete blob {blob.name}: {e}")
                    
                    if deleted_count > 0:
                        logger.info(f"Job {job.id}: Deleted {deleted_count} partial Potree output files from Azure")
                except Exception as e:
                    logger.error(f"Job {job.id}: Failed to list blobs for cleanup: {e}")
        except Exception as e:
            logger.error(f"Job {job.id}: Failed to cleanup Potree output files: {e}")
        
        # Delete local Potree output directory if provided
        if output_dir and os.path.exists(output_dir):
            try:
                shutil.rmtree(output_dir)
                logger.info(f"Job {job.id}: Deleted local Potree output directory: {output_dir}")
            except Exception as e:
                logger.error(f"Job {job.id}: Failed to delete output directory {output_dir}: {e}")
        
        logger.info(f"Job {job.id}: Cleanup completed for cancelled job")
    
    def _handle_cancellation(self, job: Job, output_dir: Optional[str] = None):
        """
        Handle a cancelled job by cleaning up resources and updating status.
        
        This method:
        1. Calls _cleanup_cancelled_job() to remove temporary files
        2. Updates the job status to 'cancelled' with completed_at timestamp
        3. Sets progress_message to "Job cancelled by user"
        
        Args:
            job: Job object that was cancelled
            output_dir: Optional local Potree output directory to clean up
        """
        logger.info(f"Job {job.id}: Handling cancellation")
        
        # Clean up resources
        self._cleanup_cancelled_job(job, output_dir)
        
        # Update job status to cancelled with completed_at timestamp
        try:
            self.db.update_job_status(
                job.id,
                "cancelled",
                progress_message="Job cancelled by user"
            )
            logger.info(f"Job {job.id}: Status updated to cancelled")
        except Exception as e:
            logger.error(f"Job {job.id}: Failed to update status to cancelled: {e}")
        
        logger.info(f"Job {job.id}: Cancellation handling completed")
    
    def process_job(self, job: Job):
        """
        Process a job through the complete pipeline.
        
        Steps:
        1. Extract metadata (CRS, location, point count)
        2. Generate thumbnail
        3. Upload thumbnail to Azure
        4. Update project with metadata and thumbnail URL
        5. Run PotreeConverter
        6. Upload Potree output to Azure
        7. Update project with cloud URL
        8. Mark job as completed
        9. Cleanup temp files
        
        The method checks for cancellation before each major step and handles
        CancellationException by cleaning up resources and updating job status.
        
        Args:
            job: Job object to process
        """
        output_dir = None
        
        try:
            logger.info(f"Starting processing for job {job.id}")
            
            # Get the project
            project = self.db.getProject({'_id': job.project_id})
            if not project:
                raise ValueError(f"Project {job.project_id} not found")
            
            # Check for cancellation before metadata extraction
            self._check_cancellation(job.id)
            
            # Step 1: Extract metadata
            logger.info(f"Job {job.id}: Extracting metadata")
            self.db.update_job_status(
                job.id,
                "processing",
                current_step="metadata",
                progress_message="Extracting point cloud metadata..."
            )
            
            # Use project's CRS for coordinate transformation
            # Format as EPSG:XXXX for CloudMetadata
            crs_epsg = f"EPSG:{project.crs.id}" if project.crs and project.crs.id else None
            metadata_extractor = CloudMetadata(job.file_path, crs_epsg=crs_epsg)
            metadata = metadata_extractor.summary()
            
            # Update project with metadata
            # Note: CRS is now provided by user during project creation, so we don't overwrite it
            # The user-provided CRS is used for Potree conversion
            
            if metadata.get('center'):
                center = metadata['center']
                # Handle None values from metadata extraction
                lat = center.get('lat') if center.get('lat') is not None else 0.0
                lon = center.get('lon') if center.get('lon') is not None else 0.0
                z = center.get('z') if center.get('z') is not None else 0.0
                
                project.location = Location(
                    lat=lat,
                    lon=lon,
                    z=z
                )
            
            if metadata.get('points'):
                project.point_count = metadata['points']
            
            logger.info(f"Job {job.id}: Metadata extracted - {metadata['points']} points, CRS: {metadata.get('crs')}")
            
            # Check for cancellation before thumbnail generation
            self._check_cancellation(job.id)
            
            # Step 2: Generate thumbnail
            logger.info(f"Job {job.id}: Generating thumbnail")
            self.db.update_job_status(
                job.id,
                "processing",
                current_step="thumbnail",
                progress_message="Generating thumbnail..."
            )
            
            try:
                thumbnail_generator = ThumbnailGenerator(size=512)
                thumbnail_bytes = thumbnail_generator.generate_from_las(job.file_path)
                
                # Step 3: Upload thumbnail to Azure
                logger.info(f"Job {job.id}: Uploading thumbnail to Azure")
                thumbnail_blob_name = f"{project.id}/thumbnail.png"
                self.db.az.upload_bytes(
                    thumbnail_bytes,
                    thumbnail_blob_name,
                    content_type="image/png",
                    overwrite=True
                )
                
                # Generate SAS URL for thumbnail (30 days validity)
                thumbnail_url = self.db.az.generate_sas_url(thumbnail_blob_name, hours_valid=720)
                project.thumbnail = thumbnail_url
                
                logger.info(f"Job {job.id}: Thumbnail uploaded successfully")
                
            except Exception as e:
                logger.warning(f"Job {job.id}: Thumbnail generation failed: {e}", exc_info=True)
                # Continue processing even if thumbnail fails
            
            # Step 4: Update project with metadata and thumbnail
            self.db.updateProject(project)
            logger.info(f"Job {job.id}: Project updated with metadata and thumbnail")
            
            # Check for cancellation before Potree conversion
            self._check_cancellation(job.id)
            
            # Step 5: Run PotreeConverter
            logger.info(f"Job {job.id}: Starting Potree conversion")
            self.db.update_job_status(
                job.id,
                "processing",
                current_step="conversion",
                progress_message="Converting to Potree format..."
            )
            
            converter = PotreeConverter()
            
            # Create temporary output directory for Potree conversion
            import tempfile
            output_dir = tempfile.mkdtemp(prefix=f"potree_{job.id}_")
            
            # Convert the point cloud file
            converter.convert(job.file_path, output_dir, project)
            
            logger.info(f"Job {job.id}: Potree conversion completed")
            
            # Check for cancellation before Azure upload
            self._check_cancellation(job.id)
            
            # Step 6: Upload Potree output to Azure
            logger.info(f"Job {job.id}: Uploading Potree files to Azure")
            self.db.update_job_status(
                job.id,
                "processing",
                current_step="upload",
                progress_message="Uploading Potree files to Azure..."
            )
            
            # Upload all files from output directory
            cloud_url = self._upload_potree_output(output_dir, project.id)
            
            # Step 7: Update project with cloud URL
            project.cloud = cloud_url
            self.db.updateProject(project)
            
            logger.info(f"Job {job.id}: Potree files uploaded, project updated with cloud URL")
            
            # Step 8: Mark job as completed
            self.db.update_job_status(
                job.id,
                "completed",
                current_step="completed",
                progress_message="Processing completed successfully"
            )
            
            logger.info(f"Job {job.id}: Processing completed successfully")
            
            # Step 9: Cleanup temp files and output directory
            self.cleanup_temp_files(job)
            
            # Clean up Potree output directory
            if output_dir and os.path.exists(output_dir):
                try:
                    shutil.rmtree(output_dir)
                    logger.info(f"Deleted Potree output directory: {output_dir}")
                except Exception as e:
                    logger.warning(f"Failed to delete output directory {output_dir}: {e}")
        
        except CancellationException as e:
            # Handle job cancellation
            logger.info(f"Job {job.id}: Cancellation exception caught: {e}")
            self._handle_cancellation(job, output_dir)
            
        except Exception as e:
            logger.error(f"Job {job.id} failed: {e}", exc_info=True)
            self.mark_failed(job, str(e))
            
            # Cleanup even on failure
            self.cleanup_temp_files(job)
            
            # Clean up output directory if it exists
            try:
                if output_dir and os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                    logger.info(f"Deleted Potree output directory after failure: {output_dir}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to delete output directory after failure: {cleanup_error}")
    
    def _upload_potree_output(self, output_dir: str, project_id: str) -> str:
        """
        Upload Potree output directory to Azure Blob Storage.
        
        Args:
            output_dir: Local directory containing Potree output files
            project_id: Project ID for organizing files in Azure
            
        Returns:
            SAS URL for the main viewer HTML file
        """
        logger.info(f"Uploading Potree output from {output_dir} to Azure")
        
        # Walk through output directory and upload all files
        for root, _, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # Create blob path maintaining directory structure
                rel_path = os.path.relpath(file_path, output_dir)
                blob_name = f"{project_id}/{rel_path}".replace('\\', '/')
                
                # Determine content type
                ext = os.path.splitext(file)[1].lower()
                content_type_map = {
                    '.html': 'text/html',
                    '.js': 'application/javascript',
                    '.json': 'application/json',
                    '.bin': 'application/octet-stream',
                    '.css': 'text/css',
                    '.png': 'image/png',
                    '.jpg': 'image/jpeg',
                }
                content_type = content_type_map.get(ext, 'application/octet-stream')
                
                # Upload file
                with open(file_path, 'rb') as f:
                    self.db.az.upload_bytes(
                        f.read(),
                        blob_name,
                        content_type=content_type,
                        overwrite=True
                    )
                
                logger.debug(f"Uploaded {blob_name}")
        
        # Generate SAS URL for the main viewer file
        # Potree typically creates a metadata.json or viewer.html (30 days validity)
        viewer_blob = f"{project_id}/metadata.json"
        sas_url = self.db.az.generate_sas_url(viewer_blob, hours_valid=720)
        
        logger.info(f"Potree output uploaded successfully, viewer URL: {sas_url}")
        return sas_url
    
    def mark_failed(self, job: Job, error_message: str):
        """
        Mark a job as failed and store the error message.
        
        Args:
            job: Job object that failed
            error_message: Error message describing the failure
        """
        logger.error(f"Marking job {job.id} as failed: {error_message}")
        
        self.db.update_job_status(
            job.id,
            "failed",
            error_message=error_message,
            progress_message="Processing failed"
        )
        
        logger.info(f"Job {job.id} marked as failed")
    
    def cleanup_temp_files(self, job: Job):
        """
        Clean up temporary files after job processing.
        
        This includes:
        - Local temporary file (job.file_path)
        - Azure job file (job.azure_path)
        
        Args:
            job: Job object containing file paths to clean up
        """
        logger.info(f"Cleaning up temporary files for job {job.id}")
        
        # Delete local temp file
        if job.file_path and os.path.exists(job.file_path):
            try:
                os.remove(job.file_path)
                logger.info(f"Deleted local temp file: {job.file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete local temp file {job.file_path}: {e}")
        
        # Delete Azure job file
        if job.azure_path:
            try:
                self.db.az.delete_blob(job.azure_path)
                logger.info(f"Deleted Azure job file: {job.azure_path}")
            except Exception as e:
                logger.warning(f"Failed to delete Azure job file {job.azure_path}: {e}")
        
        logger.info(f"Cleanup completed for job {job.id}")
