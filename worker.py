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
    
    def _download_ortho_file(self, job_id: str) -> str:
        """
        Download ortho file from Azure to local temp directory.
        
        Downloads the file from Azure storage at jobs/{job_id}.tif and saves it
        to a local temporary directory for processing.
        
        Args:
            job_id: ID of the job whose file should be downloaded
            
        Returns:
            Local file path where the file was saved
            
        Raises:
            Exception: If download fails
        """
        import tempfile
        
        logger.info(f"Job {job_id}: Downloading ortho file from Azure")
        
        try:
            # Create temp directory for this job
            temp_dir = tempfile.mkdtemp(prefix=f"ortho_{job_id}_")
            local_file_path = os.path.join(temp_dir, f"{job_id}.tif")
            
            # Download from Azure
            blob_name = f"jobs/{job_id}.tif"
            self.db.az.download_file(blob_name, local_file_path)
            
            logger.info(f"Job {job_id}: Downloaded ortho file to {local_file_path}")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Job {job_id}: Failed to download ortho file: {e}", exc_info=True)
            raise Exception(f"Failed to download ortho file from Azure: {e}")
    
    def _validate_geotiff(self, file_path: str) -> None:
        """
        Validate that file is a readable GeoTIFF using gdalinfo.
        
        Runs the gdalinfo command on the file to verify it's a valid raster file.
        This ensures the file can be processed by GDAL tools before attempting
        conversion.
        
        Args:
            file_path: Path to the file to validate
            
        Raises:
            ValueError: If the file is not a valid GeoTIFF
        """
        import subprocess
        
        logger.info(f"Validating GeoTIFF file: {file_path}")
        
        try:
            result = subprocess.run(
                ['gdalinfo', file_path],
                capture_output=True,
                text=True,
                timeout=60  # 60 second timeout for validation
            )
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                logger.error(f"GeoTIFF validation failed: {error_msg}")
                raise ValueError(f"Invalid GeoTIFF file: {error_msg}")
            
            logger.info(f"GeoTIFF validation successful for {file_path}")
            logger.debug(f"gdalinfo output: {result.stdout[:500]}")  # Log first 500 chars
            
        except subprocess.TimeoutExpired:
            logger.error(f"GeoTIFF validation timed out for {file_path}")
            raise ValueError("GeoTIFF validation timed out")
        except FileNotFoundError:
            logger.error("gdalinfo command not found - GDAL may not be installed")
            raise ValueError("GDAL tools not available on system")
        except Exception as e:
            logger.error(f"Error during GeoTIFF validation: {e}", exc_info=True)
            raise ValueError(f"Failed to validate GeoTIFF: {e}")
    
    def _convert_to_cog(self, input_path: str, job_id: str) -> str:
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG) format.
        
        Uses gdal_translate with COG driver to create an optimized GeoTIFF with:
        - JPEG compression (quality 85)
        - Tiling enabled (512px blocks)
        - Optimized for streaming and partial reads
        
        Args:
            input_path: Path to the input GeoTIFF file
            job_id: Job ID for logging and progress tracking
            
        Returns:
            Path to the output COG file
            
        Raises:
            RuntimeError: If COG conversion fails
        """
        import subprocess
        
        logger.info(f"Job {job_id}: Starting COG conversion")
        logger.info(f"Job {job_id}: Converting to COG format with JPEG compression")
        
        # Update job progress
        self.db.update_job_status(
            job_id,
            "processing",
            progress_message="Converting to COG"
        )
        
        try:
            # Create output path in same directory as input
            # Handle both .tif and .tiff extensions (case-insensitive)
            if input_path.lower().endswith('.tiff'):
                output_path = input_path[:-5] + '_cog' + input_path[-5:]
            elif input_path.lower().endswith('.tif'):
                output_path = input_path[:-4] + '_cog' + input_path[-4:]
            else:
                # Fallback - should not happen due to validation
                output_path = input_path + '_cog.tif'
            
            # Run gdal_translate with COG driver
            result = subprocess.run([
                'gdal_translate',
                '-of', 'COG',
                '-co', 'COMPRESS=JPEG',
                '-co', 'QUALITY=85',
                '-co', 'TILED=YES',
                '-co', 'BLOCKSIZE=512',
                input_path,
                output_path
            ], capture_output=True, text=True, timeout=3600)  # 1 hour timeout
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                logger.error(f"Job {job_id}: COG conversion failed: {error_msg}")
                raise RuntimeError(f"COG conversion failed: {error_msg}")
            
            # Verify output file was created
            if not os.path.exists(output_path):
                logger.error(f"Job {job_id}: COG output file not created at {output_path}")
                raise RuntimeError("COG conversion failed: output file not created")
            
            logger.info(f"Job {job_id}: COG conversion completed successfully")
            logger.info(f"Job {job_id}: Output file: {output_path}")
            
            # Delete original uploaded file after successful conversion
            if os.path.exists(input_path):
                try:
                    os.remove(input_path)
                    logger.info(f"Job {job_id}: Deleted original uploaded file: {input_path}")
                except Exception as e:
                    logger.warning(f"Job {job_id}: Failed to delete original file {input_path}: {e}")
            
            return output_path
            
        except subprocess.TimeoutExpired:
            logger.error(f"Job {job_id}: COG conversion timed out")
            raise RuntimeError("COG conversion timed out after 1 hour")
        except FileNotFoundError:
            logger.error(f"Job {job_id}: gdal_translate command not found - GDAL may not be installed")
            raise RuntimeError("GDAL tools not available on system")
        except Exception as e:
            logger.error(f"Job {job_id}: Error during COG conversion: {e}", exc_info=True)
            raise RuntimeError(f"Failed to convert to COG: {e}")
    
    def _generate_ortho_thumbnail(self, cog_path: str, job_id: str) -> Optional[str]:
        """
        Generate thumbnail from COG file.
        
        Uses gdal_translate to create a PNG preview with 512px width and proportional height.
        This method is designed to be non-blocking - if thumbnail generation fails, it logs
        the error and returns None, allowing the main job to continue successfully.
        
        Args:
            cog_path: Path to the COG file
            job_id: Job ID for logging and progress tracking
            
        Returns:
            Path to the generated thumbnail PNG file, or None if generation fails
        """
        import subprocess
        
        logger.info(f"Job {job_id}: Starting thumbnail generation")
        
        # Update job progress
        self.db.update_job_status(
            job_id,
            "processing",
            progress_message="Generating thumbnail"
        )
        
        try:
            # Create thumbnail path in same directory as COG
            # Handle both .tif and .tiff extensions (case-insensitive)
            if cog_path.lower().endswith('.tiff'):
                thumbnail_path = cog_path[:-5] + '_thumbnail.png'
            elif cog_path.lower().endswith('.tif'):
                thumbnail_path = cog_path[:-4] + '_thumbnail.png'
            else:
                # Fallback - should not happen
                thumbnail_path = cog_path + '_thumbnail.png'
            
            # Run gdal_translate to create PNG thumbnail
            # -outsize 512 0 means 512px wide with proportional height
            result = subprocess.run([
                'gdal_translate',
                '-of', 'PNG',
                '-outsize', '512', '0',
                cog_path,
                thumbnail_path
            ], capture_output=True, text=True, timeout=30)  # 30 second timeout
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                logger.warning(f"Job {job_id}: Thumbnail generation failed: {error_msg}")
                return None
            
            # Verify output file was created
            if not os.path.exists(thumbnail_path):
                logger.warning(f"Job {job_id}: Thumbnail file not created at {thumbnail_path}")
                return None
            
            logger.info(f"Job {job_id}: Thumbnail generated successfully: {thumbnail_path}")
            return thumbnail_path
            
        except subprocess.TimeoutExpired:
            logger.warning(f"Job {job_id}: Thumbnail generation timed out after 30 seconds")
            return None
        except FileNotFoundError:
            logger.warning(f"Job {job_id}: gdal_translate command not found - GDAL may not be installed")
            return None
        except Exception as e:
            logger.warning(f"Job {job_id}: Error during thumbnail generation: {e}", exc_info=True)
            return None
    
    def _upload_ortho_to_azure(self, project_id: str, cog_path: str, thumbnail_path: Optional[str], job_id: str) -> dict:
        """
        Upload COG and thumbnail to Azure Blob Storage.
        
        Uploads the COG file to {project_id}/ortho/ortho.tif and optionally uploads
        the thumbnail to {project_id}/ortho/ortho_thumbnail.png. Generates SAS URLs
        with 30-day validity for both files.
        
        Args:
            project_id: Project ID for organizing files in Azure
            cog_path: Local path to the COG file
            thumbnail_path: Optional local path to the thumbnail PNG file
            job_id: Job ID for logging and progress tracking
            
        Returns:
            Dictionary with 'file' and 'thumbnail' keys containing SAS URLs
            
        Raises:
            Exception: If Azure upload fails
        """
        logger.info(f"Job {job_id}: Starting Azure upload for project {project_id}")
        
        # Update job progress
        self.db.update_job_status(
            job_id,
            "processing",
            progress_message="Uploading to Azure"
        )
        
        try:
            # Upload COG file
            cog_blob_name = f"{project_id}/ortho/ortho.tif"
            logger.info(f"Job {job_id}: Uploading COG to {cog_blob_name}")
            
            with open(cog_path, 'rb') as f:
                self.db.az.upload_bytes(
                    data=f.read(),
                    blob_name=cog_blob_name,
                    content_type="image/tiff",
                    overwrite=True
                )
            
            logger.info(f"Job {job_id}: COG uploaded successfully")
            
            # Generate SAS URL for COG (30 days = 720 hours)
            cog_url = self.db.az.generate_sas_url(cog_blob_name, hours_valid=720)
            
            # Upload thumbnail if it exists
            thumbnail_url = None
            if thumbnail_path and os.path.exists(thumbnail_path):
                thumbnail_blob_name = f"{project_id}/ortho/ortho_thumbnail.png"
                logger.info(f"Job {job_id}: Uploading thumbnail to {thumbnail_blob_name}")
                
                with open(thumbnail_path, 'rb') as f:
                    self.db.az.upload_bytes(
                        data=f.read(),
                        blob_name=thumbnail_blob_name,
                        content_type="image/png",
                        overwrite=True
                    )
                
                logger.info(f"Job {job_id}: Thumbnail uploaded successfully")
                
                # Generate SAS URL for thumbnail (30 days = 720 hours)
                thumbnail_url = self.db.az.generate_sas_url(thumbnail_blob_name, hours_valid=720)
            else:
                logger.info(f"Job {job_id}: No thumbnail to upload")
            
            result = {
                'file': cog_url,
                'thumbnail': thumbnail_url
            }
            
            logger.info(f"Job {job_id}: Azure upload completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Job {job_id}: Failed to upload ortho to Azure storage: {e}", exc_info=True)
            raise Exception(f"Failed to upload ortho to Azure storage: {e}")
    
    def _update_project_ortho(self, project_id: str, ortho_urls: dict, job_id: str) -> None:
        """
        Update project document with ortho URLs.
        
        Retrieves the project from the database, creates an Ortho object with the
        provided URLs, updates the project's ortho field, and saves it back to the
        database.
        
        Args:
            project_id: ID of the project to update
            ortho_urls: Dictionary with 'file' and 'thumbnail' keys containing SAS URLs
            job_id: Job ID for logging
            
        Raises:
            ValueError: If project is not found
            Exception: If database update fails
        """
        logger.info(f"Job {job_id}: Updating project {project_id} with ortho URLs")
        
        try:
            # Get project from database
            project = self.db.getProject({'_id': project_id})
            if not project:
                logger.error(f"Job {job_id}: Project {project_id} not found")
                raise ValueError(f"Project {project_id} not found")
            
            # Create Ortho object with URLs
            from models.Project import Ortho
            project.ortho = Ortho(
                file=ortho_urls['file'],
                thumbnail=ortho_urls.get('thumbnail')  # Use .get() since thumbnail is optional
            )
            
            # Update project in database
            self.db.updateProject(project)
            
            logger.info(f"Job {job_id}: Successfully updated project {project_id} with ortho URLs")
            logger.info(f"Job {job_id}: Ortho file URL: {ortho_urls['file']}")
            if ortho_urls.get('thumbnail'):
                logger.info(f"Job {job_id}: Ortho thumbnail URL: {ortho_urls['thumbnail']}")
            else:
                logger.info(f"Job {job_id}: No thumbnail URL (thumbnail generation may have failed)")
            
        except ValueError:
            # Re-raise ValueError for project not found
            raise
        except Exception as e:
            logger.error(f"Job {job_id}: Failed to update project {project_id} with ortho URLs: {e}", exc_info=True)
            raise Exception(f"Failed to update project with ortho URLs: {e}")
    
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
    
    def _cleanup_ortho_files(self, job_id: str, *file_paths):
        """
        Clean up local temporary files and Azure job file for ortho processing.
        
        This method accepts a variable number of file paths and attempts to delete
        each one. It also deletes the Azure job file. All operations are wrapped
        in error handling to ensure failures don't prevent other cleanup operations.
        
        Args:
            job_id: Job ID for the Azure job file cleanup
            *file_paths: Variable number of local file paths to delete
        """
        logger.info(f"Job {job_id}: Starting ortho file cleanup")
        
        # Delete local files
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Job {job_id}: Deleted local file: {file_path}")
                except Exception as e:
                    logger.error(f"Job {job_id}: Failed to delete {file_path}: {e}")
        
        # Delete Azure job file
        try:
            self.db.az.delete_job_file(job_id)
            logger.info(f"Job {job_id}: Deleted Azure job file")
        except Exception as e:
            logger.error(f"Job {job_id}: Failed to delete Azure job file: {e}")
        
        logger.info(f"Job {job_id}: Ortho file cleanup completed")
    
    def _handle_ortho_cancellation(self, job: Job):
        """
        Handle cancellation of ortho job.
        
        This method:
        1. Deletes local temp directory
        2. Deletes Azure job file
        3. Updates job status to 'cancelled' with completed_at timestamp
        4. Sets progress_message to "Job cancelled by user"
        
        Args:
            job: Job object that was cancelled
        """
        logger.info(f"Job {job.id}: Handling ortho job cancellation")
        
        # Delete local temp directory
        temp_dir = f"/tmp/ortho_{job.id}_*"
        import glob
        for dir_path in glob.glob(temp_dir):
            if os.path.exists(dir_path):
                try:
                    shutil.rmtree(dir_path, ignore_errors=True)
                    logger.info(f"Job {job.id}: Deleted temp directory: {dir_path}")
                except Exception as e:
                    logger.error(f"Job {job.id}: Failed to delete temp directory {dir_path}: {e}")
        
        # Delete Azure job file
        try:
            self.db.az.delete_job_file(job.id)
            logger.info(f"Job {job.id}: Deleted Azure job file")
        except Exception as e:
            logger.error(f"Job {job.id}: Failed to delete Azure job file: {e}")
        
        # Update job status to cancelled
        try:
            self.db.update_job_status(
                job.id,
                "cancelled",
                progress_message="Job cancelled by user"
            )
            logger.info(f"Job {job.id}: Status updated to cancelled")
        except Exception as e:
            logger.error(f"Job {job.id}: Failed to update status to cancelled: {e}")
        
        logger.info(f"Job {job.id}: Ortho cancellation handling completed")
    
    def _handle_ortho_error(self, job: Job, error: Exception):
        """
        Handle error during ortho job processing.
        
        This method:
        1. Logs error with full stack trace
        2. Deletes local temp files
        3. Deletes Azure job file
        4. Updates job status to 'failed' with error message
        
        Args:
            job: Job object that failed
            error: Exception that caused the failure
        """
        logger.error(f"Job {job.id}: Ortho processing error: {error}", exc_info=True)
        
        # Delete local temp directory
        temp_dir = f"/tmp/ortho_{job.id}_*"
        import glob
        for dir_path in glob.glob(temp_dir):
            if os.path.exists(dir_path):
                try:
                    shutil.rmtree(dir_path, ignore_errors=True)
                    logger.info(f"Job {job.id}: Deleted temp directory: {dir_path}")
                except Exception as e:
                    logger.error(f"Job {job.id}: Failed to delete temp directory {dir_path}: {e}")
        
        # Delete Azure job file
        try:
            self.db.az.delete_job_file(job.id)
            logger.info(f"Job {job.id}: Deleted Azure job file")
        except Exception as e:
            logger.error(f"Job {job.id}: Failed to delete Azure job file: {e}")
        
        # Update job status to failed
        try:
            error_message = str(error)
            self.db.update_job_status(
                job.id,
                "failed",
                error_message=error_message,
                progress_message="Ortho processing failed"
            )
            logger.info(f"Job {job.id}: Status updated to failed with error: {error_message}")
        except Exception as e:
            logger.error(f"Job {job.id}: Failed to update status to failed: {e}")
        
        logger.info(f"Job {job.id}: Ortho error handling completed")
    
    def process_ortho_job(self, job: Job):
        """
        Process an ortho conversion job through the complete pipeline.
        
        Steps:
        1. Update status to "processing"
        2. Download ortho file from Azure
        3. Check cancellation
        4. Validate GeoTIFF with gdalinfo
        5. Check cancellation
        6. Convert to COG format
        7. Check cancellation
        8. Generate thumbnail (optional)
        9. Check cancellation
        10. Upload COG and thumbnail to Azure
        11. Update project with ortho URLs
        12. Mark job as completed
        13. Cleanup temporary files
        
        The method checks for cancellation before each major step and handles
        CancellationException by cleaning up resources and updating job status.
        
        Args:
            job: Job object to process (must have type "ortho_conversion")
        """
        local_file = None
        cog_file = None
        thumbnail_file = None
        
        try:
            logger.info(f"Job {job.id}: Starting ortho processing for project {job.project_id}")
            
            # Update status to processing
            self.db.update_job_status(
                job.id,
                "processing",
                progress_message="Starting ortho processing"
            )
            
            # Step 1: Download ortho file from Azure
            logger.info(f"Job {job.id}: Downloading ortho file")
            local_file = self._download_ortho_file(job.id)
            
            # Check cancellation
            self._check_cancellation(job.id)
            
            # Step 2: Validate GeoTIFF
            logger.info(f"Job {job.id}: Validating GeoTIFF")
            self.db.update_job_status(
                job.id,
                "processing",
                progress_message="Validating file"
            )
            self._validate_geotiff(local_file)
            
            # Check cancellation
            self._check_cancellation(job.id)
            
            # Step 3: Convert to COG
            logger.info(f"Job {job.id}: Converting to COG")
            cog_file = self._convert_to_cog(local_file, job.id)
            
            # Check cancellation
            self._check_cancellation(job.id)
            
            # Step 4: Generate thumbnail (optional)
            logger.info(f"Job {job.id}: Generating thumbnail")
            thumbnail_file = self._generate_ortho_thumbnail(cog_file, job.id)
            
            # Check cancellation
            self._check_cancellation(job.id)
            
            # Step 5: Upload to Azure
            logger.info(f"Job {job.id}: Uploading to Azure")
            ortho_urls = self._upload_ortho_to_azure(job.project_id, cog_file, thumbnail_file, job.id)
            
            # Step 6: Update project with ortho URLs
            logger.info(f"Job {job.id}: Updating project")
            self._update_project_ortho(job.project_id, ortho_urls, job.id)
            
            # Step 7: Mark job as completed
            self.db.update_job_status(
                job.id,
                "completed",
                progress_message="Ortho conversion completed successfully"
            )
            
            logger.info(f"Job {job.id}: Ortho processing completed successfully")
            
            # Step 8: Cleanup temporary files
            self._cleanup_ortho_files(job.id, local_file, cog_file, thumbnail_file)
            
        except CancellationException as e:
            # Handle job cancellation
            logger.info(f"Job {job.id}: Ortho cancellation exception caught: {e}")
            self._handle_ortho_cancellation(job)
            
            # Cleanup files
            self._cleanup_ortho_files(job.id, local_file, cog_file, thumbnail_file)
            
        except Exception as e:
            # Handle general errors
            logger.error(f"Job {job.id}: Ortho processing failed: {e}", exc_info=True)
            self._handle_ortho_error(job, e)
            
            # Cleanup files
            self._cleanup_ortho_files(job.id, local_file, cog_file, thumbnail_file)
    
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
        
        Routes the job to the appropriate handler based on job type:
        - "ortho_conversion": Routes to process_ortho_job()
        - Other types: Routes to point cloud processing pipeline
        
        Point Cloud Processing Steps:
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
        # Route to appropriate handler based on job type
        if job.type == "ortho_conversion":
            logger.info(f"Job {job.id}: Routing to ortho processing")
            self.process_ortho_job(job)
            return
        
        # Default: Point cloud processing
        logger.info(f"Job {job.id}: Routing to point cloud processing")
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
