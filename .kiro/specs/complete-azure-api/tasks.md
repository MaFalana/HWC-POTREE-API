# Implementation Plan

- [x] 1. Fix existing code issues and setup

  - Fix missing imports and parameter issues in existing routes
  - Set up project structure for new modules
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 1.1 Fix projects router issues

  - Add `import json` to `routes/projects.py` for tag parsing
  - Fix `update_project` endpoint to accept `id` parameter from path
  - _Requirements: 6.2, 6.1_

- [x] 1.2 Update PotreeConverter to use environment variable

  - Modify `utils/potree.py` to read `POTREE_PATH` from environment
  - Remove hardcoded path `/Users/malik/Documents/...`
  - _Requirements: 6.3_

- [x] 1.3 Create bin directory and setup PotreeConverter

  - Create `bin/` folder in project root
  - Copy Linux PotreeConverter binary to `bin/PotreeConverter`
  - Update `.gitignore` to track the binary
  - _Requirements: 11.4_

- [x] 2. Create Job data model and database methods

  - Implement Job model with all required fields
  - Add job management methods to DatabaseManager
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

- [x] 2.1 Create Job model

  - Create `models/Job.py` with Job and JobResponse models
  - Include fields: id, project_id, status, file_path, azure_path, current_step, progress_message, error_message, timestamps
  - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [x] 2.2 Add job methods to DatabaseManager

  - Implement `create_job()` method
  - Implement `get_job()` method
  - Implement `update_job_status()` method
  - Implement `get_jobs_by_project()` method
  - Implement `cleanup_old_jobs()` method
  - Add jobs collection initialization in `__init__`
  - _Requirements: 9.5, 9.6, 8.7_

- [x] 3. Implement thumbnail generation utility

  - Create thumbnail generator using PDAL and PIL
  - Handle RGB colors and transparent backgrounds
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.7_

- [x] 3.1 Create ThumbnailGenerator class

  - Create `utils/thumbnail.py` module
  - Implement `generate_from_las()` method using PDAL
  - Read point cloud and extract XY coordinates
  - Extract RGB colors if available
  - _Requirements: 7.1, 7.2_

- [x] 3.2 Implement density map rendering

  - Create 2D grid and bin points by XY coordinates
  - Calculate density per bin
  - Average RGB colors per bin if available
  - Create PIL Image with RGBA mode (transparent background)
  - Maintain aspect ratio with square preference
  - Return PNG bytes
  - _Requirements: 7.3, 7.4_

- [x] 4. Enhance Azure Storage Manager

  - Add methods for thumbnail upload and folder upload with MIME types
  - Add methods for file deletion
  - _Requirements: 6.4, 7.5, 10.1, 10.2, 10.5, 10.6_

- [x] 4.1 Add thumbnail upload method

  - Implement `upload_thumbnail()` method
  - Upload PNG bytes to `{project_id}/thumbnail.png`
  - Set correct content type (image/png)
  - Generate and return SAS URL
  - _Requirements: 7.5, 7.6_

- [x] 4.2 Improve folder upload with MIME types

  - Update `upload_folder()` to use `_guess_content_type()` for all files
  - Ensure proper content types for .html, .js, .json, .bin files
  - Maintain folder structure in blob paths
  - _Requirements: 6.4_

- [x] 4.3 Add file deletion methods

  - Implement `delete_project_files()` to delete all blobs with prefix `{project_id}/`
  - Implement `delete_job_file()` to delete `jobs/{job_id}.laz`
  - _Requirements: 10.1, 10.2, 10.6_

- [x] 5. Update PotreeConverter for Azure integration

  - Modify converter to upload output to Azure
  - Return SAS URL for viewer
  - _Requirements: 2.3, 2.4, 2.5, 2.6, 6.5_

- [x] 5.1 Refactor convert method

  - Update `convert()` to accept output directory parameter
  - Use environment variable for PotreeConverter path
  - Capture and log conversion output
  - Handle conversion errors properly
  - _Requirements: 2.2, 2.7_

- [x] 5.2 Add upload_output method

  - Implement `upload_output()` method
  - Upload all files from output directory to Azure
  - Organize as `{project_id}/` prefix
  - Set correct MIME types for all files
  - Generate SAS URL for main viewer file
  - Return SAS URL
  - _Requirements: 2.3, 2.4, 2.5, 6.5_

- [x] 6. Implement background worker system

  - Create worker module with job processing logic
  - Integrate worker startup with FastAPI
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.8_

- [x] 6.1 Create JobWorker class

  - Create `worker.py` module
  - Implement `JobWorker` class with `__init__`, `start()`, `stop()` methods
  - Implement `get_next_job()` to poll MongoDB
  - Implement polling loop with configurable interval
  - _Requirements: 8.1, 8.2, 8.8_

- [x] 6.2 Implement job processing logic

  - Implement `process_job()` method with try/except
  - Update job status to "processing"
  - Call metadata extraction (CloudMetadata)
  - Call thumbnail generation (ThumbnailGenerator)
  - Upload thumbnail to Azure
  - Update project with metadata and thumbnail URL
  - Call PotreeConverter
  - Upload Potree output to Azure
  - Update project with cloud URL
  - Mark job as completed
  - _Requirements: 8.3, 8.4, 8.5_

- [x] 6.3 Implement error handling and cleanup

  - Implement `mark_failed()` method to update job with error
  - Implement `cleanup_temp_files()` method
  - Delete local temp files after processing
  - Delete Azure job file after successful conversion
  - Call cleanup in both success and failure cases
  - _Requirements: 8.6, 10.1, 10.2, 10.3_

- [x] 6.4 Integrate worker with FastAPI startup

  - Add startup event handler in `main.py`
  - Reset stale "processing" jobs to "pending" on startup
  - Start worker thread as daemon
  - _Requirements: 8.1, 8.8_

- [x] 7. Update process router for combined endpoint

  - Refactor to single endpoint that creates jobs
  - Remove synchronous processing
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 6.6_

- [x] 7.1 Refactor process endpoint

  - Rename `/process/{id}/potree` or keep as is
  - Accept file upload and optional EPSG parameter
  - Generate unique job_id (UUID)
  - Save uploaded file to temp location
  - Upload file to Azure `jobs/{job_id}.laz`
  - Create job record in MongoDB with status="pending"
  - Return job_id and status immediately
  - Remove old `/process/{id}` endpoint
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 6.6_

- [x] 8. Implement jobs router endpoints

  - Create endpoints for job status retrieval
  - _Requirements: 1.5, 1.6_

- [x] 8.1 Implement GET /jobs/{job_id}

  - Get job by ID from database
  - Return 404 if not found
  - Return job status, progress, timestamps
  - _Requirements: 1.4, 1.5_

- [x] 8.2 Implement GET /jobs/project/{project_id}

  - Get all jobs for a project
  - Return list of jobs with status
  - _Requirements: 1.4_

- [x] 8.3 Register jobs router in main.py

  - Import jobs_router in `main.py`
  - Add `app.include_router(jobs_router)`
  - _Requirements: 1.6_

- [x] 9. Add health check endpoint

  - Implement health check for Azure monitoring
  - _Requirements: 5.3_

- [x] 9.1 Create health check endpoint

  - Add GET `/health` endpoint in `main.py`
  - Check MongoDB connection
  - Check Azure Blob Storage connection
  - Return status and service health
  - Return 503 if unhealthy
  - _Requirements: 5.3_

- [x] 10. Implement error handling and logging

  - Add global exception handlers
  - Configure logging for Azure
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 5.4_

- [x] 10.1 Add global exception handlers

  - Add ValidationError handler (400 response)
  - Add generic Exception handler (500 response)
  - Log all errors with full stack traces
  - _Requirements: 4.1, 4.3_

- [x] 10.2 Configure logging

  - Set up logging in `main.py` with INFO level
  - Configure format for structured logging
  - Use StreamHandler for stdout (Azure captures this)
  - Add logging to worker, routes, and utilities
  - _Requirements: 5.4_

- [x] 10.3 Add error handling to routes

  - Add try/except blocks in project routes
  - Return 404 for missing projects
  - Return 400 for validation errors
  - Handle Azure and MongoDB exceptions gracefully
  - _Requirements: 4.2, 4.4, 4.5, 4.6_

- [x] 11. Update Dockerfile and deployment config

  - Enhance Dockerfile with health check
  - Ensure PotreeConverter is included
  - _Requirements: 5.1, 5.2, 5.5, 5.6, 11.4_

- [x] 11.1 Update Dockerfile

  - Ensure `COPY bin/PotreeConverter /app/PotreeConverter` is present
  - Add HEALTHCHECK directive
  - Verify CMD uses PORT environment variable
  - _Requirements: 5.2, 5.3, 5.6, 11.4_

- [x] 11.2 Update requirements.txt

  - Add `Pillow` for thumbnail generation
  - Verify all dependencies are listed
  - _Requirements: 7.1_

- [x] 12. Review and verify CI/CD pipeline

  - Verify existing GitHub Actions workflow is working correctly
  - Ensure deployment instructions are clear
  - _Requirements: 11.1, 11.2, 11.3, 11.5_

- [x] 12.1 Verify GitHub Actions workflow

  - Review existing `.github/workflows/deploy.yml`
  - Verify workflow triggers on push to main and manual dispatch
  - Confirm Docker Hub authentication is configured
  - Verify image tags include both latest and commit SHA
  - Test workflow by pushing to main branch
  - _Requirements: 11.1, 11.2, 11.3, 11.5_

- [x] 12.2 Document deployment process

  - Update README with GitHub secrets requirements (DOCKERHUB_USERNAME, DOCKERHUB_TOKEN)
  - Document Azure Container App update command from workflow output
  - Add instructions for manual deployment if needed
  - _Requirements: 11.5_

- [x] 13. Add job cleanup scheduler

  - Implement periodic cleanup of old jobs
  - _Requirements: 8.7, 10.4_

- [x] 13.1 Add cleanup to worker

  - Call `cleanup_old_jobs(72)` periodically in worker loop
  - Run cleanup every hour or after each job
  - Delete job records older than 72 hours
  - _Requirements: 8.7, 10.4_

- [ ] 14. Update documentation

  - Update README with setup instructions
  - Document API endpoints
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 14.1 Update README.md

  - Add environment variables documentation
  - Add local development instructions with Docker
  - Add API endpoint documentation
  - Add deployment instructions
  - _Requirements: 6.5_

- [x] 14.2 Enhance API documentation

  - Add descriptions to all endpoints
  - Add example requests/responses
  - Ensure OpenAPI docs are complete
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [ ] 15. Final testing and validation

  - Test complete workflow end-to-end
  - Verify all requirements are met
  - _Requirements: All_

- [x] 15.1 Test locally with Docker

  - Build Docker image locally
  - Run container with environment variables
  - Test project creation
  - Test point cloud upload and processing
  - Test job status retrieval
  - Verify files in Azure Blob Storage
  - Verify MongoDB records
  - _Requirements: 11.6_

- [x] 15.2 Test error scenarios

  - Test invalid file upload
  - Test missing project
  - Test conversion failure
  - Verify error messages and status codes
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [x] 15.3 Verify deployment readiness
  - Check health endpoint
  - Verify logging output
  - Test worker restart behavior
  - Verify cleanup of old jobs
  - _Requirements: 5.3, 5.4, 5.5, 8.7, 8.8_
