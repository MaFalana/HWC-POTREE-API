# Requirements Document

## Introduction

This document outlines the requirements for completing the HWC Potree API, a FastAPI-based backend service that processes point cloud data (LAS/LAZ files), converts them to Potree format for web visualization, and manages project metadata. The API will be deployed on Azure using Docker containers, with MongoDB for data persistence and Azure Blob Storage for file storage.

The system enables users to upload projects, attach point cloud files to projects, process those files in the background to extract metadata and convert to Potree format, and retrieve processed data for visualization in web applications.

## Requirements

### Requirement 1: Complete Jobs/Background Processing System

**User Story:** As a user, I want to upload point cloud files and have them processed in the background, so that I can continue using the application without waiting for long-running conversions to complete.

#### Acceptance Criteria

1. WHEN a user uploads a point cloud file THEN the system SHALL create a background job to process the file
2. WHEN a background job is created THEN the system SHALL return a job ID immediately to the user
3. WHEN a job is processing THEN the system SHALL track its status (pending, processing, completed, failed)
4. WHEN a user requests job status THEN the system SHALL return the current status and progress information
5. IF a job fails THEN the system SHALL store error details and make them available via the API
6. WHEN a job completes successfully THEN the system SHALL update the associated project with the processed data URLs

### Requirement 2: Fix and Complete Potree Conversion Pipeline

**User Story:** As a user, I want my point cloud files converted to Potree format and stored in Azure Blob Storage, so that they can be visualized in web browsers.

#### Acceptance Criteria

1. WHEN a point cloud file is uploaded THEN the system SHALL temporarily store it in Azure Blob Storage
2. WHEN conversion starts THEN the system SHALL invoke PotreeConverter with correct parameters
3. WHEN conversion completes THEN the system SHALL upload all output files to Azure Blob Storage with proper MIME types
4. WHEN uploading to Azure THEN the system SHALL organize files by project ID
5. WHEN conversion completes THEN the system SHALL generate SAS URLs for the main viewer HTML file
6. WHEN conversion completes THEN the system SHALL clean up temporary files from local storage
7. IF conversion fails THEN the system SHALL log errors and update job status accordingly

### Requirement 3: Complete Project Management Endpoints

**User Story:** As a user, I want to manage my projects through a complete REST API, so that I can create, read, update, and delete projects with all necessary metadata.

#### Acceptance Criteria

1. WHEN creating a project THEN the system SHALL validate all required fields
2. WHEN updating a project THEN the system SHALL accept partial updates without requiring all fields
3. WHEN deleting a project THEN the system SHALL remove both MongoDB records and Azure Blob Storage files
4. WHEN retrieving a project THEN the system SHALL include all associated metadata
5. WHEN listing projects THEN the system SHALL support pagination
6. WHEN a project has a point cloud THEN the system SHALL return valid SAS URLs for accessing the data

### Requirement 4: Implement Proper Error Handling and Validation

**User Story:** As a developer, I want comprehensive error handling throughout the API, so that users receive clear error messages and the system remains stable.

#### Acceptance Criteria

1. WHEN invalid data is submitted THEN the system SHALL return HTTP 400 with detailed validation errors
2. WHEN a resource is not found THEN the system SHALL return HTTP 404 with a clear message
3. WHEN a server error occurs THEN the system SHALL return HTTP 500 and log the full error details
4. WHEN file uploads fail THEN the system SHALL return appropriate error codes and messages
5. WHEN Azure Blob Storage operations fail THEN the system SHALL handle exceptions gracefully
6. WHEN MongoDB operations fail THEN the system SHALL handle exceptions and provide meaningful feedback

### Requirement 5: Azure Deployment Configuration

**User Story:** As a DevOps engineer, I want the application properly configured for Azure deployment, so that it can run reliably in production.

#### Acceptance Criteria

1. WHEN deploying to Azure THEN the system SHALL use environment variables for all configuration
2. WHEN running in Docker THEN the system SHALL expose the correct port and handle Azure PORT variable
3. WHEN deployed THEN the system SHALL include health check endpoints for monitoring
4. WHEN deployed THEN the system SHALL log to stdout/stderr for Azure logging integration
5. WHEN deployed THEN the system SHALL handle graceful shutdown signals
6. WHEN PotreeConverter runs THEN the system SHALL use the Linux binary from the Docker image

### Requirement 6: Fix Existing Code Issues

**User Story:** As a developer, I want all existing code issues resolved, so that the API functions correctly.

#### Acceptance Criteria

1. WHEN the update project endpoint is called THEN the system SHALL correctly pass the id parameter
2. WHEN parsing tags THEN the system SHALL import json module in projects router
3. WHEN PotreeConverter runs THEN the system SHALL use POTREE_PATH environment variable
4. WHEN uploading folders to Azure THEN the system SHALL set correct content types for all files
5. WHEN converting point clouds THEN the system SHALL return the SAS URL for the viewer
6. WHEN the process endpoint is called THEN the system SHALL combine metadata extraction and Potree conversion

### Requirement 7: Thumbnail Generation

**User Story:** As a user, I want automatic thumbnail generation for my point clouds, so that I can preview them in listings.

#### Acceptance Criteria

1. WHEN a point cloud is processed THEN the system SHALL generate a thumbnail using PDAL
2. WHEN generating thumbnails THEN the system SHALL use RGB colors if available
3. WHEN generating thumbnails THEN the system SHALL use transparent background
4. WHEN generating thumbnails THEN the system SHALL maintain aspect ratio with square preference
5. WHEN thumbnail is generated THEN the system SHALL upload to Azure as `{project_id}/thumbnail.png`
6. WHEN thumbnail is uploaded THEN the system SHALL update project.thumbnail with SAS URL
7. WHEN thumbnail generation fails THEN the system SHALL log error but continue with conversion

### Requirement 8: Background Worker System

**User Story:** As a system administrator, I want a reliable background worker for processing jobs, so that conversions happen asynchronously.

#### Acceptance Criteria

1. WHEN the application starts THEN the system SHALL start a worker thread
2. WHEN the worker runs THEN the system SHALL poll MongoDB every 5 seconds for pending jobs
3. WHEN a pending job is found THEN the system SHALL update status to "processing"
4. WHEN a job is processing THEN the system SHALL extract metadata, generate thumbnail, and convert to Potree
5. WHEN a job completes THEN the system SHALL update project with all data and delete temp files
6. WHEN a job fails THEN the system SHALL update status to "failed" with error message
7. WHEN a job is older than 72 hours THEN the system SHALL delete it from MongoDB
8. WHEN the worker starts THEN the system SHALL reset stale "processing" jobs to "pending"

### Requirement 9: Job Management Data Model

**User Story:** As a developer, I want a clear job data model, so that job status can be tracked reliably.

#### Acceptance Criteria

1. WHEN a job is created THEN the system SHALL store job_id, project_id, status, created_at, file_path
2. WHEN job status changes THEN the system SHALL update updated_at timestamp
3. WHEN a job fails THEN the system SHALL store error_message field
4. WHEN a job completes THEN the system SHALL store completed_at timestamp
5. WHEN listing jobs THEN the system SHALL support filtering by project_id and status
6. WHEN a job is created THEN the system SHALL use a separate "jobs" collection in MongoDB

### Requirement 10: File Cleanup and Storage Management

**User Story:** As a system administrator, I want automatic cleanup of temporary files, so that storage costs are minimized.

#### Acceptance Criteria

1. WHEN a Potree conversion completes THEN the system SHALL delete the original LAS/LAZ file from temp storage
2. WHEN a Potree conversion completes THEN the system SHALL delete the original file from Azure jobs folder
3. WHEN a job completes or fails THEN the system SHALL delete local temporary files
4. WHEN a job is older than 72 hours THEN the system SHALL delete the job record from MongoDB
5. WHEN uploading to Azure THEN the system SHALL organize files as `{project_id}/` for Potree output
6. WHEN uploading to Azure THEN the system SHALL use `jobs/` folder for temporary uploaded files

### Requirement 11: CI/CD Pipeline

**User Story:** As a developer, I want automated Docker image builds, so that deployments are consistent and automated.

#### Acceptance Criteria

1. WHEN code is pushed to main branch THEN the system SHALL trigger GitHub Actions workflow
2. WHEN workflow runs THEN the system SHALL build Docker image with correct tags
3. WHEN image is built THEN the system SHALL push to Docker Hub
4. WHEN building image THEN the system SHALL include PotreeConverter Linux binary from bin/ folder
5. WHEN workflow completes THEN the system SHALL report success or failure status
6. WHEN building locally THEN developers SHALL be able to use Docker for testing Potree conversion
