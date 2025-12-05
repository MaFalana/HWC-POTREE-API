# Requirements Document: Orthophoto Upload and COG Conversion

## Introduction

This document outlines the requirements for adding orthophoto (ortho) upload functionality to the HWC Potree API. Currently, the API only handles point cloud data processing. This enhancement will allow users to upload GeoTIFF files (TIF/TIFF) for their projects, which will be automatically converted to Cloud Optimized GeoTIFF (COG) format using GDAL and stored in Azure Blob Storage alongside the project's point cloud data. This feature will enable users to view both point clouds and orthophotos together in their project visualizations.

## Requirements

### Requirement 1: Ortho Upload Endpoint

**User Story:** As a user, I want to upload an orthophoto (GeoTIFF) file for my project, so that I can view aerial imagery alongside my point cloud data.

#### Acceptance Criteria

1. WHEN a user uploads an ortho THEN the system SHALL provide a POST endpoint at `/projects/{project_id}/ortho`
2. WHEN the upload endpoint is called THEN the system SHALL verify the project exists
3. IF the project does not exist THEN the system SHALL return 404 Not Found with message "Project with id {project_id} not found"
4. WHEN a file is uploaded THEN the system SHALL accept files up to 30GB in size (matching point cloud limit)
5. WHEN a file is uploaded THEN the system SHALL validate the file extension is `.tif` or `.tiff`
6. IF the file extension is invalid THEN the system SHALL return 400 Bad Request with message "Invalid file type. Only .tif and .tiff files are supported"
7. WHEN the upload is accepted THEN the system SHALL create a new job with type "ortho_conversion"
8. WHEN the job is created THEN the system SHALL return 202 Accepted with job_id for status polling
9. WHEN the upload succeeds THEN the response SHALL include job_id, project_id, status, and created_at timestamp
10. WHEN a user uploads a new ortho to a project that already has one THEN the system SHALL overwrite the existing ortho files

### Requirement 2: File Validation

**User Story:** As a system administrator, I want uploaded files to be validated before processing, so that invalid files are rejected early and don't waste processing resources.

#### Acceptance Criteria

1. WHEN a file is uploaded THEN the endpoint SHALL check the file extension is `.tif` or `.tiff`
2. WHEN the worker begins processing THEN it SHALL use `gdalinfo` to verify the file is a valid raster
3. IF `gdalinfo` fails THEN the worker SHALL mark the job as failed with error message "Invalid GeoTIFF file"
4. WHEN validation succeeds THEN the worker SHALL proceed with COG conversion
5. IF the file is corrupted THEN the worker SHALL detect it during validation and fail the job gracefully
6. WHEN validation fails THEN the worker SHALL delete the temporary uploaded file
7. WHEN validation fails THEN the worker SHALL log the validation error with job_id and file details

### Requirement 3: COG Conversion Processing

**User Story:** As a user, I want my uploaded GeoTIFF to be converted to Cloud Optimized GeoTIFF format, so that it loads efficiently in web viewers.

#### Acceptance Criteria

1. WHEN the worker processes an ortho job THEN it SHALL download the uploaded file from Azure to local temp storage
2. WHEN the file is downloaded THEN the worker SHALL use `gdal_translate` to convert it to COG format
3. WHEN converting to COG THEN the system SHALL use compression (JPEG with quality 85)
4. WHEN converting to COG THEN the system SHALL enable tiling for efficient streaming
5. WHEN converting to COG THEN the output file SHALL be named `ortho.tif`
6. IF conversion fails THEN the worker SHALL mark the job as failed with the GDAL error message
7. WHEN conversion succeeds THEN the worker SHALL proceed to thumbnail generation
8. WHEN conversion completes THEN the worker SHALL delete the original uploaded file from local temp storage
9. WHEN conversion is in progress THEN the job status SHALL be "processing"
10. WHEN conversion completes THEN the worker SHALL update job progress to indicate upload phase

### Requirement 4: Thumbnail Generation

**User Story:** As a user, I want a thumbnail preview of my orthophoto, so that I can quickly identify projects visually in the project list.

#### Acceptance Criteria

1. WHEN COG conversion succeeds THEN the worker SHALL attempt to generate a thumbnail
2. WHEN generating a thumbnail THEN the system SHALL use `gdal_translate` to create a PNG preview
3. WHEN generating a thumbnail THEN the output SHALL be 512 pixels wide with proportional height
4. WHEN generating a thumbnail THEN the output file SHALL be named `ortho_thumbnail.png`
5. IF thumbnail generation fails THEN the worker SHALL log the error but continue processing
6. IF thumbnail generation fails THEN the job SHALL still succeed (thumbnail is optional)
7. WHEN thumbnail generation succeeds THEN the worker SHALL upload it to Azure
8. WHEN thumbnail generation succeeds THEN the project SHALL be updated with the thumbnail URL
9. WHEN no thumbnail is generated THEN the project.ortho.thumbnail field SHALL be null
10. WHEN thumbnail is uploaded THEN it SHALL use a 30-day SAS URL validity period

### Requirement 5: Azure Storage Integration

**User Story:** As a system administrator, I want ortho files stored in Azure Blob Storage organized by project, so that they are easily accessible and manageable alongside point cloud data.

#### Acceptance Criteria

1. WHEN an ortho is uploaded THEN the temporary file SHALL be stored at `jobs/{job_id}.tif` in Azure
2. WHEN COG conversion completes THEN the COG file SHALL be uploaded to `{project_id}/ortho/ortho.tif`
3. WHEN thumbnail generation completes THEN the thumbnail SHALL be uploaded to `{project_id}/ortho/ortho_thumbnail.png`
4. WHEN uploading to Azure THEN the system SHALL use the existing Azure storage client
5. WHEN uploading completes THEN the system SHALL generate SAS URLs with 30-day validity
6. WHEN a new ortho is uploaded for a project THEN existing ortho files SHALL be overwritten
7. WHEN overwriting files THEN the system SHALL use the `overwrite=True` parameter
8. WHEN files are uploaded THEN the worker SHALL log each upload operation with blob name
9. WHEN upload fails THEN the worker SHALL mark the job as failed with the Azure error message
10. WHEN the job is cancelled THEN the worker SHALL delete temporary files from Azure using `delete_job_file()`

### Requirement 6: Project Model Updates

**User Story:** As a frontend developer, I want the project object to include ortho URLs, so that I can display orthophotos in the UI alongside point cloud data.

#### Acceptance Criteria

1. WHEN the Project model is updated THEN it SHALL include an `ortho` field
2. WHEN the `ortho` field exists THEN it SHALL be an object with subfields: `file` and `thumbnail`
3. WHEN an ortho is uploaded THEN `project.ortho.file` SHALL contain the SAS URL to the COG file
4. WHEN a thumbnail is generated THEN `project.ortho.thumbnail` SHALL contain the SAS URL to the thumbnail
5. WHEN no ortho exists THEN the `ortho` field SHALL be null or omitted
6. WHEN no thumbnail exists THEN `project.ortho.thumbnail` SHALL be null
7. WHEN the project is retrieved via GET `/projects/{id}` THEN the ortho field SHALL be included in the response
8. WHEN projects are listed via GET `/projects/` THEN the ortho field SHALL be included for each project
9. WHEN the ortho field is serialized THEN it SHALL follow this structure:
   ```json
   {
     "ortho": {
       "file": "https://storage.blob.core.windows.net/.../ortho.tif?sas_token",
       "thumbnail": "https://storage.blob.core.windows.net/.../ortho_thumbnail.png?sas_token"
     }
   }
   ```
10. WHEN a project is deleted THEN the ortho files SHALL be deleted along with other project files

### Requirement 7: Job Management

**User Story:** As a user, I want to track the progress of my ortho conversion job, so that I know when my orthophoto is ready to view.

#### Acceptance Criteria

1. WHEN an ortho upload job is created THEN it SHALL have type "ortho_conversion"
2. WHEN the job is created THEN it SHALL be added to the job queue with status "pending"
3. WHEN the worker picks up the job THEN the status SHALL change to "processing"
4. WHEN the job completes successfully THEN the status SHALL change to "completed"
5. IF the job fails THEN the status SHALL change to "failed" with an error message
6. WHEN the job is in progress THEN users SHALL be able to poll GET `/jobs/{job_id}` for status
7. WHEN the job completes THEN the completed_at timestamp SHALL be set
8. WHEN the job is processing THEN the progress_message SHALL indicate the current step (validation, conversion, thumbnail, upload)
9. WHEN the job is cancelled THEN it SHALL follow the same cancellation workflow as point cloud jobs
10. WHEN listing jobs for a project THEN ortho jobs SHALL be included in the results

### Requirement 8: Worker Integration

**User Story:** As a system administrator, I want ortho conversion to use the existing worker infrastructure, so that it benefits from the same reliability, logging, and cancellation features as point cloud processing.

#### Acceptance Criteria

1. WHEN processing ortho jobs THEN the system SHALL use the existing JobWorker class
2. WHEN the worker processes a job THEN it SHALL check the job type and route to appropriate handler
3. WHEN the job type is "ortho_conversion" THEN the worker SHALL call the ortho processing method
4. WHEN processing ortho jobs THEN the worker SHALL check for cancellation at each major step
5. WHEN an ortho job is cancelled THEN the worker SHALL clean up temporary files and partial outputs
6. WHEN processing ortho jobs THEN the worker SHALL use the same error handling patterns as point cloud jobs
7. WHEN processing ortho jobs THEN the worker SHALL log all operations with job_id and step details
8. WHEN the worker encounters an error THEN it SHALL update the job with the error message
9. WHEN the worker completes processing THEN it SHALL update the project document with ortho URLs
10. WHEN multiple jobs are queued THEN ortho jobs SHALL be processed in FIFO order alongside point cloud jobs

### Requirement 9: Error Handling

**User Story:** As a user, I want clear error messages when ortho upload fails, so that I can understand what went wrong and how to fix it.

#### Acceptance Criteria

1. WHEN file validation fails THEN the error message SHALL indicate "Invalid GeoTIFF file"
2. WHEN GDAL conversion fails THEN the error message SHALL include the GDAL error output
3. WHEN Azure upload fails THEN the error message SHALL indicate "Failed to upload ortho to Azure storage"
4. WHEN the project doesn't exist THEN the error message SHALL be "Project with id {project_id} not found"
5. WHEN the file is too large THEN the error message SHALL indicate the maximum file size
6. WHEN the file extension is wrong THEN the error message SHALL list supported formats (.tif, .tiff)
7. WHEN an error occurs THEN the job status SHALL be set to "failed"
8. WHEN an error occurs THEN the error message SHALL be stored in the job document
9. WHEN an error occurs THEN the worker SHALL log the full error with stack trace
10. WHEN cleanup fails after an error THEN the cleanup error SHALL be logged but not fail the job again

### Requirement 10: API Documentation

**User Story:** As a frontend developer, I want comprehensive API documentation for the ortho upload endpoint, so that I can integrate it correctly in the UI.

#### Acceptance Criteria

1. WHEN viewing API docs THEN the POST `/projects/{project_id}/ortho` endpoint SHALL be documented
2. WHEN viewing endpoint docs THEN it SHALL include a description of the ortho upload feature
3. WHEN viewing endpoint docs THEN it SHALL list all request parameters (project_id, file)
4. WHEN viewing endpoint docs THEN it SHALL list all possible response codes (202, 400, 404, 500)
5. WHEN viewing endpoint docs THEN it SHALL include example request and response payloads
6. WHEN viewing endpoint docs THEN it SHALL document the file size limit (30GB)
7. WHEN viewing endpoint docs THEN it SHALL document supported file formats (.tif, .tiff)
8. WHEN viewing endpoint docs THEN it SHALL explain the job polling workflow
9. WHEN viewing endpoint docs THEN it SHALL document the project.ortho field structure
10. WHEN viewing the root endpoint THEN the ortho upload endpoint SHALL be listed in the available endpoints

### Requirement 11: Performance Requirements

**User Story:** As a system administrator, I want ortho conversion to be efficient and not block other operations, so that the system remains responsive under load.

#### Acceptance Criteria

1. WHEN converting small files (<1GB) THEN the conversion SHALL complete within 5 minutes
2. WHEN converting large files (>10GB) THEN the conversion SHALL complete within 30 minutes
3. WHEN generating thumbnails THEN the operation SHALL complete within 30 seconds
4. WHEN uploading to Azure THEN the system SHALL use streaming to avoid memory issues
5. WHEN processing ortho jobs THEN the worker SHALL not block point cloud job processing
6. WHEN multiple ortho jobs are queued THEN they SHALL be processed concurrently if resources allow
7. WHEN conversion is in progress THEN the worker SHALL update progress at least every 30 seconds
8. WHEN files are downloaded from Azure THEN the system SHALL use streaming to avoid memory issues
9. WHEN the system is under load THEN ortho jobs SHALL not starve point cloud jobs of resources
10. WHEN cleanup occurs THEN it SHALL complete within 60 seconds

### Requirement 12: Security Requirements

**User Story:** As a security administrator, I want ortho uploads to be secure and validated, so that malicious files cannot compromise the system.

#### Acceptance Criteria

1. WHEN a file is uploaded THEN the system SHALL validate the file extension before accepting it
2. WHEN a file is uploaded THEN the system SHALL enforce the 30GB size limit
3. WHEN generating SAS URLs THEN they SHALL expire after 30 days
4. WHEN storing files in Azure THEN they SHALL be organized by project_id to prevent unauthorized access
5. WHEN processing files THEN the worker SHALL run GDAL commands with appropriate resource limits
6. WHEN validation fails THEN the uploaded file SHALL be deleted immediately
7. WHEN a job is cancelled THEN all temporary files SHALL be cleaned up
8. WHEN errors occur THEN error messages SHALL not expose internal system paths
9. WHEN logging operations THEN sensitive information SHALL not be logged
10. WHEN users access ortho files THEN they SHALL use SAS URLs with read-only permissions

## Non-Functional Requirements

### Performance

- COG conversion should leverage GDAL's built-in optimization
- Thumbnail generation should be fast and not block the main conversion
- Azure uploads should use chunked/streaming uploads for large files

### Scalability

- The system should handle multiple concurrent ortho conversions
- Queue-based processing should prevent resource exhaustion
- Worker should be able to process ortho and point cloud jobs in parallel

### Reliability

- Failed conversions should not leave orphaned files in Azure
- Cancellation should reliably clean up all temporary files
- The system should recover gracefully from GDAL errors

### Maintainability

- Ortho processing code should follow the same patterns as point cloud processing
- GDAL commands should be configurable (compression, quality, tiling options)
- Logging should provide sufficient detail for troubleshooting

## Dependencies

- GDAL library with COG driver support
- Existing Azure Blob Storage infrastructure
- Existing job queue and worker system
- MongoDB for storing project and job data
- Python GDAL bindings (osgeo.gdal)

## Out of Scope

- Multiple orthos per project (only one ortho per project in this version)
- Ortho reprojection or coordinate system conversion
- Ortho mosaicking or stitching multiple files
- Advanced GDAL options (overviews, masks, etc.)
- Ortho editing or manipulation
- Direct ortho viewing in the API (viewing handled by frontend)
