# Implementation Tasks: Orthophoto Upload and COG Conversion

## Task Breakdown

### Phase 1: Data Models and Database

- [x] 1. Update Project model with ortho field

  - [x] 1.1 Create Ortho class in models/Project.py

    - Add `file` field (Optional[str]) for COG URL
    - Add `thumbnail` field (Optional[str]) for thumbnail URL
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.9_

  - [x] 1.2 Add ortho field to Project class

    - Add `ortho: Optional[Ortho] = None` field
    - Update `_to_dict()` method to include ortho field
    - _Requirements: 6.1, 6.5, 6.7, 6.8_

  - [x] 1.3 Test Project model serialization
    - Test with ortho present
    - Test with ortho absent (null)
    - Test with thumbnail absent but file present
    - _Requirements: 6.5, 6.6, 6.9_

- [x] 2. Add database methods for ortho management

  - [x] 2.1 Implement update_project_ortho() in DatabaseManager

    - Accept project_id, file_url, thumbnail_url parameters
    - Update project document with ortho field
    - Set updated_at timestamp
    - Return success boolean
    - _Requirements: 6.3, 6.4_

  - [x] 2.2 Update deleteProject() to handle ortho files

    - Ensure ortho files are deleted with project
    - Use existing delete_project_files() method
    - _Requirements: 6.10_

  - [x] 2.3 Write unit tests for database methods
    - Test update_project_ortho() with valid data
    - Test update_project_ortho() with missing project
    - Test project deletion includes ortho files
    - _Requirements: 6.3, 6.4, 6.10_

### Phase 2: API Endpoint

- [x] 3. Create ortho upload endpoint

  - [x] 3.1 Add upload_ortho route to routes/projects.py

    - Create POST endpoint at `/projects/{project_id}/ortho`
    - Accept file upload (multipart/form-data)
    - Validate project exists (return 404 if not)
    - Validate file extension is .tif or .tiff (return 400 if not)
    - Validate file size <= 30GB (return 413 if exceeded)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [x] 3.2 Implement job creation in upload_ortho

    - Upload file to Azure at `jobs/{job_id}.tif`
    - Create job with type "ortho_conversion"
    - Set status to "pending"
    - Return 202 Accepted with job details
    - _Requirements: 1.7, 1.8, 1.9_

  - [x] 3.3 Add API documentation

    - Add comprehensive docstring with examples
    - Document all parameters and responses
    - Document error codes and messages
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8_

  - [x] 3.4 Update main.py endpoints dict

    - Add ortho upload endpoint to root response
    - Update OpenAPI description
    - _Requirements: 10.10_

  - [x] 3.5 Write endpoint tests
    - Test successful upload
    - Test invalid file extension
    - Test project not found
    - Test file too large
    - Test missing file
    - _Requirements: 1.2, 1.3, 1.5, 1.6_

### Phase 3: Worker - File Validation

- [x] 4. Implement GeoTIFF validation

  - [x] 4.1 Add \_validate_geotiff() method to JobWorker

    - Run `gdalinfo` command on file
    - Check return code for success
    - Raise ValueError if validation fails
    - Log validation results
    - _Requirements: 2.2, 2.3, 2.6, 2.7_

  - [x] 4.2 Add \_download_ortho_file() method

    - Download file from Azure `jobs/{job_id}.tif`
    - Save to local temp directory
    - Return local file path
    - Handle download errors
    - _Requirements: 3.1_

  - [x] 4.3 Write validation tests
    - Test with valid GeoTIFF
    - Test with invalid file (not a GeoTIFF)
    - Test with corrupted file
    - Test gdalinfo error handling
    - _Requirements: 2.2, 2.3, 2.5_

### Phase 4: Worker - COG Conversion

- [x] 5. Implement COG conversion

  - [x] 5.1 Add \_convert_to_cog() method to JobWorker

    - Run `gdal_translate` with COG driver
    - Use JPEG compression with quality 85
    - Enable tiling with 512px blocks
    - Return output file path
    - Raise RuntimeError if conversion fails
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.6_

  - [x] 5.2 Add progress tracking for conversion

    - Update job progress_message to "Converting to COG"
    - Log conversion start and completion
    - _Requirements: 3.9, 7.8_

  - [x] 5.3 Add cleanup for original file

    - Delete original uploaded file after conversion
    - Log cleanup operations
    - _Requirements: 3.8_

  - [x] 5.4 Write conversion tests
    - Test successful COG conversion
    - Test with various GeoTIFF formats
    - Test GDAL error handling
    - Verify COG output format
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.6_

### Phase 5: Worker - Thumbnail Generation

- [x] 6. Implement thumbnail generation

  - [x] 6.1 Add \_generate_ortho_thumbnail() method to JobWorker

    - Run `gdal_translate` to create PNG
    - Set output size to 512px wide
    - Return thumbnail file path or None
    - Catch and log errors without failing job
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [x] 6.2 Add progress tracking for thumbnail

    - Update job progress_message to "Generating thumbnail"
    - Log thumbnail generation results
    - _Requirements: 4.7, 7.8_

  - [x] 6.3 Write thumbnail tests
    - Test successful thumbnail generation
    - Test with various image sizes
    - Test graceful failure (returns None)
    - Verify thumbnail dimensions
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

### Phase 6: Worker - Azure Upload

- [x] 7. Implement Azure upload for ortho files

  - [x] 7.1 Add \_upload_ortho_to_azure() method to JobWorker

    - Upload COG to `{project_id}/ortho/ortho.tif`
    - Upload thumbnail to `{project_id}/ortho/ortho_thumbnail.png` (if exists)
    - Use overwrite=True parameter
    - Generate SAS URLs with 30-day validity
    - Return dict with file and thumbnail URLs
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.10_

  - [x] 7.2 Add progress tracking for upload

    - Update job progress_message to "Uploading to Azure"
    - Log each file upload
    - _Requirements: 5.8, 7.8, 7.10_

  - [x] 7.3 Handle upload errors

    - Catch Azure exceptions
    - Mark job as failed with error message
    - _Requirements: 5.9, 9.3_

  - [x] 7.4 Write upload tests
    - Test successful COG upload
    - Test successful thumbnail upload
    - Test upload without thumbnail
    - Test Azure error handling
    - Verify SAS URL generation
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.10_

### Phase 7: Worker - Project Update

- [x] 8. Implement project update with ortho URLs

  - [x] 8.1 Add \_update_project_ortho() method to JobWorker

    - Get project from database
    - Create Ortho object with URLs
    - Update project.ortho field
    - Call db.updateProject()
    - Handle project not found error
    - _Requirements: 6.3, 6.4, 8.9_

  - [x] 8.2 Write project update tests
    - Test successful project update
    - Test with both file and thumbnail
    - Test with file only (no thumbnail)
    - Test project not found error
    - _Requirements: 6.3, 6.4, 6.6_

### Phase 8: Worker - Main Processing Flow

- [x] 9. Implement main ortho processing method

  - [x] 9.1 Add process_ortho_job() method to JobWorker

    - Update status to "processing"
    - Call \_download_ortho_file()
    - Check cancellation
    - Call \_validate_geotiff()
    - Check cancellation
    - Call \_convert_to_cog()
    - Check cancellation
    - Call \_generate_ortho_thumbnail()
    - Check cancellation
    - Call \_upload_ortho_to_azure()
    - Call \_update_project_ortho()
    - Mark job as completed
    - Call cleanup method
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

  - [x] 9.2 Add error handling wrapper

    - Catch CancellationException
    - Catch general exceptions
    - Call appropriate error handlers
    - _Requirements: 7.6, 8.8, 9.7, 9.8, 9.9_

  - [x] 9.3 Update process_job() to route ortho jobs

    - Check job.type
    - If "ortho_conversion", call process_ortho_job()
    - Otherwise, use existing point cloud processing
    - _Requirements: 8.2, 8.3_

  - [x] 9.4 Write integration tests for main flow
    - Test complete successful workflow
    - Test with thumbnail generation
    - Test without thumbnail generation
    - Test error at each step
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

### Phase 9: Worker - Cleanup and Cancellation

- [x] 10. Implement cleanup for ortho jobs

  - [x] 10.1 Add \_cleanup_ortho_files() method to JobWorker

    - Accept variable number of file paths
    - Delete each local file if exists
    - Call az.delete_job_file() for Azure cleanup
    - Log all cleanup operations
    - Handle errors gracefully
    - _Requirements: 5.10, 8.7_

  - [x] 10.2 Add \_handle_ortho_cancellation() method

    - Delete local temp directory
    - Delete Azure job file
    - Update job status to "cancelled"
    - Set completed_at timestamp
    - Set progress_message to "Job cancelled by user"
    - _Requirements: 7.9, 8.4, 8.5_

  - [x] 10.3 Add \_handle_ortho_error() method

    - Log error with full stack trace
    - Delete local temp files
    - Delete Azure job file
    - Update job status to "failed"
    - Set error message from exception
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 9.9, 9.10_

  - [x] 10.4 Integrate cancellation checks

    - Add \_check_cancellation() calls at each major step
    - Ensure cancellation is checked before long operations
    - _Requirements: 7.9, 8.4_

  - [x] 10.5 Write cleanup and cancellation tests
    - Test cleanup of all file types
    - Test cancellation at each step
    - Test error handling and cleanup
    - Test Azure file deletion
    - _Requirements: 5.10, 7.9, 8.4, 8.5, 9.10_

### Phase 10: Integration and Testing

- [x] 11. End-to-end integration testing

  - [x] 11.1 Test complete upload workflow

    - Upload small GeoTIFF (<1GB)
    - Verify job creation
    - Verify worker processing
    - Verify COG in Azure
    - Verify thumbnail in Azure
    - Verify project updated
    - Verify SAS URLs work
    - _Requirements: 1.1-1.10, 3.1-3.10, 7.1-7.10_

  - [x] 11.2 Test large file handling

    - Upload large GeoTIFF (>10GB)
    - Verify streaming works
    - Verify performance requirements
    - _Requirements: 1.4, 11.1, 11.2_

  - [x] 11.3 Test overwrite functionality

    - Upload ortho to project
    - Upload different ortho to same project
    - Verify old files are overwritten
    - Verify project has new URLs
    - _Requirements: 1.10, 5.6, 5.7_

  - [x] 11.4 Test cancellation workflow

    - Start ortho job
    - Cancel during validation
    - Cancel during conversion
    - Cancel during upload
    - Verify cleanup in each case
    - _Requirements: 7.9, 8.4, 8.5_

  - [x] 11.5 Test error scenarios

    - Upload invalid file
    - Upload corrupted GeoTIFF
    - Simulate Azure upload failure
    - Simulate GDAL failure
    - Verify error messages
    - Verify cleanup
    - _Requirements: 9.1-9.10_

  - [x] 11.6 Test concurrent processing
    - Queue multiple ortho jobs
    - Queue ortho and point cloud jobs together
    - Verify both types process correctly
    - Verify no resource conflicts
    - _Requirements: 8.5, 8.10, 11.5, 11.6_

### Phase 11: Performance and Security

- [x] 12. Performance testing

  - [x] 12.1 Test small file conversion (<1GB)

    - Measure total processing time
    - Verify completes within 5 minutes
    - _Requirements: 11.1_

  - [x] 12.2 Test large file conversion (>10GB)

    - Measure total processing time
    - Verify completes within 30 minutes
    - _Requirements: 11.2_

  - [x] 12.3 Test thumbnail generation performance

    - Measure thumbnail generation time
    - Verify completes within 30 seconds
    - _Requirements: 11.3_

  - [x] 12.4 Test Azure upload performance

    - Verify streaming is used
    - Monitor memory usage
    - _Requirements: 11.4, 11.8_

  - [x] 12.5 Test worker responsiveness
    - Verify progress updates every 30 seconds
    - Verify cancellation is responsive
    - _Requirements: 11.7_

- [x] 13. Security testing

  - [x] 13.1 Test file validation

    - Attempt upload with .exe file renamed to .tif
    - Attempt upload with malicious file
    - Verify validation catches issues
    - _Requirements: 12.1, 12.6_

  - [x] 13.2 Test size limits

    - Attempt upload >30GB
    - Verify rejection
    - _Requirements: 12.2_

  - [x] 13.3 Test SAS URL security

    - Verify 30-day expiration
    - Verify read-only permissions
    - _Requirements: 12.3_

  - [x] 13.4 Test path security

    - Verify no path traversal possible
    - Verify files stored in correct project directory
    - _Requirements: 12.4_

  - [x] 13.5 Test error message security
    - Verify no internal paths exposed
    - Verify no sensitive info in errors
    - _Requirements: 12.8_

### Phase 12: Documentation and Deployment

- [x] 14. Documentation

  - [x] 14.1 Update API documentation

    - Document POST /projects/{id}/ortho endpoint
    - Add examples and use cases
    - Document error responses
    - _Requirements: 10.1-10.10_

  - [x] 14.2 Update README

    - Add ortho upload feature description
    - Document GDAL dependency
    - Add usage examples
    - _Requirements: Dependencies section_

  - [x] 14.3 Create deployment guide

    - Document GDAL installation steps
    - Document Python package requirements
    - Document environment setup
    - _Requirements: Deployment section_

  - [x] 14.4 Write user guide
    - Explain ortho upload workflow
    - Explain COG benefits
    - Provide troubleshooting tips
    - _Requirements: 10.8, 10.9_

- [x] 15. Deployment preparation

  - [x] 15.1 Update requirements.txt

    - Add GDAL Python bindings
    - Specify version requirements
    - _Requirements: Dependencies section_

  - [x] 15.2 Update Dockerfile (if applicable)

    - Add GDAL installation
    - Add required system packages
    - _Requirements: Deployment section_

  - [x] 15.3 Test deployment on staging

    - Deploy to staging environment
    - Verify GDAL is available
    - Run smoke tests
    - _Requirements: Deployment section_

  - [x] 15.4 Create rollback plan
    - Document rollback steps
    - Test backward compatibility
    - _Requirements: Deployment section_

## Task Dependencies

```
Phase 1 (Models) → Phase 2 (API) → Phase 3-9 (Worker) → Phase 10 (Integration) → Phase 11 (Performance/Security) → Phase 12 (Documentation/Deployment)

Within Worker phases:
Phase 3 (Validation) → Phase 4 (Conversion) → Phase 5 (Thumbnail) → Phase 6 (Upload) → Phase 7 (Project Update) → Phase 8 (Main Flow) → Phase 9 (Cleanup)
```

## Estimated Effort

- Phase 1: 2-3 hours
- Phase 2: 3-4 hours
- Phase 3: 2-3 hours
- Phase 4: 3-4 hours
- Phase 5: 2-3 hours
- Phase 6: 3-4 hours
- Phase 7: 2 hours
- Phase 8: 3-4 hours
- Phase 9: 3-4 hours
- Phase 10: 4-6 hours
- Phase 11: 3-4 hours
- Phase 12: 3-4 hours

**Total: 35-48 hours**

## Success Criteria

- [ ] Users can upload GeoTIFF files via API
- [ ] Files are automatically converted to COG format
- [ ] Thumbnails are generated for visual preview
- [ ] COG and thumbnails are stored in Azure
- [ ] Project documents include ortho URLs
- [ ] Jobs can be cancelled during processing
- [ ] All temporary files are cleaned up
- [ ] Error messages are clear and helpful
- [ ] Performance meets requirements (<5min for small files, <30min for large)
- [ ] Security validations prevent malicious uploads
- [ ] API documentation is complete and accurate
- [ ] All tests pass
