# Implementation Plan

- [x] 1. Update Job model and database schema for cancellation support

  - Add `cancelled` boolean field to Job model with default value False
  - Update Job.\_to_dict() method to include cancelled field
  - _Requirements: 1.4, 1.5, 6.1, 6.2_

- [x] 2. Implement database methods for cancellation

  - [x] 2.1 Implement cancel_job() method in DatabaseManager

    - Accept job_id and cancelled_at timestamp parameters
    - Update job document with cancelled=True, status='cancelled', updated_at, and completed_at
    - Add logging for cancellation events
    - _Requirements: 1.4, 1.5, 6.2, 6.4_

  - [x] 2.2 Implement is_job_cancelled() method in DatabaseManager
    - Query only the cancelled field for efficiency
    - Return boolean indicating cancellation status
    - Handle case where job doesn't exist (return False)
    - _Requirements: 2.1, 6.3, 13.3_

- [x] 3. Create job cancellation API endpoint

  - [x] 3.1 Add cancel_job route to routes/jobs.py

    - Create POST endpoint at /jobs/{job_id}/cancel
    - Validate job exists (return 404 if not found)
    - Check job status and reject cancellation for completed/failed/cancelled jobs (return 409)
    - Call DB.cancel_job() for valid cancellations
    - Return success response with job details
    - _Requirements: 1.1, 1.2, 1.3, 1.6, 1.7, 1.8, 1.9, 1.10, 8.1, 8.2, 8.3, 8.4_

  - [ ] 3.2 Write unit tests for cancellation endpoint
    - Test successful cancellation of pending job
    - Test successful cancellation of processing job
    - Test rejection of completed job cancellation
    - Test rejection of failed job cancellation
    - Test rejection of already cancelled job
    - Test 404 for non-existent job
    - _Requirements: 1.6, 1.7, 1.8_

- [x] 4. Implement worker cancellation handling

  - [x] 4.1 Add CancellationException class to worker.py

    - Create custom exception for cancellation events
    - _Requirements: 2.2_

  - [x] 4.2 Implement \_check_cancellation() method in JobWorker

    - Call DB.is_job_cancelled() with job_id
    - Raise CancellationException if cancelled
    - Add logging for cancellation detection
    - _Requirements: 2.1, 2.10, 13.3, 13.4_

  - [x] 4.3 Implement \_cleanup_cancelled_job() method in JobWorker

    - Delete local temporary file with error handling
    - Delete Azure job file with error handling
    - Delete partial Potree output files with error handling
    - Make cleanup idempotent
    - Log all cleanup operations
    - _Requirements: 2.3, 2.4, 2.5, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

  - [x] 4.4 Implement \_handle_cancellation() method in JobWorker

    - Call \_cleanup_cancelled_job()
    - Update job with completed_at timestamp
    - Set progress_message to "Job cancelled by user"
    - Log cancellation handling
    - _Requirements: 2.7, 2.8, 2.9_

  - [x] 4.5 Integrate cancellation checks into process_job()
    - Add \_check_cancellation() call before metadata extraction step
    - Add \_check_cancellation() call before thumbnail generation step
    - Add \_check_cancellation() call before Potree conversion step
    - Add \_check_cancellation() call before Azure upload step
    - Wrap process_job in try-catch for CancellationException
    - Call \_handle_cancellation() when CancellationException caught
    - _Requirements: 2.1, 2.2, 2.6_

- [x] 5. Implement database indexes for performance

  - [x] 5.1 Add project indexes to \_ensure_indexes()

    - Create index on created_at field (descending)
    - Create index on name field
    - Create index on client field
    - Create index on tags field (array index)
    - Create text index on name and description fields
    - Create compound index on client and created_at
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 13.6_

  - [x] 5.2 Add cancellation index to \_ensure_indexes()
    - Create index on cancelled field in jobs collection
    - _Requirements: 13.3_

- [x] 6. Implement pagination and filtering database methods

  - [x] 6.1 Implement get_projects_paginated() method in DatabaseManager

    - Accept query_filter, sort_by, sort_order, limit, offset parameters
    - Build MongoDB aggregation pipeline with $match for filters
    - Add $addFields stage to handle null values in sorting
    - Add $sort stage with primary and secondary sort
    - Add $skip and $limit stages for pagination
    - Use collation for case-insensitive text sorting
    - Execute aggregation and return results with total count
    - _Requirements: 3.11, 5.9, 5.10, 5.11, 5.12, 13.1, 13.6_

  - [x] 6.2 Write unit tests for get_projects_paginated()
    - Test pagination with various limit and offset values
    - Test sorting by each field (created_at, date, name, client)
    - Test ascending and descending sort orders
    - Test null value handling in sorting
    - Test filtering by search, client, and tags
    - Test combined filters
    - _Requirements: 5.8, 5.9, 10.2, 10.3, 10.4, 10.5, 10.6_

- [x] 7. Enhance GET /projects/ endpoint with pagination and filtering

  - [x] 7.1 Add query parameters to get_all_projects()

    - Add limit parameter with validation (1-100, default 50)
    - Add offset parameter with validation (>=0, default 0)
    - Add sort_by parameter with validation (created_at, date, name, client)
    - Add sort_order parameter with validation (asc, desc, default desc)
    - Add search parameter (optional)
    - Add client parameter (optional)
    - Add tags parameter (optional, comma-separated)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 5.1, 5.2, 5.3, 5.5, 5.6, 10.1, 10.2, 10.3, 10.4_

  - [x] 7.2 Implement query filter building

    - Build search filter with $or on name and description (case-insensitive regex)
    - Build client filter with exact match (case-insensitive)
    - Build tags filter with $in operator (OR logic)
    - Combine filters with AND logic
    - Handle invalid parameters gracefully
    - _Requirements: 10.2, 10.3, 10.4, 10.5, 10.6, 10.10_

  - [x] 7.3 Update response structure with pagination metadata

    - Call DB.get_projects_paginated() with filters and pagination params
    - Calculate has_more flag using (offset + limit) < total
    - Return response with Projects array and pagination object
    - Maintain backward compatibility by keeping Projects in same location
    - _Requirements: 3.7, 3.8, 3.9, 3.10, 4.1, 4.2, 4.3, 4.4, 4.5, 9.1, 9.4, 9.5_

  - [x] 7.4 Add validation error handling

    - Return 400 for invalid sort_by values
    - Return 400 for invalid sort_order values
    - Return 400 for invalid limit values
    - Return 400 for invalid offset values
    - Include helpful error messages
    - _Requirements: 3.3, 3.4, 3.5, 3.6, 5.4, 5.7_

  - [x] 7.5 Write integration tests for enhanced projects endpoint
    - Test pagination with multiple pages
    - Test search functionality
    - Test client filtering
    - Test tags filtering
    - Test combined filters with pagination
    - Test sorting by different fields
    - Test backward compatibility (no query params)
    - _Requirements: 9.1, 10.7, 10.9_

- [x] 8. Implement statistics endpoint

  - [x] 8.1 Create routes/stats.py file

    - Create APIRouter with /stats prefix
    - Add Statistics tag
    - _Requirements: 11.1_

  - [x] 8.2 Implement get_statistics() method in DatabaseManager

    - Count total projects using count_documents()
    - Calculate total points using aggregation pipeline with $sum
    - Handle null point_count values (treat as 0)
    - Count active jobs (status in pending, processing)
    - Count completed jobs in last 24 hours
    - Count failed jobs in last 24 hours
    - Return dictionary with all statistics
    - _Requirements: 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 13.7_

  - [x] 8.3 Implement GET /stats endpoint

    - Call DB.get_statistics()
    - Add timestamp to response
    - Handle errors with 500 status
    - Add logging
    - _Requirements: 11.1, 11.2_

  - [x] 8.4 Write unit tests for statistics
    - Test total projects calculation
    - Test total points calculation with null values
    - Test active jobs count
    - Test 24-hour job counts
    - Test with empty database
    - _Requirements: 11.4, 11.5, 11.6, 11.7, 11.8, 11.9_

- [x] 9. Register new routes in main.py

  - Import stats_router from routes.stats
  - Add app.include_router(stats_router) after existing routers
  - Verify all routes are registered correctly
  - _Requirements: 11.1_

- [x] 10. Update API documentation

  - Update main.py endpoints dict to include cancellation endpoint
  - Add stats endpoint to endpoints dict in root response
  - Update OpenAPI description with new features (job cancellation, pagination, statistics)
  - _Requirements: 1.1, 11.1_

- [x] 11. Performance testing and optimization

  - [x] 11.1 Test pagination performance with large dataset

    - Create 10,000 test projects
    - Measure query time for various offsets
    - Verify query time is under 200ms
    - Check index usage with explain()
    - _Requirements: 12.5, 13.1_

  - [x] 11.2 Test search performance

    - Test search with various patterns
    - Measure query time
    - Verify text index usage
    - Ensure query time is under 200ms
    - _Requirements: 10.8, 12.5_

  - [x] 11.3 Test statistics performance

    - Test with large dataset
    - Measure aggregation time
    - Verify query time is under 500ms
    - Check aggregation pipeline efficiency
    - _Requirements: 11.10, 11.11, 13.7_

  - [x] 11.4 Test cancellation check overhead
    - Measure time for is_job_cancelled() call
    - Verify overhead is under 100ms
    - Test with various database loads
    - _Requirements: 13.3, 13.4_

- [x] 12. End-to-end integration testing

  - [x] 12.1 Test complete cancellation workflow

    - Start a job
    - Cancel it during processing
    - Verify worker stops processing
    - Verify cleanup occurred
    - Verify status updated correctly
    - _Requirements: 1.1-1.10, 2.1-2.10_

  - [x] 12.2 Test pagination with search and filters

    - Create diverse test dataset
    - Test various filter combinations
    - Verify pagination works correctly with filters
    - Test sorting with filters
    - _Requirements: 3.1-3.11, 10.1-10.10_

  - [x] 12.3 Test backward compatibility
    - Call /projects/ without query parameters
    - Verify response structure unchanged
    - Verify pagination metadata is added
    - Test with existing API clients
    - _Requirements: 9.1-9.5_
