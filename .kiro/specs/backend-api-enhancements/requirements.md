# Requirements Document

## Introduction

This document outlines the requirements for enhancing the HWC Potree API backend with job cancellation, pagination, search/filtering, and statistics capabilities. Currently, the API lacks the ability to cancel in-progress jobs, does not support pagination for project listings, has no search functionality, and requires clients to fetch all data to calculate statistics. These enhancements will improve user experience by allowing users to stop unwanted processing jobs, efficiently browse and search large project collections, and display dashboard statistics without performance overhead.

## Requirements

### Requirement 1: Job Cancellation Endpoint

**User Story:** As a user, I want to cancel a processing job that is taking too long or was started by mistake, so that I can free up system resources and avoid unnecessary processing costs.

#### Acceptance Criteria

1. WHEN a user requests job cancellation THEN the system SHALL provide a POST endpoint at `/jobs/{job_id}/cancel`
2. WHEN the cancellation endpoint is called THEN the system SHALL verify the job exists
3. IF the job does not exist THEN the system SHALL return 404 Not Found
4. WHEN a job with status "pending" is cancelled THEN the system SHALL update the status to "cancelled" immediately
5. WHEN a job with status "processing" is cancelled THEN the system SHALL set a cancellation flag that the worker checks
6. WHEN a job with status "completed" is cancelled THEN the system SHALL return 409 Conflict with message "Cannot cancel completed job"
7. WHEN a job with status "failed" is cancelled THEN the system SHALL return 409 Conflict with message "Cannot cancel failed job"
8. WHEN a job with status "cancelled" is cancelled THEN the system SHALL return 409 Conflict with message "Job already cancelled"
9. WHEN cancellation succeeds THEN the system SHALL return 200 OK with a confirmation message and updated job object
10. WHEN a job is cancelled THEN the response SHALL include the job_id, project_id, status, and updated_at timestamp

### Requirement 2: Worker Cancellation Handling

**User Story:** As a system administrator, I want the background worker to respect cancellation requests, so that cancelled jobs stop processing and clean up their resources properly.

#### Acceptance Criteria

1. WHEN the worker processes a job THEN it SHALL check for cancellation status before each major step (metadata, thumbnail, conversion, upload)
2. WHEN a cancellation flag is detected THEN the worker SHALL stop processing immediately
3. WHEN a job is cancelled during processing THEN the worker SHALL delete the local temporary file
4. WHEN a job is cancelled during processing THEN the worker SHALL delete the Azure job file at `jobs/{job_id}.laz`
5. WHEN a job is cancelled during processing THEN the worker SHALL delete any partial Potree output files from Azure
6. WHEN a job is cancelled during processing THEN the worker SHALL NOT update the project with partial results
7. WHEN a cancelled job cleanup completes THEN the worker SHALL update the job status to "cancelled" with updated_at timestamp
8. WHEN a cancelled job cleanup completes THEN the worker SHALL set completed_at to the current timestamp
9. IF cleanup fails THEN the worker SHALL log the error but still mark the job as cancelled
10. WHEN the worker detects cancellation THEN it SHALL log the cancellation event with job_id and current_step

### Requirement 3: Pagination for Project List Endpoint

**User Story:** As a user with many projects, I want the project list to load quickly and support pagination, so that I can browse my projects efficiently without long load times.

#### Acceptance Criteria

1. WHEN requesting projects THEN the GET `/projects/` endpoint SHALL accept optional query parameters `limit`, `offset`, `sort_by`, `sort_order`, `search`, `client`, and `tags`
2. WHEN `limit` is not provided THEN the system SHALL default to 50 projects per page
3. WHEN `limit` exceeds 100 THEN the system SHALL cap it at 100 and return a maximum of 100 projects
4. WHEN `limit` is less than 1 THEN the system SHALL return 400 Bad Request with message "limit must be at least 1"
5. WHEN `offset` is not provided THEN the system SHALL default to 0 (first page)
6. WHEN `offset` is negative THEN the system SHALL return 400 Bad Request with message "offset cannot be negative"
7. WHEN returning paginated results THEN the response SHALL include fields: `total`, `limit`, `offset`, and `Projects` array
8. WHEN calculating total THEN the system SHALL return the total count of projects matching the current filters
9. WHEN no projects exist THEN the system SHALL return an empty array with total: 0
10. WHEN the offset exceeds the total count THEN the system SHALL return an empty array with the correct total count
11. WHEN query parameters are combined THEN the system SHALL apply filters first, then sorting, then pagination

### Requirement 4: Pagination Response Structure

**User Story:** As a frontend developer, I want a consistent pagination response structure, so that I can easily implement pagination controls in the UI.

#### Acceptance Criteria

1. WHEN returning paginated projects THEN the response SHALL use the following structure:
   ```json
   {
     "Message": "Successfully retrieved projects",
     "Projects": [...],
     "pagination": {
       "total": 150,
       "limit": 50,
       "offset": 0,
       "has_more": true
     }
   }
   ```
2. WHEN there are more results available THEN `has_more` SHALL be true
3. WHEN the current page is the last page THEN `has_more` SHALL be false
4. WHEN calculating `has_more` THEN the system SHALL use the formula: `(offset + limit) < total`
5. WHEN the response includes pagination THEN all four fields (total, limit, offset, has_more) SHALL be present

### Requirement 5: Project Sorting

**User Story:** As a user, I want to sort projects by different fields (date, name, client) in ascending or descending order, so that I can organize and find projects according to my needs.

#### Acceptance Criteria

1. WHEN retrieving projects THEN the GET `/projects/` endpoint SHALL accept optional query parameters `sort_by` and `sort_order`
2. WHEN `sort_by` is not provided THEN the system SHALL default to sorting by `created_at`
3. WHEN `sort_by` is provided THEN it SHALL accept values: `created_at`, `date`, `name`, `client`
4. IF `sort_by` contains an invalid value THEN the system SHALL return 400 Bad Request with message "Invalid sort_by field. Allowed: created_at, date, name, client"
5. WHEN `sort_order` is not provided THEN the system SHALL default to `desc` (descending)
6. WHEN `sort_order` is provided THEN it SHALL accept values: `asc` (ascending) or `desc` (descending)
7. IF `sort_order` contains an invalid value THEN the system SHALL return 400 Bad Request with message "Invalid sort_order. Allowed: asc, desc"
8. WHEN sorting by `created_at` with `desc` THEN projects SHALL be ordered newest first
9. WHEN sorting by `name` or `client` THEN the system SHALL use case-insensitive alphabetical sorting
10. WHEN a project has a null value for the sort field THEN it SHALL be placed at the end of the results
11. WHEN implementing sorting THEN the system SHALL apply sorting before pagination
12. WHEN multiple projects have the same sort value THEN the system SHALL use `created_at desc` as a secondary sort for consistency

### Requirement 6: Job Cancellation Database Schema

**User Story:** As a developer, I want the Job model to support cancellation tracking, so that the system can properly manage cancelled jobs.

#### Acceptance Criteria

1. WHEN a job is created THEN it SHALL include a `cancelled` boolean field defaulting to false
2. WHEN a cancellation is requested THEN the system SHALL set `cancelled` to true
3. WHEN the worker checks for cancellation THEN it SHALL query the `cancelled` field from the database
4. WHEN a job is cancelled THEN the `status` field SHALL be updated to "cancelled"
5. WHEN querying jobs THEN the system SHALL support filtering by status including "cancelled"

### Requirement 7: Cancellation Cleanup Safety

**User Story:** As a system administrator, I want cancellation cleanup to be safe and idempotent, so that partial failures don't leave the system in an inconsistent state.

#### Acceptance Criteria

1. WHEN cleaning up a cancelled job THEN each cleanup operation SHALL be wrapped in try-catch blocks
2. IF deleting the local temp file fails THEN the system SHALL log the error and continue with other cleanup steps
3. IF deleting the Azure job file fails THEN the system SHALL log the error and continue with other cleanup steps
4. IF deleting partial Potree files fails THEN the system SHALL log the error and continue with other cleanup steps
5. WHEN all cleanup attempts complete THEN the job status SHALL be updated to "cancelled" regardless of individual failures
6. WHEN cleanup is idempotent THEN calling cleanup multiple times SHALL not cause errors

### Requirement 8: Cancellation API Response

**User Story:** As a frontend developer, I want detailed cancellation responses, so that I can provide appropriate feedback to users.

#### Acceptance Criteria

1. WHEN cancellation succeeds THEN the response SHALL include:
   ```json
   {
     "message": "Job cancelled successfully",
     "job_id": "uuid",
     "project_id": "XXXX-XXX-A",
     "status": "cancelled",
     "previous_status": "processing",
     "cancelled_at": "2025-11-12T10:00:00Z"
   }
   ```
2. WHEN cancellation fails due to invalid state THEN the response SHALL include the current job status
3. WHEN cancellation fails THEN the response SHALL include a user-friendly error message
4. WHEN returning the cancelled job THEN the system SHALL include the current_step that was interrupted

### Requirement 9: Backward Compatibility

**User Story:** As a system administrator, I want these enhancements to be backward compatible, so that existing API clients continue to work without modification.

#### Acceptance Criteria

1. WHEN pagination parameters are not provided THEN the API SHALL return the first 50 projects (maintaining current behavior)
2. WHEN the cancellation endpoint is added THEN existing endpoints SHALL remain unchanged
3. WHEN the Job model is updated THEN existing job queries SHALL continue to work
4. WHEN the response structure changes THEN the `Projects` array SHALL remain in the same location for backward compatibility
5. IF a client does not use pagination THEN they SHALL receive the same response structure as before (with added pagination metadata)

### Requirement 10: Project Search and Filtering

**User Story:** As a user, I want to search and filter projects by name, client, and tags, so that I can quickly find specific projects without scrolling through long lists.

#### Acceptance Criteria

1. WHEN searching projects THEN the GET `/projects/` endpoint SHALL accept optional query parameters `search`, `client`, and `tags`
2. WHEN `search` is provided THEN the system SHALL perform case-insensitive partial matching on project `name` and `description` fields
3. WHEN `client` is provided THEN the system SHALL filter projects where the client field matches exactly (case-insensitive)
4. WHEN `tags` is provided THEN it SHALL accept a comma-separated list of tags (e.g., "FIELD,LOI")
5. WHEN filtering by tags THEN the system SHALL return projects that contain ANY of the specified tags (OR logic)
6. WHEN multiple filters are provided THEN the system SHALL apply AND logic (e.g., search AND client AND tags)
7. WHEN search returns no results THEN the system SHALL return an empty array with total: 0
8. WHEN implementing search THEN the system SHALL use database-level text search or regex (not in-memory filtering)
9. WHEN combining search with pagination THEN the system SHALL apply filters first, then sorting, then pagination
10. WHEN search/filter parameters are invalid THEN the system SHALL ignore them and return all projects (graceful degradation)

### Requirement 11: Statistics Endpoint

**User Story:** As a user, I want to see dashboard statistics (total projects, total points, active jobs) without fetching all project data, so that my dashboard loads quickly and efficiently.

#### Acceptance Criteria

1. WHEN requesting statistics THEN the system SHALL provide a GET endpoint at `/stats`
2. WHEN the statistics endpoint is called THEN it SHALL return aggregated data without requiring authentication
3. WHEN calculating statistics THEN the response SHALL include the following fields:
   ```json
   {
     "total_projects": 150,
     "total_points": 45000000,
     "active_jobs": 3,
     "completed_jobs_24h": 12,
     "failed_jobs_24h": 1
   }
   ```
4. WHEN calculating `total_projects` THEN the system SHALL count all projects in the database
5. WHEN calculating `total_points` THEN the system SHALL sum the `point_count` field across all projects
6. IF a project has null `point_count` THEN it SHALL be treated as 0 in the sum
7. WHEN calculating `active_jobs` THEN the system SHALL count jobs with status "pending" or "processing"
8. WHEN calculating `completed_jobs_24h` THEN the system SHALL count jobs completed in the last 24 hours
9. WHEN calculating `failed_jobs_24h` THEN the system SHALL count jobs that failed in the last 24 hours
10. WHEN implementing statistics THEN the system SHALL use efficient database aggregation queries
11. WHEN the statistics endpoint is called THEN it SHALL complete in less than 500ms even with thousands of projects

### Requirement 12: Search Performance and Indexing

**User Story:** As a system administrator, I want search and filtering to be performant, so that users get instant results even with large datasets.

#### Acceptance Criteria

1. WHEN implementing search THEN the system SHALL create database indexes on `name`, `client`, and `tags` fields
2. WHEN searching by name THEN the query SHALL use a case-insensitive index if available
3. WHEN filtering by client THEN the query SHALL use an exact match index
4. WHEN filtering by tags THEN the query SHALL use an array index for efficient lookups
5. WHEN search queries execute THEN they SHALL complete in less than 200ms for databases with up to 10,000 projects
6. WHEN combining multiple filters THEN the system SHALL use compound indexes where beneficial

### Requirement 13: Performance Optimization

**User Story:** As a system administrator, I want all enhancements to be performant, so that the API remains responsive under load.

#### Acceptance Criteria

1. WHEN implementing pagination THEN the system SHALL use database-level limit and offset (not in-memory filtering)
2. WHEN counting total projects THEN the system SHALL use an efficient count query
3. WHEN checking for cancellation THEN the worker SHALL query only the `cancelled` field (not the entire job document)
4. WHEN implementing cancellation checks THEN the overhead SHALL be less than 100ms per check
5. WHEN deleting Azure blobs THEN the system SHALL use batch operations when possible
6. WHEN querying projects THEN the system SHALL use appropriate database indexes on `created_at`, `name`, `client`, and `tags` fields
7. WHEN calculating statistics THEN the system SHALL use database aggregation pipelines (not fetching all documents)
