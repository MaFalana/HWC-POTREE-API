from fastapi import APIRouter, File, UploadFile, Form # Import the APIRouter class from fastapi
from storage.db import DatabaseManager # Import classes from MangaManager.py
from typing import Optional, List
from datetime import datetime
import json
import logging

from models.Project import Project, ProjectResponse, Location, CRS

from config.main import DB

logger = logging.getLogger(__name__)


 # Initialize the database manager
#DB.getProject() # Test the database connection


# Projects ROUTER

project_router = APIRouter(
    prefix="/projects", # Set the prefix of the router
    tags=["Projects"], # Set the tag of the router
    responses={404: {"description": "Not found"}}, # Set the 404 response
) # Initialize the router


def parse_tags(raw: Optional[str]) -> List[str]:
    """
    Accepts tags from a form either as:
      - 'FIELD, LOI'
      - '["FIELD", "LOI"]'
      - None
    Returns a clean list of strings.
    """
    if not raw:
        return []
    s = raw.strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            return [str(t).strip() for t in arr if str(t).strip()]
        except Exception:
            pass
    return [t.strip() for t in s.split(",") if t.strip()]

@project_router.get(
    '/',
    summary="List all projects",
    description="Retrieve a list of all projects with their metadata, including point cloud URLs, thumbnails, and location data. Supports pagination, sorting, and filtering.",
    response_description="List of projects with metadata and pagination information"
)
async def get_all_projects(
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    search: Optional[str] = None,
    client: Optional[str] = None,
    tags: Optional[str] = None
):
    """
    List all projects in the database with pagination, sorting, and filtering support.
    
    Returns a list of projects with their complete metadata including:
    - Project identification (id, name, client)
    - Point cloud data (cloud URL, CRS, location)
    - Thumbnails and visualization data
    - Timestamps (created_at, updated_at)
    
    **Query Parameters:**
    - **limit** (optional): Number of projects per page (1-100, default: 50)
    - **offset** (optional): Number of projects to skip (default: 0)
    - **sort_by** (optional): Field to sort by - created_at, date, name, client (default: created_at)
    - **sort_order** (optional): Sort order - asc or desc (default: desc)
    - **search** (optional): Search term for project name and description (case-insensitive)
    - **client** (optional): Filter by client name (case-insensitive exact match)
    - **tags** (optional): Comma-separated list of tags to filter by (OR logic)
    
    **Example Response:**
    ```json
    {
      "Message": "Successfully retrieved a list of projects from database",
      "Projects": [
        {
          "_id": "XXXX-XXX-A",
          "name": "Project Name",
          "client": "Client Name",
          "cloud": "https://storage.blob.core.windows.net/...",
          "thumbnail": "https://storage.blob.core.windows.net/..."
        }
      ],
      "pagination": {
        "total": 150,
        "limit": 50,
        "offset": 0,
        "has_more": true
      }
    }
    ```
    """
    from fastapi import HTTPException, Query
    
    logger.info(f"Retrieving projects with pagination (limit={limit}, offset={offset}, sort_by={sort_by}, sort_order={sort_order})")
    
    try:
        # Validate limit parameter
        if limit < 1:
            raise HTTPException(status_code=400, detail="limit must be at least 1")
        if limit > 100:
            limit = 100  # Cap at 100
        
        # Validate offset parameter
        if offset < 0:
            raise HTTPException(status_code=400, detail="offset cannot be negative")
        
        # Validate sort_by parameter
        valid_sort_fields = ["created_at", "date", "name", "client"]
        if sort_by not in valid_sort_fields:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid sort_by field. Allowed: {', '.join(valid_sort_fields)}"
            )
        
        # Validate sort_order parameter
        valid_sort_orders = ["asc", "desc"]
        if sort_order not in valid_sort_orders:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort_order. Allowed: {', '.join(valid_sort_orders)}"
            )
        
        # Build query filter
        query_filter = {}
        
        # Add search filter (case-insensitive partial match on name and description)
        if search:
            search_pattern = {"$regex": search, "$options": "i"}
            query_filter["$or"] = [
                {"name": search_pattern},
                {"description": search_pattern}
            ]
        
        # Add client filter (case-insensitive exact match)
        if client:
            query_filter["client"] = {"$regex": f"^{client}$", "$options": "i"}
        
        # Add tags filter (OR logic - match any of the provided tags)
        if tags:
            tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
            if tags_list:
                query_filter["tags"] = {"$in": tags_list}
        
        # Get paginated projects from database
        result = DB.get_projects_paginated(
            query_filter=query_filter,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset
        )
        
        projects = result['projects']
        total = result['total']
        
        # Calculate has_more flag
        has_more = (offset + limit) < total
        
        logger.info(f"Retrieved {len(projects)} projects (total: {total}, has_more: {has_more})")

        data = {
            "Message": "Successfully retrieved a list of projects from database",
            'Projects': projects,
            'pagination': {
                'total': total,
                'limit': limit,
                'offset': offset,
                'has_more': has_more
            }
        }

        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve projects: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve projects from database")

@project_router.post(
    '/upload',
    status_code=201,
    summary="Create a new project",
    description="Create a new project with metadata. The project ID must be unique.",
    response_description="Created project with metadata and timestamps"
)
async def upload_project(
    id: str = Form(..., description="Unique project identifier (e.g., 'XXXX-XXX-A')"),
    crs_id: str = Form(..., description="EPSG code (e.g., '26916')"),
    crs_name: str = Form(..., description="CRS human-readable name (e.g., 'NAD83 UTM Zone 16N')"),
    crs_proj4: str = Form(..., description="Proj4 string for coordinate system"),
    name: Optional[str] = Form(None, description="Project name"),
    client: Optional[str] = Form(None, description="Client name"),
    date: Optional[datetime] = Form(None, description="Project date"),
    description: Optional[str] = Form(None, description="Project description"),
    tags: Optional[list] = Form([], description="Project tags (comma-separated or JSON array)")
):
    """
    Create a new project in the database.
    
    The project ID must be unique. If a project with the same ID already exists,
    a 409 Conflict error will be returned.
    
    **Form Parameters:**
    - **id** (required): Unique project identifier (e.g., "XXXX-XXX-A")
    - **crs_proj4** (required): Proj4 string for the coordinate system (used by PotreeConverter)
    - **name** (optional): Human-readable project name
    - **client** (optional): Client or organization name
    - **date** (optional): Project date (ISO 8601 format)
    - **description** (optional): Detailed project description
    - **tags** (optional): Tags as comma-separated string or JSON array
    - **crs_id** (optional): EPSG code for reference (e.g., "26916")
    
    **Example Request:**
    ```
    POST /projects/upload
    Content-Type: multipart/form-data
    
    id=XXXX-XXX-A
    crs_proj4=+proj=utm +zone=16 +datum=NAD83 +units=m +no_defs
    name=Highway Survey Project
    client=DOT
    crs_id=26916
    ```
    
    **Returns:**
    - 201: Project created successfully
    - 409: Project with this ID already exists
    - 500: Server error
    """
    from fastapi import HTTPException
    
    logger.info(f"Creating new project with id: {id}")

    try:
        cleaned_tags = parse_tags(tags)

        newProject = Project()
        newProject.id = id
        newProject.name = name
        newProject.client = client
        newProject.date = date
        newProject.description = description
        newProject.tags = cleaned_tags
        newProject.location = Location()
        newProject.crs = CRS(_id=crs_id, name=crs_name, proj4=crs_proj4)

        await DB.addProject(newProject)
        
        logger.info(f"Successfully created project: {id}")

        data = {
            "Message": "Successfully uploaded project to database",
            "Project": newProject,
            "ID": newProject.id,
            "Uploaded": datetime.now()
        }
        return data
    except Exception as e:
        logger.error(f"Failed to create project {id}: {e}", exc_info=True)
        # Check if it's a duplicate key error (project already exists)
        if "duplicate" in str(e).lower() or "E11000" in str(e):
            raise HTTPException(status_code=409, detail=f"Project with id {id} already exists")
        raise HTTPException(status_code=500, detail="Failed to create project")

    

@project_router.post(
    '/{id}/refresh-urls',
    summary="Refresh SAS URLs",
    description="Regenerate expired SAS URLs for project cloud and thumbnail without re-processing.",
    response_description="Updated project with fresh SAS URLs"
)
async def refresh_project_urls(id: str):
    """
    Refresh expired SAS URLs for a project.
    
    When SAS URLs expire (after 30 days), use this endpoint to generate
    fresh URLs without re-processing the point cloud data.
    
    **Path Parameters:**
    - **id**: Project identifier
    
    **Returns:**
    - 200: URLs refreshed successfully
    - 404: Project not found or no files to refresh
    - 500: Server error
    
    **Example Response:**
    ```json
    {
      "message": "SAS URLs refreshed successfully",
      "project_id": "PROJ-001",
      "cloud": "https://storage.blob.core.windows.net/.../metadata.json?sv=...",
      "thumbnail": "https://storage.blob.core.windows.net/.../thumbnail.png?sv=..."
    }
    ```
    
    **Use Cases:**
    - SAS URLs expired and point cloud won't load
    - Sharing project with new expiration time
    - Extending access without re-uploading
    """
    from fastapi import HTTPException
    
    logger.info(f"Refreshing SAS URLs for project: {id}")
    
    try:
        # Check if project exists
        project = DB.getProject({'_id': id})
        if not project:
            logger.warning(f"Project not found: {id}")
            raise HTTPException(status_code=404, detail=f"Project with id {id} not found")
        
        # Check if project has files to refresh
        metadata_blob = f"{id}/metadata.json"
        thumbnail_blob = f"{id}/thumbnail.png"
        
        refreshed_urls = {}
        
        # Try to refresh cloud URL (metadata.json)
        try:
            new_cloud_url = DB.az.generate_sas_url(metadata_blob, hours_valid=720)  # 30 days
            project.cloud = new_cloud_url
            refreshed_urls['cloud'] = new_cloud_url
            logger.info(f"Refreshed cloud URL for project {id}")
        except Exception as e:
            logger.warning(f"Could not refresh cloud URL for {id}: {e}")
        
        # Try to refresh thumbnail URL
        try:
            new_thumbnail_url = DB.az.generate_sas_url(thumbnail_blob, hours_valid=720)  # 30 days
            project.thumbnail = new_thumbnail_url
            refreshed_urls['thumbnail'] = new_thumbnail_url
            logger.info(f"Refreshed thumbnail URL for project {id}")
        except Exception as e:
            logger.warning(f"Could not refresh thumbnail URL for {id}: {e}")
        
        if not refreshed_urls:
            raise HTTPException(
                status_code=404, 
                detail=f"No files found to refresh for project {id}. Project may not have been processed yet."
            )
        
        # Update project in database
        DB.updateProject(project)
        
        response = {
            "message": "SAS URLs refreshed successfully",
            "project_id": id,
            **refreshed_urls
        }
        
        logger.info(f"Successfully refreshed {len(refreshed_urls)} URL(s) for project {id}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to refresh URLs for project {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to refresh SAS URLs")


@project_router.get(
    '/{id}',
    response_model=Project,
    summary="Get project by ID",
    description="Retrieve a specific project by its unique identifier.",
    response_description="Project details with all metadata"
)
async def get_project(id: str):
    """
    Get a specific project by its ID.
    
    Returns complete project information including point cloud URLs,
    thumbnails, location data, and processing status.
    
    **Path Parameters:**
    - **id**: Project identifier (e.g., "XXXX-XXX-A")
    
    **Returns:**
    - 200: Project found and returned
    - 404: Project not found
    - 500: Server error
    
    **Example Response:**
    ```json
    {
      "_id": "XXXX-XXX-A",
      "name": "Highway Survey Project",
      "client": "DOT",
      "cloud": "https://storage.blob.core.windows.net/.../viewer.html",
      "thumbnail": "https://storage.blob.core.windows.net/.../thumbnail.png",
      "location": {
        "lat": 40.7128,
        "lon": -74.0060,
        "z": 10.5
      },
      "crs": {
        "_id": "EPSG:26916",
        "name": "NAD83 / UTM zone 16N"
      },
      "point_count": 1500000
    }
    ```
    """
    from fastapi import HTTPException
    
    logger.info(f"Retrieving project: {id}")
    
    try:
        data = DB.getProject({'_id': id})
        
        if not data:
            logger.warning(f"Project not found: {id}")
            raise HTTPException(status_code=404, detail=f"Project with id {id} not found")
        
        logger.info(f"Retrieved project: {id}")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve project {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve project from database")

@project_router.put(
    '/{id}/update',
    summary="Update project metadata",
    description="Update project metadata. Accepts partial updates - only provided fields will be updated.",
    response_description="Confirmation message"
)
async def update_project(
    id: str,
    name: Optional[str] = Form(None, description="Updated project name"),
    client: Optional[str] = Form(None, description="Updated client name"),
    date: Optional[datetime] = Form(None, description="Updated project date"),
    description: Optional[str] = Form(None, description="Updated description"),
    tags: Optional[str] = Form(None, description="Updated tags (comma-separated or JSON array)")
):
    """
    Update project metadata.
    
    This endpoint accepts partial updates - you only need to provide the fields
    you want to update. Other fields will remain unchanged.
    
    **Path Parameters:**
    - **id**: Project identifier
    
    **Form Parameters (all optional):**
    - **name**: Updated project name
    - **client**: Updated client name
    - **date**: Updated project date
    - **description**: Updated description
    - **tags**: Updated tags
    
    **Example Request:**
    ```
    PUT /projects/XXXX-XXX-A/update
    Content-Type: multipart/form-data
    
    name=Updated Project Name
    tags=survey,lidar,updated
    ```
    
    **Returns:**
    - 200: Project updated successfully
    - 404: Project not found
    - 500: Server error
    """
    from fastapi import HTTPException
    
    logger.info(f"Updating project: {id}")
    
    try:
        # Check if project exists
        project = DB.getProject({'_id': id})
        if not project:
            logger.warning(f"Project not found for update: {id}")
            raise HTTPException(status_code=404, detail=f"Project with id {id} not found")
        
        # Update only provided fields
        if name is not None:
            project.name = name
        if client is not None:
            project.client = client
        if date is not None:
            project.date = date
        if description is not None:
            project.description = description
        if tags is not None:
            project.tags = parse_tags(tags)
        
        DB.updateProject(project)
        logger.info(f"Successfully updated project: {id}")
        
        data = {
            "Message": f"Updated project with id {id}",
            "description": project.description
        }
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update project {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update project")



@project_router.delete(
    '/delete',
    summary="Batch delete projects",
    description="Delete multiple projects and all associated files from Azure Blob Storage.",
    response_description="Batch deletion results with success and failure details"
)
async def batch_delete_projects(project_ids: List[str]):
    """
    Delete multiple projects and all associated files in a single request.
    
    This operation will:
    1. Delete each project record from MongoDB
    2. Delete all associated files from Azure Blob Storage (point clouds, thumbnails, etc.)
    3. Return detailed results for each project (success or failure)
    
    **Warning:** This operation cannot be undone.
    
    **Request Body:**
    ```json
    ["PROJ-001", "PROJ-002", "PROJ-003"]
    ```
    
    **Returns:**
    - 200: Batch deletion completed (check response for individual results)
    - 400: Invalid request (empty array, invalid format)
    - 500: Server error
    
    **Example Response:**
    ```json
    {
      "message": "Batch deletion completed",
      "deleted": ["PROJ-001", "PROJ-002"],
      "failed": [
        {
          "id": "PROJ-003",
          "error": "Project not found"
        }
      ],
      "deleted_count": 2,
      "failed_count": 1,
      "total": 3
    }
    ```
    
    **Behavior:**
    - Continues processing even if individual deletions fail
    - Returns detailed results for each project
    - Does not rollback successful deletions if later ones fail
    """
    from fastapi import HTTPException
    
    logger.info(f"Batch delete requested for {len(project_ids)} projects")
    
    # Validate input
    if not project_ids or len(project_ids) == 0:
        raise HTTPException(status_code=400, detail="project_ids array cannot be empty")
    
    if len(project_ids) > 100:
        raise HTTPException(status_code=400, detail="Cannot delete more than 100 projects at once")
    
    deleted = []
    failed = []
    
    for project_id in project_ids:
        try:
            # Check if project exists
            project = DB.getProject({'_id': project_id})
            if not project:
                logger.warning(f"Project not found for deletion: {project_id}")
                failed.append({
                    "id": project_id,
                    "error": "Project not found"
                })
                continue
            
            # Delete project and associated files
            DB.deleteProject(project_id)
            deleted.append(project_id)
            logger.info(f"Successfully deleted project: {project_id}")
            
        except Exception as e:
            logger.error(f"Failed to delete project {project_id}: {e}", exc_info=True)
            error_msg = "Failed to delete project"
            if "azure" in str(e).lower() or "blob" in str(e).lower():
                error_msg = "Failed to delete project files from Azure storage"
            
            failed.append({
                "id": project_id,
                "error": error_msg
            })
    
    response = {
        "message": "Batch deletion completed",
        "deleted": deleted,
        "failed": failed,
        "deleted_count": len(deleted),
        "failed_count": len(failed),
        "total": len(project_ids)
    }
    
    logger.info(f"Batch deletion completed: {len(deleted)} succeeded, {len(failed)} failed")
    return response


@project_router.post(
    '/{project_id}/ortho',
    status_code=202,
    summary="Upload orthophoto for project",
    description="Upload a GeoTIFF file for a project. The file will be converted to Cloud Optimized GeoTIFF (COG) format.",
    response_description="Job created for ortho conversion"
)
async def upload_ortho(
    project_id: str,
    file: UploadFile = File(...)
):
    """
    Upload an orthophoto (GeoTIFF) file for a project.
    
    The uploaded file will be:
    1. Validated for correct file extension (.tif or .tiff)
    2. Uploaded to Azure temporary storage
    3. Queued for conversion to Cloud Optimized GeoTIFF (COG) format
    4. Processed to generate a thumbnail preview
    5. Stored in the project's Azure storage location
    
    **Path Parameters:**
    - **project_id**: Project identifier
    
    **Form Parameters:**
    - **file**: GeoTIFF file (.tif or .tiff, max 30GB)
    
    **Returns:**
    - 202 Accepted: File uploaded and job created
    - 400 Bad Request: Invalid file type or missing file
    - 404 Not Found: Project not found
    - 413 Payload Too Large: File exceeds 30GB limit
    - 500 Internal Server Error: Server error
    
    **Example Response:**
    ```json
    {
      "message": "Ortho upload accepted for processing",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "project_id": "PROJ-001",
      "status": "pending",
      "created_at": "2024-01-15T10:30:00Z"
    }
    ```
    
    **Workflow:**
    1. Upload ortho using this endpoint
    2. Monitor job status using GET /jobs/{job_id}
    3. Access processed ortho from updated project (GET /projects/{id})
    
    **Notes:**
    - Uploading a new ortho will overwrite any existing ortho for the project
    - Processing time varies based on file size (typically 5-30 minutes)
    - The job can be cancelled using POST /jobs/{job_id}/cancel
    """
    from fastapi import HTTPException
    import uuid
    import tempfile
    import os
    
    logger.info(f"Ortho upload requested for project: {project_id}")
    
    try:
        # Validate project exists
        project = DB.getProject({'_id': project_id})
        if not project:
            logger.warning(f"Project not found for ortho upload: {project_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Project with id {project_id} not found"
            )
        
        # Validate file is provided
        if not file:
            raise HTTPException(
                status_code=400,
                detail="No file provided"
            )
        
        # Validate file extension
        filename = file.filename.lower()
        if not (filename.endswith('.tif') or filename.endswith('.tiff')):
            logger.warning(f"Invalid file extension for ortho upload: {filename}")
            raise HTTPException(
                status_code=400,
                detail="Invalid file type. Only .tif and .tiff files are supported"
            )
        
        # Validate file size (30GB limit)
        MAX_FILE_SIZE = 30 * 1024 * 1024 * 1024  # 30GB in bytes
        file.file.seek(0, 2)  # Seek to end of file
        file_size = file.file.tell()
        file.file.seek(0)  # Reset to beginning
        
        if file_size > MAX_FILE_SIZE:
            logger.warning(f"File too large for ortho upload: {file_size} bytes")
            raise HTTPException(
                status_code=413,
                detail=f"File size exceeds 30GB limit (uploaded: {file_size / (1024**3):.2f}GB)"
            )
        
        logger.info(f"Ortho file validated: {filename}, size: {file_size / (1024**3):.2f}GB")
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Save file to temporary location
        temp_file_path = os.path.join(tempfile.gettempdir(), f"{job_id}.tif")
        with open(temp_file_path, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)
        
        logger.info(f"Saved ortho file to temporary location: {temp_file_path}")
        
        # Upload to Azure temporary storage
        azure_blob_name = f"jobs/{job_id}.tif"
        DB.az.upload_file(temp_file_path, azure_blob_name)
        logger.info(f"Uploaded ortho to Azure: {azure_blob_name}")
        
        # Clean up temporary file
        os.remove(temp_file_path)
        
        # Create job in database
        from models.Job import Job
        
        job = Job(
            id=job_id,
            project_id=project_id,
            status="pending",
            file_path="",  # Will be set by worker
            azure_path=azure_blob_name,
            current_step="queued",
            progress_message="Ortho conversion job queued",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Add type field for ortho jobs
        job_dict = job._to_dict()
        job_dict['type'] = 'ortho_conversion'
        
        DB.jobsCollection.insert_one(job_dict)
        logger.info(f"Created ortho conversion job: {job_id}")
        
        response = {
            "message": "Ortho upload accepted for processing",
            "job_id": job_id,
            "project_id": project_id,
            "status": "pending",
            "created_at": job.created_at.isoformat()
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload ortho for project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to upload ortho file")


@project_router.delete(
    '/{id}/delete',
    summary="Delete project",
    description="Delete a project and all associated files from Azure Blob Storage.",
    response_description="Confirmation message"
)
async def delete_project(id: str):
    """
    Delete a project and all associated files.
    
    This operation will:
    1. Delete the project record from MongoDB
    2. Delete all associated files from Azure Blob Storage (point clouds, thumbnails, etc.)
    
    **Warning:** This operation cannot be undone.
    
    **Path Parameters:**
    - **id**: Project identifier
    
    **Returns:**
    - 200: Project deleted successfully
    - 404: Project not found
    - 500: Server error (including Azure storage errors)
    
    **Example Response:**
    ```json
    {
      "Message": "Deleted project with id XXXX-XXX-A"
    }
    ```
    """
    from fastapi import HTTPException
    
    logger.info(f"Deleting project: {id}")
    
    try:
        # Check if project exists
        project = DB.getProject({'_id': id})
        if not project:
            logger.warning(f"Project not found for deletion: {id}")
            raise HTTPException(status_code=404, detail=f"Project with id {id} not found")
        
        # Delete project and associated files
        DB.deleteProject(id)
        logger.info(f"Successfully deleted project: {id}")
        
        data = {
            "Message": f"Deleted project with id {id}"
        }
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete project {id}: {e}", exc_info=True)
        # Check if it's an Azure storage error
        if "azure" in str(e).lower() or "blob" in str(e).lower():
            raise HTTPException(status_code=500, detail="Failed to delete project files from Azure storage")
        raise HTTPException(status_code=500, detail="Failed to delete project")