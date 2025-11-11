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
    description="Retrieve a list of all projects with their metadata, including point cloud URLs, thumbnails, and location data.",
    response_description="List of projects with metadata"
)
async def get_all_projects():
    """
    List all projects in the database.
    
    Returns a list of all projects with their complete metadata including:
    - Project identification (id, name, client)
    - Point cloud data (cloud URL, CRS, location)
    - Thumbnails and visualization data
    - Timestamps (created_at, updated_at)
    
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
      "total": 1
    }
    ```
    """
    from fastapi import HTTPException
    
    logger.info("Retrieving all projects")
    
    try:
        projects = DB.getProjects({})
        
        logger.info(f"Retrieved {len(projects)} projects")

        data = {
            "Message": "Successfully retrieved a list of projects from database",
            'Projects': projects,
            'total': len(projects)
        }

        return data
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