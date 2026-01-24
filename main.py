from config.main import *

from routes.projects import project_router
from routes.process import process_router
from routes.jobs import jobs_router
from routes.stats import stats_router
from worker import JobWorker
import threading
import logging
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Azure captures stdout/stderr
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="HWC Potree API",
    version="2.0.0",
    description="""
    A FastAPI-based backend service for processing LiDAR point cloud data (LAS/LAZ files) 
    and converting them to Potree format for web-based 3D visualization, plus orthophoto 
    (georeferenced raster) upload and PNG overlay conversion with Leaflet bounds.
    
    **File Size Limits:**
    - Maximum upload size: 30GB (Potree handles downsampling automatically)
    - Recommended: Files under 10GB for faster processing
    
    ## Features
    
    * **Project Management**: Create, read, update, and delete projects with metadata
    * **Pagination & Search**: Efficient browsing with pagination, sorting, and filtering by name, client, and tags
    * **Background Processing**: Asynchronous point cloud processing with job tracking
    * **Job Cancellation**: Cancel in-progress jobs to free up system resources
    * **Metadata Extraction**: Automatic extraction of CRS, location, and point count
    * **Thumbnail Generation**: Automatic preview image generation from point clouds and orthophotos
    * **Potree Conversion**: Convert LAS/LAZ files to web-viewable Potree format
    * **Orthophoto Upload**: Upload georeferenced rasters and convert to PNG overlays with Leaflet bounds
    * **Statistics Dashboard**: Real-time statistics on projects, points, and job status
    * **Azure Integration**: Seamless integration with Azure Blob Storage
    * **Health Monitoring**: Built-in health checks for production monitoring
    
    ## Workflow
    
    ### Point Cloud Workflow:
    1. Create a project using `POST /projects/upload`
    2. Upload a point cloud file using `POST /process/{id}/potree`
    3. Monitor job status using `GET /jobs/{job_id}`
    4. Cancel a job if needed using `POST /jobs/{job_id}/cancel`
    5. Access processed data from the updated project
    
    ### Orthophoto Workflow:
    1. Create or use an existing project
    2. Upload an orthophoto using `POST /projects/{project_id}/ortho`
    3. Monitor job status using `GET /jobs/{job_id}`
    4. Access processed COG and thumbnail from the updated project
    
    ### General:
    - Browse projects with pagination and filters using `GET /projects/`
    - View dashboard statistics using `GET /stats`
    
    ## Authentication
    
    Currently, this API does not require authentication (internal use only).
    
    ## Rate Limits
    
    No rate limits are currently enforced.
    
    ## Support
    
    For issues or questions, please contact the development team.
    """,
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    swagger_ui_favicon_url="/assets/favicon.ico",
    contact={
        "name": "HWC Development Team",
        "url": "https://github.com/MaFalana/HWC-POTREE-API",
    },
    license_info={
        "name": "Proprietary",
    }
)

app.include_router(project_router) # Include the routers in the app
app.include_router(process_router)
app.include_router(jobs_router)
app.include_router(stats_router)

# and enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global Exception Handlers

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    """
    Handle FastAPI request validation errors.
    
    Returns 400 Bad Request with detailed validation error information.
    """
    logger.warning(f"Validation error on {request.url.path}: {exc.errors()}")
    
    return JSONResponse(
        status_code=400,
        content={
            "error": "Validation Error",
            "message": "Invalid request data",
            "details": exc.errors(),
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(ValidationError)
async def pydantic_validation_exception_handler(request, exc: ValidationError):
    """
    Handle Pydantic validation errors.
    
    Returns 400 Bad Request with detailed validation error information.
    """
    logger.warning(f"Pydantic validation error on {request.url.path}: {exc.errors()}")
    
    return JSONResponse(
        status_code=400,
        content={
            "error": "Validation Error",
            "message": "Invalid data format",
            "details": exc.errors(),
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    """
    Handle all unhandled exceptions.
    
    Returns 500 Internal Server Error and logs the full stack trace.
    """
    logger.error(
        f"Unhandled exception on {request.method} {request.url.path}: {exc}",
        exc_info=True
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred. Please try again later.",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.on_event("startup")
async def startup_event():
    """
    FastAPI startup event handler.
    
    This function runs when the application starts and:
    1. Resets any stale "processing" jobs to "pending" status
    2. Starts the background worker thread
    """
    logger.info("Application startup: Initializing background worker")
    
    # Reset stale jobs that were processing when the app shut down
    try:
        result = DB.jobsCollection.update_many(
            {"status": "processing"},
            {"$set": {"status": "pending", "updated_at": datetime.utcnow()}}
        )
        if result.modified_count > 0:
            logger.info(f"Reset {result.modified_count} stale 'processing' jobs to 'pending'")
    except Exception as e:
        logger.error(f"Failed to reset stale jobs: {e}", exc_info=True)
    
    # Start the worker thread as a daemon
    try:
        worker = JobWorker(DB, poll_interval=5)
        worker_thread = threading.Thread(target=worker.start, daemon=True, name="JobWorker")
        worker_thread.start()
        logger.info("Background worker thread started successfully")
    except Exception as e:
        logger.error(f"Failed to start worker thread: {e}", exc_info=True)


@app.get(
    '/',
    summary="API root",
    description="Get basic API information and available endpoints.",
    tags=["Root"]
)
def root():
    """
    API root endpoint.
    
    Returns basic information about the API and links to documentation.
    
    **Example Response:**
    ```json
    {
      "name": "HWC Potree API",
      "version": "2.0.0",
      "description": "Point cloud processing and Potree conversion service",
      "documentation": "/docs",
      "health": "/health"
    }
    ```
    """
    data = {
        "name": "HWC Potree API",
        "version": "2.0.0",
        "description": "Point cloud processing and Potree conversion service",
        "framework": "FastAPI",
        "documentation": "/docs",
        "redoc": "/redoc",
        "openapi": "/openapi.json",
        "health": "/health",
        "endpoints": {
            "projects": "/projects/",
            "process": "/process/{id}/potree",
            "upload_ortho": "/projects/{project_id}/ortho",
            "jobs": "/jobs/{job_id}",
            "cancel_job": "/jobs/{job_id}/cancel",
            "statistics": "/stats"
        }
    }

    return data


@app.get(
    '/health',
    summary="Health check",
    description="Check the health status of the API and its dependencies.",
    response_description="Service health status",
    tags=["Health"]
)
async def health_check():
    """
    Health check endpoint for monitoring and Azure health probes.
    
    This endpoint verifies connectivity to critical services:
    - **MongoDB**: Database connection and availability
    - **Azure Blob Storage**: Storage connection and container access
    
    **Use Cases:**
    - Azure Container Apps health probes
    - Monitoring and alerting systems
    - Deployment verification
    - Troubleshooting connectivity issues
    
    **Returns:**
    - 200 OK: All services are healthy
    - 503 Service Unavailable: One or more services are unhealthy
    
    **Example Response (Healthy):**
    ```json
    {
      "status": "healthy",
      "timestamp": "2025-11-09T10:00:00Z",
      "services": {
        "mongodb": "connected",
        "azure_blob": "connected"
      }
    }
    ```
    
    **Example Response (Unhealthy):**
    ```json
    {
      "status": "unhealthy",
      "timestamp": "2025-11-09T10:00:00Z",
      "services": {
        "mongodb": "error: connection timeout",
        "azure_blob": "connected"
      }
    }
    ```
    
    **Docker Health Check:**
    The Dockerfile includes a health check that calls this endpoint:
    ```dockerfile
    HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
      CMD python -c "import requests; requests.get('http://localhost:8000/health')"
    ```
    """
    from fastapi.responses import JSONResponse
    
    services = {}
    is_healthy = True
    
    # Check MongoDB connection
    try:
        DB.client.server_info()
        services['mongodb'] = 'connected'
        logger.info("Health check: MongoDB connection OK")
    except Exception as e:
        services['mongodb'] = f'error: {str(e)}'
        is_healthy = False
        logger.error(f"Health check: MongoDB connection failed: {e}")
    
    # Check Azure Blob Storage connection
    try:
        DB.az.container_client.get_container_properties()
        services['azure_blob'] = 'connected'
        logger.info("Health check: Azure Blob Storage connection OK")
    except Exception as e:
        services['azure_blob'] = f'error: {str(e)}'
        is_healthy = False
        logger.error(f"Health check: Azure Blob Storage connection failed: {e}")
    
    response_data = {
        'status': 'healthy' if is_healthy else 'unhealthy',
        'timestamp': datetime.utcnow().isoformat(),
        'services': services
    }
    
    if is_healthy:
        return response_data
    else:
        return JSONResponse(
            status_code=503,
            content=response_data
        )


# Start the server when the script is run directly
if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    #uvicorn.run()