from fastapi import APIRouter, HTTPException
from datetime import datetime
import logging

from config.main import DB

logger = logging.getLogger(__name__)

# Statistics ROUTER
stats_router = APIRouter(
    prefix="/stats",
    tags=["Statistics"],
    responses={404: {"description": "Not found"}},
)


@stats_router.get(
    '/',
    summary="Get system statistics",
    description="Retrieve aggregated statistics including total projects, points, and job counts.",
    response_description="System statistics with timestamp"
)
async def get_statistics():
    """
    Get dashboard statistics.
    
    Returns aggregated data for displaying system-wide statistics:
    - Total number of projects
    - Total point count across all projects
    - Number of active jobs (pending or processing)
    - Number of jobs completed in the last 24 hours
    - Number of jobs failed in the last 24 hours
    
    This endpoint uses efficient database aggregation queries to calculate
    statistics without fetching all documents.
    
    **Example Response:**
    ```json
    {
      "total_projects": 150,
      "total_points": 45000000,
      "active_jobs": 3,
      "completed_jobs_24h": 12,
      "failed_jobs_24h": 1,
      "timestamp": "2025-11-25T10:00:00Z"
    }
    ```
    
    **Returns:**
    - 200: Statistics retrieved successfully
    - 500: Server error
    """
    logger.info("Retrieving system statistics")
    
    try:
        # Get statistics from database
        stats = DB.get_statistics()
        
        # Add timestamp to response
        stats['timestamp'] = datetime.utcnow().isoformat()
        
        logger.info(f"Successfully retrieved statistics: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Failed to retrieve statistics: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve statistics from database"
        )
