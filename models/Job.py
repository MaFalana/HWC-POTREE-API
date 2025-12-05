from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class Job(BaseModel):
    model_config = {"populate_by_name": True}  # Allow populating by both field name and alias
    
    id: str = Field("", alias="_id", description="Job ID (UUID)")
    project_id: str = Field(..., description="Associated project ID")
    status: str = Field("pending", description="Job status: pending, processing, completed, failed")
    type: Optional[str] = Field(None, description="Job type: point_cloud, ortho_conversion")
    
    # File paths
    file_path: str = Field(..., description="Local temporary file path")
    azure_path: str = Field(..., description="Azure blob path (jobs/{job_id}.laz)")
    
    # Progress tracking
    current_step: Optional[str] = Field(None, description="Current processing step: metadata, thumbnail, conversion, upload")
    progress_message: Optional[str] = Field(None, description="Human-readable progress message")
    
    # Error handling
    error_message: Optional[str] = Field(None, description="Error message if job failed")
    retry_count: int = Field(0, description="Number of retry attempts")
    
    # Cancellation support
    cancelled: bool = Field(False, description="Whether the job has been cancelled")
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")
    completed_at: Optional[datetime] = Field(None, description="Job completion timestamp")

    def _to_dict(self):
        return self.model_dump(by_alias=True)


class JobResponse(Job):
    """
    Extended Job model for API responses with additional computed fields
    """
    pass
