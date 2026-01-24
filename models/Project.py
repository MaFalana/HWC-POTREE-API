from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime   


class Location(BaseModel):
    lat: float = Field(0.0000, description="Latitude in WGS84")
    lon: float = Field(0.0000, description="Longitude in WGS84")
    z: float = Field(0.0000, description="Elevation")

    def _to_dict(self):
        return self.dict()


class CRS(BaseModel):
    id: str = Field("", alias="_id", description="EPSG Code")
    name: Optional[str] = Field(None, description="CRS human-readable name")
    proj4: Optional[str] = Field(None, description="Full proj4 string")

    class Config:
        populate_by_name = True  # Allow populating by both 'id' and '_id'

    def _to_dict(self):
        return self.dict(by_alias=True)


class Ortho(BaseModel):
    url: Optional[str] = Field(None, description="Public URL to ortho PNG overlay")
    thumbnail: Optional[str] = Field(None, description="Public URL to thumbnail PNG")
    bounds: Optional[List[List[float]]] = Field(None, description="Leaflet bounds [[south, west], [north, east]]")

    def _to_dict(self):
        return self.dict()


class Project(BaseModel):
    id: str = Field("", alias="_id",description="Project Job Number")
    name: Optional[str] = Field(None, description="Project Name")
    client: Optional[str] = Field(None, description="Client Name")
    date: Optional[datetime] = Field(None, description="Acquisition Date")
    tags: List[str] = Field(default_factory=list, description="List of tags (e.g. FIELD, LOI)")
    cloud: Optional[str] = Field(None, description="Blob URL or path to point cloud file")
    crs: Optional[CRS] = Field(None, description="Coordinate Reference System info")
    location: Optional[Location] = Field(None, description="Mean center location (lat/lon/z)")
    description: Optional[str] = Field(None, description="Project description or notes")
    thumbnail: Optional[str] = Field(None, description="URL to thumbnail or preview image")
    point_count: Optional[int] = Field(None, description="Total number of points in the point cloud")
    ortho: Optional[Ortho] = Field(None, description="Orthophoto PNG overlay with bounds and thumbnail")

    def _to_dict(self):
        result = self.dict(by_alias=True)
        # Convert nested Ortho object to dict if present
        if self.ortho is not None:
            result['ortho'] = self.ortho._to_dict()
        return result


class ProjectResponse(Project):
    """
    Same as Project, but can be extended later to include server-computed fields
    like createdAt, updatedAt, or status.
    """
    createdAt: Optional[datetime] = Field(None, description="Project creation date")
    updatedAt: Optional[datetime] = Field(None, description="Project update date")

    status: Optional[str] = Field(None, description="Project status")
