"""
Utility for converting georeferenced rasters to Leaflet-compatible PNG overlays.

This module provides functionality to convert GeoTIFF and other georeferenced
raster formats into transparent PNG images with EPSG:4326 bounds for use with
Leaflet's imageOverlay feature.
"""

import json
import subprocess
import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)


def _run(cmd: List[str]) -> str:
    """
    Run a command and return stdout.
    
    Args:
        cmd: Command and arguments as a list
        
    Returns:
        Command stdout as string
        
    Raises:
        RuntimeError: If command fails
    """
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout


def raster_to_leaflet_overlay(
    input_path: str,
    output_png: str,
) -> Dict[str, List[List[float]]]:
    """
    Convert a georeferenced raster into a transparent PNG and return Leaflet bounds.
    
    This function:
    1. Reprojects the raster to EPSG:4326 (WGS84)
    2. Adds alpha channel for transparency
    3. Converts to PNG with maximum compression
    4. Extracts corner coordinates as Leaflet bounds
    
    Supported input formats:
    - GeoTIFF (.tif, .tiff)
    - JPG/PNG with world files (.jgw, .pgw, .wld)
    - Any GDAL-supported georeferenced raster
    
    Args:
        input_path: Path to input georeferenced raster file
        output_png: Path where output PNG should be saved
        
    Returns:
        Dictionary with bounds in Leaflet format:
        {"bounds": [[south, west], [north, east]]}
        
    Raises:
        RuntimeError: If conversion fails or input has no georeferencing
        
    Example:
        >>> result = raster_to_leaflet_overlay("input.tif", "overlay.png")
        >>> bounds = result["bounds"]
        >>> # Use in Leaflet: L.imageOverlay("/overlay.png", bounds).addTo(map);
    """
    logger.info(f"Converting raster to Leaflet overlay: {input_path} -> {output_png}")
    
    input_path = str(Path(input_path))
    output_png = str(Path(output_png))
    tmp = str(Path(output_png).with_suffix(".warp.tif"))
    
    try:
        # Step 1: Reproject to EPSG:4326 with alpha channel
        logger.info("Reprojecting to EPSG:4326 with alpha channel")
        _run([
            "gdalwarp",
            "-t_srs", "EPSG:4326",
            "-dstalpha",
            "-r", "cubic",
            input_path,
            tmp
        ])
        
        # Step 2: Convert to PNG with compression
        logger.info("Converting to PNG format")
        _run([
            "gdal_translate",
            "-of", "PNG",
            "-co", "ZLEVEL=9",
            tmp,
            output_png
        ])
        
        # Step 3: Extract bounds from warped file
        logger.info("Extracting bounds from georeferenced raster")
        info = json.loads(_run(["gdalinfo", "-json", tmp]))
        corners = info.get("cornerCoordinates")
        if not corners:
            raise RuntimeError("Input raster has no georeferencing.")
        
        ul = corners["upperLeft"]
        lr = corners["lowerRight"]
        
        west, north = ul
        east, south = lr
        
        bounds = [[south, west], [north, east]]
        
        logger.info(f"Extracted bounds: {bounds}")
        
        # Clean up temporary warped file
        Path(tmp).unlink(missing_ok=True)
        logger.info("Conversion completed successfully")
        
        return {"bounds": bounds}
        
    except Exception as e:
        # Clean up temporary file on error
        Path(tmp).unlink(missing_ok=True)
        logger.error(f"Failed to convert raster to Leaflet overlay: {e}")
        raise
