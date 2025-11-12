"""
Thumbnail generation utility for point cloud files.
Generates density map thumbnails with RGB colors and transparent backgrounds.
"""

import logging
import numpy as np
from io import BytesIO
from typing import Optional, Tuple
import laspy
from PIL import Image

logger = logging.getLogger(__name__)


class ThumbnailGenerator:
    """Generate thumbnail images from LAS/LAZ point cloud files."""
    
    def __init__(self, size: int = 512):
        """
        Initialize thumbnail generator.
        
        Args:
            size: Target size for the thumbnail (width and height in pixels)
        """
        self.size = size
    
    def generate_from_las(self, las_path: str) -> bytes:
        """
        Generate a thumbnail from a LAS/LAZ file using PDAL.
        
        Creates a 2D density map with RGB colors (if available) and transparent background.
        Maintains aspect ratio while fitting to a square canvas.
        
        Args:
            las_path: Path to the LAS/LAZ file
            
        Returns:
            PNG image data as bytes
            
        Raises:
            FileNotFoundError: If the LAS file doesn't exist
            Exception: If thumbnail generation fails
        """
        try:
            logger.info(f"Generating thumbnail for {las_path}")
            
            # Read point cloud with PDAL
            points, has_rgb = self._read_point_cloud(las_path)
            
            if len(points) == 0:
                raise ValueError("Point cloud contains no points")
            
            logger.info(f"Read {len(points)} points, RGB available: {has_rgb}")
            
            # Create density map
            image_data = self._create_density_map(points, has_rgb)
            
            # Convert to PIL Image and save as PNG
            png_bytes = self._render_to_png(image_data)
            
            logger.info(f"Thumbnail generated successfully ({len(png_bytes)} bytes)")
            return png_bytes
            
        except FileNotFoundError:
            logger.error(f"LAS file not found: {las_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to generate thumbnail: {e}", exc_info=True)
            raise
    
    def _read_point_cloud(self, las_path: str, sample_rate: float = 0.01) -> Tuple[np.ndarray, bool]:
        """
        Read point cloud data using laspy with chunked streaming (same approach as CloudMetadata).
        Uses sampling for large files to avoid memory issues.
        
        Args:
            las_path: Path to the LAS/LAZ file
            sample_rate: Fraction of points to sample (0.01 = 1%)
            
        Returns:
            Tuple of (points array with X, Y, R, G, B columns, has_rgb flag)
        """
        # Use chunked reading like CloudMetadata does
        sampled_points = []
        rng = np.random.default_rng()
        has_rgb = False
        
        with laspy.open(las_path) as f:
            # Check if RGB is available
            has_rgb = 'red' in f.header.point_format.dimension_names
            total_points = f.header.point_count
            
            logger.info(f"Reading point cloud with {total_points:,} points (sampling {sample_rate*100:.1f}%)")
            
            # Read in chunks to avoid memory issues
            for chunk in f.chunk_iterator(5_000_000):
                # Sample points from this chunk
                n_chunk = len(chunk.x)
                
                # Random sampling
                mask = rng.random(n_chunk) < sample_rate
                if not np.any(mask):
                    continue
                
                x = chunk.x[mask]
                y = chunk.y[mask]
                
                if has_rgb:
                    # Extract RGB values (typically 16-bit, scale to 8-bit)
                    r = (chunk.red[mask] / 256).astype(np.uint8)
                    g = (chunk.green[mask] / 256).astype(np.uint8)
                    b = (chunk.blue[mask] / 256).astype(np.uint8)
                    chunk_points = np.column_stack([x, y, r, g, b])
                else:
                    chunk_points = np.column_stack([x, y])
                
                sampled_points.append(chunk_points)
        
        # Combine all sampled chunks
        if not sampled_points:
            raise ValueError("No points sampled from point cloud")
        
        points = np.vstack(sampled_points)
        logger.info(f"Sampled {len(points):,} points for thumbnail generation")
        
        return points, has_rgb
    
    def _create_density_map(self, points: np.ndarray, has_rgb: bool) -> np.ndarray:
        """
        Create a 2D density map from point cloud data.
        
        Args:
            points: Array of points with X, Y, and optionally R, G, B columns
            has_rgb: Whether RGB data is available
            
        Returns:
            RGBA image array (height, width, 4)
        """
        # Extract XY coordinates
        xy = points[:, :2]
        
        # Calculate bounds and aspect ratio
        x_min, y_min = xy.min(axis=0)
        x_max, y_max = xy.max(axis=0)
        
        x_range = x_max - x_min
        y_range = y_max - y_min
        
        if x_range == 0 or y_range == 0:
            raise ValueError("Point cloud has zero extent in X or Y dimension")
        
        # Maintain aspect ratio, fit to square
        aspect_ratio = x_range / y_range
        
        if aspect_ratio > 1:
            # Wider than tall
            grid_width = self.size
            grid_height = int(self.size / aspect_ratio)
        else:
            # Taller than wide
            grid_height = self.size
            grid_width = int(self.size * aspect_ratio)
        
        # Ensure minimum size
        grid_width = max(grid_width, 1)
        grid_height = max(grid_height, 1)
        
        logger.info(f"Creating density map: {grid_width}x{grid_height} (aspect ratio: {aspect_ratio:.2f})")
        
        # Bin points into 2D grid
        x_bins = np.linspace(x_min, x_max, grid_width + 1)
        y_bins = np.linspace(y_min, y_max, grid_height + 1)
        
        # Digitize points into bins
        x_indices = np.digitize(xy[:, 0], x_bins) - 1
        y_indices = np.digitize(xy[:, 1], y_bins) - 1
        
        # Clip to valid range
        x_indices = np.clip(x_indices, 0, grid_width - 1)
        y_indices = np.clip(y_indices, 0, grid_height - 1)
        
        # Create RGBA image array
        image = np.zeros((grid_height, grid_width, 4), dtype=np.uint8)
        
        if has_rgb:
            # Average RGB colors per bin
            rgb_data = points[:, 2:5].astype(np.float32)
            
            # Create accumulator array with float32 to avoid casting issues
            rgb_accumulator = np.zeros((grid_height, grid_width, 3), dtype=np.float32)
            
            for i in range(len(points)):
                x_idx = x_indices[i]
                y_idx = y_indices[i]
                
                # Accumulate RGB values in float32 array
                rgb_accumulator[y_idx, x_idx] += rgb_data[i]
        
        # Calculate density per bin
        density = np.zeros((grid_height, grid_width), dtype=np.int32)
        
        for i in range(len(points)):
            x_idx = x_indices[i]
            y_idx = y_indices[i]
            density[y_idx, x_idx] += 1
        
        # Normalize RGB by density and set alpha based on density
        max_density = density.max()
        
        for y in range(grid_height):
            for x in range(grid_width):
                count = density[y, x]
                
                if count > 0:
                    if has_rgb:
                        # Average the accumulated RGB values and convert to uint8
                        image[y, x, :3] = np.clip(rgb_accumulator[y, x] / count, 0, 255).astype(np.uint8)
                    else:
                        # Use grayscale based on density
                        intensity = int(255 * (count / max_density))
                        image[y, x, :3] = intensity
                    
                    # Set alpha based on density (more points = more opaque)
                    alpha = int(255 * min(1.0, count / (max_density * 0.1)))
                    image[y, x, 3] = max(alpha, 128)  # Minimum alpha of 128
                else:
                    # Transparent for empty bins
                    image[y, x, 3] = 0
        
        # Flip Y axis (image coordinates are top-down, point cloud is bottom-up)
        image = np.flipud(image)
        
        return image
    
    def _render_to_png(self, image_data: np.ndarray) -> bytes:
        """
        Render image data to PNG bytes.
        
        Args:
            image_data: RGBA image array
            
        Returns:
            PNG image as bytes
        """
        # Create PIL Image from numpy array
        img = Image.fromarray(image_data, mode='RGBA')
        
        # If image is not square, paste onto square canvas with transparent background
        height, width = image_data.shape[:2]
        
        if height != width:
            # Create square canvas
            canvas_size = max(height, width)
            canvas = Image.new('RGBA', (canvas_size, canvas_size), (0, 0, 0, 0))
            
            # Center the image on the canvas
            x_offset = (canvas_size - width) // 2
            y_offset = (canvas_size - height) // 2
            
            canvas.paste(img, (x_offset, y_offset))
            img = canvas
        
        # Save to bytes
        buffer = BytesIO()
        img.save(buffer, format='PNG', optimize=True)
        
        return buffer.getvalue()
