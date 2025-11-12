# cloud_metadata.py
import os
import numpy as np
import laspy
import logging
from typing import Optional, Dict, Tuple
from pyproj import CRS, Transformer

logger = logging.getLogger(__name__)


class CloudMetadata:
    """
    Extracts the true mean center (and CRS) from a LAS/LAZ file,
    transforms it to WGS84 (EPSG:4326), and rounds coordinates to 4 decimals.
    Falls back gracefully to bbox center if needed.

    Ideal for drone / mobile LiDAR datasets to center a Leaflet or Potree viewer.
    """

    def __init__(
        self,
        path: str,
        *,
        chunk_size: int = 5_000_000,
        crs_epsg: Optional[str] = None,
        sample_rate: float = 1.0,  # 1.0 = exact mean; <1.0 = random sampling for speed
    ):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        if not path.lower().endswith((".las", ".laz")):
            raise ValueError("Unsupported file type. Expected .las or .laz")
        if not (0 < sample_rate <= 1):
            raise ValueError("sample_rate must be in (0,1]")

        self.path = path
        self.chunk_size = chunk_size
        self.crs_epsg = crs_epsg
        self.sample_rate = sample_rate

    # ---------- CRS Handling ----------

    def _get_crs_obj(self) -> Optional[CRS]:
        """Get CRS object from provided EPSG code."""
        if self.crs_epsg:
            try:
                # Handle both "EPSG:26916" and "26916" formats
                epsg_code = self.crs_epsg.replace("EPSG:", "").replace("epsg:", "")
                return CRS.from_epsg(int(epsg_code))
            except Exception as e:
                logger.error(f"Failed to create CRS from EPSG code '{self.crs_epsg}': {e}")
                return None
        
        logger.warning("No CRS EPSG code provided to CloudMetadata")
        return None

    def get_crs(self) -> str:
        """Return EPSG code if available."""
        if self.crs_epsg:
            # Normalize to EPSG:XXXX format
            epsg_code = self.crs_epsg.replace("EPSG:", "").replace("epsg:", "")
            return f"EPSG:{epsg_code}"
        return "No CRS provided"

    # ---------- Native-space Centers ----------

    def _center_streaming_native(self) -> Dict[str, float]:
        """Compute true mean (or sampled mean) center in native coordinates."""
        total_x = total_y = total_z = 0.0
        n = 0
        rng = np.random.default_rng()

        with laspy.open(self.path) as f:
            for pts in f.chunk_iterator(self.chunk_size):
                x = np.asarray(pts.x, dtype=np.float64)
                y = np.asarray(pts.y, dtype=np.float64)
                z = np.asarray(pts.z, dtype=np.float64)

                if self.sample_rate < 1.0:
                    mask = rng.random(x.shape[0]) < self.sample_rate
                    if not np.any(mask):
                        continue
                    x = x[mask]; y = y[mask]; z = z[mask]

                total_x += x.sum()
                total_y += y.sum()
                total_z += z.sum()
                n += x.size

        if n == 0:
            raise ValueError("No points read from LAS file.")
        return {
            "x": total_x / n,
            "y": total_y / n,
            "z": total_z / n,
        }

    def _bbox_center_native(self) -> Dict[str, float]:
        """Fallback: center of bounding box (native coordinates)."""
        with laspy.open(self.path) as f:
            h = f.header
            minx, miny, minz = h.mins
            maxx, maxy, maxz = h.maxs
            return {
                "x": (minx + maxx) / 2.0,
                "y": (miny + maxy) / 2.0,
                "z": (minz + maxz) / 2.0,
            }

    # ---------- Transform to WGS84 ----------

    def _to_wgs84(self, x: float, y: float) -> Tuple[Optional[float], Optional[float]]:
        """Transform native coordinates (x,y) → WGS84 (lon,lat)."""
        crs_src = self._get_crs_obj()
        if crs_src is None:
            logger.warning("No CRS provided, cannot transform to WGS84")
            return None, None
        
        try:
            transformer = Transformer.from_crs(crs_src, CRS.from_epsg(4326), always_xy=True)
            lon, lat = transformer.transform(x, y)
            return lon, lat
        except Exception as e:
            logger.error(f"Error creating Transformer from CRS '{self.crs_epsg}': {e}")
            return None, None

    # ---------- Public Methods ----------

    def get_center_wgs84(self) -> Dict[str, Optional[float]]:
        """
        Returns the true mean center (WGS84) with lat/lon/z rounded to 4 decimals.
        Falls back to bbox center if necessary.
        """
        try:
            native = self._center_streaming_native()
        except Exception:
            native = self._bbox_center_native()

        lon, lat = self._to_wgs84(native["x"], native["y"])
        return {
            "lat": None if lat is None else round(lat, 4),
            "lon": None if lon is None else round(lon, 4),
            "z": round(native["z"], 4),
        }

    def summary(self) -> dict:
        """Summarized info for dashboards or APIs."""
        logger.info(f"Extracting metadata from: {self.path}")
        with laspy.open(self.path) as f:
            count = int(f.header.point_count)
        
        center = self.get_center_wgs84()
        crs = self.get_crs()
        
        logger.info(f"Metadata extracted: {count:,} points, CRS: {crs}")
        
        return {
            "file": os.path.basename(self.path),
            "points": count,
            "center": center,
            "crs": crs,
        }


# ---------- Example usage ----------
if __name__ == "__main__":
    path = r"/Volumes/HWC Photos/Point Cloud Resources/DATA/POINTCLOUDS/MOBILE/IN_West_Inidivid-Paths.Mobile1_cloud_7.laz"

    # Example with EPSG code (e.g., NAD83 / UTM zone 16N)
    meta = CloudMetadata(path, crs_epsg="EPSG:26916")

    info = meta.summary()
    print(f"\nFile: {info['file']}")
    print(f"\nPoints: {info['points']:,}")
    print(f"\nCenter (WGS84): {info['center']}")
    print(f"\nCRS: {info['crs']}")
