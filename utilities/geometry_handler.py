from loguru import logger
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Polygon

class GeometryHandler:
    """Handles geometry-related operations for satellite data."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.geometry = self._load_geometry()

    def _load_geometry(self) -> Polygon:
        """Load geometry data from the specified file.
            handles all geometry file types (e.g., GeoJSON, WKT)
            returns shapely geometry object
        """
        if self.file_path.endswith('.geojson'):
            logger.info(f"Loading geometry from GeoJSON file: {self.file_path}")
            return gpd.read_file(self.file_path).geometry.values[0]
        elif self.file_path.endswith('.wkt'):
            logger.info(f"Loading geometry from WKT file: {self.file_path}")
            with open(self.file_path, 'r') as file:
                return wkt.loads(file.read())
        else:
            raise ValueError("Unsupported geometry file format. Use .geojson or .wkt")