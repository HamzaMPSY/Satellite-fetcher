from loguru import logger
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Polygon

class GeometryHandler:
    """
    Handles geometry-related operations for satellite data.

    Able to load various supported geometry file formats (WKT, GeoJSON) and makes the geometry
    available as a Shapely Polygon for downstream processing.

    Attributes:
        file_path (str): Path to the geometry file (.geojson or .wkt).
        geometry (Polygon): The loaded Shapely geometry object.
    """
    def __init__(self, file_path: str):
        """
        Initialize the GeometryHandler and automatically load the geometry.

        Args:
            file_path (str): Path to the geometry file (.geojson or .wkt).

        Raises:
            ValueError: If the file type is unsupported.
        """
        self.file_path = file_path
        self.geometries = []
        self._load_geometry()

    def _load_geometry(self) -> Polygon:
        """
        Load geometry data from the specified file, supporting GeoJSON (.geojson)
        and Well-Known Text (.wkt) formats.

        Returns:
            Polygon: A Shapely geometry object loaded from the file.

        Raises:
            ValueError: If the file format is not supported.
        """
        # Check for supported file extensions and load using the right library
        if self.file_path.endswith('.geojson'):
            logger.info(f"Loading geometry from GeoJSON file: {self.file_path}")
            return gpd.read_file(self.file_path).geometry.values[0]
        elif self.file_path.endswith('.wkt'):
            logger.info(f"Loading geometry from WKT file: {self.file_path}")
            with open(self.file_path, 'r') as file:
                for line in file.readlines():
                    # Assuming the WKT file contains a single geometry
                    if line.strip():
                        self.geometries.append(wkt.loads(line.strip()))
        else:
            logger.error(f"Unsupported geometry file format: {self.file_path}. Must be '.geojson' or '.wkt'.")
            raise ValueError("Unsupported geometry file format. Use .geojson or .wkt")
