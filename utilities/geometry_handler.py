from loguru import logger
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Polygon, MultiPolygon

class GeometryHandler:
    """Handles geometry-related operations for satellite data."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.geometries = self._load_geometries()

    def _load_geometries(self):
        """Load geometry data from the specified file into a list of shapely geometry objects."""
        if self.file_path.endswith('.geojson'):
            logger.info(f"Loading geometries from GeoJSON file: {self.file_path}")
            gdf = gpd.read_file(self.file_path)
            return list(gdf.geometry)

        elif self.file_path.endswith('.wkt'):
            logger.info(f"Loading geometries from WKT file: {self.file_path}")
            with open(self.file_path, 'r') as file:
                wkt_data = file.read().strip()

                # Try to handle multiple WKT geometries in one file
                lines = [line.strip() for line in wkt_data.splitlines() if line.strip()]
                geometries = []
                for line in lines:
                    geom = wkt.loads(line)
                    if isinstance(geom, (Polygon, MultiPolygon)):
                        if isinstance(geom, MultiPolygon):
                            geometries.extend(list(geom.geoms))
                        else:
                            geometries.append(geom)
                return geometries

        else:
            raise ValueError("Unsupported geometry file format. Use .geojson or .wkt")

    def get_all_geometries(self):
        """Return all loaded geometries as a list."""
        return self.geometries
