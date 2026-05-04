from __future__ import annotations

import math


class TileMath:
    """Helpers for Bing/Web Mercator tile math and QuadKey conversion."""

    MIN_LAT = -85.05112878
    MAX_LAT = 85.05112878
    MIN_LON = -180.0
    MAX_LON = 180.0

    @staticmethod
    def clip(value: float, min_value: float, max_value: float) -> float:
        return min(max(value, min_value), max_value)

    @staticmethod
    def map_size(level_of_detail: int) -> int:
        return 256 << level_of_detail

    @staticmethod
    def lat_lon_to_pixel_xy(lat: float, lon: float, level_of_detail: int) -> tuple[int, int]:
        lat = TileMath.clip(lat, TileMath.MIN_LAT, TileMath.MAX_LAT)
        lon = TileMath.clip(lon, TileMath.MIN_LON, TileMath.MAX_LON)

        x = (lon + 180.0) / 360.0
        sin_lat = math.sin(lat * math.pi / 180.0)
        y = 0.5 - math.log((1.0 + sin_lat) / (1.0 - sin_lat)) / (4.0 * math.pi)

        map_size = TileMath.map_size(level_of_detail)
        pixel_x = int(TileMath.clip(x * map_size + 0.5, 0.0, float(map_size - 1)))
        pixel_y = int(TileMath.clip(y * map_size + 0.5, 0.0, float(map_size - 1)))
        return pixel_x, pixel_y

    @staticmethod
    def pixel_xy_to_tile_xy(pixel_x: int, pixel_y: int) -> tuple[int, int]:
        return pixel_x // 256, pixel_y // 256

    @staticmethod
    def tile_xy_to_quadkey(tile_x: int, tile_y: int, level_of_detail: int) -> str:
        quadkey: list[str] = []
        for level in range(level_of_detail, 0, -1):
            digit = 0
            mask = 1 << (level - 1)
            if (tile_x & mask) != 0:
                digit += 1
            if (tile_y & mask) != 0:
                digit += 2
            quadkey.append(str(digit))
        return "".join(quadkey)

    @staticmethod
    def quadkey_to_tile_xy(quadkey: str) -> tuple[int, int, int]:
        tile_x = 0
        tile_y = 0
        level_of_detail = len(quadkey)
        for index, char in enumerate(quadkey):
            mask = 1 << (level_of_detail - index - 1)
            if char == "1":
                tile_x |= mask
            elif char == "2":
                tile_y |= mask
            elif char == "3":
                tile_x |= mask
                tile_y |= mask
            elif char != "0":
                raise ValueError("Invalid QuadKey digit sequence.")
        return tile_x, tile_y, level_of_detail
