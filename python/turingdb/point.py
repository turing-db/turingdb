from typing import NamedTuple, Optional

_CRS_NAMES = {7203: "cartesian", 9157: "cartesian-3d", 4326: "wgs-84", 4979: "wgs-84-3d"}


class Point(NamedTuple):
    """A point a query returns. z is None for a 2D point."""

    srid: int
    x: float
    y: float
    z: Optional[float] = None

    @property
    def crs(self) -> str:
        return _CRS_NAMES[self.srid]
