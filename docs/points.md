We support the Cypher point type as a stored property value, following Neo4j's spatial values:
https://neo4j.com/docs/cypher-manual/current/values-and-types/spatial/.

# The value

`ValueType::Point` is a scalar property type, held by `TypedPropertyContainer<types::Point>`.
`db::Point` (`storage/metadata/Point.h`) is 32 trivially copyable bytes: `x`, `y` and `z` as
doubles, then a one-byte `PointCRS` and 7 bytes of zero padding. The CRS is one of Neo4j's four:

```
PointCRS       name          SRID   dimension
Cartesian      cartesian     7203   2
Cartesian3D    cartesian-3d  9157   3
WGS84          wgs-84        4326   2
WGS84_3D       wgs-84-3d     4979   3
```

`PointCRS` is written to disk as its ordinal, inside every stored point, so it is append-only.

The constructor makes two equal points equal in every byte, so a dump, a list encoding and a
DISTINCT key can all compare raw bytes: a 2D point holds a `z` of 0, `-0.0` becomes `0.0`, and
the padding is zero. It also rejects what no point holds: a non-finite coordinate, and a
geographic latitude outside [-90, 90]. A geographic longitude outside [-180, 180] is wrapped into
it, so `point({longitude: 190, latitude: 0})` has an x of -170.

A point is a `CellTag::Point` inside a list or a map, written as its 32 bytes.

# Building one

`point(map)` reads the map's keys, ignoring case and ignoring any key it does not know:

```
point({x, y})                          cartesian
point({x, y, z})                       cartesian-3d
point({longitude, latitude})           wgs-84
point({longitude, latitude, height})   wgs-84-3d; z may stand for height
point({..., crs: 'wgs-84'})            the CRS by name, ignoring case
point({..., srid: 4326})               the CRS by SRID
```

`x` and `y` win over `longitude` and `latitude` where a map holds both, and are read as longitude
and latitude in a geographic CRS. A null map, or a null value under any key, gives null.

It raises on a map naming both a `crs` and an `srid`, on an unknown CRS, on longitude and latitude
in a cartesian CRS, on a number of coordinates the CRS does not have, and on a coordinate that is
no number. Neo4j parses a numeric string coordinate; TuringDB does not.

# Reading one

`p.x`, `p.y` and `p.z` are its coordinates, and `p.longitude`, `p.latitude` and `p.height` the
same ones in a geographic CRS; all six are doubles. `p.crs` is the CRS name and `p.srid` its SRID.
`p.z` and `p.height` raise on a 2D point, and the three geographic names raise on a cartesian
point. The CRS is a property of the value, not of the column, so a column of points mixing CRSs
raises on the row that holds the wrong one.

A point renders as the text Neo4j's `toString` writes, in `toString(p)` and in the shell:
`point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})`. The binary protocol carries it as its 32 bytes,
under the `POINT` column kind.

# Comparing

Two points are equal when they share a CRS and every coordinate. `<`, `<=`, `>` and `>=` between
points are null, as they are between maps. ORDER BY, DISTINCT and grouping order points by SRID,
then by x, y and z. Between types, points sort after paths and before datetimes, as in Neo4j.

# Distance and bounding boxes

`point.distance(a, b)` is the Euclidean distance between two cartesian points. Between two
wgs-84 points it is the great-circle distance in metres, by the haversine formula on a sphere of
the IUGG mean Earth radius, 6,371,008.8 m. Neo4j uses 6,378,140 m, so its distances are 0.11%
longer. Between two wgs-84-3d points the great circle is taken at the mean of the two heights and
combined with the height difference, as Neo4j does. It is null for points in different CRSs.

`point.withinBBox(p, lowerLeft, upperRight)` is whether `p` lies in the box, bounds included. In a
geographic CRS a `lowerLeft` east of `upperRight` makes a box that crosses the 180th meridian, and
a `lowerLeft` north of `upperRight` an empty one. It is null unless the three points share a CRS.

# Importing

`LOAD JSONL` reads a point out of a JSON object under a name a `WITH POINTS [...]` clause lists,
by the rules `point()` reads a map with:

```
LOAD JSONL '/data/cities.jsonl' AS cities WITH POINTS [at]

{"type": "node", "id": "0", "labels": ["City"],
 "properties": {"name": "Paris", "at": {"longitude": 2.3522, "latitude": 48.8566}}}
```

A JSON null leaves the property unset. An object naming no point, or holding a null, raises.
`WITH POINTS`, `WITH DATETIMES` and `WITH DURATIONS` fill one `TypedPropertySpec`, which maps
each name to the type it is read as, and a name given two types is rejected.
