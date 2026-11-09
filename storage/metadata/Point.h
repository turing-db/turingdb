#pragma once

#include <stdint.h>

#include <compare>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace db {

class MapView;

// A coordinate reference system, written to disk as its ordinal inside every point
enum class PointCRS : uint8_t {
    Cartesian = 0,
    Cartesian3D,
    WGS84,
    WGS84_3D,
};

// A field of a point. Longitude, latitude and height name x, y and z in a geographic CRS.
enum class PointPart : uint8_t {
    X,
    Y,
    Z,
    Longitude,
    Latitude,
    Height,
    CRS,
    SRID,
};

// A location in one of the coordinate reference systems Cypher names. A 2D point holds a z
// of 0, and no coordinate is -0.0, so two equal points are equal in every byte.
class Point {
public:
    // The value of a component: a coordinate, the name of the CRS or its SRID
    using Component = std::variant<double, int64_t, std::string_view>;

    Point() = default;

    // Raises on a non-finite coordinate and on a latitude outside [-90, 90]. A longitude
    // outside [-180, 180] is wrapped into it.
    Point(PointCRS crs, double x, double y, double z);

    PointCRS getCRS() const { return _crs; }
    double getX() const { return _x; }
    double getY() const { return _y; }
    double getZ() const { return _z; }

    bool operator==(const Point& other) const;

    // The order ORDER BY sorts points in: by SRID, then by coordinates
    std::strong_ordering operator<=>(const Point& other) const;

    static size_t getDimension(PointCRS crs);

    static bool isGeographic(PointCRS crs);

    static int64_t getSRID(PointCRS crs);

    static std::string_view getCRSName(PointCRS crs);

    // The CRS @param name names, whatever its case
    static bool crsNamed(std::string_view name, PointCRS& crs);

    static bool crsOfSRID(int64_t srid, PointCRS& crs);

    static bool partNamed(std::string_view name, PointPart& part);

    static std::string_view getPartName(PointPart part);

    // A coordinate the point has. Raises for z of a 2D point, and for longitude, latitude
    // and height of a cartesian one.
    static double coordinate(Point value, PointPart part);

    static Component component(Point value, PointPart part);

    // point() of a map. False where the map holds a null.
    static bool fromMap(const MapView& map, Point& value);

    // Nullopt for two points in different CRSs
    static std::optional<double> distance(Point lhs, Point rhs);

    // Nullopt unless the three points share a CRS
    static std::optional<bool> withinBBox(Point value, Point lowerLeft, Point upperRight);

    // The mean of @param count points whose mean is @param mean, and @param value. A mean
    // of valid points of one CRS is a valid point of it, so a running mean is a Point.
    // Raises for a value in another CRS.
    static Point addToMean(Point mean, uint64_t count, Point value);

    static void format(std::string& out, Point value);

private:
    // No initializers: the datapart loader reads a trivial property's values straight out of
    // the mapped page, which needs a trivial default constructor
    double _x;
    double _y;
    double _z;
    PointCRS _crs;
    [[maybe_unused]] uint8_t _padding[7];

    static std::string_view requiredKeys(PointCRS crs);

    static double haversineDistance(Point lhs, Point rhs, double radius);
};

}

template <>
struct std::hash<db::Point> {
    size_t operator()(const db::Point& value) const noexcept;
};
