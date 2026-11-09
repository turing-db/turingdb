#include "Point.h"

#include <math.h>
#include <stdint.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <numbers>

#include <spdlog/fmt/fmt.h>

#include "QueryTypeName.h"
#include "StringUtils.h"
#include "TuringException.h"
#include "columns/ValueText.h"
#include "map/MapView.h"

using namespace db;

namespace {

constexpr double EARTH_MEAN_RADIUS_METRES = 6371008.8;

constexpr std::array<PointCRS, 4> CRS_SYSTEMS = {
    PointCRS::Cartesian,
    PointCRS::Cartesian3D,
    PointCRS::WGS84,
    PointCRS::WGS84_3D,
};

// The name of each PointPart, at its ordinal
constexpr std::array<std::string_view, 8> PART_NAMES = {
    "x", "y", "z", "longitude", "latitude", "height", "crs", "srid",
};

static_assert(PART_NAMES.size() == static_cast<size_t>(PointPart::SRID) + 1);

struct PointKeys {
    std::array<double, PART_NAMES.size()> _coordinates {};
    std::array<bool, PART_NAMES.size()> _present {};
    std::string_view _crsName;
    int64_t _srid {0};

    bool has(PointPart part) const { return _present[static_cast<size_t>(part)]; }
    double get(PointPart part) const { return _coordinates[static_cast<size_t>(part)]; }
};

// point() reads its keys whatever their case, as Neo4j does
bool keyNamed(std::string_view name, PointPart& part) {
    const auto it = std::ranges::find_if(PART_NAMES, [name](std::string_view partName) {
        return StringUtils::equalsIgnoringCase(partName, name);
    });

    if (it == PART_NAMES.end()) {
        return false;
    }

    part = static_cast<PointPart>(std::distance(PART_NAMES.begin(), it));

    return true;
}

double coordinateOf(std::string_view key, CellView value) {
    const CellTag tag = value.getTag();

    if (tag == CellTag::Int) {
        return static_cast<double>(value.getAs<int64_t>());
    } else if (tag == CellTag::UInt) {
        return static_cast<double>(value.getAs<uint64_t>());
    } else if (tag == CellTag::Double) {
        return value.getAs<double>();
    }

    throw TuringException(fmt::format("point() reads a number for '{}', not '{}'", key, queryTypeName(tag)));
}

// Reads the keys point() knows; it ignores any other, as Neo4j does. False where a value is null.
bool readKeys(const MapView& map, PointKeys& keys) {
    bool holdsANull = false;

    for (const MapEntryView entry : map) {
        const std::string_view name = entry.getKey();
        const CellView value = entry.getValue();
        const CellTag tag = value.getTag();

        if (tag == CellTag::Null) {
            holdsANull = true;
            continue;
        }

        PointPart part {PointPart::X};
        if (!keyNamed(name, part)) {
            continue;
        }

        const size_t index = static_cast<size_t>(part);
        if (keys._present[index]) {
            throw TuringException(fmt::format("point() reads the key '{}' twice", name));
        }

        keys._present[index] = true;

        if (part == PointPart::CRS) {
            if (tag != CellTag::String) {
                throw TuringException(fmt::format("point() reads a string for 'crs', not '{}'", queryTypeName(tag)));
            }

            keys._crsName = value.getAs<std::string_view>();
        } else if (part == PointPart::SRID) {
            if (tag == CellTag::Int) {
                keys._srid = value.getAs<int64_t>();
            } else if (tag == CellTag::UInt) {
                const uint64_t srid = value.getAs<uint64_t>();
                keys._srid = srid > static_cast<uint64_t>(INT64_MAX) ? -1 : static_cast<int64_t>(srid);
            } else {
                throw TuringException(fmt::format("point() reads an integer for 'srid', not '{}'", queryTypeName(tag)));
            }
        } else {
            keys._coordinates[index] = coordinateOf(name, value);
        }
    }

    return !holdsANull;
}

// The CRS the map names with 'crs' or 'srid', false where it names none
bool namedCRS(const PointKeys& keys, PointCRS& crs) {
    const bool namesACRS = keys.has(PointPart::CRS);
    const bool namesAnSRID = keys.has(PointPart::SRID);

    if (namesACRS && namesAnSRID) {
        throw TuringException("point() takes a 'crs' or an 'srid', not both");
    } else if (namesACRS) {
        if (!Point::crsNamed(keys._crsName, crs)) {
            throw TuringException(fmt::format("Unknown coordinate reference system: '{}'", keys._crsName));
        }

        return true;
    } else if (namesAnSRID) {
        if (!Point::crsOfSRID(keys._srid, crs)) {
            throw TuringException(fmt::format("Unknown coordinate reference system SRID: {}", keys._srid));
        }

        return true;
    }

    return false;
}

double finiteCoordinate(double coordinate) {
    if (!isfinite(coordinate)) {
        throw TuringException("A point cannot have a non-finite coordinate");
    }

    return coordinate == 0.0 ? 0.0 : coordinate;
}

bool withinRange(double value, double lower, double upper) {
    return value >= lower && value <= upper;
}

double wrappedLongitude(double longitude) {
    if (withinRange(longitude, -180.0, 180.0)) {
        return longitude;
    }

    double wrapped = fmod(longitude, 360.0);
    if (wrapped > 180.0) {
        wrapped -= 360.0;
    } else if (wrapped < -180.0) {
        wrapped += 360.0;
    }

    return wrapped == 0.0 ? 0.0 : wrapped;
}

std::strong_ordering compareCoordinates(double lhs, double rhs) {
    if (lhs < rhs) {
        return std::strong_ordering::less;
    } else if (lhs > rhs) {
        return std::strong_ordering::greater;
    }

    return std::strong_ordering::equal;
}

// A longitude range whose lower end lies east of its upper end crosses the 180th meridian
bool withinLongitudes(double longitude, double west, double east) {
    if (west <= east) {
        return withinRange(longitude, west, east);
    }

    return longitude >= west || longitude <= east;
}

}

Point::Point(PointCRS crs, double x, double y, double z)
    : _x(finiteCoordinate(x)),
    _y(finiteCoordinate(y)),
    _z(getDimension(crs) == 3 ? finiteCoordinate(z) : 0.0),
    _crs(crs)
{
    std::ranges::fill(_padding, 0);

    if (!isGeographic(crs)) {
        return;
    }

    if (!withinRange(_y, -90.0, 90.0)) {
        throw TuringException(fmt::format("The latitude of a {} point lies in [-90, 90], not {}", getCRSName(crs), _y));
    }

    _x = wrappedLongitude(_x);
}

bool Point::operator==(const Point& other) const {
    return _crs == other._crs && _x == other._x && _y == other._y && _z == other._z;
}

std::strong_ordering Point::operator<=>(const Point& other) const {
    const std::strong_ordering bySRID = getSRID(_crs) <=> getSRID(other._crs);
    if (bySRID != 0) {
        return bySRID;
    }

    const std::strong_ordering byX = compareCoordinates(_x, other._x);
    if (byX != 0) {
        return byX;
    }

    const std::strong_ordering byY = compareCoordinates(_y, other._y);
    if (byY != 0) {
        return byY;
    }

    return compareCoordinates(_z, other._z);
}

size_t Point::getDimension(PointCRS crs) {
    switch (crs) {
        case PointCRS::Cartesian:
        case PointCRS::WGS84:
            return 2;
        break;
        case PointCRS::Cartesian3D:
        case PointCRS::WGS84_3D:
            return 3;
        break;
    }

    return 2;
}

bool Point::isGeographic(PointCRS crs) {
    return crs == PointCRS::WGS84 || crs == PointCRS::WGS84_3D;
}

int64_t Point::getSRID(PointCRS crs) {
    switch (crs) {
        case PointCRS::Cartesian:
            return 7203;
        break;
        case PointCRS::Cartesian3D:
            return 9157;
        break;
        case PointCRS::WGS84:
            return 4326;
        break;
        case PointCRS::WGS84_3D:
            return 4979;
        break;
    }

    return 0;
}

std::string_view Point::getCRSName(PointCRS crs) {
    switch (crs) {
        case PointCRS::Cartesian:
            return "cartesian";
        break;
        case PointCRS::Cartesian3D:
            return "cartesian-3d";
        break;
        case PointCRS::WGS84:
            return "wgs-84";
        break;
        case PointCRS::WGS84_3D:
            return "wgs-84-3d";
        break;
    }

    return "";
}

bool Point::partNamed(std::string_view name, PointPart& part) {
    const auto it = std::ranges::find(PART_NAMES, name);
    if (it == PART_NAMES.end()) {
        return false;
    }

    part = static_cast<PointPart>(std::distance(PART_NAMES.begin(), it));

    return true;
}

std::string_view Point::getPartName(PointPart part) {
    return PART_NAMES[static_cast<size_t>(part)];
}

double Point::coordinate(Point value, PointPart part) {
    const PointCRS crs = value.getCRS();
    const bool isThreeDimensional = getDimension(crs) == 3;
    const bool namesAGeographicPart = part == PointPart::Longitude
                                   || part == PointPart::Latitude
                                   || part == PointPart::Height;
    const bool namesAThirdCoordinate = part == PointPart::Z || part == PointPart::Height;

    const bool missesThePart = (namesAGeographicPart && !isGeographic(crs))
                            || (namesAThirdCoordinate && !isThreeDimensional);

    if (missesThePart) {
        std::string text;
        format(text, value);
        throw TuringException(fmt::format("The point {} has no {}", text, getPartName(part)));
    }

    switch (part) {
        case PointPart::X:
        case PointPart::Longitude:
            return value.getX();
        break;
        case PointPart::Y:
        case PointPart::Latitude:
            return value.getY();
        break;
        case PointPart::Z:
        case PointPart::Height:
            return value.getZ();
        break;
        case PointPart::CRS:
        case PointPart::SRID:
            throw TuringException("The crs and the srid of a point are not coordinates");
        break;
    }

    return 0.0;
}

Point::Component Point::component(Point value, PointPart part) {
    if (part == PointPart::CRS) {
        return getCRSName(value.getCRS());
    } else if (part == PointPart::SRID) {
        return getSRID(value.getCRS());
    }

    return coordinate(value, part);
}

bool Point::fromMap(const MapView& map, Point& value) {
    PointKeys keys;
    if (!readKeys(map, keys)) {
        return false;
    }

    PointCRS crs {PointCRS::Cartesian};
    const bool namesACRS = namedCRS(keys, crs);

    const bool hasXY = keys.has(PointPart::X) && keys.has(PointPart::Y);
    const bool hasLongitudeLatitude = keys.has(PointPart::Longitude) && keys.has(PointPart::Latitude);

    double x = 0.0;
    double y = 0.0;
    double z = 0.0;
    size_t dimension = 2;

    if (hasXY) {
        x = keys.get(PointPart::X);
        y = keys.get(PointPart::Y);

        if (keys.has(PointPart::Z)) {
            z = keys.get(PointPart::Z);
            dimension = 3;
        }

        if (!namesACRS) {
            crs = dimension == 3 ? PointCRS::Cartesian3D : PointCRS::Cartesian;
        }
    } else if (hasLongitudeLatitude) {
        x = keys.get(PointPart::Longitude);
        y = keys.get(PointPart::Latitude);

        if (keys.has(PointPart::Z)) {
            z = keys.get(PointPart::Z);
            dimension = 3;
        } else if (keys.has(PointPart::Height)) {
            z = keys.get(PointPart::Height);
            dimension = 3;
        }

        if (!namesACRS) {
            crs = dimension == 3 ? PointCRS::WGS84_3D : PointCRS::WGS84;
        } else if (!isGeographic(crs)) {
            throw TuringException(fmt::format("A point given by longitude and latitude takes a geographic CRS, not '{}'",
                                              getCRSName(crs)));
        }
    } else if (namesACRS) {
        throw TuringException(fmt::format("A {} point needs {}", getCRSName(crs), requiredKeys(crs)));
    } else {
        throw TuringException("point() needs 'x' and 'y', or 'longitude' and 'latitude'");
    }

    const size_t crsDimension = getDimension(crs);
    if (crsDimension != dimension) {
        throw TuringException(fmt::format("A {} point has {} coordinates, not {}",
                                          getCRSName(crs),
                                          crsDimension,
                                          dimension));
    }

    value = Point(crs, x, y, z);

    return true;
}

bool Point::crsNamed(std::string_view name, PointCRS& crs) {
    for (const PointCRS system : CRS_SYSTEMS) {
        if (StringUtils::equalsIgnoringCase(getCRSName(system), name)) {
            crs = system;
            return true;
        }
    }

    return false;
}

bool Point::crsOfSRID(int64_t srid, PointCRS& crs) {
    for (const PointCRS system : CRS_SYSTEMS) {
        if (getSRID(system) == srid) {
            crs = system;
            return true;
        }
    }

    return false;
}

std::string_view Point::requiredKeys(PointCRS crs) {
    switch (crs) {
        case PointCRS::Cartesian:
            return "'x' and 'y'";
        break;
        case PointCRS::Cartesian3D:
            return "'x', 'y' and 'z'";
        break;
        case PointCRS::WGS84:
            return "'longitude' and 'latitude'";
        break;
        case PointCRS::WGS84_3D:
            return "'longitude', 'latitude' and 'height'";
        break;
    }

    return "";
}

double Point::haversineDistance(Point lhs, Point rhs, double radius) {
    constexpr double RADIANS_PER_DEGREE = std::numbers::pi / 180.0;

    const double lhsLatitude = lhs.getY();
    const double rhsLatitude = rhs.getY();

    // Subtracting in degrees keeps a point at 0 from itself and the distance the same either
    // way, which a product contracted into an FMA breaks
    const double halfLatitudeSine = sin((rhsLatitude - lhsLatitude) * RADIANS_PER_DEGREE / 2.0);
    const double halfLongitudeSine = sin((rhs.getX() - lhs.getX()) * RADIANS_PER_DEGREE / 2.0);
    const double latitudeCosines = cos(lhsLatitude * RADIANS_PER_DEGREE) * cos(rhsLatitude * RADIANS_PER_DEGREE);

    // The square of a half chord, which rounding can push past 1 between antipodes
    const double alpha = std::clamp(halfLatitudeSine * halfLatitudeSine + latitudeCosines * halfLongitudeSine * halfLongitudeSine,
                                    0.0,
                                    1.0);

    return radius * 2.0 * atan2(sqrt(alpha), sqrt(1.0 - alpha));
}

std::optional<double> Point::distance(Point lhs, Point rhs) {
    const PointCRS crs = lhs.getCRS();
    if (crs != rhs.getCRS()) {
        return std::nullopt;
    }

    const double lhsHeight = lhs.getZ();
    const double rhsHeight = rhs.getZ();
    const double heightDifference = lhsHeight - rhsHeight;

    if (!isGeographic(crs)) {
        const double xDifference = lhs.getX() - rhs.getX();
        const double yDifference = lhs.getY() - rhs.getY();

        return sqrt(xDifference * xDifference + yDifference * yDifference + heightDifference * heightDifference);
    } else if (getDimension(crs) == 2) {
        return haversineDistance(lhs, rhs, EARTH_MEAN_RADIUS_METRES);
    }

    const double averageHeight = (lhsHeight + rhsHeight) / 2.0;
    const double surfaceDistance = haversineDistance(lhs, rhs, EARTH_MEAN_RADIUS_METRES + averageHeight);

    return sqrt(surfaceDistance * surfaceDistance + heightDifference * heightDifference);
}

std::optional<bool> Point::withinBBox(Point value, Point lowerLeft, Point upperRight) {
    const PointCRS crs = value.getCRS();
    const bool sharesTheCRS = lowerLeft.getCRS() == crs && upperRight.getCRS() == crs;
    if (!sharesTheCRS) {
        return std::nullopt;
    }

    const bool withinHeights = withinRange(value.getZ(), lowerLeft.getZ(), upperRight.getZ());

    if (!isGeographic(crs)) {
        return withinRange(value.getX(), lowerLeft.getX(), upperRight.getX())
            && withinRange(value.getY(), lowerLeft.getY(), upperRight.getY())
            && withinHeights;
    }

    return withinLongitudes(value.getX(), lowerLeft.getX(), upperRight.getX())
        && withinRange(value.getY(), lowerLeft.getY(), upperRight.getY())
        && withinHeights;
}

Point Point::addToMean(Point mean, uint64_t count, Point value) {
    if (count == 0) {
        return value;
    }

    const PointCRS crs = mean.getCRS();
    const PointCRS valueCRS = value.getCRS();
    if (valueCRS != crs) {
        throw TuringException(fmt::format("avg() reads points of one CRS, not {} and {}",
                                          getCRSName(crs),
                                          getCRSName(valueCRS)));
    }

    const double meanX = mean.getX();
    const double meanY = mean.getY();
    const double meanZ = mean.getZ();

    const double total = static_cast<double>(count + 1);
    const double x = meanX + (value.getX() - meanX) / total;
    const double y = meanY + (value.getY() - meanY) / total;
    const double z = meanZ + (value.getZ() - meanZ) / total;

    return Point(crs, x, y, z);
}

void Point::format(std::string& out, Point value) {
    ValueTextScratch scratch;
    const auto output = std::back_inserter(out);

    const PointCRS crs = value.getCRS();

    fmt::format_to(output, "point({{x: {}", valueTextInto(value.getX(), scratch));
    fmt::format_to(output, ", y: {}", valueTextInto(value.getY(), scratch));

    if (getDimension(crs) == 3) {
        fmt::format_to(output, ", z: {}", valueTextInto(value.getZ(), scratch));
    }

    fmt::format_to(output, ", crs: '{}'}})", getCRSName(crs));
}

size_t std::hash<Point>::operator()(const Point& value) const noexcept {
    const std::hash<double> hasher;

    size_t hash = static_cast<size_t>(value.getCRS());
    hash = hash * 31 + hasher(value.getX());
    hash = hash * 31 + hasher(value.getY());
    hash = hash * 31 + hasher(value.getZ());

    return hash;
}
