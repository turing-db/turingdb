#include "Point.h"

#include <ctype.h>
#include <math.h>
#include <stdint.h>

#include <array>
#include <iterator>
#include <unordered_map>
#include <utility>

#include <spdlog/fmt/fmt.h>

#include "QueryTypeName.h"
#include "TuringException.h"
#include "columns/ValueText.h"
#include "map/MapView.h"

using namespace db;

namespace {

enum class PointKey : uint8_t {
    X = 0,
    Y,
    Z,
    Longitude,
    Latitude,
    Height,
    CRS,
    SRID,
    _SIZE,
};

struct PointKeys {
    std::array<double, static_cast<size_t>(PointKey::_SIZE)> _coordinates {};
    std::array<bool, static_cast<size_t>(PointKey::_SIZE)> _present {};
    std::string_view _crsName;
    int64_t _srid {0};

    bool has(PointKey key) const { return _present[static_cast<size_t>(key)]; }
    double get(PointKey key) const { return _coordinates[static_cast<size_t>(key)]; }
};

bool equalsIgnoringCase(std::string_view lhs, std::string_view rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (size_t index = 0; index < lhs.size(); index++) {
        const int lhsCharacter = tolower(static_cast<unsigned char>(lhs[index]));
        const int rhsCharacter = tolower(static_cast<unsigned char>(rhs[index]));

        if (lhsCharacter != rhsCharacter) {
            return false;
        }
    }

    return true;
}

bool keyNamed(std::string_view name, PointKey& key) {
    static constexpr std::array<std::pair<std::string_view, PointKey>, 8> keys = {{
        {"x",         PointKey::X        },
        {"y",         PointKey::Y        },
        {"z",         PointKey::Z        },
        {"longitude", PointKey::Longitude},
        {"latitude",  PointKey::Latitude },
        {"height",    PointKey::Height   },
        {"crs",       PointKey::CRS      },
        {"srid",      PointKey::SRID     },
    }};

    for (const auto& [keyName, namedKey] : keys) {
        if (equalsIgnoringCase(keyName, name)) {
            key = namedKey;
            return true;
        }
    }

    return false;
}

bool crsNamed(std::string_view name, PointCRS& crs) {
    static constexpr std::array<PointCRS, 4> systems = {
        PointCRS::Cartesian,
        PointCRS::Cartesian3D,
        PointCRS::WGS84,
        PointCRS::WGS84_3D,
    };

    for (const PointCRS system : systems) {
        if (equalsIgnoringCase(Point::getCRSName(system), name)) {
            crs = system;
            return true;
        }
    }

    return false;
}

bool crsOfSRID(int64_t srid, PointCRS& crs) {
    if (srid == 7203) {
        crs = PointCRS::Cartesian;
    } else if (srid == 9157) {
        crs = PointCRS::Cartesian3D;
    } else if (srid == 4326) {
        crs = PointCRS::WGS84;
    } else if (srid == 4979) {
        crs = PointCRS::WGS84_3D;
    } else {
        return false;
    }

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

        PointKey key {PointKey::X};
        if (!keyNamed(name, key)) {
            continue;
        }

        const size_t index = static_cast<size_t>(key);
        if (keys._present[index]) {
            throw TuringException(fmt::format("point() reads the key '{}' twice", name));
        }

        keys._present[index] = true;

        if (key == PointKey::CRS) {
            if (tag != CellTag::String) {
                throw TuringException(fmt::format("point() reads a string for 'crs', not '{}'", queryTypeName(tag)));
            }

            keys._crsName = value.getAs<std::string_view>();
        } else if (key == PointKey::SRID) {
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
    const bool namesACRS = keys.has(PointKey::CRS);
    const bool namesAnSRID = keys.has(PointKey::SRID);

    if (namesACRS && namesAnSRID) {
        throw TuringException("point() takes a 'crs' or an 'srid', not both");
    } else if (namesACRS) {
        if (!crsNamed(keys._crsName, crs)) {
            throw TuringException(fmt::format("Unknown coordinate reference system: '{}'", keys._crsName));
        }

        return true;
    } else if (namesAnSRID) {
        if (!crsOfSRID(keys._srid, crs)) {
            throw TuringException(fmt::format("Unknown coordinate reference system SRID: {}", keys._srid));
        }

        return true;
    }

    return false;
}

std::string_view requiredKeys(PointCRS crs) {
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

double finiteCoordinate(double coordinate) {
    if (!isfinite(coordinate)) {
        throw TuringException("A point cannot have a non-finite coordinate");
    }

    return coordinate == 0.0 ? 0.0 : coordinate;
}

double wrappedLongitude(double longitude) {
    if (longitude <= 180.0 && longitude >= -180.0) {
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

bool withinRange(double value, double lower, double upper) {
    return value >= lower && value <= upper;
}

}

Point::Point(PointCRS crs, double x, double y, double z)
    : _x(finiteCoordinate(x)),
    _y(finiteCoordinate(y)),
    _z(getDimension(crs) == 3 ? finiteCoordinate(z) : 0.0),
    _crs(crs),
    _padding {}
{
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
    static const std::unordered_map<std::string_view, PointPart> parts = {
        {"x",         PointPart::X        },
        {"y",         PointPart::Y        },
        {"z",         PointPart::Z        },
        {"longitude", PointPart::Longitude},
        {"latitude",  PointPart::Latitude },
        {"height",    PointPart::Height   },
        {"crs",       PointPart::CRS      },
        {"srid",      PointPart::SRID     },
    };

    const auto it = parts.find(name);
    if (it == parts.end()) {
        return false;
    }

    part = it->second;

    return true;
}

std::string_view Point::getPartName(PointPart part) {
    switch (part) {
        case PointPart::X:
            return "x";
        break;
        case PointPart::Y:
            return "y";
        break;
        case PointPart::Z:
            return "z";
        break;
        case PointPart::Longitude:
            return "longitude";
        break;
        case PointPart::Latitude:
            return "latitude";
        break;
        case PointPart::Height:
            return "height";
        break;
        case PointPart::CRS:
            return "crs";
        break;
        case PointPart::SRID:
            return "srid";
        break;
    }

    return "";
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

bool Point::fromMap(const MapView& map, Point& value) {
    PointKeys keys;
    if (!readKeys(map, keys)) {
        return false;
    }

    PointCRS crs {PointCRS::Cartesian};
    const bool namesACRS = namedCRS(keys, crs);

    const bool hasXY = keys.has(PointKey::X) && keys.has(PointKey::Y);
    const bool hasLongitudeLatitude = keys.has(PointKey::Longitude) && keys.has(PointKey::Latitude);

    double x = 0.0;
    double y = 0.0;
    double z = 0.0;
    size_t dimension = 2;

    if (hasXY) {
        x = keys.get(PointKey::X);
        y = keys.get(PointKey::Y);

        if (keys.has(PointKey::Z)) {
            z = keys.get(PointKey::Z);
            dimension = 3;
        }

        if (!namesACRS) {
            crs = dimension == 3 ? PointCRS::Cartesian3D : PointCRS::Cartesian;
        }
    } else if (hasLongitudeLatitude) {
        x = keys.get(PointKey::Longitude);
        y = keys.get(PointKey::Latitude);

        if (keys.has(PointKey::Z)) {
            z = keys.get(PointKey::Z);
            dimension = 3;
        } else if (keys.has(PointKey::Height)) {
            z = keys.get(PointKey::Height);
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

    if (getDimension(crs) != dimension) {
        throw TuringException(fmt::format("A {} point has {} coordinates, not {}",
                                          getCRSName(crs),
                                          getDimension(crs),
                                          dimension));
    }

    value = Point(crs, x, y, z);

    return true;
}

void Point::format(std::string& out, Point value) {
    ValueTextScratch scratch;
    const auto output = std::back_inserter(out);

    fmt::format_to(output, "point({{x: {}", valueTextInto(value.getX(), scratch));
    fmt::format_to(output, ", y: {}", valueTextInto(value.getY(), scratch));

    if (getDimension(value.getCRS()) == 3) {
        fmt::format_to(output, ", z: {}", valueTextInto(value.getZ(), scratch));
    }

    fmt::format_to(output, ", crs: '{}'}})", getCRSName(value.getCRS()));
}

size_t std::hash<Point>::operator()(const Point& value) const noexcept {
    const std::hash<double> hasher;

    size_t hash = static_cast<size_t>(value.getCRS());
    hash = hash * 31 + hasher(value.getX());
    hash = hash * 31 + hasher(value.getY());
    hash = hash * 31 + hasher(value.getZ());

    return hash;
}
