#include "ParquetGeometry.h"

#include <ctype.h>
#include <math.h>
#include <string.h>

#include <algorithm>
#include <array>
#include <bit>

#include <arrow/util/key_value_metadata.h>
#include <nlohmann/json.hpp>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>
#include <spdlog/fmt/fmt.h>

#include "TuringException.h"

using namespace db;

using json = nlohmann::json;

namespace {

constexpr uint32_t WKB_POINT = 1;
constexpr uint32_t WKB_DIMENSION_STEP = 1000;
constexpr uint32_t EWKB_Z_FLAG = 0x80000000;
constexpr uint32_t EWKB_M_FLAG = 0x40000000;
constexpr uint32_t EWKB_SRID_FLAG = 0x20000000;
constexpr uint32_t EWKB_FLAGS = EWKB_Z_FLAG | EWKB_M_FLAG | EWKB_SRID_FLAG;

constexpr std::string_view GEOPARQUET_KEY = "geo";
constexpr std::string_view PROJJSON_PREFIX = "projjson:";

class WKBReader {
public:
    explicit WKBReader(std::string_view bytes)
        : _bytes(bytes)
    {
    }

    template <typename Value>
    bool read(Value& value) {
        if (_offset + sizeof(Value) > _bytes.size()) {
            return false;
        }

        std::array<char, sizeof(Value)> raw;
        memcpy(raw.data(), _bytes.data() + _offset, sizeof(Value));

        const bool swapsBytes = _bigEndian != (std::endian::native == std::endian::big);
        if (swapsBytes) {
            std::reverse(raw.begin(), raw.end());
        }

        memcpy(&value, raw.data(), sizeof(Value));
        _offset += sizeof(Value);

        return true;
    }

    void setBigEndian(bool bigEndian) { _bigEndian = bigEndian; }

    bool atEnd() const { return _offset == _bytes.size(); }

private:
    std::string_view _bytes;
    size_t _offset {0};
    bool _bigEndian {false};
};

std::string_view geometryName(uint32_t kind) {
    switch (kind) {
        case 1:
            return "Point";
        break;
        case 2:
            return "LineString";
        break;
        case 3:
            return "Polygon";
        break;
        case 4:
            return "MultiPoint";
        break;
        case 5:
            return "MultiLineString";
        break;
        case 6:
            return "MultiPolygon";
        break;
        case 7:
            return "GeometryCollection";
        break;
        default:
            return "geometry of an unknown type";
        break;
    }
}

[[noreturn]] void throwNoWKB(std::string_view columnName) {
    throw TuringException(fmt::format("Geometry column '{}' holds a value that is no WKB.", columnName));
}

bool equalsIgnoringCase(std::string_view lhs, std::string_view rhs) {
    return lhs.size() == rhs.size()
        && std::equal(lhs.begin(), lhs.end(), rhs.begin(), [](char lhsCharacter, char rhsCharacter) {
               return tolower(static_cast<unsigned char>(lhsCharacter)) == tolower(static_cast<unsigned char>(rhsCharacter));
           });
}

bool namesAGeographicCRS(std::string_view crs) {
    constexpr std::array<std::string_view, 6> geographic = {
        "OGC:CRS84", "OGC:CRS84h", "EPSG:4326", "EPSG:4979", "srid:4326", "srid:4979",
    };

    return std::ranges::any_of(geographic, [crs](std::string_view name) {
        return equalsIgnoringCase(name, crs);
    });
}

ParquetGeometryCRS crsOfProjJson(std::string_view columnName, const json& projjson) {
    const auto id = projjson.find("id");
    if (id != projjson.end() && id->is_object()) {
        const std::string authority = id->value("authority", "");
        const json& code = id->value("code", json());
        const std::string codeText = code.is_string() ? code.get<std::string>() : code.dump();

        if (namesAGeographicCRS(fmt::format("{}:{}", authority, codeText))) {
            return ParquetGeometryCRS::Geographic;
        }
    }

    throw TuringException(fmt::format("Geometry column '{}' is in the CRS '{}', and a point is cartesian or wgs-84.",
                                      columnName,
                                      projjson.value("name", projjson.dump())));
}

}

ParquetGeometry::ParquetGeometry(const parquet::FileMetaData& metadata) {
    const std::shared_ptr<const parquet::KeyValueMetadata>& keyValues = metadata.key_value_metadata();

    if (keyValues) {
        for (int64_t index = 0; index < keyValues->size(); index++) {
            _keyValues.emplace(keyValues->key(index), keyValues->value(index));
        }
    }

    readGeoParquetColumns();
}

ParquetGeometry::~ParquetGeometry() {
}

bool ParquetGeometry::findColumn(const std::string& name,
                                 const parquet::ColumnDescriptor& descriptor,
                                 ParquetGeometryCRS& crs) const {
    const parquet::LogicalType* logicalType = descriptor.logical_type().get();

    if (logicalType && logicalType->is_geometry()) {
        crs = crsOfString(name, static_cast<const parquet::GeometryLogicalType&>(*logicalType).crs());
        return true;
    } else if (logicalType && logicalType->is_geography()) {
        crs = crsOfString(name, static_cast<const parquet::GeographyLogicalType&>(*logicalType).crs());
        return true;
    }

    const auto it = _geoParquetColumns.find(name);
    if (it == _geoParquetColumns.end()) {
        return false;
    }

    crs = it->second;

    return true;
}

bool ParquetGeometry::readPoint(std::string_view columnName,
                                std::string_view wkb,
                                ParquetGeometryCRS crs,
                                Point& point) {
    WKBReader reader(wkb);

    uint8_t byteOrder = 0;
    if (!reader.read(byteOrder) || byteOrder > 1) {
        throwNoWKB(columnName);
    }

    reader.setBigEndian(byteOrder == 0);

    uint32_t type = 0;
    if (!reader.read(type)) {
        throwNoWKB(columnName);
    }

    // EWKB flags its dimensions in the top bits, ISO WKB in the thousands of the type
    const uint32_t isoType = type & ~EWKB_FLAGS;
    const uint32_t kind = isoType % WKB_DIMENSION_STEP;
    const uint32_t isoDimensions = isoType / WKB_DIMENSION_STEP;

    const bool hasZ = (type & EWKB_Z_FLAG) != 0 || isoDimensions == 1 || isoDimensions == 3;
    const bool hasM = (type & EWKB_M_FLAG) != 0 || isoDimensions == 2 || isoDimensions == 3;

    if ((type & EWKB_SRID_FLAG) != 0) {
        uint32_t srid = 0;
        if (!reader.read(srid)) {
            throwNoWKB(columnName);
        }
    }

    if (kind != WKB_POINT) {
        throw TuringException(fmt::format("Geometry column '{}' holds a {}, not a point.", columnName, geometryName(kind)));
    } else if (hasM) {
        throw TuringException(fmt::format("Geometry column '{}' holds a point with a measure, which no point carries.",
                                          columnName));
    }

    double x = 0.0;
    double y = 0.0;
    double z = 0.0;
    const bool readsCoordinates = reader.read(x) && reader.read(y) && (!hasZ || reader.read(z));

    if (!readsCoordinates || !reader.atEnd()) {
        throwNoWKB(columnName);
    }

    // WKB spells an empty point with NaN coordinates
    if (isnan(x) && isnan(y)) {
        return false;
    }

    const bool isGeographic = crs == ParquetGeometryCRS::Geographic;
    const PointCRS pointCRS = isGeographic ? (hasZ ? PointCRS::WGS84_3D : PointCRS::WGS84)
                                           : (hasZ ? PointCRS::Cartesian3D : PointCRS::Cartesian);

    point = Point(pointCRS, x, y, z);

    return true;
}

// GeoParquet names its geometry columns in the JSON of the file's "geo" key. A column with no
// "crs" is in OGC:CRS84, and one whose "crs" is null in an undefined, cartesian CRS.
void ParquetGeometry::readGeoParquetColumns() {
    const auto geo = _keyValues.find(std::string(GEOPARQUET_KEY));
    if (geo == _keyValues.end()) {
        return;
    }

    const json document = json::parse(geo->second, nullptr, false);
    if (document.is_discarded() || !document.contains("columns") || !document["columns"].is_object()) {
        throw TuringException("The GeoParquet metadata of the file names no columns.");
    }

    for (const auto& [name, column] : document["columns"].items()) {
        const std::string encoding = column.value("encoding", "");
        if (!equalsIgnoringCase(encoding, "WKB")) {
            throw TuringException(fmt::format("Geometry column '{}' is encoded as '{}', not as WKB.", name, encoding));
        }

        const auto crs = column.find("crs");
        if (crs == column.end()) {
            _geoParquetColumns.emplace(name, ParquetGeometryCRS::Geographic);
        } else if (crs->is_null()) {
            _geoParquetColumns.emplace(name, ParquetGeometryCRS::Cartesian);
        } else if (crs->is_string()) {
            _geoParquetColumns.emplace(name, crsOfString(name, crs->get<std::string>()));
        } else {
            _geoParquetColumns.emplace(name, crsOfProjJson(name, *crs));
        }
    }
}

// Parquet's logical types name a CRS by a string, OGC:CRS84 where it is empty. A
// "projjson:<key>" one names a key of the file metadata holding the PROJJSON.
ParquetGeometryCRS ParquetGeometry::crsOfString(std::string_view columnName, std::string_view crs) const {
    if (crs.empty() || namesAGeographicCRS(crs)) {
        return ParquetGeometryCRS::Geographic;
    }

    if (crs.starts_with(PROJJSON_PREFIX)) {
        const auto projjson = _keyValues.find(std::string(crs.substr(PROJJSON_PREFIX.size())));
        if (projjson != _keyValues.end()) {
            const json document = json::parse(projjson->second, nullptr, false);
            if (!document.is_discarded()) {
                return crsOfProjJson(columnName, document);
            }
        }
    }

    throw TuringException(fmt::format("Geometry column '{}' is in the CRS '{}', and a point is cartesian or wgs-84.",
                                      columnName,
                                      crs));
}
