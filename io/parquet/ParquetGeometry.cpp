#include "ParquetGeometry.h"

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

#include "StringUtils.h"
#include "TuringException.h"

using namespace db;

using json = nlohmann::json;

namespace {

constexpr uint32_t WKB_POINT = 1;
constexpr uint32_t WKB_DIMENSION_STEP = 1000;
constexpr uint32_t WKB_DIMENSION_CODE_COUNT = 4;
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

bool namesAGeographicCRS(std::string_view crs) {
    constexpr std::array<std::string_view, 6> GEOGRAPHIC_CRS_NAMES = {
        "OGC:CRS84", "OGC:CRS84h", "EPSG:4326", "EPSG:4979", "srid:4326", "srid:4979",
    };

    return std::ranges::any_of(GEOGRAPHIC_CRS_NAMES, [crs](std::string_view name) {
        return StringUtils::equalsIgnoringCase(name, crs);
    });
}

// The text a JSON value spells: a string's own characters, any other value's JSON
void jsonText(const json& value, std::string& text) {
    text = value.is_string() ? value.get<std::string>() : value.dump();
}

ParquetGeometryCRS crsOfProjJson(std::string_view columnName, const json& projjson) {
    const auto id = projjson.find("id");
    const bool hasAnID = id != projjson.end() && id->is_object();

    if (hasAnID) {
        const auto authority = id->find("authority");
        const auto code = id->find("code");
        const bool namesAnAuthority = authority != id->end() && authority->is_string();
        const bool namesACode = code != id->end();

        if (namesAnAuthority && namesACode) {
            std::string codeText;
            jsonText(*code, codeText);

            if (namesAGeographicCRS(fmt::format("{}:{}", authority->get<std::string_view>(), codeText))) {
                return ParquetGeometryCRS::Geographic;
            }
        }
    }

    const auto name = projjson.find("name");
    std::string crsName;
    jsonText(name != projjson.end() ? *name : projjson, crsName);

    throw TuringException(fmt::format("Geometry column '{}' is in the CRS '{}', and a point is cartesian or wgs-84.",
                                      columnName,
                                      crsName));
}

}

ParquetGeometry::ParquetGeometry(const parquet::FileMetaData& metadata)
    : _keyValues(metadata.key_value_metadata().get())
{
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
    if (isoDimensions >= WKB_DIMENSION_CODE_COUNT) {
        throwNoWKB(columnName);
    }

    const bool hasZ = (type & EWKB_Z_FLAG) != 0 || isoDimensions == 1 || isoDimensions == 3;
    const bool hasM = (type & EWKB_M_FLAG) != 0 || isoDimensions == 2 || isoDimensions == 3;

    uint32_t srid = 0;
    const bool hasSRID = (type & EWKB_SRID_FLAG) != 0;
    if (hasSRID && !reader.read(srid)) {
        throwNoWKB(columnName);
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

    const bool isGeographic = crs == ParquetGeometryCRS::Geographic;
    PointCRS sridCRS {PointCRS::Cartesian};
    const bool sridIsGeographic = Point::crsOfSRID(srid, sridCRS) && Point::isGeographic(sridCRS);
    const bool sridNamesTheColumnCRS = srid == 0 || (isGeographic && sridIsGeographic);

    if (!sridNamesTheColumnCRS) {
        throw TuringException(fmt::format("Geometry column '{}' holds a point with SRID {}, which is not the column's CRS.",
                                          columnName,
                                          srid));
    }

    // WKB spells an empty point with NaN coordinates
    if (isnan(x) && isnan(y)) {
        return false;
    }

    const PointCRS pointCRS = isGeographic ? (hasZ ? PointCRS::WGS84_3D : PointCRS::WGS84)
                                           : (hasZ ? PointCRS::Cartesian3D : PointCRS::Cartesian);

    point = Point(pointCRS, x, y, z);

    return true;
}

// GeoParquet names its geometry columns in the JSON of the file's "geo" key. A column with no
// "crs" is in OGC:CRS84, and one whose "crs" is null in an undefined, cartesian CRS.
void ParquetGeometry::readGeoParquetColumns() {
    const std::string* geo = findKeyValue(GEOPARQUET_KEY);
    if (!geo) {
        return;
    }

    const json document = json::parse(*geo, nullptr, false);
    const auto columns = document.find("columns");
    const bool namesColumns = columns != document.end() && columns->is_object();

    if (!namesColumns) {
        throw TuringException("The GeoParquet metadata of the file names no columns.");
    }

    for (const auto& [name, column] : columns->items()) {
        if (!column.is_object()) {
            throw TuringException(fmt::format("The GeoParquet metadata of column '{}' is no object.", name));
        }

        const auto encoding = column.find("encoding");
        std::string encodingText;
        if (encoding != column.end()) {
            jsonText(*encoding, encodingText);
        }

        if (!StringUtils::equalsIgnoringCase(encodingText, "WKB")) {
            throw TuringException(fmt::format("Geometry column '{}' is encoded as '{}', not as WKB.", name, encodingText));
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

const std::string* ParquetGeometry::findKeyValue(std::string_view key) const {
    if (!_keyValues) {
        return nullptr;
    }

    const int index = _keyValues->FindKey(key);
    if (index < 0) {
        return nullptr;
    }

    return &_keyValues->value(index);
}

// Parquet's logical types name a CRS by a string, OGC:CRS84 where it is empty. A
// "projjson:<key>" one names a key of the file metadata holding the PROJJSON, and Arrow writes
// any other CRS as the PROJJSON text itself.
ParquetGeometryCRS ParquetGeometry::crsOfString(std::string_view columnName, std::string_view crs) const {
    if (crs.empty() || namesAGeographicCRS(crs)) {
        return ParquetGeometryCRS::Geographic;
    }

    const std::string* referenced = crs.starts_with(PROJJSON_PREFIX) ? findKeyValue(crs.substr(PROJJSON_PREFIX.size())) : nullptr;
    const std::string_view projjsonText = referenced ? std::string_view(*referenced) : crs;
    const json projjson = json::parse(projjsonText.begin(), projjsonText.end(), nullptr, false);

    if (projjson.is_object()) {
        return crsOfProjJson(columnName, projjson);
    }

    throw TuringException(fmt::format("Geometry column '{}' is in the CRS '{}', and a point is cartesian or wgs-84.",
                                      columnName,
                                      crs));
}
