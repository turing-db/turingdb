#pragma once

#include <stdint.h>

#include <string>
#include <string_view>
#include <unordered_map>

#include "metadata/Point.h"

namespace arrow {
class KeyValueMetadata;
}

namespace parquet {
class ColumnDescriptor;
class FileMetaData;
}

namespace db {

// The dimension of each point comes from its own WKB, so a column names no 3D CRS
enum class ParquetGeometryCRS : uint8_t {
    Geographic,
    Cartesian,
};

// The geometry columns of a Parquet file, which hold one WKB value per row: the columns
// Parquet's GEOMETRY or GEOGRAPHY logical type marks, and the ones GeoParquet's "geo" file
// metadata names. It reads the file metadata it is built from, which outlives it.
class ParquetGeometry {
public:
    explicit ParquetGeometry(const parquet::FileMetaData& metadata);
    ~ParquetGeometry();

    // Whether the column holds geometries, and the CRS of their points. Raises for a CRS no
    // point is in.
    bool findColumn(const std::string& name,
                    const parquet::ColumnDescriptor& descriptor,
                    ParquetGeometryCRS& crs) const;

    // The point a WKB value holds, false for an empty point. Raises on a value that holds
    // another geometry, a measure, an SRID that is not the column's CRS, or bytes that are
    // no WKB.
    static bool readPoint(std::string_view columnName,
                          std::string_view wkb,
                          ParquetGeometryCRS crs,
                          Point& point);

private:
    const arrow::KeyValueMetadata* _keyValues {nullptr};
    std::unordered_map<std::string, ParquetGeometryCRS> _geoParquetColumns;

    void readGeoParquetColumns();

    const std::string* findKeyValue(std::string_view key) const;

    ParquetGeometryCRS crsOfString(std::string_view columnName, std::string_view crs) const;
};

}
