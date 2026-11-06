#pragma once

#include <stdint.h>

#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "PointImportTest.h"

namespace turing::test {

// How a node file marks its "at" column as holding geometries
enum class GeometryMarking {
    GeometryLogicalType,
    GeographyLogicalType,
    GeoParquetMetadata,
};

// A little-endian ISO WKB point, 3D where @param z is given
std::string wkbPoint(double x, double y, std::optional<double> z = std::nullopt);

std::string wkbOfType(uint32_t type, std::initializer_list<double> values);

// A big-endian EWKB point carrying the SRID 4326
std::string bigEndianEWKBPoint(double x, double y);

// A little-endian EWKB point carrying @param srid, 3D where @param z is given
std::string ewkbPoint(double x, double y, uint32_t srid, std::optional<double> z = std::nullopt);

// A PointImportTest that writes its imports as Parquet node and edge files
class ParquetPointImportTest : public PointImportTest {
protected:
    // Writes nodes 0, 1, ... labelled City, node i holding @param points[i] under "at", and
    // the minimal edge file, into the import directory @param directory. @param crs is the
    // logical type's CRS, or the whole "geo" metadata for GeoParquetMetadata.
    void writeImport(std::string_view directory,
                     GeometryMarking marking,
                     std::string_view crs,
                     const std::vector<std::optional<std::string>>& points,
                     const std::vector<std::vector<std::string>>& routes = {});
};

}
