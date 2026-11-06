#include <gtest/gtest.h>

#include <string_view>

#include "ParquetPointImportTest.h"

using namespace turing::test;

namespace {

constexpr std::string_view UNDEFINED_CRS_GEO =
    R"({"version": "1.1.0", "primary_column": "at", "columns": {"at": {"encoding": "WKB", "crs": null}}})";

}

// An EWKB value carries the SRID of its point. A non-zero one names a CRS, which has to be
// the CRS of the point's column: a column's points are all in one CRS.
class ParquetPointSRIDTest : public ParquetPointImportTest {
};

TEST_F(ParquetPointSRIDTest, ReadsAGeographicSRIDInAGeographicColumn) {
    writeImport("cities",
                GeometryMarking::GeometryLogicalType,
                "OGC:CRS84",
                {ewkbPoint(2.3522, 48.8566, 4326), ewkbPoint(6.8652, 45.8326, 4979, 4808.0), ewkbPoint(1, 2, 4326, 3.0)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 1.0, y: 2.0, z: 3.0, crs: 'wgs-84-3d'})"},
                      {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"},
                      {"point({x: 6.8652, y: 45.8326, z: 4808.0, crs: 'wgs-84-3d'})"}});
}

TEST_F(ParquetPointSRIDTest, ReadsAnSRIDOfZeroAsNamingNoCRS) {
    writeImport("grid", GeometryMarking::GeoParquetMetadata, UNDEFINED_CRS_GEO, {ewkbPoint(1, 2, 0), wkbPoint(3, 4)});
    writeImport("cities", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {ewkbPoint(2.3522, 48.8566, 0), wkbPoint(1, 2)});

    load("LOAD PARQUET 'grid' AS grid");
    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "grid",
                     {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"point({x: 3.0, y: 4.0, crs: 'cartesian'})"}});
    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}, {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}

TEST_F(ParquetPointSRIDTest, RejectsAGeographicSRIDInAColumnOfUndefinedCRS) {
    writeImport("grid", GeometryMarking::GeoParquetMetadata, UNDEFINED_CRS_GEO, {wkbPoint(1, 2), ewkbPoint(2.3522, 48.8566, 4326)});

    runQueryExpectingError("LOAD PARQUET 'grid' AS grid",
                           "Geometry column 'at' holds a point with SRID 4326, which is not the column's CRS.");
}

TEST_F(ParquetPointSRIDTest, RejectsAProjectedSRIDInAGeographicColumn) {
    writeImport("mercator", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbPoint(1, 2), ewkbPoint(1000.0, 50.0, 3857)});

    runQueryExpectingError("LOAD PARQUET 'mercator' AS mercator",
                           "Geometry column 'at' holds a point with SRID 3857, which is not the column's CRS.");
}
