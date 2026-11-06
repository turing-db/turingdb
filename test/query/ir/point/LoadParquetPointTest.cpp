#include <gtest/gtest.h>

#include <limits>
#include <optional>

#include "ParquetPointImportTest.h"

using namespace turing::test;

// A Parquet column holds points where Parquet's GEOMETRY or GEOGRAPHY logical type marks it,
// or GeoParquet's "geo" file metadata names it: one WKB point per row.
class LoadParquetPointTest : public ParquetPointImportTest {
};

TEST_F(LoadParquetPointTest, ReadsAGeometryColumn) {
    writeImport("cities",
                GeometryMarking::GeometryLogicalType,
                "OGC:CRS84",
                {wkbPoint(2.3522, 48.8566), wkbPoint(6.8652, 45.8326, 4808.0), std::nullopt});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"null"},
                      {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"},
                      {"point({x: 6.8652, y: 45.8326, z: 4808.0, crs: 'wgs-84-3d'})"}});

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "cities",
                     {{"at", "Point"}});
}

TEST_F(LoadParquetPointTest, ReadsAGeographyColumn) {
    writeImport("cities", GeometryMarking::GeographyLogicalType, "EPSG:4326", {wkbPoint(2.3522, 48.8566), wkbPoint(0, 0)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 0.0, y: 0.0, crs: 'wgs-84'})"}, {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}

TEST_F(LoadParquetPointTest, ReadsTheDefaultCRSAsWGS84) {
    writeImport("cities", GeometryMarking::GeometryLogicalType, "", {wkbPoint(1, 2), wkbPoint(3, 4)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at.crs", "cities", {{"wgs-84"}, {"wgs-84"}});
}

TEST_F(LoadParquetPointTest, ReadsAColumnGeoParquetNames) {
    writeImport("cities",
                GeometryMarking::GeoParquetMetadata,
                R"({"version": "1.1.0", "primary_column": "at", "columns": {"at": {"encoding": "WKB", "geometry_types": ["Point"]}}})",
                {wkbPoint(2.3522, 48.8566), wkbPoint(1, 2)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}, {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}

TEST_F(LoadParquetPointTest, ReadsAnUndefinedCRSAsCartesian) {
    writeImport("grid",
                GeometryMarking::GeoParquetMetadata,
                R"({"version": "1.1.0", "primary_column": "at", "columns": {"at": {"encoding": "WKB", "crs": null}}})",
                {wkbPoint(1, 2), wkbPoint(1, 2, 3)});

    load("LOAD PARQUET 'grid' AS grid");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "grid",
                     {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})"}});
}

TEST_F(LoadParquetPointTest, ReadsAPROJJSONCRS) {
    writeImport("cities",
                GeometryMarking::GeoParquetMetadata,
                R"({"version": "1.1.0", "primary_column": "at", "columns": {"at": {"encoding": "WKB",
                    "crs": {"type": "GeographicCRS", "name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}}}})",
                {wkbPoint(1, 2), wkbPoint(3, 4)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at.srid", "cities", {{"4326"}, {"4326"}});
}

TEST_F(LoadParquetPointTest, ReadsBigEndianEWKB) {
    writeImport("cities", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {bigEndianEWKBPoint(2.3522, 48.8566), wkbPoint(1, 2)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}, {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}

TEST_F(LoadParquetPointTest, ReadsAnEmptyPointAsNull) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    writeImport("cities", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbPoint(nan, nan), wkbPoint(1, 2)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at", "cities", {{"null"}, {"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}});
}

TEST_F(LoadParquetPointTest, MeasuresBetweenImportedPoints) {
    writeImport("cities", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbPoint(0, 0), wkbPoint(1, 0)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (a:City), (b:City) WHERE id(a) < id(b) RETURN point.distance(a.at, b.at)",
                     "cities",
                     {{"111195.0802335329"}});
}

TEST_F(LoadParquetPointTest, RejectsAGeometryThatIsNoPoint) {
    writeImport("lines", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbOfType(2, {}), wkbPoint(1, 2)});

    runQueryExpectingError("LOAD PARQUET 'lines' AS lines", "Geometry column 'at' holds a LineString, not a point.");
}

TEST_F(LoadParquetPointTest, RejectsAPointWithAMeasure) {
    writeImport("measured", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbOfType(2001, {1, 2, 3}), wkbPoint(1, 2)});

    runQueryExpectingError("LOAD PARQUET 'measured' AS measured", "Geometry column 'at' holds a point with a measure");
}

TEST_F(LoadParquetPointTest, RejectsBytesThatAreNoWKB) {
    writeImport("truncated", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbPoint(1, 2).substr(0, 9), wkbPoint(1, 2)});

    runQueryExpectingError("LOAD PARQUET 'truncated' AS truncated", "Geometry column 'at' holds a value that is no WKB.");
}

TEST_F(LoadParquetPointTest, RejectsACRSNoPointIsIn) {
    writeImport("mercator", GeometryMarking::GeometryLogicalType, "EPSG:3857", {wkbPoint(1, 2), wkbPoint(3, 4)});

    runQueryExpectingError("LOAD PARQUET 'mercator' AS mercator",
                           "Geometry column 'at' is in the CRS 'EPSG:3857', and a point is cartesian or wgs-84.");
}

TEST_F(LoadParquetPointTest, RejectsAnEncodingThatIsNoWKB) {
    writeImport("native",
                GeometryMarking::GeoParquetMetadata,
                R"({"version": "1.1.0", "primary_column": "at", "columns": {"at": {"encoding": "point"}}})",
                {wkbPoint(1, 2), wkbPoint(3, 4)});

    runQueryExpectingError("LOAD PARQUET 'native' AS native", "Geometry column 'at' is encoded as 'point', not as WKB.");
}

TEST_F(LoadParquetPointTest, RejectsALatitudePastAPole) {
    writeImport("pole", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbPoint(0, 91), wkbPoint(1, 2)});

    runQueryExpectingError("LOAD PARQUET 'pole' AS pole", "The latitude of a wgs-84 point lies in [-90, 90], not 91");
}

TEST_F(LoadParquetPointTest, ReadsAListOfGeometries) {
    writeImport("routes",
                GeometryMarking::GeometryLogicalType,
                "OGC:CRS84",
                {wkbPoint(0, 0), wkbPoint(1, 1)},
                {{wkbPoint(0, 0), wkbPoint(0, 1, 5.0)}, {}});

    load("LOAD PARQUET 'routes' AS routes");

    expectSortedRows("MATCH (n:City) RETURN n.at, n.route",
                     "routes",
                     {{"point({x: 0.0, y: 0.0, crs: 'wgs-84'})",
                       "point({x: 0.0, y: 0.0, crs: 'wgs-84'}), point({x: 0.0, y: 1.0, z: 5.0, crs: 'wgs-84-3d'})"},
                      {"point({x: 1.0, y: 1.0, crs: 'wgs-84'})", ""}});
}
