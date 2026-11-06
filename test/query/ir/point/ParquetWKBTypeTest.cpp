#include <gtest/gtest.h>

#include <stdint.h>

#include <string>

#include "ParquetPointImportTest.h"

using namespace turing::test;

// ISO WKB spells the dimensions of a geometry in the thousands of its type: 0 for XY, 1 for
// XYZ, 2 for XYM and 3 for XYZM. A type with any other thousands is no WKB.
class ParquetWKBTypeTest : public ParquetPointImportTest {
};

TEST_F(ParquetWKBTypeTest, ReadsEachISODimensionCodeOfAPoint) {
    writeImport("cities", GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbOfType(1, {1, 2}), wkbOfType(1001, {3, 4, 5})});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}, {"point({x: 3.0, y: 4.0, z: 5.0, crs: 'wgs-84-3d'})"}});
}

TEST_F(ParquetWKBTypeTest, RejectsATypeWithNoISODimensionCode) {
    for (const uint32_t type : {4001u, 1000001u, 9001u}) {
        const std::string directory = "dimensions" + std::to_string(type);
        writeImport(directory, GeometryMarking::GeometryLogicalType, "OGC:CRS84", {wkbOfType(type, {1, 2}), wkbPoint(1, 2)});

        runQueryExpectingError("LOAD PARQUET '" + directory + "' AS " + directory,
                               "Geometry column 'at' holds a value that is no WKB.");
    }
}
