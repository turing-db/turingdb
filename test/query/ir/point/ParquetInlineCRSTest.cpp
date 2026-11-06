#include <gtest/gtest.h>

#include "ParquetPointImportTest.h"

using namespace turing::test;

// Arrow writes the CRS of a GEOMETRY or GEOGRAPHY logical type other than OGC:CRS84 as the
// PROJJSON text itself, not as a "projjson:<key>" reference, and that text names its CRS as
// the referenced PROJJSON does.
class ParquetInlineCRSTest : public ParquetPointImportTest {
};

TEST_F(ParquetInlineCRSTest, ReadsAGeographicCRSWrittenAsInlinePROJJSON) {
    writeImport("peaks",
                GeometryMarking::GeometryLogicalType,
                R"json({"type": "GeographicCRS", "name": "WGS 84 (3D)", "id": {"authority": "EPSG", "code": 4979}})json",
                {wkbPoint(6.8652, 45.8326, 4808.0), wkbPoint(1, 2)});

    load("LOAD PARQUET 'peaks' AS peaks");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "peaks",
                     {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}, {"point({x: 6.8652, y: 45.8326, z: 4808.0, crs: 'wgs-84-3d'})"}});
}

TEST_F(ParquetInlineCRSTest, RejectsAProjectedCRSWrittenAsInlinePROJJSON) {
    writeImport("mercator",
                GeometryMarking::GeographyLogicalType,
                R"json({"type": "ProjectedCRS", "name": "WGS 84 / Pseudo-Mercator", "id": {"authority": "EPSG", "code": 3857}})json",
                {wkbPoint(1, 2), wkbPoint(3, 4)});

    runQueryExpectingError("LOAD PARQUET 'mercator' AS mercator",
                           "Geometry column 'at' is in the CRS 'WGS 84 / Pseudo-Mercator', and a point is cartesian or wgs-84.");
}
