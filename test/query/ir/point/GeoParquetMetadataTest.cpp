#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "ParquetPointImportTest.h"

using namespace turing::test;

// GeoParquet's "geo" file metadata is JSON the file's writer chose. A value of the wrong
// kind anywhere in it is malformed input, rejected with a message that names the column.
class GeoParquetMetadataTest : public ParquetPointImportTest {
};

TEST_F(GeoParquetMetadataTest, RejectsAValueOfTheWrongKindByNamingTheColumn) {
    const std::vector<std::pair<std::string_view, std::string_view>> cases = {
        {R"({"columns": {"at": 5}})",
         "The GeoParquet metadata of column 'at' is no object."},
        {R"({"columns": {"at": {"encoding": 1}}})",
         "Geometry column 'at' is encoded as '1', not as WKB."},
        {R"({"columns": {"at": {"encoding": "WKB", "crs": 5}}})",
         "Geometry column 'at' is in the CRS '5', and a point is cartesian or wgs-84."},
        {R"({"columns": {"at": {"encoding": "WKB", "crs": {"name": 7, "id": {"authority": "EPSG", "code": 3857}}}}})",
         "Geometry column 'at' is in the CRS '7', and a point is cartesian or wgs-84."},
        {R"({"columns": {"at": {"encoding": "WKB", "crs": {"name": "WGS 84", "id": {"authority": 4, "code": 4326}}}}})",
         "Geometry column 'at' is in the CRS 'WGS 84', and a point is cartesian or wgs-84."},
    };

    size_t caseIndex = 0;
    for (const auto& [geo, reason] : cases) {
        const std::string directory = "malformed" + std::to_string(caseIndex++);
        writeImport(directory, GeometryMarking::GeoParquetMetadata, geo, {wkbPoint(1, 2), wkbPoint(3, 4)});

        runQueryExpectingError("LOAD PARQUET '" + directory + "' AS " + directory, reason);
    }
}

TEST_F(GeoParquetMetadataTest, RejectsMetadataNamingNoColumns) {
    writeImport("bare", GeometryMarking::GeoParquetMetadata, "5", {wkbPoint(1, 2), wkbPoint(3, 4)});

    runQueryExpectingError("LOAD PARQUET 'bare' AS bare", "The GeoParquet metadata of the file names no columns.");
}

TEST_F(GeoParquetMetadataTest, ReadsTheEPSGCodeOfAPROJJSONCRSWrittenAsAString) {
    writeImport("cities",
                GeometryMarking::GeoParquetMetadata,
                R"({"columns": {"at": {"encoding": "WKB", "crs": {"name": "WGS 84", "id": {"authority": "EPSG", "code": "4326"}}}}})",
                {wkbPoint(2.3522, 48.8566), wkbPoint(1, 2)});

    load("LOAD PARQUET 'cities' AS cities");

    expectSortedRows("MATCH (n:City) RETURN n.at",
                     "cities",
                     {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}, {"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}
