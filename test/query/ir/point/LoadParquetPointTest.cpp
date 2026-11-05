#include <gtest/gtest.h>

#include <stdint.h>
#include <string.h>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/io/file.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "FileUtils.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// How a node file marks its "at" column as holding geometries
enum class GeometryMarking {
    GeometryLogicalType,
    GeographyLogicalType,
    GeoParquetMetadata,
};

template <typename Value>
void appendValue(std::string& bytes, Value value) {
    char raw[sizeof(Value)];
    memcpy(raw, &value, sizeof(Value));
    bytes.append(raw, sizeof(Value));
}

// A little-endian ISO WKB point, 3D where @param z is given
std::string wkbPoint(double x, double y, std::optional<double> z = std::nullopt) {
    std::string bytes;
    appendValue<uint8_t>(bytes, 1);
    appendValue<uint32_t>(bytes, z ? 1001 : 1);
    appendValue(bytes, x);
    appendValue(bytes, y);

    if (z) {
        appendValue(bytes, *z);
    }

    return bytes;
}

std::string wkbOfType(uint32_t type, std::initializer_list<double> values) {
    std::string bytes;
    appendValue<uint8_t>(bytes, 1);
    appendValue<uint32_t>(bytes, type);

    for (const double value : values) {
        appendValue(bytes, value);
    }

    return bytes;
}

// A big-endian EWKB point carrying an SRID
std::string bigEndianEWKBPoint(double x, double y) {
    std::string bytes;
    bytes.push_back(0);

    const auto appendBigEndian = [&bytes]<typename Value>(Value value) {
        char raw[sizeof(Value)];
        memcpy(raw, &value, sizeof(Value));
        for (size_t index = sizeof(Value); index > 0; index--) {
            bytes.push_back(raw[index - 1]);
        }
    };

    appendBigEndian(uint32_t {0x20000001});
    appendBigEndian(uint32_t {4326});
    appendBigEndian(x);
    appendBigEndian(y);

    return bytes;
}

}

// A Parquet column holds points where Parquet's GEOMETRY or GEOGRAPHY logical type marks it,
// or GeoParquet's "geo" file metadata names it: one WKB point per row.
class LoadParquetPointTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    // Writes nodes 0, 1, ... labelled City, node i holding @param points[i] under "at", and
    // the minimal edge file, into the import directory @param directory
    void writeImport(std::string_view directory,
                     GeometryMarking marking,
                     std::string_view crs,
                     const std::vector<std::optional<std::string>>& points,
                     const std::vector<std::vector<std::string>>& routes = {}) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / std::string {directory};
        FileUtils::createDirectory(importDir);

        const FileUtils::Path fixtureDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(fixtureDir / "minimal_edges.parquet", importDir / "edges.parquet");

        namespace schema = parquet::schema;

        const std::string crsText {crs};
        std::shared_ptr<const parquet::LogicalType> atType = parquet::LogicalType::None();
        std::shared_ptr<const arrow::KeyValueMetadata> keyValues;

        if (marking == GeometryMarking::GeometryLogicalType) {
            atType = parquet::LogicalType::Geometry(crsText);
        } else if (marking == GeometryMarking::GeographyLogicalType) {
            atType = parquet::LogicalType::Geography(crsText);
        } else {
            keyValues = arrow::key_value_metadata({"geo"}, {crsText});
        }

        const schema::NodePtr element = schema::PrimitiveNode::Make("element",
                                                                    parquet::Repetition::OPTIONAL,
                                                                    parquet::LogicalType::String(),
                                                                    parquet::Type::BYTE_ARRAY);
        const schema::NodePtr list = schema::GroupNode::Make("list", parquet::Repetition::REPEATED, {element});

        schema::NodeVector fields = {
            schema::PrimitiveNode::Make("__id", parquet::Repetition::REQUIRED, parquet::Type::INT64),
            schema::GroupNode::Make("__labels", parquet::Repetition::OPTIONAL, {list}, parquet::LogicalType::List()),
            schema::PrimitiveNode::Make("at", parquet::Repetition::OPTIONAL, atType, parquet::Type::BYTE_ARRAY),
        };

        if (!routes.empty()) {
            const schema::NodePtr stop = schema::PrimitiveNode::Make("element",
                                                                     parquet::Repetition::OPTIONAL,
                                                                     atType,
                                                                     parquet::Type::BYTE_ARRAY);
            const schema::NodePtr stops = schema::GroupNode::Make("list", parquet::Repetition::REPEATED, {stop});
            fields.push_back(schema::GroupNode::Make("route", parquet::Repetition::OPTIONAL, {stops}, parquet::LogicalType::List()));
        }

        const auto root = std::static_pointer_cast<schema::GroupNode>(
            schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

        const std::string nodePath = (importDir / "nodes.parquet").string();
        auto file = arrow::io::FileOutputStream::Open(nodePath).ValueOrDie();
        std::unique_ptr<parquet::ParquetFileWriter> writer =
            parquet::ParquetFileWriter::Open(file, root, parquet::default_writer_properties(), keyValues);

        parquet::RowGroupWriter* rowGroup = writer->AppendRowGroup();
        const int64_t rowCount = static_cast<int64_t>(points.size());

        std::vector<int64_t> ids(points.size());
        for (size_t index = 0; index < points.size(); index++) {
            ids[index] = static_cast<int64_t>(index);
        }

        static_cast<parquet::Int64Writer*>(rowGroup->NextColumn())->WriteBatch(rowCount, nullptr, nullptr, ids.data());

        parquet::ByteArrayWriter* labelWriter = static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn());
        const int16_t labelDefLevel = labelWriter->descr()->max_definition_level();
        const std::vector<int16_t> labelDefLevels(points.size(), labelDefLevel);
        const std::vector<int16_t> labelRepLevels(points.size(), 0);
        const std::vector<parquet::ByteArray> labels(points.size(), parquet::ByteArray("City"));
        labelWriter->WriteBatch(rowCount, labelDefLevels.data(), labelRepLevels.data(), labels.data());

        std::vector<int16_t> atDefLevels;
        std::vector<parquet::ByteArray> atValues;
        for (const std::optional<std::string>& point : points) {
            atDefLevels.push_back(point ? 1 : 0);
            if (point) {
                atValues.emplace_back(static_cast<uint32_t>(point->size()),
                                      reinterpret_cast<const uint8_t*>(point->data()));
            }
        }

        static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn())->WriteBatch(rowCount,
                                                                                   atDefLevels.data(),
                                                                                   nullptr,
                                                                                   atValues.data());

        if (!routes.empty()) {
            parquet::ByteArrayWriter* routeWriter = static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn());
            const int16_t stopDefLevel = routeWriter->descr()->max_definition_level();

            std::vector<int16_t> routeDefLevels;
            std::vector<int16_t> routeRepLevels;
            std::vector<parquet::ByteArray> routeValues;

            for (const std::vector<std::string>& route : routes) {
                if (route.empty()) {
                    routeDefLevels.push_back(stopDefLevel - 2);
                    routeRepLevels.push_back(0);
                }

                for (size_t index = 0; index < route.size(); index++) {
                    routeDefLevels.push_back(stopDefLevel);
                    routeRepLevels.push_back(index == 0 ? 0 : 1);
                    routeValues.emplace_back(static_cast<uint32_t>(route[index].size()),
                                             reinterpret_cast<const uint8_t*>(route[index].data()));
                }
            }

            routeWriter->WriteBatch(static_cast<int64_t>(routeDefLevels.size()),
                                    routeDefLevels.data(),
                                    routeRepLevels.data(),
                                    routeValues.data());
        }

        writer->Close();
    }

    void runQuery(std::string_view query, std::string_view graphName, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void load(std::string_view query) {
        StringRowSink sink;
        runQuery(query, _sessionGraph, sink);
    }

    void runQueryExpectingError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _sessionGraph,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    void expectSortedRows(std::string_view query,
                          std::string_view graphName,
                          const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, graphName, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }

    const std::string _sessionGraph = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
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
