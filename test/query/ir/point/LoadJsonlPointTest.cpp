#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

// JSON has no point, so LOAD JSONL is told which object properties hold one with
// WITH POINTS [...]. An object is read as point() reads a map.
class LoadJsonlPointTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void writeFixture(std::string_view fileName, std::string_view contents) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};

        std::ofstream file(dataDir / std::string {fileName});
        ASSERT_TRUE(file.is_open());

        file << contents;
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

TEST_F(LoadJsonlPointTest, ReadsANamedObjectAsAPoint) {
    writeFixture("cities.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"name":"Paris","at":{"longitude":2.3522,"latitude":48.8566},"meta":{"x":1}}}
{"type":"node","id":"1","labels":["City"],"properties":{"name":"Tower","at":{"x":1,"y":2,"z":3}}}
{"type":"node","id":"2","labels":["City"],"properties":{"name":"Grid","at":{"x":1,"y":2,"crs":"cartesian"}}}
{"type":"node","id":"3","labels":["City"],"properties":{"name":"Peak","at":{"longitude":6.8652,"latitude":45.8326,"height":4808,"srid":4979}}}
)");

    load(R"(LOAD JSONL "cities.jsonl" AS cities WITH POINTS [at])");

    expectSortedRows("MATCH (n:City) RETURN n.name, n.at",
                     "cities",
                     {{"Grid", "point({x: 1.0, y: 2.0, crs: 'cartesian'})"},
                      {"Paris", "point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"},
                      {"Peak", "point({x: 6.8652, y: 45.8326, z: 4808.0, crs: 'wgs-84-3d'})"},
                      {"Tower", "point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})"}});

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "cities",
                     {{"at", "Point"}, {"meta", "String"}, {"name", "String"}});
}

TEST_F(LoadJsonlPointTest, ReadsAPointOnAnEdge) {
    writeFixture("roads.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"name":"a"}}
{"type":"node","id":"1","labels":["City"],"properties":{"name":"b"}}
{"type":"relationship","id":"2","label":"ROAD","start":{"id":"0"},"end":{"id":"1"},"properties":{"midpoint":{"x":0.5,"y":0.5}}}
)");

    load(R"(LOAD JSONL "roads.jsonl" AS roads WITH POINTS [midpoint])");

    expectSortedRows("MATCH ()-[r:ROAD]->() RETURN r.midpoint", "roads", {{"point({x: 0.5, y: 0.5, crs: 'cartesian'})"}});
}

TEST_F(LoadJsonlPointTest, SkipsANullPoint) {
    writeFixture("sparse.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"name":"a","at":null}}
{"type":"node","id":"1","labels":["City"],"properties":{"name":"b","at":{"x":1,"y":2}}}
)");

    load(R"(LOAD JSONL "sparse.jsonl" AS sparse WITH POINTS [at])");

    expectSortedRows("MATCH (n:City) RETURN n.name, n.at", "sparse", {{"a", "null"}, {"b", "point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(LoadJsonlPointTest, MeasuresBetweenImportedPoints) {
    writeFixture("pair.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"name":"a","at":{"x":0,"y":0}}}
{"type":"node","id":"1","labels":["City"],"properties":{"name":"b","at":{"x":3,"y":4}}}
)");

    load(R"(LOAD JSONL "pair.jsonl" AS pair WITH POINTS [at])");

    expectSortedRows("MATCH (a:City {name: 'a'}), (b:City {name: 'b'}) RETURN point.distance(a.at, b.at)",
                     "pair",
                     {{"5"}});
}

TEST_F(LoadJsonlPointTest, RejectsAValueThatIsNoObject) {
    writeFixture("text.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"at":"1,2"}}
)");

    runQueryExpectingError(R"(LOAD JSONL "text.jsonl" AS text WITH POINTS [at])", "property 'at' is not an object");
}

TEST_F(LoadJsonlPointTest, RejectsAnObjectNamingNoPoint) {
    const std::vector<std::pair<std::string_view, std::string_view>> cases = {
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":1}}})",
         "property 'at': point() needs 'x' and 'y', or 'longitude' and 'latitude'"},
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"longitude":0,"latitude":91}}})",
         "property 'at': The latitude of a wgs-84 point lies in [-90, 90], not 91"},
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":1,"y":null}}})",
         "property 'at' holds a null"},
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":[1],"y":2}}})",
         "property 'at' holds '[1]' under 'x'"},
    };

    for (const auto& [line, reason] : cases) {
        writeFixture("bad.jsonl", line);
        runQueryExpectingError(R"(LOAD JSONL "bad.jsonl" AS bad WITH POINTS [at])", reason);
    }
}

TEST_F(LoadJsonlPointTest, RejectsANameGivenTwoTypes) {
    runQueryExpectingError(R"(LOAD JSONL "clash.jsonl" AS clash WITH DATETIMES ["at"] WITH POINTS ["at"])",
                           "Property 'at' is named both a datetime and a point.");
    runQueryExpectingError(R"(LOAD JSONL "clash.jsonl" AS clash WITH POINTS ["v"] WITH EMBEDDINGS [{"v", 3}])",
                           "Property 'v' is named both an embedding and a point.");
}

TEST_F(LoadJsonlPointTest, StillReadsPointsAsAnIdentifier) {
    expectSortedRows("WITH 1 AS points RETURN points", _sessionGraph, {{"1"}});
}
