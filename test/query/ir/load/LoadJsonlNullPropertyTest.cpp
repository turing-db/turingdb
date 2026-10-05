#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
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

// A JSONL export spells a missing column as a null, so a null property says the entity has
// no value for that name. Storing it would write the text "null" under a second property
// type, since the name already carries the type the other lines gave it.
class LoadJsonlNullPropertyTest : public TuringTest {
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

TEST_F(LoadJsonlNullPropertyTest, KeepsANodePropertyASingleType) {
    writeFixture("nodenulls.jsonl",
                 R"({"type":"node","id":"0","labels":["Measure"],"properties":{"v":1.5}}
{"type":"node","id":"1","labels":["Measure"],"properties":{"v":null}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "nodenulls.jsonl" AS nodenulls)", _sessionGraph, loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType "
                     "RETURN propertyType, valueType",
                     "nodenulls",
                     {{"v", "Double"}});

    expectSortedRows("MATCH (n:Measure) RETURN n.v", "nodenulls", {{"1.5"}, {"null"}});
}

TEST_F(LoadJsonlNullPropertyTest, KeepsAnEdgePropertyASingleType) {
    writeFixture("edgenulls.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{}}
{"type":"node","id":"1","labels":["Person"],"properties":{}}
{"type":"relationship","id":"0","label":"RATED","properties":{"w":2.5},"start":0,"end":1}
{"type":"relationship","id":"1","label":"RATED","properties":{"w":null},"start":1,"end":0}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "edgenulls.jsonl" AS edgenulls)", _sessionGraph, loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType "
                     "RETURN propertyType, valueType",
                     "edgenulls",
                     {{"w", "Double"}});

    expectSortedRows("MATCH ()-[e:RATED]->() RETURN e.w", "edgenulls", {{"2.5"}, {"null"}});
}

TEST_F(LoadJsonlNullPropertyTest, RegistersNoTypeForANameOnlyEverNull) {
    writeFixture("allnull.jsonl",
                 R"({"type":"node","id":"0","labels":["Measure"],"properties":{"v":null}}
{"type":"node","id":"1","labels":["Measure"],"properties":{"v":null}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "allnull.jsonl" AS allnull)", _sessionGraph, loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType "
                     "RETURN propertyType, valueType",
                     "allnull",
                     {});

    expectSortedRows("MATCH (n:Measure) RETURN n.v", "allnull", {{"null"}, {"null"}});
}

TEST_F(LoadJsonlNullPropertyTest, ImportsTheOtherPropertiesOfARecordHoldingANull) {
    writeFixture("mixed.jsonl",
                 R"({"type":"node","id":"0","labels":["Measure"],"properties":{"name":"a","v":1.5,"tag":"x"}}
{"type":"node","id":"1","labels":["Measure"],"properties":{"name":"b","v":null,"tag":"y"}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "mixed.jsonl" AS mixed)", _sessionGraph, loadSink);

    expectSortedRows("MATCH (n:Measure) RETURN n.name, n.v, n.tag",
                     "mixed",
                     {{"a", "1.5", "x"}, {"b", "null", "y"}});
}
