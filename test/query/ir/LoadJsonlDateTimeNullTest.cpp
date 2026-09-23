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

// A JSONL export spells a missing column as a null, so a null in a property the query named
// as a datetime says the entity has no instant - not that the file contradicts the clause.
// The property is absent on that entity and the rest of the file still imports.
class LoadJsonlDateTimeNullTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
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
                              &_env->getMem(),
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

TEST_F(LoadJsonlDateTimeNullTest, ImportsARecordWhoseInstantIsNull) {
    writeFixture("nulls.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"name":"a","created":"2024-03-14T09:30:00Z"}}
{"type":"node","id":"1","labels":["Event"],"properties":{"name":"b","created":null}}
{"type":"node","id":"2","labels":["Event"],"properties":{"name":"c","created":"2025-01-01T00:00:00Z"}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "nulls.jsonl" AS nulls WITH DATETIMES ["created"])",
             _sessionGraph,
             loadSink);

    // The null record is imported, and every record after it - the file is not cut short
    expectSortedRows("MATCH (n:Event) RETURN n.name", "nulls", {{"a"}, {"b"}, {"c"}});

    expectSortedRows("MATCH (n:Event) RETURN n.name, n.created",
                     "nulls",
                     {{"a", "2024-03-14T09:30:00Z"},
                      {"b", "null"},
                      {"c", "2025-01-01T00:00:00Z"}});
}

// The null leaves the property absent rather than registering a second one holding text,
// so the name still carries one type
TEST_F(LoadJsonlDateTimeNullTest, KeepsTheInstantPropertyASingleDateTime) {
    writeFixture("onetype.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"2024-03-14T09:30:00Z"}}
{"type":"node","id":"1","labels":["Event"],"properties":{"created":null}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "onetype.jsonl" AS onetype WITH DATETIMES ["created"])",
             _sessionGraph,
             loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType "
                     "RETURN propertyType, valueType",
                     "onetype",
                     {{"created", "DateTime"}});
}

TEST_F(LoadJsonlDateTimeNullTest, ImportsANullInstantOnAnEdge) {
    writeFixture("edgenulls.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{}}
{"type":"node","id":"1","labels":["Person"],"properties":{}}
{"type":"relationship","id":"0","label":"MET","properties":{"at":null},"start":0,"end":1}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "edgenulls.jsonl" AS edgenulls WITH DATETIMES ["at"])",
             _sessionGraph,
             loadSink);

    expectSortedRows("MATCH ()-[e:MET]->() RETURN e.at", "edgenulls", {{"null"}});
}
