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

class LoadJsonlEdgeEndpointTest : public TuringTest {
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

TEST_F(LoadJsonlEdgeEndpointTest, ResolvesABareEndpointThroughTheFileIDs) {
    writeFixture("sparse.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{"name":"Alice"}}
{"type":"node","id":"5","labels":["Person"],"properties":{"name":"Bob"}}
{"type":"relationship","id":"0","label":"KNOWS","start":{"id":"0"},"end":5,"properties":{"since":2021}}
)");

    load(R"(LOAD JSONL "sparse.jsonl" AS sparse)");

    expectSortedRows("MATCH (a)-[e:KNOWS]->(b) RETURN a.name, e.since, b.name",
                     "sparse",
                     {{"Alice", "2021", "Bob"}});
}

TEST_F(LoadJsonlEdgeEndpointTest, LinksBareEndpointsToTheNodesTheyName) {
    writeFixture("offset.jsonl",
                 R"({"type":"node","id":"1","labels":["Person"],"properties":{"name":"Alice"}}
{"type":"node","id":"2","labels":["Person"],"properties":{"name":"Bob"}}
{"type":"relationship","id":"0","label":"KNOWS","start":1,"end":2,"properties":{}}
)");

    load(R"(LOAD JSONL "offset.jsonl" AS offset)");

    expectSortedRows("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
                     "offset",
                     {{"Alice", "Bob"}});
}

TEST_F(LoadJsonlEdgeEndpointTest, RejectsABareEndpointNamingNoNode) {
    writeFixture("bare_unknown.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{}}
{"type":"relationship","id":"0","label":"KNOWS","start":0,"end":7,"properties":{}}
)");

    runQueryExpectingError(R"(LOAD JSONL "bare_unknown.jsonl" AS bare_unknown)",
                           "Edge target references an unknown node id' at line 2:\nnode id 7");
}

TEST_F(LoadJsonlEdgeEndpointTest, RejectsAnObjectEndpointNamingNoNode) {
    writeFixture("object_unknown.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{}}
{"type":"relationship","id":"0","label":"KNOWS","start":{"id":"7"},"end":{"id":"0"},"properties":{}}
)");

    runQueryExpectingError(R"(LOAD JSONL "object_unknown.jsonl" AS object_unknown)",
                           "Edge source references an unknown node id' at line 2:\nnode id 7");
}
