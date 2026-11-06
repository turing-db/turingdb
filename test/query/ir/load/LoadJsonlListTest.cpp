#include <gtest/gtest.h>

#include <stddef.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

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

// LOAD JSONL builds each line's lists in a scratch it clears before the next line, so every
// list has to read back whole once the lines after it have been imported.
class LoadJsonlListTest : public TuringTest {
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

TEST_F(LoadJsonlListTest, ReadsTheListsOfEveryLine) {
    writeFixture("items.jsonl",
                 R"({"type":"node","id":"0","labels":["Item"],"properties":{"name":"a","tags":["red","green"]}}
{"type":"node","id":"1","labels":["Item"],"properties":{"name":"b","tags":["blue"],"grid":[[1,2],[3]]}}
{"type":"node","id":"2","labels":["Item"],"properties":{"name":"c","tags":[]}}
{"type":"relationship","id":"3","label":"NEXT","start":{"id":"0"},"end":{"id":"1"},"properties":{"stops":["x","y"]}}
)");

    load(R"(LOAD JSONL "items.jsonl" AS items)");

    expectSortedRows("MATCH (n:Item) RETURN n.name, n.tags, n.grid",
                     "items",
                     {{"a", "red, green", "null"}, {"b", "blue", "[1, 2], [3]"}, {"c", "", "null"}});

    expectSortedRows("MATCH ()-[r:NEXT]->() RETURN r.stops", "items", {{"x, y"}});
}

TEST_F(LoadJsonlListTest, ReadsTheListsOfAFileLongerThanOneScratchChunk) {
    constexpr size_t LINE_COUNT = 2000;

    std::string contents;
    for (size_t line = 0; line < LINE_COUNT; line++) {
        contents += fmt::format(R"({{"type":"node","id":"{0}","labels":["Item"],"properties":{{"name":"n{0}","tags":["first-{0}","second-{0}"]}}}})",
                                line);
        contents += '\n';
    }

    writeFixture("long.jsonl", contents);

    load(R"(LOAD JSONL "long.jsonl" AS long)");

    expectSortedRows("MATCH (n:Item) RETURN count(n)", "long", {{"2000"}});
    expectSortedRows("MATCH (n:Item) WHERE n.name IN ['n0', 'n1000', 'n1999'] RETURN n.name, n.tags",
                     "long",
                     {{"n0", "first-0, second-0"}, {"n1000", "first-1000, second-1000"}, {"n1999", "first-1999, second-1999"}});
}
