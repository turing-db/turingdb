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

// JSON has no duration, so LOAD JSONL is told which integer properties count microseconds
// with WITH DURATIONS [...], the sibling of WITH DATETIMES.
class LoadJsonlDurationTest : public TuringTest {
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

TEST_F(LoadJsonlDurationTest, ReadsANamedIntegerAsMicroseconds) {
    writeFixture("tasks.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"name":"a","took":90061000000,"count":90061000000}}
{"type":"node","id":"1","labels":["Task"],"properties":{"name":"b","took":-1000000,"count":1}}
)");

    load(R"(LOAD JSONL "tasks.jsonl" AS tasks WITH DURATIONS ["took"])");

    expectSortedRows("MATCH (n:Task) RETURN n.name, n.took, n.count",
                     "tasks",
                     {{"a", "PT25H1M1S", "90061000000"}, {"b", "PT-1S", "1"}});

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "tasks",
                     {{"count", "Int64"}, {"name", "String"}, {"took", "Duration"}});
}

TEST_F(LoadJsonlDurationTest, ReadsADurationOnAnEdge) {
    writeFixture("edges.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{}}
{"type":"node","id":"1","labels":["Person"],"properties":{}}
{"type":"relationship","id":"0","label":"WAITED","properties":{"wait":5000000},"start":0,"end":1}
)");

    load(R"(LOAD JSONL "edges.jsonl" AS edges WITH DURATIONS ["wait"])");

    expectSortedRows("MATCH ()-[e:WAITED]->() RETURN e.wait", "edges", {{"PT5S"}});
}

TEST_F(LoadJsonlDurationTest, SkipsANullDuration) {
    writeFixture("nulls.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"name":"a","took":null}}
{"type":"node","id":"1","labels":["Task"],"properties":{"name":"b","took":2000000}}
)");

    load(R"(LOAD JSONL "nulls.jsonl" AS nulls WITH DURATIONS ["took"])");

    expectSortedRows("MATCH (n:Task) RETURN n.name, n.took",
                     "nulls",
                     {{"a", "null"}, {"b", "PT2S"}});
}

TEST_F(LoadJsonlDurationTest, ComputesOverTheDurationsItRead) {
    writeFixture("computed.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"took":90061000000}}
)");

    load(R"(LOAD JSONL "computed.jsonl" AS computed WITH DURATIONS ["took"])");

    expectSortedRows("MATCH (n:Task) RETURN n.took.hours, n.took + duration(1000000)",
                     "computed",
                     {{"25", "PT25H1M2S"}});
}

TEST_F(LoadJsonlDurationTest, RejectsAString) {
    writeFixture("string.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"took":"PT1S"}}
)");

    runQueryExpectingError(R"(LOAD JSONL "string.jsonl" AS string WITH DURATIONS ["took"])",
                           "Found a value that is not a count of microseconds in a duration property' at line 1:\nproperty 'took' reads '\"PT1S\"'");
}

TEST_F(LoadJsonlDurationTest, RejectsAFloat) {
    writeFixture("float.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"took":1.5}}
)");

    runQueryExpectingError(R"(LOAD JSONL "float.jsonl" AS float WITH DURATIONS ["took"])",
                           "Found a value that is not a count of microseconds in a duration property' at line 1:\nproperty 'took' reads '1.5'");
}

TEST_F(LoadJsonlDurationTest, RejectsACountPastInt64) {
    writeFixture("huge.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"took":9223372036854775808}}
)");

    runQueryExpectingError(R"(LOAD JSONL "huge.jsonl" AS huge WITH DURATIONS ["took"])",
                           "Found a value that is not a count of microseconds in a duration property' at line 1:\nproperty 'took' reads '9223372036854775808'");
}

TEST_F(LoadJsonlDurationTest, ReadsDateTimesAndDurationsInEitherOrder) {
    writeFixture("both.jsonl",
                 R"({"type":"node","id":"0","labels":["Task"],"properties":{"at":"2024-03-14T09:30:00Z","took":1000000}}
)");

    load(R"(LOAD JSONL "both.jsonl" AS both WITH DATETIMES ["at"] WITH DURATIONS ["took"])");
    load(R"(LOAD JSONL "both.jsonl" AS reordered WITH DURATIONS ["took"] WITH DATETIMES ["at"])");

    for (const std::string_view graphName : {"both", "reordered"}) {
        expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                         graphName,
                         {{"at", "DateTime"}, {"took", "Duration"}});
    }
}

TEST_F(LoadJsonlDurationTest, RejectsANameInBothClauses) {
    runQueryExpectingError(R"(LOAD JSONL "clash.jsonl" AS clash WITH DATETIMES ["at"] WITH DURATIONS ["at"])",
                           "Property 'at' is named both a datetime and a duration.");
}

TEST_F(LoadJsonlDurationTest, StillReadsDurationsAsAnIdentifier) {
    expectSortedRows("WITH 1 AS durations RETURN durations", _sessionGraph, {{"1"}});
}
