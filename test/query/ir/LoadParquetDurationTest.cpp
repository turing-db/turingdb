#include <gtest/gtest.h>

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

// Parquet has no duration type, so LOAD PARQUET is told which INT64 columns count
// microseconds with WITH DURATIONS [...].
class LoadParquetDurationTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / "durations";
        FileUtils::createDirectory(importDir);

        const FileUtils::Path fixtureDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(fixtureDir / "duration_property_nodes.parquet", importDir / "nodes.parquet");
        FileUtils::copy(fixtureDir / "duration_property_edges.parquet", importDir / "edges.parquet");
    }

protected:
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

    void runQueryExpectingError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _sessionGraph,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
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

TEST_F(LoadParquetDurationTest, ReadsNamedInt64ColumnsAsMicroseconds) {
    StringRowSink loadSink;
    runQuery("LOAD PARQUET 'durations' AS tasks WITH DURATIONS ['elapsed', 'laps', 'wait']",
             _sessionGraph,
             loadSink);

    expectSortedRows("MATCH (n:Task) RETURN n.count, n.elapsed, n.laps",
                     "tasks",
                     {{"1", "PT25H1M1S", "PT1S, PT2S"}, {"2", "PT-1S", ""}, {"3", "null", "null"}});

    expectSortedRows("MATCH ()-[e:WAITED]->() RETURN e.wait", "tasks", {{"PT5S"}});

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType "
                     "WHERE propertyType IN ['count', 'elapsed', 'laps', 'wait'] RETURN propertyType, valueType",
                     "tasks",
                     {{"count", "Int64"}, {"elapsed", "Duration"}, {"laps", "List"}, {"wait", "Duration"}});
}

TEST_F(LoadParquetDurationTest, LeavesInt64ColumnsAloneWithNoClause) {
    StringRowSink loadSink;
    runQuery("LOAD PARQUET 'durations' AS plain", _sessionGraph, loadSink);

    expectSortedRows("MATCH ()-[e:WAITED]->() RETURN e.wait", "plain", {{"5000000"}});
}

TEST_F(LoadParquetDurationTest, RejectsATimestampColumn) {
    runQueryExpectingError("LOAD PARQUET 'durations' AS timestamps WITH DURATIONS ['startedAt']",
                           "Duration property 'startedAt' must be an INT64 column counting microseconds.");
}

TEST_F(LoadParquetDurationTest, RejectsAStringColumn) {
    runQueryExpectingError("LOAD PARQUET 'durations' AS strings WITH DURATIONS ['note']",
                           "Duration property 'note' must be an INT64 column counting microseconds.");
}
