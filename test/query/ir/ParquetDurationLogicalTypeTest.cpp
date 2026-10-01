#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

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

// An INT64 column whose logical type names what it counts - an unsigned integer, a time
// of day - does not count signed microseconds, so WITH DURATIONS turns it away.
class ParquetDurationLogicalTypeTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / "durations";
        FileUtils::createDirectory(importDir);

        const FileUtils::Path fixtureDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(fixtureDir / "duration_logical_type_nodes.parquet", importDir / "nodes.parquet");
        FileUtils::copy(fixtureDir / "minimal_edges.parquet", importDir / "edges.parquet");
    }

protected:
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

    const std::string _sessionGraph = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ParquetDurationLogicalTypeTest, RejectsAnUnsignedColumn) {
    runQueryExpectingError("LOAD PARQUET 'durations' AS unsigned WITH DURATIONS ['unsignedCount']",
                           "Duration property 'unsignedCount' must be an INT64 column counting microseconds.");
}

TEST_F(ParquetDurationLogicalTypeTest, RejectsATimeOfDayColumn) {
    runQueryExpectingError("LOAD PARQUET 'durations' AS times WITH DURATIONS ['timeOfDay']",
                           "Duration property 'timeOfDay' must be an INT64 column counting microseconds.");
}
