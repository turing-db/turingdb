#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class TemporalValueOrderTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// openCypher sorts values of different types as MAP, NODE, RELATIONSHIP, LIST, PATH,
// DATETIME, ..., DURATION, STRING, BOOLEAN, NUMBER, with null last
TEST_F(TemporalValueOrderTest, sortsInstantsAndDurationsBeforeStringsBooleansAndNumbers) {
    expectOrderedRows("UNWIND [1, true, 'a', duration(1000000), datetime('2026-01-01T00:00:00Z'), [2]] AS v "
                      "RETURN v ORDER BY v",
                      {{"[2]"}, {"2026-01-01T00:00:00Z"}, {"PT1S"}, {"a"}, {"true"}, {"1"}});
}

TEST_F(TemporalValueOrderTest, sortsDescendingInTheReverseOrder) {
    expectOrderedRows("UNWIND [1, 'a', duration(1000000), datetime('2026-01-01T00:00:00Z')] AS v "
                      "RETURN v ORDER BY v DESC",
                      {{"1"}, {"a"}, {"PT1S"}, {"2026-01-01T00:00:00Z"}});
}
