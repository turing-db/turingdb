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

class DateTimeCellComparisonTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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
                              &_env->getMem(),
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

TEST_F(DateTimeCellComparisonTest, comparesAnUnwoundCellForEqualityWithAnInstant) {
    expectOrderedRows("UNWIND [datetime('2026-01-01T00:00:00Z'), 1, datetime('2027-01-01T00:00:00Z')] AS x "
                      "WITH x WHERE x = datetime('2026-01-01T00:00:00Z') RETURN x",
                      {{"2026-01-01T00:00:00Z"}});
}

TEST_F(DateTimeCellComparisonTest, ordersAnUnwoundCellAgainstAnInstant) {
    expectOrderedRows("UNWIND [datetime('2026-01-01T00:00:00Z'), 'a', datetime('2028-01-01T00:00:00Z')] AS x "
                      "WITH x WHERE x < datetime('2027-01-01T00:00:00Z') RETURN x",
                      {{"2026-01-01T00:00:00Z"}});
}

TEST_F(DateTimeCellComparisonTest, comparesAnInstantWithAnUnwoundCellWrittenOnTheRight) {
    expectOrderedRows("UNWIND [datetime('2026-01-01T00:00:00Z'), 1, datetime('2027-01-01T00:00:00Z')] AS x "
                      "WITH x WHERE datetime('2027-01-01T00:00:00Z') = x RETURN x",
                      {{"2027-01-01T00:00:00Z"}});
}
