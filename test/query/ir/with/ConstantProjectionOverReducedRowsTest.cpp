#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

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

// A projection that reads no variable still yields one row per incoming row, so after a
// clause that dedups, filters, cuts or aggregates the rows it yields one row per row kept.
class ConstantProjectionOverReducedRowsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ConstantProjectionOverReducedRowsTest, returnsOneConstantRowPerDistinctValue) {
    expectRows("UNWIND [1, 1, 2] AS x WITH DISTINCT x RETURN 1", {{"1"}, {"1"}});
}

TEST_F(ConstantProjectionOverReducedRowsTest, returnsOneConstantRowPerDistinctEndNode) {
    expectRows("MATCH (a)-->(b) WITH DISTINCT b RETURN 'x'",
               std::vector<StringRowSink::Row>(12, {"x"}));
}

TEST_F(ConstantProjectionOverReducedRowsTest, returnsOneConstantRowPerDistinctPropertyValue) {
    expectRows("MATCH (n:Person) WITH DISTINCT n.isFrench AS f RETURN 1", {{"1"}, {"1"}});
}

TEST_F(ConstantProjectionOverReducedRowsTest, filtersTheDistinctRowsBeforeTheConstant) {
    expectRows("UNWIND [1, 1, 2] AS x WITH DISTINCT x WHERE x > 1 RETURN 1", {{"1"}});
}

TEST_F(ConstantProjectionOverReducedRowsTest, returnsNoConstantRowWhenTheOnlyRowIsFiltered) {
    expectRows("WITH 1 AS a WHERE a > 5 RETURN 1", {});
}

TEST_F(ConstantProjectionOverReducedRowsTest, returnsNoConstantRowPastALimitOfZero) {
    expectRows("WITH 1 AS a LIMIT 0 RETURN 1", {});
}

TEST_F(ConstantProjectionOverReducedRowsTest, returnsOneConstantRowForAKeylessAggregate) {
    expectRows("UNWIND [1, 2] AS x WITH count(*) AS c RETURN 1", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
