#include <gtest/gtest.h>

#include <algorithm>
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

// A sum carries the type of what it tallies: integers sum to an integer, and a null among
// them is skipped rather than widening the tally. Only a genuinely mixed numeric list sums
// to a double.
class IntegerSumPastNullTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
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

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(IntegerSumPastNullTest, sumsIntegersPastANull) {
    expectRows("UNWIND [1, null, 3] AS x RETURN sum(x)", {{"4"}});
}

TEST_F(IntegerSumPastNullTest, sumsIntegersHoldingNoNull) {
    expectRows("UNWIND [1, 3] AS x RETURN sum(x)", {{"4"}});
}

TEST_F(IntegerSumPastNullTest, sumsAStoredIntegerProperty) {
    expectRows("MATCH (n:Person) RETURN sum(n.age)", {{"64"}});
}

TEST_F(IntegerSumPastNullTest, sumsAColumnNullOnEveryRow) {
    expectRows("MATCH (n:Person) RETURN sum(n.nope)", {{"0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
