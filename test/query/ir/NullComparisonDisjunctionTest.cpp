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

// A comparison against null is null on every row, and three-valued logic makes null OR
// TRUE true - so a disjunction of one with a constant keeps every row the match found.
// simpledb has 8 Persons, of whom only Remy and Adam carry an age.
class NullComparisonDisjunctionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
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

TEST_F(NullComparisonDisjunctionTest, keepsEveryRowWhenAnEqualityAgainstNullIsOredWithTrue) {
    expectRows("MATCH (n:Person) WHERE n.age = null OR true RETURN count(n)", {{"8"}});
}

TEST_F(NullComparisonDisjunctionTest, keepsEveryRowWhenAnOrderingAgainstNullIsOredWithTrue) {
    expectRows("MATCH (n:Person) WHERE n.age > null OR true RETURN count(n)", {{"8"}});
}

TEST_F(NullComparisonDisjunctionTest, keepsEveryRowWhenAnInequalityAgainstNullIsOredWithTrue) {
    expectRows("MATCH (n:Person) WHERE n.age <> null OR true RETURN count(n)", {{"8"}});
}

TEST_F(NullComparisonDisjunctionTest, keepsEveryRowWhenANullTestIsOredWithTrue) {
    expectRows("MATCH (n:Person) WHERE n.age IS NULL OR true RETURN count(n)", {{"8"}});
}

TEST_F(NullComparisonDisjunctionTest, keepsTheRowsTheOtherOperandHoldsWhenItIsAColumn) {
    expectRows("MATCH (n:Person) WHERE n.age > null OR n.age > 30 RETURN count(n)", {{"2"}});
}

TEST_F(NullComparisonDisjunctionTest, dropsEveryRowWhenTheComparisonStandsAlone) {
    expectRows("MATCH (n:Person) WHERE n.age = null RETURN count(n)", {{"0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
