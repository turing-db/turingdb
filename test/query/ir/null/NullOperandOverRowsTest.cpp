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

// A null a WITH binds over rows is unknown in the logical operators as a literal null is:
// null OR true is true, null AND false is false, and every other combination is null. Bio
// has no isReal and Gym's is true.
class NullOperandOverRowsTest : public TuringTest {
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

TEST_F(NullOperandOverRowsTest, takesTheOtherOperandWhenItDecides) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH null AS x RETURN x OR true, x AND false, true OR x, false AND x",
               {{"true", "false", "true", "false"}, {"true", "false", "true", "false"}});
}

TEST_F(NullOperandOverRowsTest, answersNullWhenTheOtherOperandDoesNotDecide) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH null AS x RETURN x OR false, x AND true, x XOR true, x XOR false",
               {{"null", "null", "null", "null"}, {"null", "null", "null", "null"}});
}

TEST_F(NullOperandOverRowsTest, combinesWithABooleanReadPerRow) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH n, null AS x RETURN n.name, x OR n.isReal, x AND n.isReal",
               {{"Bio", "null", "null"}, {"Gym", "true", "null"}});
}

TEST_F(NullOperandOverRowsTest, filtersOnALogicalOperatorOverTheNull) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH n, null AS x WHERE x OR n.isReal RETURN n.name",
               {{"Gym"}});
}

TEST_F(NullOperandOverRowsTest, combinesWithABooleanReadPerRowPastAnotherMatch) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH null AS x MATCH (m:Interest) WHERE m.name IN ['Bio', 'Gym'] RETURN m.name, x OR m.isReal, NOT x",
               {{"Bio", "null", "null"}, {"Bio", "null", "null"}, {"Gym", "true", "null"}, {"Gym", "true", "null"}});
}

TEST_F(NullOperandOverRowsTest, combinesInASubqueryRunOncePerRow) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH null AS x CALL { WITH x RETURN x OR true AS y UNION RETURN false AS y } RETURN y",
               {{"true"}, {"false"}, {"true"}, {"false"}});
}

TEST_F(NullOperandOverRowsTest, combinesInAListComprehension) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] WITH null AS x RETURN [v IN [true, false] | v OR x]",
               {{"true, null"}, {"true, null"}});
}

TEST_F(NullOperandOverRowsTest, combinesACaseOfNullBranchesAsTheNull) {
    expectRows("MATCH (n:Interest) WHERE n.name IN ['Bio', 'Gym'] RETURN n.name, (CASE WHEN n.isReal THEN null END) OR true, NOT (CASE WHEN n.isReal THEN null END)",
               {{"Bio", "true", "null"}, {"Gym", "true", "null"}});
}
