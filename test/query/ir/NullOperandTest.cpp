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

// An arithmetic operator over a null answers null, and a boolean connective answers by
// the three-valued truth tables, whether the null reaches it as a literal or as a row of
// a column. A null operand is a value to compute with, not a query to turn away.
class NullOperandTest : public TuringTest {
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

TEST_F(NullOperandTest, addsANullColumnToAnInteger) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.age + 1", {{"33"}});
}

TEST_F(NullOperandTest, addsANullLiteralToAnInteger) {
    expectRows("RETURN 1 + null", {{"null"}});
}

TEST_F(NullOperandTest, subtractsANullLiteralFromAnInteger) {
    expectRows("RETURN 1 - null", {{"null"}});
}

TEST_F(NullOperandTest, negatesANullLiteral) {
    expectRows("RETURN -null", {{"null"}});
}

TEST_F(NullOperandTest, conjoinsABooleanWithANullLiteral) {
    expectRows("RETURN false AND null", {{"false"}});
}

TEST_F(NullOperandTest, conjoinsTwoNullLiterals) {
    expectRows("RETURN null AND null", {{"null"}});
}

TEST_F(NullOperandTest, disjoinsTwoNullLiterals) {
    expectRows("RETURN null OR null", {{"null"}});
}

TEST_F(NullOperandTest, negatesANullLiteralBoolean) {
    expectRows("RETURN NOT null", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
