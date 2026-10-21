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

class NumberEqualityAcrossTypesTest : public TuringTest {
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

TEST_F(NumberEqualityAcrossTypesTest, anIntegerEqualsTheFloatOfItsValue) {
    expectRows("RETURN 1 = 1.0", {{"true"}});
    expectRows("RETURN 1.0 = 1", {{"true"}});
}

TEST_F(NumberEqualityAcrossTypesTest, anIntegerDiffersFromAFloatOfAnotherValue) {
    expectRows("RETURN 1 <> 1.5", {{"true"}});
    expectRows("RETURN 1 = 1.5", {{"false"}});
}

TEST_F(NumberEqualityAcrossTypesTest, equalityIsExactPastTheDoublePrecision) {
    expectRows("RETURN 9007199254740993 = 9007199254740992.0", {{"false"}});
    expectRows("RETURN 9007199254740993 <> 9007199254740992.0", {{"true"}});
}

TEST_F(NumberEqualityAcrossTypesTest, filtersAnIntegerPropertyOnAFloatLiteral) {
    expectRows("MATCH (n) WHERE n.age = 32.0 RETURN n.name", {{"Remy"}, {"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
