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

namespace {

constexpr size_t simpleGraphNodeCount = 18;

}

class NullInListElementTest : public TuringTest {
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

TEST_F(NullInListElementTest, answersNullForNullInAConstantNullElement) {
    expectRows("RETURN null IN [null][0]", {{"null"}});
}

TEST_F(NullInListElementTest, answersNullForNullInAMissingPropertyElement) {
    const std::vector<StringRowSink::Row> expected(simpleGraphNodeCount, {"null"});

    expectRows("MATCH (n) RETURN null IN [n.y][0]", expected);
}

TEST_F(NullInListElementTest, answersNullForNullInAMissingPropertyElementPastAWith) {
    const std::vector<StringRowSink::Row> expected(simpleGraphNodeCount, {"null"});

    expectRows("MATCH (n) WITH [n.y] AS l RETURN null IN l[0]", expected);
}

TEST_F(NullInListElementTest, answersNullForAnIntegerInAMissingPropertyElement) {
    const std::vector<StringRowSink::Row> expected(simpleGraphNodeCount, {"null"});

    expectRows("MATCH (n) RETURN 1 IN [n.y][0]", expected);
}

TEST_F(NullInListElementTest, answersNullForNullInANestedListElement) {
    expectRows("RETURN null IN [[1]][0]", {{"null"}});
}

TEST_F(NullInListElementTest, answersFalseForNullInAnEmptyNestedListElement) {
    expectRows("RETURN null IN [[]][0]", {{"false"}});
}

TEST_F(NullInListElementTest, answersNullForNullInANestedMissingPropertyElement) {
    const std::vector<StringRowSink::Row> expected(simpleGraphNodeCount, {"null"});

    expectRows("MATCH (n) RETURN null IN [[n.y]][0]", expected);
}

TEST_F(NullInListElementTest, answersNullForNullInNull) {
    expectRows("RETURN null IN null", {{"null"}});
}

TEST_F(NullInListElementTest, answersNullForAPropertyInNull) {
    const std::vector<StringRowSink::Row> expected(simpleGraphNodeCount, {"null"});

    expectRows("MATCH (n) RETURN n.name IN null", expected);
}

TEST_F(NullInListElementTest, filtersEveryRowOnAPropertyInNull) {
    expectRows("MATCH (n) WHERE n.name IN null RETURN n.name", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
