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

// A null is a value of every type rather than a type of its own, so a branch projecting
// the null literal takes the column type the other branches name, and the UNION holds
// the rows of both.
class UnionNullColumnTest : public TuringTest {
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

TEST_F(UnionNullColumnTest, unionsAPropertyAbsentFromOneLabelWithOneItCarries) {
    expectRows("MATCH (n:Interest) RETURN n.age AS x UNION MATCH (n:Person) RETURN n.age AS x",
               {{"null"}, {"32"}});
}

TEST_F(UnionNullColumnTest, appendsANullLiteralToAnIntegerBranch) {
    expectRows("RETURN 1 AS x UNION ALL RETURN null AS x", {{"1"}, {"null"}});
}

TEST_F(UnionNullColumnTest, prependsANullLiteralToAnIntegerBranch) {
    expectRows("RETURN null AS x UNION ALL RETURN 1 AS x", {{"null"}, {"1"}});
}

TEST_F(UnionNullColumnTest, appendsANullLiteralToAMatchedColumn) {
    expectRows("MATCH (n:Person) RETURN n.age AS x UNION RETURN null AS x",
               {{"32"}, {"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
