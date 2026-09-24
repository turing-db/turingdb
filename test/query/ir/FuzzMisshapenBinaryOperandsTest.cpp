#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
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

// AFL inputs that tripped the 'lhs->size() == rhs->size()' assertion of BinaryOperators.h:
// a list comprehension added to an aggregate.
class FuzzMisshapenBinaryOperandsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectRowCount(std::string_view query, size_t rowCount) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows().size(), rowCount) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FuzzMisshapenBinaryOperandsTest, Return000149) {
    expectRowCount("MATCH (n), (m)  REtURN [x IN [1, 2, 3] WHERE x > 1]+ 10 + 10 + COUNT(n) * 10 + 10 + 20010 + 0, n.age / 3* n.ERRORa2000", 1);
}

TEST_F(FuzzMisshapenBinaryOperandsTest, Return000150) {
    expectRowCount("MATCH (n), (m)  REtURN [x IN [1, 2, 3] WHERE x > 1]+ COUNT(n) * 10 + 10 + 20010 + 0, n.age / 3* n.a2000", 1);
}

TEST_F(FuzzMisshapenBinaryOperandsTest, Return000153) {
    expectRowCount("MATCH (n), (m)  REtURN [x IN [1, 2, 3] WHERE x > 1]+ 10f+ 10 + COUNT(n) * 10 + 10 + 20010 + 0, n.age / 3* n.a2000", 1);
}

TEST_F(FuzzMisshapenBinaryOperandsTest, Return000158) {
    expectRows("MATCH (n) REtURN [x IN [1, 2,                         1]+1]+ 10 + 10 + COUNT(n) * 10 + 10 + 20010              + 0",
               {{"[1, 2, 1, 1, 10, 10, 180, 10, 20010, 0]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnComprehensionPlusCount) {
    expectRows("MATCH (n) RETURN [x IN [1] | x] + count(n)", {{"[1, 18]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnComprehensionPlusScalarPlusCount) {
    expectRows("MATCH (n) RETURN [x IN [1] | x] + 1 + count(n)", {{"[1, 1, 18]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnSizeOfComprehensionPlusCount) {
    expectRows("MATCH (n) RETURN size([x IN [1] | x]) + count(n)", {{"19"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnFilteredComprehensionPlusCount) {
    expectRows("MATCH (n) RETURN [x IN range(1, 3) WHERE x > 1] + count(n)", {{"[2, 3, 18]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnComprehensionBesideCount) {
    expectRows("MATCH (n) RETURN [x IN [1] | x], count(n)", {{"[1]", "18"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnCountBesideComprehension) {
    expectRows("MATCH (n) RETURN count(n), [x IN [1] | x]", {{"18", "[1]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnCountPlusComprehension) {
    expectRows("MATCH (n) RETURN count(n) + [x IN [1] | x]", {{"[18, 1]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnComprehensionPlusSumPlusScalar) {
    expectRows("MATCH (n) RETURN [x IN [1, 2, 3] WHERE x > 1] + sum(n.age) * 10 + 10", {{"[2, 3, 640, 10]"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnListPlusCountBesideConstantKey) {
    expectRows("MATCH (n) RETURN [1] + count(n) + 10, n.nope", {{"[1, 18, 10]", "null"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnCountBesideCountOfConstant) {
    expectRows("MATCH (n) RETURN count(n), count(42)", {{"18", "18"}});
}

TEST_F(FuzzMisshapenBinaryOperandsTest, ReturnSumBesideSumOfConstant) {
    expectRows("MATCH (n) RETURN sum(n.age), sum(1)", {{"64", "18"}});
}
