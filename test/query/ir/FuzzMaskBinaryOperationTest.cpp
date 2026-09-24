#include <gtest/gtest.h>

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

// AFL inputs that threw 'Unsupported binary operation of columns of kinds ColumnMask and
// ColumnMask' or 'ColumnMask and ColumnVector<ListElementView>': two predicates ordered
// against each other, and a predicate compared to the element of a mixed list.
class FuzzMaskBinaryOperationTest : public TuringTest {
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

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectNoRows(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_TRUE(sink.rows().empty()) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

// A node compared to an integer compares its ID, so n = 0 holds on Remy alone
TEST_F(FuzzMaskBinaryOperationTest, Where000001) {
    expectRows("MATCH (n:Person) WHERE (n = 0)  > (n = 1) RETURN n.name", {{"Remy"}});
}

TEST_F(FuzzMaskBinaryOperationTest, Where000002) {
    expectRows("MATCH (n:Person) WHERE (n = 0) <= (n = 1) RETURN n.name",
               {{"Adam"},
                {"Cyrus"},
                {"Doruk"},
                {"Luc"},
                {"Martina"},
                {"Maxime"},
                {"Suhas"}});
}

TEST_F(FuzzMaskBinaryOperationTest, OrderBy000021) {
    expectNoRows("MATCH (a)-->(x), (b)-->(b)-->(b), (c)-->(z), (), (d)-->(x)\n"
                 "RETURN a.me+ c.name, d.name, x.name\n"
                 "ORDER BY  (b)-->(x)< (c)-->(x), (d)-->(),a.name, b.name, c.name, d.name, x.name");
}

TEST_F(FuzzMaskBinaryOperationTest, ReturnPatternPredicatesOrdered) {
    expectRows("MATCH (a:Person) RETURN a.name, (a)-[:KNOWS_WELL]->() < (a)-[:INTERESTED_IN]->()",
               {{"Adam", "false"},
                {"Cyrus", "true"},
                {"Doruk", "true"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Maxime", "true"},
                {"Remy", "false"},
                {"Suhas", "true"}});
}

TEST_F(FuzzMaskBinaryOperationTest, Where000006) {
    expectNoRows("UNWIND ['Remy', 1] AS v MATCH (n) WHERE n.name = v AND v STARTS WITH 'Rem'  = v AND v STARTS WITH 'Rem'RETURN n.name");
}

TEST_F(FuzzMaskBinaryOperationTest, ReturnPredicateEqualsMixedListElement) {
    expectRows("UNWIND ['Remy', 1] AS v RETURN v, (v = 'Remy') = v",
               {{"1", "false"},
                {"Remy", "false"}});
}

TEST_F(FuzzMaskBinaryOperationTest, ReturnPredicateEqualsBooleanListElement) {
    expectRows("UNWIND [true, 'Remy'] AS v MATCH (n:Person {name: 'Remy'}) RETURN v, (n = 0) = v",
               {{"Remy", "false"},
                {"true", "true"}});
}
