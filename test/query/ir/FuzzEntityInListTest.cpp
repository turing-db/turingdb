#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
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

// AFL inputs that threw 'Unsupported binary operation of columns of kinds
// ColumnVector<ID<unsigned long, 1>> and ColumnVector<ListView>': a node tested for
// membership in a list.
class FuzzEntityInListTest : public TuringTest {
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

    void expectOneRowPerNode(std::string_view query, size_t columnCount) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), _nodeCount) << "query: " << query;

        const bool everyRowHasEveryColumn = std::ranges::all_of(rows, [columnCount](const Row& row) {
            return row.size() == columnCount;
        });
        EXPECT_TRUE(everyRowHasEveryColumn) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
    size_t _nodeCount {18};
};

TEST_F(FuzzEntityInListTest, Return000067) {
    expectOneRowPerNode("MATCH (n), (m) WHERE m.age = 32 REtURN COUNT(n)+   range(3, 5),     +  n.age > null OR n IN  + COUNT(n) ^0 +                     [] +  +    +  n.age > null OR n.age >  20110 + 0, n        , n.a2000", 4);
}

TEST_F(FuzzEntityInListTest, ReturnNodeInEmptyList) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n IN []", {{"false"}});
}

TEST_F(FuzzEntityInListTest, ReturnNodeInListOfItself) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n IN [n]", {{"true"}});
}

TEST_F(FuzzEntityInListTest, ReturnNodeInListOfAnotherNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}), (m:Person {name: 'Adam'}) RETURN n IN [m]", {{"false"}});
}

TEST_F(FuzzEntityInListTest, ReturnNodeInCollectedNodes) {
    expectRows("MATCH (n:Person {name: 'Remy'}), (m:Person) WITH n, collect(m) AS people RETURN n IN people", {{"true"}});
}

TEST_F(FuzzEntityInListTest, ReturnEdgeInEmptyList) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e IN []", {{"false"}});
}

TEST_F(FuzzEntityInListTest, ReturnEdgeInListOfItself) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e IN [e]", {{"true"}});
}
