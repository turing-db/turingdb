#include <gtest/gtest.h>

#include <stddef.h>

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

using Rows = std::vector<StringRowSink::Row>;

}

// A join whose key property the query also returns. The projection and the key read one
// property of one variable, so the reuse pass hands both the same column - and the cut
// still has to fuse, since the join yields that column among its results.
class HashJoinProjectedKeyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
        _interpreter->setForceValueHashJoin(true);
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void explainStage(std::string_view query, std::string_view stage, std::string& program) {
        StringRowSink sink;
        runQuery(query, sink);

        program.clear();
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                program = row.back();
            }
        }

        EXPECT_FALSE(program.empty()) << "query: " << query << "\nno " << stage << " stage reported";
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    // The rows the join produces against the rows the cross product and its filter
    // produce, which is what the cut stands as when the join is not forced. Neither form
    // is ordered, so the two are held against each other sorted.
    void expectJoinMatchesTheProduct(std::string_view query) {
        StringRowSink joined;
        _interpreter->setForceValueHashJoin(true);
        runQuery(query, joined);

        StringRowSink product;
        _interpreter->setForceValueHashJoin(false);
        runQuery(query, product);
        _interpreter->setForceValueHashJoin(true);

        Rows joinedRows = joined.getRows();
        Rows productRows = product.getRows();
        std::sort(joinedRows.begin(), joinedRows.end());
        std::sort(productRows.begin(), productRows.end());

        EXPECT_FALSE(joinedRows.empty()) << "query: " << query;
        EXPECT_EQ(joinedRows, productRows) << "query: " << query;
    }

    static bool contains(std::string_view text, std::string_view part) {
        return text.find(part) != std::string_view::npos;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(HashJoinProjectedKeyTest, fusesWhenTheReturnProjectsTheKeyProperty) {
    std::string program;
    explainStage("EXPLAIN (db) MATCH (n), (m) WHERE n.name = m.name RETURN n.name", "db", program);

    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
}

TEST_F(HashJoinProjectedKeyTest, fusesWhenTheReturnProjectsTheKeyOfBothSides) {
    std::string program;
    explainStage("EXPLAIN (db) MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name", "db", program);

    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
}

// Every node carries its own name, so the join is the diagonal: one row per node, whose
// projected key is that node's name.
TEST_F(HashJoinProjectedKeyTest, projectsTheKeyOfEveryJoinedRow) {
    StringRowSink sink;
    runQuery("MATCH (n), (m) WHERE n.name = m.name RETURN n.name", sink);

    const Rows& rows = sink.getRows();
    EXPECT_EQ(rows.size(), 18u);

    for (const StringRowSink::Row& row : rows) {
        ASSERT_EQ(row.size(), 1u);
        EXPECT_FALSE(row.front().empty());
    }
}

TEST_F(HashJoinProjectedKeyTest, projectsTheKeyOfBothSidesOfEveryJoinedRow) {
    StringRowSink sink;
    runQuery("MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name", sink);

    const Rows& rows = sink.getRows();
    EXPECT_EQ(rows.size(), 18u);

    for (const StringRowSink::Row& row : rows) {
        ASSERT_EQ(row.size(), 2u);
        EXPECT_EQ(row.front(), row.back());
    }
}

// Only Remy and Adam carry an age, and both carry 32, so the join on the age is the four
// pairs of those two - and a null key matches nothing, so no other node reaches a row.
TEST_F(HashJoinProjectedKeyTest, projectsANullableKeyOffTheJoinedRows) {
    std::string program;
    explainStage("EXPLAIN (db) MATCH (n), (m) WHERE n.age = m.age RETURN n.name, n.age", "db", program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;

    expectRows("MATCH (n), (m) WHERE n.age = m.age RETURN n.name, n.age",
               {{"Remy", "32"}, {"Remy", "32"}, {"Adam", "32"}, {"Adam", "32"}});
}

TEST_F(HashJoinProjectedKeyTest, joinsTheRowsTheProductAndItsFilterProduce) {
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.name = m.name RETURN n.name");
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name");
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.age = m.age RETURN n.name, n.age");
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.name = m.name RETURN n, n.name, m");
}
