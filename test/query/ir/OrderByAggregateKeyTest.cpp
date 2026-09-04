#include <gtest/gtest.h>

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

namespace {

// The eight simpledb people with an out-edge, ordered by how many they have and then by
// name: the order ORDER BY count(*) DESC, a.name asks for over the seventeen out-edge
// rows of MATCH (a:Person)-->(b).
const Rows peopleByOutDegreeDescending = {
    {"Remy"}, {"Adam"}, {"Cyrus"}, {"Luc"}, {"Maxime"}, {"Suhas"}, {"Doruk"}, {"Martina"},
};

const Rows peopleByOutDegreeAscending = {
    {"Doruk"}, {"Martina"}, {"Cyrus"}, {"Luc"}, {"Maxime"}, {"Suhas"}, {"Adam"}, {"Remy"},
};

const Rows peopleAndOutDegreeDescending = {
    {"Remy", "4"},
    {"Adam", "3"},
    {"Cyrus", "2"},
    {"Luc", "2"},
    {"Maxime", "2"},
    {"Suhas", "2"},
    {"Doruk", "1"},
    {"Martina", "1"},
};

}

// An ORDER BY keyed on an aggregate orders the groups by what the aggregate reduced for
// each of them. count(*) reduces a value per group like every other aggregate, so it is a
// key like every other one - the argument it reads no column through says nothing about
// whether the key varies from group to group.
class OrderByAggregateKeyTest : public TuringTest {
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

    // The rows in the order the query emits them, which is what an ORDER BY is
    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(OrderByAggregateKeyTest, ordersGroupsByTheirCountOfRows) {
    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name ORDER BY count(*) DESC, a.name",
                      peopleByOutDegreeDescending);
}

TEST_F(OrderByAggregateKeyTest, ordersGroupsByTheirCountOfRowsAscending) {
    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name ORDER BY count(*), a.name",
                      peopleByOutDegreeAscending);
}

// The top-N shape the key exists for: the three people with the most out-edges, not the
// three that come first alphabetically.
TEST_F(OrderByAggregateKeyTest, takesTheGroupsWithTheMostRows) {
    const Rows topThree {peopleByOutDegreeDescending.begin(), peopleByOutDegreeDescending.begin() + 3};

    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name ORDER BY count(*) DESC, a.name LIMIT 3",
                      topThree);
}

// The key orders the groups whether or not the projection returns the count as well.
TEST_F(OrderByAggregateKeyTest, ordersGroupsByTheProjectedCountOfRows) {
    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name, count(*) ORDER BY count(*) DESC, a.name",
                      peopleAndOutDegreeDescending);
}

// With no second key the surviving group is whichever one holds the fewest rows, and one
// row is the fewest any group of a match holds: only the count it kept is the query's
// answer, since the people holding it are tied.
TEST_F(OrderByAggregateKeyTest, takesTheGroupWithTheFewestRows) {
    expectCounts("MATCH (a:Person)-->(b) RETURN a.name, count(*) ORDER BY count(*) LIMIT 1", {1});
}

// count(1) reduces a value per group as count(*) does, so it is a key on the same terms.
TEST_F(OrderByAggregateKeyTest, ordersGroupsByTheirCountOverAConstant) {
    const Rows topThree {peopleByOutDegreeDescending.begin(), peopleByOutDegreeDescending.begin() + 3};

    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name ORDER BY count(1) DESC, a.name LIMIT 3",
                      topThree);
}

// An item may carry an aggregate without being one, and so may a key: negating the count
// orders the groups by it ascending, which is the descending order of the count itself.
TEST_F(OrderByAggregateKeyTest, ordersGroupsByAnArithmeticOverTheirCountOfRows) {
    const Rows topThree {peopleByOutDegreeDescending.begin(), peopleByOutDegreeDescending.begin() + 3};

    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name ORDER BY count(*) * -1, a.name LIMIT 3",
                      topThree);
}

TEST_F(OrderByAggregateKeyTest, ordersGroupsByTheirCountOfRowsAtABarrier) {
    expectRowsInOrder("MATCH (a:Person)-->(b) WITH a.name AS person, count(*) AS outEdges "
                      "ORDER BY count(*) DESC, person LIMIT 3 RETURN person, outEdges",
                      {{"Remy", "4"}, {"Adam", "3"}, {"Cyrus", "2"}});
}

// The same order asked for through a key that reads a column, and through the alias of
// the count: the two spellings that already order the groups, so the key above has to
// land on the same rows as these.
TEST_F(OrderByAggregateKeyTest, ordersGroupsByACountOverAColumn) {
    const Rows topThree {peopleByOutDegreeDescending.begin(), peopleByOutDegreeDescending.begin() + 3};

    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name ORDER BY count(b) DESC, a.name LIMIT 3",
                      topThree);
}

TEST_F(OrderByAggregateKeyTest, ordersGroupsByTheAliasOfTheirCountOfRows) {
    expectRowsInOrder("MATCH (a:Person)-->(b) RETURN a.name, count(*) AS outEdges "
                      "ORDER BY outEdges DESC, a.name LIMIT 3",
                      {{"Remy", "4"}, {"Adam", "3"}, {"Cyrus", "2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
