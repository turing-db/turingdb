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

// An item may be aliased to a name its own expression reads: WITH sum(fans) AS fans folds
// the fans column the barrier before it published and publishes the total under that name
// again. The alias names a column of its own, so the aggregate reads the one it was given
// and reduces nothing that is already reduced.
class ShadowedAliasTest : public TuringTest {
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

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_NE(error.find(reason), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Fifteen INTERESTED_IN edges reach ten interests. count(p) behind the (i, p) key is 1 on
// every row, so the sum behind i alone is how many people reach that interest, and Gym is
// reached by three, more than any other
TEST_F(ShadowedAliasTest, sumsAGroupedCountUnderTheNameItReads) {
    expectRowsInOrder("MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person) "
                      "WITH i, p, count(p) AS fans "
                      "WITH i, sum(fans) AS fans "
                      "RETURN i.name, fans ORDER BY fans DESC LIMIT 1",
                      {{"Gym", "3"}});
}

// The WHERE reads the name the aggregate took over, so it reads the sum: Bio, Computers
// and Cooking are reached by two people each, Gym by three
TEST_F(ShadowedAliasTest, filtersOnTheNameTheAggregateTookOver) {
    expectRows("MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person) "
               "WITH i, p, count(p) AS fans "
               "WITH i, sum(fans) AS fans WHERE fans > 1 "
               "RETURN i.name, fans",
               {{"Bio", "2"}, {"Computers", "2"}, {"Cooking", "2"}, {"Gym", "3"}});
}

// The same shadowing on a RETURN: the ten published counts sum back to the fifteen edges
// they were counted from
TEST_F(ShadowedAliasTest, sumsAPublishedCountUnderItsOwnNameOnReturn) {
    expectRows("MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person) "
               "WITH i.name AS interest, count(p) AS fans "
               "RETURN sum(fans) AS fans",
               {{"15"}});
}

// The shadowing alias behind a grouping key, which is the shape the two barriers above
// reduce to: each person's reach is summed back under the name it was published with
TEST_F(ShadowedAliasTest, groupsASumUnderTheNameItReads) {
    expectRows("MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person) "
               "WITH p.name AS person, i.name AS interest, count(i) AS reach "
               "RETURN person, sum(reach) AS reach",
               {{"Adam", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Maxime", "2"},
                {"Remy", "3"},
                {"Suhas", "2"}});
}

// A second item reading the alias of an aggregate is still an aggregate over an aggregate,
// whether or not that alias shadows a name the barrier published
TEST_F(ShadowedAliasTest, rejectsAnAggregateOverAnAliasOfTheSameProjection) {
    expectRejected("MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person) "
                   "WITH i.name AS interest, count(p) AS fans "
                   "RETURN sum(fans) AS fans, count(fans)",
                   "Aggregate functions may not be nested");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
