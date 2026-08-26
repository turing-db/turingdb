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

// The window a WITH publishes: what its ORDER BY, SKIP and LIMIT leave for the part after
// it to read, and the order they leave it in.
class WithCutTest : public TuringTest {
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

    // The rows in the order the query emits them, which is what a barrier's ORDER BY is
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

    void expectRejected(std::string_view query, QueryStatus::Status stage) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), stage)
            << "query: " << query
            << "\nstage: " << QueryStatusDescription::value(status.getStatus())
            << "\nerror: " << error;

        EXPECT_EQ(error.find("Unexpected exception"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// An ORDER BY with nothing to cut still orders: the rows reach the part after the barrier
// sorted, not in the order the match produced them
TEST_F(WithCutTest, ordersThePublishedRowsWithoutCuttingThem) {
    expectRowsInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name DESC RETURN name",
                      {{"Suhas"}, {"Remy"}, {"Maxime"}, {"Martina"},
                       {"Luc"}, {"Doruk"}, {"Cyrus"}, {"Adam"}});
}

TEST_F(WithCutTest, keepsTheOrderOfTheRowsItsCutLeft) {
    expectRowsInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name DESC LIMIT 3 "
                      "RETURN name",
                      {{"Suhas"}, {"Remy"}, {"Maxime"}});
}

TEST_F(WithCutTest, publishesNothingUnderAZeroLimit) {
    expectRows("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 0 RETURN name", {});
}

TEST_F(WithCutTest, publishesNothingPastTheLastRow) {
    expectRows("MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 100 RETURN name", {});
}

// A cut with no order to cut on takes as many rows as it is asked for, whichever they are
TEST_F(WithCutTest, limitsAnUnorderedProjection) {
    expectCounts("MATCH (p:Person) WITH p.name AS name LIMIT 3 RETURN count(*)", {3});
}

TEST_F(WithCutTest, skipsAnUnorderedProjection) {
    expectCounts("MATCH (p:Person) WITH p.name AS name SKIP 6 RETURN count(*)", {2});
}

// The bound is an expression, not only a literal
TEST_F(WithCutTest, cutsOnAComputedBound) {
    expectRowsInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 1 + 1 "
                      "RETURN name",
                      {{"Adam"}, {"Cyrus"}});
}

TEST_F(WithCutTest, rejectsANegativeLimit) {
    expectRejected("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT -1 RETURN name",
                   QueryStatus::Status::PLAN_ERROR);
}

TEST_F(WithCutTest, rejectsANegativeSkip) {
    expectRejected("MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP -1 RETURN name",
                   QueryStatus::Status::PLAN_ERROR);
}

// Suhas is the last Person in name order, and the hop runs over the one row the skip left
TEST_F(WithCutTest, hopsFromTheSuffixACutLeft) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name SKIP 7 "
               "MATCH (p)-->(x) "
               "RETURN p.name, x.name",
               {{"Suhas", "Gym"}, {"Suhas", "JiuJitsu"}});
}

// count(*) past a cut counts the rows the cut left, not the ones the match produced
TEST_F(WithCutTest, countsTheRowsACutLeft) {
    expectCounts("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 "
                 "WITH count(*) AS c "
                 "RETURN c",
                 {3});
}

// The cut reads the deduplicated rows: the last two of the ten interests in name order
TEST_F(WithCutTest, cutsTheRowsADistinctLeft) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "WITH DISTINCT i.name AS interest ORDER BY interest DESC LIMIT 2 "
                      "RETURN interest",
                      {{"Travel"}, {"Padel"}});
}

// Eight Persons crossed with ten Interests, cut down to three rows
TEST_F(WithCutTest, cutsACrossProduct) {
    expectCounts("MATCH (a:Person), (b:Interest) "
                 "WITH a.name AS person, b.name AS interest LIMIT 3 "
                 "RETURN count(*)",
                 {3});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
