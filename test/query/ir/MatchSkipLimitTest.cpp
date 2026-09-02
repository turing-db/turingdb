#include <gtest/gtest.h>

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

// The SKIP and LIMIT siblings of the ORDER BY a MATCH may carry: they cut the rows the match
// produced, so the rest of the query reads the window rather than everything matched.
class MatchSkipLimitTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRowsInOrder(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The three nodes the match produced first, which it answers in the order it scanned them
TEST_F(MatchSkipLimitTest, keepsTheFirstMatchedRows) {
    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}, {"2"}};

    expectRowsInOrder("MATCH (n) LIMIT 3 RETURN n", expected);
}

// Adam, Animals and Bio, the first three of simpledb by name
TEST_F(MatchSkipLimitTest, keepsTheFirstOfTheOrderedRows) {
    const std::vector<StringRowSink::Row> expected {{"1"}, {"10"}, {"4"}};

    expectRowsInOrder("MATCH (n) ORDER BY n.name LIMIT 3 RETURN n", expected);
}

// Remy, Suhas and Travel, the three the order leaves once fifteen are passed
TEST_F(MatchSkipLimitTest, dropsTheOrderedRowsItSkipsPast) {
    const std::vector<StringRowSink::Row> expected {{"0"}, {"12"}, {"14"}};

    expectRowsInOrder("MATCH (n) ORDER BY n.name SKIP 15 RETURN n", expected);
}

// The SKIP cuts before the LIMIT, so the window is Bio, Computers and Cooking
TEST_F(MatchSkipLimitTest, keepsTheWindowBothCutsLeave) {
    const std::vector<StringRowSink::Row> expected {{"4"}, {"2"}, {"5"}};

    expectRowsInOrder("MATCH (n) ORDER BY n.name SKIP 2 LIMIT 3 RETURN n", expected);
}

TEST_F(MatchSkipLimitTest, keepsNothingWhenTheSkipPassesEveryMatchedRow) {
    expectRowsInOrder("MATCH (n) SKIP 20 RETURN n", {});
}

// The projection cuts what the match handed it, so the RETURN skips into the five rows the
// match kept and answers the two that are left, not into the eighteen it matched
TEST_F(MatchSkipLimitTest, cutsAgainWhenTheProjectionCarriesItsOwnWindow) {
    const std::vector<StringRowSink::Row> expected {{"2"}, {"5"}};

    expectRowsInOrder("MATCH (n) ORDER BY n.name LIMIT 5 RETURN n SKIP 3", expected);
}

// An aggregate folds over the rows the cut left, not over everything the match produced
TEST_F(MatchSkipLimitTest, countsOnlyTheRowsTheCutLeft) {
    const std::vector<StringRowSink::Row> expected {{"3"}};

    expectRowsInOrder("MATCH (n) LIMIT 3 RETURN count(n)", expected);
}

// A literal UNWIND after the cut spreads the window the cut left, so the cut applies to the
// rows the match produced and not to the product the UNWIND made out of them
TEST_F(MatchSkipLimitTest, spreadsTheLimitedRowsOverALiteralUnwind) {
    const std::vector<StringRowSink::Row> expected {
        {"Adam", "1"}, {"Adam", "2"}, {"Animals", "1"}, {"Animals", "2"},
    };

    expectRowsInOrder("MATCH (n) ORDER BY n.name LIMIT 2 UNWIND [1, 2] AS x "
                      "RETURN n.name, x ORDER BY n.name, x",
                      expected);
}

// Remy, Suhas and Travel are the three the order leaves once fifteen are passed, each
// paired with both elements - the skipped names may not come back through the product
TEST_F(MatchSkipLimitTest, spreadsTheRowsLeftBySkipOverALiteralUnwind) {
    const std::vector<StringRowSink::Row> expected {
        {"Remy", "1"}, {"Remy", "2"}, {"Suhas", "1"},
        {"Suhas", "2"}, {"Travel", "1"}, {"Travel", "2"},
    };

    expectRowsInOrder("MATCH (n) ORDER BY n.name SKIP 15 UNWIND [1, 2] AS x "
                      "RETURN n.name, x ORDER BY n.name, x",
                      expected);
}

TEST_F(MatchSkipLimitTest, countsTheRowsALiteralUnwindSpreadsTheCutOver) {
    const std::vector<StringRowSink::Row> expected {{"6"}};

    expectRowsInOrder("MATCH (n) LIMIT 3 UNWIND [1, 2] AS x RETURN count(*)", expected);
}

// A WITH after the UNWIND does not stand in for the cut's own part: the product is already
// made by the time it publishes
TEST_F(MatchSkipLimitTest, spreadsTheLimitedRowsOverALiteralUnwindPublishedByAWith) {
    const std::vector<StringRowSink::Row> expected {
        {"Adam", "1"}, {"Adam", "2"}, {"Animals", "1"}, {"Animals", "2"},
    };

    expectRowsInOrder("MATCH (n) ORDER BY n.name LIMIT 2 UNWIND [1, 2] AS x "
                      "WITH n.name AS name, x RETURN name, x ORDER BY name, x",
                      expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
