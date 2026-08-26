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

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// The query test suite's join-bad-keys-error case on the v3 engine:
// MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN a
//
// Its four patterns share three variables - a between the first and the third, g between
// the third and the fourth, c between the second and the fourth - and each of the three is
// a join key. Dropping one of them cross-products the patterns it constrained instead,
// which widens the match rather than failing: the legacy plan joins on a and on g only,
// and answers 624 rows where the shared c admits 160.
class CommaPatternJoinKeysTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
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

    void expectCount(std::string_view query, size_t expected) {
        StringRowSink sink;
        runQuery(query, sink);

        const Rows expectedRows {{std::to_string(expected)}};
        EXPECT_EQ(sink.getRows(), expectedRows) << "query: " << query;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows().size(), expected) << "query: " << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<db::QueryInterpreterV3> _interpreter;
};

// Each pair of the reported query's patterns on its own, so the whole match's count is
// read against the joins it is made of rather than against one hand-derived number.
TEST_F(CommaPatternJoinKeysTest, joinsTwoPatternsOnTheirSharedStart) {
    expectCount("MATCH (a)-->(b),(a)-->(f)-->(g) RETURN count(*)", 32);
}

TEST_F(CommaPatternJoinKeysTest, joinsTwoPatternsOnTheirSharedEnd) {
    expectCount("MATCH (a)-->(f)-->(g),(c)-->(g) RETURN count(*)", 18);
}

// The pair the legacy plan cross-produces: c starts the two-hop chain and the one-hop
// pattern alike, so the chain's start is the one-hop pattern's start and not any node.
TEST_F(CommaPatternJoinKeysTest, joinsAChainAndAHopOnTheirSharedStart) {
    expectCount("MATCH (c)-->(d)-->(e),(c)-->(g) RETURN count(*)", 32);
}

TEST_F(CommaPatternJoinKeysTest, countsTheFourPatternsOfTheReportedQuery) {
    expectCount("MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN count(*)", 160);
}

TEST_F(CommaPatternJoinKeysTest, returnsOneRowPerMatchOfTheReportedQuery) {
    expectRowCount("MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN a", 160);
}

// Remy (0), Adam (1) and Ghosts (6) are the only nodes with both an out-edge to bind b and
// a two-hop chain to bind f and g.
TEST_F(CommaPatternJoinKeysTest, talliesTheStartNodeOfTheReportedQuery) {
    const Rows expected {{"0", "96"}, {"1", "48"}, {"6", "16"}};
    expectRows("MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN a, count(*)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
