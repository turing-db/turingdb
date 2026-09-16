#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// The receptive field of a 3-layer GraphSAGE around one seed: every node reachable in up
// to three hops, each hop a branch of its own and the union deduplicating what the hops
// share. One branch per hop is what the chained sampling of GnnFrontierUnionTest cannot
// express - each of its layers sees only the newest hop.
constexpr const char* threeHopUnion =
    "MATCH (s {name: 'Remy'})--(a) RETURN id(a) AS id UNION "
    "MATCH (s {name: 'Remy'})--()--(b) RETURN id(b) AS id UNION "
    "MATCH (s {name: 'Remy'})--()--()--(c) RETURN id(c) AS id";

constexpr const char* threeHopUnionAll =
    "MATCH (s {name: 'Remy'})--(a) RETURN id(a) AS id UNION ALL "
    "MATCH (s {name: 'Remy'})--()--(b) RETURN id(b) AS id UNION ALL "
    "MATCH (s {name: 'Remy'})--()--()--(c) RETURN id(c) AS id";

// The same three hops under a per-hop fanout limit: each branch keeps at most two of the
// nodes its hop reaches, so the neighbourhood is gathered from a bounded slice of every
// layer instead of all of it. The limit is the branch's own, charged before the union
// dedups, so it bounds what a hop contributes and not what the union keeps.
constexpr const char* cappedThreeHopUnion =
    "MATCH (s {name: 'Remy'})--(a) RETURN DISTINCT id(a) AS id LIMIT 2 UNION "
    "MATCH (s {name: 'Remy'})--()--(b) RETURN DISTINCT id(b) AS id LIMIT 2 UNION "
    "MATCH (s {name: 'Remy'})--()--()--(c) RETURN DISTINCT id(c) AS id LIMIT 2";

// The out-edge walk of the same three hops, which reaches less of the graph
constexpr const char* directedThreeHopUnion =
    "MATCH (s {name: 'Remy'})-->(a) RETURN id(a) AS id UNION "
    "MATCH (s {name: 'Remy'})-->()-->(b) RETURN id(b) AS id UNION "
    "MATCH (s {name: 'Remy'})-->()-->()-->(c) RETURN id(c) AS id";

}

// A k-hop neighbourhood gathered as one branch per hop. simpledb's edges around Remy(0)
// are: Remy knows Adam(1) and is interested in Ghosts(6), Computers(2) and Eighties(3);
// Adam knows Remy and is interested in Bio(4) and Cooking(5); Ghosts knows Remy; Luc(9) is
// interested in Computers and Animals(10); Maxime(8) in Bio; Martina(11) in Cooking.
class MultiHopUnionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    // The rows in the order the query emits them: the union concatenates its branches, so
    // a node comes out under the first hop that reaches it
    void expectRows(std::string_view query, const Rows& expected, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        RowSink sink;
        _interpreter->setChunkSize(chunkSize);

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query
                                         << "\nchunk size: " << chunkSize
                                         << "\nactual:\n" << actualText;
    }

    // The same, by set: a traversal's row order depends on how its nest is chunked, so
    // which of two nodes a hop reaches first is not the query's to fix
    void expectSortedRows(std::string_view query, const Rows& expected, size_t chunkSize) {
        RowSink sink;
        _interpreter->setChunkSize(chunkSize);

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query
                                          << "\nchunk size: " << chunkSize
                                          << "\nactual:\n" << actualText;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowSink sink;
        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query;
    }

    static Rows idRows(const std::vector<std::string>& ids) {
        Rows rows;
        for (const std::string& id : ids) {
            rows.push_back(Row {id});
        }

        return rows;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};

    // Hop 1 reaches Adam, Ghosts, Computers and Eighties; hop 2 adds Remy itself (every
    // hop 1 node has an edge back), Bio, Cooking and Luc; hop 3 adds Maxime, Animals and
    // Martina, and finds the four hop 1 nodes again. Eleven of simpledb's eighteen nodes.
    const std::vector<std::string> _threeHopNeighbourhood {"1", "6", "2", "3",
                                                           "0", "4", "5", "9",
                                                           "8", "10", "11"};
    const std::vector<size_t> _chunkSizes {1, 2, 3, 7};
};

TEST_F(MultiHopUnionTest, gathersTheThreeHopNeighbourhood) {
    expectRows(threeHopUnion, idRows(_threeHopNeighbourhood));
}

// Each hop on its own, so the union above is read against what its branches contribute:
// the four of hop 1, the four of hop 2, and the seven of hop 3 - of which only Maxime(8),
// Animals(10) and Martina(11) are new.
TEST_F(MultiHopUnionTest, gathersTheSameNodesOneHopAtATime) {
    expectRows("MATCH (s {name: 'Remy'})--(a) RETURN DISTINCT id(a) AS id",
               idRows({"1", "6", "2", "3"}));

    expectRows("MATCH (s {name: 'Remy'})--()--(b) RETURN DISTINCT id(b) AS id",
               idRows({"0", "4", "5", "9"}));

    expectRows("MATCH (s {name: 'Remy'})--()--()--(c) RETURN DISTINCT id(c) AS id",
               idRows({"1", "6", "2", "3", "8", "10", "11"}));
}

// The hops are walks and not simple paths, so they revisit heavily: 6 walks at one hop, 15
// at two and 70 at three. UNION ALL reports every one of those 91 endpoints, and the dedup
// is what turns them into the 11 nodes the neighbourhood holds.
TEST_F(MultiHopUnionTest, dedupsTheWalksThatShareAnEndpoint) {
    expectRowCount(threeHopUnionAll, 91);
    expectRowCount(threeHopUnion, 11);
}

// Following out-edges only, the third hop walks back through Remy and reaches nothing the
// first did not: its four nodes are all deduped away, so the neighbourhood is the seven of
// the first two hops.
TEST_F(MultiHopUnionTest, dropsAHopThatReachesNothingNew) {
    expectRows(directedThreeHopUnion, idRows({"1", "6", "2", "3", "0", "4", "5"}));

    expectRows("MATCH (s {name: 'Remy'})-->(a) RETURN DISTINCT id(a) AS id",
               idRows({"1", "6", "2", "3"}));
    expectRows("MATCH (s {name: 'Remy'})-->()-->()-->(c) RETURN DISTINCT id(c) AS id",
               idRows({"1", "6", "2", "3"}));
}

// A fanout of two per hop keeps Adam(1) and Ghosts(6) of the first, Remy itself(0) and
// Bio(4) of the second, and of the third only nodes the first already had - four nodes
// where the uncapped walk of the same three hops gathers eleven.
TEST_F(MultiHopUnionTest, capsEachHopAtItsOwnFanoutLimit) {
    expectRows(cappedThreeHopUnion, idRows({"1", "6", "0", "4"}));

    expectRows("MATCH (s {name: 'Remy'})--(a) RETURN DISTINCT id(a) AS id LIMIT 2",
               idRows({"1", "6"}));
    expectRows("MATCH (s {name: 'Remy'})--()--(b) RETURN DISTINCT id(b) AS id LIMIT 2",
               idRows({"0", "4"}));
    expectRows("MATCH (s {name: 'Remy'})--()--()--(c) RETURN DISTINCT id(c) AS id LIMIT 2",
               idRows({"1", "6"}));
}

// The limit is charged per branch, so three hops capped at two each report six rows under
// UNION ALL - one budget per hop, not one shared by the union
TEST_F(MultiHopUnionTest, chargesTheFanoutLimitPerHop) {
    expectRowCount("MATCH (s {name: 'Remy'})--(a) RETURN DISTINCT id(a) AS id LIMIT 2 UNION ALL "
                   "MATCH (s {name: 'Remy'})--()--(b) RETURN DISTINCT id(b) AS id LIMIT 2 UNION ALL "
                   "MATCH (s {name: 'Remy'})--()--()--(c) RETURN DISTINCT id(c) AS id LIMIT 2",
                   6);
}

// The seen-set spans every branch and every chunk of each, so a node first reached in one
// hop's last chunk is still recognised in the next hop's first: the neighbourhood is the
// same eleven nodes however the hops are chunked. Which of them comes out first is not -
// a hop's walks are emitted in the order its nest reaches them, and the chunking decides
// that, so Animals(10) and Martina(11) trade places at the smaller sizes.
TEST_F(MultiHopUnionTest, gathersTheNeighbourhoodAcrossChunkBoundaries) {
    for (const size_t chunkSize : _chunkSizes) {
        expectSortedRows(threeHopUnion, idRows(_threeHopNeighbourhood), chunkSize);
    }
}
