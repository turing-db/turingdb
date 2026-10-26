#include <gtest/gtest.h>

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
#include "iterators/ChunkConfig.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "CallV3Test.h"
#include "StringRowSink.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

// Each KNOWS_WELL walk of one or two hops from a Person, once per interest of the node it
// ends on: Adam has 2 interests, Remy 3
const Rows walkSizesPerInterest {
    {"Remy", "1"}, {"Remy", "1"},
    {"Remy", "2"}, {"Remy", "2"}, {"Remy", "2"},
    {"Adam", "1"}, {"Adam", "1"}, {"Adam", "1"},
    {"Adam", "2"}, {"Adam", "2"}};

}

// A walk variable read in a scope other than the one its MATCH bound it in, past an
// OPTIONAL MATCH, a CALL, a cut, or inside a subquery or a pattern comprehension, reads
// as the list or the path it stands for
class WalkReadPastAScopeTest : public CallV3Test {
protected:
    void stageDump(std::string_view query, std::string_view stage, std::string& dump) {
        StringRowSink sink;
        runQuery(query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                dump = row.back();
            }
        }

        ASSERT_FALSE(dump.empty()) << "no " << stage << " stage for: " << query;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sorted(expected)) << "query: " << query;
    }
};

TEST_F(WalkReadPastAScopeTest, readsTheSizeOfAWalkAnOptionalMatchJoinsOntoOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) OPTIONAL MATCH (m)-[:INTERESTED_IN]->(x) RETURN n.name, size(e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(WalkReadPastAScopeTest, sizesAWalkAnOptionalMatchJoinsOnto) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) OPTIONAL MATCH (m)-[:INTERESTED_IN]->(x) RETURN n.name, size(e)",
                     walkSizesPerInterest);
}

TEST_F(WalkReadPastAScopeTest, sizesAQuantifiedRelationshipAnOptionalMatchJoinsOnto) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL]->{1,2}(m) OPTIONAL MATCH (m)-[:INTERESTED_IN]->(x) RETURN n.name, size(e)",
                     walkSizesPerInterest);
}

TEST_F(WalkReadPastAScopeTest, sizesTheGroupsOfAWalkAnOptionalMatchJoinsOnto) {
    expectSortedRows("MATCH (n:Person) ((a)-[r:KNOWS_WELL]->(b)){1,2} (m) OPTIONAL MATCH (m)-[:INTERESTED_IN]->(x) "
                     "RETURN n.name, size(r), size(a) = size(r) AND size(b) = size(r)",
                     {{"Remy", "1", "true"}, {"Remy", "1", "true"},
                      {"Remy", "2", "true"}, {"Remy", "2", "true"}, {"Remy", "2", "true"},
                      {"Adam", "1", "true"}, {"Adam", "1", "true"}, {"Adam", "1", "true"},
                      {"Adam", "2", "true"}, {"Adam", "2", "true"}});
}

TEST_F(WalkReadPastAScopeTest, sizesAWalkPastACallSubquery) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) CALL { WITH m MATCH (m)-[:INTERESTED_IN]->(x) RETURN x } RETURN n.name, size(e)",
                     walkSizesPerInterest);
}

TEST_F(WalkReadPastAScopeTest, sizesAWalkPastAnOptionalCallSubquery) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) OPTIONAL CALL { WITH m MATCH (m)-[:INTERESTED_IN]->(x) RETURN x } "
                     "RETURN n.name, size(e)",
                     walkSizesPerInterest);
}

TEST_F(WalkReadPastAScopeTest, sizesAWalkPastAMatchCut) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) ORDER BY n.name MATCH (m)-[:INTERESTED_IN]->(x) RETURN n.name, size(e)",
                     walkSizesPerInterest);
}

// Adam's walks end on Remy (3 interests) then on Adam (2), Remy's the other way round
TEST_F(WalkReadPastAScopeTest, sizesAWalkInsideACountSubquery) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) "
                     "RETURN n.name, size(e), COUNT { MATCH (m)-[:INTERESTED_IN]->(x) WHERE size(e) > 1 }",
                     {{"Remy", "1", "0"}, {"Remy", "2", "3"}, {"Adam", "1", "0"}, {"Adam", "2", "2"}});
}

TEST_F(WalkReadPastAScopeTest, sizesAWalkInsideAnExistsSubquery) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) WHERE EXISTS { MATCH (m)-[:INTERESTED_IN]->(x) WHERE size(e) > 1 } "
                     "RETURN n.name, size(e)",
                     {{"Remy", "2"}, {"Adam", "2"}});
}

TEST_F(WalkReadPastAScopeTest, sizesAWalkInsideAPatternComprehension) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) RETURN n.name, size(e), [(m)-[:INTERESTED_IN]->(x) | size(e)]",
                     {{"Remy", "1", "1, 1"},
                      {"Remy", "2", "2, 2, 2"},
                      {"Adam", "1", "1, 1, 1"},
                      {"Adam", "2", "2, 2"}});
}

// No KNOWS_WELL edge reaches an Interest, so the OPTIONAL MATCH keeps each walk once
TEST_F(WalkReadPastAScopeTest, readsANamedWalkAnOptionalMatchJoinsOnto) {
    expectSortedRows("MATCH p = (n:Person)-[:KNOWS_WELL*1..2]->(m) OPTIONAL MATCH (m)-[:KNOWS_WELL]->(x:Interest) "
                     "RETURN p, nodes(p), length(p), size(nodes(p)), size(relationships(p))",
                     {{"(0), [0], (1)", "0, 1", "1", "2", "1"},
                      {"(0), [0], (1), [4], (0)", "0, 1, 0", "2", "3", "2"},
                      {"(1), [4], (0)", "1, 0", "1", "2", "1"},
                      {"(1), [4], (0), [0], (1)", "1, 0, 1", "2", "3", "2"}});
}

TEST_F(WalkReadPastAScopeTest, readsANamedWalkPastACallSubquery) {
    expectSortedRows("MATCH p = (n:Person)-[:KNOWS_WELL*1..2]->(m) CALL { WITH m MATCH (m)-[:INTERESTED_IN]->(x) RETURN count(x) AS c } "
                     "RETURN p, length(p), c",
                     {{"(0), [0], (1)", "1", "2"},
                      {"(0), [0], (1), [4], (0)", "2", "3"},
                      {"(1), [4], (0)", "1", "3"},
                      {"(1), [4], (0), [0], (1)", "2", "2"}});
}

TEST_F(WalkReadPastAScopeTest, readsANamedWalkPastAMatchCut) {
    expectSortedRows("MATCH p = (n:Person)-[:KNOWS_WELL*1..2]->(m) ORDER BY n.name MATCH (m)-[:KNOWS_WELL]->(x) RETURN p, length(p)",
                     {{"(0), [0], (1)", "1"},
                      {"(0), [0], (1), [4], (0)", "2"},
                      {"(1), [4], (0)", "1"},
                      {"(1), [4], (0), [0], (1)", "2"}});
}

TEST_F(WalkReadPastAScopeTest, readsANamedWalkInsideAPatternComprehension) {
    expectSortedRows("MATCH p = (n:Person)-[:KNOWS_WELL*1..2]->(m) "
                     "RETURN length(p), [(m)-[:KNOWS_WELL]->(x) | length(p)], [(m)-[:KNOWS_WELL]->(x) | size(nodes(p))]",
                     {{"1", "1", "2"}, {"1", "1", "2"}, {"2", "2", "3"}, {"2", "2", "3"}});
}

// The sort of an ORDER BY on a MATCH keeps rows across chunks, and a walk's handles hold for
// the chunk that emitted them: the walk's variables read the same at any chunk size
class WalkThroughAMatchSortTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectSortedRowsAtEveryChunkSize(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);

            StringRowSink sink;
            QueryStatus status;
            _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

            Rows rows;
            sink.sortedRows(rows);

            EXPECT_EQ(rows, sorted(expected)) << "chunk size " << chunkSize << ", query: " << query;
        }
    }

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(WalkThroughAMatchSortTest, readsAWalkPastTheSort) {
    expectSortedRowsAtEveryChunkSize("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) ORDER BY n.name RETURN n.name, e, size(e)",
                                     {{"Remy", "0", "1"}, {"Remy", "0, 4", "2"}, {"Adam", "4", "1"}, {"Adam", "4, 0", "2"}});
}

TEST_F(WalkThroughAMatchSortTest, readsTheGroupsOfAWalkPastTheSort) {
    expectSortedRowsAtEveryChunkSize("MATCH (n:Person) ((a)-[r:KNOWS_WELL]->(b)){1,2} (m) ORDER BY n.name RETURN n.name, a, r, b",
                                     {{"Remy", "0", "0", "1"},
                                      {"Remy", "0, 1", "0, 4", "1, 0"},
                                      {"Adam", "1", "4", "0"},
                                      {"Adam", "1, 0", "4, 0", "0, 1"}});
}

TEST_F(WalkThroughAMatchSortTest, readsANamedWalkPastTheSort) {
    expectSortedRowsAtEveryChunkSize("MATCH p = (n:Person)-[:KNOWS_WELL*1..2]->(m) ORDER BY n.name RETURN p, nodes(p), length(p)",
                                     {{"(0), [0], (1)", "0, 1", "1"},
                                      {"(0), [0], (1), [4], (0)", "0, 1, 0", "2"},
                                      {"(1), [4], (0)", "1, 0", "1"},
                                      {"(1), [4], (0), [0], (1)", "1, 0, 1", "2"}});
}

TEST_F(WalkThroughAMatchSortTest, readsAWalkPastTheSortAndTheMatchAfterIt) {
    expectSortedRowsAtEveryChunkSize("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) ORDER BY n.name MATCH (m)-[:INTERESTED_IN]->(x) "
                                     "RETURN n.name, e, x.name",
                                     {{"Remy", "0", "Bio"}, {"Remy", "0", "Cooking"},
                                      {"Remy", "0, 4", "Computers"}, {"Remy", "0, 4", "Eighties"}, {"Remy", "0, 4", "Ghosts"},
                                      {"Adam", "4", "Computers"}, {"Adam", "4", "Eighties"}, {"Adam", "4", "Ghosts"},
                                      {"Adam", "4, 0", "Bio"}, {"Adam", "4, 0", "Cooking"}});
}

TEST_F(WalkThroughAMatchSortTest, readsAWalkPastTheSortOfAnOptionalMatch) {
    expectSortedRowsAtEveryChunkSize("MATCH (n:Person)-[e:KNOWS_WELL*1..2]->(m) OPTIONAL MATCH (m)-[:INTERESTED_IN]->(x) ORDER BY x.name "
                                     "RETURN n.name, e, x.name",
                                     {{"Remy", "0", "Bio"}, {"Remy", "0", "Cooking"},
                                      {"Remy", "0, 4", "Computers"}, {"Remy", "0, 4", "Eighties"}, {"Remy", "0, 4", "Ghosts"},
                                      {"Adam", "4", "Computers"}, {"Adam", "4", "Eighties"}, {"Adam", "4", "Ghosts"},
                                      {"Adam", "4, 0", "Bio"}, {"Adam", "4, 0", "Cooking"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
