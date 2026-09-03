#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class RowCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// The tail of every two-hop path. Only Remy, Adam and Ghosts have both an edge in and an
// edge out: one path lands on Adam, two on Remy, one on Ghosts.
const std::vector<StringRowSink::Row> twoHopTailNames {
    {"Remy"}, {"Bio"}, {"Cooking"},
    {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"},
    {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"},
    {"Remy"}};

// Each Person beside how many edges leave it. Ghosts has edges out too, but is an Interest.
const std::vector<StringRowSink::Row> personOutDegrees {
    {"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
    {"Martina", "1"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Suhas", "2"}};

}

// The hop and filter shapes whose carry sets the trim pass cuts down, run on simpledb: the
// rows have to come out as they did with every column carried.
class TrimUnreadColumnsCypherTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void run(std::string_view query, NLOutputSink& sink) {
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

    void expectRows(std::string_view query, std::vector<StringRowSink::Row> expected) {
        StringRowSink sink;
        ASSERT_NO_FATAL_FAILURE(run(query, sink));

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowCountSink sink;
        ASSERT_NO_FATAL_FAILURE(run(query, sink));

        EXPECT_EQ(sink.getRowCount(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(TrimUnreadColumnsCypherTest, readsTheTailOfATwoHopChain) {
    expectRows("MATCH (a)-->(b)-->(c) RETURN c.name", twoHopTailNames);
}

TEST_F(TrimUnreadColumnsCypherTest, readsTheTailOfAThreeHopChain) {
    expectRows("MATCH (a)-->(b)-->(c)-->(d) RETURN d.name",
               {{"Remy"}, {"Bio"}, {"Cooking"}, {"Remy"}, {"Bio"}, {"Cooking"},
                {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"},
                {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"},
                {"Remy"}, {"Remy"}});
}

// Remy and Adam are the only nodes aged 32; Remy has two edges in, Adam one.
TEST_F(TrimUnreadColumnsCypherTest, readsTheTargetAFilterKept) {
    expectRows("MATCH (a)-->(b) WHERE b.age = 32 RETURN b.name", {{"Remy"}, {"Remy"}, {"Adam"}});
}

TEST_F(TrimUnreadColumnsCypherTest, projectsAConstantOverTheRowsAFilterKept) {
    expectRowCount("MATCH (a)-->(b) WHERE b.age = 32 RETURN 1", 3);
}

TEST_F(TrimUnreadColumnsCypherTest, countsTheRowsAFilterKept) {
    expectRows("MATCH (a)-->(b) WHERE b.age = 32 RETURN count(*)", {{"3"}});
}

// The cut keeps the target column, not the loop's own first chunk, and must still size the
// constant.
TEST_F(TrimUnreadColumnsCypherTest, projectsAConstantOverTheRowsALimitKept) {
    expectRowCount("MATCH (a)-->(b) WITH b, a LIMIT 3 RETURN 1", 3);
}

// KNOWS_WELL runs Remy -> Adam, Adam -> Remy and Ghosts -> Remy; the second hop then walks
// Adam's three edges once and Remy's four edges twice.
TEST_F(TrimUnreadColumnsCypherTest, hopsFromATypedEdgeIntoAnUntypedOne) {
    expectRows("MATCH (a)-[:KNOWS_WELL]->(b)-->(c) RETURN c.name",
               {{"Remy"}, {"Bio"}, {"Cooking"},
                {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"},
                {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}});
}

// A cut or a sort in a WITH that publishes the root column too, which the second pattern
// never reads.
TEST_F(TrimUnreadColumnsCypherTest, hopsFromALimitedWith) {
    expectRows("MATCH (a)-->(b) WITH a, b LIMIT 100 MATCH (b)-->(c) RETURN c.name", twoHopTailNames);
}

TEST_F(TrimUnreadColumnsCypherTest, hopsFromASkippedWith) {
    expectRows("MATCH (a)-->(b) WITH a, b SKIP 0 MATCH (b)-->(c) RETURN c.name", twoHopTailNames);
}

TEST_F(TrimUnreadColumnsCypherTest, hopsFromASortedWith) {
    expectRows("MATCH (a)-->(b) WITH a, b ORDER BY b.name MATCH (b)-->(c) RETURN c.name", twoHopTailNames);
}

// Sorted by target name, the first two rows land on Adam and Animals; only Adam has edges out.
TEST_F(TrimUnreadColumnsCypherTest, hopsFromASortedAndLimitedWith) {
    expectRows("MATCH (a)-->(b) WITH a, b ORDER BY b.name LIMIT 2 MATCH (b)-->(c) RETURN c.name",
               {{"Remy"}, {"Bio"}, {"Cooking"}});
}

// Sorted by target name, the last two of the 18 rows land on Remy and Travel; only Remy has
// edges out.
TEST_F(TrimUnreadColumnsCypherTest, hopsFromASortedAndSkippedWith) {
    expectRows("MATCH (a)-->(b) WITH a, b ORDER BY b.name SKIP 16 MATCH (b)-->(c) RETURN c.name",
               {{"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}});
}

// KNOWS_WELL runs Remy -> Adam, Adam -> Remy and Ghosts -> Remy on each side of the product.
TEST_F(TrimUnreadColumnsCypherTest, crossesTwoTypedHopsReadingTheirTargets) {
    expectRows("MATCH (a)-[:KNOWS_WELL]->(b), (c)-[:KNOWS_WELL]->(d) RETURN b.name, d.name",
               {{"Adam", "Adam"}, {"Adam", "Remy"}, {"Adam", "Remy"},
                {"Remy", "Adam"}, {"Remy", "Adam"},
                {"Remy", "Remy"}, {"Remy", "Remy"}, {"Remy", "Remy"}, {"Remy", "Remy"}});
}

// 18 nodes crossed with 18 edges.
TEST_F(TrimUnreadColumnsCypherTest, countsAProductNothingReads) {
    expectRows("MATCH (a), (b)-->(c) RETURN count(*)", {{"324"}});
}

TEST_F(TrimUnreadColumnsCypherTest, groupsWithAnUnreadAggregate) {
    expectRows("MATCH (p:Person)-->(x) WITH p.name AS name, count(x) AS c, sum(x.age) AS s RETURN name, c",
               personOutDegrees);
}

TEST_F(TrimUnreadColumnsCypherTest, groupsByAnUnreadKey) {
    expectRows("MATCH (p:Person)-->(x) WITH p.name AS name, count(x) AS c RETURN c",
               {{"4"}, {"3"}, {"2"}, {"2"}, {"1"}, {"2"}, {"1"}, {"2"}});
}

TEST_F(TrimUnreadColumnsCypherTest, collectsWithAnUnreadList) {
    expectRows("MATCH (p:Person)-->(x) WITH p.name AS name, collect(x.name) AS xs, count(x) AS c RETURN name, c",
               personOutDegrees);
}

// Remy and Adam are the only Persons with an age; collect skips the nulls of the others.
TEST_F(TrimUnreadColumnsCypherTest, collectsUngroupedWithAnUnreadList) {
    expectRows("MATCH (p:Person) WITH collect(p.name) AS names, collect(p.age) AS ages RETURN ages",
               {{"32, 32"}});
}
