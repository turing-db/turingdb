#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "ID.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "BioAssert.h"

using namespace db;
using namespace turing::test;

namespace {

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

class NullSink : public NLOutputSink {
public:
    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override {}
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

// Remy is a Person, Ghosts an Interest, and the third ID names no node of simpledb. They
// score 0, 9 and 36 against (1, 0, 0, 0).
constexpr std::string_view searchThree = "VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) ";

constexpr uint64_t danglingNodeID = 1000;

// What binds the seeded node, and the equality spelled two ways: the one re-rooting reads
// and an ID comparison it leaves to the cross product, which gives the expected rows
struct Seed {
    std::string_view _prefix;
    std::string_view _seeded;
    std::string_view _oracle;
    size_t _scans {0};
    bool _fetched {true};
};

// Remy (0), Adam (1), Ghosts (6) and an ID naming no node
const Seed vectorSeed {"VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids ", "n = ids", "id(n) = id(ids)"};
const Seed listSeed {"WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i ", "n = ss[i]", "id(n) = ss[i]"};
const Seed matchedSeed {"MATCH (s:Founder) WITH s ", "n = s", "id(n) = id(s)", 1, false};
const Seed literalSeed {"UNWIND [0, 6, 99] AS x ", "n = x", "id(n) = x"};

// Node and edge constraints on the seeded node n, and on what the pattern reaches from it
const std::vector<std::string_view> patterns {
    "MATCH (n:Person) WHERE {} RETURN n.name",
    "MATCH (n:Person {age: 32}) WHERE {} RETURN n.name",
    "MATCH (n) WHERE {} AND n.name <> 'Adam' RETURN n.name",
    "MATCH (n)-[e:INTERESTED_IN]->(m) WHERE {} RETURN n.name, m.name",
    "MATCH (n)-[e:KNOWS_WELL {duration: 20}]->(m:Person) WHERE {} RETURN n.name, m.name, e.name",
    "MATCH (m)-[e]->(n:Person) WHERE {} RETURN m.name, n.name",
    "MATCH (m:Person)-[e:KNOWS_WELL]->(n) WHERE {} RETURN m.name, e.name",
    "MATCH (n)-[e]-(m) WHERE {} RETURN n.name, m.name",
    "MATCH (n)-[e:INTERESTED_IN]->(m:Interest {name: 'Ghosts'}) WHERE {} RETURN n.name",
    "MATCH (n)-[e]->(m) WHERE {} AND m.name <> 'Adam' RETURN n.name, m.name",
    "MATCH (a)-->(n)-->(m) WHERE {} RETURN a.name, n.name, m.name",
    "MATCH (a:Person)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(n) WHERE {} RETURN a.name, b.name",
    "MATCH (n)-[*1..2]->(m) WHERE {} RETURN n.name, m.name",
    "MATCH (a)-->(n) WHERE {} AND a.age > 20 RETURN a.name",
};

size_t countOccurrences(std::string_view text, std::string_view needle) {
    size_t count = 0;
    for (size_t position = text.find(needle); position != std::string_view::npos; position = text.find(needle, position + 1)) {
        count++;
    }

    return count;
}

std::string substitute(std::string_view pattern, std::string_view predicate) {
    std::string query(pattern);
    query.replace(query.find("{}"), 2, predicate);
    return query;
}

}

// A MATCH whose node is equated to the nodes a VECTOR SEARCH yielded fetches those nodes
// rather than scanning, and keeps the rows a scan would have kept.
class RerootPatternAtSeedTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            Graph* graph = system.createGraph(_graphName);
            SimpleGraph::createSimpleGraph(graph);

            _remy = SimpleGraph::findNodeID(graph, "Remy");
            _ghosts = SimpleGraph::findNodeID(graph, "Ghosts");
        }

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        loadPeopleVectors();
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink, ChangeID change = ChangeID::head()) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              change,
                              &sink);
    }

    ChangeID openChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        bioassert(opened, "Failed to open a change");

        return opened.value()->id();
    }

    void runWrite(std::string_view query) {
        const ChangeID change = openChange();

        QueryStatus status;
        NullSink sink;
        runQuery(query, status, sink, change);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        const QueryState submitState(_graphName, &_env->getMem(), &_env->getCompilerContext(), &_queryConfig, nullptr, CommitHash::head(), change);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    void loadPeopleVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "people.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n"
             << _ghosts.getValue() << ",4,0,0,0\n"
             << danglingNodeID << ",7,0,0,0\n";
        file.close();

        QueryStatus createStatus;
        NullSink createSink;
        runQuery("CREATE VECTOR INDEX people WITH DIMENSION 4 METRIC EUCLID", createStatus, createSink);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        NullSink loadSink;
        runQuery("LOAD VECTOR FROM \"people.csv\" IN people", loadStatus, loadSink);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected);
    }

    void rowsOf(std::string_view query, std::vector<StringRowSink::Row>& rows) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        sink.sortedRows(rows);
    }

    void expectRerootedAtTheSeed(std::string_view query, const Seed& seed) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();
        ASSERT_EQ(sink.getRows().size(), 1u);
        const std::string& program = sink.getRows().front().back();

        const bool fetchesTheSeed = contains(program, "db.fetch_nodes") || contains(program, "db.const_scan_nodes");
        EXPECT_EQ(fetchesTheSeed, seed._fetched) << program;
        EXPECT_EQ(countOccurrences(program, "db.scan_"), seed._scans) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
        EXPECT_FALSE(contains(program, "db.hash_join")) << program;
    }

    // Every pattern, seeded through the equality, starts at the seed and returns the rows
    // the ID comparison does
    void expectEveryPatternRerooted(const Seed& seed) {
        for (const std::string_view pattern : patterns) {
            const std::string query = std::string(seed._prefix) + substitute(pattern, seed._seeded);
            const std::string oracle = std::string(seed._prefix) + substitute(pattern, seed._oracle);
            SCOPED_TRACE(query);

            expectRerootedAtTheSeed(query, seed);

            std::vector<StringRowSink::Row> rows;
            std::vector<StringRowSink::Row> expected;
            rowsOf(query, rows);
            rowsOf(oracle, expected);

            EXPECT_EQ(rows, expected);
        }
    }

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _ghosts;
    QueryConfig _queryConfig;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(RerootPatternAtSeedTest, vectorSearchSeedsEveryPattern) {
    expectEveryPatternRerooted(vectorSeed);
}

TEST_F(RerootPatternAtSeedTest, listIndexSeedsEveryPattern) {
    expectEveryPatternRerooted(listSeed);
}

TEST_F(RerootPatternAtSeedTest, matchedNodeSeedsEveryPattern) {
    expectEveryPatternRerooted(matchedSeed);
}

TEST_F(RerootPatternAtSeedTest, literalListSeedsEveryPattern) {
    expectEveryPatternRerooted(literalSeed);
}

TEST_F(RerootPatternAtSeedTest, unprovenEdgeCheckSeedsEveryPattern) {
    runWrite("MATCH (r {name: 'Remy'}) CREATE (r)-[:KNOWS_WELL]->(r)");

    QueryStatus status;
    StringRowSink sink;
    runQuery("EXPLAIN (db) UNWIND [0, 6, 99] AS x MATCH (a)-->(n)-->(m) WHERE n = x RETURN a.name", status, sink);
    ASSERT_TRUE(status.isOk()) << status.getError();
    ASSERT_EQ(sink.getRows().size(), 1u);
    const std::string& program = sink.getRows().front().back();
    EXPECT_TRUE(contains(program, "db.check_edge_distinct") || contains(program, "distinct_from")) << program;

    for (const Seed* seed : {&vectorSeed, &listSeed, &matchedSeed, &literalSeed}) {
        expectEveryPatternRerooted(*seed);
    }
}

TEST_F(RerootPatternAtSeedTest, seedDropsTheIDNamingNoNode) {
    expectRows(std::string(searchThree) + "YIELD ids MATCH (n) WHERE n = ids RETURN n.name",
               {{"Remy"}, {"Ghosts"}});
}

TEST_F(RerootPatternAtSeedTest, patternCarriesTheSeedColumns) {
    expectRows(std::string(searchThree) + "YIELD ids, score MATCH (n)-[:INTERESTED_IN]->(m) WHERE n = ids "
               "RETURN m.name, score",
               {{"Computers", "0"}, {"Eighties", "0"}, {"Ghosts", "0"}});
}

TEST_F(RerootPatternAtSeedTest, aPredicateOnTheSeedColumnsStillCuts) {
    expectRows(std::string(searchThree) + "YIELD ids, score MATCH (m)-[e]->(n) WHERE n = ids AND score > 1 "
               "RETURN m.name",
               {{"Remy"}});
}

TEST_F(RerootPatternAtSeedTest, patternDropsADeletedSeed) {
    runWrite("MATCH (n {name: 'Ghosts'}) DETACH DELETE n");

    expectRows(std::string(searchThree) + "YIELD ids MATCH (n) WHERE n = ids RETURN n.name",
               {{"Remy"}});
}

TEST_F(RerootPatternAtSeedTest, patternStartsFromANodeTheQueryCreated) {
    const ChangeID change = openChange();

    QueryStatus status;
    StringRowSink sink;
    runQuery("MATCH (r {name: 'Remy'}) CREATE (z:Person {name: 'Zed'})-[:KNOWS_WELL]->(r) "
             "WITH z MATCH (n)-[:KNOWS_WELL]->(m) WHERE n = z RETURN m.name",
             status,
             sink,
             change);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}
