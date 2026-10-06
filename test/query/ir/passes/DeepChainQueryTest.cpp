#include <gtest/gtest.h>

#include <pthread.h>
#include <stddef.h>
#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Each query builds a use-def chain of CHAIN_LENGTH ops, one per list element or clause. A walk
// recursing once per op needs megabytes of stack for it, and the query runs on a thread given
// STACK_BYTES, so such a walk fails here whatever the compiler's frame sizes.
class DeepChainQueryTest : public CallV3Test {
protected:
    static constexpr size_t CHAIN_LENGTH = 10000;
    static constexpr size_t STACK_BYTES = 512 * 1024;

    static std::string repeat(const std::string& clause) {
        std::string text;
        for (size_t index = 0; index < CHAIN_LENGTH; index++) {
            text += clause;
        }

        return text;
    }

    static std::string namesListedAmongAbsentOnes() {
        std::string list = "['Remy', 'Adam'";
        for (size_t index = 0; index < CHAIN_LENGTH; index++) {
            list += ", 'absent" + std::to_string(index) + "'";
        }

        return list + "]";
    }

    static std::string nodeIDsListedAmongAbsentOnes() {
        std::string list = "[0";
        for (size_t nodeID = 1; nodeID < CHAIN_LENGTH; nodeID++) {
            list += ", " + std::to_string(nodeID);
        }

        return list + "]";
    }

    static void sortRows(const StringRowSink& sink, std::vector<StringRowSink::Row>& rows) {
        rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
    }

    void runOnSmallStack(const std::string& query, StringRowSink& sink) {
        struct QueryRun {
            DeepChainQueryTest* _test {nullptr};
            const std::string* _query {nullptr};
            StringRowSink* _sink {nullptr};
        };

        QueryRun run {this, &query, &sink};

        const auto runQueryOnThread = [](void* argument) -> void* {
            QueryRun* const run = static_cast<QueryRun*>(argument);
            run->_test->runQuery(*run->_query, *run->_sink);

            return nullptr;
        };

        pthread_attr_t attributes;
        ASSERT_EQ(pthread_attr_init(&attributes), 0);
        ASSERT_EQ(pthread_attr_setstacksize(&attributes, STACK_BYTES), 0);

        pthread_t thread;
        ASSERT_EQ(pthread_create(&thread, &attributes, runQueryOnThread, &run), 0);
        ASSERT_EQ(pthread_join(thread, nullptr), 0);
        pthread_attr_destroy(&attributes);
    }
};

TEST_F(DeepChainQueryTest, hopsFromNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n)-->(m) WHERE n.name = x RETURN count(m)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, limitsNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x WITH n SKIP 1 LIMIT 1 RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, limitsNodesBelowALongChainOfFilters) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE n.name <> 'x'") + " WITH n LIMIT 3 RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, returnsALongChainComputedOverACount) {
    const std::string query = "MATCH (n) WITH count(n) AS c" + repeat(" WITH c + 1 AS c") + " RETURN c";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{std::to_string(18 + CHAIN_LENGTH)}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, filtersOnALongChainComputedOverAProperty) {
    const std::string query = "MATCH (n) WITH n, n.age AS v" + repeat(" WITH n, v + 1 AS v") + " WITH n, v WHERE v > 0 RETURN v";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::string age = std::to_string(32 + CHAIN_LENGTH);
    const std::vector<StringRowSink::Row> expected {{age}, {age}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAtASeedCarriedThroughALongChainOfFilters) {
    const std::string query = "MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y" + repeat(" WITH a, b, y WHERE y >= 0") + " WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsBelowAFilterOnALongChainOverTheSeed) {
    const std::string query = "MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y, y AS z" + repeat(" WITH a, b, y, z + 0 AS z") + " WITH a, b, y WHERE z >= 0 WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAtIDsComputedThroughALongChain) {
    const std::string query = "MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y" + repeat(" WITH a, b, y + 0 AS y") + " WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAPatternFilteredOnALongChain) {
    const std::string query = "MATCH (a)-[e]->(b) WITH a, b, a.age AS v" + repeat(" WITH a, b, v + 0 AS v") + " WITH a, b WHERE v > 0 UNWIND [0, 2, 7] AS y WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, pushesALongChainOfFiltersOnTheHopStart) {
    const std::string query = "MATCH (a)-->(b) WITH a, b" + repeat(" WITH a, b WHERE a.name <> 'Remy'") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"14"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, pushesALongChainOfFiltersOnTheHopEnd) {
    const std::string query = "MATCH (a)-->(b) WITH a, b" + repeat(" WITH a, b WHERE b.name <> 'Gym'") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"15"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAtASeedEquatedAlongALongChain) {
    const std::string query = "MATCH (s:Founder) WITH s MATCH (n)-->(m) WITH s, n, m" + repeat(" WITH s, n, m WHERE n = s") + " RETURN count(m)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, equatesTwoMatchesAlongALongChain) {
    const std::string query = "MATCH (s {name: 'Adam'}) MATCH (n) WITH s, n" + repeat(" WITH s, n WHERE n = s") + " RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, equatesTheHopEndsAlongALongChain) {
    const std::string query = "MATCH (a)-->(b) WITH a, b" + repeat(" WITH a, b WHERE b = a") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, boundsAnExplorationEndAlongALongChain) {
    const std::string query = "MATCH (s {name: 'Adam'}) MATCH (a)-[*1..2]->(b) WITH s, a, b" + repeat(" WITH s, a, b WHERE b = s") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, fusesAScanUnderALongChainOfNodeIDs) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE n = 1") + " RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, fusesAPatternScanUnderALongChainOfNodeIDs) {
    const std::string query = "MATCH (a)-->(b) WITH a, b" + repeat(" WITH a, b WHERE a = 1") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, countsNodesMatchedOptionallyByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x OPTIONAL MATCH (n) WHERE n.name = x RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, hopsFromNodesCarriedPastALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x WITH n MATCH (n)-->(m) RETURN count(m)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, matchesEdgePropertiesAgainstALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH ()-[e]->() WHERE e.name = x RETURN count(e)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, conjoinsALongDisjunctionWithAComparison) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x AND n.age > 0 RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, conjoinsALongDisjunctionWithAnExists) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x AND EXISTS { (n)-->() } RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, matchesALongDisjunctionWrittenTheOtherWayRound) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE x = n.name RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, hopsTwiceThroughNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n)-->(m)-->(o) WHERE m.name = x RETURN count(o)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"11"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, exploresFromNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n)-[*1..2]->(m) WHERE n.name = x RETURN count(m)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"15"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, hopsFromNodesMatchedByALongNodeIDDisjunction) {
    const std::string query = "UNWIND " + nodeIDsListedAmongAbsentOnes() + " AS x MATCH (n)-->(m) WHERE id(n) = x RETURN count(m)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"18"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, hopsToNodesMatchedByALongNodeIDDisjunction) {
    const std::string query = "UNWIND " + nodeIDsListedAmongAbsentOnes() + " AS x MATCH (n)-->(m) WHERE m = x RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"18"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, hopsTwiceThroughNodesMatchedByALongNodeIDDisjunction) {
    const std::string query = "UNWIND " + nodeIDsListedAmongAbsentOnes() + " AS x MATCH (n)-->(m)-->(o) WHERE m = x RETURN count(o)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"12"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, matchesAPropertyAgainstALongDisjunctionOfIntegers) {
    const std::string query = "UNWIND " + nodeIDsListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.age = x RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, limitsOptionalRowsBelowALongChainOfFilters) {
    const std::string query = "MATCH (n) OPTIONAL MATCH (n)-->(m) WITH n, m" + repeat(" WITH n, m WHERE n.name <> 'x'") + " WITH n, m LIMIT 3 RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, filtersAHopEndAlongALongChain) {
    const std::string query = "MATCH (a)-->(b) WITH b" + repeat(" WITH b WHERE b.name <> 'x'") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"18"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, filtersAScanAlongALongChain) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE n.name <> 'x'") + " RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"18"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, checksLabelsAlongALongChain) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE n:Person") + " RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, comparesNodeIDsAlongALongChain) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE id(n) = 0") + " RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, comparesAPropertyValueAlongALongChain) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE n.name = 'Remy'") + " RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, checksTheHopEndLabelsAlongALongChain) {
    const std::string query = "MATCH (a)-->(b) WITH a, b" + repeat(" WITH a, b WHERE b:Interest") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"15"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, checksTheHopStartLabelsAlongALongChain) {
    const std::string query = "MATCH (a)-->(b) WITH a, b" + repeat(" WITH a, b WHERE a:Person") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"17"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, checksTheEdgeScanEndLabelsAlongALongChain) {
    const std::string query = "MATCH ()-[e]->(b) WITH e, b" + repeat(" WITH e, b WHERE b:Interest") + " RETURN count(e)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"15"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, checksTheExplorationEndLabelsAlongALongChain) {
    const std::string query = "MATCH (a)-[*1..2]->(b) WITH a, b" + repeat(" WITH a, b WHERE b:Interest") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"23"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, comparesTheExplorationEndPropertyAlongALongChain) {
    const std::string query = "MATCH (a)-[*1..2]->(b) WITH a, b" + repeat(" WITH a, b WHERE b.name = 'Gym'") + " RETURN count(b)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, fetchesNodesBelowALongChainOfFilters) {
    const std::string query = "UNWIND [0, 1] AS i MATCH (n) WITH i, n" + repeat(" WITH i, n WHERE i >= 0") + " WITH i, n WHERE n = i RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, returnsNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x RETURN n.name";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, returnsNodesHoppingToALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n)-->(m) WHERE m.name = x RETURN n.name";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Ghosts"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, ordersNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x RETURN n.name ORDER BY n.name LIMIT 1";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, returnsLabelledNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n:Person) WHERE n.name = x RETURN n.name";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, returnsNodesReachedByATypedHopFromALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n)-[:INTERESTED_IN]->(m) WHERE m.name = x RETURN n.name";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, groupsNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x RETURN n.name, count(*)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam", "1"}, {"Remy", "1"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, returnsDistinctNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x RETURN DISTINCT n.name";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, returnsTheUnwoundElementBesideNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x RETURN x, n.age";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {{"Adam", "32"}, {"Remy", "32"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DeepChainQueryTest, returnsNodesMatchedByALongNodeIDDisjunction) {
    const std::string query = "UNWIND " + nodeIDsListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n = x RETURN n.name";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    std::vector<StringRowSink::Row> rows;
    sortRows(sink, rows);

    const std::vector<StringRowSink::Row> expected {
        {"Adam"},
        {"Animals"},
        {"Bio"},
        {"Computers"},
        {"Cooking"},
        {"Cyrus"},
        {"Doruk"},
        {"Eighties"},
        {"Ghosts"},
        {"Gym"},
        {"JiuJitsu"},
        {"Luc"},
        {"Martina"},
        {"Maxime"},
        {"Padel"},
        {"Remy"},
        {"Suhas"},
        {"Travel"},
    };
    EXPECT_EQ(rows, expected);
}
